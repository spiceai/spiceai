/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! AWS KMS as a [`JwtSigner`]: KMS holds an RSA private key, such as a GitHub App's, imported
//! once and never exportable, and signs with it.
//!
//! Spice calls KMS with the AWS credentials of the workload, from the standard AWS credential
//! chain: environment variables, a shared profile, a web identity token (EKS IRSA, a GitHub
//! Actions role), EKS Pod Identity, an ECS task role or an EC2 instance profile. The key's ARN
//! names its region.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::Region;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_kms::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{MessageType, SigningAlgorithmSpec};
use snafu::prelude::*;
use token_provider::jwt_signer::{JwtSigner, SignError};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "'{value}' is not an AWS KMS key ARN or alias ARN, such as arn:aws:kms:us-east-1:111122223333:alias/github-app or arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab. The ARN names the key's region."
    ))]
    InvalidKeyArn { value: String },

    /// Worded as the cause a caller's message names after the key it was signing with.
    #[snafu(display("AWS KMS: {reason}{hint}"))]
    SignFailed { reason: String, hint: &'static str },

    #[snafu(display("AWS KMS returned no signature."))]
    NoSignature,
}

/// A key in AWS KMS, named by its key ARN or an alias ARN.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KmsKey {
    arn: String,
    region: String,
}

/// Whether every character of `value` is ASCII and satisfies `allowed`, and there is one.
fn is_made_of(value: &str, allowed: impl Fn(char) -> bool) -> bool {
    !value.is_empty() && value.chars().all(allowed)
}

impl KmsKey {
    /// The key `value` names: `arn:<partition>:kms:<region>:<account>:key/<key id>`, or
    /// `arn:<partition>:kms:<region>:<account>:alias/<alias>`.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidKeyArn`] for anything else, including a bare key ID or alias, which would
    /// leave the region to the environment.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let value = value.trim();
        let parts: Vec<&str> = value.splitn(6, ':').collect();
        let ["arn", partition, "kms", region, account, resource] = parts.as_slice() else {
            return InvalidKeyArnSnafu { value }.fail();
        };
        let resource_is_valid = match resource.split_once('/') {
            Some(("key", id)) => is_made_of(id, |c| c.is_ascii_alphanumeric() || c == '-'),
            Some(("alias", name)) => is_made_of(name, |c| {
                c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-')
            }),
            _ => false,
        };
        ensure!(
            partition.starts_with("aws")
                && is_made_of(partition, |c| c.is_ascii_lowercase() || c == '-')
                && is_made_of(region, |c| c.is_ascii_lowercase()
                    || c.is_ascii_digit()
                    || c == '-')
                && account.len() == 12
                && is_made_of(account, |c| c.is_ascii_digit())
                && resource_is_valid,
            InvalidKeyArnSnafu { value }
        );
        Ok(Self {
            arn: value.to_string(),
            region: (*region).to_string(),
        })
    }

    /// The key's ARN, as given.
    #[must_use]
    pub fn arn(&self) -> &str {
        &self.arn
    }

    /// The key's region, from its ARN.
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }
}

impl fmt::Display for KmsKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.arn)
    }
}

/// What to do about a failed signature, as a sentence that follows the reason: by the error code KMS
/// returned, or for a request that never reached KMS, by its `message`.
fn refusal_hint(code: Option<&str>, message: &str) -> &'static str {
    match code {
        Some("AccessDeniedException") => {
            ". Grant the workload's IAM identity `kms:Sign` on the key, in the key policy or an IAM policy."
        }
        Some("NotFoundException") => {
            ". Verify the ARN, and that the key or alias is in that account and region."
        }
        Some("DisabledException" | "KMSInvalidStateException") => {
            ". Enable the key, and finish importing its key material if it was created for import."
        }
        Some("InvalidKeyUsageException") => {
            ". The key must be an RSA key (RSA_2048 or larger) with the key usage SIGN_VERIFY, holding the GitHub App's private key."
        }
        Some(
            "UnrecognizedClientException"
            | "InvalidClientTokenId"
            | "ExpiredTokenException"
            | "IncompleteSignature",
        ) => ". Verify the workload's AWS credentials.",
        None if message.contains("credentials") => {
            ". Run Spice with AWS credentials: an IAM role (EKS Pod Identity, IRSA, an ECS task role or an EC2 instance profile), a profile, or in GitHub Actions the role that aws-actions/configure-aws-credentials assumes."
        }
        _ => "",
    }
}

/// Bounds the reason quoted from KMS or the SDK.
const MAX_REASON_LEN: usize = 300;

/// `text` as a clause of a longer message: on one line, without a trailing period, and bounded.
fn clause(text: &str) -> String {
    let text: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let text = text.trim().trim_end_matches('.');
    match text.char_indices().nth(MAX_REASON_LEN) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

/// The deepest cause of `error`: what an error that wraps it adds is context, such as the
/// request it failed, which the SDK prints in full.
fn root_cause(error: &(dyn std::error::Error + 'static)) -> String {
    let mut cause = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    cause.to_string()
}

/// Why a request KMS never answered failed: what went wrong, and its deepest cause.
fn unanswered_reason<E, R>(error: &SdkError<E, R>) -> String
where
    E: std::error::Error + 'static,
    R: fmt::Debug,
{
    let what = match error {
        SdkError::ConstructionFailure(_) => "the request could not be built",
        SdkError::TimeoutError(_) => "the request timed out",
        SdkError::DispatchFailure(_) => "the request was not sent",
        SdkError::ResponseError(_) => "the response could not be read",
        _ => "the request failed",
    };
    match std::error::Error::source(error) {
        Some(source) => format!("{what}: {}", root_cause(source)),
        None => what.to_string(),
    }
}

/// A [`JwtSigner`] that has AWS KMS sign with a key it holds, as
/// `RSASSA_PKCS1_V1_5_SHA_256` over a SHA-256 digest: RS256.
pub struct AwsKmsSigner {
    key: KmsKey,
    client: aws_sdk_kms::Client,
}

impl fmt::Debug for AwsKmsSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsKmsSigner")
            .field("key", &self.key.arn)
            .finish_non_exhaustive()
    }
}

impl AwsKmsSigner {
    /// A signer for `key`, in its region, with the workload's AWS credentials. Credentials are
    /// resolved by the first signature, not here.
    pub async fn new(key: KmsKey) -> Self {
        let config = aws_sdk_credential_bridge::default_aws_config()
            .region(Region::new(key.region.clone()))
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_attempt_timeout(Duration::from_secs(10))
                    .operation_timeout(Duration::from_secs(30))
                    .build(),
            )
            .load()
            .await;
        Self {
            client: aws_sdk_kms::Client::new(&config),
            key,
        }
    }

    async fn sign(&self, digest: &[u8; 32]) -> Result<Vec<u8>, Error> {
        let output = self
            .client
            .sign()
            .key_id(&self.key.arn)
            .message(Blob::new(digest.to_vec()))
            .message_type(MessageType::Digest)
            .signing_algorithm(SigningAlgorithmSpec::RsassaPkcs1V15Sha256)
            .send()
            .await
            .map_err(|error| {
                let (code, reason) = match error.as_service_error() {
                    Some(service) => (
                        service.code(),
                        format!(
                            "{}: {}",
                            service.code().unwrap_or("error"),
                            service.message().unwrap_or("no message")
                        ),
                    ),
                    None => (None, unanswered_reason(&error)),
                };
                // The hint looks at the whole chain, which names a missing credential deep down.
                let chain = DisplayErrorContext(&error).to_string();
                Error::SignFailed {
                    hint: refusal_hint(code, if code.is_some() { &reason } else { &chain }),
                    reason: clause(&reason),
                }
            })?;
        output
            .signature
            .map(Blob::into_inner)
            .context(NoSignatureSnafu)
    }
}

#[async_trait]
impl JwtSigner for AwsKmsSigner {
    async fn sign_sha256_digest(&self, digest: &[u8; 32]) -> Result<Vec<u8>, SignError> {
        self.sign(digest)
            .await
            .map_err(|error| Box::new(error) as _)
    }

    fn key_id(&self) -> &str {
        &self.key.arn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_named_by_its_key_or_alias_arn_which_names_its_region() {
        let parse = |value: &str| {
            KmsKey::parse(value)
                .map(|key| (key.arn, key.region))
                .map_err(|error| error.to_string())
        };
        assert_eq!(
            [
                parse("arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"),
                parse(" arn:aws:kms:eu-west-2:111122223333:alias/github-app "),
                parse("arn:aws-us-gov:kms:us-gov-west-1:111122223333:alias/ci/github_app"),
                parse("arn:aws-cn:kms:cn-north-1:111122223333:key/mrk-1234abcd12ab34cd56ef1234567890ab"),
            ],
            [
                Ok(("arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab".to_string(), "us-east-1".to_string())),
                Ok(("arn:aws:kms:eu-west-2:111122223333:alias/github-app".to_string(), "eu-west-2".to_string())),
                Ok(("arn:aws-us-gov:kms:us-gov-west-1:111122223333:alias/ci/github_app".to_string(), "us-gov-west-1".to_string())),
                Ok(("arn:aws-cn:kms:cn-north-1:111122223333:key/mrk-1234abcd12ab34cd56ef1234567890ab".to_string(), "cn-north-1".to_string())),
            ]
        );
        for value in [
            "1234abcd-12ab-34cd-56ef-1234567890ab",
            "alias/github-app",
            "arn:aws:s3:::bucket",
            "arn:aws:kms::111122223333:alias/github-app",
            "arn:aws:kms:us-east-1:1111:alias/github-app",
            "arn:aws:kms:us-east-1:111122223333:alias/",
            "arn:aws:kms:us-east-1:111122223333:secret/github-app",
            "arn:aws:kms:us-east-1:111122223333:alias/github app",
            "arn:gcp:kms:us-east-1:111122223333:alias/github-app",
            "",
        ] {
            assert_eq!(
                parse(value),
                Err(format!(
                    "'{value}' is not an AWS KMS key ARN or alias ARN, such as arn:aws:kms:us-east-1:111122223333:alias/github-app or arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab. The ARN names the key's region."
                )),
                "{value:?}"
            );
        }
    }

    #[test]
    fn a_request_kms_never_answered_is_named_by_its_deepest_cause_on_one_bounded_line() {
        #[derive(Debug)]
        struct Wrapping(&'static str, Option<Box<Wrapping>>);
        impl fmt::Display for Wrapping {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Wrapping {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1.as_deref().map(|cause| cause as _)
            }
        }
        let error = Wrapping(
            "failed to apply endpoint to request `Request { body: SdkBody { … } }`",
            Some(Box::new(Wrapping(
                "invalid URL\nrelative URL without a base.",
                None,
            ))),
        );
        assert_eq!(
            root_cause(&error),
            "invalid URL\nrelative URL without a base."
        );
        assert_eq!(
            clause(&root_cause(&error)),
            "invalid URL relative URL without a base"
        );
        assert_eq!(clause(&"x".repeat(400)), format!("{}…", "x".repeat(300)));
    }

    #[test]
    fn each_failure_says_what_to_change() {
        let refused = |code: Option<&str>, reason: &str| {
            Error::SignFailed {
                reason: reason.to_string(),
                hint: refusal_hint(code, reason),
            }
            .to_string()
        };
        assert_eq!(
            [
                refused(
                    Some("AccessDeniedException"),
                    "AccessDeniedException: User: arn:aws:sts::111122223333:assumed-role/spice/i-0 is not authorized to perform: kms:Sign on this resource because no identity-based policy allows the kms:Sign action"
                ),
                refused(
                    Some("InvalidKeyUsageException"),
                    "InvalidKeyUsageException: You cannot perform this operation on a ENCRYPT_DECRYPT key"
                ),
                refused(
                    None,
                    "dispatch failure: other: no providers in chain provided credentials"
                ),
                refused(Some("ThrottlingException"), "ThrottlingException: Rate exceeded"),
            ],
            [
                "AWS KMS: AccessDeniedException: User: arn:aws:sts::111122223333:assumed-role/spice/i-0 is not authorized to perform: kms:Sign on this resource because no identity-based policy allows the kms:Sign action. Grant the workload's IAM identity `kms:Sign` on the key, in the key policy or an IAM policy.",
                "AWS KMS: InvalidKeyUsageException: You cannot perform this operation on a ENCRYPT_DECRYPT key. The key must be an RSA key (RSA_2048 or larger) with the key usage SIGN_VERIFY, holding the GitHub App's private key.",
                "AWS KMS: dispatch failure: other: no providers in chain provided credentials. Run Spice with AWS credentials: an IAM role (EKS Pod Identity, IRSA, an ECS task role or an EC2 instance profile), a profile, or in GitHub Actions the role that aws-actions/configure-aws-credentials assumes.",
                "AWS KMS: ThrottlingException: Rate exceeded",
            ]
            .map(str::to_string)
        );
    }
}
