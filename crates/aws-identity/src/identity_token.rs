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

//! The workload's AWS identity as the identity token another provider's federation exchanges:
//! AWS STS `GetWebIdentityToken` returns a JWT that AWS signs for the caller's IAM role, from its
//! ambient credentials, on EC2, ECS, Lambda or EKS alike. The account must enable outbound web
//! identity federation once, and the role needs `sts:GetWebIdentityToken`.

use async_trait::async_trait;
use aws_config::Region;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_sts::error::ProvideErrorMetadata;
use aws_sdk_sts::operation::get_web_identity_token::GetWebIdentityTokenError;
use aws_smithy_runtime_api::client::result::SdkError;
use secrecy::SecretString;
use token_provider::workload_identity::IdentityTokenSource;

use crate::named_chain::credential_source;
use crate::{DEFAULT_REGION, default_aws_config};

/// How long a requested token lives. The exchange happens right away, so a short life costs
/// nothing and limits the use of a token that leaks.
const TOKEN_LIFETIME_SECS: i32 = 900;

/// The workload's AWS IAM role, whose identity tokens AWS STS signs for an audience.
pub struct AwsWebIdentityToken {
    sts: aws_sdk_sts::Client,
    audience: String,
    credentials_from: &'static str,
}

impl AwsWebIdentityToken {
    /// The workload's AWS identity for tokens of `audience`, when the AWS credential chain finds
    /// credentials. The region is the configured one (`AWS_REGION`, the profile), or
    /// `us-east-1`.
    ///
    /// # Errors
    ///
    /// When the chain finds no credentials, with each link tried and what it said.
    pub async fn detect(audience: &str) -> Result<Self, String> {
        let config = default_aws_config()
            .region(RegionProviderChain::default_provider().or_else(Region::new(DEFAULT_REGION)))
            .load()
            .await;
        let Some(chain) = config.credentials_provider() else {
            return Err("found no AWS credentials".to_string());
        };
        let credentials_from = credential_source(&chain, "configured AWS credentials").await?;
        Ok(Self {
            sts: aws_sdk_sts::Client::new(&config),
            audience: audience.to_string(),
            credentials_from,
        })
    }
}

#[async_trait]
impl IdentityTokenSource for AwsWebIdentityToken {
    async fn fetch(&self) -> Result<SecretString, String> {
        let response = self
            .sts
            .get_web_identity_token()
            .audience(&self.audience)
            .signing_algorithm("RS256")
            .duration_seconds(TOKEN_LIFETIME_SECS)
            .send()
            .await
            .map_err(|error| refusal(&error))?;
        response
            .web_identity_token()
            .map(|token| SecretString::from(token.to_string()))
            .ok_or_else(|| "AWS STS returned no token".to_string())
    }

    fn describe(&self) -> String {
        format!(
            "the AWS IAM role (STS GetWebIdentityToken, with credentials from {})",
            self.credentials_from
        )
    }

    fn hash_key(&self) -> String {
        format!("aws_web_identity_{}", self.audience)
    }
}

/// Why STS refused a token, and what grants or settings fix it.
fn refusal<R: std::fmt::Debug>(error: &SdkError<GetWebIdentityTokenError, R>) -> String {
    let Some(service) = error.as_service_error() else {
        return format!("AWS STS could not be reached: {error}");
    };
    if service.is_outbound_web_identity_federation_disabled_exception() {
        return "AWS refused: outbound web identity federation is not enabled for the account. Enable it once in IAM, under Account settings, Outbound web identity federation".to_string();
    }
    let reason = match (service.code(), service.message()) {
        (Some(code), Some(message)) => format!("{code}: {message}"),
        (Some(code), None) => code.to_string(),
        (None, Some(message)) => message.to_string(),
        (None, None) => "no reason given".to_string(),
    };
    let reason = reason.trim().trim_end_matches('.').to_string();
    if service.code() == Some("AccessDenied") {
        format!("AWS refused ({reason}). Grant the workload's role `sts:GetWebIdentityToken`")
    } else {
        format!("AWS refused ({reason})")
    }
}
