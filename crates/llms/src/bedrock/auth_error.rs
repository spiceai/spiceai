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

//! Turning a Bedrock authentication or authorization rejection into a message the operator
//! can act on.
//!
//! AWS models only some of these rejections on the Bedrock operation error enums.
//! `UnrecognizedClientException` — what AWS returns for an access key it does not know — is not
//! one of them, so the SDK renders it as `unhandled error (UnrecognizedClientException)`: a
//! string that names neither the model that failed nor anything to change. The modelled
//! `AccessDeniedException` reads better but still names no model and no Spice parameter.

use aws_sdk_bedrockruntime::error::ProvideErrorMetadata;
use snafu::Snafu;

/// The model name to report when the request never carried one.
pub(crate) const UNKNOWN_MODEL: &str = "<unknown>";

/// Which Bedrock call the rejection came from.
///
/// A remedy has to get three things right, and they do not vary together: the docs page that
/// documents the credentials, the credential parameters that component actually accepts, and
/// the IAM action AWS checks. Carrying the operation rather than any one of them is what keeps
/// the three consistent at each call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    /// `Converse` — a chat model's non-streaming request.
    Chat,
    /// `ConverseStream` — a chat model's streaming request.
    ChatStream,
    /// `InvokeModel` — how every Bedrock embedding model is called.
    Embeddings,
}

impl Operation {
    /// The page documenting the credential parameters for the component that made the call.
    fn docs_url(self) -> &'static str {
        match self {
            Self::Chat | Self::ChatStream => "https://spiceai.org/docs/components/models/bedrock",
            Self::Embeddings => "https://spiceai.org/docs/components/embeddings/bedrock",
        }
    }

    /// The IAM action AWS checks for this call. `ConverseStream` is authorized by
    /// `bedrock:InvokeModelWithResponseStream`, so an identity granted only
    /// `bedrock:InvokeModel` still has streaming chat denied.
    fn iam_action(self) -> &'static str {
        match self {
            Self::ChatStream => "bedrock:InvokeModelWithResponseStream",
            Self::Chat | Self::Embeddings => "bedrock:InvokeModel",
        }
    }

    /// The credential parameters this component accepts. `aws_profile` is embeddings-only —
    /// `BedrockModelParams` neither accepts nor forwards it — so a chat remedy that named it
    /// would send the operator to a setting the chat path ignores.
    fn credentials_remedy(self) -> &'static str {
        match self {
            Self::Chat | Self::ChatStream => {
                "Set `aws_access_key_id` and `aws_secret_access_key` to an active key pair (and \
                 `aws_session_token` if the credentials are temporary), or set \
                 `aws_iam_role_source` to resolve them instead."
            }
            Self::Embeddings => {
                "Set `aws_access_key_id` and `aws_secret_access_key` to an active key pair (and \
                 `aws_session_token` if the credentials are temporary), or set \
                 `aws_iam_role_source` or `aws_profile` to resolve them instead."
            }
        }
    }
}

/// Codes AWS returns when it does not accept the identity behind the request at all — the
/// access key is unknown to AWS, no key reached it, or the session token has expired. Retrying
/// cannot help; the credentials themselves have to change.
const CREDENTIALS_REJECTED_CODES: &[&str] = &[
    "UnrecognizedClientException",
    "InvalidClientTokenId",
    "MissingAuthenticationToken",
    "ExpiredToken",
    "ExpiredTokenException",
];

/// Codes AWS returns when it could not verify the request's *signature*. This is deliberately
/// not the credential class: a signature can fail to verify with a perfectly valid key pair —
/// a host clock more than a few minutes out makes the signature expire, and a signature scoped
/// to the wrong region is rejected as mismatched. Telling those operators to replace their keys
/// is advice that cannot work, which is worse than saying nothing.
const SIGNATURE_REJECTED_CODES: &[&str] = &["InvalidSignatureException", "IncompleteSignature"];

/// AWS's own message distinguishes the causes behind these two codes ("Signature expired…" vs
/// "…does not match the signature you provided"), and [`describe`] carries it through, so the
/// remedy names every cause rather than guessing at one.
const SIGNATURE_REMEDY: &str = concat!(
    "AWS could not verify the request signature, which a valid key pair can still fail: ",
    "check that the host clock is accurate (a signature expires minutes after it is made), ",
    "that `aws_region` names the region serving this model, ",
    "and that `aws_secret_access_key` belongs with `aws_access_key_id`."
);

/// Codes AWS returns when the credentials are valid but the identity may not make this call —
/// a missing IAM action, or the identity not having access to this model in this region.
const ACCESS_DENIED_CODES: &[&str] = &["AccessDeniedException", "UnauthorizedException"];

#[derive(Debug, Snafu)]
#[snafu(display("Failed to call Bedrock model '{model_id}': {detail}. {remedy} See: {docs_url}"))]
pub struct BedrockAuthError {
    model_id: String,
    detail: String,
    remedy: String,
    docs_url: &'static str,
}

/// Build the operator-facing message for a rejection AWS has already labelled with `code`.
///
/// Returns `None` for any code that is not an authentication or authorization rejection, which
/// leaves every other error rendering exactly as the SDK renders it.
fn describe(
    code: Option<&str>,
    message: Option<&str>,
    model_id: &str,
    operation: Operation,
) -> Option<BedrockAuthError> {
    let code = code?;
    let remedy = if CREDENTIALS_REJECTED_CODES.contains(&code) {
        operation.credentials_remedy().to_string()
    } else if SIGNATURE_REJECTED_CODES.contains(&code) {
        SIGNATURE_REMEDY.to_string()
    } else if ACCESS_DENIED_CODES.contains(&code) {
        // Not "request access in the console": Bedrock grants model access automatically for
        // most models now, and the ones that don't route through Marketplace or a provider
        // use-case form. Point at the state to confirm, not at one console flow.
        format!(
            "Grant the identity the `{}` action on this model, and confirm the identity has \
             access to this model in the region set by `aws_region`.",
            operation.iam_action()
        )
    } else {
        return None;
    };

    // Keep whatever AWS said alongside its code: the code is what the remedy is chosen from, and
    // the message is often the only thing distinguishing two causes behind one code.
    let detail = match message.map(str::trim).filter(|m| !m.is_empty()) {
        Some(message) => format!("AWS rejected the request ({code}: {message})"),
        None => format!("AWS rejected the request ({code})"),
    };

    Some(BedrockAuthError {
        model_id: model_id.to_string(),
        detail,
        remedy,
        docs_url: operation.docs_url(),
    })
}

/// Box a Bedrock service error for the caller, substituting the operator-facing explanation
/// when the rejection was an authentication or authorization failure and leaving every other
/// error to render exactly as the SDK renders it.
pub(crate) fn explain<E>(
    err: E,
    model_id: &str,
    operation: Operation,
) -> Box<dyn std::error::Error + Send + Sync>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match describe(err.code(), err.message(), model_id, operation) {
        Some(explained) => Box::new(explained),
        None => Box::new(err),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ACCESS_DENIED_CODES, CREDENTIALS_REJECTED_CODES, Operation, SIGNATURE_REJECTED_CODES,
        UNKNOWN_MODEL, describe, explain,
    };
    use aws_smithy_types::error::metadata::{ErrorMetadata, ProvideErrorMetadata};

    const MODEL: &str = "amazon.titan-embed-text-v2:0";
    const EVERY_OPERATION: [Operation; 3] = [
        Operation::Chat,
        Operation::ChatStream,
        Operation::Embeddings,
    ];

    /// Stands in for a Bedrock operation error: the SDK's own types carry both of these
    /// impls, and `explain` needs both to box the error it was handed.
    #[derive(Debug)]
    struct FakeServiceError(ErrorMetadata);

    impl std::fmt::Display for FakeServiceError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            // How the SDK renders a code it does not model.
            match ProvideErrorMetadata::code(self) {
                Some(code) => write!(f, "unhandled error ({code})"),
                None => f.write_str("unhandled error"),
            }
        }
    }

    impl std::error::Error for FakeServiceError {}

    impl ProvideErrorMetadata for FakeServiceError {
        fn meta(&self) -> &ErrorMetadata {
            &self.0
        }
    }

    fn service_error(code: &str, message: &str) -> FakeServiceError {
        FakeServiceError(ErrorMetadata::builder().code(code).message(message).build())
    }

    fn rendered(code: &str, message: Option<&str>, operation: Operation) -> String {
        describe(Some(code), message, MODEL, operation)
            .unwrap_or_else(|| panic!("{code} must be classified"))
            .to_string()
    }

    #[test]
    fn credential_rejection_names_the_model_the_keys_and_the_docs() {
        let out = rendered(
            "UnrecognizedClientException",
            Some("The security token included in the request is invalid."),
            Operation::Embeddings,
        );

        assert!(out.contains(MODEL), "must name the model: {out}");
        assert!(
            out.contains("UnrecognizedClientException"),
            "must keep the AWS code: {out}"
        );
        assert!(
            out.contains("The security token included in the request is invalid."),
            "must keep the AWS message: {out}"
        );
        assert!(
            out.contains("`aws_access_key_id`") && out.contains("`aws_secret_access_key`"),
            "must name the parameters to change: {out}"
        );
        assert!(
            out.contains(Operation::Embeddings.docs_url()),
            "must link the docs: {out}"
        );
        // The whole point of the rewrite: the SDK's own rendering said only this.
        assert!(
            !out.contains("unhandled error"),
            "must not read as an unhandled error: {out}"
        );
    }

    #[test]
    fn access_denial_asks_for_the_grant_not_for_new_keys() {
        let out = rendered(
            "AccessDeniedException",
            Some("You don't have access to the model with the specified model ID."),
            Operation::Embeddings,
        );

        assert!(
            out.contains("bedrock:InvokeModel"),
            "must name the IAM action: {out}"
        );
        assert!(
            !out.contains("`aws_access_key_id`"),
            "credentials that AWS accepted must not be blamed: {out}"
        );
    }

    #[test]
    fn a_streaming_chat_denial_asks_for_the_streaming_iam_action() {
        // AWS authorizes `ConverseStream` with `bedrock:InvokeModelWithResponseStream`. An
        // identity granted only `bedrock:InvokeModel` still has streaming chat denied, so a
        // shared remedy would send the operator to a grant that does not lift the denial.
        let streaming = rendered("AccessDeniedException", None, Operation::ChatStream);
        assert!(
            streaming.contains("`bedrock:InvokeModelWithResponseStream`"),
            "streaming chat must ask for the streaming action: {streaming}"
        );

        for operation in [Operation::Chat, Operation::Embeddings] {
            let out = rendered("AccessDeniedException", None, operation);
            assert!(
                out.contains("`bedrock:InvokeModel`")
                    && !out.contains("InvokeModelWithResponseStream"),
                "{operation:?} is authorized by bedrock:InvokeModel alone: {out}"
            );
        }
    }

    #[test]
    fn a_chat_credential_remedy_names_only_parameters_chat_accepts() {
        // `aws_profile` is an embeddings-only parameter: `BedrockModelParams` neither accepts
        // nor forwards it, so a chat operator who followed that advice would stay
        // unauthenticated while believing they had acted on the message.
        for operation in [Operation::Chat, Operation::ChatStream] {
            let out = rendered("UnrecognizedClientException", None, operation);
            assert!(
                !out.contains("aws_profile"),
                "{operation:?} does not accept `aws_profile`: {out}"
            );
            assert!(
                out.contains("`aws_iam_role_source`"),
                "{operation:?} does accept `aws_iam_role_source`: {out}"
            );
        }

        let embeddings = rendered("UnrecognizedClientException", None, Operation::Embeddings);
        assert!(
            embeddings.contains("`aws_profile`"),
            "embeddings do accept `aws_profile`: {embeddings}"
        );

        // The two remedies differ only in `aws_profile`, so the rest is duplicated prose that
        // one arm could lose in an edit without the assertions above noticing.
        for operation in EVERY_OPERATION {
            let out = rendered("UnrecognizedClientException", None, operation);
            for param in [
                "`aws_access_key_id`",
                "`aws_secret_access_key`",
                "`aws_session_token`",
            ] {
                assert!(
                    out.contains(param),
                    "{operation:?} must name {param}: {out}"
                );
            }
        }
    }

    #[test]
    fn chat_and_embeddings_are_sent_to_their_own_docs_page() {
        assert_ne!(Operation::Chat.docs_url(), Operation::Embeddings.docs_url());
        assert_eq!(Operation::Chat.docs_url(), Operation::ChatStream.docs_url());
        for operation in EVERY_OPERATION {
            let out = rendered("ExpiredTokenException", None, operation);
            assert!(
                out.ends_with(operation.docs_url()),
                "{operation:?} must link its own page: {out}"
            );
        }
    }

    #[test]
    fn every_listed_code_is_described_and_carries_a_remedy() {
        for code in CREDENTIALS_REJECTED_CODES
            .iter()
            .chain(SIGNATURE_REJECTED_CODES.iter())
            .chain(ACCESS_DENIED_CODES.iter())
        {
            for operation in EVERY_OPERATION {
                let out = rendered(code, None, operation);
                assert!(out.contains(code), "{code} must appear in {out}");
                assert!(
                    out.contains(operation.docs_url()),
                    "{code}/{operation:?} must link the docs: {out}"
                );
            }
        }
    }

    #[test]
    fn the_code_lists_are_pairwise_disjoint() {
        // A code in two lists takes whichever remedy is tested first, so the operator could be
        // told to rotate credentials AWS had already accepted, or to fix a clock when the key
        // is simply unknown.
        let lists = [
            ("credentials", CREDENTIALS_REJECTED_CODES),
            ("signature", SIGNATURE_REJECTED_CODES),
            ("access denied", ACCESS_DENIED_CODES),
        ];
        for (i, (name, codes)) in lists.iter().enumerate() {
            for (other_name, other) in &lists[i + 1..] {
                for code in *codes {
                    assert!(
                        !other.contains(code),
                        "{code} is classified as both a {name} and an {other_name} rejection"
                    );
                }
            }
        }
    }

    #[test]
    fn a_signature_failure_is_not_reported_as_a_bad_key_pair() {
        // A signature can fail to verify with a valid key pair — a skewed host clock expires
        // it, and a signature scoped to the wrong region is rejected as mismatched. An operator
        // told to replace their keys would rotate a working key pair and still be broken.
        // Pin the membership, not just the wording: emptying the list would leave the loop
        // below with nothing to iterate and the test would pass with the defect restored.
        assert_eq!(
            SIGNATURE_REJECTED_CODES,
            ["InvalidSignatureException", "IncompleteSignature"],
            "a signature code moved out of this class silently takes the credential remedy"
        );

        for code in SIGNATURE_REJECTED_CODES {
            for operation in EVERY_OPERATION {
                let out = rendered(code, None, operation);
                assert!(
                    out.contains("clock") && out.contains("`aws_region`"),
                    "{code} must name the causes a key rotation cannot fix: {out}"
                );
                assert!(
                    !out.contains("to an active key pair"),
                    "{code} must not be reported as a credential replacement: {out}"
                );
                assert!(
                    !out.contains("bedrock:InvokeModel"),
                    "{code} is not an authorization failure: {out}"
                );
            }
        }
    }

    #[test]
    fn a_non_auth_code_is_left_to_the_sdk() {
        for code in [
            "ThrottlingException",
            "ValidationException",
            "ModelTimeoutException",
            "ServiceUnavailableException",
            "InternalServerException",
        ] {
            assert!(
                describe(Some(code), Some("some detail"), MODEL, Operation::Chat).is_none(),
                "{code} is not an auth failure and must render as the SDK renders it"
            );
        }
    }

    #[test]
    fn an_error_with_no_code_is_left_to_the_sdk() {
        assert!(
            describe(
                None,
                Some("a message but no code"),
                MODEL,
                Operation::Embeddings
            )
            .is_none(),
            "a rejection AWS did not label cannot be classified"
        );
    }

    #[test]
    fn an_empty_aws_message_does_not_leave_a_dangling_separator() {
        for message in [Some(""), Some("   "), None] {
            let out = rendered(
                "UnrecognizedClientException",
                message,
                Operation::Embeddings,
            );
            assert!(
                out.contains("(UnrecognizedClientException)"),
                "an absent AWS message must leave the code alone: {out}"
            );
            assert!(
                !out.contains(": )"),
                "must not render an empty message: {out}"
            );
        }
    }

    #[test]
    fn a_request_with_no_model_id_still_renders() {
        let out = describe(
            Some("ExpiredTokenException"),
            None,
            UNKNOWN_MODEL,
            Operation::Chat,
        )
        .expect("still a credential rejection")
        .to_string();
        assert!(
            out.contains(UNKNOWN_MODEL),
            "must be explicit that the model is unknown: {out}"
        );
    }

    #[test]
    fn explain_replaces_an_auth_rejection_with_the_actionable_message() {
        let err = service_error(
            "UnrecognizedClientException",
            "The security token included in the request is invalid.",
        );
        assert_eq!(
            err.to_string(),
            "unhandled error (UnrecognizedClientException)",
            "the SDK rendering this replaces"
        );

        let out = explain(err, MODEL, Operation::Embeddings).to_string();
        assert!(out.contains(MODEL), "must name the model: {out}");
        assert!(
            out.contains("The security token included in the request is invalid."),
            "must carry AWS's own message through: {out}"
        );
        assert!(
            out.contains("`aws_access_key_id`"),
            "must name the parameter to change: {out}"
        );
        assert!(
            !out.contains("unhandled error"),
            "the SDK rendering must not survive: {out}"
        );
    }

    #[test]
    fn no_message_renders_with_broken_spacing() {
        // These messages are assembled from wrapped source, and `rustfmt` joins a continued
        // top-level `const` onto one line while keeping the indentation the continuation was
        // meant to swallow — which put runs of spaces inside the rendered text.
        for code in CREDENTIALS_REJECTED_CODES
            .iter()
            .chain(SIGNATURE_REJECTED_CODES.iter())
            .chain(ACCESS_DENIED_CODES.iter())
        {
            for operation in EVERY_OPERATION {
                let out = rendered(code, Some("an AWS message"), operation);
                assert!(
                    !out.contains("  "),
                    "{code}/{operation:?} renders a run of spaces: {out}"
                );
                assert!(
                    !out.contains('\n'),
                    "{code}/{operation:?} must stay on one line: {out}"
                );
            }
        }
    }

    #[test]
    fn explain_passes_a_non_auth_error_through_unchanged() {
        // Every other Bedrock failure must keep rendering exactly as the SDK renders it.
        for operation in EVERY_OPERATION {
            let err = service_error("ValidationException", "input too long");
            let sdk_rendering = err.to_string();
            assert_eq!(explain(err, MODEL, operation).to_string(), sdk_rendering);
        }
    }
}
