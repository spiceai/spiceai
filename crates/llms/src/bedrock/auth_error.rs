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

/// The Bedrock chat docs, which carry the credential parameters for a chat model.
pub(crate) const CHAT_DOCS_URL: &str = "https://spiceai.org/docs/components/models/bedrock";

/// The Bedrock embeddings docs, which carry the credential parameters for an embedding model.
pub(crate) const EMBEDDINGS_DOCS_URL: &str =
    "https://spiceai.org/docs/components/embeddings/bedrock";

/// The model name to report when the request never carried one.
pub(crate) const UNKNOWN_MODEL: &str = "<unknown>";

/// Codes AWS returns when it will not accept the request's credentials at all — the key is
/// unknown, the signature does not match it, or the session token has expired. Retrying cannot
/// help; the credentials themselves have to change.
const CREDENTIALS_REJECTED_CODES: &[&str] = &[
    "UnrecognizedClientException",
    "InvalidSignatureException",
    "InvalidClientTokenId",
    "MissingAuthenticationToken",
    "IncompleteSignature",
    "ExpiredToken",
    "ExpiredTokenException",
];

/// Codes AWS returns when the credentials are valid but the identity may not make this call —
/// a missing IAM action, or model access not granted for the model in this region.
const ACCESS_DENIED_CODES: &[&str] = &["AccessDeniedException", "UnauthorizedException"];

/// What the operator should change, chosen by which half of the rejection this is.
const CREDENTIALS_REJECTED_REMEDY: &str = "Set `aws_access_key_id` and `aws_secret_access_key` to an active key pair (and `aws_session_token` if the credentials are temporary), or set `aws_iam_role_source` or `aws_profile` to resolve them instead.";
const ACCESS_DENIED_REMEDY: &str = "Grant the identity the `bedrock:InvokeModel` action on this model, and request access to the model in the Amazon Bedrock console for the region in `aws_region`.";

#[derive(Debug, Snafu)]
#[snafu(display("Failed to call Bedrock model '{model_id}': {detail}. {remedy} See: {docs_url}"))]
pub struct BedrockAuthError {
    model_id: String,
    detail: String,
    remedy: &'static str,
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
    docs_url: &'static str,
) -> Option<BedrockAuthError> {
    let code = code?;
    let remedy = if CREDENTIALS_REJECTED_CODES.contains(&code) {
        CREDENTIALS_REJECTED_REMEDY
    } else if ACCESS_DENIED_CODES.contains(&code) {
        ACCESS_DENIED_REMEDY
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
        docs_url,
    })
}

/// Box a Bedrock service error for the caller, substituting the operator-facing explanation
/// when the rejection was an authentication or authorization failure and leaving every other
/// error to render exactly as the SDK renders it.
///
/// `docs_url` is the page for the component that made the call: a Bedrock client serves both
/// chat and embedding models, and each has its own configuration page.
pub(crate) fn explain<E>(
    err: E,
    model_id: &str,
    docs_url: &'static str,
) -> Box<dyn std::error::Error + Send + Sync>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match describe(err.code(), err.message(), model_id, docs_url) {
        Some(explained) => Box::new(explained),
        None => Box::new(err),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ACCESS_DENIED_CODES, CHAT_DOCS_URL, CREDENTIALS_REJECTED_CODES, EMBEDDINGS_DOCS_URL,
        UNKNOWN_MODEL, describe, explain,
    };
    use aws_smithy_types::error::metadata::{ErrorMetadata, ProvideErrorMetadata};

    const MODEL: &str = "amazon.titan-embed-text-v2:0";

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

    #[test]
    fn credential_rejection_names_the_model_the_keys_and_the_docs() {
        let err = describe(
            Some("UnrecognizedClientException"),
            Some("The security token included in the request is invalid."),
            MODEL,
            EMBEDDINGS_DOCS_URL,
        )
        .expect("UnrecognizedClientException is a credential rejection");
        let rendered = err.to_string();

        assert!(rendered.contains(MODEL), "must name the model: {rendered}");
        assert!(
            rendered.contains("UnrecognizedClientException"),
            "must keep the AWS code: {rendered}"
        );
        assert!(
            rendered.contains("The security token included in the request is invalid."),
            "must keep the AWS message: {rendered}"
        );
        assert!(
            rendered.contains("`aws_access_key_id`")
                && rendered.contains("`aws_secret_access_key`"),
            "must name the parameters to change: {rendered}"
        );
        assert!(
            rendered.contains(EMBEDDINGS_DOCS_URL),
            "must link the docs: {rendered}"
        );
        // The whole point of the rewrite: the SDK's own rendering said only this.
        assert!(
            !rendered.contains("unhandled error"),
            "must not read as an unhandled error: {rendered}"
        );
    }

    #[test]
    fn access_denial_asks_for_the_grant_not_for_new_keys() {
        let rendered = describe(
            Some("AccessDeniedException"),
            Some("You don't have access to the model with the specified model ID."),
            MODEL,
            EMBEDDINGS_DOCS_URL,
        )
        .expect("AccessDeniedException is an authorization rejection")
        .to_string();

        assert!(
            rendered.contains("bedrock:InvokeModel"),
            "must name the IAM action: {rendered}"
        );
        assert!(
            !rendered.contains("`aws_access_key_id`"),
            "credentials that AWS accepted must not be blamed: {rendered}"
        );
    }

    #[test]
    fn every_listed_code_is_described_and_carries_a_remedy() {
        for code in CREDENTIALS_REJECTED_CODES
            .iter()
            .chain(ACCESS_DENIED_CODES.iter())
        {
            let rendered = describe(Some(code), None, MODEL, EMBEDDINGS_DOCS_URL)
                .unwrap_or_else(|| panic!("{code} is listed, so it must be described"))
                .to_string();
            assert!(rendered.contains(code), "{code} must appear in {rendered}");
            assert!(
                rendered.contains(EMBEDDINGS_DOCS_URL),
                "{code} must link the docs: {rendered}"
            );
        }
    }

    #[test]
    fn the_two_code_lists_are_disjoint() {
        // A code in both lists would take whichever remedy is tested first, so the operator
        // could be told to rotate credentials AWS had already accepted.
        for code in CREDENTIALS_REJECTED_CODES {
            assert!(
                !ACCESS_DENIED_CODES.contains(code),
                "{code} is classified as both a credential rejection and an access denial"
            );
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
                describe(Some(code), Some("some detail"), MODEL, EMBEDDINGS_DOCS_URL).is_none(),
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
                EMBEDDINGS_DOCS_URL
            )
            .is_none(),
            "a rejection AWS did not label cannot be classified"
        );
    }

    #[test]
    fn an_empty_aws_message_does_not_leave_a_dangling_separator() {
        for message in [Some(""), Some("   "), None] {
            let rendered = describe(
                Some("UnrecognizedClientException"),
                message,
                MODEL,
                EMBEDDINGS_DOCS_URL,
            )
            .expect("still a credential rejection")
            .to_string();
            assert!(
                rendered.contains("(UnrecognizedClientException)"),
                "an absent AWS message must leave the code alone: {rendered}"
            );
            assert!(
                !rendered.contains(": )"),
                "must not render an empty message: {rendered}"
            );
        }
    }

    #[test]
    fn a_request_with_no_model_id_still_renders() {
        let rendered = describe(
            Some("ExpiredTokenException"),
            None,
            UNKNOWN_MODEL,
            EMBEDDINGS_DOCS_URL,
        )
        .expect("still a credential rejection")
        .to_string();
        assert!(
            rendered.contains(UNKNOWN_MODEL),
            "must be explicit that the model is unknown: {rendered}"
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

        let rendered = explain(err, MODEL, EMBEDDINGS_DOCS_URL).to_string();
        assert!(rendered.contains(MODEL), "must name the model: {rendered}");
        assert!(
            rendered.contains("The security token included in the request is invalid."),
            "must carry AWS's own message through: {rendered}"
        );
        assert!(
            rendered.contains("`aws_access_key_id`"),
            "must name the parameter to change: {rendered}"
        );
        assert!(
            !rendered.contains("unhandled error"),
            "the SDK rendering must not survive: {rendered}"
        );
    }

    #[test]
    fn explain_passes_a_non_auth_error_through_unchanged() {
        // Every other Bedrock failure must keep rendering exactly as the SDK renders it.
        let err = service_error("ValidationException", "input too long");
        let sdk_rendering = err.to_string();
        assert_eq!(
            explain(err, MODEL, EMBEDDINGS_DOCS_URL).to_string(),
            sdk_rendering
        );
    }

    #[test]
    fn explain_links_the_docs_page_of_the_calling_component() {
        // A Bedrock client serves chat and embeddings, and the credential parameters are
        // documented on a different page for each.
        assert_ne!(CHAT_DOCS_URL, EMBEDDINGS_DOCS_URL);
        for docs_url in [CHAT_DOCS_URL, EMBEDDINGS_DOCS_URL] {
            let rendered = explain(
                service_error("AccessDeniedException", "no access to the model"),
                MODEL,
                docs_url,
            )
            .to_string();
            assert!(
                rendered.ends_with(docs_url),
                "{rendered} must end with {docs_url}"
            );
        }
    }
}
