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
    ///
    /// The precedence in the remedy is real and load-bearing:
    /// `aws_sdk_credential_bridge::initiate_config_with_credentials` installs a static
    /// credentials provider whenever both key parameters are set and never consults
    /// `aws_iam_role_source`, and a later `profile_name` does not displace it. An operator who
    /// adds the fallback while leaving the rejected keys in place keeps sending those keys.
    fn credentials_remedy(self) -> &'static str {
        match self {
            Self::Chat | Self::ChatStream => {
                "Replace `aws_access_key_id` and `aws_secret_access_key` with an active key pair \
                 (and `aws_session_token` if the credentials are temporary). To resolve \
                 credentials some other way, remove both of those parameters first and then set \
                 `aws_iam_role_source` — an explicit key pair takes precedence and is sent even \
                 when a role source is configured."
            }
            Self::Embeddings => {
                "Replace `aws_access_key_id` and `aws_secret_access_key` with an active key pair \
                 (and `aws_session_token` if the credentials are temporary). To resolve \
                 credentials some other way, remove both of those parameters first and then set \
                 `aws_iam_role_source` or `aws_profile` — an explicit key pair takes precedence \
                 and is sent even when a role source or profile is configured."
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
const SIGNATURE_REJECTED_CODES: &[&str] = &[
    "InvalidSignatureException",
    "IncompleteSignature",
    "RequestExpired",
];

/// AWS's own message distinguishes the causes behind these codes ("Signature expired…" vs
/// "…does not match the signature you provided"), and [`describe`] carries it through, so the
/// remedy names every cause rather than guessing at one.
const SIGNATURE_REMEDY: &str = concat!(
    "AWS could not verify the request signature, which a valid key pair can still fail: ",
    "check that the host clock is accurate (a signature expires minutes after it is made), ",
    "that `aws_region` names the region serving this model, ",
    "that `aws_secret_access_key` belongs with `aws_access_key_id`, ",
    "and that nothing between this host and AWS is rewriting the request."
);

/// Codes AWS returns when it knows the identity but will not let this call through — a missing
/// IAM action, or the identity not having access to this model in this region.
///
/// `NotAuthorized` is from AWS's common-error set rather than Bedrock's own modelled errors, so
/// it arrives unmodelled exactly as the credential codes do.
const ACCESS_DENIED_CODES: &[&str] = &["AccessDeniedException", "NotAuthorized"];

/// The code AWS returns when the *account* is not subscribed to the service, rather than the
/// identity lacking a permission. An IAM grant cannot resolve it, and suggesting one would
/// broaden access while Bedrock stays unavailable — so it is deliberately not an access denial.
const NOT_SUBSCRIBED_CODES: &[&str] = &["OptInRequired"];

const NOT_SUBSCRIBED_REMEDY: &str = concat!(
    "Amazon Bedrock is not enabled for this AWS account in the region set by `aws_region`. ",
    "Enable Bedrock for the account in that region and request access to this model there; ",
    "no IAM grant on the identity can resolve this."
);

#[derive(Debug, Snafu)]
#[snafu(display("Failed to call Bedrock model '{model_id}': {detail}. {remedy} See: {docs_url}"))]
pub struct BedrockAuthError {
    model_id: String,
    detail: String,
    remedy: String,
    docs_url: &'static str,
    /// The SDK error this explains, kept as the source so nothing the replacement does not
    /// render is lost with it: the operation error stays downcastable and its own source chain
    /// stays walkable. Only [`Display`](std::fmt::Display) changes.
    source: Box<dyn std::error::Error + Send + Sync>,
}

/// Squeeze every run of whitespace — including the newlines AWS's own message may carry — down
/// to a single space, so an error stays on one line as the repository requires.
fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Whether AWS's message names the action it refused.
///
/// This is IAM's standard denial phrasing across services ("User: arn:… is not authorized to
/// perform: bedrock:ApplyGuardrail on resource: …"), and it is the only thing that makes
/// "the action named in AWS's message" refer to anything.
fn names_a_denied_action(message: Option<&str>) -> bool {
    message.is_some_and(|m| m.contains("not authorized to perform"))
}

/// Build the `(detail, remedy)` halves of the operator-facing message for a rejection AWS has
/// already labelled with `code`.
///
/// Returns `None` for any code that is not an authentication or authorization rejection, which
/// leaves every other error rendering exactly as the SDK renders it.
fn describe(
    code: Option<&str>,
    message: Option<&str>,
    request_id: Option<&str>,
    operation: Operation,
) -> Option<(String, String)> {
    let code = code?;
    let remedy = if CREDENTIALS_REJECTED_CODES.contains(&code) {
        operation.credentials_remedy().to_string()
    } else if SIGNATURE_REJECTED_CODES.contains(&code) {
        SIGNATURE_REMEDY.to_string()
    } else if NOT_SUBSCRIBED_CODES.contains(&code) {
        NOT_SUBSCRIBED_REMEDY.to_string()
    } else if ACCESS_DENIED_CODES.contains(&code) {
        // Naming one action is not enough on its own: a request carrying a guardrail also needs
        // `bedrock:ApplyGuardrail`, and an inference profile needs its own actions, so an
        // identity that already holds the invoke action can still be denied. When AWS names the
        // action it refused, `detail` carries it and the remedy points there.
        //
        // But it does not always name one — `OptInRequired`, and an `AccessDeniedException`
        // about model access rather than IAM, name none. Referring the operator to an action
        // that is not in the message points them at nothing, so ask for the invoke action
        // outright in that case.
        //
        // Not "request access in the console" either: Bedrock grants model access automatically
        // for most models now, and the rest route through Marketplace or a provider use-case
        // form. Point at the state to confirm, not at one console flow.
        let action = operation.iam_action();
        let grant = if names_a_denied_action(message) {
            format!(
                "Grant the identity the action named in AWS's message — this call needs at least `{action}`"
            )
        } else {
            format!("Grant the identity `{action}` on this model")
        };
        format!(
            "{grant}, and a request using a guardrail or an inference profile needs the further \
             actions those require. Then confirm the account and the identity have access to \
             this model in the region set by `aws_region`."
        )
    } else {
        return None;
    };

    // Keep whatever AWS said alongside its code: the code is what the remedy is chosen from, and
    // the message is often the only thing distinguishing two causes behind one code. The request
    // ID goes in too — it is what AWS support and the service logs are searched by, and this
    // rendering is all an operator sees.
    let mut detail = match message.map(collapse_whitespace).filter(|m| !m.is_empty()) {
        Some(message) => format!("AWS rejected the request ({code}: {message}"),
        None => format!("AWS rejected the request ({code}"),
    };
    if let Some(request_id) = request_id.map(str::trim).filter(|id| !id.is_empty()) {
        detail.push_str("; AWS request ID ");
        detail.push_str(request_id);
    }
    detail.push(')');

    Some((detail, remedy))
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
    // The key AWS's own SDK files the request ID under (`aws_types::request_id`); read from the
    // metadata directly rather than taking a dependency on that crate for one accessor.
    let request_id = err.meta().extra("aws_request_id").map(ToOwned::to_owned);
    match describe(err.code(), err.message(), request_id.as_deref(), operation) {
        Some((detail, remedy)) => Box::new(BedrockAuthError {
            model_id: model_id.to_string(),
            detail,
            remedy,
            docs_url: operation.docs_url(),
            source: Box::new(err),
        }),
        None => Box::new(err),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ACCESS_DENIED_CODES, CREDENTIALS_REJECTED_CODES, NOT_SUBSCRIBED_CODES, Operation,
        SIGNATURE_REJECTED_CODES, UNKNOWN_MODEL, explain,
    };
    use aws_sdk_bedrockruntime::error::ErrorMetadata;
    use aws_sdk_bedrockruntime::operation::{
        converse::ConverseError, converse_stream::ConverseStreamError,
        invoke_model::InvokeModelError,
    };

    const MODEL: &str = "amazon.titan-embed-text-v2:0";
    const EVERY_OPERATION: [Operation; 3] = [
        Operation::Chat,
        Operation::ChatStream,
        Operation::Embeddings,
    ];

    fn metadata(code: &str, message: Option<&str>, request_id: Option<&str>) -> ErrorMetadata {
        let mut builder = ErrorMetadata::builder().code(code);
        if let Some(message) = message {
            builder = builder.message(message);
        }
        if let Some(request_id) = request_id {
            builder = builder.custom("aws_request_id", request_id);
        }
        builder.build()
    }

    /// Render through the real SDK error type an unmodelled code actually arrives as, so these
    /// assertions exercise the same `ProvideErrorMetadata` path the call sites do.
    fn rendered(code: &str, message: Option<&str>, operation: Operation) -> String {
        let err = InvokeModelError::generic(metadata(code, message, None));
        let out = explain(err, MODEL, operation).to_string();
        assert!(
            !out.contains("unhandled error"),
            "{code} must be classified, not left to the SDK: {out}"
        );
        out
    }

    /// What the SDK renders for a code it does not model — the string this whole module exists
    /// to replace, and the one every unclassified error must still get.
    fn sdk_rendering(code: &str) -> String {
        format!("unhandled error ({code})")
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
    fn access_denial_defers_to_the_action_aws_named() {
        // Naming one action would be wrong whenever the request needs more than the invoke
        // action — a guardrail also needs `bedrock:ApplyGuardrail`, and this chat client
        // supports guardrails — so an identity holding only the invoke action is still denied.
        // AWS names the action it refused; the message has to point there, not assert one.
        const IAM_DENIAL: &str = "User: arn:aws:iam::123456789012:user/svc is not authorized to \
                                  perform: bedrock:ApplyGuardrail on resource: *";
        for operation in EVERY_OPERATION {
            let out = rendered("AccessDeniedException", Some(IAM_DENIAL), operation);
            assert!(
                out.contains("the action named in AWS's message"),
                "{operation:?} must defer to AWS's own named action: {out}"
            );
            assert!(
                out.contains("guardrail"),
                "{operation:?} must say further actions can be required: {out}"
            );
            assert!(
                out.contains("at least"),
                "{operation:?} must give the invoke action as a floor, not the whole answer: {out}"
            );
        }
    }

    #[test]
    fn access_denial_asks_outright_when_aws_named_no_action() {
        // `OptInRequired`, and a model-access denial rather than an IAM one, name no action.
        // Referring the operator to "the action named in AWS's message" then points them at
        // something that is not there, so the remedy has to ask for the action outright.
        for (code, message) in [
            (
                "AccessDeniedException",
                Some("You don't have access to the model with the specified model ID."),
            ),
            ("NotAuthorized", None),
        ] {
            for operation in EVERY_OPERATION {
                let out = rendered(code, message, operation);
                assert!(
                    !out.contains("named in AWS's message"),
                    "{code}/{operation:?} names no action, so must not point at one: {out}"
                );
                assert!(
                    out.contains(&format!("Grant the identity `{}`", operation.iam_action())),
                    "{code}/{operation:?} must ask for the action outright: {out}"
                );
                assert!(
                    out.contains("`aws_region`"),
                    "{code}/{operation:?} must still ask for the access check: {out}"
                );
            }
        }
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
    fn a_signature_failure_is_not_reported_as_a_bad_key_pair() {
        // A signature can fail to verify with a valid key pair — a skewed host clock expires
        // it, a signature scoped to the wrong region is rejected as mismatched, and something
        // rewriting the request in flight breaks it. An operator told to replace their keys
        // would rotate a working key pair and still be broken.
        //
        // Pin the membership, not just the wording: emptying the list would leave the loop
        // below with nothing to iterate and this test would pass with the defect restored.
        assert_eq!(
            SIGNATURE_REJECTED_CODES,
            [
                "InvalidSignatureException",
                "IncompleteSignature",
                "RequestExpired"
            ],
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
                    !out.contains("with an active key pair"),
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
    fn an_unsubscribed_account_is_not_reported_as_a_missing_iam_grant() {
        // AWS defines `OptInRequired` as the account needing the service enabled, not the
        // identity needing a permission. Advising an IAM grant broadens access and leaves
        // Bedrock exactly as unavailable as it was.
        assert_eq!(NOT_SUBSCRIBED_CODES, ["OptInRequired"]);
        assert!(
            !ACCESS_DENIED_CODES.contains(&"OptInRequired"),
            "account enablement is not an authorization failure"
        );

        for operation in EVERY_OPERATION {
            let out = rendered("OptInRequired", None, operation);
            assert!(
                out.contains("not enabled for this AWS account"),
                "{operation:?} must name account enablement: {out}"
            );
            assert!(
                out.contains("no IAM grant"),
                "{operation:?} must say an IAM grant cannot resolve it: {out}"
            );
            assert!(
                !out.contains("Grant the identity"),
                "{operation:?} must not ask for an IAM grant: {out}"
            );
        }
    }

    #[test]
    fn the_credential_fallback_says_the_rejected_keys_must_be_removed() {
        // `aws_sdk_credential_bridge::initiate_config_with_credentials` installs a static
        // credentials provider whenever both key parameters are set, and never reaches
        // `aws_iam_role_source`; a later `profile_name` does not displace it. So "set a role
        // source instead" without "remove the keys first" is advice that changes nothing — the
        // operator keeps sending the very keys AWS rejected.
        for operation in EVERY_OPERATION {
            let out = rendered("UnrecognizedClientException", None, operation);
            assert!(
                out.contains("remove both of those parameters first"),
                "{operation:?} must say the keys have to go first: {out}"
            );
            assert!(
                out.contains("takes precedence"),
                "{operation:?} must say why: {out}"
            );
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
        for code in every_code() {
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

    fn every_code() -> impl Iterator<Item = &'static str> {
        CREDENTIALS_REJECTED_CODES
            .iter()
            .chain(SIGNATURE_REJECTED_CODES.iter())
            .chain(NOT_SUBSCRIBED_CODES.iter())
            .chain(ACCESS_DENIED_CODES.iter())
            .copied()
    }

    #[test]
    fn the_code_lists_are_pairwise_disjoint() {
        // A code in two lists takes whichever remedy is tested first, so the operator could be
        // told to rotate credentials AWS had already accepted, or to fix a clock when the key
        // is simply unknown.
        let lists = [
            ("credentials", CREDENTIALS_REJECTED_CODES),
            ("signature", SIGNATURE_REJECTED_CODES),
            ("not subscribed", NOT_SUBSCRIBED_CODES),
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
    fn the_aws_request_id_survives_into_the_message() {
        // It is what AWS support and the service logs are searched by, and the rendered message
        // is all an operator sees, so losing it here loses it entirely.
        let err = InvokeModelError::generic(metadata(
            "UnrecognizedClientException",
            Some("The security token included in the request is invalid."),
            Some("11111111-2222-3333-4444-555555555555"),
        ));
        let out = explain(err, MODEL, Operation::Embeddings).to_string();
        assert!(
            out.contains("AWS request ID 11111111-2222-3333-4444-555555555555"),
            "must carry the request ID: {out}"
        );

        // And its absence must not leave a dangling separator.
        let without = rendered("UnrecognizedClientException", Some("nope"), Operation::Chat);
        assert!(
            !without.contains("request ID"),
            "no request ID means no mention of one: {without}"
        );
        assert!(
            without.contains("(UnrecognizedClientException: nope)"),
            "the detail must still close cleanly: {without}"
        );
    }

    #[test]
    fn the_sdk_error_is_kept_as_the_source() {
        // The replacement changes how the failure reads, not what is reachable behind it: the
        // operation error must stay downcastable and its own chain walkable.
        let err = InvokeModelError::generic(metadata("AccessDeniedException", Some("no"), None));
        let boxed = explain(err, MODEL, Operation::Chat);

        let source =
            std::error::Error::source(boxed.as_ref()).expect("the SDK error is the source");
        assert!(
            source.downcast_ref::<InvokeModelError>().is_some(),
            "the original operation error must stay downcastable: {source}"
        );
        assert_eq!(source.to_string(), sdk_rendering("AccessDeniedException"));
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
            for operation in EVERY_OPERATION {
                let err = InvokeModelError::generic(metadata(code, Some("some detail"), None));
                assert_eq!(
                    explain(err, MODEL, operation).to_string(),
                    sdk_rendering(code),
                    "{code} is not an auth failure and must render as the SDK renders it"
                );
            }
        }
    }

    #[test]
    fn an_error_with_no_code_is_left_to_the_sdk() {
        let err = InvokeModelError::generic(ErrorMetadata::builder().message("no code").build());
        assert_eq!(
            explain(err, MODEL, Operation::Embeddings).to_string(),
            "unhandled error",
            "a rejection AWS did not label cannot be classified"
        );
    }

    #[test]
    fn an_empty_aws_message_does_not_leave_a_dangling_separator() {
        for message in [Some(""), Some("   "), None] {
            let err =
                InvokeModelError::generic(metadata("UnrecognizedClientException", message, None));
            let out = explain(err, MODEL, Operation::Embeddings).to_string();
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
        let err = InvokeModelError::generic(metadata("ExpiredTokenException", None, None));
        let out = explain(err, UNKNOWN_MODEL, Operation::Chat).to_string();
        assert!(
            out.contains(UNKNOWN_MODEL),
            "must be explicit that the model is unknown: {out}"
        );
    }

    #[test]
    fn each_operations_own_sdk_error_type_is_classified() {
        // The three call sites hand `explain` three different SDK error enums. They share a
        // `ProvideErrorMetadata` impl, but nothing in the type system says so — assert each
        // one, through the type that call site actually produces.
        let code = "UnrecognizedClientException";
        let converse = explain(
            ConverseError::generic(metadata(code, None, None)),
            MODEL,
            Operation::Chat,
        )
        .to_string();
        let stream = explain(
            ConverseStreamError::generic(metadata(code, None, None)),
            MODEL,
            Operation::ChatStream,
        )
        .to_string();
        let invoke = explain(
            InvokeModelError::generic(metadata(code, None, None)),
            MODEL,
            Operation::Embeddings,
        )
        .to_string();

        for (name, out) in [
            ("Converse", &converse),
            ("ConverseStream", &stream),
            ("InvokeModel", &invoke),
        ] {
            assert!(
                out.contains(code) && !out.contains("unhandled error"),
                "{name} must be classified: {out}"
            );
        }
        assert_eq!(converse, stream, "both chat calls share the chat remedy");
        assert_ne!(converse, invoke, "embeddings has its own remedy and page");
    }

    #[test]
    fn explain_replaces_an_auth_rejection_with_the_actionable_message() {
        let err = InvokeModelError::generic(metadata(
            "UnrecognizedClientException",
            Some("The security token included in the request is invalid."),
            None,
        ));
        assert_eq!(
            err.to_string(),
            sdk_rendering("UnrecognizedClientException"),
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
        // AWS's message is service text, and `trim` only reaches the ends: a message with an
        // embedded newline or an internal run of spaces would pass straight through into the
        // rendered line. Feed one that has both.
        for message in [
            "an AWS message",
            "first line\nsecond line",
            "  padded  and\t\ttabbed \r\n wrapped  ",
        ] {
            for code in every_code() {
                for operation in EVERY_OPERATION {
                    let out = rendered(code, Some(message), operation);
                    assert!(
                        !out.contains("  "),
                        "{code}/{operation:?} renders a run of spaces: {out}"
                    );
                    assert!(
                        !out.chars().any(|c| c == '\n' || c == '\r' || c == '\t'),
                        "{code}/{operation:?} must stay on one line: {out}"
                    );
                }
            }
        }
    }
}
