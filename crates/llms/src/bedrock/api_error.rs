/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Turns a Bedrock failure into an [`ApiError`] that [`ApiErrorKind::of`] can classify.
//!
//! AWS names the failure with an error code such as `ValidationException`. The code goes in
//! `code`, and the matching `OpenAI` error type goes in `type`. Without the type, every Bedrock
//! failure is read as a server error: HTTP 500, and retried by callers that retry server errors.

use std::error::Error;

use async_openai::error::{ApiError, OpenAIError};
use aws_sdk_bedrockruntime::error::ProvideErrorMetadata;
use aws_sdk_bedrockruntime::operation::{
    converse::ConverseError, converse_stream::ConverseStreamError,
};
use aws_sdk_bedrockruntime::types::error::ThrottlingException;

use super::auth_error::{
    ACCESS_DENIED_CODES, CREDENTIALS_REJECTED_CODES, NOT_SUBSCRIBED_CODES, SIGNATURE_REJECTED_CODES,
};
use crate::chat::ApiErrorKind;

const THROTTLING_CODE: &str = "ThrottlingException";

/// The kind of failure an AWS error code names, or `None` for a failure that a later attempt
/// can pass, such as `InternalServerException` or `ModelNotReadyException`.
fn kind_of_code(code: &str) -> Option<ApiErrorKind> {
    match code {
        "ValidationException" => Some(ApiErrorKind::InvalidRequest),
        "ResourceNotFoundException" => Some(ApiErrorKind::NotFound),
        THROTTLING_CODE => Some(ApiErrorKind::RateLimited),
        code if CREDENTIALS_REJECTED_CODES.contains(&code)
            || SIGNATURE_REJECTED_CODES.contains(&code) =>
        {
            Some(ApiErrorKind::Authentication)
        }
        code if ACCESS_DENIED_CODES.contains(&code) || NOT_SUBSCRIBED_CODES.contains(&code) => {
            Some(ApiErrorKind::PermissionDenied)
        }
        _ => None,
    }
}

/// The AWS error code of `err`, or of the first error in its source chain that has one.
///
/// [`super::BedrockClient`] can wrap the SDK error (see [`super::BedrockAuthError`]), and gives
/// back a bare [`ThrottlingException`] when its throttling retries are spent.
fn aws_error_code<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a str> {
    std::iter::successors(Some(err), |&e| e.source()).find_map(|e| {
        if let Some(e) = e.downcast_ref::<ConverseError>() {
            e.code()
        } else if let Some(e) = e.downcast_ref::<ConverseStreamError>() {
            e.code()
        } else if let Some(e) = e.downcast_ref::<ThrottlingException>() {
            Some(e.code().unwrap_or(THROTTLING_CODE))
        } else {
            None
        }
    })
}

/// An [`ApiError`] with `message`, and with the AWS error `code` when there is one.
pub(crate) fn with_code(message: impl Into<String>, code: Option<&str>) -> OpenAIError {
    OpenAIError::ApiError(ApiError {
        message: message.into(),
        r#type: code
            .and_then(kind_of_code)
            .map(|kind| kind.error_type().to_string()),
        param: None,
        code: code.map(ToString::to_string),
    })
}

/// The [`ApiError`] for a failed [`super::BedrockClient`] call.
pub(crate) fn from_client_error(err: &(dyn Error + 'static)) -> OpenAIError {
    with_code(err.to_string(), aws_error_code(err))
}

#[cfg(test)]
mod tests {
    use super::{from_client_error, with_code};
    use crate::bedrock::auth_error::{Operation, explain};
    use crate::chat::ApiErrorKind;
    use async_openai::error::{ApiError, OpenAIError};
    use aws_sdk_bedrockruntime::error::ErrorMetadata;
    use aws_sdk_bedrockruntime::operation::{
        converse::ConverseError, converse_stream::ConverseStreamError,
    };
    use aws_sdk_bedrockruntime::types::error::{ThrottlingException, ValidationException};

    fn api_error(error: OpenAIError) -> ApiError {
        let OpenAIError::ApiError(api) = error else {
            panic!("expected an ApiError, got {error:?}");
        };
        api
    }

    fn metadata(code: &str) -> ErrorMetadata {
        ErrorMetadata::builder()
            .code(code)
            .message("from AWS")
            .build()
    }

    /// The error a `Converse` call fails with, as `BedrockClient::do_converse` gives it back.
    fn converse_failure(code: &str) -> Box<dyn std::error::Error + Send + Sync> {
        explain(
            ConverseError::generic(metadata(code)),
            "us.meta.llama3-3-70b-instruct-v1:0",
            Operation::Chat,
        )
    }

    #[test]
    fn a_prompt_that_is_too_long_is_an_invalid_request() {
        let err: Box<dyn std::error::Error + Send + Sync> =
            Box::new(ConverseError::ValidationException(
                ValidationException::builder()
                    .message("maximum context length is 131072 tokens")
                    .meta(metadata("ValidationException"))
                    .build(),
            ));
        let api = api_error(from_client_error(err.as_ref()));
        assert_eq!(ApiErrorKind::of(&api), Some(ApiErrorKind::InvalidRequest));
        assert_eq!(api.code.as_deref(), Some("ValidationException"));
        assert_eq!(api.message, err.to_string());
    }

    #[test]
    fn client_errors_keep_their_kind() {
        for (code, kind) in [
            ("ValidationException", ApiErrorKind::InvalidRequest),
            ("ResourceNotFoundException", ApiErrorKind::NotFound),
            ("ThrottlingException", ApiErrorKind::RateLimited),
            ("UnrecognizedClientException", ApiErrorKind::Authentication),
            ("ExpiredTokenException", ApiErrorKind::Authentication),
            ("InvalidSignatureException", ApiErrorKind::Authentication),
            ("AccessDeniedException", ApiErrorKind::PermissionDenied),
            ("OptInRequired", ApiErrorKind::PermissionDenied),
        ] {
            let err = converse_failure(code);
            let api = api_error(from_client_error(err.as_ref()));
            assert_eq!(ApiErrorKind::of(&api), Some(kind), "{code}");
            assert_eq!(api.code.as_deref(), Some(code), "{code}");
            // An access denial is wrapped to explain the fix; the message must keep it.
            assert_eq!(api.message, err.to_string(), "{code}");
        }
    }

    #[test]
    fn server_errors_have_no_kind() {
        for code in [
            "InternalServerException",
            "ServiceUnavailableException",
            "ModelNotReadyException",
            "ModelTimeoutException",
            "ModelErrorException",
        ] {
            let err = converse_failure(code);
            let api = api_error(from_client_error(err.as_ref()));
            assert_eq!(ApiErrorKind::of(&api), None, "{code}");
            assert_eq!(api.code.as_deref(), Some(code), "{code}");
        }
    }

    #[test]
    fn a_streaming_call_is_classified_like_a_request() {
        let err: Box<dyn std::error::Error + Send + Sync> = explain(
            ConverseStreamError::generic(metadata("AccessDeniedException")),
            "us.amazon.nova-micro-v1:0",
            Operation::ChatStream,
        );
        let api = api_error(from_client_error(err.as_ref()));
        assert_eq!(ApiErrorKind::of(&api), Some(ApiErrorKind::PermissionDenied));
    }

    #[test]
    fn spent_throttling_retries_are_rate_limited() {
        // `BedrockClient` gives back the bare exception, which may carry no metadata.
        let err: Box<dyn std::error::Error + Send + Sync> =
            Box::new(ThrottlingException::builder().message("slow down").build());
        let api = api_error(from_client_error(err.as_ref()));
        assert_eq!(ApiErrorKind::of(&api), Some(ApiErrorKind::RateLimited));
        assert_eq!(api.code.as_deref(), Some("ThrottlingException"));
    }

    #[test]
    fn an_error_without_an_aws_code_has_no_kind() {
        let err: Box<dyn std::error::Error + Send + Sync> = "connection reset".into();
        let api = api_error(from_client_error(err.as_ref()));
        assert_eq!(ApiErrorKind::of(&api), None);
        assert_eq!(api.code, None);

        let api = api_error(with_code("stream failed", None));
        assert_eq!(api.r#type, None);
    }
}
