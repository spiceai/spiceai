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

#![expect(
    clippy::expect_used,
    reason = "a failed set-up in a test should name itself and stop"
)]

//! How the `OpenAI`-compatible model listers classify a refusal.
//!
//! These drive the whole path a real failure takes — HTTP status, error body, `async-openai`'s
//! own `read_response` mapping, and the lister's classification — against a local server standing
//! in for the provider, so no credentials are involved and the classification is exercised on the
//! error value the client actually builds rather than one written by hand.
//!
//! Every case here is one the client treats as **permanent**: `async-openai` retries a 5xx, and a
//! 429 that is not `insufficient_quota`, under its own backoff, which a one-shot server cannot
//! serve. Those arms are covered by the classifier's unit tests instead.

use std::io::{Read, Write};
use std::net::TcpListener;

use llms::openai::OpenAiModelLister;
use llms::provider::{ListModels, ListModelsError};
use llms::spiceai::SpiceAiModelLister;
use llms::xai::XaiModelLister;
use secrecy::SecretString;

/// A 404 whose message carries a model id containing `0401`. `contains("401")` matches the
/// snapshot date, so a substring classifier reads a model-not-found as a rejected key.
const MODEL_NOT_FOUND_BODY: &str = r#"{"error":{"message":"The model `gpt-4o-mini-2024-0401` does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}"#;

/// A real 401. `OpenAIError`'s `Display` renders `type`, `message` and `code` — none of which
/// spell `401` or `Unauthorized`, so a substring classifier misses the genuine credential
/// failure. The key is the literal placeholder the provider echoes back, not a secret.
const INVALID_API_KEY_BODY: &str = r#"{"error":{"message":"Incorrect API key provided: sk-xxx. You can find your API key at https://platform.openai.com/account/api-keys.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}"#;

/// A 429 carrying `insufficient_quota`, which the client treats as permanent (it retries only a
/// 429 that is *not* out of quota).
const INSUFFICIENT_QUOTA_BODY: &str = r#"{"error":{"message":"You exceeded your current quota, please check your plan and billing details.","type":"insufficient_quota","param":null,"code":"insufficient_quota"}}"#;

/// A 403. The message names a project, not a permission, so nothing in it spells the failure.
const PERMISSION_DENIED_BODY: &str = r#"{"error":{"message":"Project `proj_429` does not have access to model `gpt-4o`.","type":"invalid_request_error","param":null,"code":"model_not_found"}}"#;

/// Serves one request with `status` and `body`, then closes. Returns the base URL to point a
/// lister at.
fn serve_one_error(status: &'static str, body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
    let port = listener
        .local_addr()
        .expect("read the bound address")
        .port();

    std::thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };

        // Read just enough to know the request headers ended; the body is not inspected.
        let mut seen = Vec::new();
        let mut byte = [0u8; 1];
        while stream.read(&mut byte).unwrap_or(0) == 1 {
            seen.push(byte[0]);
            if seen.ends_with(b"\r\n\r\n") {
                break;
            }
        }

        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    });

    format!("http://127.0.0.1:{port}")
}

/// Names a `ListModelsError` variant, for the probe output that carries this test's evidence.
fn variant_of(error: &ListModelsError) -> &'static str {
    match error {
        ListModelsError::RateLimited { .. } => "RateLimited",
        ListModelsError::QuotaExceeded { .. } => "QuotaExceeded",
        ListModelsError::InvalidCredentials { .. } => "InvalidCredentials",
        ListModelsError::AuthenticationFailed { .. } => "AuthenticationFailed",
        ListModelsError::NetworkError { .. } => "NetworkError",
        ListModelsError::ProviderRefused { .. } => "ProviderRefused",
        ListModelsError::MissingParameter { .. } => "MissingParameter",
        ListModelsError::NotSupported { .. } => "NotSupported",
    }
}

/// The three listers this classification is shared by.
#[derive(Clone, Copy)]
enum Provider {
    OpenAi,
    Xai,
    Spice,
}

impl Provider {
    const ALL: [Self; 3] = [Self::OpenAi, Self::Xai, Self::Spice];

    fn name(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Xai => "xai",
            Self::Spice => "spice",
        }
    }

    fn lister(self, base: &str) -> Box<dyn ListModels> {
        let key = SecretString::from("not-a-real-key");
        match self {
            Self::OpenAi => Box::new(OpenAiModelLister::new(&key, Some(base))),
            Self::Xai => Box::new(XaiModelLister::new(&key, Some(base))),
            Self::Spice => Box::new(SpiceAiModelLister::new(Some(&key), Some(base))),
        }
    }
}

/// Drives every lister against its own one-shot server — the server answers a single request, so
/// each provider needs a fresh one — and returns what each of them reported.
async fn refusals(status: &'static str, body: &'static str) -> Vec<ListModelsError> {
    let mut reported = Vec::with_capacity(Provider::ALL.len());

    for provider in Provider::ALL {
        let base = serve_one_error(status, body);
        let error = provider
            .lister(&base)
            .list_models()
            .await
            .expect_err("the server answered with an error status");
        eprintln!(
            "PROBE {:<6} {status} -> {} | {error}",
            provider.name(),
            variant_of(&error)
        );
        reported.push(error);
    }

    reported
}

/// A model id is caller data that the provider echoes back into its message, so no classification
/// may be read out of it. `gpt-4o-mini-2024-0401` contains `401`; a model-not-found must not be
/// reported as a rejected key, which would send the reader to rotate a working credential.
///
/// Regression test for #13747.
#[tokio::test]
async fn a_model_id_containing_401_is_not_a_credential_failure() {
    for error in refusals("404 Not Found", MODEL_NOT_FOUND_BODY).await {
        assert!(
            !matches!(error, ListModelsError::InvalidCredentials { .. }),
            "a model-not-found was reported as a credential failure: {error}"
        );
    }
}

/// The converse half: a genuine 401 must be reported as one. `OpenAIError` drops the HTTP status,
/// and its `Display` renders only `type`, `message` and `code` — so the rendered text of a real
/// rejected key spells neither `401` nor `Unauthorized`.
///
/// Regression test for #13747.
#[tokio::test]
async fn a_rejected_api_key_is_a_credential_failure() {
    for error in refusals("401 Unauthorized", INVALID_API_KEY_BODY).await {
        assert!(
            matches!(error, ListModelsError::InvalidCredentials { .. }),
            "a rejected API key was not reported as a credential failure: {error}"
        );
    }
}

/// An exhausted quota is typed `insufficient_quota` and carries no `402` anywhere in its text.
///
/// Regression test for #13747.
#[tokio::test]
async fn an_exhausted_quota_is_reported_as_one() {
    for error in refusals("429 Too Many Requests", INSUFFICIENT_QUOTA_BODY).await {
        assert!(
            matches!(error, ListModelsError::QuotaExceeded { .. }),
            "an exhausted quota was not reported as one: {error}"
        );
    }
}

/// A project id is caller data too. `proj_429` contains `429`, and a substring classifier reads
/// the refusal as rate limiting — which tells the reader to wait for a condition that will never
/// clear.
///
/// Regression test for #13747.
#[tokio::test]
async fn a_project_id_containing_429_is_not_rate_limiting() {
    for error in refusals("403 Forbidden", PERMISSION_DENIED_BODY).await {
        assert!(
            !matches!(error, ListModelsError::RateLimited { .. }),
            "a permission refusal was reported as rate limiting: {error}"
        );
    }
}
