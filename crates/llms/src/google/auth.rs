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

//! Auth resolution for the Google connector: Google AI Studio (API key) and Vertex AI
//! (GCP service-account/ADC `OAuth2`) are both selected here from `model.params`/
//! `embeddings.params`, producing a ready-to-use [`google_genai::Client`].

use std::str::FromStr;
use std::sync::Arc;

use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use token_provider::{TokenProvider, gcp_service_account_token::GcpServiceAccountTokenProvider};

/// The `OAuth2` scope requested for Vertex AI access tokens.
const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Which Google backend the connector talks to. Selected via the `google_api` param
/// (`model.params.google_api` / `embeddings.params.google_api`), default `google_ai`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GoogleApi {
    /// The public Google AI Studio (Generative Language) API, authenticated with an API key.
    #[default]
    GoogleAi,
    /// Vertex AI, GCP-project/region-scoped and authenticated via a service account.
    VertexAi,
}

impl GoogleApi {
    pub const VALUES: &'static [&'static str] = &["google_ai", "vertex_ai"];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GoogleAi => "google_ai",
            Self::VertexAi => "vertex_ai",
        }
    }
}

impl FromStr for GoogleApi {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "google_ai" => Ok(Self::GoogleAi),
            "vertex_ai" => Ok(Self::VertexAi),
            other => Err(format!(
                "must be one of: {}. Found {other}",
                Self::VALUES.join(", ")
            )),
        }
    }
}

/// Vertex AI-specific params, gathered from whichever `model.params`/`embeddings.params` struct
/// is calling in. Auth-method fields are mutually exclusive — see [`build_client`].
pub struct VertexAuthParams<'a> {
    pub project: Option<&'a str>,
    pub location: Option<&'a str>,
    pub service_account_path: Option<&'a str>,
    pub service_account_key: Option<&'a SecretString>,
    pub application_default_credentials: bool,
}

#[derive(Debug, Snafu)]
pub enum GoogleAuthError {
    #[snafu(display(
        "`{params_prefix}.google_api_key` is required when `{params_prefix}.google_api` is `google_ai` (the default)."
    ))]
    MissingApiKey { params_prefix: String },

    #[snafu(display(
        "`{params_prefix}.google_project` and `{params_prefix}.google_location` are required when `{params_prefix}.google_api` is `vertex_ai`."
    ))]
    MissingProjectOrLocation { params_prefix: String },

    #[snafu(display(
        "Exactly one of `{params_prefix}.google_service_account_path`, `{params_prefix}.google_service_account_key`, or `{params_prefix}.google_application_default_credentials` is required when `{params_prefix}.google_api` is `vertex_ai`."
    ))]
    NoAuthMethodSpecified { params_prefix: String },

    #[snafu(display(
        "Only one of `{params_prefix}.google_service_account_path`, `{params_prefix}.google_service_account_key`, or `{params_prefix}.google_application_default_credentials` may be set."
    ))]
    MultipleAuthMethodsSpecified { params_prefix: String },

    #[snafu(display(
        "`{params_prefix}.google_application_default_credentials` is set, but the `GOOGLE_APPLICATION_CREDENTIALS` environment variable is not."
    ))]
    MissingAdcEnvVar { params_prefix: String },

    #[snafu(display("Failed to read the GCP service account key file at '{path}': {source}"))]
    ReadServiceAccountFile {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to build GCP credentials: {source}"))]
    BuildTokenProvider {
        source: token_provider::gcp_service_account_token::GcpAuthError,
    },

    #[snafu(display("Failed to create Google client: {source}"))]
    BuildClient { source: google_genai::Error },
}

/// Resolves auth params into a ready-to-use [`google_genai::Client`], covering both Google AI
/// Studio (`api_key`) and Vertex AI (`vertex`) modes. `params_prefix` (`"model.params"` or
/// `"embeddings.params"`) is only used to make error messages point at the right spicepod
/// section.
///
/// # Errors
/// Returns [`GoogleAuthError`] if required params are missing, more than one Vertex AI auth
/// method is specified, the service-account key can't be read/parsed, or the token exchange
/// fails.
pub async fn build_client(
    api: GoogleApi,
    api_key: Option<&SecretString>,
    vertex: VertexAuthParams<'_>,
    params_prefix: &str,
) -> Result<google_genai::Client, GoogleAuthError> {
    match api {
        GoogleApi::GoogleAi => {
            let api_key = api_key.context(MissingApiKeySnafu {
                params_prefix: params_prefix.to_string(),
            })?;
            google_genai::Client::new(api_key.expose_secret().to_string()).context(BuildClientSnafu)
        }
        GoogleApi::VertexAi => build_vertex_client(vertex, params_prefix).await,
    }
}

async fn build_vertex_client(
    vertex: VertexAuthParams<'_>,
    params_prefix: &str,
) -> Result<google_genai::Client, GoogleAuthError> {
    let (Some(project), Some(location)) = (vertex.project, vertex.location) else {
        return MissingProjectOrLocationSnafu {
            params_prefix: params_prefix.to_string(),
        }
        .fail();
    };

    let service_account_json = resolve_service_account_json(&vertex, params_prefix).await?;

    let token_provider =
        GcpServiceAccountTokenProvider::try_new(&service_account_json, CLOUD_PLATFORM_SCOPE)
            .await
            .context(BuildTokenProviderSnafu)?;

    google_genai::Client::with_bearer_token(
        Arc::new(token_provider) as Arc<dyn TokenProvider>,
        vertex_base_url(project, location),
    )
    .context(BuildClientSnafu)
}

async fn resolve_service_account_json(
    vertex: &VertexAuthParams<'_>,
    params_prefix: &str,
) -> Result<SecretString, GoogleAuthError> {
    let methods_specified = [
        vertex.service_account_path.is_some(),
        vertex.service_account_key.is_some(),
        vertex.application_default_credentials,
    ]
    .into_iter()
    .filter(|&specified| specified)
    .count();

    if methods_specified > 1 {
        return MultipleAuthMethodsSpecifiedSnafu {
            params_prefix: params_prefix.to_string(),
        }
        .fail();
    }

    if let Some(path) = vertex.service_account_path {
        let contents =
            tokio::fs::read_to_string(path)
                .await
                .context(ReadServiceAccountFileSnafu {
                    path: path.to_string(),
                })?;
        return Ok(SecretString::from(contents));
    }

    if let Some(key) = vertex.service_account_key {
        return Ok(key.clone());
    }

    if vertex.application_default_credentials {
        let path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").map_err(|_| {
            GoogleAuthError::MissingAdcEnvVar {
                params_prefix: params_prefix.to_string(),
            }
        })?;
        let contents = tokio::fs::read_to_string(&path)
            .await
            .context(ReadServiceAccountFileSnafu { path })?;
        return Ok(SecretString::from(contents));
    }

    NoAuthMethodSpecifiedSnafu {
        params_prefix: params_prefix.to_string(),
    }
    .fail()
}

/// Builds the Vertex AI base URL for the Gemini publisher-model API:
/// `https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/publishers/google`,
/// except `location: global`, which uses the non-regionalized host
/// `https://aiplatform.googleapis.com`.
fn vertex_base_url(project: &str, location: &str) -> String {
    let host = if location.eq_ignore_ascii_case("global") {
        "https://aiplatform.googleapis.com".to_string()
    } else {
        format!("https://{location}-aiplatform.googleapis.com")
    };
    format!("{host}/v1/projects/{project}/locations/{location}/publishers/google")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_api_parses_known_values_case_insensitively() {
        assert_eq!("google_ai".parse(), Ok(GoogleApi::GoogleAi));
        assert_eq!("VERTEX_AI".parse(), Ok(GoogleApi::VertexAi));
        assert_eq!(" vertex_ai ".parse(), Ok(GoogleApi::VertexAi));
    }

    #[test]
    fn google_api_rejects_unknown_values() {
        "bedrock"
            .parse::<GoogleApi>()
            .expect_err("bedrock is not a valid GoogleApi value");
    }

    #[test]
    fn google_api_defaults_to_google_ai() {
        assert_eq!(GoogleApi::default(), GoogleApi::GoogleAi);
    }

    #[test]
    fn vertex_base_url_uses_regional_host() {
        assert_eq!(
            vertex_base_url("my-project", "us-central1"),
            "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/publishers/google"
        );
    }

    #[test]
    fn vertex_base_url_uses_non_regional_host_for_global() {
        assert_eq!(
            vertex_base_url("my-project", "global"),
            "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/publishers/google"
        );
    }

    #[tokio::test]
    async fn missing_api_key_is_reported() {
        let err = build_client(
            GoogleApi::GoogleAi,
            None,
            VertexAuthParams {
                project: None,
                location: None,
                service_account_path: None,
                service_account_key: None,
                application_default_credentials: false,
            },
            "model.params",
        )
        .await
        .expect_err("missing api_key should fail");
        assert!(matches!(err, GoogleAuthError::MissingApiKey { .. }));
    }

    #[tokio::test]
    async fn vertex_requires_project_and_location() {
        let err = build_client(
            GoogleApi::VertexAi,
            None,
            VertexAuthParams {
                project: None,
                location: None,
                service_account_path: None,
                service_account_key: None,
                application_default_credentials: false,
            },
            "model.params",
        )
        .await
        .expect_err("missing project/location should fail");
        assert!(matches!(
            err,
            GoogleAuthError::MissingProjectOrLocation { .. }
        ));
    }

    #[tokio::test]
    async fn vertex_requires_exactly_one_auth_method() {
        let err = build_client(
            GoogleApi::VertexAi,
            None,
            VertexAuthParams {
                project: Some("proj"),
                location: Some("us-central1"),
                service_account_path: None,
                service_account_key: None,
                application_default_credentials: false,
            },
            "model.params",
        )
        .await
        .expect_err("no auth method should fail");
        assert!(matches!(err, GoogleAuthError::NoAuthMethodSpecified { .. }));
    }

    #[tokio::test]
    async fn vertex_rejects_multiple_auth_methods() {
        let key = SecretString::from("{}");
        let err = build_client(
            GoogleApi::VertexAi,
            None,
            VertexAuthParams {
                project: Some("proj"),
                location: Some("us-central1"),
                service_account_path: Some("/tmp/does-not-matter.json"),
                service_account_key: Some(&key),
                application_default_credentials: false,
            },
            "model.params",
        )
        .await
        .expect_err("multiple auth methods should fail");
        assert!(matches!(
            err,
            GoogleAuthError::MultipleAuthMethodsSpecified { .. }
        ));
    }

    #[tokio::test]
    async fn vertex_adc_without_env_var_is_reported() {
        // SAFETY: test-only removal of an env var we don't expect other concurrent tests in
        // this process to depend on.
        unsafe {
            std::env::remove_var("GOOGLE_APPLICATION_CREDENTIALS");
        }
        let err = build_client(
            GoogleApi::VertexAi,
            None,
            VertexAuthParams {
                project: Some("proj"),
                location: Some("us-central1"),
                service_account_path: None,
                service_account_key: None,
                application_default_credentials: true,
            },
            "model.params",
        )
        .await
        .expect_err("ADC without env var should fail");
        assert!(matches!(err, GoogleAuthError::MissingAdcEnvVar { .. }));
    }
}
