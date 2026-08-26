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

//! Vertex AI auth resolution for the Google connector: `model.params`/`embeddings.params`
//! (GCP service-account/ADC `OAuth2`) are resolved here into a ready-to-use
//! [`google_genai::Client`], or into the [`VertexCredentials`] the model lister signs its own
//! requests with.

use std::sync::Arc;

use secrecy::SecretString;
use snafu::prelude::*;
use token_provider::{TokenProvider, gcp_service_account_token::GcpServiceAccountTokenProvider};

/// The `OAuth2` scope requested for Vertex AI access tokens.
const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Vertex AI auth params, gathered from whichever `model.params`/`embeddings.params` struct is
/// calling in. Auth-method fields are mutually exclusive — see [`build_client`].
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
        "`{params_prefix}.google_project` and `{params_prefix}.google_location` are required."
    ))]
    MissingProjectOrLocation { params_prefix: String },

    #[snafu(display(
        "`{params_prefix}.google_project` ('{project}') is not a valid GCP project id: only lowercase letters, digits, and hyphens are allowed."
    ))]
    InvalidProject {
        params_prefix: String,
        project: String,
    },

    #[snafu(display(
        "`{params_prefix}.google_location` ('{location}') is not a valid GCP region: only lowercase letters, digits, and hyphens are allowed (or `global`)."
    ))]
    InvalidLocation {
        params_prefix: String,
        location: String,
    },

    #[snafu(display(
        "Exactly one of `{params_prefix}.google_service_account_path`, `{params_prefix}.google_service_account_key`, or `{params_prefix}.google_application_default_credentials` is required."
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

/// Resolves Vertex AI auth params into a ready-to-use [`google_genai::Client`]. `params_prefix`
/// (`"model.params"` or `"embeddings.params"`) is only used to make error messages point at the
/// right spicepod section.
///
/// # Errors
/// Returns [`GoogleAuthError`] if required params are missing, more than one auth method is
/// specified, the service-account key can't be read/parsed, or the token exchange fails.
pub async fn build_client(
    vertex: VertexAuthParams<'_>,
    params_prefix: &str,
) -> Result<google_genai::Client, GoogleAuthError> {
    let VertexCredentials {
        project,
        location,
        token_provider,
    } = resolve_credentials(vertex, params_prefix).await?;

    google_genai::Client::with_bearer_token(token_provider, vertex_base_url(&project, &location))
        .context(BuildClientSnafu)
}

/// A validated project/location pair and the token provider that authenticates requests against
/// them. `project` and `location` have passed [`is_safe_gcp_identifier`], so callers may
/// interpolate them into a request URL — build every Vertex AI URL from these fields rather than
/// from the raw params, or [`resolve_credentials`]'s URL-injection check is bypassed.
pub(super) struct VertexCredentials {
    pub project: String,
    pub location: String,
    pub token_provider: Arc<dyn TokenProvider>,
}

/// Validates `vertex` and exchanges its service-account credentials for a Vertex AI token
/// provider. Shared by [`build_client`] and the model lister so both reach Vertex AI through
/// the same validation and the same credential resolution.
pub(super) async fn resolve_credentials(
    vertex: VertexAuthParams<'_>,
    params_prefix: &str,
) -> Result<VertexCredentials, GoogleAuthError> {
    let (Some(project), Some(location)) = (vertex.project, vertex.location) else {
        return MissingProjectOrLocationSnafu {
            params_prefix: params_prefix.to_string(),
        }
        .fail();
    };

    // `project`/`location` are concatenated directly into the request URL's host
    // (`vertex_base_url`) and authority; without validation, a value containing `/` or `.`
    // could redirect the outgoing request (carrying a live GCP bearer token) to an
    // attacker-controlled host. GCP's own project-id/region syntax is a strict subset of
    // this check, so rejecting anything else can't reject a legitimate value.
    ensure!(
        is_safe_gcp_identifier(project),
        InvalidProjectSnafu {
            params_prefix: params_prefix.to_string(),
            project: project.to_string(),
        }
    );
    ensure!(
        location.eq_ignore_ascii_case("global") || is_safe_gcp_identifier(location),
        InvalidLocationSnafu {
            params_prefix: params_prefix.to_string(),
            location: location.to_string(),
        }
    );

    let service_account_json = resolve_service_account_json(&vertex, params_prefix).await?;

    let token_provider =
        GcpServiceAccountTokenProvider::try_new(&service_account_json, CLOUD_PLATFORM_SCOPE)
            .await
            .context(BuildTokenProviderSnafu)?;

    Ok(VertexCredentials {
        project: project.to_string(),
        location: location.to_string(),
        token_provider: Arc::new(token_provider) as Arc<dyn TokenProvider>,
    })
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

/// Whether `s` is safe to interpolate into a URL host/path segment unescaped: this is a strict
/// subset of valid GCP project-id and region syntax (lowercase letters, digits, hyphens; no
/// leading/trailing hyphen), so it can never reject a legitimate project id or region.
fn is_safe_gcp_identifier(s: &str) -> bool {
    !s.is_empty()
        && !s.starts_with('-')
        && !s.ends_with('-')
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// The Vertex AI service endpoint for `location`: the regional host, except `location: global`,
/// which is served by the non-regionalized `aiplatform.googleapis.com`.
pub(super) fn vertex_host(location: &str) -> String {
    if location.eq_ignore_ascii_case("global") {
        "https://aiplatform.googleapis.com".to_string()
    } else {
        format!("https://{location}-aiplatform.googleapis.com")
    }
}

/// Builds the Vertex AI base URL for the Gemini publisher-model API:
/// `https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/publishers/google`.
fn vertex_base_url(project: &str, location: &str) -> String {
    format!(
        "{}/v1/projects/{project}/locations/{location}/publishers/google",
        vertex_host(location)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn vertex_requires_project_and_location() {
        let err = build_client(
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

    #[test]
    fn is_safe_gcp_identifier_accepts_real_project_ids_and_regions() {
        assert!(is_safe_gcp_identifier("my-project-123"));
        assert!(is_safe_gcp_identifier("us-central1"));
        assert!(is_safe_gcp_identifier("sacred-garden-23"));
    }

    #[test]
    fn is_safe_gcp_identifier_rejects_url_injection_attempts() {
        // A `/` or `.` would let `location`/`project` escape the intended URL segment and
        // redirect the request (and its bearer token) to an attacker-controlled host.
        assert!(!is_safe_gcp_identifier("evil.example"));
        assert!(!is_safe_gcp_identifier("evil.example/"));
        assert!(!is_safe_gcp_identifier("us-central1/../../evil"));
        assert!(!is_safe_gcp_identifier(""));
        assert!(!is_safe_gcp_identifier("-leading-hyphen"));
        assert!(!is_safe_gcp_identifier("trailing-hyphen-"));
    }

    #[tokio::test]
    async fn vertex_rejects_invalid_location() {
        let key = SecretString::from("{}");
        let err = build_client(
            VertexAuthParams {
                project: Some("my-project"),
                location: Some("evil.example/"),
                service_account_path: None,
                service_account_key: Some(&key),
                application_default_credentials: false,
            },
            "model.params",
        )
        .await
        .expect_err("an invalid location should be rejected before any network call");
        assert!(matches!(err, GoogleAuthError::InvalidLocation { .. }));
    }

    #[tokio::test]
    async fn vertex_rejects_invalid_project() {
        let key = SecretString::from("{}");
        let err = build_client(
            VertexAuthParams {
                project: Some("evil.example/"),
                location: Some("us-central1"),
                service_account_path: None,
                service_account_key: Some(&key),
                application_default_credentials: false,
            },
            "model.params",
        )
        .await
        .expect_err("an invalid project should be rejected before any network call");
        assert!(matches!(err, GoogleAuthError::InvalidProject { .. }));
    }
}
