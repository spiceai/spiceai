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

//! Model listing for the Google (Gemini) provider, used to suggest valid model ids when a
//! configured one can't be found. Reads the Vertex AI Model Garden catalog with the same
//! service-account credentials the models themselves authenticate with.

use async_trait::async_trait;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;

use crate::google::auth::{VertexAuthParams, VertexCredentials, resolve_credentials};
use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
    map_status_to_error,
};

const PROVIDER_NAME: &str = "Google";

/// `ModelGardenService.ListPublisherModels`, which lists the models a publisher offers. The
/// parent is the publisher alone (`publishers/google`) rather than a project/location, so this
/// path carries no project segment — only the host is location-specific. Listing is served by
/// `v1beta1`; the inference paths this crate builds elsewhere are `v1`.
const PUBLISHER_MODELS_PATH: &str = "/v1beta1/publishers/google/models";

/// Requested in a single page. The hint shows only a handful of models, and one page this size
/// covers Google's published catalog, so the response is never paginated.
const PAGE_SIZE: u32 = 200;

/// The resource-name prefix on every model in the `publishers/google` catalog.
const MODEL_NAME_PREFIX: &str = "publishers/google/models/";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublisherModelsResponse {
    #[serde(default)]
    publisher_models: Vec<PublisherModel>,
}

#[derive(Debug, Deserialize)]
struct PublisherModel {
    name: String,
}

/// The Vertex AI auth params owned by the lister. [`VertexAuthParams`] borrows, and the token
/// exchange can only happen in the async [`ListModels::list_models`], so the values are held
/// here between the two.
struct OwnedVertexAuthParams {
    project: String,
    location: String,
    service_account_path: Option<String>,
    service_account_key: Option<SecretString>,
    application_default_credentials: bool,
}

/// Google (Gemini) model lister that reads the Vertex AI Model Garden catalog.
pub struct GoogleModelLister {
    vertex: OwnedVertexAuthParams,
}

impl GoogleModelLister {
    /// Creates a new model lister from a `from: google` model's parameters.
    ///
    /// Required parameters: `google_project`, `google_location`. The credential parameters are
    /// the same ones the model itself authenticates with, so a config that loads a Google model
    /// can always list them too.
    ///
    /// # Errors
    /// Returns [`ListModelsError::MissingParameter`] if `google_project` or `google_location`
    /// is absent.
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let project = get_required_param(params, "google_project")?
            .expose_secret()
            .to_string();
        let location = get_required_param(params, "google_location")?
            .expose_secret()
            .to_string();

        Ok(Self {
            vertex: OwnedVertexAuthParams {
                project,
                location,
                service_account_path: params
                    .get("google_service_account_path")
                    .map(|s| s.expose_secret().to_string()),
                service_account_key: params.get("google_service_account_key").cloned(),
                application_default_credentials: params
                    .get("google_application_default_credentials")
                    .is_some_and(|s| s.expose_secret().eq_ignore_ascii_case("true")),
            },
        })
    }

    /// Mints credentials for a single listing request. The token provider refreshes in the
    /// background for as long as it is held, so it is built per call and dropped with the
    /// response rather than kept alive for a path that runs only when a model fails to load.
    async fn credentials(&self) -> ListModelsResult<VertexCredentials> {
        resolve_credentials(
            VertexAuthParams {
                project: Some(&self.vertex.project),
                location: Some(&self.vertex.location),
                service_account_path: self.vertex.service_account_path.as_deref(),
                service_account_key: self.vertex.service_account_key.as_ref(),
                application_default_credentials: self.vertex.application_default_credentials,
            },
            "model.params",
        )
        .await
        .map_err(|e| ListModelsError::AuthenticationFailed {
            provider: PROVIDER_NAME.to_string(),
            message: e.to_string(),
        })
    }
}

/// Builds the Model Garden listing URL for a location that [`resolve_credentials`] has already
/// validated — taking it from [`VertexCredentials`] rather than the raw params keeps the
/// URL-injection check between the user's `google_location` and this request's host.
fn publisher_models_url(credentials: &VertexCredentials) -> String {
    format!(
        "{}{PUBLISHER_MODELS_PATH}?pageSize={PAGE_SIZE}",
        credentials.location.host()
    )
}

/// Extracts the configurable model ids from a `ListPublisherModels` response body, keeping only
/// the Gemini models `from: google` can serve.
fn parse_publisher_models(body: &str) -> ListModelsResult<Vec<String>> {
    let response: PublisherModelsResponse =
        serde_json::from_str(body).map_err(|e| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: format!("Failed to parse response: {e}"),
        })?;

    Ok(response
        .publisher_models
        .into_iter()
        .map(|m| {
            m.name
                .strip_prefix(MODEL_NAME_PREFIX)
                .unwrap_or(&m.name)
                .to_string()
        })
        .filter(|id| id.starts_with("gemini"))
        .collect())
}

#[async_trait]
impl ListModels for GoogleModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let client = create_http_client().ok_or_else(|| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: "Failed to create HTTP client".to_string(),
        })?;

        let credentials = self.credentials().await?;

        let mut bearer = HeaderValue::from_str(&format!(
            "Bearer {}",
            credentials.token_provider.get_token()
        ))
        .map_err(|e| ListModelsError::AuthenticationFailed {
            provider: PROVIDER_NAME.to_string(),
            message: format!("Invalid access token: {e}"),
        })?;
        bearer.set_sensitive(true);

        let response = client
            .get(publisher_models_url(&credentials))
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, bearer)
            .send()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(map_status_to_error(response.status(), PROVIDER_NAME));
        }

        let body = response
            .text()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        parse_publisher_models(&body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::auth::VertexLocation;

    fn vertex_params() -> HashMap<String, SecretString> {
        HashMap::from([
            (
                "google_project".to_string(),
                SecretString::from("my-project"),
            ),
            (
                "google_location".to_string(),
                SecretString::from("us-central1"),
            ),
            (
                "google_service_account_path".to_string(),
                SecretString::from("/tmp/key.json"),
            ),
        ])
    }

    #[test]
    fn from_params_requires_project_and_location() {
        let result = GoogleModelLister::from_params(&HashMap::new());
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));

        let mut params = vertex_params();
        params.remove("google_location");
        assert!(matches!(
            GoogleModelLister::from_params(&params),
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn from_params_reads_vertex_credentials() {
        let lister =
            GoogleModelLister::from_params(&vertex_params()).expect("vertex params should parse");
        assert_eq!(lister.vertex.project, "my-project");
        assert_eq!(lister.vertex.location, "us-central1");
        assert_eq!(
            lister.vertex.service_account_path.as_deref(),
            Some("/tmp/key.json")
        );
        assert!(!lister.vertex.application_default_credentials);
    }

    #[test]
    fn from_params_reads_application_default_credentials() {
        let mut params = vertex_params();
        params.remove("google_service_account_path");
        params.insert(
            "google_application_default_credentials".to_string(),
            SecretString::from("true"),
        );
        let lister = GoogleModelLister::from_params(&params).expect("adc params should parse");
        assert!(lister.vertex.application_default_credentials);
        assert!(lister.vertex.service_account_path.is_none());
    }

    /// An API key is no longer an accepted Google credential, so it must not on its own produce
    /// a lister — otherwise the model-not-found hint would send a leftover `google_api_key` to
    /// Google AI Studio, an endpoint the runtime otherwise no longer talks to.
    #[test]
    fn from_params_rejects_an_api_key_alone() {
        let params = HashMap::from([(
            "google_api_key".to_string(),
            SecretString::from("an-api-key"),
        )]);
        assert!(matches!(
            GoogleModelLister::from_params(&params),
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    fn credentials_for(location: VertexLocation) -> VertexCredentials {
        VertexCredentials {
            project: "my-project".to_string(),
            location,
            token_provider: std::sync::Arc::new(token_provider::StaticTokenProvider::new(
                SecretString::from("test-token"),
            )),
        }
    }

    #[test]
    fn publisher_models_url_uses_the_regional_host() {
        assert_eq!(
            publisher_models_url(&credentials_for(VertexLocation::Region(
                "us-central1".to_string()
            ))),
            "https://us-central1-aiplatform.googleapis.com/v1beta1/publishers/google/models?pageSize=200"
        );
    }

    #[test]
    fn publisher_models_url_uses_the_non_regional_host_for_global() {
        assert_eq!(
            publisher_models_url(&credentials_for(VertexLocation::Global)),
            "https://aiplatform.googleapis.com/v1beta1/publishers/google/models?pageSize=200"
        );
    }

    #[test]
    fn parse_publisher_models_strips_the_resource_prefix_and_keeps_gemini_models() {
        // The `ListPublisherModels` response shape: every model is a full resource name.
        let body = r#"{
            "publisherModels": [
                {"name": "publishers/google/models/gemini-2.0-flash"},
                {"name": "publishers/google/models/imagen-3.0-generate-001"},
                {"name": "publishers/google/models/gemini-2.5-pro"},
                {"name": "publishers/google/models/text-bison"}
            ],
            "nextPageToken": "abc"
        }"#;
        assert_eq!(
            parse_publisher_models(body).expect("should parse"),
            vec!["gemini-2.0-flash".to_string(), "gemini-2.5-pro".to_string()]
        );
    }

    #[test]
    fn parse_publisher_models_handles_an_empty_catalog() {
        assert!(
            parse_publisher_models("{}")
                .expect("an empty response should parse")
                .is_empty()
        );
    }

    #[test]
    fn parse_publisher_models_reports_a_malformed_body() {
        assert!(matches!(
            parse_publisher_models("not json"),
            Err(ListModelsError::NetworkError { .. })
        ));
    }
}
