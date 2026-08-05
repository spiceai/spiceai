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

//! HTTP client for the Spice Cloud API.

use std::time::Duration;

use reqwest::{Client, RequestBuilder};
use snafu::ResultExt;

use crate::error::{self, HttpRequestSnafu, Result};
use crate::types::{
    ApiKeysResponse, AuthContext, AuthContextRaw, AuthExchangeRequest, AuthExchangeResponse,
    ContainerImagesResponse, CreateDeploymentRequest, CreateProjectRequest, Deployment,
    DeploymentsResponse, LogsResponse, MetricsResponse, OAuthTokenRequest, OAuthTokenResponse, Org,
    OrgsResponse, Project, ProjectsResponse, RegenerateApiKeyRequest, RegenerateApiKeyResponse,
    RegionsResponse, Secret, SecretsResponse, SetSecretRequest, UpdateProjectRequest,
};

const DEFAULT_BASE_URL: &str = "https://api.spice.ai";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Header carrying the organization a management request should act on. Tokens
/// minted for a single org ignore it; the server is the authority on membership.
const ORG_HEADER: &str = "X-Org-Name";

/// HTTP client for the Spice Cloud API.
#[derive(Clone)]
pub struct CloudClient {
    base_url: String,
    client: Client,
    token: Option<String>,
    org: Option<String>,
}

#[expect(
    clippy::missing_errors_doc,
    reason = "All public methods return Result with self-describing error types"
)]
impl CloudClient {
    /// Create a new client with the given base URL.
    ///
    /// Use [`Self::with_token`] to set an authentication token, and
    /// [`Self::with_timeout`] to override the default 30-second timeout.
    pub fn new(base_url: &str) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(DEFAULT_TIMEOUT)
            .https_only(true)
            .build()
            .context(HttpRequestSnafu)?;

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
            token: None,
            org: None,
        })
    }

    /// Create a new client pointing at the default production API.
    pub fn default_url() -> Result<Self> {
        Self::new(DEFAULT_BASE_URL)
    }

    /// Set the bearer token used for authentication.
    #[must_use]
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Set the organization every authenticated request should act on.
    ///
    /// Sent as `X-Org-Name`. Membership is enforced server-side, so this only
    /// states intent — it never grants access the token does not already have.
    #[must_use]
    pub fn with_org(mut self, org: impl Into<String>) -> Self {
        self.org = Some(org.into());
        self
    }

    /// Override the HTTP request timeout (default: 30 s).
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be rebuilt with the given timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self> {
        self.client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(timeout)
            .https_only(true)
            .build()
            .context(HttpRequestSnafu)?;
        Ok(self)
    }

    /// Return the configured base URL.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    // ========================================================================
    // Auth
    // ========================================================================

    /// Build the browser auth URL for the device login flow.
    #[must_use]
    pub fn get_auth_url(&self, auth_code: &str) -> String {
        format!("{}/auth/token?code={}", self.oauth_base_url(), auth_code)
    }

    /// Exchange a device auth code for an access token.
    ///
    /// Returns `Ok(None)` while the user has not yet completed the browser flow.
    /// Returns [`error::Error::AuthorizationDenied`] when the user denies the
    /// browser authorization request. Other errors represent HTTP transport
    /// failures, non-success API responses, or response parsing failures.
    pub async fn exchange_code(&self, auth_code: &str) -> Result<Option<AuthExchangeResponse>> {
        let url = format!("{}/auth/token/exchange", self.oauth_base_url());
        let request = AuthExchangeRequest { code: auth_code };
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let status = response.status();
        if auth_exchange_status_is_pending(status) {
            return Ok(None);
        }

        if !status.is_success() {
            self.handle_empty_response(response).await?;
            return Err(error::Error::Api {
                status: status.as_u16(),
                message: "Unexpected non-success status while exchanging auth code".to_string(),
            });
        }

        let body: AuthExchangeResponse = response.json().await.context(HttpRequestSnafu)?;
        auth_exchange_result(body)
    }

    /// Returns the base URL for OAuth endpoints by stripping the API host segment.
    /// For example, `https://api.spice.ai` → `https://spice.ai`,
    /// `https://dev-api.spice.ai` → `https://dev.spice.ai`,
    /// and `https://staging.api.spice.ai` → `https://staging.spice.ai`.
    fn oauth_base_url(&self) -> String {
        let Ok(mut url) = reqwest::Url::parse(&self.base_url) else {
            return self.base_url.clone();
        };

        let Some(host) = url.host_str() else {
            return self.base_url.clone();
        };

        let Some(rewritten_host) = oauth_host(host) else {
            return self.base_url.clone();
        };

        if url.set_host(Some(&rewritten_host)).is_err() {
            return self.base_url.clone();
        }

        url.to_string().trim_end_matches('/').to_string()
    }

    /// Exchange `OAuth2` client credentials for an access token.
    pub async fn exchange_client_credentials(
        &self,
        client_id: &str,
        client_secret: &str,
    ) -> Result<OAuthTokenResponse> {
        let url = format!("{}/api/oauth/token", self.oauth_base_url());
        let body = OAuthTokenRequest {
            client_id,
            client_secret,
            grant_type: "client_credentials",
        };
        let response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// Get the authentication context for the current token.
    pub async fn get_auth_context(&self) -> Result<AuthContext> {
        self.get_auth_context_for_org(self.org.as_deref()).await
    }

    /// Get the authentication context for `org`, rather than the token's own org.
    ///
    /// The endpoint answers "who am I, in this org" and returns the app API key
    /// for that org's default app, so it doubles as a membership probe. A caller
    /// that is not a member gets a `Forbidden` or `NotFound` error.
    pub async fn get_auth_context_for_org(&self, org: Option<&str>) -> Result<AuthContext> {
        let url = format!("{}/api/spice-cli/auth", self.oauth_base_url());
        let mut request = self.authed(self.client.get(&url));
        if let Some(org) = org {
            request = request.query(&[("org_name", org)]);
        }
        let response = request.send().await.context(HttpRequestSnafu)?;

        let raw: AuthContextRaw = self.handle_response(response).await?;
        Ok(raw.into())
    }

    // ========================================================================
    // Organizations
    // ========================================================================

    /// List the organizations the authenticated identity belongs to.
    ///
    /// Returns [`error::Error::NotFound`] when the deployment does not serve the
    /// endpoint; callers that can degrade should treat that as "unknown" rather
    /// than "no orgs".
    pub async fn list_orgs(&self) -> Result<Vec<Org>> {
        let url = format!("{}/v1/orgs", self.base_url);
        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let orgs: OrgsResponse = self.handle_response(response).await?;
        Ok(orgs.into_orgs())
    }

    // ========================================================================
    // Projects
    // ========================================================================

    /// List all projects visible to the current token.
    ///
    /// Uses `/v1/apps`, which Spice Cloud preserves as a permanent alias of
    /// `/v1/projects` specifically so existing CLI, Terraform, and SDK clients
    /// keep working across the rename. The response envelope differs by path
    /// (`apps` vs `projects`); [`ProjectsResponse`] accepts either, so moving
    /// to the canonical path later is a one-line change.
    pub async fn list_projects(&self) -> Result<Vec<Project>> {
        let url = format!("{}/v1/apps", self.base_url);
        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let projects: ProjectsResponse = self.handle_response(response).await?;
        Ok(projects.into_projects())
    }

    /// Get a single project by numeric ID.
    pub async fn get_project_by_id(&self, project_id: i64) -> Result<Project> {
        let url = format!("{}/v1/apps/{}", self.base_url, project_id);
        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// Create a new project.
    pub async fn create_project(&self, request: &CreateProjectRequest) -> Result<Project> {
        let url = format!("{}/v1/apps", self.base_url);
        let response = self
            .authed(self.client.post(&url))
            .json(request)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// Update an existing project.
    pub async fn update_project(
        &self,
        project_id: i64,
        request: &UpdateProjectRequest,
    ) -> Result<Project> {
        let url = format!("{}/v1/apps/{}", self.base_url, project_id);
        let response = self
            .authed(self.client.put(&url))
            .json(request)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// Delete (soft-delete) a project.
    pub async fn delete_project(&self, project_id: i64) -> Result<()> {
        let url = format!("{}/v1/apps/{}", self.base_url, project_id);
        let response = self
            .authed(self.client.delete(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_empty_response(response).await
    }

    // ========================================================================
    // Deployments
    // ========================================================================

    /// List deployments for a project.
    pub async fn list_deployments(
        &self,
        project_id: i64,
        limit: usize,
        status: Option<&str>,
    ) -> Result<Vec<Deployment>> {
        use std::fmt::Write;

        let mut url = format!(
            "{}/v1/apps/{}/deployments?limit={}",
            self.base_url, project_id, limit
        );
        if let Some(s) = status {
            let _ = write!(url, "&status={s}");
        }

        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let resp: DeploymentsResponse = self.handle_response(response).await?;
        Ok(resp.deployments)
    }

    /// Create a new deployment.
    pub async fn create_deployment(
        &self,
        project_id: i64,
        request: &CreateDeploymentRequest,
    ) -> Result<Deployment> {
        let url = format!("{}/v1/apps/{}/deployments", self.base_url, project_id);
        let response = self
            .authed(self.client.post(&url))
            .json(request)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// Get deployment logs.
    pub async fn get_deployment_logs(
        &self,
        project_id: i64,
        deployment_id: i64,
        limit: usize,
        since: Option<&str>,
    ) -> Result<LogsResponse> {
        use std::fmt::Write;

        let mut url = format!(
            "{}/v1/apps/{}/deployments/{}/logs?limit={}",
            self.base_url, project_id, deployment_id, limit
        );
        if let Some(s) = since {
            let _ = write!(url, "&since={s}");
        }

        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    // ========================================================================
    // Regions & Images
    // ========================================================================

    /// List available deployment regions.
    pub async fn list_regions(&self, env: Option<&str>) -> Result<RegionsResponse> {
        use std::fmt::Write;

        let mut url = format!("{}/v1/regions", self.base_url);
        if let Some(e) = env {
            let _ = write!(url, "?env={e}");
        }

        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// List available container images.
    pub async fn list_container_images(
        &self,
        channel: Option<&str>,
    ) -> Result<ContainerImagesResponse> {
        use std::fmt::Write;

        let mut url = format!("{}/v1/container-images", self.base_url);
        if let Some(c) = channel {
            let _ = write!(url, "?channel={c}");
        }

        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    // ========================================================================
    // Secrets
    // ========================================================================

    /// List secrets for a project.
    pub async fn list_secrets(&self, project_id: i64) -> Result<Vec<Secret>> {
        let url = format!("{}/v1/apps/{}/secrets", self.base_url, project_id);
        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let resp: SecretsResponse = self.handle_response(response).await?;
        Ok(resp.secrets)
    }

    /// Get a single secret by name.
    pub async fn get_secret(&self, project_id: i64, name: &str) -> Result<Secret> {
        let url = format!("{}/v1/apps/{}/secrets/{}", self.base_url, project_id, name);
        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// Create or update a secret.
    pub async fn set_secret(&self, project_id: i64, name: &str, value: &str) -> Result<Secret> {
        let url = format!("{}/v1/apps/{}/secrets", self.base_url, project_id);
        let request = SetSecretRequest {
            name: name.to_string(),
            value: value.to_string(),
        };

        let response = self
            .authed(self.client.post(&url))
            .json(&request)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// Delete a secret.
    pub async fn delete_secret(&self, project_id: i64, name: &str) -> Result<()> {
        let url = format!("{}/v1/apps/{}/secrets/{}", self.base_url, project_id, name);
        let response = self
            .authed(self.client.delete(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_empty_response(response).await
    }

    // ========================================================================
    // API Keys
    // ========================================================================

    /// Get API keys for a project.
    pub async fn get_api_keys(&self, project_id: i64) -> Result<ApiKeysResponse> {
        let url = format!("{}/v1/apps/{}/api-keys", self.base_url, project_id);
        let response = self
            .authed(self.client.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    /// Regenerate an API key.
    pub async fn regenerate_api_key(
        &self,
        project_id: i64,
        key_number: u8,
    ) -> Result<RegenerateApiKeyResponse> {
        let url = format!("{}/v1/apps/{}/api-keys", self.base_url, project_id);
        let request = RegenerateApiKeyRequest { key_number };

        let response = self
            .authed(self.client.post(&url))
            .json(&request)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    // ========================================================================
    // Metrics
    // ========================================================================

    /// Get metrics for a project's instances.
    pub async fn get_project_metrics(
        &self,
        project_id: i64,
        window: Option<&str>,
    ) -> Result<MetricsResponse> {
        let url = format!("{}/v1/apps/{}/metrics", self.base_url, project_id);
        let mut request = self.authed(self.client.get(&url));
        if let Some(w) = window {
            request = request.query(&[("window", w)]);
        }
        let response = request.send().await.context(HttpRequestSnafu)?;

        self.handle_response(response).await
    }

    // ========================================================================
    // Response handling
    // ========================================================================

    async fn handle_response<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T> {
        let status = response.status();
        let body = response.text().await.context(HttpRequestSnafu)?;
        match status.as_u16() {
            200..=202 => {
                serde_json::from_str(&body).map_err(|source| error::Error::JsonParse { source })
            }
            401 => error::UnauthorizedSnafu {
                message: body_or("invalid or expired token", &body),
            }
            .fail(),
            403 => error::ForbiddenSnafu {
                message: body_or("insufficient permissions", &body),
            }
            .fail(),
            404 => error::NotFoundSnafu {
                message: body_or("resource not found", &body),
            }
            .fail(),
            409 => error::ConflictSnafu {
                message: body_or("conflict", &body),
            }
            .fail(),
            _ => error::ApiSnafu {
                status: status.as_u16(),
                message: body,
            }
            .fail(),
        }
    }

    async fn handle_empty_response(&self, response: reqwest::Response) -> Result<()> {
        let status = response.status();
        let body = response.text().await.context(HttpRequestSnafu)?;

        match status.as_u16() {
            200..=204 => Ok(()),
            401 => error::UnauthorizedSnafu {
                message: body_or("invalid or expired token", &body),
            }
            .fail(),
            403 => error::ForbiddenSnafu {
                message: body_or("insufficient permissions", &body),
            }
            .fail(),
            404 => error::NotFoundSnafu {
                message: body_or("resource not found", &body),
            }
            .fail(),
            409 => error::ConflictSnafu {
                message: body_or("conflict", &body),
            }
            .fail(),
            _ => error::ApiSnafu {
                status: status.as_u16(),
                message: body,
            }
            .fail(),
        }
    }

    fn token_str(&self) -> &str {
        self.token.as_deref().unwrap_or("")
    }

    /// Apply the bearer token and, when set, the org context header.
    ///
    /// An org name that cannot be encoded as a header value surfaces as a
    /// request error at send time rather than being dropped, so a command never
    /// silently runs against the token's default org instead of the requested one.
    fn authed(&self, request: RequestBuilder) -> RequestBuilder {
        let request = request.bearer_auth(self.token_str());
        match &self.org {
            Some(org) => request.header(ORG_HEADER, org),
            None => request,
        }
    }
}

fn auth_exchange_result(body: AuthExchangeResponse) -> Result<Option<AuthExchangeResponse>> {
    if body.access_denied {
        return error::AuthorizationDeniedSnafu.fail();
    }

    if body.access_token.is_none() {
        return Ok(None);
    }

    if body.access_token.as_deref().is_some_and(str::is_empty) {
        return error::InvalidResponseSnafu {
            message: "Auth token exchange completed without an access token".to_string(),
        }
        .fail();
    }

    Ok(Some(body))
}

fn auth_exchange_status_is_pending(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::ACCEPTED
}

fn body_or<'a>(fallback: &'a str, body: &'a str) -> &'a str {
    if body.trim().is_empty() {
        fallback
    } else {
        body
    }
}

fn oauth_host(host: &str) -> Option<String> {
    let mut labels: Vec<&str> = host.split('.').collect();

    if let Some(api_index) = labels.iter().position(|label| *label == "api") {
        labels.remove(api_index);
    } else if let Some(label) = labels.iter_mut().find(|label| label.ends_with("-api")) {
        *label = label.trim_end_matches("-api");
    } else {
        return None;
    }

    let rewritten_host = labels.join(".");
    if rewritten_host.is_empty() || labels.iter().any(|label| label.is_empty()) {
        return None;
    }

    Some(rewritten_host)
}

#[cfg(test)]
mod tests {
    use super::{CloudClient, auth_exchange_result, auth_exchange_status_is_pending};
    use crate::types::{AuthContext, AuthContextApp, AuthContextOrg, AuthContextRaw};
    use crate::{error, types::AuthExchangeResponse};

    #[test]
    fn oauth_base_url_rewrites_api_hosts() {
        let cases = [
            ("https://api.spice.ai", "https://spice.ai"),
            ("https://api.spice.ai/", "https://spice.ai"),
            ("https://dev-api.spice.ai", "https://dev.spice.ai"),
            ("https://staging.api.spice.ai", "https://staging.spice.ai"),
        ];

        for (base_url, expected) in cases {
            let client = CloudClient::new(base_url).expect("cloud client should build");
            assert_eq!(client.oauth_base_url(), expected);
        }
    }

    #[test]
    fn oauth_base_url_leaves_non_api_hosts_unchanged() {
        let client = CloudClient::new("https://localhost:8090").expect("cloud client should build");
        assert_eq!(client.oauth_base_url(), "https://localhost:8090");
    }

    #[test]
    fn auth_url_uses_oauth_token_path() {
        let client = CloudClient::new("https://api.spice.ai").expect("cloud client should build");
        assert_eq!(
            client.get_auth_url("ABCD1234"),
            "https://spice.ai/auth/token?code=ABCD1234"
        );
    }

    #[test]
    fn authed_requests_carry_the_org_header() {
        let client = CloudClient::new("https://api.spice.ai")
            .expect("cloud client should build")
            .with_token("token")
            .with_org("spicehq");

        let request = client
            .authed(client.client.get("https://api.spice.ai/v1/apps"))
            .build()
            .expect("request should build");

        assert_eq!(
            request
                .headers()
                .get(super::ORG_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("spicehq")
        );
        assert!(
            request
                .headers()
                .contains_key(reqwest::header::AUTHORIZATION)
        );
    }

    #[test]
    fn authed_requests_omit_the_org_header_when_no_org_is_set() {
        let client = CloudClient::new("https://api.spice.ai")
            .expect("cloud client should build")
            .with_token("token");

        let request = client
            .authed(client.client.get("https://api.spice.ai/v1/apps"))
            .build()
            .expect("request should build");

        assert!(
            !request.headers().contains_key(super::ORG_HEADER),
            "no org context should mean no org header, so the server uses the token's own org"
        );
    }

    #[test]
    fn auth_exchange_denial_is_error() {
        let result = auth_exchange_result(AuthExchangeResponse {
            access_token: None,
            access_denied: true,
        });

        let Err(error::Error::AuthorizationDenied) = result else {
            panic!("denied auth exchange should return an authorization-denied error");
        };
    }

    #[test]
    fn auth_exchange_without_token_is_pending() {
        let result = auth_exchange_result(AuthExchangeResponse {
            access_token: None,
            access_denied: false,
        })
        .expect("pending auth exchange should not fail");

        assert!(result.is_none());
    }

    #[test]
    fn auth_exchange_empty_token_is_error() {
        let result = auth_exchange_result(AuthExchangeResponse {
            access_token: Some(String::new()),
            access_denied: false,
        });

        let Err(error::Error::InvalidResponse { message }) = result else {
            panic!("empty auth token should return an invalid-response error");
        };
        assert!(message.contains("without an access token"));
    }

    #[test]
    fn auth_exchange_accepted_status_is_pending() {
        assert!(auth_exchange_status_is_pending(
            reqwest::StatusCode::ACCEPTED
        ));
        assert!(!auth_exchange_status_is_pending(reqwest::StatusCode::OK));
    }

    #[test]
    fn auth_exchange_with_token_is_success() {
        let result = auth_exchange_result(AuthExchangeResponse {
            access_token: Some("token".to_string()),
            access_denied: false,
        })
        .expect("completed auth exchange should not fail");

        assert!(result.is_some());
    }

    #[test]
    fn auth_context_raw_flattens_nested_org_and_app() {
        let raw = AuthContextRaw {
            username: "ada".to_string(),
            email: "ada@example.com".to_string(),
            org: Some(AuthContextOrg {
                name: Some("analytics".to_string()),
            }),
            app: Some(AuthContextApp {
                name: Some("dashboard".to_string()),
                api_key: Some("secret".to_string()),
            }),
        };
        let ctx: AuthContext = raw.into();
        assert_eq!(ctx.org_name, "analytics");
        assert_eq!(ctx.app_name.as_deref(), Some("dashboard"));
        assert_eq!(ctx.app_api_key.as_deref(), Some("secret"));
    }

    #[test]
    fn auth_context_raw_tolerates_missing_org_and_app() {
        let raw: AuthContextRaw =
            serde_json::from_str(r#"{"username":"ada","email":"ada@example.com"}"#)
                .expect("parse minimal auth context");
        let ctx: AuthContext = raw.into();
        assert_eq!(ctx.username, "ada");
        assert_eq!(ctx.org_name, "");
        assert!(ctx.app_name.is_none());
        assert!(ctx.app_api_key.is_none());
    }

    #[test]
    fn auth_context_raw_parses_nested_wire_format() {
        let body = r#"{
            "username": "ada",
            "email": "ada@example.com",
            "org": {"name": "analytics"},
            "app": {"name": "dashboard", "api_key": "secret"}
        }"#;
        let raw: AuthContextRaw = serde_json::from_str(body).expect("parse nested auth context");
        let ctx: AuthContext = raw.into();
        assert_eq!(ctx.org_name, "analytics");
        assert_eq!(ctx.app_name.as_deref(), Some("dashboard"));
        assert_eq!(ctx.app_api_key.as_deref(), Some("secret"));
    }
}
