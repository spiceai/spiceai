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

//! Narrow client for listing attachable projects and atomically attaching one.

use futures::StreamExt as _;
use reqwest::{StatusCode, redirect::Policy};
use runtime_cloud_connect::Identity;
use runtime_cloud_connect::enroll::SessionToken;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

const ATTACH_PATH: &str = "/v1/cloud-connect/attach";
const ATTACHABLE_PROJECTS_PATH: &str = "/v1/cloud-connect/attachable-projects";
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_ATTACHABLE_PROJECT_PAGES: usize = 100;
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display(
        "Failed to attach the Spice Cloud project: the request could not be sent: {source}"
    ))]
    Transport { source: reqwest::Error },

    #[snafu(display(
        "Failed to attach the Spice Cloud project: the response could not be read: {source}"
    ))]
    ResponseBody { source: reqwest::Error },

    #[snafu(display(
        "Failed to attach the Spice Cloud project: the response exceeded the 64 KiB limit"
    ))]
    ResponseTooLarge,

    #[snafu(display(
        "Failed to attach the Spice Cloud project: the server returned an invalid response ({reason})"
    ))]
    InvalidResponse { reason: &'static str },

    #[snafu(display(
        "Failed to attach the Spice Cloud project: the enrolled identity could not sign the request: {reason}"
    ))]
    IdentityProof { reason: String },

    #[snafu(display(
        "Spice Cloud did not attach the project ({status}, code={code}, retryable={retryable})"
    ))]
    Denied {
        status: u16,
        code: ProjectErrorCode,
        retryable: bool,
    },
}

impl Error {
    #[must_use]
    pub(super) fn is_already_attached(&self) -> bool {
        matches!(
            self,
            Self::Denied {
                status: 409,
                code: ProjectErrorCode::InstanceAlreadyAttached,
                retryable: false,
            }
        )
    }

    /// A response that may have followed a committed mutation but did not
    /// prove the authoritative result. Exact replay is the only safe recovery.
    ///
    /// The complement is exactly the non-retryable structured denial, which
    /// proves the requested mutation did not commit — so a negated call is how
    /// a caller asks "is it accurate to report this identity as unattached?".
    #[must_use]
    pub(super) fn is_attachment_ambiguous(&self) -> bool {
        !matches!(
            self,
            Self::Denied {
                retryable: false,
                ..
            }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectErrorCode {
    InvalidProject,
    Unauthenticated,
    Forbidden,
    InstanceNotFound,
    ProjectAlreadyAttached,
    ProjectNotStandalone,
    InstanceNotEnrolled,
    InstanceAlreadyAttached,
    RateLimited,
    Internal,
    Unknown,
}

impl ProjectErrorCode {
    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("invalid_project" | "invalid_project_id") => Self::InvalidProject,
            Some("unauthenticated") => Self::Unauthenticated,
            Some("forbidden") => Self::Forbidden,
            Some("instance_not_found") => Self::InstanceNotFound,
            Some("project_already_attached") => Self::ProjectAlreadyAttached,
            Some("project_not_standalone" | "project_kind_not_standalone") => {
                Self::ProjectNotStandalone
            }
            Some("instance_not_enrolled") => Self::InstanceNotEnrolled,
            Some("instance_already_attached") => Self::InstanceAlreadyAttached,
            Some("rate_limited") => Self::RateLimited,
            Some("internal") => Self::Internal,
            _ => Self::Unknown,
        }
    }
}

impl std::fmt::Display for ProjectErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidProject => "invalid_project",
            Self::Unauthenticated => "unauthenticated",
            Self::Forbidden => "forbidden",
            Self::InstanceNotFound => "instance_not_found",
            Self::ProjectAlreadyAttached => "project_already_attached",
            Self::ProjectNotStandalone => "project_not_standalone",
            Self::InstanceNotEnrolled => "instance_not_enrolled",
            Self::InstanceAlreadyAttached => "instance_already_attached",
            Self::RateLimited => "rate_limited",
            Self::Internal => "internal",
            Self::Unknown => "unknown",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ProjectAttachment {
    pub instance_id: String,
    pub organization: String,
    pub project_id: i64,
    pub project_name: String,
    pub monitor_url: String,
}

pub(crate) struct ProjectClient {
    http: reqwest::Client,
    attach_url: String,
    attachable_projects_url: String,
}

impl std::fmt::Debug for ProjectClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectClient").finish_non_exhaustive()
    }
}

impl ProjectClient {
    pub(crate) fn new(endpoint: &str) -> std::result::Result<Self, Error> {
        Self::build(endpoint, true)
    }

    /// Build against Wiremock's plaintext listener without weakening the
    /// production constructor's HTTPS-only transport.
    #[cfg(test)]
    fn new_allowing_http_for_test(endpoint: &str) -> std::result::Result<Self, Error> {
        Self::build(endpoint, false)
    }

    fn build(endpoint: &str, https_only: bool) -> std::result::Result<Self, Error> {
        reqwest::Url::parse(endpoint).map_err(|_| Error::InvalidResponse {
            reason: "the Cloud endpoint was not an absolute URL",
        })?;
        let http = reqwest::Client::builder()
            // Shorter than the transaction's replay window so a single lost
            // response cannot consume the entire exact-replay opportunity.
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(std::time::Duration::from_secs(10))
            // A redirect must never carry the user bearer to another origin.
            .redirect(Policy::none())
            .https_only(https_only)
            .build()
            .map_err(|source| Error::Transport { source })?;
        Ok(Self {
            http,
            attach_url: format!("{}{ATTACH_PATH}", endpoint.trim_end_matches('/')),
            attachable_projects_url: format!(
                "{}{ATTACHABLE_PROJECTS_PATH}",
                endpoint.trim_end_matches('/')
            ),
        })
    }

    pub(super) async fn attach(
        &self,
        token: &SessionToken,
        organization: &str,
        mutation: &ProjectMutation,
    ) -> std::result::Result<ProjectAttachment, Error> {
        let response = self
            .http
            .post(&self.attach_url)
            .bearer_auth(token.expose_secret())
            .header("X-Org-Name", organization)
            .json(mutation)
            .send()
            .await
            .map_err(|source| Error::Transport { source })?;
        let status = response.status();
        if status.is_redirection() {
            return Err(Error::InvalidResponse {
                reason: "redirect responses are not allowed",
            });
        }
        let body = bounded_body(response).await?;

        if status == StatusCode::NOT_FOUND && serde_json::from_slice::<ErrorWire>(&body).is_err() {
            return Err(Error::InvalidResponse {
                reason: "the Cloud endpoint does not provide /v1/cloud-connect/attach; update the endpoint before linking",
            });
        }
        if !matches!(status, StatusCode::OK | StatusCode::CREATED) {
            return Err(parse_denial(status, &body)?);
        }

        let result = serde_json::from_slice::<ProjectResponse>(&body).map_err(|_| {
            Error::InvalidResponse {
                reason: "response was not the documented JSON object",
            }
        })?;
        let monitor_url = validate_response(
            &result,
            organization,
            &mutation.instance_id,
            mutation.project_id,
        )?;

        Ok(ProjectAttachment {
            instance_id: result.instance_id,
            organization: result.organization.name,
            project_id: result.project.id,
            project_name: result.project.name,
            monitor_url,
        })
    }

    pub(crate) async fn list_attachable(
        &self,
        token: &SessionToken,
    ) -> std::result::Result<Vec<AttachableProject>, Error> {
        let mut projects = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = std::collections::BTreeSet::new();
        for _ in 0..MAX_ATTACHABLE_PROJECT_PAGES {
            let mut url = reqwest::Url::parse(&self.attachable_projects_url).map_err(|_| {
                Error::InvalidResponse {
                    reason: "the attachable-projects endpoint was not an absolute URL",
                }
            })?;
            {
                let mut query = url.query_pairs_mut();
                query.append_pair("limit", "100");
                if let Some(cursor) = cursor.as_deref() {
                    query.append_pair("cursor", cursor);
                }
            }
            let response = self
                .http
                .get(url)
                .bearer_auth(token.expose_secret())
                .send()
                .await
                .map_err(|source| Error::Transport { source })?;
            let status = response.status();
            if status.is_redirection() {
                return Err(Error::InvalidResponse {
                    reason: "redirect responses are not allowed",
                });
            }
            let body = bounded_body(response).await?;
            if status == StatusCode::NOT_FOUND
                && serde_json::from_slice::<ErrorWire>(&body).is_err()
            {
                return Err(Error::InvalidResponse {
                    reason: "the Cloud endpoint does not provide /v1/cloud-connect/attachable-projects; update the endpoint before linking",
                });
            }
            if !status.is_success() {
                return Err(parse_denial(status, &body)?);
            }
            let page = serde_json::from_slice::<AttachableProjectsPage>(&body).map_err(|_| {
                Error::InvalidResponse {
                    reason: "attachable-projects response was not the documented JSON object",
                }
            })?;
            validate_attachable_projects(&page.projects)?;
            projects.extend(page.projects);
            let Some(next_cursor) = page.next_cursor.filter(|cursor| !cursor.is_empty()) else {
                return Ok(projects);
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(Error::InvalidResponse {
                    reason: "attachable-projects response repeated a pagination cursor",
                });
            }
            cursor = Some(next_cursor);
        }
        Err(Error::InvalidResponse {
            reason: "attachable-projects response exceeded the pagination limit",
        })
    }
}

fn validate_attachable_projects(projects: &[AttachableProject]) -> std::result::Result<(), Error> {
    for project in projects {
        if project.id <= 0 || project.name.is_empty() || project.org.is_empty() {
            return Err(Error::InvalidResponse {
                reason: "attachable-projects response contained incomplete project metadata",
            });
        }
        let project_text_is_unsafe = project
            .name
            .chars()
            .chain(project.org.chars())
            .chain(project.region.as_deref().unwrap_or_default().chars())
            .any(char::is_control);
        let instance_text_is_unsafe = project.instances.iter().any(|instance| {
            instance
                .id
                .chars()
                .chain(instance.location.as_deref().unwrap_or_default().chars())
                .chain(instance.enrolled_at.as_deref().unwrap_or_default().chars())
                .any(char::is_control)
        });
        if project_text_is_unsafe || instance_text_is_unsafe {
            return Err(Error::InvalidResponse {
                reason: "attachable-projects response contained control characters",
            });
        }
    }
    Ok(())
}

fn parse_denial(status: StatusCode, body: &[u8]) -> std::result::Result<Error, Error> {
    let wire = serde_json::from_slice::<ErrorWire>(body).map_err(|_| Error::InvalidResponse {
        reason: "non-success response was not the documented JSON object",
    })?;
    let code = ProjectErrorCode::parse(wire.code.as_deref());
    let documented = matches!(
        (status, code),
        (
            StatusCode::BAD_REQUEST,
            ProjectErrorCode::InvalidProject | ProjectErrorCode::ProjectNotStandalone
        ) | (StatusCode::UNAUTHORIZED, ProjectErrorCode::Unauthenticated)
            | (StatusCode::FORBIDDEN, ProjectErrorCode::Forbidden)
            | (
                StatusCode::NOT_FOUND,
                ProjectErrorCode::InstanceNotFound | ProjectErrorCode::InstanceNotEnrolled
            )
            | (
                StatusCode::CONFLICT,
                ProjectErrorCode::ProjectAlreadyAttached
                    | ProjectErrorCode::InstanceAlreadyAttached
            )
            | (StatusCode::TOO_MANY_REQUESTS, ProjectErrorCode::RateLimited)
            | (
                StatusCode::INTERNAL_SERVER_ERROR,
                ProjectErrorCode::Internal
            )
    );
    if !documented {
        return Err(Error::InvalidResponse {
            reason: "non-success response status and code did not match the documented contract",
        });
    }
    let transport_retryable = matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT
    ) || status.is_server_error();
    if wire.retryable != transport_retryable {
        return Err(Error::InvalidResponse {
            reason: "non-success response retryability did not match its status",
        });
    }
    Ok(Error::Denied {
        status: status.as_u16(),
        code,
        retryable: transport_retryable,
    })
}

async fn bounded_body(response: reqwest::Response) -> std::result::Result<Vec<u8>, Error> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|source| Error::ResponseBody { source })?;
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(Error::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn validate_response(
    response: &ProjectResponse,
    expected_org: &str,
    expected_instance: &str,
    expected_project_id: i64,
) -> std::result::Result<String, Error> {
    if response.instance_id != expected_instance {
        return Err(Error::InvalidResponse {
            reason: "instance_id did not match the request",
        });
    }
    if !response
        .organization
        .name
        .eq_ignore_ascii_case(expected_org)
    {
        return Err(Error::InvalidResponse {
            reason: "organization did not match the request",
        });
    }
    if response.project.id != expected_project_id {
        return Err(Error::InvalidResponse {
            reason: "project id did not match the request",
        });
    }
    if response.project.id <= 0 {
        return Err(Error::InvalidResponse {
            reason: "project id was not positive",
        });
    }
    if response
        .organization
        .name
        .chars()
        .chain(response.project.name.chars())
        .any(char::is_control)
    {
        return Err(Error::InvalidResponse {
            reason: "project metadata contained control characters",
        });
    }
    // The same rule every Cloud-delivered link is held to, applied in one place:
    // a second copy of it here is a second thing to keep in step, and the two
    // would sooner or later accept different links.
    runtime_cloud_connect::config::safe_portal_url(&response.monitor_url).ok_or(
        Error::InvalidResponse {
            reason: "monitor_url was not a safe HTTP URL",
        },
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ProjectMutation {
    pub(super) instance_id: String,
    pub(super) project_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) location: Option<String>,
    pub(super) cert_pem: String,
    pub(super) pop_sig: String,
}

impl ProjectMutation {
    pub(super) fn signed(
        identity: &Identity,
        organization: &str,
        project_id: i64,
        location: Option<&str>,
    ) -> std::result::Result<Self, Error> {
        let proof_payload = format!(
            "spice-cloud-connect/attach/v1\n{organization}\n{}\n{project_id}",
            identity.identifier,
        );
        let pop_sig = runtime_cloud_connect::sign_identity_proof(
            &identity.private_key_pem,
            proof_payload.as_bytes(),
        )
        .map_err(|reason| Error::IdentityProof { reason })?;
        Ok(Self {
            instance_id: identity.identifier.clone(),
            project_id,
            location: location.map(ToString::to_string),
            cert_pem: identity.identity_cert_pem.clone(),
            pop_sig,
        })
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(crate) struct AttachableProject {
    pub(crate) id: i64,
    pub(crate) name: String,
    pub(crate) org: String,
    #[serde(default)]
    pub(crate) region: Option<String>,
    #[serde(default)]
    pub(crate) instances: Vec<AttachableInstance>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(crate) struct AttachableInstance {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) location: Option<String>,
    #[serde(default)]
    pub(crate) enrolled_at: Option<String>,
}

#[derive(Deserialize)]
struct AttachableProjectsPage {
    projects: Vec<AttachableProject>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct ProjectResponse {
    instance_id: String,
    organization: NamedResource,
    project: NamedResource,
    monitor_url: String,
}

#[derive(Deserialize)]
struct NamedResource {
    id: i64,
    name: String,
}

#[derive(Deserialize)]
struct ErrorWire {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    retryable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TOKEN: &str = "login-secret-that-must-not-leak";

    fn test_identity() -> Identity {
        let key_pair = rcgen::KeyPair::generate().expect("generate project proof key");
        let certificate = rcgen::CertificateParams::new(Vec::<String>::new())
            .expect("build project proof certificate parameters")
            .self_signed(&key_pair)
            .expect("sign project proof certificate");
        Identity {
            identifier: "inst_8fa21c".to_string(),
            identity_cert_pem: certificate.pem(),
            private_key_pem: key_pair.serialize_pem(),
            public_key_pem: key_pair.public_key_pem(),
            ca_bundle_pem: String::new(),
            gateway_addr: String::new(),
            not_after_unix: None,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
            app_id: None,
            org_name: None,
            app_name: None,
            monitor_url: None,
            new_project_url: None,
            control_plane_endpoint: None,
        }
    }

    fn test_mutation(identity: &Identity) -> ProjectMutation {
        ProjectMutation::signed(identity, "acme", 314, Some("us-east-1"))
            .expect("sign project mutation")
    }

    fn success_body() -> serde_json::Value {
        serde_json::json!({
            "instance_id": "inst_8fa21c",
            "organization": {"id": 42, "name": "acme"},
            "project": {"id": 314, "name": "retail-analytics"},
            "monitor_url": "https://spice.ai/acme/retail-analytics/monitor"
        })
    }

    #[tokio::test]
    async fn attach_sends_the_exact_atomic_project_contract() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ATTACH_PATH))
            .and(header("Authorization", format!("Bearer {TOKEN}")))
            .and(header("X-Org-Name", "acme"))
            .respond_with(ResponseTemplate::new(201).set_body_json(success_body()))
            .expect(1)
            .mount(&server)
            .await;

        let identity = test_identity();
        let mutation = test_mutation(&identity);
        let result = ProjectClient::new_allowing_http_for_test(&server.uri())
            .expect("client")
            .attach(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect("project attached");
        assert_eq!(result.project_id, 314);
        assert_eq!(result.project_name, "retail-analytics");
        let requests = server
            .received_requests()
            .await
            .expect("read received project request");
        let request: serde_json::Value =
            serde_json::from_slice(&requests[0].body).expect("parse project request body");
        assert_eq!(request["instance_id"], "inst_8fa21c");
        assert_eq!(request["project_id"], 314);
        assert_eq!(request["location"], "us-east-1");
        assert!(request.get("name").is_none());
        assert_eq!(request["cert_pem"], identity.identity_cert_pem);
        assert!(
            request["pop_sig"]
                .as_str()
                .is_some_and(|value| !value.is_empty()),
            "project request must carry a proof-of-possession signature"
        );
    }

    #[tokio::test]
    async fn exact_replay_accepts_the_same_attachment_with_status_200() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ATTACH_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_body()))
            .expect(2)
            .mount(&server)
            .await;
        let client = ProjectClient::new_allowing_http_for_test(&server.uri()).expect("client");
        let token = SessionToken::new(TOKEN.to_string());
        let identity = test_identity();
        let mutation = test_mutation(&identity);

        for _ in 0..2 {
            client
                .attach(&token, "acme", &mutation)
                .await
                .expect("exact replay succeeds");
        }
    }

    #[tokio::test]
    async fn attachment_conflicts_are_typed_and_server_detail_is_redacted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "code": "project_already_attached",
                "error": format!("{TOKEN} acme retail-analytics https://private.example"),
                "retryable": false
            })))
            .mount(&server)
            .await;
        let identity = test_identity();
        let mutation = test_mutation(&identity);
        let err = ProjectClient::new_allowing_http_for_test(&server.uri())
            .expect("client")
            .attach(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect_err("conflict");

        assert!(matches!(
            err,
            Error::Denied {
                code: ProjectErrorCode::ProjectAlreadyAttached,
                retryable: false,
                ..
            }
        ));
        let rendered = format!("{err:?} {err}");
        for secret in [TOKEN, "acme", "retail-analytics", "private.example"] {
            assert!(!rendered.contains(secret), "leaked {secret}: {rendered}");
        }
    }

    #[tokio::test]
    async fn mismatched_success_is_not_persistable_attachment_state() {
        let server = MockServer::start().await;
        let mut body = success_body();
        body["instance_id"] = serde_json::Value::String("inst_other".to_string());
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(201).set_body_json(body))
            .mount(&server)
            .await;
        let identity = test_identity();
        let mutation = test_mutation(&identity);
        let err = ProjectClient::new_allowing_http_for_test(&server.uri())
            .expect("client")
            .attach(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect_err("mismatch must fail closed");
        assert!(matches!(err, Error::InvalidResponse { .. }));
    }

    #[tokio::test]
    async fn plaintext_remote_monitor_url_is_rejected() {
        let server = MockServer::start().await;
        let mut body = success_body();
        body["monitor_url"] =
            serde_json::Value::String("http://example.invalid/monitor".to_string());
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(201).set_body_json(body))
            .mount(&server)
            .await;
        let identity = test_identity();
        let mutation = test_mutation(&identity);
        let err = ProjectClient::new_allowing_http_for_test(&server.uri())
            .expect("client")
            .attach(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect_err("plaintext remote monitor URL must fail closed");
        assert!(matches!(err, Error::InvalidResponse { .. }));
    }

    #[tokio::test]
    async fn project_redirect_is_rejected_without_forwarding_the_bearer() {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/stolen"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&target)
            .await;
        Mock::given(method("POST"))
            .and(path(ATTACH_PATH))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/stolen", target.uri())),
            )
            .expect(1)
            .mount(&source)
            .await;

        let identity = test_identity();
        let mutation = test_mutation(&identity);
        let err = ProjectClient::new_allowing_http_for_test(&source.uri())
            .expect("client")
            .attach(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect_err("redirect must fail closed");
        assert!(matches!(err, Error::InvalidResponse { .. }), "{err}");
        target.verify().await;
    }

    #[tokio::test]
    async fn attachable_projects_are_paginated_without_client_side_filtering() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ATTACHABLE_PROJECTS_PATH))
            .and(query_param("limit", "100"))
            .and(query_param_is_missing("cursor"))
            .and(header("Authorization", format!("Bearer {TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "projects": [{
                    "id": 11,
                    "name": "attached-project",
                    "org": "acme",
                    "region": "us-east-1",
                    "instances": [{
                        "id": "inst_other",
                        "location": "iad",
                        "enrolled_at": "2026-08-20T00:00:00Z"
                    }]
                }],
                "next_cursor": "page-2"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(ATTACHABLE_PROJECTS_PATH))
            .and(query_param("limit", "100"))
            .and(query_param("cursor", "page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "projects": [{
                    "id": 12,
                    "name": "standalone-project",
                    "org": "globex",
                    "instances": []
                }],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;

        let projects = ProjectClient::new_allowing_http_for_test(&server.uri())
            .expect("client")
            .list_attachable(&SessionToken::new(TOKEN.to_string()))
            .await
            .expect("list every server-returned project");

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].name, "attached-project");
        assert_eq!(projects[0].instances[0].id, "inst_other");
        assert_eq!(projects[1].org, "globex");
    }

    #[tokio::test]
    async fn repeated_pagination_cursor_is_rejected_without_looping() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ATTACHABLE_PROJECTS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "projects": [],
                "next_cursor": "same-page"
            })))
            .expect(2)
            .mount(&server)
            .await;

        let error = ProjectClient::new_allowing_http_for_test(&server.uri())
            .expect("client")
            .list_attachable(&SessionToken::new(TOKEN.to_string()))
            .await
            .expect_err("a repeated cursor must not loop forever");

        assert!(matches!(error, Error::InvalidResponse { .. }), "{error}");
    }

    #[tokio::test]
    async fn missing_attachable_projects_route_is_actionable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ATTACHABLE_PROJECTS_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let err = ProjectClient::new_allowing_http_for_test(&server.uri())
            .expect("client")
            .list_attachable(&SessionToken::new(TOKEN.to_string()))
            .await
            .expect_err("an old endpoint must not masquerade as an empty project list");

        assert!(err.to_string().contains("update the endpoint"), "{err}");
    }

    #[tokio::test]
    async fn missing_attach_route_is_actionable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ATTACH_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let identity = test_identity();
        let mutation = test_mutation(&identity);

        let err = ProjectClient::new_allowing_http_for_test(&server.uri())
            .expect("client")
            .attach(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect_err("an old endpoint must not look like a malformed documented denial");

        let message = err.to_string();
        assert!(message.contains(ATTACH_PATH), "{message}");
        assert!(message.contains("update the endpoint"), "{message}");
    }

    #[test]
    fn only_authoritative_denials_are_safe_to_report_as_unattached() {
        let denied = Error::Denied {
            status: 409,
            code: ProjectErrorCode::ProjectAlreadyAttached,
            retryable: false,
        };
        assert!(!denied.is_attachment_ambiguous());

        for ambiguous in [
            Error::ResponseTooLarge,
            Error::InvalidResponse {
                reason: "invalid success body",
            },
        ] {
            assert!(ambiguous.is_attachment_ambiguous());
        }
    }
}
