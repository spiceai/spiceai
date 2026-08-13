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

//! Narrow client for the atomic Cloud Connect project operation.

use futures::StreamExt as _;
use reqwest::{StatusCode, redirect::Policy};
use runtime_cloud_connect::Identity;
use runtime_cloud_connect::enroll::SessionToken;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

const PROJECT_PATH: &str = "/v1/cloud-connect/project";
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Snafu)]
pub(super) enum Error {
    #[snafu(display(
        "Failed to create and attach the Spice Cloud project: the request could not be sent"
    ))]
    Transport { source: reqwest::Error },

    #[snafu(display(
        "Failed to create and attach the Spice Cloud project: the response could not be read"
    ))]
    ResponseBody { source: reqwest::Error },

    #[snafu(display(
        "Failed to create and attach the Spice Cloud project: the response exceeded the 64 KiB limit"
    ))]
    ResponseTooLarge,

    #[snafu(display(
        "Failed to create and attach the Spice Cloud project: the server returned an invalid response ({reason})"
    ))]
    InvalidResponse { reason: &'static str },

    #[snafu(display(
        "Spice Cloud did not create the project ({status}, code={code}, retryable={retryable})"
    ))]
    Denied {
        status: u16,
        code: ProjectErrorCode,
        retryable: bool,
    },
}

impl Error {
    #[must_use]
    pub(super) fn is_name_conflict(&self) -> bool {
        matches!(
            self,
            Self::Denied {
                status: 409,
                code: ProjectErrorCode::ProjectNameConflict,
                retryable: false,
            }
        )
    }

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

    #[must_use]
    pub(super) fn is_retryable(&self) -> bool {
        matches!(self, Self::Transport { .. } | Self::ResponseBody { .. })
            || matches!(
                self,
                Self::Denied {
                    retryable: true,
                    ..
                }
            )
    }

    /// A response that may have followed a committed mutation but did not
    /// prove the authoritative result. Exact replay is the only safe recovery.
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

    /// A non-retryable structured denial proves that the requested mutation
    /// did not commit, so reporting the still-unattached identity is accurate.
    #[must_use]
    pub(super) fn is_authoritative_non_mutation(&self) -> bool {
        matches!(
            self,
            Self::Denied {
                retryable: false,
                ..
            }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProjectErrorCode {
    InvalidProjectName,
    Unauthenticated,
    Forbidden,
    InstanceNotFound,
    ProjectNameConflict,
    InstanceNotEnrolled,
    InstanceAlreadyAttached,
    RateLimited,
    Internal,
    Unknown,
}

impl ProjectErrorCode {
    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("invalid_project_name") => Self::InvalidProjectName,
            Some("unauthenticated") => Self::Unauthenticated,
            Some("forbidden") => Self::Forbidden,
            Some("instance_not_found") => Self::InstanceNotFound,
            Some("project_name_conflict") => Self::ProjectNameConflict,
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
            Self::InvalidProjectName => "invalid_project_name",
            Self::Unauthenticated => "unauthenticated",
            Self::Forbidden => "forbidden",
            Self::InstanceNotFound => "instance_not_found",
            Self::ProjectNameConflict => "project_name_conflict",
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

pub(super) struct ProjectClient {
    http: reqwest::Client,
    url: String,
}

impl std::fmt::Debug for ProjectClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectClient").finish_non_exhaustive()
    }
}

impl ProjectClient {
    pub(super) fn new(endpoint: &str) -> std::result::Result<Self, Error> {
        let parsed = reqwest::Url::parse(endpoint).map_err(|_| Error::InvalidResponse {
            reason: "the Cloud endpoint was not an absolute URL",
        })?;
        let local_http =
            parsed.scheme() == "http" && parsed.host_str().is_some_and(is_loopback_host);
        let http = reqwest::Client::builder()
            // Shorter than the transaction's replay window so a single lost
            // response cannot consume the entire exact-replay opportunity.
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(std::time::Duration::from_secs(10))
            // A redirect must never carry the user bearer to another origin.
            .redirect(Policy::none())
            // Plain HTTP exists only for an explicit loopback fixture.
            .https_only(!local_http)
            .build()
            .map_err(|source| Error::Transport { source })?;
        Ok(Self {
            http,
            url: format!("{}{PROJECT_PATH}", endpoint.trim_end_matches('/')),
        })
    }

    pub(super) async fn create(
        &self,
        token: &SessionToken,
        organization: &str,
        mutation: &ProjectMutation,
    ) -> std::result::Result<ProjectAttachment, Error> {
        let response = self
            .http
            .post(&self.url)
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

        if !matches!(status, StatusCode::OK | StatusCode::CREATED) {
            return Err(parse_denial(status, &body)?);
        }

        let result = serde_json::from_slice::<ProjectResponse>(&body).map_err(|_| {
            Error::InvalidResponse {
                reason: "response was not the documented JSON object",
            }
        })?;
        let monitor_url =
            validate_response(&result, organization, &mutation.instance_id, &mutation.name)?;

        Ok(ProjectAttachment {
            instance_id: result.instance_id,
            organization: result.organization.name,
            project_id: result.project.id,
            project_name: result.project.name,
            monitor_url,
        })
    }
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
            ProjectErrorCode::InvalidProjectName
        ) | (StatusCode::UNAUTHORIZED, ProjectErrorCode::Unauthenticated)
            | (StatusCode::FORBIDDEN, ProjectErrorCode::Forbidden)
            | (
                StatusCode::NOT_FOUND,
                ProjectErrorCode::InstanceNotFound | ProjectErrorCode::InstanceNotEnrolled
            )
            | (
                StatusCode::CONFLICT,
                ProjectErrorCode::ProjectNameConflict | ProjectErrorCode::InstanceAlreadyAttached
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

fn is_loopback_host(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
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
    expected_name: &str,
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
    if response.project.name != expected_name {
        return Err(Error::InvalidResponse {
            reason: "project name did not match the request",
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
    let monitor_url =
        reqwest::Url::parse(&response.monitor_url).map_err(|_| Error::InvalidResponse {
            reason: "monitor_url was not an absolute URL",
        })?;
    let local_http =
        monitor_url.scheme() == "http" && monitor_url.host_str().is_some_and(is_loopback_host);
    if (monitor_url.scheme() != "https" && !local_http)
        || monitor_url.host_str().is_none()
        || !monitor_url.username().is_empty()
        || monitor_url.password().is_some()
    {
        return Err(Error::InvalidResponse {
            reason: "monitor_url was not a safe HTTP URL",
        });
    }
    Ok(monitor_url.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ProjectMutation {
    pub(super) instance_id: String,
    pub(super) name: String,
    pub(super) cert_pem: String,
    pub(super) pop_sig: String,
}

impl ProjectMutation {
    pub(super) fn signed(
        identity: &Identity,
        organization: &str,
        name: &str,
    ) -> std::result::Result<Self, Error> {
        let proof_payload = format!(
            "spice-cloud-connect/project/v1\n{organization}\n{}\n{name}",
            identity.identifier
        );
        let pop_sig = runtime_cloud_connect::sign_identity_proof(
            &identity.private_key_pem,
            proof_payload.as_bytes(),
        )
        .map_err(|_| Error::InvalidResponse {
            reason: "the enrolled identity could not sign the project request",
        })?;
        Ok(Self {
            instance_id: identity.identifier.clone(),
            name: name.to_string(),
            cert_pem: identity.identity_cert_pem.clone(),
            pop_sig,
        })
    }
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
    use wiremock::matchers::{header, method, path};
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
            control_plane_endpoint: None,
        }
    }

    fn test_mutation(identity: &Identity) -> ProjectMutation {
        ProjectMutation::signed(identity, "acme", "retail-analytics")
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
    async fn first_create_sends_the_exact_atomic_project_contract() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(PROJECT_PATH))
            .and(header("Authorization", format!("Bearer {TOKEN}")))
            .and(header("X-Org-Name", "acme"))
            .respond_with(ResponseTemplate::new(201).set_body_json(success_body()))
            .expect(1)
            .mount(&server)
            .await;

        let identity = test_identity();
        let mutation = test_mutation(&identity);
        let result = ProjectClient::new(&server.uri())
            .expect("client")
            .create(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect("project created");
        assert_eq!(result.project_id, 314);
        assert_eq!(result.project_name, "retail-analytics");
        let requests = server
            .received_requests()
            .await
            .expect("read received project request");
        let request: serde_json::Value =
            serde_json::from_slice(&requests[0].body).expect("parse project request body");
        assert_eq!(request["instance_id"], "inst_8fa21c");
        assert_eq!(request["name"], "retail-analytics");
        assert_eq!(request["cert_pem"], identity.identity_cert_pem);
        assert!(
            request["pop_sig"]
                .as_str()
                .is_some_and(|value| !value.is_empty()),
            "project request must carry a proof-of-possession signature"
        );
    }

    #[tokio::test]
    async fn exact_replay_accepts_the_same_project_with_status_200() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(PROJECT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_body()))
            .expect(2)
            .mount(&server)
            .await;
        let client = ProjectClient::new(&server.uri()).expect("client");
        let token = SessionToken::new(TOKEN.to_string());
        let identity = test_identity();
        let mutation = test_mutation(&identity);

        for _ in 0..2 {
            client
                .create(&token, "acme", &mutation)
                .await
                .expect("exact replay succeeds");
        }
    }

    #[tokio::test]
    async fn project_conflicts_are_typed_and_server_detail_is_redacted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "code": "project_name_conflict",
                "error": format!("{TOKEN} acme retail-analytics https://private.example"),
                "retryable": false
            })))
            .mount(&server)
            .await;
        let identity = test_identity();
        let mutation = test_mutation(&identity);
        let err = ProjectClient::new(&server.uri())
            .expect("client")
            .create(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect_err("conflict");

        assert!(err.is_name_conflict());
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
        let err = ProjectClient::new(&server.uri())
            .expect("client")
            .create(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
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
        let err = ProjectClient::new(&server.uri())
            .expect("client")
            .create(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
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
            .and(path(PROJECT_PATH))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/stolen", target.uri())),
            )
            .expect(1)
            .mount(&source)
            .await;

        let identity = test_identity();
        let mutation = test_mutation(&identity);
        let err = ProjectClient::new(&source.uri())
            .expect("client")
            .create(&SessionToken::new(TOKEN.to_string()), "acme", &mutation)
            .await
            .expect_err("redirect must fail closed");
        assert!(matches!(err, Error::InvalidResponse { .. }), "{err}");
        target.verify().await;
    }

    #[test]
    fn only_authoritative_denials_are_safe_to_report_as_unattached() {
        let denied = Error::Denied {
            status: 409,
            code: ProjectErrorCode::ProjectNameConflict,
            retryable: false,
        };
        assert!(denied.is_authoritative_non_mutation());
        assert!(!denied.is_attachment_ambiguous());

        for ambiguous in [
            Error::ResponseTooLarge,
            Error::InvalidResponse {
                reason: "invalid success body",
            },
        ] {
            assert!(ambiguous.is_attachment_ambiguous());
            assert!(!ambiguous.is_authoritative_non_mutation());
        }
    }
}
