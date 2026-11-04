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

//! Workload identity for GitHub datasets: the GitHub token comes from an Octo STS exchange of the
//! identity token the platform gives the workload (a GitHub Actions job, a Kubernetes pod), in
//! place of `github_token` or a GitHub App's private key.
//!
//! GitHub accepts no external identity token itself. Octo STS is a GitHub App that does: it
//! checks the identity token against a trust policy kept in the target repository and returns an
//! installation token with the permissions the policy grants.

use token_provider::workload_identity::{Exchange, OCTO_STS_ENDPOINT, SubjectTokenSource};

const DOCS: &str = "https://spiceai.org/docs/components/data-connectors/github#workload-identity";

/// The credential params a dataset can set instead of workload identity: the spec name, and the
/// name the user writes.
pub(crate) const CREDENTIAL_PARAMS: [(&str, &str); 4] = [
    ("token", "github_token"),
    ("client_id", "github_client_id"),
    ("private_key", "github_private_key"),
    ("installation_id", "github_installation_id"),
];

/// A dataset's workload identity params, as the Spicepod sets them, and what decides their
/// defaults.
#[derive(Debug, Default)]
pub(crate) struct WorkloadIdentityParams<'a> {
    /// `github_sts_identity`.
    pub identity: Option<&'a str>,
    /// `github_sts_scope`.
    pub scope: Option<&'a str>,
    /// `github_sts_endpoint`.
    pub endpoint: Option<&'a str>,
    /// `github_identity_token_file`.
    pub token_file: Option<&'a str>,
    /// `github_identity_token_audience`.
    pub audience: Option<&'a str>,
    /// The other credential params the Spicepod sets, as the user names them. A credential a
    /// secret store supplies unasked does not count: workload identity takes precedence over it.
    pub credentials_set: Vec<&'static str>,
    /// The owner the dataset's path names.
    pub owner: Option<&'a str>,
    /// The repository the dataset's path names, for a repository-level table.
    pub repo: Option<&'a str>,
}

/// The variables GitHub sets in a job with `permissions: id-token: write`.
#[derive(Debug, Default)]
pub(crate) struct GitHubActionsEnvironment {
    /// `ACTIONS_ID_TOKEN_REQUEST_URL`.
    pub request_url: Option<String>,
    /// `ACTIONS_ID_TOKEN_REQUEST_TOKEN`.
    pub request_token: Option<String>,
}

impl GitHubActionsEnvironment {
    pub(crate) fn from_process() -> Self {
        Self {
            request_url: std::env::var("ACTIONS_ID_TOKEN_REQUEST_URL").ok(),
            request_token: std::env::var("ACTIONS_ID_TOKEN_REQUEST_TOKEN").ok(),
        }
    }
}

/// A dataset's workload identity, ready to exchange.
#[derive(Debug)]
pub(crate) struct WorkloadIdentity {
    pub source: SubjectTokenSource,
    pub exchange: Exchange,
    endpoint: String,
    scope: String,
    identity: String,
    /// Where the identity token comes from, as the load message names it.
    token_origin: String,
}

impl WorkloadIdentity {
    /// What the dataset's GitHub requests to `github_endpoint` are metered against. Octo STS mints
    /// every token for an owner from that owner's installation of its GitHub App, and GitHub meters
    /// an installation's tokens together, so datasets of one owner share a quota whatever their
    /// scope or trust policy. The broker is named by the URL the exchange is sent to, which is the
    /// same for every spelling of its endpoint, and the GitHub instance by its host.
    pub(crate) fn rate_limit_key(&self, github_endpoint: &str) -> String {
        let owner = self
            .scope
            .split_once('/')
            .map_or(self.scope.as_str(), |(owner, _)| owner);
        let github = url::Url::parse(github_endpoint.trim())
            .ok()
            .and_then(|url| {
                let port = url.port().map(|port| format!(":{port}")).unwrap_or_default();
                url.host_str().map(|host| format!("{host}{port}"))
            })
            .unwrap_or_else(|| github_endpoint.trim().to_ascii_lowercase());
        format!(
            "octo-sts:{}:{github}:{}",
            self.exchange.token_url(),
            owner.to_ascii_lowercase()
        )
    }

    /// The `info` line logged once `component` has its first token.
    pub(crate) fn load_message(&self, component: &str) -> String {
        format!(
            "Authenticated {component} with the Octo STS trust policy '{}' of '{}' at {}, using the identity token from {}.",
            self.identity, self.scope, self.endpoint, self.token_origin
        )
    }
}

/// A value the user set, or `None` for an absent or blank one.
fn set(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn invalid(param: &str, message: &str) -> String {
    format!("Invalid value for parameter {param}. {message} See: {DOCS}")
}

/// Whether `identity` names one trust policy file, as Octo STS requires: a single path segment.
fn is_valid_identity(identity: &str) -> bool {
    !identity.is_empty()
        && identity != "."
        && identity != ".."
        && !identity.contains('/')
        && !identity.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// Whether `name` can be a GitHub owner or repository name.
fn is_github_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// Whether `scope` is a repository (`owner/repo`) or an owner.
fn is_valid_scope(scope: &str) -> bool {
    match scope.split_once('/') {
        Some((owner, repo)) => is_github_name(owner) && is_github_name(repo),
        None => is_github_name(scope),
    }
}

/// The dataset's workload identity: `None` when `github_sts_identity` is not set, or the message
/// of the configuration error to report. Nothing here reaches a network.
pub(crate) fn resolve(
    params: &WorkloadIdentityParams<'_>,
    github_actions: GitHubActionsEnvironment,
) -> Result<Option<WorkloadIdentity>, String> {
    let scope = set(params.scope);
    let endpoint = set(params.endpoint);
    let token_file = set(params.token_file);
    let audience = set(params.audience);

    let Some(identity) = set(params.identity) else {
        let stray = [
            ("github_sts_scope", scope),
            ("github_sts_endpoint", endpoint),
            ("github_identity_token_file", token_file),
            ("github_identity_token_audience", audience),
        ]
        .into_iter()
        .find_map(|(param, value)| value.map(|_| param));
        return match stray {
            Some(param) => Err(invalid(
                param,
                &format!(
                    "`{param}` applies only to workload identity, which `github_sts_identity` turns on, and `github_sts_identity` is not set. Set `github_sts_identity`, or remove `{param}`."
                ),
            )),
            None => Ok(None),
        };
    };

    if let Some(credential) = params.credentials_set.first() {
        return Err(invalid(
            credential,
            &format!(
                "A dataset authenticates with `github_token`, a GitHub App (`github_client_id`, `github_private_key` and `github_installation_id`) or workload identity (`github_sts_identity`), and both `{credential}` and `github_sts_identity` are set. Remove one of them."
            ),
        ));
    }

    if !is_valid_identity(identity) {
        return Err(invalid(
            "github_sts_identity",
            &format!(
                "`github_sts_identity` names one trust policy file, `.github/chainguard/<name>.sts.yaml`, so it cannot contain '/' or whitespace, or be '.' or '..'. Found '{identity}'."
            ),
        ));
    }

    let scope = match (scope, set(params.owner), set(params.repo)) {
        (Some(scope), _, _) => {
            if !is_valid_scope(scope) {
                return Err(invalid(
                    "github_sts_scope",
                    &format!(
                        "`github_sts_scope` is the repository (`owner/repo`) or the owner whose trust policy `github_sts_identity` names. Found '{scope}'."
                    ),
                ));
            }
            scope.to_string()
        }
        (None, Some(owner), Some(repo)) => format!("{owner}/{repo}"),
        (None, Some(owner), None) => owner.to_string(),
        (None, None, _) => {
            return Err(invalid(
                "github_sts_scope",
                "The dataset's path names no GitHub owner, so the scope of its trust policy cannot be derived from it. Set `github_sts_scope` to the repository (`owner/repo`) or the owner whose trust policy `github_sts_identity` names.",
            ));
        }
    };

    let endpoint = endpoint.unwrap_or(OCTO_STS_ENDPOINT);
    let exchange = Exchange::octo_sts(endpoint, scope.as_str(), identity)
        .map_err(|error| invalid("github_sts_endpoint", &error.to_string()))?;

    let (source, token_origin) = match token_file {
        Some(_) if audience.is_some() => {
            return Err(invalid(
                "github_identity_token_audience",
                "`github_identity_token_audience` applies only to an identity token Spice requests from GitHub Actions, but the identity token comes from `github_identity_token_file`, whose audience is set where it is issued. Remove one of them.",
            ));
        }
        Some(path) => (
            SubjectTokenSource::File(path.into()),
            format!("the file '{path}'"),
        ),
        None => {
            let audience = audience
                .or(exchange.default_audience())
                .unwrap_or(OCTO_STS_ENDPOINT)
                .to_string();
            let source = SubjectTokenSource::github_actions(
                github_actions.request_url,
                github_actions.request_token,
                audience,
            )
            .map_err(|_| {
                format!(
                    "Workload identity (`github_sts_identity`) needs an identity token, but `github_identity_token_file` is not set and this is not a GitHub Actions job with `permissions: id-token: write`. Set `github_identity_token_file` to a file that holds one, such as a Kubernetes projected service account token, or run Spice in such a job. See: {DOCS}"
                )
            })?;
            (source, "this GitHub Actions job".to_string())
        }
    };

    Ok(Some(WorkloadIdentity {
        source,
        exchange,
        endpoint: endpoint.to_string(),
        scope,
        identity: identity.to_string(),
        token_origin,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn github_actions() -> GitHubActionsEnvironment {
        GitHubActionsEnvironment {
            request_url: Some("https://pipelines.actions.githubusercontent.com/id?api-version=2.0".to_string()),
            request_token: Some("request-token".to_string()),
        }
    }

    fn repo_dataset<'a>() -> WorkloadIdentityParams<'a> {
        WorkloadIdentityParams {
            identity: Some("spice-reader"),
            owner: Some("acme"),
            repo: Some("widgets"),
            ..WorkloadIdentityParams::default()
        }
    }

    /// The resolved identity as the parts a test checks: the exchange's URL, the source, the
    /// rate-limit key and the load message.
    fn resolved(
        params: &WorkloadIdentityParams<'_>,
        env: GitHubActionsEnvironment,
    ) -> Result<Option<(String, String, String, String)>, String> {
        resolve(params, env).map(|identity| {
            identity.map(|identity| {
                let Exchange::OctoSts {
                    token_url,
                    scope,
                    identity: policy,
                    ..
                } = &identity.exchange
                else {
                    panic!("an Octo STS exchange, not {:?}", identity.exchange);
                };
                (
                    format!("{token_url}?scope={scope}&identity={policy}"),
                    format!("{:?}", identity.source),
                    identity.rate_limit_key("https://api.github.com"),
                    identity.load_message("dataset 'stars' (github)"),
                )
            })
        })
    }

    #[test]
    fn a_dataset_without_github_sts_identity_has_no_workload_identity() {
        assert!(matches!(
            resolve(&WorkloadIdentityParams::default(), github_actions()),
            Ok(None)
        ));
        let blank = WorkloadIdentityParams {
            identity: Some("  "),
            ..WorkloadIdentityParams::default()
        };
        assert!(matches!(resolve(&blank, github_actions()), Ok(None)));
    }

    #[test]
    fn the_scope_defaults_to_the_datasets_repository_or_owner_and_the_audience_to_the_domain() {
        assert_eq!(
            resolved(&repo_dataset(), github_actions()),
            Ok(Some((
                "https://octo-sts.dev/sts/exchange?scope=acme/widgets&identity=spice-reader".to_string(),
                r#"GitHubActions { audience: "octo-sts.dev", .. }"#.to_string(),
                "octo-sts:https://octo-sts.dev/sts/exchange:api.github.com:acme".to_string(),
                "Authenticated dataset 'stars' (github) with the Octo STS trust policy 'spice-reader' of 'acme/widgets' at https://octo-sts.dev, using the identity token from this GitHub Actions job.".to_string(),
            )))
        );

        let owner_level = WorkloadIdentityParams {
            repo: None,
            ..repo_dataset()
        };
        assert_eq!(
            resolved(&owner_level, github_actions()).map(|r| r.map(|r| r.0)),
            Ok(Some(
                "https://octo-sts.dev/sts/exchange?scope=acme&identity=spice-reader".to_string()
            ))
        );
    }

    #[test]
    fn set_params_replace_the_defaults() {
        let params = WorkloadIdentityParams {
            scope: Some("Acme/platform"),
            endpoint: Some("https://sts.acme.example/"),
            audience: Some("acme-sts"),
            ..repo_dataset()
        };
        assert_eq!(
            resolved(&params, github_actions()),
            Ok(Some((
                "https://sts.acme.example/sts/exchange?scope=Acme/platform&identity=spice-reader".to_string(),
                r#"GitHubActions { audience: "acme-sts", .. }"#.to_string(),
                "octo-sts:https://sts.acme.example/sts/exchange:api.github.com:acme".to_string(),
                "Authenticated dataset 'stars' (github) with the Octo STS trust policy 'spice-reader' of 'Acme/platform' at https://sts.acme.example/, using the identity token from this GitHub Actions job.".to_string(),
            )))
        );

        let file = WorkloadIdentityParams {
            token_file: Some("/var/run/secrets/tokens/octo-sts"),
            ..repo_dataset()
        };
        assert_eq!(
            resolved(&file, GitHubActionsEnvironment::default()).map(|r| r.map(|r| (r.1, r.3))),
            Ok(Some((
                r#"File("/var/run/secrets/tokens/octo-sts")"#.to_string(),
                "Authenticated dataset 'stars' (github) with the Octo STS trust policy 'spice-reader' of 'acme/widgets' at https://octo-sts.dev, using the identity token from the file '/var/run/secrets/tokens/octo-sts'.".to_string(),
            )))
        );
    }

    #[test]
    fn datasets_of_one_owner_share_a_rate_limit_key_whatever_the_spelling_of_the_broker() {
        let key = |endpoint: Option<&str>, scope: Option<&str>, github_endpoint: &str| {
            let params = WorkloadIdentityParams {
                endpoint,
                scope,
                ..repo_dataset()
            };
            resolve(&params, github_actions())
                .expect("a workload identity")
                .expect("github_sts_identity is set")
                .rate_limit_key(github_endpoint)
        };
        let shared = "octo-sts:https://octo-sts.dev/sts/exchange:api.github.com:acme";
        assert_eq!(
            [
                key(None, None, "https://api.github.com"),
                key(Some("octo-sts.dev"), None, "https://api.github.com/"),
                key(Some("https://octo-sts.dev/"), Some("ACME"), "https://API.github.com"),
                key(Some("HTTPS://Octo-STS.dev"), Some("acme/gadgets"), "https://api.github.com"),
            ],
            [shared, shared, shared, shared].map(str::to_string)
        );
        assert_eq!(
            [
                key(None, Some("other"), "https://api.github.com"),
                key(Some("https://sts.acme.example"), None, "https://api.github.com"),
                key(None, None, "https://github.acme.example/api/v3"),
            ],
            [
                "octo-sts:https://octo-sts.dev/sts/exchange:api.github.com:other",
                "octo-sts:https://sts.acme.example/sts/exchange:api.github.com:acme",
                "octo-sts:https://octo-sts.dev/sts/exchange:github.acme.example:acme",
            ]
            .map(str::to_string)
        );
    }

    #[test]
    fn each_configuration_mistake_names_the_param_to_fix() {
        let error = |params: WorkloadIdentityParams<'_>, env: GitHubActionsEnvironment| {
            resolve(&params, env).expect_err("a configuration error")
        };
        assert_eq!(
            [
                error(
                    WorkloadIdentityParams {
                        identity: None,
                        endpoint: Some("https://octo-sts.dev"),
                        ..repo_dataset()
                    },
                    github_actions()
                ),
                error(
                    WorkloadIdentityParams {
                        credentials_set: vec!["github_token"],
                        ..repo_dataset()
                    },
                    github_actions()
                ),
                error(
                    WorkloadIdentityParams {
                        identity: Some("../policy"),
                        ..repo_dataset()
                    },
                    github_actions()
                ),
                error(
                    WorkloadIdentityParams {
                        scope: Some("acme/widgets/extra"),
                        ..repo_dataset()
                    },
                    github_actions()
                ),
                error(
                    WorkloadIdentityParams {
                        owner: None,
                        repo: None,
                        ..repo_dataset()
                    },
                    github_actions()
                ),
                error(
                    WorkloadIdentityParams {
                        endpoint: Some("http://octo-sts.dev"),
                        ..repo_dataset()
                    },
                    github_actions()
                ),
                error(
                    WorkloadIdentityParams {
                        token_file: Some("/var/run/secrets/tokens/octo-sts"),
                        audience: Some("octo-sts.dev"),
                        ..repo_dataset()
                    },
                    github_actions()
                ),
                error(repo_dataset(), GitHubActionsEnvironment::default()),
            ],
            [
                "Invalid value for parameter github_sts_endpoint. `github_sts_endpoint` applies only to workload identity, which `github_sts_identity` turns on, and `github_sts_identity` is not set. Set `github_sts_identity`, or remove `github_sts_endpoint`. See: https://spiceai.org/docs/components/data-connectors/github#workload-identity",
                "Invalid value for parameter github_token. A dataset authenticates with `github_token`, a GitHub App (`github_client_id`, `github_private_key` and `github_installation_id`) or workload identity (`github_sts_identity`), and both `github_token` and `github_sts_identity` are set. Remove one of them. See: https://spiceai.org/docs/components/data-connectors/github#workload-identity",
                "Invalid value for parameter github_sts_identity. `github_sts_identity` names one trust policy file, `.github/chainguard/<name>.sts.yaml`, so it cannot contain '/' or whitespace, or be '.' or '..'. Found '../policy'. See: https://spiceai.org/docs/components/data-connectors/github#workload-identity",
                "Invalid value for parameter github_sts_scope. `github_sts_scope` is the repository (`owner/repo`) or the owner whose trust policy `github_sts_identity` names. Found 'acme/widgets/extra'. See: https://spiceai.org/docs/components/data-connectors/github#workload-identity",
                "Invalid value for parameter github_sts_scope. The dataset's path names no GitHub owner, so the scope of its trust policy cannot be derived from it. Set `github_sts_scope` to the repository (`owner/repo`) or the owner whose trust policy `github_sts_identity` names. See: https://spiceai.org/docs/components/data-connectors/github#workload-identity",
                "Invalid value for parameter github_sts_endpoint. The Octo STS endpoint must be an https:// URL, such as https://octo-sts.dev, because the workload identity token is sent to it and the GitHub token comes back from it. http:// is accepted only for a broker on this host: localhost, 127.0.0.1 or [::1]. See: https://spiceai.org/docs/components/data-connectors/github#workload-identity",
                "Invalid value for parameter github_identity_token_audience. `github_identity_token_audience` applies only to an identity token Spice requests from GitHub Actions, but the identity token comes from `github_identity_token_file`, whose audience is set where it is issued. Remove one of them. See: https://spiceai.org/docs/components/data-connectors/github#workload-identity",
                "Workload identity (`github_sts_identity`) needs an identity token, but `github_identity_token_file` is not set and this is not a GitHub Actions job with `permissions: id-token: write`. Set `github_identity_token_file` to a file that holds one, such as a Kubernetes projected service account token, or run Spice in such a job. See: https://spiceai.org/docs/components/data-connectors/github#workload-identity",
            ]
            .map(str::to_string)
        );
    }

    #[test]
    fn a_trust_policy_name_is_one_path_segment_and_a_scope_one_or_two_github_names() {
        assert_eq!(
            ["spice-reader", "a.b_c", "", ".", "..", "a/b", "a b", "a\tb"].map(is_valid_identity),
            [true, true, false, false, false, false, false, false]
        );
        assert_eq!(
            [
                "acme",
                "acme/widgets",
                "Acme-1/w.i_d",
                "",
                "acme/",
                "/widgets",
                "acme/widgets/x",
                "acme/..",
                "ac me",
                "acme?x",
            ]
            .map(is_valid_scope),
            [true, true, true, false, false, false, false, false, false, false]
        );
    }
}
