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

//! The workload's Microsoft Entra identity, found the way the Azure SDKs' default credential
//! finds it and with no configuration of its own, so every Azure component of Spice finds the
//! same one:
//!
//! 1. A workload identity: `AZURE_TENANT_ID` and `AZURE_CLIENT_ID`, with the identity token from
//!    `AZURE_FEDERATED_TOKEN_FILE` (the AKS workload identity webhook sets all three) or, in a
//!    GitHub Actions job, the job's OIDC token.
//! 2. A managed identity (an Azure VM, App Service, Container Apps or Functions), user-assigned
//!    when `AZURE_CLIENT_ID` names one.
//! 3. The Azure CLI's signed-in account, for local development.
//!
//! [`resolve`] finds the identity and its first access token for a scope; the identity then
//! gives access tokens directly ([`AzureIdentity::access_token`]), as a refreshing
//! [`TokenProvider`] ([`AzureIdentity::into_token_provider`]), or as the identity token another
//! provider's federation exchanges ([`AzureIdentityToken`]).

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use azure_core::credentials::{AccessToken, TokenCredential};
use azure_identity::{
    AzureCliCredential, ManagedIdentityCredential, ManagedIdentityCredentialOptions, UserAssignedId,
};
use chrono::{DateTime, Utc};
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use token_provider::TokenProvider;
use token_provider::workload_identity::{
    ENTRA_AUDIENCE, Exchange, IdentityTokenSource, SubjectTokenSource, WorkloadIdentityError,
    WorkloadIdentityTokenProvider, refresh_failure_message,
};
use tokio::sync::watch;
use util::fibonacci_backoff::{Backoff as _, FibonacciBackoffBuilder};

/// How long the managed identity endpoint has to answer. Outside Azure there is none, and the
/// wait is the cost of looking for one.
pub const MANAGED_IDENTITY_BUDGET: Duration = Duration::from_secs(3);

/// How long the Azure CLI has to answer: it is a process that may refresh its own token.
pub const AZURE_CLI_BUDGET: Duration = Duration::from_secs(20);

/// The Microsoft Entra ID authority of the Azure public cloud.
pub const PUBLIC_AUTHORITY: &str = "https://login.microsoftonline.com";

/// Bounds the reason quoted from a credential's error.
const MAX_REASON_LEN: usize = 300;

/// The longest wait between retries of a failed refresh.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(15);

/// The shortest wait before getting a token again.
const MIN_REFRESH_WAIT: Duration = Duration::from_secs(1);

/// The workload identity the platform gives the process, in the environment variables the Azure
/// SDKs read, and the GitHub Actions job it may run in.
#[derive(Clone, Debug, Default)]
pub struct AzureIdentityEnvironment {
    /// `AZURE_TENANT_ID`.
    pub tenant_id: Option<String>,
    /// `AZURE_CLIENT_ID`.
    pub client_id: Option<String>,
    /// `AZURE_FEDERATED_TOKEN_FILE`.
    pub federated_token_file: Option<String>,
    /// `AZURE_AUTHORITY_HOST`, which replaces the authority of the target's cloud.
    pub authority_host: Option<String>,
    /// `ACTIONS_ID_TOKEN_REQUEST_URL`.
    pub github_actions_request_url: Option<String>,
    /// `ACTIONS_ID_TOKEN_REQUEST_TOKEN`.
    pub github_actions_request_token: Option<String>,
}

impl AzureIdentityEnvironment {
    /// The variables of this process. An empty variable counts as unset.
    #[must_use]
    pub fn from_process() -> Self {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        Self {
            tenant_id: var("AZURE_TENANT_ID"),
            client_id: var("AZURE_CLIENT_ID"),
            federated_token_file: var("AZURE_FEDERATED_TOKEN_FILE"),
            authority_host: var("AZURE_AUTHORITY_HOST"),
            github_actions_request_url: var("ACTIONS_ID_TOKEN_REQUEST_URL"),
            github_actions_request_token: var("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
        }
    }
}

/// What an access token is for: the Microsoft Entra authority of the cloud it is issued in, and
/// its scope, such as `https://vault.azure.net/.default`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntraTarget {
    pub authority: String,
    pub scope: String,
}

/// Why no identity was found. Each caller words the message around the resource it was for.
#[derive(Debug, Snafu)]
pub enum ResolveError {
    /// `AZURE_FEDERATED_TOKEN_FILE` is set without the tenant or client it belongs to; `missing`
    /// names it, such as ``"`AZURE_TENANT_ID` is"``. Retrying cannot fix it.
    #[snafu(display("{missing} not set"))]
    IncompleteWorkloadIdentity { missing: &'static str },

    /// The tenant or the authority is invalid. Retrying cannot fix it.
    #[snafu(display("{source}"))]
    InvalidIdentity { source: WorkloadIdentityError },

    /// Microsoft Entra ID refused the workload identity, or could not be reached.
    #[snafu(display("{source}"))]
    AccessToken { source: WorkloadIdentityError },

    /// No identity answered; `tried` lists each one tried and what it said.
    #[snafu(display("{tried}"))]
    NoIdentity { tried: String },
}

impl ResolveError {
    /// Whether the error is a mistake in the configuration, which retrying cannot fix, as
    /// opposed to an identity that is not there yet, a refusal, or an unreachable service.
    #[must_use]
    pub fn is_configuration(&self) -> bool {
        matches!(
            self,
            Self::IncompleteWorkloadIdentity { .. } | Self::InvalidIdentity { .. }
        )
    }
}

/// What a message lists for the workload identity it looked for and did not find.
pub const NO_WORKLOAD_IDENTITY: &str = "a workload identity (`AZURE_FEDERATED_TOKEN_FILE` is not set, and this is not a GitHub Actions job with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `permissions: id-token: write`)";

/// What a message says to do when no identity answered.
pub const HOW_TO_GIVE_AN_IDENTITY: &str = "On AKS, label the pod `azure.workload.identity/use: \"true\"`; on an Azure VM, App Service or Container Apps, assign the workload a managed identity; locally, run `az login`.";

/// The workload identity in `env` for `target`, if the environment configures one: the identity
/// token's source, the Microsoft Entra exchange, and where the token comes from, as a message
/// names it. `None` when it configures none, and another identity is looked for.
///
/// # Errors
///
/// When `AZURE_FEDERATED_TOKEN_FILE` is set without `AZURE_TENANT_ID` or `AZURE_CLIENT_ID`, or
/// the tenant or authority is invalid.
pub fn workload_identity(
    target: &EntraTarget,
    env: &AzureIdentityEnvironment,
) -> Result<Option<(SubjectTokenSource, Exchange, &'static str)>, ResolveError> {
    let (source, origin) = match &env.federated_token_file {
        Some(path) => (
            SubjectTokenSource::File(path.into()),
            "`AZURE_FEDERATED_TOKEN_FILE`",
        ),
        None if env.tenant_id.is_some() && env.client_id.is_some() => {
            match SubjectTokenSource::github_actions(
                env.github_actions_request_url.clone(),
                env.github_actions_request_token.clone(),
                ENTRA_AUDIENCE,
            ) {
                Ok(source) => (source, "this GitHub Actions job"),
                Err(_) => return Ok(None),
            }
        }
        None => return Ok(None),
    };
    let (tenant_id, client_id) = match (&env.tenant_id, &env.client_id) {
        (Some(tenant_id), Some(client_id)) => (tenant_id, client_id),
        (tenant_id, _) => {
            let missing = if tenant_id.is_none() {
                "`AZURE_TENANT_ID` is"
            } else {
                "`AZURE_CLIENT_ID` is"
            };
            return IncompleteWorkloadIdentitySnafu { missing }.fail();
        }
    };
    let exchange = Exchange::entra(
        Some(env.authority_host.as_deref().unwrap_or(&target.authority)),
        tenant_id,
        client_id.clone(),
        target.scope.clone(),
    )
    .context(InvalidIdentitySnafu)?;
    Ok(Some((source, exchange, origin)))
}

/// `text` as a clause of a longer message: on one line, without a trailing period, and bounded.
#[must_use]
pub fn clause(text: &str) -> String {
    let text = util::single_line(text);
    let text = text.trim().trim_end_matches('.');
    match text.char_indices().nth(MAX_REASON_LEN) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

/// An Azure SDK credential to try, as a message names it, and how long it has to answer.
pub type Candidate = (
    &'static str,
    azure_core::Result<Arc<dyn TokenCredential>>,
    Duration,
);

/// The managed identity, then the Azure CLI, as [`first_answering`] tries them: the managed
/// identity is user-assigned when `AZURE_CLIENT_ID` names one.
#[must_use]
pub fn default_candidates(env: &AzureIdentityEnvironment) -> Vec<Candidate> {
    let managed_identity = ManagedIdentityCredential::new(Some(ManagedIdentityCredentialOptions {
        user_assigned_id: env.client_id.clone().map(UserAssignedId::ClientId),
        ..ManagedIdentityCredentialOptions::default()
    }))
    .map(|credential| credential as Arc<dyn TokenCredential>);
    let azure_cli =
        AzureCliCredential::new(None).map(|credential| credential as Arc<dyn TokenCredential>);
    vec![
        (
            "a managed identity",
            managed_identity,
            MANAGED_IDENTITY_BUDGET,
        ),
        ("the Azure CLI", azure_cli, AZURE_CLI_BUDGET),
    ]
}

/// The first of `candidates` that gets an access token for `scope` within its budget, its name,
/// and the token; or, when none does, what each said, as a message lists them.
///
/// # Errors
///
/// When no candidate answers, with what each said.
pub async fn first_answering(
    candidates: Vec<Candidate>,
    scope: &str,
) -> Result<(Arc<dyn TokenCredential>, &'static str, AccessToken), String> {
    let mut tried = Vec::new();
    for (name, credential, budget) in candidates {
        let credential = match credential {
            Ok(credential) => credential,
            Err(error) => {
                tried.push(format!("{name} ({})", clause(&error.to_string())));
                continue;
            }
        };
        match tokio::time::timeout(budget, credential.get_token(&[scope], None)).await {
            Ok(Ok(token)) => return Ok((credential, name, token)),
            Ok(Err(error)) => tried.push(format!("{name} ({})", clause(&error.to_string()))),
            Err(_) => tried.push(format!("{name} (no answer within {}s)", budget.as_secs())),
        }
    }
    Err(match tried.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    })
}

/// How Spice gets access tokens as the workload's Microsoft Entra identity.
pub enum AzureIdentity {
    /// A workload identity, exchanged by Spice and refreshed in the background.
    Workload(Arc<dyn TokenProvider>),
    /// An Azure SDK credential: a managed identity or the Azure CLI, for `scope`.
    Credential {
        credential: Arc<dyn TokenCredential>,
        scope: String,
    },
}

impl fmt::Debug for AzureIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Workload(provider) => f.debug_tuple("Workload").field(provider).finish(),
            Self::Credential { scope, .. } => f
                .debug_struct("Credential")
                .field("scope", scope)
                .finish_non_exhaustive(),
        }
    }
}

impl AzureIdentity {
    /// A current access token.
    ///
    /// # Errors
    ///
    /// When the credential cannot get one, with its reason as a clause.
    pub async fn access_token(&self) -> Result<String, String> {
        match self {
            Self::Workload(provider) => Ok(provider.get_token()),
            Self::Credential { credential, scope } => credential
                .get_token(&[scope], None)
                .await
                .map(|token| token.token.secret().to_string())
                .map_err(|error| clause(&error.to_string())),
        }
    }

    /// A [`TokenProvider`] whose current token a request reads, refreshed in the background
    /// before it expires. `first` is the access token [`resolve`] already obtained. `component`
    /// names what the tokens authenticate in the refresh warning.
    #[must_use]
    pub fn into_token_provider(
        self,
        component: &str,
        first: Option<AccessToken>,
    ) -> Arc<dyn TokenProvider> {
        match self {
            Self::Workload(provider) => provider,
            Self::Credential { credential, scope } => Arc::new(CredentialTokenProvider::start(
                component, credential, scope, first,
            )),
        }
    }
}

/// An identity Spice found as the workload's Microsoft Entra identity, its first access token,
/// and what it is, as a message names it, such as `the workload's managed identity`.
pub struct Resolved {
    pub identity: AzureIdentity,
    pub first_token: Option<AccessToken>,
    pub description: String,
}

impl fmt::Debug for Resolved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolved")
            .field("identity", &self.identity)
            .field("description", &self.description)
            .finish_non_exhaustive()
    }
}

/// The workload's Microsoft Entra identity for `target`: its workload identity in `env`, else its
/// managed identity, else the Azure CLI's account. The identity's first access token is obtained
/// here, so a missing or refused identity fails here. `component` names what the tokens
/// authenticate, as the logs show it.
///
/// # Errors
///
/// When the workload identity is incomplete or invalid ([`ResolveError::is_configuration`]),
/// Microsoft Entra ID refuses it, or no identity answers.
pub async fn resolve(
    component: &str,
    target: &EntraTarget,
    env: &AzureIdentityEnvironment,
) -> Result<Resolved, ResolveError> {
    resolve_with(component, target, env, default_candidates(env)).await
}

/// [`resolve`], trying `candidates` after the workload identity.
///
/// # Errors
///
/// As [`resolve`].
pub async fn resolve_with(
    component: &str,
    target: &EntraTarget,
    env: &AzureIdentityEnvironment,
    candidates: Vec<Candidate>,
) -> Result<Resolved, ResolveError> {
    if let Some((source, exchange, origin)) = workload_identity(target, env)? {
        let provider = WorkloadIdentityTokenProvider::try_new(component, source, exchange)
            .await
            .context(AccessTokenSnafu)?;
        return Ok(Resolved {
            identity: AzureIdentity::Workload(Arc::new(provider)),
            first_token: None,
            description: format!(
                "the Microsoft Entra workload identity in `AZURE_TENANT_ID` and `AZURE_CLIENT_ID`, with the identity token from {origin}"
            ),
        });
    }
    match first_answering(candidates, &target.scope).await {
        Ok((credential, name, token)) => Ok(Resolved {
            identity: AzureIdentity::Credential {
                credential,
                scope: target.scope.clone(),
            },
            first_token: Some(token),
            description: match name {
                "the Azure CLI" => "the Azure CLI's signed-in account".to_string(),
                _ => "the workload's managed identity".to_string(),
            },
        }),
        Err(tried) => NoIdentitySnafu {
            tried: format!("{NO_WORKLOAD_IDENTITY}, {tried}"),
        }
        .fail(),
    }
}

/// A [`TokenProvider`] over an Azure SDK credential, which gets a new access token before the
/// current one expires.
struct CredentialTokenProvider {
    rx: watch::Receiver<SecretString>,
    scope: String,
}

impl fmt::Debug for CredentialTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialTokenProvider")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// When `token` expires.
fn expiry(token: &AccessToken) -> DateTime<Utc> {
    DateTime::from_timestamp(token.expires_on.unix_timestamp(), 0).unwrap_or_else(Utc::now)
}

/// How long to keep a token that expires at `expires_at` (compared with `now`) before getting
/// one again: half its remaining life. The Azure SDK credentials return their cached token until
/// it nears expiry, so asking early costs nothing and leaves time for retries.
fn refresh_wait(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    (expires_at - now)
        .to_std()
        .map_or(MIN_REFRESH_WAIT, |remaining| {
            (remaining / 2).max(MIN_REFRESH_WAIT)
        })
}

impl CredentialTokenProvider {
    fn start(
        component: &str,
        credential: Arc<dyn TokenCredential>,
        scope: String,
        first: Option<AccessToken>,
    ) -> Self {
        let (first_token, mut expires_at) = match first {
            Some(token) => (token.token.secret().to_string(), expiry(&token)),
            None => (String::new(), Utc::now()),
        };
        let (tx, rx) = watch::channel(SecretString::from(first_token));
        let component = component.to_string();
        let task_scope = scope.clone();
        tokio::spawn(async move {
            let mut backoff = FibonacciBackoffBuilder::new()
                .max_duration(Some(MAX_RETRY_WAIT))
                .build();
            let mut next_wait = refresh_wait(expires_at, Utc::now());
            loop {
                tokio::select! {
                    () = tokio::time::sleep(next_wait) => {}
                    () = tx.closed() => break,
                }
                match credential.get_token(&[&task_scope], None).await {
                    Ok(token) => {
                        backoff.reset();
                        expires_at = expiry(&token);
                        next_wait = refresh_wait(expires_at, Utc::now());
                        if tx
                            .send(SecretString::from(token.token.secret().to_string()))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        next_wait = backoff.next_duration().unwrap_or(MAX_RETRY_WAIT);
                        tracing::warn!(
                            "{}",
                            refresh_failure_message(
                                &component,
                                expires_at,
                                Utc::now(),
                                next_wait,
                                &clause(&error.to_string()),
                                None,
                            )
                        );
                    }
                }
            }
        });
        Self { rx, scope }
    }
}

impl TokenProvider for CredentialTokenProvider {
    fn get_token(&self) -> String {
        self.rx.borrow().expose_secret().to_string()
    }

    fn dyn_hash(&self) -> String {
        format!("azure_credential_{}", self.scope)
    }

    fn subscribe(&self) -> Option<watch::Receiver<String>> {
        let mut secret_rx = self.rx.clone();
        let (tx, rx) = watch::channel(secret_rx.borrow().expose_secret().to_string());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = tx.closed() => break,
                    changed = secret_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let exposed = secret_rx.borrow().expose_secret().to_string();
                        if tx.send(exposed).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Some(rx)
    }
}

/// The workload's Microsoft Entra identity as the identity token another provider's federation
/// exchanges: an Entra access token for the app registration that represents that provider in
/// the tenant (`audience`, such as `api://<app ID>`), whose `sub` is the identity's object ID.
pub struct AzureIdentityToken {
    identity: AzureIdentity,
    scope: String,
    description: String,
}

impl AzureIdentityToken {
    /// The workload's Microsoft Entra identity, as [`resolve`] finds it, for tokens of
    /// `audience`. `component` names what the tokens authenticate.
    ///
    /// # Errors
    ///
    /// When no identity answers or Entra refuses, with the reason as a clause; an audience that
    /// is not an app registration of the tenant adds how to name one.
    pub async fn detect(
        component: &str,
        audience: &str,
        env: &AzureIdentityEnvironment,
    ) -> Result<Self, String> {
        let scope = format!("{}/.default", audience.trim_end_matches('/'));
        let target = EntraTarget {
            authority: env
                .authority_host
                .clone()
                .unwrap_or_else(|| PUBLIC_AUTHORITY.to_string()),
            scope: scope.clone(),
        };
        match resolve(component, &target, env).await {
            Ok(resolved) => Ok(Self {
                identity: resolved.identity,
                scope,
                description: resolved.description,
            }),
            Err(error) => {
                let reason = error.to_string();
                // AADSTS50001 (managed identity endpoints) and AADSTS500011 (the token endpoint):
                // the audience is not an app registration of the tenant.
                let hint = if (reason.contains("AADSTS50001") || reason.contains("AADSTS500011"))
                    && !audience.starts_with("api://")
                {
                    " Microsoft Entra ID issues a token only for an app registration of the tenant: create one that represents the provider and set the audience to its identifier URI, `api://<app ID>`."
                } else {
                    ""
                };
                Err(format!("{reason}{hint}"))
            }
        }
    }
}

#[async_trait]
impl IdentityTokenSource for AzureIdentityToken {
    async fn fetch(&self) -> Result<SecretString, String> {
        self.identity.access_token().await.map(SecretString::from)
    }

    fn describe(&self) -> String {
        format!("the Azure identity ({})", self.description)
    }

    fn hash_key(&self) -> String {
        format!("azure_identity_{}", self.scope)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use azure_core::credentials::TokenRequestOptions;
    use azure_core::time::OffsetDateTime;

    use super::*;

    /// A credential that answers with `outcome`, after `delay`, and records the scopes it was
    /// asked for.
    #[derive(Debug)]
    struct FakeCredential {
        outcome: Result<&'static str, &'static str>,
        delay: Duration,
        lifetime: Duration,
        scopes: Mutex<Vec<String>>,
        issued: AtomicUsize,
    }

    impl FakeCredential {
        fn new(outcome: Result<&'static str, &'static str>, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                outcome,
                delay,
                lifetime: Duration::from_secs(3600),
                scopes: Mutex::new(Vec::new()),
                issued: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl TokenCredential for FakeCredential {
        async fn get_token(
            &self,
            scopes: &[&str],
            _options: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            self.scopes
                .lock()
                .expect("lock")
                .extend(scopes.iter().map(|scope| (*scope).to_string()));
            tokio::time::sleep(self.delay).await;
            match self.outcome {
                Ok(token) => {
                    let n = self.issued.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok(AccessToken::new(
                        format!("{token}-{n}"),
                        OffsetDateTime::now_utc() + self.lifetime,
                    ))
                }
                Err(reason) => Err(azure_core::Error::with_message(
                    azure_core::error::ErrorKind::Credential,
                    reason,
                )),
            }
        }
    }

    fn vault() -> EntraTarget {
        EntraTarget {
            authority: PUBLIC_AUTHORITY.to_string(),
            scope: "https://vault.azure.net/.default".to_string(),
        }
    }

    #[tokio::test]
    async fn the_first_identity_that_answers_within_its_budget_is_used_and_the_others_are_listed() {
        let hanging = FakeCredential::new(Ok("never"), Duration::from_secs(60));
        let refusing = FakeCredential::new(
            Err("ERROR: Please run 'az login' to setup account.\n"),
            Duration::ZERO,
        );
        let answering = FakeCredential::new(Ok("vault-token"), Duration::ZERO);
        let candidates = |answering: Option<Arc<FakeCredential>>| {
            let mut candidates: Vec<Candidate> = vec![
                (
                    "a managed identity",
                    Ok(Arc::clone(&hanging) as Arc<dyn TokenCredential>),
                    Duration::from_millis(50),
                ),
                (
                    "a broken credential",
                    Err(azure_core::Error::with_message(
                        azure_core::error::ErrorKind::Credential,
                        "Azure Arc managed identity isn't supported",
                    )),
                    Duration::from_millis(50),
                ),
                (
                    "the Azure CLI",
                    Ok(Arc::clone(&refusing) as Arc<dyn TokenCredential>),
                    Duration::from_millis(50),
                ),
            ];
            if let Some(answering) = answering {
                candidates.push((
                    "the last resort",
                    Ok(answering as Arc<dyn TokenCredential>),
                    Duration::from_millis(50),
                ));
            }
            candidates
        };

        let resolved = resolve_with(
            "model 'gpt' (azure)",
            &vault(),
            &AzureIdentityEnvironment::default(),
            candidates(Some(Arc::clone(&answering))),
        )
        .await
        .expect("the last candidate answers");
        assert_eq!(resolved.description, "the workload's managed identity");
        assert_eq!(
            resolved
                .first_token
                .as_ref()
                .map(|token| token.token.secret().to_string()),
            Some("vault-token-1".to_string())
        );
        assert_eq!(
            resolved.identity.access_token().await,
            Ok("vault-token-2".to_string())
        );
        assert_eq!(
            *answering.scopes.lock().expect("lock"),
            vec!["https://vault.azure.net/.default".to_string(); 2]
        );

        let error = resolve_with(
            "model 'gpt' (azure)",
            &vault(),
            &AzureIdentityEnvironment::default(),
            candidates(None),
        )
        .await
        .expect_err("no candidate answers");
        assert_eq!(
            error.to_string(),
            "a workload identity (`AZURE_FEDERATED_TOKEN_FILE` is not set, and this is not a GitHub Actions job with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `permissions: id-token: write`), a managed identity (no answer within 0s), a broken credential (Azure Arc managed identity isn't supported) and the Azure CLI (ERROR: Please run 'az login' to setup account)"
        );
        assert!(!error.is_configuration());
    }

    #[tokio::test]
    async fn a_token_file_without_its_tenant_is_a_configuration_error() {
        let error = resolve_with(
            "model 'gpt' (azure)",
            &vault(),
            &AzureIdentityEnvironment {
                client_id: Some("00000000-0000-0000-0000-000000000001".to_string()),
                federated_token_file: Some("/var/run/secrets/azure/tokens/token".to_string()),
                ..AzureIdentityEnvironment::default()
            },
            Vec::new(),
        )
        .await
        .expect_err("no tenant");
        assert_eq!(error.to_string(), "`AZURE_TENANT_ID` is not set");
        assert!(error.is_configuration());
    }

    #[tokio::test]
    async fn a_credentials_token_is_refreshed_before_it_expires() {
        let credential = Arc::new(FakeCredential {
            outcome: Ok("short"),
            delay: Duration::ZERO,
            lifetime: Duration::from_secs(4),
            scopes: Mutex::new(Vec::new()),
            issued: AtomicUsize::new(0),
        });
        let first = credential
            .get_token(&["scope"], None)
            .await
            .expect("the first token");
        let provider = AzureIdentity::Credential {
            credential: Arc::clone(&credential) as Arc<dyn TokenCredential>,
            scope: "scope".to_string(),
        }
        .into_token_provider("model 'gpt' (azure)", Some(first));
        assert_eq!(provider.get_token(), "short-1");
        // Refreshed halfway through its remaining four seconds.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while provider.get_token() == "short-1" {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the token was not refreshed"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(provider.get_token(), "short-2");
    }

    #[test]
    fn a_token_is_refreshed_halfway_through_its_remaining_life() {
        let now = Utc::now();
        assert_eq!(
            [3600, 60, 1, -5].map(|secs| refresh_wait(now + chrono::Duration::seconds(secs), now)),
            [
                Duration::from_mins(30),
                Duration::from_secs(30),
                MIN_REFRESH_WAIT,
                MIN_REFRESH_WAIT
            ]
        );
    }
}
