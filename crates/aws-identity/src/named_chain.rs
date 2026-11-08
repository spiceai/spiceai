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

//! The AWS credential chain every AWS component of Spice uses, in the order of the AWS SDKs'
//! default chain, with one more link: the identity of a GitHub Actions job when `AWS_ROLE_ARN`
//! names a role and no token file is set. Each link has a name, so Spice can say which one
//! answered, and when none does, what each said.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_config::ecs::EcsCredentialsProvider;
use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::provider_config::ProviderConfig;
use aws_config::web_identity_token::WebIdentityTokenCredentialsProvider;
use aws_config::{Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{self, ProvideCredentials, SharedCredentialsProvider, future};
use aws_sdk_sts::error::ProvideErrorMetadata;
use aws_smithy_runtime_api::client::result::SdkError;
use secrecy::ExposeSecret;
use token_provider::workload_identity::{IdentityTokenClaims, SubjectTokenSource};

/// The audience GitHub Actions issues an identity token for when AWS STS exchanges it, as
/// `aws-actions/configure-aws-credentials` requests it by default.
pub const STS_AUDIENCE: &str = "sts.amazonaws.com";

/// What a message calls each link.
pub const ENVIRONMENT: &str = "environment keys";
pub const PROFILE: &str = "the AWS profile";
pub const WEB_IDENTITY: &str = "web identity (`AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`)";
pub const GITHUB_ACTIONS: &str = "this GitHub Actions job (`AWS_ROLE_ARN`)";
pub const CONTAINER: &str = "container credentials (ECS or EKS Pod Identity)";
pub const INSTANCE: &str = "the instance's IAM role (IMDS)";

/// Bounds the reason quoted for a link.
const MAX_REASON_LEN: usize = 200;

/// The link of a [`NamedChain`] that gave a set of credentials, as a message names it. It is set
/// on the [`Credentials`] the chain returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialSource(pub &'static str);

/// Why no link of a [`NamedChain`] gave credentials: each link tried and what it said.
#[derive(Debug)]
pub struct NoCredentials {
    pub tried: String,
}

impl fmt::Display for NoCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "found no AWS credentials. Tried {}", self.tried)
    }
}

impl std::error::Error for NoCredentials {}

/// AWS STS refusing to assume a role, as [`assume_role_refusal`] words it. The message bounds its
/// own parts, so [`credential_source`] passes it on whole, where it bounds any other failure.
#[derive(Clone, Debug)]
struct AssumeRoleRefused(String);

impl fmt::Display for AssumeRoleRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AssumeRoleRefused {}

/// `text` as a clause of a longer message: on one line, without a trailing period, and bounded.
fn clause(text: &str) -> String {
    let text = util::single_line(text);
    let text = text.trim().trim_end_matches('.');
    match text.char_indices().nth(MAX_REASON_LEN) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

/// `error` and each error it wraps, from the outermost to the cause.
fn causes<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'a (dyn std::error::Error + 'static)> {
    std::iter::successors(Some(error), |&error| error.source())
}

/// The innermost reason of `error` as a bounded clause, which names the cause rather than the
/// wrapper.
fn innermost(error: &(dyn std::error::Error + 'static)) -> String {
    clause(&causes(error).last().unwrap_or(error).to_string())
}

/// Credential providers tried in order, each under its name. The first that gives credentials
/// wins, and they carry its name as a [`CredentialSource`]. A link that is not configured passes
/// to the next, as in the AWS SDKs' default chain; one that fails otherwise stops the chain.
#[derive(Debug, Default)]
pub struct NamedChain {
    links: Vec<(&'static str, SharedCredentialsProvider)>,
}

impl NamedChain {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The chain with `provider` tried after the links before it, under `name`.
    #[must_use]
    pub fn with(mut self, name: &'static str, provider: impl ProvideCredentials + 'static) -> Self {
        self.links
            .push((name, SharedCredentialsProvider::new(provider)));
        self
    }

    async fn credentials(&self) -> provider::Result {
        let mut tried = Vec::new();
        for (name, link) in &self.links {
            match link.provide_credentials().await {
                Ok(mut credentials) => {
                    credentials.set_property(CredentialSource(name));
                    return Ok(credentials);
                }
                Err(error @ CredentialsError::CredentialsNotLoaded(_)) => {
                    tried.push(format!("{name} ({})", innermost(&error)));
                }
                Err(error) => return Err(error),
            }
        }
        let tried = match tried.as_slice() {
            [] => String::new(),
            [only] => only.clone(),
            [rest @ .., last] => format!("{}, and {last}", rest.join(", ")),
        };
        Err(CredentialsError::not_loaded(NoCredentials { tried }))
    }
}

impl ProvideCredentials for NamedChain {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(self.credentials())
    }
}

/// The chain of the AWS SDKs' default credential provider, in its order, with the identity of a
/// GitHub Actions job after web identity: environment keys, the AWS profile (`profile` or
/// `AWS_PROFILE`), web identity, this GitHub Actions job, container credentials, then the
/// instance's IAM role. Each link calls AWS as a client of `config` does: in its region (STS in
/// `us-east-1` when it has none, which assumes a role of any region), with its FIPS and dual-stack
/// endpoints, HTTP client and time source. [`crate::AwsConfigLoader`] builds it.
pub(crate) fn default_named_chain(config: &SdkConfig, profile: Option<&str>) -> NamedChain {
    let region = config
        .region()
        .cloned()
        .unwrap_or_else(|| Region::new(crate::DEFAULT_REGION));
    let mut provider_config = ProviderConfig::default()
        .with_region(Some(region.clone()))
        .with_use_fips(config.use_fips())
        .with_use_dual_stack(config.use_dual_stack())
        .with_behavior_version(config.behavior_version());
    if let Some(http_client) = config.http_client() {
        provider_config = provider_config.with_http_client(http_client);
    }
    if let Some(sleep_impl) = config.sleep_impl() {
        provider_config = provider_config.with_sleep_impl(sleep_impl);
    }
    if let Some(time_source) = config.time_source() {
        provider_config = provider_config.with_time_source(time_source);
    }
    // The component's endpoint is its own service's: STS keeps its own (`AWS_ENDPOINT_URL_STS`),
    // as it does for the SDK's providers above.
    let mut sts_config = config.to_builder();
    sts_config.set_endpoint_url(None);
    let sts_config = sts_config.region(region).build();

    let mut profile_provider =
        ProfileFileCredentialsProvider::builder().configure(&provider_config);
    if let Some(profile) = profile {
        profile_provider = profile_provider.profile_name(profile);
    }
    NamedChain::new()
        .with(ENVIRONMENT, EnvironmentVariableCredentialsProvider::new())
        .with(PROFILE, profile_provider.build())
        .with(
            WEB_IDENTITY,
            WebIdentityTokenCredentialsProvider::builder()
                .configure(&provider_config)
                .build(),
        )
        .with(GITHUB_ACTIONS, GitHubActionsRole::from_env(sts_config))
        .with(
            CONTAINER,
            EcsCredentialsProvider::builder()
                .configure(&provider_config)
                .build(),
        )
        .with(
            INSTANCE,
            ImdsCredentialsProvider::builder()
                .configure(&provider_config)
                .build(),
        )
}

/// Gets credentials from `provider` once, and names where they came from: the link of a
/// [`NamedChain`] that gave them, or `static_source` for credentials the component was given.
///
/// # Errors
///
/// When no credentials are found, with each link tried and what it said, or when getting them
/// fails, with the reason.
pub async fn credential_source(
    provider: &SharedCredentialsProvider,
    static_source: &'static str,
) -> Result<&'static str, String> {
    match provider.provide_credentials().await {
        Ok(credentials) => Ok(credentials
            .get_property::<CredentialSource>()
            .map_or(static_source, |source| source.0)),
        Err(error) => {
            if let Some(none) =
                causes(&error).find_map(|cause| cause.downcast_ref::<NoCredentials>())
            {
                return Err(none.to_string());
            }
            let reason =
                match causes(&error).find_map(|cause| cause.downcast_ref::<AssumeRoleRefused>()) {
                    Some(refused) => refused.to_string(),
                    None => innermost(&error),
                };
            Err(format!("failed to get AWS credentials: {reason}"))
        }
    }
}

/// The identity of the GitHub Actions job this process runs in, exchanged with AWS STS
/// (`AssumeRoleWithWebIdentity`) for credentials of the role `AWS_ROLE_ARN` names. It answers
/// only in a job with `permissions: id-token: write`, with `AWS_ROLE_ARN` set and no
/// `AWS_WEB_IDENTITY_TOKEN_FILE`, which web identity reads instead. Each time the SDK refreshes
/// the credentials, a new identity token is requested.
pub(crate) struct GitHubActionsRole {
    role_arn: Option<String>,
    token_file_set: bool,
    /// The job's OIDC token endpoint, which never formats `ACTIONS_ID_TOKEN_REQUEST_TOKEN`, the
    /// credential that obtains the job's identity tokens.
    job: Option<SubjectTokenSource>,
    /// The configuration of the STS client, without credentials: `AssumeRoleWithWebIdentity` is
    /// not signed, and this link must not ask the chain it is part of.
    sts_config: SdkConfig,
}

impl fmt::Debug for GitHubActionsRole {
    /// Not the STS configuration itself, whose `Debug` prints the parsed AWS profiles,
    /// `aws_secret_access_key` among them.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GitHubActionsRole")
            .field("role_arn", &self.role_arn)
            .field("token_file_set", &self.token_file_set)
            .field("job", &self.job)
            .field("sts_region", &self.sts_config.region())
            .field("sts_endpoint_url", &self.sts_config.endpoint_url())
            .finish()
    }
}

impl GitHubActionsRole {
    /// The job and role of this process's environment, with STS called as `sts_config` says.
    fn from_env(sts_config: SdkConfig) -> Self {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        Self {
            role_arn: var("AWS_ROLE_ARN"),
            token_file_set: var("AWS_WEB_IDENTITY_TOKEN_FILE").is_some(),
            job: SubjectTokenSource::github_actions(
                var("ACTIONS_ID_TOKEN_REQUEST_URL"),
                var("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
                STS_AUDIENCE,
            )
            .ok(),
            sts_config,
        }
    }

    /// The identity token source, or why this link does not apply, as a clause.
    fn source(&self) -> Result<(&str, &SubjectTokenSource), &'static str> {
        let Some(role_arn) = self.role_arn.as_deref() else {
            return Err("`AWS_ROLE_ARN` is not set");
        };
        if self.token_file_set {
            return Err("`AWS_WEB_IDENTITY_TOKEN_FILE` is set, which web identity reads");
        }
        let job = self
            .job
            .as_ref()
            .ok_or("this is not a GitHub Actions job with `permissions: id-token: write`")?;
        Ok((role_arn, job))
    }

    async fn credentials(&self) -> provider::Result {
        let (role_arn, source) = self.source().map_err(CredentialsError::not_loaded)?;
        let token = source
            .fetch_identity_token()
            .await
            .map_err(CredentialsError::provider_error)?;
        let sts = aws_sdk_sts::Client::new(&self.sts_config);
        let session = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or_default();
        let response = sts
            .assume_role_with_web_identity()
            .role_arn(role_arn)
            .role_session_name(format!("spice-github-actions-{session}"))
            .web_identity_token(token.expose_secret())
            .send()
            .await
            .map_err(|error| {
                CredentialsError::provider_error(assume_role_refusal(
                    role_arn,
                    &error,
                    token.expose_secret(),
                ))
            })?;
        let Some(credentials) = response.credentials() else {
            return Err(CredentialsError::provider_error(
                "AWS STS assumed the role but returned no credentials",
            ));
        };
        Ok(Credentials::new(
            credentials.access_key_id(),
            credentials.secret_access_key(),
            Some(credentials.session_token().to_string()),
            SystemTime::try_from(*credentials.expiration()).ok(),
            "GitHubActions",
        ))
    }
}

impl ProvideCredentials for GitHubActionsRole {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(self.credentials())
    }
}

/// The message for STS refusing to assume `role_arn` with a GitHub Actions identity token: STS's
/// reason, the token's claims for the user to compare with the role's trust policy, and what to
/// check. Never the token.
fn assume_role_refusal<E, R>(
    role_arn: &str,
    error: &SdkError<E, R>,
    token: &str,
) -> AssumeRoleRefused
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
    R: fmt::Debug + 'static,
{
    let reason = match error.as_service_error() {
        Some(service) => match (service.code(), service.message()) {
            (Some(code), Some(message)) => format!("{code}: {}", clause(message)),
            (Some(code), None) => code.to_string(),
            (None, Some(message)) => clause(message),
            (None, None) => "no reason given".to_string(),
        },
        None => innermost(error),
    };
    let claims = IdentityTokenClaims::read(token)
        .map(|claims| claims.sentence("GitHub Actions"))
        .unwrap_or_default();
    AssumeRoleRefused(format!(
        "AWS STS refused to assume the role '{role_arn}' with the GitHub Actions identity token: {reason}.{claims} Verify that the role's trust policy trusts `token.actions.githubusercontent.com` for this job's subject with the audience `{STS_AUDIENCE}`."
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A link that is not configured, with `reason`.
    #[derive(Debug)]
    struct NotConfigured(&'static str);

    impl ProvideCredentials for NotConfigured {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            future::ProvideCredentials::ready(Err(CredentialsError::not_loaded(self.0)))
        }
    }

    /// A link that gives credentials, counting how often it is asked.
    #[derive(Debug, Default)]
    struct Giving(Arc<AtomicUsize>);

    impl ProvideCredentials for Giving {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            self.0.fetch_add(1, Ordering::SeqCst);
            future::ProvideCredentials::ready(Ok(Credentials::new(
                "AKID", "secret", None, None, "test",
            )))
        }
    }

    /// A link that fails with `reason`, an error other than not being configured.
    #[derive(Debug)]
    struct Failing(String);

    impl ProvideCredentials for Failing {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            future::ProvideCredentials::ready(Err(CredentialsError::provider_error(self.0.clone())))
        }
    }

    #[tokio::test]
    async fn the_first_link_that_gives_credentials_is_named() {
        let later = Arc::new(AtomicUsize::new(0));
        let chain = SharedCredentialsProvider::new(
            NamedChain::new()
                .with(
                    ENVIRONMENT,
                    NotConfigured("environment variable not set: AWS_ACCESS_KEY_ID"),
                )
                .with(WEB_IDENTITY, Giving::default())
                .with(INSTANCE, Giving(Arc::clone(&later))),
        );
        assert_eq!(
            credential_source(&chain, "the static keys").await,
            Ok(WEB_IDENTITY)
        );
        assert_eq!(
            later.load(Ordering::SeqCst),
            0,
            "the chain stops at the first answer"
        );

        let fixed = SharedCredentialsProvider::new(Credentials::new(
            "AKID", "secret", None, None, "static",
        ));
        assert_eq!(
            credential_source(&fixed, "the access key in `aws_access_key_id`").await,
            Ok("the access key in `aws_access_key_id`")
        );
    }

    #[tokio::test]
    async fn no_credentials_name_every_link_tried_and_what_it_said() {
        let chain = SharedCredentialsProvider::new(
            NamedChain::new()
                .with(
                    ENVIRONMENT,
                    NotConfigured("environment variable not set: AWS_ACCESS_KEY_ID."),
                )
                .with(GITHUB_ACTIONS, NotConfigured("`AWS_ROLE_ARN` is not set"))
                .with(
                    INSTANCE,
                    NotConfigured("failed to load credentials from IMDS"),
                ),
        );
        assert_eq!(
            credential_source(&chain, "the static keys").await,
            Err("found no AWS credentials. Tried environment keys (environment variable not set: AWS_ACCESS_KEY_ID), this GitHub Actions job (`AWS_ROLE_ARN`) (`AWS_ROLE_ARN` is not set), and the instance's IAM role (IMDS) (failed to load credentials from IMDS)".to_string())
        );
    }

    /// A link that STS refuses, as the GitHub Actions link reports it.
    #[derive(Debug)]
    struct Refusing(AssumeRoleRefused);

    impl ProvideCredentials for Refusing {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            future::ProvideCredentials::ready(Err(CredentialsError::provider_error(self.0.clone())))
        }
    }

    #[tokio::test]
    async fn a_refusal_reaches_the_component_whole() {
        use base64::Engine as _;
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;

        let token = format!(
            "eyJhbGciOiJSUzI1NiJ9.{}.c2ln",
            URL_SAFE_NO_PAD.encode(
                r#"{"iss":"https://token.actions.githubusercontent.com","sub":"repo:acme/widgets:environment:ci","aud":"sts.amazonaws.com","exp":1794060300}"#
            )
        );
        let refusal = assume_role_refusal::<
            aws_sdk_sts::operation::assume_role_with_web_identity::AssumeRoleWithWebIdentityError,
            (),
        >(
            "arn:aws:iam::111122223333:role/spice-ci",
            &SdkError::construction_failure(
                "Not authorized to perform sts:AssumeRoleWithWebIdentity",
            ),
            &token,
        );
        let message = refusal.to_string();
        assert!(
            message.contains("has issuer 'https://token.actions.githubusercontent.com', subject 'repo:acme/widgets:environment:ci', audience 'sts.amazonaws.com' and expiry 2026-11-07T14:05:00Z")
                && message.ends_with("Verify that the role's trust policy trusts `token.actions.githubusercontent.com` for this job's subject with the audience `sts.amazonaws.com`."),
            "{message}"
        );
        let chain = SharedCredentialsProvider::new(
            NamedChain::new().with(GITHUB_ACTIONS, Refusing(refusal)),
        );
        assert_eq!(
            credential_source(&chain, "the static keys").await,
            Err(format!("failed to get AWS credentials: {message}"))
        );

        // The same words from any other provider are bounded to one clause.
        let other = SharedCredentialsProvider::new(
            NamedChain::new().with(WEB_IDENTITY, Failing(message.clone())),
        );
        let bounded: String = message.chars().take(MAX_REASON_LEN).collect();
        assert_eq!(
            credential_source(&other, "the static keys").await,
            Err(format!("failed to get AWS credentials: {bounded}…"))
        );
    }

    #[test]
    fn the_github_actions_link_never_formats_its_request_token() {
        let role = GitHubActionsRole {
            role_arn: Some("arn:aws:iam::111122223333:role/spice-ci".to_string()),
            token_file_set: false,
            job: SubjectTokenSource::github_actions(
                Some("https://actions.example/token?secret-url-part".to_string()),
                Some("OIDC_REQUEST_TOKEN_CANARY".to_string()),
                STS_AUDIENCE,
            )
            .ok(),
            // Its `Debug` would print it, as it would the parsed AWS profiles.
            sts_config: SdkConfig::builder()
                .app_name(aws_config::AppName::new("sts-config-canary").expect("an app name"))
                .build(),
        };
        let formatted = format!("{:?}", NamedChain::new().with(GITHUB_ACTIONS, role));
        assert!(
            !formatted.contains("OIDC_REQUEST_TOKEN_CANARY")
                && !formatted.contains("secret-url-part")
                && !formatted.contains("sts-config-canary"),
            "{formatted}"
        );
        assert!(
            formatted.contains("arn:aws:iam::111122223333:role/spice-ci")
                && formatted.contains(r#"GitHubActions { audience: "sts.amazonaws.com", .. }"#),
            "{formatted}"
        );
    }

    #[tokio::test]
    async fn a_link_that_fails_otherwise_stops_the_chain() {
        let later = Arc::new(AtomicUsize::new(0));
        let chain = SharedCredentialsProvider::new(
            NamedChain::new()
                .with(
                    WEB_IDENTITY,
                    Failing("the role's trust policy refused".to_string()),
                )
                .with(INSTANCE, Giving(Arc::clone(&later))),
        );
        assert_eq!(
            credential_source(&chain, "the static keys").await,
            Err("failed to get AWS credentials: the role's trust policy refused".to_string())
        );
        assert_eq!(later.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn the_github_actions_link_needs_a_job_a_role_and_no_token_file() {
        let role = |role_arn: Option<&str>, token_file_set: bool, job: bool| GitHubActionsRole {
            role_arn: role_arn.map(str::to_string),
            token_file_set,
            job: SubjectTokenSource::github_actions(
                job.then(|| "https://actions.example/token".to_string()),
                job.then(|| "request-token".to_string()),
                STS_AUDIENCE,
            )
            .ok(),
            sts_config: SdkConfig::builder().build(),
        };
        let arn = Some("arn:aws:iam::111122223333:role/spice-ci");
        assert_eq!(
            [
                role(None, false, true)
                    .source()
                    .map(|(arn, _)| arn.to_string()),
                role(arn, true, true)
                    .source()
                    .map(|(arn, _)| arn.to_string()),
                role(arn, false, false)
                    .source()
                    .map(|(arn, _)| arn.to_string()),
                role(arn, false, true)
                    .source()
                    .map(|(arn, _)| arn.to_string()),
            ],
            [
                Err("`AWS_ROLE_ARN` is not set"),
                Err("`AWS_WEB_IDENTITY_TOKEN_FILE` is set, which web identity reads"),
                Err("this is not a GitHub Actions job with `permissions: id-token: write`"),
                Ok("arn:aws:iam::111122223333:role/spice-ci".to_string()),
            ]
        );
        let github = role(arn, false, true);
        let Ok((_, source)) = github.source() else {
            panic!("a GitHub Actions source");
        };
        assert_eq!(
            format!("{source:?}"),
            r#"GitHubActions { audience: "sts.amazonaws.com", .. }"#
        );
    }
}
