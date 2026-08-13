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

//! The resumable `spice connect` enrollment and project transaction.

use std::cell::Cell;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Password, Select};
use runtime_cloud_connect::enroll::{
    EnrollNowOutcome, EnrollmentAuthority, InstanceFacts, RetryPolicy, SessionToken,
};
use runtime_cloud_connect::enrollment_key::EnrollmentKey;
use runtime_cloud_connect::identity::{AppAttachment, Identity, IdentityStore};
use runtime_cloud_connect::{CloudConnectConfig, EnrollmentTransactionLock};

use crate::commands::cloud::{CloudClient, org as cloud_org};
use crate::commands::login::connect_org::{
    OrgResolution, resolve_connect_organization_with_client,
};
use crate::commands::login::session::{CredentialStore, LoginContinuation, login_inline};
use crate::context::RuntimeContext;
use crate::error::{CloudErrorCode, Error, Result};

use super::naming::{collision_suggestion, initial_suggestion, validate_project_name};
use super::project::{ProjectAttachment, ProjectClient, ProjectMutation};
use super::state::{ConnectLock, ConnectOperation, ProjectOperation};

pub(super) struct ConnectRequest {
    pub org: Option<String>,
    pub project: Option<String>,
    pub token: Option<EnrollmentKey>,
    pub region: Option<String>,
    pub dir: Option<PathBuf>,
    pub endpoint: Option<String>,
}

struct EnrollmentResult {
    identity: Identity,
    recovery_url: Option<String>,
    already_enrolled: bool,
}

struct TransactionEndpoint {
    value: String,
    persist_file: bool,
    permits_stored_credentials: bool,
}

const PROJECT_RETRY_DEADLINE: Duration = Duration::from_secs(30);
const PROJECT_MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthChoice {
    Login,
    EnrollmentKey,
}

trait Prompter {
    fn interactive(&self) -> bool;
    async fn choose_auth(&mut self) -> Result<Option<AuthChoice>>;
    async fn read_enrollment_key(&mut self, portal_url: &str) -> Result<Option<String>>;
    async fn confirm_project_assignment(&mut self) -> Result<Option<bool>>;
    async fn project_name(&mut self, suggestion: &str) -> Result<Option<String>>;
}

struct TerminalPrompter {
    interactive: bool,
}

impl TerminalPrompter {
    fn new() -> Self {
        Self {
            interactive: std::io::stdin().is_terminal(),
        }
    }
}

impl Prompter for TerminalPrompter {
    fn interactive(&self) -> bool {
        self.interactive
    }

    async fn choose_auth(&mut self) -> Result<Option<AuthChoice>> {
        let result = tokio::task::spawn_blocking(|| {
            Select::with_theme(&ColorfulTheme::default())
                .with_prompt("Connect this directory to Spice Cloud")
                .items([
                    "Log in to Spice Cloud (recommended)",
                    "Use an enrollment key",
                ])
                .default(0)
                .interact_opt()
        })
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("authentication-choice task panicked: {source}"),
        })?;
        map_optional_prompt(result, "authentication choice").map(|choice| {
            choice.map(|index| {
                if index == 0 {
                    AuthChoice::Login
                } else {
                    AuthChoice::EnrollmentKey
                }
            })
        })
    }

    async fn read_enrollment_key(&mut self, portal_url: &str) -> Result<Option<String>> {
        println!("Open this URL to create or copy an enrollment key:");
        println!("{portal_url}");
        println!();
        let open_url = portal_url.to_string();
        tokio::task::spawn_blocking(move || {
            // The printed URL is authoritative recovery when the platform has
            // no opener or the opener fails.
            let _ = system_open::that(open_url);
        });
        let result = tokio::task::spawn_blocking(|| {
            Password::with_theme(&ColorfulTheme::default())
                .with_prompt("Enrollment key")
                .interact()
        })
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("enrollment-key prompt task panicked: {source}"),
        })?;
        map_required_prompt(result, "enrollment key")
    }

    async fn confirm_project_assignment(&mut self) -> Result<Option<bool>> {
        let result = tokio::task::spawn_blocking(|| {
            Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt("Create a project for this instance now?")
                .default(false)
                .interact_opt()
        })
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("project confirmation task panicked: {source}"),
        })?;
        map_optional_prompt(result, "project confirmation")
    }

    async fn project_name(&mut self, suggestion: &str) -> Result<Option<String>> {
        let suggestion = suggestion.to_string();
        let result = tokio::task::spawn_blocking(move || {
            Input::<String>::with_theme(&ColorfulTheme::default())
                .with_prompt("Project name")
                .default(suggestion)
                .allow_empty(false)
                .interact_text()
        })
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("project-name prompt task panicked: {source}"),
        })?;
        map_required_prompt(result, "project name")
    }
}

fn map_optional_prompt<T>(result: dialoguer::Result<Option<T>>, what: &str) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(value),
        Err(dialoguer::Error::IO(source))
            if matches!(
                source.kind(),
                std::io::ErrorKind::Interrupted | std::io::ErrorKind::UnexpectedEof
            ) =>
        {
            Ok(None)
        }
        Err(source) => Err(Error::CloudConnectIo {
            message: format!("read {what}: {source}"),
        }),
    }
}

fn map_required_prompt<T>(result: dialoguer::Result<T>, what: &str) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(dialoguer::Error::IO(source))
            if matches!(
                source.kind(),
                std::io::ErrorKind::Interrupted | std::io::ErrorKind::UnexpectedEof
            ) =>
        {
            Ok(None)
        }
        Err(source) => Err(Error::CloudConnectIo {
            message: format!("read {what}: {source}"),
        }),
    }
}

struct LoginCredential {
    token: SessionToken,
    credential_org: Option<String>,
}

struct FlowTelemetry {
    auth_path: Cell<&'static str>,
    completion: Cell<&'static str>,
    failure_stage: Cell<&'static str>,
}

impl FlowTelemetry {
    fn new() -> Self {
        Self {
            auth_path: Cell::new("unknown"),
            completion: Cell::new("failed"),
            failure_stage: Cell::new("initialization"),
        }
    }

    fn auth(&self, path: &'static str) {
        self.auth_path.set(path);
    }

    fn stage(&self, stage: &'static str) {
        self.failure_stage.set(stage);
    }

    fn complete(&self, outcome: &'static str) {
        self.completion.set(outcome);
        self.failure_stage.set("none");
    }
}

impl Drop for FlowTelemetry {
    fn drop(&mut self) {
        tracing::debug!(
            target: "spice_cli::connect",
            auth_path = self.auth_path.get(),
            completion = self.completion.get(),
            failure_stage = self.failure_stage.get(),
            "Spice Connect flow completed"
        );
    }
}

pub(super) async fn execute(ctx: &RuntimeContext, request: ConnectRequest) -> Result<()> {
    execute_with(ctx, request, &mut TerminalPrompter::new()).await
}

async fn execute_with<P: Prompter>(
    ctx: &RuntimeContext,
    request: ConnectRequest,
    prompter: &mut P,
) -> Result<()> {
    let telemetry = FlowTelemetry::new();
    preflight_request(&request)?;
    telemetry.stage("local_state");
    let directory = canonical_instance_directory(request.dir.as_deref()).await?;
    let config_dir = CloudConnectConfig::resolve_config_dir(Some(&directory));
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    if !prompter.interactive()
        && request.token.is_none()
        && (request.org.is_none() || request.project.is_none())
        && !identity_path.exists()
    {
        return Err(invalid_usage(
            "non-interactive Cloud Connect requires either a login with --org <org> --project <name>, or --token <enrollment-key>.",
        ));
    }
    let _lock = ConnectLock::acquire(&config_dir, "connect")
        .await
        .map_err(|error| state_error(&error))?;
    let identity = load_identity(&identity_path).await?;
    if let Some(identity) = identity.as_ref() {
        validate_existing_identity(&identity_path, identity)?;
    }
    let (draft, _, _) = load_operations(&config_dir).await?;
    let resolved_endpoint = resolve_transaction_endpoint(
        &config_dir,
        request.endpoint.as_deref(),
        identity.as_ref(),
        draft.as_ref(),
    )?;
    let endpoint = resolved_endpoint.value.clone();
    reconcile_journal(&config_dir, &directory, &endpoint, identity.as_ref()).await?;
    let project_operation =
        reconcile_project_journal(&config_dir, &directory, &endpoint, identity.as_ref()).await?;

    if let Some(identity) = identity {
        telemetry.auth("existing");
        return existing_identity_flow(
            &request,
            prompter,
            ExistingIdentityContext {
                config_dir: &config_dir,
                directory: &directory,
                identity_path: &identity_path,
                endpoint: &endpoint,
                persist_endpoint_file: resolved_endpoint.persist_file,
                permits_stored_credentials: resolved_endpoint.permits_stored_credentials,
                identity,
                pending_project: project_operation,
            },
            &telemetry,
        )
        .await;
    }

    if let Some(key) = request.token {
        telemetry.auth("token");
        telemetry.stage("enrollment");
        let enrolled = enroll(
            ctx,
            &config_dir,
            &directory,
            &resolved_endpoint,
            request.region,
            request.org.clone().unwrap_or_default(),
            EnrollmentAuthority::Token {
                key,
                expected_org: request.org,
            },
        )
        .await?;
        print_enrollment_result(&enrolled);
        telemetry.complete(if enrolled.identity.app_id.is_some() {
            "already_attached"
        } else {
            "unattached"
        });
        return Ok(());
    }

    if !prompter.interactive() && (request.org.is_none() || request.project.is_none()) {
        return Err(invalid_usage(
            "non-interactive Cloud Connect requires either a login with --org <org> --project <name>, or --token <enrollment-key>.",
        ));
    }

    telemetry.stage("authentication");
    let login = match if resolved_endpoint.permits_stored_credentials {
        stored_user_login(&endpoint, request.org.as_deref()).await?
    } else {
        None
    } {
        Some(login) => login,
        None if !prompter.interactive() => {
            if !resolved_endpoint.permits_stored_credentials {
                return Err(legacy_endpoint_requires_explicit_authority(&endpoint));
            }
            return Err(invalid_usage(
                "non-interactive Cloud Connect requires either a login with --org <org> --project <name>, or --token <enrollment-key>.",
            ));
        }
        None => match prompter.choose_auth().await? {
            Some(AuthChoice::Login) => {
                if !resolved_endpoint.permits_stored_credentials {
                    return Err(legacy_endpoint_requires_explicit_authority(&endpoint));
                }
                match login_inline(CredentialStore::EnvFile).await? {
                    LoginContinuation::Authenticated(session) => LoginCredential {
                        token: SessionToken::new(session.access_token().to_string()),
                        credential_org: Some(session.org_name().to_string()),
                    },
                    LoginContinuation::Cancelled => {
                        telemetry.complete("cancelled");
                        return Ok(());
                    }
                }
            }
            Some(AuthChoice::EnrollmentKey) => {
                telemetry.auth("token");
                let Some(raw) = prompter.read_enrollment_key(&connect_portal_url()).await? else {
                    telemetry.complete("cancelled");
                    return Ok(());
                };
                let key = EnrollmentKey::parse(&raw).map_err(|source| Error::InvalidUsage {
                    message: source.to_string(),
                })?;
                telemetry.stage("enrollment");
                let enrolled = enroll(
                    ctx,
                    &config_dir,
                    &directory,
                    &resolved_endpoint,
                    request.region,
                    request.org.clone().unwrap_or_default(),
                    EnrollmentAuthority::Token {
                        key,
                        expected_org: request.org,
                    },
                )
                .await?;
                print_enrollment_result(&enrolled);
                telemetry.complete(if enrolled.identity.app_id.is_some() {
                    "already_attached"
                } else {
                    "unattached"
                });
                return Ok(());
            }
            None => {
                telemetry.complete("cancelled");
                return Ok(());
            }
        },
    };
    telemetry.auth("login");

    let management = CloudClient::with_token_for_org_at(
        login.token.expose_secret().to_string(),
        None,
        &endpoint,
    )?;
    telemetry.stage("organization_selection");
    let selected = match resolve_connect_organization_with_client(
        &management,
        request.org.as_deref(),
        login.credential_org.as_deref(),
        prompter.interactive(),
    )
    .await
    {
        Ok(OrgResolution::Selected(org)) => org,
        Ok(OrgResolution::Cancelled) => {
            telemetry.complete("cancelled");
            return Ok(());
        }
        Err(error)
            if request.org.is_none()
                && prompter.interactive()
                && error.cloud_code() == Some(CloudErrorCode::Forbidden) =>
        {
            eprintln!("{error}");
            eprintln!(
                "An owner or admin can supply an enrollment key instead; that path enrolls this instance without creating a project."
            );
            telemetry.auth("token");
            let Some(raw) = prompter.read_enrollment_key(&connect_portal_url()).await? else {
                telemetry.complete("cancelled");
                return Ok(());
            };
            let key = EnrollmentKey::parse(&raw).map_err(|source| Error::InvalidUsage {
                message: source.to_string(),
            })?;
            telemetry.stage("enrollment");
            let enrolled = enroll(
                ctx,
                &config_dir,
                &directory,
                &resolved_endpoint,
                request.region,
                request.org.clone().unwrap_or_default(),
                EnrollmentAuthority::Token {
                    key,
                    expected_org: request.org,
                },
            )
            .await?;
            print_enrollment_result(&enrolled);
            telemetry.complete(if enrolled.identity.app_id.is_some() {
                "already_attached"
            } else {
                "unattached"
            });
            return Ok(());
        }
        Err(error) => return Err(error),
    };

    telemetry.stage("enrollment");
    let enrolled = enroll(
        ctx,
        &config_dir,
        &directory,
        &resolved_endpoint,
        request.region,
        selected.name.clone(),
        EnrollmentAuthority::AuthenticatedSession {
            access_token: login.token.clone(),
            org: selected.name.clone(),
        },
    )
    .await?;

    if enrolled.already_enrolled && enrolled.identity.app_id.is_some() {
        print_attached(&enrolled.identity);
        telemetry.complete("already_attached");
        return Ok(());
    }

    telemetry.stage("project_assignment");
    let project =
        project_name_after_enrollment(request.project.as_deref(), &directory, prompter).await?;
    let Some(project) = project else {
        print_unattached(&enrolled.identity, enrolled.recovery_url.as_deref());
        telemetry.complete("unattached");
        return Ok(());
    };
    let assignment = assign_project(
        ProjectAssignmentContext {
            endpoint: &endpoint,
            config_dir: &config_dir,
            directory: &directory,
            token: &login.token,
            organization: &selected.name,
            identity: &enrolled.identity,
            recovery_url: enrolled.recovery_url.as_deref(),
        },
        project,
        prompter,
    )
    .await?;
    telemetry.complete(match assignment {
        ProjectAssignment::Attached => "attached",
        ProjectAssignment::Cancelled => "cancelled",
    });
    Ok(())
}

struct ExistingIdentityContext<'a> {
    config_dir: &'a Path,
    directory: &'a Path,
    identity_path: &'a Path,
    endpoint: &'a str,
    persist_endpoint_file: bool,
    permits_stored_credentials: bool,
    identity: Identity,
    pending_project: Option<ProjectOperation>,
}

async fn existing_identity_flow<P: Prompter>(
    request: &ConnectRequest,
    prompter: &mut P,
    context: ExistingIdentityContext<'_>,
    telemetry: &FlowTelemetry,
) -> Result<()> {
    let ExistingIdentityContext {
        config_dir,
        directory,
        identity_path,
        endpoint,
        persist_endpoint_file,
        permits_stored_credentials,
        identity,
        pending_project,
    } = context;
    if identity.app_id.is_some() {
        if let Some(asserted) = request.org.as_deref() {
            let stored = identity.org_name.as_deref().ok_or_else(|| {
                invalid_usage(
                    "the stored attachment has no organization metadata, so --org cannot be verified.",
                )
            })?;
            if !stored.eq_ignore_ascii_case(asserted) {
                return Err(invalid_usage(format!(
                    "this instance is already attached in organization {stored}; --org {asserted} does not match."
                )));
            }
        }
        if let Some(asserted) = request.project.as_deref() {
            let stored = identity.app_name.as_deref().ok_or_else(|| {
                invalid_usage(
                    "the stored attachment has no project metadata, so --project cannot be verified.",
                )
            })?;
            if stored != asserted {
                return Err(invalid_usage(format!(
                    "this instance is already attached to project {stored}; --project {asserted} does not match."
                )));
            }
        }
        if persist_endpoint_file {
            persist_endpoint(config_dir, endpoint).await?;
        }
        print_attached(&identity);
        telemetry.complete("already_attached");
        return Ok(());
    }

    let fixed_org = identity
        .org_name
        .as_deref()
        .filter(|org| !org.is_empty())
        .ok_or_else(|| Error::CloudConnectIo {
            message: format!(
                "the existing unattached identity at {} has no Cloud-provided organization metadata; update the runtime and reconnect before assigning a project",
                identity_path.display()
            ),
        })?;
    if let Some(asserted) = request.org.as_deref()
        && !asserted.eq_ignore_ascii_case(fixed_org)
    {
        return Err(invalid_usage(format!(
            "this instance is enrolled in organization {fixed_org}, not {asserted}. Run `spice connect remove` before enrolling it into another organization."
        )));
    }

    if let Some(pending) = pending_project.as_ref() {
        if !pending.organization.eq_ignore_ascii_case(fixed_org)
            || pending.request.instance_id != identity.identifier
        {
            return Err(Error::CloudConnectIo {
                message: "the pending project transaction does not match the enrolled identity"
                    .to_string(),
            });
        }
        if let Some(asserted) = request.project.as_deref()
            && asserted != pending.request.name
        {
            return Err(invalid_usage(format!(
                "project attachment for {} is already pending; retry that exact project name.",
                pending.request.name
            )));
        }
    }

    let explicit_project = pending_project
        .as_ref()
        .map(|operation| operation.request.name.as_str())
        .or(request.project.as_deref());
    if !prompter.interactive() {
        if explicit_project.is_none() {
            print_unattached(&identity, None);
            telemetry.complete("unattached");
            return Ok(());
        }
        if request.org.is_none() && pending_project.is_none() {
            return Err(invalid_usage(
                "non-interactive project setup requires both --org <org> and --project <name>.",
            ));
        }
    }

    if persist_endpoint_file {
        persist_endpoint(config_dir, endpoint).await?;
    }

    telemetry.stage("authentication");
    if !permits_stored_credentials {
        return Err(legacy_endpoint_requires_explicit_authority(endpoint));
    }
    let mut login = stored_user_login(endpoint, Some(fixed_org)).await?;
    if login.is_none() && prompter.interactive() {
        login = match login_inline(CredentialStore::EnvFile).await? {
            LoginContinuation::Authenticated(session) => Some(LoginCredential {
                token: SessionToken::new(session.access_token().to_string()),
                credential_org: Some(session.org_name().to_string()),
            }),
            LoginContinuation::Cancelled => {
                print_unattached(&identity, None);
                telemetry.complete("cancelled");
                return Ok(());
            }
        };
    }
    let Some(login) = login else {
        return Err(org_credential_missing(fixed_org));
    };
    if let Some(credential_org) = login.credential_org.as_deref()
        && !credential_org.eq_ignore_ascii_case(fixed_org)
    {
        return Err(org_credential_wrong_org(fixed_org, credential_org));
    }
    let token = login.token;
    telemetry.auth("login");
    if explicit_project.is_none() {
        let confirmed = prompter.confirm_project_assignment().await?;
        if confirmed != Some(true) {
            print_unattached(&identity, None);
            telemetry.complete(if confirmed.is_none() {
                "cancelled"
            } else {
                "unattached"
            });
            return Ok(());
        }
    }

    telemetry.stage("project_assignment");
    let project = project_name_after_enrollment(
        explicit_project,
        &super::instance_dir_for(config_dir),
        prompter,
    )
    .await?;
    let Some(project) = project else {
        print_unattached(&identity, None);
        telemetry.complete("cancelled");
        return Ok(());
    };
    let assignment = assign_project(
        ProjectAssignmentContext {
            endpoint,
            config_dir,
            directory,
            token: &token,
            organization: fixed_org,
            identity: &identity,
            recovery_url: None,
        },
        project,
        prompter,
    )
    .await?;
    telemetry.complete(match assignment {
        ProjectAssignment::Attached => "attached",
        ProjectAssignment::Cancelled => "cancelled",
    });
    Ok(())
}

async fn stored_user_login(
    endpoint: &str,
    org_hint: Option<&str>,
) -> Result<Option<LoginCredential>> {
    if let Some(org) = org_hint
        && let Some(token) = cloud_org::token_for_org(org)
    {
        return Ok(Some(LoginCredential {
            token: SessionToken::new(token),
            credential_org: Some(org.to_string()),
        }));
    }
    let token = cloud_org::default_token();
    let Some(token) = token else {
        return Ok(None);
    };
    let client = CloudClient::with_token_for_org_at(token.clone(), None, endpoint)?;
    let Some(context) = client.optional_user_auth_context().await? else {
        // Service-account credentials have no user auth context. Their
        // organization is fixed by the issuer and remains server-authorized.
        return Ok(Some(LoginCredential {
            token: SessionToken::new(token),
            credential_org: None,
        }));
    };
    if let Some(org) = org_hint
        && !context.org_name.eq_ignore_ascii_case(org)
    {
        return Err(org_credential_wrong_org(org, &context.org_name));
    }
    Ok(Some(LoginCredential {
        token: SessionToken::new(token),
        credential_org: (!context.org_name.is_empty()).then_some(context.org_name),
    }))
}

fn org_credential_wrong_org(requested: &str, actual: &str) -> Error {
    Error::cloud_with_hint(
        CloudErrorCode::OrgCredentialMissing,
        format!(
            "No Spice Cloud credential is stored for organization '{requested}'; your selected credential belongs to '{actual}'."
        ),
        format!(
            "Authenticate for it with 'spice cloud login pat --org {requested}' (or 'spice cloud login api --org {requested}' for automation)."
        ),
    )
}

fn org_credential_missing(org: &str) -> Error {
    Error::cloud_with_hint(
        CloudErrorCode::OrgCredentialMissing,
        format!("No Spice Cloud credential is stored for organization '{org}'."),
        format!(
            "Authenticate for it with 'spice cloud login pat --org {org}' (or 'spice cloud login api --org {org}' for automation), or set {}.",
            cloud_org::org_token_var(org)
        ),
    )
}

fn legacy_endpoint_requires_explicit_authority(endpoint: &str) -> Error {
    invalid_usage(format!(
        "the repository-local cloud-endpoint file selects {endpoint}, so Spice will not send a stored or newly-created login credential there without explicit authority. Re-run with --endpoint {endpoint}, set SPICE_CLOUD_ENDPOINT, or use --token <enrollment-key>."
    ))
}

async fn enroll(
    ctx: &RuntimeContext,
    config_dir: &Path,
    directory: &Path,
    endpoint: &TransactionEndpoint,
    region: Option<String>,
    journal_org: String,
    authority: EnrollmentAuthority,
) -> Result<EnrollmentResult> {
    if let Some(region) = region.as_deref()
        && !runtime_cloud_connect::is_valid_instance_region(region)
    {
        return Err(invalid_usage(format!(
            "invalid --region value '{region}': expected 2-64 lowercase letters, digits, and hyphens, starting and ending with a letter or digit."
        )));
    }
    let runtime_version = ctx
        .runtime_version()
        .unwrap_or_else(|_| crate::commands::version::cli_version());
    let mut config =
        CloudConnectConfig::from_env_at(runtime_version.clone(), config_dir.to_path_buf());
    config.enroll_endpoint.clone_from(&endpoint.value);
    if endpoint.persist_file {
        persist_endpoint(config_dir, &endpoint.value).await?;
    }
    let endpoint = endpoint.value.as_str();

    let enrollment_transaction = EnrollmentTransactionLock::acquire_async(config_dir)
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("acquire retry-safe enrollment transaction: {source}"),
        })?;

    let prepare_dir = config_dir.to_path_buf();
    let canonical_dir = directory.to_path_buf();
    let journal_endpoint = endpoint.to_string();
    let draft_binding = runtime_cloud_connect::EnrollmentRequestBinding {
        endpoint: endpoint.to_string(),
        authority: match &authority {
            EnrollmentAuthority::Token { expected_org, .. } => {
                runtime_cloud_connect::EnrollmentAuthorityBinding::Token {
                    expected_org: expected_org.clone(),
                }
            }
            EnrollmentAuthority::AuthenticatedSession { org, .. } => {
                runtime_cloud_connect::EnrollmentAuthorityBinding::AuthenticatedSession {
                    organization: org.clone(),
                }
            }
        },
    };
    let (enrollment_transaction, mut operation) = tokio::task::spawn_blocking(move || {
        let facts = InstanceFacts::gather(&runtime_version);
        let draft = enrollment_transaction
            .load_or_create(&facts, region.as_deref(), &draft_binding)
            .map_err(|source| Error::CloudConnectIo {
                message: format!("prepare retry-safe enrollment: {source}"),
            })?;
        let operation = ConnectOperation::prepare(
            &prepare_dir,
            &canonical_dir,
            &draft.enrollment_operation_id,
            &journal_org,
            &journal_endpoint,
            region.as_deref(),
        )
        .map_err(|error| state_error(&error))?;
        Ok::<_, Error>((enrollment_transaction, operation))
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("enrollment preparation task panicked: {source}"),
    })??;

    config.instance_region = operation.region.clone();
    let (identity, recovery_url, already_enrolled) =
        match runtime_cloud_connect::enroll::enroll_now_with_transaction(
            &config,
            &authority,
            RetryPolicy::INTERACTIVE,
            enrollment_transaction,
        )
        .await
        .map_err(|source| Error::CloudConnectEnroll {
            message: source.to_string(),
        })? {
            EnrollNowOutcome::AlreadyEnrolled { identity } => (identity, None, true),
            EnrollNowOutcome::Enrolled { identity, metadata } => {
                (identity, metadata.new_project_url, false)
            }
        };

    let finish_dir = config_dir.to_path_buf();
    let finish_identity = identity.clone();
    tokio::task::spawn_blocking(move || {
        operation
            .mark_enrolled(&finish_dir, &finish_identity)
            .map_err(|error| state_error(&error))?;
        ConnectOperation::delete(&finish_dir).map_err(|error| state_error(&error))
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("enrollment journal finalization task panicked: {source}"),
    })??;
    Ok(EnrollmentResult {
        identity,
        recovery_url,
        already_enrolled,
    })
}

async fn project_name_after_enrollment<P: Prompter>(
    explicit: Option<&str>,
    directory: &Path,
    prompter: &mut P,
) -> Result<Option<String>> {
    if let Some(name) = explicit {
        validate_project_name(name)
            .map_err(|reason| invalid_usage(format!("invalid --project value: {reason}.")))?;
        return Ok(Some(name.to_string()));
    }
    if !prompter.interactive() {
        return Err(invalid_usage(
            "non-interactive project setup requires both --org <org> and --project <name>.",
        ));
    }

    let suggestion = initial_suggestion(directory);
    loop {
        let Some(name) = prompter.project_name(&suggestion).await? else {
            return Ok(None);
        };
        match validate_project_name(&name) {
            Ok(()) => return Ok(Some(name)),
            Err(reason) => eprintln!("Project name {reason}."),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectAssignment {
    Attached,
    Cancelled,
}

struct ProjectAssignmentContext<'a> {
    endpoint: &'a str,
    config_dir: &'a Path,
    directory: &'a Path,
    token: &'a SessionToken,
    organization: &'a str,
    identity: &'a Identity,
    recovery_url: Option<&'a str>,
}

async fn assign_project<P: Prompter>(
    context: ProjectAssignmentContext<'_>,
    first_name: String,
    prompter: &mut P,
) -> Result<ProjectAssignment> {
    let client = ProjectClient::new(context.endpoint).map_err(|error| project_error(&error))?;
    let base = first_name.clone();
    let mut name = first_name;
    let mut collision_number = 2_u32;
    let started = Instant::now();
    let mut retry_attempt = 0_u32;

    loop {
        let operation = prepare_project_operation(&context, &name).await?;
        match client
            .create(context.token, context.organization, &operation.request)
            .await
        {
            Ok(attachment) => {
                persist_attachment(context.config_dir, &attachment).await?;
                delete_project_operation(context.config_dir).await?;
                println!(
                    "Spice Cloud Connect: connected to {} / {}",
                    attachment.organization, attachment.project_name
                );
                println!("Monitor: {}", attachment.monitor_url);
                return Ok(ProjectAssignment::Attached);
            }
            Err(error) if error.is_name_conflict() && prompter.interactive() => {
                delete_project_operation(context.config_dir).await?;
                eprintln!(
                    "A project named '{name}' already exists. Choose a new name; the existing project was not linked."
                );
                let suggestion = collision_suggestion(&base, collision_number);
                collision_number = collision_number.saturating_add(1);
                loop {
                    let Some(next) = prompter.project_name(&suggestion).await? else {
                        print_unattached(context.identity, context.recovery_url);
                        return Ok(ProjectAssignment::Cancelled);
                    };
                    match validate_project_name(&next) {
                        Ok(()) => {
                            name = next;
                            break;
                        }
                        Err(reason) => eprintln!("Project name {reason}."),
                    }
                }
                retry_attempt = 0;
            }
            Err(error) if error.is_name_conflict() => {
                delete_project_operation(context.config_dir).await?;
                print_unattached(context.identity, context.recovery_url);
                return Err(project_error(&error));
            }
            // The control plane knows this instance has an attachment, but
            // does not return enough state in this denial to persist it
            // locally. Never tell the operator it is unattached.
            Err(error) if error.is_already_attached() => return Err(project_error(&error)),
            Err(error)
                if (error.is_retryable() || error.is_attachment_ambiguous())
                    && retry_attempt + 1 < PROJECT_MAX_ATTEMPTS
                    && started.elapsed() < PROJECT_RETRY_DEADLINE =>
            {
                let exponential = 200_u64
                    .saturating_mul(1_u64.checked_shl(retry_attempt.min(4)).unwrap_or(u64::MAX));
                let window_ms = exponential.min(2_000);
                retry_attempt = retry_attempt.saturating_add(1);
                let delay = Duration::from_millis(rand::random_range(1..=window_ms));
                tokio::time::sleep(delay).await;
            }
            Err(error) if error.is_authoritative_non_mutation() => {
                delete_project_operation(context.config_dir).await?;
                print_unattached(context.identity, context.recovery_url);
                return Err(project_error(&error));
            }
            Err(error) => return Err(ambiguous_project_error(&error)),
        }
    }
}

async fn prepare_project_operation(
    context: &ProjectAssignmentContext<'_>,
    project_name: &str,
) -> Result<ProjectOperation> {
    let config_dir = context.config_dir.to_path_buf();
    let directory = context.directory.to_path_buf();
    let endpoint = context.endpoint.to_string();
    let organization = context.organization.to_string();
    let project_name = project_name.to_string();
    let request = ProjectMutation::signed(context.identity, context.organization, &project_name)
        .map_err(|error| project_error(&error))?;
    tokio::task::spawn_blocking(move || {
        ProjectOperation::prepare(&config_dir, &directory, &endpoint, &organization, request)
            .map_err(|error| state_error(&error))
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("project journal preparation task panicked: {source}"),
    })?
}

async fn delete_project_operation(config_dir: &Path) -> Result<()> {
    let config_dir = config_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        ProjectOperation::delete(&config_dir).map_err(|error| state_error(&error))
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("project journal cleanup task panicked: {source}"),
    })?
}

async fn persist_attachment(config_dir: &Path, attachment: &ProjectAttachment) -> Result<()> {
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let attachment = AppAttachment {
        app_id: attachment.project_id.to_string(),
        org_name: Some(attachment.organization.clone()),
        app_name: Some(attachment.project_name.clone()),
        monitor_url: Some(attachment.monitor_url.clone()),
    };
    tokio::task::spawn_blocking(move || {
        IdentityStore::set_attachment(&identity_path, Some(&attachment)).map_err(|source| {
            Error::CloudConnectIo {
                message: format!("persist project attachment: {source}"),
            }
        })
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("project attachment persistence task panicked: {source}"),
    })??
    .ok_or_else(|| Error::CloudConnectIo {
        message: "the enrolled identity disappeared before project attachment was persisted"
            .to_string(),
    })?;
    Ok(())
}

async fn load_identity(path: &Path) -> Result<Option<Identity>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking({
        let path = path.clone();
        move || IdentityStore::load_optional(&path)
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("identity load task panicked: {source}"),
    })?
    .map_err(|source| Error::CloudConnectIo {
        message: format!("load identity at {}: {source}", path.display()),
    })
}

fn validate_existing_identity(path: &Path, identity: &Identity) -> Result<()> {
    if let Some(reason) = identity.reconnect_validation_error() {
        return Err(Error::CloudConnectIo {
            message: format!(
                "the Cloud Connect identity at {} is unusable ({reason}); run `spice connect remove --yes` before enrolling again",
                path.display()
            ),
        });
    }
    if identity.is_expired() {
        return Err(Error::CloudConnectIo {
            message: format!(
                "the Cloud Connect identity at {} is expired; start spiced to renew it before assigning or reporting a project",
                path.display()
            ),
        });
    }
    for (field, value) in [
        ("organization", identity.org_name.as_deref()),
        ("project", identity.app_name.as_deref()),
    ] {
        if value.is_some_and(|value| value.chars().any(char::is_control)) {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "the Cloud Connect identity at {} has unsafe {field} metadata",
                    path.display()
                ),
            });
        }
    }
    if identity
        .monitor_url
        .as_deref()
        .is_some_and(|url| safe_recovery_url(url).is_none())
    {
        return Err(Error::CloudConnectIo {
            message: format!(
                "the Cloud Connect identity at {} has an unsafe monitor URL",
                path.display()
            ),
        });
    }
    Ok(())
}

async fn load_operations(
    config_dir: &Path,
) -> Result<(
    Option<runtime_cloud_connect::EnrollmentDraft>,
    Option<ConnectOperation>,
    Option<ProjectOperation>,
)> {
    let config_dir = config_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let draft = runtime_cloud_connect::EnrollmentDraft::load_optional(&config_dir).map_err(
            |source| Error::CloudConnectIo {
                message: format!("load enrollment draft: {source}"),
            },
        )?;
        let enrollment =
            ConnectOperation::load_optional(&config_dir).map_err(|error| state_error(&error))?;
        let project =
            ProjectOperation::load_optional(&config_dir).map_err(|error| state_error(&error))?;
        Ok::<_, Error>((draft, enrollment, project))
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("Cloud Connect journal load task panicked: {source}"),
    })?
}

fn resolve_transaction_endpoint(
    config_dir: &Path,
    explicit: Option<&str>,
    identity: Option<&Identity>,
    draft: Option<&runtime_cloud_connect::EnrollmentDraft>,
) -> Result<TransactionEndpoint> {
    let environment = std::env::var("SPICE_CLOUD_ENDPOINT").ok();
    resolve_transaction_endpoint_with_env(
        config_dir,
        explicit,
        environment.as_deref(),
        identity,
        draft,
    )
}

fn resolve_transaction_endpoint_with_env(
    config_dir: &Path,
    explicit: Option<&str>,
    environment: Option<&str>,
    identity: Option<&Identity>,
    draft: Option<&runtime_cloud_connect::EnrollmentDraft>,
) -> Result<TransactionEndpoint> {
    let requested = explicit
        .filter(|endpoint| !endpoint.is_empty())
        .or_else(|| environment.filter(|endpoint| !endpoint.is_empty()))
        .map(normalize_control_plane_endpoint)
        .transpose()?;
    let bound = identity
        .and_then(|identity| identity.control_plane_endpoint.as_deref())
        .or_else(|| draft.map(|draft| draft.binding.endpoint.as_str()))
        .map(normalize_control_plane_endpoint)
        .transpose()?;
    if let (Some(requested), Some(bound)) = (requested.as_deref(), bound.as_deref())
        && requested != bound
    {
        return Err(invalid_usage(format!(
            "--endpoint {requested} does not match this pending or enrolled instance's control plane {bound}."
        )));
    }
    if let Some(value) = requested {
        return Ok(TransactionEndpoint {
            value,
            persist_file: true,
            permits_stored_credentials: true,
        });
    }
    if let Some(value) = bound {
        return Ok(TransactionEndpoint {
            value,
            persist_file: true,
            permits_stored_credentials: true,
        });
    }

    let legacy = CloudConnectConfig::read_normalized_enroll_endpoint_override(config_dir).map_err(
        |source| Error::CloudConnectIo {
            message: source.to_string(),
        },
    )?;
    let permits_stored_credentials = legacy.is_none();
    Ok(TransactionEndpoint {
        value: legacy
            .unwrap_or_else(|| runtime_cloud_connect::config::DEFAULT_ENDPOINT.to_string()),
        // The legacy file already contains its operator-authored value, while
        // the compiled default needs no state file. Only an explicit request
        // or durable binding is eligible to create/replace this file.
        persist_file: false,
        permits_stored_credentials,
    })
}

async fn reconcile_journal(
    config_dir: &Path,
    directory: &Path,
    endpoint: &str,
    identity: Option<&Identity>,
) -> Result<()> {
    let config_dir = config_dir.to_path_buf();
    let directory = directory.to_path_buf();
    let endpoint = endpoint.to_string();
    let identity = identity.cloned();
    tokio::task::spawn_blocking(move || {
        ConnectOperation::reconcile(&config_dir, &directory, &endpoint, identity.as_ref())
            .map_err(|error| state_error(&error))
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("enrollment journal reconciliation task panicked: {source}"),
    })?
}

async fn reconcile_project_journal(
    config_dir: &Path,
    directory: &Path,
    endpoint: &str,
    identity: Option<&Identity>,
) -> Result<Option<ProjectOperation>> {
    let config_dir = config_dir.to_path_buf();
    let directory = directory.to_path_buf();
    let endpoint = endpoint.to_string();
    let identity = identity.cloned();
    tokio::task::spawn_blocking(move || {
        ProjectOperation::reconcile(&config_dir, &directory, &endpoint, identity.as_ref())
            .map_err(|error| state_error(&error))
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("project journal reconciliation task panicked: {source}"),
    })?
}

async fn persist_endpoint(config_dir: &Path, endpoint: &str) -> Result<()> {
    let path = config_dir.join(super::CLOUD_ENDPOINT_FILE);
    let endpoint = endpoint.to_string();
    tokio::task::spawn_blocking(move || {
        super::state::atomic_write_owner_only(&path, format!("{endpoint}\n").as_bytes())
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("endpoint persistence task panicked: {source}"),
    })?
    .map_err(|error| state_error(&error))
}

async fn canonical_instance_directory(explicit: Option<&Path>) -> Result<PathBuf> {
    let directory = match explicit {
        Some(directory) => directory.to_path_buf(),
        None => std::env::current_dir().map_err(|source| Error::CloudConnectIo {
            message: format!("resolve current instance directory: {source}"),
        })?,
    };
    let reported = directory.clone();
    tokio::task::spawn_blocking(move || std::fs::canonicalize(directory))
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("directory canonicalization task panicked: {source}"),
        })?
        .map_err(|source| Error::CloudConnectIo {
            message: format!(
                "canonicalize instance directory {}: {source}",
                reported.display()
            ),
        })
}

fn connect_portal_url() -> String {
    format!(
        "{}/connect",
        crate::commands::login::spice_base_url().trim_end_matches('/')
    )
}

fn normalize_control_plane_endpoint(endpoint: &str) -> Result<String> {
    runtime_cloud_connect::config::normalize_control_plane_endpoint(endpoint)
        .map_err(|source| invalid_usage(format!("invalid --endpoint: {source}.")))
}

#[cfg(test)]
fn validate_control_plane_endpoint(endpoint: &str) -> Result<()> {
    normalize_control_plane_endpoint(endpoint).map(|_| ())
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

fn print_unattached(identity: &Identity, recovery_url: Option<&str>) {
    let org = identity.org_name.as_deref().unwrap_or("Spice Cloud");
    println!("Spice Cloud Connect: connected to {org} — not yet attached to a project");
    let recovery_url = recovery_url
        .and_then(safe_recovery_url)
        .unwrap_or_else(connect_portal_url);
    println!("Create one: {recovery_url}");
}

fn print_enrollment_result(enrolled: &EnrollmentResult) {
    if enrolled.identity.app_id.is_some() {
        print_attached(&enrolled.identity);
    } else {
        print_unattached(&enrolled.identity, enrolled.recovery_url.as_deref());
    }
}

fn safe_recovery_url(candidate: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(candidate).ok()?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return None;
    }
    let local_http = parsed.scheme() == "http" && parsed.host_str().is_some_and(is_loopback_host);
    if parsed.scheme() != "https" && !local_http {
        return None;
    }
    Some(parsed.to_string())
}

fn preflight_request(request: &ConnectRequest) -> Result<()> {
    if request.token.is_some() && request.project.is_some() {
        return Err(invalid_usage(
            "--project cannot be used with --token; an enrollment key can enroll an instance but cannot create a project.",
        ));
    }
    if let Some(project) = request.project.as_deref() {
        validate_project_name(project)
            .map_err(|reason| invalid_usage(format!("invalid --project value: {reason}.")))?;
    }
    if let Some(org) = request.org.as_deref() {
        cloud_org::validate_org_name(org)?;
    }
    if let Some(region) = request.region.as_deref()
        && !runtime_cloud_connect::is_valid_instance_region(region)
    {
        return Err(invalid_usage(format!(
            "invalid --region value '{region}': expected 2-64 lowercase letters, digits, and hyphens, starting and ending with a letter or digit."
        )));
    }
    if let Some(endpoint) = request.endpoint.as_deref() {
        normalize_control_plane_endpoint(endpoint)?;
    }
    if request.endpoint.is_none()
        && let Ok(endpoint) = std::env::var("SPICE_CLOUD_ENDPOINT")
        && !endpoint.is_empty()
    {
        normalize_control_plane_endpoint(&endpoint)?;
    }
    Ok(())
}

fn print_attached(identity: &Identity) {
    let org = identity.org_name.as_deref().unwrap_or("Spice Cloud");
    let project = identity.app_name.as_deref().unwrap_or("attached project");
    println!("Spice Cloud Connect: connected to {org} / {project}");
    if let Some(url) = identity.monitor_url.as_deref() {
        println!("Monitor: {url}");
    }
}

fn invalid_usage(message: impl Into<String>) -> Error {
    Error::InvalidUsage {
        message: message.into(),
    }
}

fn state_error(source: &super::state::Error) -> Error {
    Error::CloudConnectIo {
        message: source.to_string(),
    }
}

fn project_error(source: &super::project::Error) -> Error {
    Error::CloudConnectProject {
        message: source.to_string(),
    }
}

fn ambiguous_project_error(source: &super::project::Error) -> Error {
    Error::CloudConnectProject {
        message: format!(
            "{source}. The attachment result is unknown because the server may have committed the request before the response failed. Retry the exact same command to reconcile it; do not create another project."
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ScriptedPrompter {
        interactive: bool,
        auth: Option<AuthChoice>,
        key: Option<String>,
        confirm: Option<bool>,
        names: std::collections::VecDeque<Option<String>>,
    }

    impl Prompter for ScriptedPrompter {
        fn interactive(&self) -> bool {
            self.interactive
        }

        async fn choose_auth(&mut self) -> Result<Option<AuthChoice>> {
            Ok(self.auth.take())
        }

        async fn read_enrollment_key(&mut self, _portal_url: &str) -> Result<Option<String>> {
            Ok(self.key.take())
        }

        async fn confirm_project_assignment(&mut self) -> Result<Option<bool>> {
            Ok(self.confirm.take())
        }

        async fn project_name(&mut self, _suggestion: &str) -> Result<Option<String>> {
            Ok(self.names.pop_front().flatten())
        }
    }

    #[tokio::test]
    async fn scripted_auth_choice_has_exact_two_paths() {
        let mut prompt = ScriptedPrompter {
            interactive: true,
            auth: Some(AuthChoice::Login),
            key: None,
            confirm: None,
            names: std::collections::VecDeque::new(),
        };
        assert_eq!(
            prompt.choose_auth().await.expect("choice"),
            Some(AuthChoice::Login)
        );
        prompt.auth = Some(AuthChoice::EnrollmentKey);
        assert_eq!(
            prompt.choose_auth().await.expect("choice"),
            Some(AuthChoice::EnrollmentKey)
        );
    }

    #[tokio::test]
    async fn cancellation_and_eof_are_clean_prompt_outcomes() {
        let mut prompt = ScriptedPrompter {
            interactive: true,
            auth: None,
            key: None,
            confirm: None,
            names: std::collections::VecDeque::from([None]),
        };
        assert_eq!(prompt.choose_auth().await.expect("cancel"), None);
        assert_eq!(
            prompt.project_name("steady-spice").await.expect("eof"),
            None
        );
    }

    #[test]
    fn terminal_prompt_interrupt_and_eof_map_to_clean_cancellation() {
        for kind in [
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::UnexpectedEof,
        ] {
            let optional = map_optional_prompt::<usize>(
                Err(dialoguer::Error::IO(std::io::Error::from(kind))),
                "test prompt",
            )
            .expect("cancellation is not an error");
            assert_eq!(optional, None);
            let required = map_required_prompt::<String>(
                Err(dialoguer::Error::IO(std::io::Error::from(kind))),
                "test prompt",
            )
            .expect("cancellation is not an error");
            assert_eq!(required, None);
        }
    }

    #[test]
    fn telemetry_vocabulary_contains_no_customer_values() {
        let telemetry = FlowTelemetry::new();
        telemetry.auth("login");
        telemetry.stage("project_assignment");
        assert_eq!(telemetry.auth_path.get(), "login");
        assert_eq!(telemetry.failure_stage.get(), "project_assignment");
    }

    #[test]
    fn control_plane_endpoint_requires_https_except_for_loopback_fixtures() {
        for valid in [
            "https://api.spice.ai",
            "https://cloud.example.test/api",
            "http://127.0.0.1:8090",
            "http://[::1]:8090",
            "http://localhost:8090",
        ] {
            validate_control_plane_endpoint(valid).expect("valid control-plane URL");
        }
        for invalid in [
            "not-a-url",
            "http://cloud.example.test",
            "ftp://127.0.0.1/file",
            "https://user:secret@cloud.example.test",
            "https://cloud.example.test?token=secret",
            "https://cloud.example.test#fragment",
        ] {
            validate_control_plane_endpoint(invalid).expect_err("unsafe URL must fail");
        }
    }

    #[test]
    fn explicit_project_and_region_are_validated_before_mutation() {
        let request = ConnectRequest {
            org: Some("acme".to_string()),
            project: Some("INVALID PROJECT".to_string()),
            token: None,
            region: Some("lab-seoul".to_string()),
            dir: None,
            endpoint: None,
        };
        assert!(preflight_request(&request).is_err());

        let request = ConnectRequest {
            project: Some("valid-project".to_string()),
            region: Some("INVALID_REGION".to_string()),
            ..request
        };
        assert!(preflight_request(&request).is_err());
    }

    #[test]
    fn fresh_transactions_use_the_compiled_endpoint_without_a_binding() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fresh = resolve_transaction_endpoint_with_env(dir.path(), None, None, None, None)
            .expect("resolve fresh endpoint");
        assert_eq!(fresh.value, runtime_cloud_connect::config::DEFAULT_ENDPOINT);
        assert!(!fresh.persist_file);
        assert!(fresh.permits_stored_credentials);
    }

    #[test]
    fn fresh_transactions_honor_but_do_not_rewrite_the_legacy_endpoint() {
        let dir = tempfile::tempdir().expect("create tempdir");
        std::fs::write(
            dir.path().join("cloud-endpoint"),
            "https://private.example/\n",
        )
        .expect("write endpoint override");
        let resolved = resolve_transaction_endpoint_with_env(dir.path(), None, None, None, None)
            .expect("resolve legacy endpoint");
        assert_eq!(resolved.value, "https://private.example");
        assert!(!resolved.persist_file);
        assert!(!resolved.permits_stored_credentials);
    }

    #[test]
    fn unsafe_cloud_recovery_urls_are_never_opened_or_printed() {
        assert_eq!(
            safe_recovery_url("https://spice.ai/connect").as_deref(),
            Some("https://spice.ai/connect")
        );
        assert_eq!(
            safe_recovery_url("http://127.0.0.1:8080/connect").as_deref(),
            Some("http://127.0.0.1:8080/connect")
        );
        for unsafe_url in [
            "javascript:alert(1)",
            "http://attacker.example/connect",
            "https://user:secret@spice.ai/connect",
        ] {
            assert_eq!(safe_recovery_url(unsafe_url), None);
        }
    }
}
