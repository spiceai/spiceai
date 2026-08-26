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

//! The resumable `spice cloud link` enrollment and project transaction.

use std::cell::Cell;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use dialoguer::Password;
use dialoguer::theme::ColorfulTheme;
use runtime_cloud_connect::enroll::{
    EnrollNowOutcome, EnrollmentAuthority, InstanceFacts, RetryPolicy, SessionToken,
};
use runtime_cloud_connect::enrollment_key::EnrollmentKey;
use runtime_cloud_connect::identity::{AppAttachment, Identity, IdentityStore};
use runtime_cloud_connect::{CloudConnectConfig, EnrollmentTransactionLock};
use zeroize::Zeroizing;

use crate::commands::cloud::org as cloud_org;
use crate::context::RuntimeContext;
use crate::error::{CloudErrorCode, Error, Result};

use super::project::{ProjectAttachment, ProjectClient, ProjectMutation};
use super::state::{ConnectOperation, IdentityFacts, OrganizationBinding, ProjectOperation};

pub(crate) struct ConnectRequest {
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

/// How the control plane for this transaction was chosen. Both downstream
/// rules are functions of this, so the source is carried instead of the
/// derived flags — that keeps "persist a file we were never given" and
/// "withhold a credential we are allowed to send" unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointSource {
    /// `--endpoint` or `SPICE_CLOUD_ENDPOINT`.
    Explicit,
    /// The durable identity or pending draft binding.
    Bound,
    /// The operator-authored instance-local `cloud-endpoint` file.
    LegacyFile,
    /// No binding, no override: the compiled-in public control plane.
    CompiledDefault,
}

#[derive(Debug)]
struct TransactionEndpoint {
    value: String,
    source: EndpointSource,
}

impl TransactionEndpoint {
    /// Only an explicit request or a durable binding is eligible to create or
    /// replace the endpoint file: the legacy file already holds its own value,
    /// and the compiled default needs no state file at all.
    fn persist_file(&self) -> bool {
        matches!(
            self.source,
            EndpointSource::Explicit | EndpointSource::Bound
        )
    }

    /// A repo-local `cloud-endpoint` file selects a destination but does not
    /// authorize sending any bearer credential to it.
    fn permits_credentials(&self) -> bool {
        !matches!(self.source, EndpointSource::LegacyFile)
    }
}

const PROJECT_RETRY_DEADLINE: Duration = Duration::from_secs(30);
const PROJECT_MAX_ATTEMPTS: u32 = 5;

trait Prompter {
    fn interactive(&self) -> bool;
    async fn read_enrollment_key(&mut self, portal_url: &str) -> Result<Option<Zeroizing<String>>>;
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

    async fn read_enrollment_key(&mut self, portal_url: &str) -> Result<Option<Zeroizing<String>>> {
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
        map_required_prompt(result, "enrollment key").map(|value| value.map(Zeroizing::new))
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

/// The same interrupt/EOF-to-cancellation mapping for a prompt whose success
/// value is unconditional.
fn map_required_prompt<T>(result: dialoguer::Result<T>, what: &str) -> Result<Option<T>> {
    map_optional_prompt(result.map(Some), what)
}

struct LoginCredential {
    token: SessionToken,
}

struct FlowTelemetry {
    auth_path: Cell<&'static str>,
    completion: Cell<&'static str>,
    failure_stage: Cell<&'static str>,
    cancelled: Option<Arc<AtomicBool>>,
}

impl FlowTelemetry {
    fn new(cancelled: Option<Arc<AtomicBool>>) -> Self {
        Self {
            auth_path: Cell::new("unknown"),
            completion: Cell::new("failed"),
            failure_stage: Cell::new("initialization"),
            cancelled,
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
        if outcome == "cancelled"
            && let Some(cancelled) = self.cancelled.as_ref()
        {
            cancelled.store(true, Ordering::Release);
        }
    }
}

impl Drop for FlowTelemetry {
    fn drop(&mut self) {
        tracing::debug!(
            target: "spice_cli::connect",
            auth_path = self.auth_path.get(),
            completion = self.completion.get(),
            failure_stage = self.failure_stage.get(),
            "Spice Cloud link flow completed"
        );
    }
}

pub(crate) async fn execute(
    ctx: &RuntimeContext,
    mut request: ConnectRequest,
) -> Result<Option<PathBuf>> {
    // Resolve the instance directory once before the transaction. A caller may
    // spell it through a symlink, but retargeting that symlink after enrollment
    // must never redirect later state writes.
    let directory = canonical_instance_directory(request.dir.as_deref()).await?;
    request.dir = Some(directory.clone());
    let cancelled = Arc::new(AtomicBool::new(false));
    let result = execute_with_inner(
        ctx,
        request,
        &mut TerminalPrompter::new(),
        Some(Arc::clone(&cancelled)),
    )
    .await;
    result?;
    Ok((!cancelled.load(Ordering::Acquire)).then_some(directory))
}

#[cfg(test)]
async fn execute_with<P: Prompter>(
    ctx: &RuntimeContext,
    request: ConnectRequest,
    prompter: &mut P,
) -> Result<()> {
    execute_with_inner(ctx, request, prompter, None).await
}

async fn execute_with_inner<P: Prompter>(
    ctx: &RuntimeContext,
    mut request: ConnectRequest,
    prompter: &mut P,
    cancelled: Option<Arc<AtomicBool>>,
) -> Result<()> {
    let telemetry = FlowTelemetry::new(cancelled);
    preflight_request(&request)?;
    telemetry.stage("local_state");
    let directory = canonical_instance_directory(request.dir.as_deref()).await?;
    let config_dir = CloudConnectConfig::resolve_config_dir(Some(&directory));
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    // A pending draft names the authority its operation already committed to,
    // so it decides what a resumed run needs — and says so in terms of that
    // operation. This guard stays a cheap pre-lock probe and defers to
    // [`resumable_authority`] once the draft can be read under the lock.
    //
    // Only a *definitive* absence takes the shortcut. A probe that cannot answer
    // — a permission or metadata failure — must not be read as "nothing is
    // pending": that would answer a protected retry state with generic
    // fresh-enrollment guidance. Deferring instead reaches the draft loader under
    // the lock, which is the layer that reports the real I/O failure.
    let draft_pending = !matches!(
        tokio::fs::try_exists(runtime_cloud_connect::EnrollmentDraft::path_in(&config_dir)).await,
        Ok(false)
    );
    let identity_present = tokio::fs::try_exists(&identity_path).await.unwrap_or(true);
    if !prompter.interactive()
        && request.token.is_none()
        && (request.org.is_none() || request.project.is_none())
        && !identity_present
        && !draft_pending
    {
        return Err(invalid_usage(
            "non-interactive Cloud Connect requires either a login with --org <org> --project <name>, or --token <enrollment-key>.",
        ));
    }
    // Serialize the complete CLI mutation with service lifecycle actions,
    // cloud-dispatched removal, and delivered-secret cache writes. This is the
    // outer lock; the runtime lease and enrollment transaction are acquired
    // only after it, preserving the shared lock order.
    let mutation_lock = runtime_cloud_connect::MutationLock::acquire(&config_dir, "connect")
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("acquire Cloud Connect state for enrollment: {source}"),
        })?;
    // The guard pins the directory that was actually locked. From here on,
    // derive every state path from it so retargeting a symlink used by the
    // caller cannot redirect the operation after acquisition.
    mutation_lock
        .ensure_directory_stable()
        .map_err(|source| Error::CloudConnectIo {
            message: format!("validate locked Cloud Connect state: {source}"),
        })?;
    let config_dir = mutation_lock
        .descriptor_relative_config_dir()
        .map_err(|source| Error::CloudConnectIo {
            message: format!("pin locked Cloud Connect state: {source}"),
        })?;
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let _runtime_lock =
        runtime_cloud_connect::RuntimeLock::acquire(&config_dir).map_err(|source| {
            Error::CloudConnectIo {
                message: format!(
                    "{source} Stop the running instance before using `spice cloud link`."
                ),
            }
        })?;
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
            ExistingIdentityContext {
                config_dir: &config_dir,
                directory: &directory,
                identity_path: &identity_path,
                endpoint: &endpoint,
                persist_endpoint_file: resolved_endpoint.persist_file(),
                permits_stored_credentials: resolved_endpoint.permits_credentials(),
                identity,
                pending_project: project_operation,
            },
            &telemetry,
        )
        .await;
    }

    // The pending draft, if any, decides the authority: the cloud may already
    // hold its operation, so only the mode that published it can replay the
    // operation ID and key material instead of creating a sibling instance.
    //
    // This read is not under [`EnrollmentTransactionLock`], and deliberately so:
    // what follows it is a credential prompt, and holding the enrollment
    // transaction across unbounded human input would block every other
    // enrollment in this directory — including a `spiced --token` bootstrap — for
    // as long as an operator leaves a prompt open. The decision is therefore a
    // *routing* decision, never the safety boundary. Another process may finish,
    // replace, or abandon the operation while this one is prompting; the
    // authoritative re-check happens under the transaction lock inside
    // `enroll`, where a durable identity answers `AlreadyEnrolled` and a changed
    // binding fails closed with `RequestBindingMismatch`, leaving the state the
    // other process wrote intact. The cost of losing that race is a prompt
    // answered for nothing, not a wrong enrollment.
    let resumable = draft
        .as_ref()
        .map(|draft| resumable_authority(draft, &request, prompter.interactive()))
        .transpose()?;
    // The operation the decision above was made for. Carried so the enrollment
    // transaction can refuse an operation this run never routed on.
    let resumed_operation = resumable
        .as_ref()
        .and(draft.as_ref())
        .map(|draft| draft.enrollment_operation_id.clone());
    // A resumed operation replays the location recorded when it was published:
    // the enrollment request is built from the draft, and the draft deliberately
    // keeps its region so a differing `--region` on a retry cannot invalidate
    // exact-replay state. The journal has to record that same value or it
    // describes a request that was never sent — and then rejects the retry that
    // names the region the operation actually carries.
    let region = match resumable.as_ref().and(draft.as_ref()) {
        Some(pending) => {
            if let Some(requested) = request.region.as_deref()
                && pending.region.as_deref() != Some(requested)
            {
                println!(
                    "The pending enrollment declares location {}, which is what it replays; --region {requested} applies to a new enrollment.",
                    pending.region.as_deref().unwrap_or("(none)")
                );
            }
            pending.region.clone()
        }
        None => request.region.clone(),
    };
    let token = request.token.take();

    let key_enrollment =
        |expected_org: Option<String>, resumed_operation: Option<String>| KeyEnrollment {
            ctx,
            config_dir: &config_dir,
            directory: &directory,
            endpoint: &resolved_endpoint,
            region: region.clone(),
            expected_org,
            resumed_operation,
        };

    match resumable {
        Some(ResumableAuthority::EnrollmentKey { expected_org }) => {
            // The enrollment key is bearer material that is deliberately never
            // persisted, so resuming asks for a current one — and only for
            // that. Offering the authentication chooser here would offer an
            // authority this operation cannot be finished under.
            if !resolved_endpoint.permits_credentials() {
                return Err(legacy_endpoint_requires_explicit_authority(&endpoint));
            }
            telemetry.auth("token");
            let key = if let Some(key) = token {
                key
            } else {
                println!(
                    "Resuming the pending enrollment for this directory. Enrollment keys are never stored, so this needs a current one."
                );
                let Some(raw) = prompter.read_enrollment_key(&connect_portal_url()).await? else {
                    telemetry.complete("cancelled");
                    return Ok(());
                };
                EnrollmentKey::parse(raw.as_str()).map_err(|source| Error::InvalidUsage {
                    message: source.to_string(),
                })?
            };
            return enroll_with_key(
                key_enrollment(expected_org, resumed_operation),
                key,
                &telemetry,
            )
            .await;
        }
        Some(ResumableAuthority::Login { organization }) => {
            return resume_pending_login(
                PendingLogin {
                    ctx,
                    config_dir: &config_dir,
                    directory: &directory,
                    endpoint: &resolved_endpoint,
                    region,
                    organization,
                    resumed_operation,
                },
                &request,
                &telemetry,
            )
            .await;
        }
        None => {}
    }

    if let Some(key) = token {
        // A key cannot create a project, in any mode. This sits below the draft
        // decision so that a pending operation answers first: a login-mode
        // operation asked to finish with a key names the command that finishes it
        // and the one that abandons it, which is more use than a rule about two
        // flags.
        if request.project.is_some() {
            return Err(invalid_usage(
                "--project cannot be used with --token; an enrollment key can enroll an instance but cannot create a project.",
            ));
        }
        if !resolved_endpoint.permits_credentials() {
            return Err(legacy_endpoint_requires_explicit_authority(&endpoint));
        }
        return enroll_with_key(key_enrollment(request.org.clone(), None), key, &telemetry).await;
    }

    if !prompter.interactive() && (request.org.is_none() || request.project.is_none()) {
        return Err(invalid_usage(
            "non-interactive Cloud Connect requires either a login with --org <org> --project <name>, or --token <enrollment-key>.",
        ));
    }

    telemetry.stage("authentication");
    let Some(login) = (if resolved_endpoint.permits_credentials() {
        stored_user_login(&endpoint, request.org.as_deref()).await?
    } else {
        None
    }) else {
        if !resolved_endpoint.permits_credentials() {
            return Err(legacy_endpoint_requires_explicit_authority(&endpoint));
        }
        return Err(org_credential_missing(
            request.org.as_deref().unwrap_or("the target organization"),
        ));
    };
    telemetry.auth("login");

    let selected = request.org.clone().ok_or_else(|| {
        invalid_usage("Cloud linking requires an explicit organization and project.")
    })?;

    enroll_with_login(
        LoginEnrollment {
            ctx,
            config_dir: &config_dir,
            directory: &directory,
            endpoint: &resolved_endpoint,
            region,
            organization: selected,
            login,
            resumed_operation: None,
        },
        &request,
        &telemetry,
    )
    .await
}

/// The authority a pending enrollment draft already committed to, reconciled
/// with this invocation's explicit inputs.
#[derive(Debug, PartialEq, Eq)]
enum ResumableAuthority {
    /// An enrollment-key operation. The key itself is bearer material that is
    /// never persisted, so a resume supplies a current one; the operation ID,
    /// key material, and organization assertion come from the draft.
    EnrollmentKey { expected_org: Option<String> },
    /// A login-session operation, bound to one organization for its lifetime.
    Login { organization: String },
}

/// Decide what can finish the pending operation `draft` describes.
///
/// The draft is authoritative. Spice Cloud may already hold its operation under
/// the persisted ID, so the authority that published it is the only one that
/// can replay it — which is why a resume never asks the operator to choose an
/// authentication mode again. Explicit inputs that would move the operation to
/// another authority, or that cannot finish it at all, fail here: before any
/// prompt, credential, or request, with the draft left on disk so the command
/// the error names can still replay it exactly.
fn resumable_authority(
    draft: &runtime_cloud_connect::EnrollmentDraft,
    request: &ConnectRequest,
    interactive: bool,
) -> Result<ResumableAuthority> {
    match &draft.binding.authority {
        runtime_cloud_connect::EnrollmentAuthorityBinding::Token { expected_org } => {
            if request.project.is_some() {
                return Err(pending_operation_conflict(
                    "was authorized by an enrollment key, which cannot create a project",
                    "--project",
                    "re-run without --project; attach a project from the Spice Cloud portal, or after the pending enrollment finishes",
                ));
            }
            if !interactive && request.token.is_none() {
                return Err(invalid_usage(
                    "this directory has a pending enrollment that was authorized by an enrollment key. Enrollment keys are never stored, so finishing it non-interactively requires --token <enrollment-key>.",
                ));
            }
            // `--org` on a resume corrects the assertion rather than
            // contradicting it. Spice Cloud checks the assertion before it
            // consumes the key, so an operation that failed on a mistyped one is
            // still replayable — and refusing the correction here would strand the
            // draft with no way to finish it but abandoning it. A flag naming the
            // organization the draft already asserts is not a correction, and
            // replays the draft's own value.
            let expected_org = match (request.org.as_deref(), expected_org.as_deref()) {
                (Some(requested), Some(pending)) if requested.eq_ignore_ascii_case(pending) => {
                    expected_org.clone()
                }
                (Some(requested), _) => Some(requested.to_string()),
                (None, _) => expected_org.clone(),
            };
            Ok(ResumableAuthority::EnrollmentKey { expected_org })
        }
        runtime_cloud_connect::EnrollmentAuthorityBinding::AuthenticatedSession {
            organization,
        } => {
            // The public CLI is interactive-only: a rerun resumes the durable
            // operation and asks for the missing project choice. The explicit
            // force command is the only local-abandon path.
            let finish = "re-run `spice cloud link` interactively to finish it, or run `spice cloud unlink` to abandon it explicitly";
            if request.token.is_some() {
                return Err(pending_operation_conflict(
                    &format!("was authorized by a login to organization {organization}"),
                    "--token",
                    finish,
                ));
            }
            if let Some(requested) = request.org.as_deref()
                && !requested.eq_ignore_ascii_case(organization)
            {
                return Err(pending_operation_conflict(
                    &format!("is bound to organization {organization}"),
                    &format!("--org {requested}"),
                    finish,
                ));
            }
            if !interactive && request.project.is_none() {
                return Err(invalid_usage(format!(
                    "this directory has a pending enrollment for organization {organization}. Finishing it non-interactively requires --project <name>."
                )));
            }
            Ok(ResumableAuthority::Login {
                organization: organization.clone(),
            })
        }
    }
}

/// One message shape for every way an explicit input contradicts the pending
/// operation: what the operation is, what contradicts it, and what to do —
/// never a silent switch to the requested authority, and never a discarded
/// draft.
fn pending_operation_conflict(pending: &str, requested: &str, remedy: &str) -> Error {
    invalid_usage(format!(
        "this directory has a pending enrollment that {pending}, so {requested} cannot be applied to it. The exact-replay state was preserved: {remedy}."
    ))
}

/// Everything an enrollment-key attempt needs beyond the key itself.
struct KeyEnrollment<'a> {
    ctx: &'a RuntimeContext,
    config_dir: &'a Path,
    directory: &'a Path,
    endpoint: &'a TransactionEndpoint,
    region: Option<String>,
    /// The operation a resumed run routed on, verified under the enrollment
    /// transaction before the key is spent. `None` for a fresh enrollment,
    /// which has no operation to hold to.
    resumed_operation: Option<String>,
    /// The organization this attempt asserts the key belongs to, checked
    /// against the enrollment response. `None` asserts nothing.
    expected_org: Option<String>,
}

/// Enroll with an enrollment key and report the outcome.
async fn enroll_with_key(
    context: KeyEnrollment<'_>,
    key: EnrollmentKey,
    telemetry: &FlowTelemetry,
) -> Result<()> {
    telemetry.auth("token");
    telemetry.stage("enrollment");
    let expected_org = context.expected_org;
    let enrolled = enroll(EnrollAttempt {
        ctx: context.ctx,
        config_dir: context.config_dir,
        directory: context.directory,
        endpoint: context.endpoint,
        region: context.region,
        journal_org: expected_org.clone().unwrap_or_default(),
        authority: EnrollmentAuthority::Token { key, expected_org },
        resumed_operation: context.resumed_operation,
    })
    .await?;
    print_enrollment_result(&enrolled);
    telemetry.complete(if enrolled.identity.app_id.is_some() {
        "already_attached"
    } else {
        "unattached"
    });
    Ok(())
}

/// A login-authorized enrollment and the project assignment that follows it.
struct LoginEnrollment<'a> {
    ctx: &'a RuntimeContext,
    config_dir: &'a Path,
    directory: &'a Path,
    endpoint: &'a TransactionEndpoint,
    /// The location this enrollment declares. A resumed operation carries the
    /// one its draft recorded, not this invocation's flag.
    region: Option<String>,
    organization: String,
    login: LoginCredential,
    /// As [`KeyEnrollment::resumed_operation`].
    resumed_operation: Option<String>,
}

/// Enroll under a login session, then attach the explicitly selected project.
async fn enroll_with_login(
    context: LoginEnrollment<'_>,
    request: &ConnectRequest,
    telemetry: &FlowTelemetry,
) -> Result<()> {
    telemetry.stage("enrollment");
    let enrolled = enroll(EnrollAttempt {
        ctx: context.ctx,
        config_dir: context.config_dir,
        directory: context.directory,
        endpoint: context.endpoint,
        region: context.region,
        journal_org: context.organization.clone(),
        authority: EnrollmentAuthority::AuthenticatedSession {
            access_token: context.login.token.clone(),
            org: context.organization.clone(),
        },
        resumed_operation: context.resumed_operation,
    })
    .await?;

    if enrolled.already_enrolled && enrolled.identity.app_id.is_some() {
        print_attached(&enrolled.identity);
        telemetry.complete("already_attached");
        return Ok(());
    }

    telemetry.stage("project_assignment");
    let project = request.project.clone().ok_or_else(|| {
        invalid_usage("`spice cloud link` requires an existing project selected by name.")
    })?;
    assign_project(
        ProjectAssignmentContext {
            endpoint: &context.endpoint.value,
            config_dir: context.config_dir,
            directory: context.directory,
            token: &context.login.token,
            organization: &context.organization,
            identity: &enrolled.identity,
            recovery_url: enrolled.recovery_url.as_deref(),
            rollback_enrollment_on_refusal: !enrolled.already_enrolled,
        },
        project,
    )
    .await?;
    telemetry.complete("attached");
    Ok(())
}

/// The pending login-mode operation this directory must finish.
struct PendingLogin<'a> {
    ctx: &'a RuntimeContext,
    config_dir: &'a Path,
    directory: &'a Path,
    endpoint: &'a TransactionEndpoint,
    /// As [`LoginEnrollment::region`].
    region: Option<String>,
    organization: String,
    /// As [`KeyEnrollment::resumed_operation`].
    resumed_operation: Option<String>,
}

/// The credential a resumed login operation presents.
///
/// The organization-bound credential is preferred; a default credential is used
/// as it is, without asking the control plane which organization it belongs to.
/// That identification is the `/api/spice-cli/auth` route, which a split-origin
/// deployment serves from the portal rather than the control plane — requesting
/// it against the pending operation's enrollment endpoint answers 404 there and
/// is not needed anywhere: the enroll request names the bound organization, so
/// Spice Cloud refuses a credential that is not entitled to it, and the
/// response organization is checked against the binding before any identity is
/// promoted. A wrong-organization credential therefore fails closed rather than
/// enrolling somewhere the operator did not ask for.
fn stored_resume_credential(organization: &str) -> Option<LoginCredential> {
    cloud_org::token_for_org(organization)
        .or_else(cloud_org::default_token)
        .map(|token| LoginCredential {
            token: SessionToken::new(token),
        })
}

/// Finish a pending login-mode operation under the organization it is bound to.
///
/// The authentication chooser is deliberately absent: an enrollment key would
/// publish a different authority than the operation Spice Cloud may already
/// hold. Organization discovery is equally absent — the binding already chose
/// the organization, so there is nothing to resolve and no way for a listing to
/// retarget the pending operation. Spice Cloud re-validates the owner/admin role
/// before the enrollment commits, as it does for the run that published the
/// draft.
async fn resume_pending_login(
    context: PendingLogin<'_>,
    request: &ConnectRequest,
    telemetry: &FlowTelemetry,
) -> Result<()> {
    let endpoint = context.endpoint.value.as_str();
    if !context.endpoint.permits_credentials() {
        return Err(legacy_endpoint_requires_explicit_authority(endpoint));
    }
    // Nothing else would explain the absent chooser or where this run is going.
    println!(
        "Resuming the pending enrollment for organization {}.",
        context.organization
    );
    telemetry.stage("authentication");
    let Some(login) = stored_resume_credential(&context.organization) else {
        return Err(org_credential_missing(&context.organization));
    };
    telemetry.auth("login");

    enroll_with_login(
        LoginEnrollment {
            ctx: context.ctx,
            config_dir: context.config_dir,
            directory: context.directory,
            endpoint: context.endpoint,
            region: context.region,
            organization: context.organization,
            login,
            resumed_operation: context.resumed_operation,
        },
        request,
        telemetry,
    )
    .await
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

async fn existing_identity_flow(
    request: &ConnectRequest,
    context: ExistingIdentityContext<'_>,
    telemetry: &FlowTelemetry,
) -> Result<()> {
    // An enrolled instance ignores an enrollment key — the identity wins — so
    // accepting one here would take a secret from the operator, drop it, and then
    // attach the project with a stored login credential instead. Moving the flag
    // rule below the draft decision left this path unguarded; it is invalid in
    // every mode, so it is refused wherever it is reachable.
    if request.token.is_some() && request.project.is_some() {
        return Err(invalid_usage(
            "--project cannot be used with --token; an enrollment key can enroll an instance but cannot create a project.",
        ));
    }

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

    if identity.is_expired() {
        // Only the runtime owns identity renewal. Linking never starts it, so
        // refuse before any project mutation and let the operator renew
        // explicitly.
        telemetry.complete("renewal_required");
        return Err(Error::cloud_with_hint(
            CloudErrorCode::InvalidRequest,
            "The enrolled instance identity is outside its validity period, so the project link was not changed.",
            "Run `spice run` so Spice Cloud can renew the identity, stop the runtime, then retry `spice cloud link`.",
        ));
    }

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
            "this instance is enrolled in organization {fixed_org}, not {asserted}. Run `spice cloud unlink` before enrolling it into another organization."
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
            && asserted != pending.project_name
        {
            return Err(invalid_usage(format!(
                "project attachment for {} is already pending; retry that exact project name.",
                pending.project_name
            )));
        }
    }

    let explicit_project = pending_project
        .as_ref()
        .map(|operation| operation.project_name.as_str())
        .or(request.project.as_deref());
    let project = explicit_project.ok_or_else(|| {
        invalid_usage("`spice cloud link` requires an existing project selected by name.")
    })?;

    if persist_endpoint_file {
        persist_endpoint(config_dir, endpoint).await?;
    }

    telemetry.stage("authentication");
    if !permits_stored_credentials {
        return Err(legacy_endpoint_requires_explicit_authority(endpoint));
    }
    let login = stored_user_login(endpoint, Some(fixed_org)).await?;
    let Some(login) = login else {
        return Err(org_credential_missing(fixed_org));
    };
    let token = login.token;
    telemetry.auth("login");

    telemetry.stage("project_assignment");
    assign_project(
        ProjectAssignmentContext {
            endpoint,
            config_dir,
            directory,
            token: &token,
            organization: fixed_org,
            identity: &identity,
            recovery_url: None,
            rollback_enrollment_on_refusal: false,
        },
        project.to_string(),
    )
    .await?;
    telemetry.complete("attached");
    Ok(())
}

async fn stored_user_login(
    endpoint: &str,
    org_hint: Option<&str>,
) -> Result<Option<LoginCredential>> {
    let candidates = crate::commands::cloud::client::user_credential_candidates(org_hint);
    // Share the preflight's policy rather than restating it: `spice cloud link`
    // chooses a credential before this transaction runs, and a rule that
    // accepted one here but not there would strand a link half-done.
    match crate::commands::cloud::client::first_user_credential(&candidates, endpoint, org_hint)
        .await?
    {
        crate::commands::cloud::client::UserCredentialSearch::Found(token) => {
            Ok(Some(LoginCredential {
                token: SessionToken::new(token),
            }))
        }
        crate::commands::cloud::client::UserCredentialSearch::NoneStored => Ok(None),
        crate::commands::cloud::client::UserCredentialSearch::AllRejected => Err(
            crate::commands::cloud::rejected_user_credential_error(Some("link"), org_hint),
        ),
    }
}

fn org_credential_missing(org: &str) -> Error {
    Error::cloud_with_hint(
        CloudErrorCode::OrgCredentialMissing,
        format!("No Spice Cloud credential is stored for organization '{org}'."),
        format!(
            "Authenticate for it with 'spice cloud login token --org {org}' (or 'spice cloud login api --org {org}' for automation), or set {}.",
            cloud_org::org_token_var(org)
        ),
    )
}

fn legacy_endpoint_requires_explicit_authority(endpoint: &str) -> Error {
    invalid_usage(format!(
        "the repository-local cloud-endpoint file selects {endpoint}, so Spice will not send a login or enrollment-key credential there without explicit authority. Re-run with --endpoint {endpoint} or set SPICE_CLOUD_ENDPOINT."
    ))
}

/// One enrollment attempt: where it goes, what authorizes it, and what it has to
/// stay faithful to.
struct EnrollAttempt<'a> {
    ctx: &'a RuntimeContext,
    config_dir: &'a Path,
    directory: &'a Path,
    endpoint: &'a TransactionEndpoint,
    region: Option<String>,
    /// The organization recorded in the enrollment journal, which is compared
    /// case-insensitively on every replay.
    journal_org: String,
    authority: EnrollmentAuthority,
    /// The operation a resumed run routed on, verified under the enrollment
    /// transaction before the credential is spent. `None` for a fresh
    /// enrollment, which has no operation to hold to.
    resumed_operation: Option<String>,
}

async fn enroll(attempt: EnrollAttempt<'_>) -> Result<EnrollmentResult> {
    let EnrollAttempt {
        ctx,
        config_dir,
        directory,
        endpoint,
        region,
        journal_org,
        authority,
        resumed_operation,
    } = attempt;
    // Which authority is enrolling decides whether the organization recorded for
    // this operation can still be corrected: an enrollment key asserts one and is
    // checked before it is spent, a session is issued for one.
    let organization_binding = match &authority {
        EnrollmentAuthority::Token { .. } => OrganizationBinding::Assertion,
        EnrollmentAuthority::AuthenticatedSession { .. } => OrganizationBinding::Fixed,
    };
    let runtime_version = ctx
        .runtime_version()
        .unwrap_or_else(|_| crate::commands::version::cli_version());
    let mut config =
        CloudConnectConfig::from_env_at(runtime_version.clone(), config_dir.to_path_buf());
    config.enroll_endpoint.clone_from(&endpoint.value);
    if endpoint.persist_file() {
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
        // The instance's own config directory is what makes the fingerprint
        // name this instance rather than the host it runs on.
        let facts = InstanceFacts::gather(&runtime_version, &prepare_dir);
        // A resumed run exists to finish one operation, so it settles that
        // question before anything can publish a new one. `load_or_create`
        // creates when the draft is absent, and creating here would answer a
        // vanished operation by leaving a phantom one on disk for the next run to
        // resume — so the resumed case is decided first, from what the
        // transaction can now see, and refuses instead.
        //
        // The binding check inside `load_or_create` cannot stand in for this: it
        // admits a corrected organization assertion, which is what lets a
        // mistyped `--org` recover, and therefore cannot tell a replay from a
        // different operation published under the same binding.
        if let Some(resumed) = resumed_operation.as_deref() {
            match runtime_cloud_connect::EnrollmentDraft::load_optional(&prepare_dir)
                .map_err(|source| Error::CloudConnectIo {
                    message: format!("read the pending enrollment: {source}"),
                })?
            {
                Some(pending) if pending.enrollment_operation_id == resumed => {}
                Some(_) => {
                    return Err(invalid_usage(
                        "the pending enrollment for this directory changed while this run was preparing: it now holds a different operation. Nothing was sent. Re-run `spice cloud link` to continue the operation that is pending now, or run `spice cloud unlink` to abandon it explicitly.",
                    ));
                }
                None if prepare_dir
                    .join(runtime_cloud_connect::config::IDENTITY_FILE)
                    .exists() =>
                {
                    return Err(invalid_usage(
                        "this directory finished enrolling while this run was preparing, so the pending operation is gone. Nothing was sent, and the enrollment key was not redeemed. Re-run `spice cloud link` to start a new link transaction.",
                    ));
                }
                None => {
                    return Err(invalid_usage(
                        "the pending enrollment for this directory was removed while this run was preparing. Nothing was sent, and the enrollment key was not redeemed. Re-run `spice cloud link` to enroll this directory again.",
                    ));
                }
            }
        }
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
            organization_binding,
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

struct ProjectAssignmentContext<'a> {
    endpoint: &'a str,
    config_dir: &'a Path,
    directory: &'a Path,
    token: &'a SessionToken,
    organization: &'a str,
    identity: &'a Identity,
    recovery_url: Option<&'a str>,
    rollback_enrollment_on_refusal: bool,
}

async fn assign_project(context: ProjectAssignmentContext<'_>, project_name: String) -> Result<()> {
    let client = ProjectClient::new(context.endpoint).map_err(|error| project_error(&error))?;
    let projects = client
        .list_attachable(context.token)
        .await
        .map_err(|error| project_error(&error))?;
    let project = projects.into_iter().find(|project| {
        project.name == project_name && project.org.eq_ignore_ascii_case(context.organization)
    });
    let Some(project) = project else {
        if context.rollback_enrollment_on_refusal {
            rollback_refused_fresh_enrollment(&context).await?;
        }
        return Err(Error::cloud_with_hint(
            CloudErrorCode::ProjectNotFound,
            format!(
                "Project '{}/{}' is not attachable to this enrolled instance.",
                context.organization, project_name
            ),
            "Run `spice cloud link` without a project to list every project the server currently allows this instance to attach to.",
        ));
    };
    let location = project
        .instances
        .iter()
        .find(|instance| instance.id == context.identity.identifier)
        .and_then(|instance| instance.location.as_deref())
        .or(project.region.as_deref());
    let started = Instant::now();
    let mut retry_attempt = 0_u32;

    loop {
        let operation =
            prepare_project_operation(&context, &project.name, project.id, location).await?;
        match client
            .attach(context.token, context.organization, &operation.request)
            .await
        {
            Ok(attachment) => {
                persist_attachment(context.config_dir, &attachment).await?;
                delete_project_operation(context.config_dir).await?;
                println!(
                    "Spice Cloud Connect: enrollment complete; attached to {} / {}",
                    attachment.organization, attachment.project_name
                );
                println!("Monitor: {}", attachment.monitor_url);
                return Ok(());
            }
            // The control plane knows this instance has an attachment, but
            // does not return enough state in this denial to persist it
            // locally. Never tell the operator it is unattached.
            Err(error) if error.is_already_attached() => return Err(project_error(&error)),
            Err(error)
                if error.is_attachment_ambiguous()
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
            Err(error) if !error.is_attachment_ambiguous() => {
                if context.rollback_enrollment_on_refusal {
                    rollback_refused_fresh_enrollment(&context).await?;
                } else {
                    print_unattached(context.identity, context.recovery_url);
                }
                delete_project_operation(context.config_dir).await?;
                return Err(project_error(&error));
            }
            Err(error) => return Err(ambiguous_project_error(&error)),
        }
    }
}

async fn prepare_project_operation(
    context: &ProjectAssignmentContext<'_>,
    project_name: &str,
    project_id: i64,
    location: Option<&str>,
) -> Result<ProjectOperation> {
    let config_dir = context.config_dir.to_path_buf();
    let directory = context.directory.to_path_buf();
    let endpoint = context.endpoint.to_string();
    let organization = context.organization.to_string();
    let project_name = project_name.to_string();
    let request =
        ProjectMutation::signed(context.identity, context.organization, project_id, location)
            .map_err(|error| project_error(&error))?;
    tokio::task::spawn_blocking(move || {
        ProjectOperation::prepare(
            &config_dir,
            &directory,
            &endpoint,
            &organization,
            &project_name,
            request,
        )
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

async fn rollback_refused_fresh_enrollment(context: &ProjectAssignmentContext<'_>) -> Result<()> {
    match runtime_cloud_connect::release::release(
        context.endpoint,
        context.identity,
        context.token.expose_secret(),
        None,
    )
    .await
    {
        Ok(_) => {}
        Err(error) if error.is_not_found() => {}
        Err(error) => {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "Spice Cloud refused the project attachment and could not roll back the fresh enrollment, so the local identity was kept for retry: {error}"
                ),
            });
        }
    }
    let identity_path = context
        .config_dir
        .join(runtime_cloud_connect::config::IDENTITY_FILE);
    IdentityStore::clear_async(&identity_path)
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("clear the rolled-back enrolled identity: {source}"),
        })
}

pub(crate) async fn clear_local_state(config_dir: &Path, identity_path: &Path) -> Result<()> {
    let enrollment_transaction = Arc::new(
        runtime_cloud_connect::EnrollmentTransactionLock::try_acquire_async(config_dir)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!("acquire the enrollment transaction before unlink: {source}"),
            })?,
    );
    let blocking_config_dir = config_dir.to_path_buf();
    let identity_path = identity_path.to_path_buf();
    let transaction = Arc::clone(&enrollment_transaction);
    tokio::task::spawn_blocking(move || -> Result<()> {
        let cache_path =
            blocking_config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
        super::state::remove_durable_file(&cache_path).map_err(|source| Error::CloudConnectIo {
            message: format!("remove the delivered-secrets cache: {source}"),
        })?;
        for (label, file) in [
            (
                "cloud-managed Spicepod",
                runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE,
            ),
            (
                "staged cloud-managed Spicepod",
                "spicepod-cloud-managed.incoming.yml",
            ),
            (
                "cloud-managed Spicepod replacement backup",
                "spicepod-cloud-managed.bak",
            ),
            (
                "deployment transaction marker",
                runtime_cloud_connect::config::DEPLOYMENT_TRANSACTION_FILE,
            ),
            (
                "staged deployment transaction marker",
                runtime_cloud_connect::config::DEPLOYMENT_TRANSACTION_INCOMING_FILE,
            ),
            (
                "staged delivered-secret cache",
                runtime_cloud_connect::config::INCOMING_SECRET_CACHE_FILE,
            ),
            (
                "previous delivered-secret cache",
                runtime_cloud_connect::config::PREVIOUS_SECRET_CACHE_FILE,
            ),
            (
                "previous cloud-managed Spicepod",
                runtime_cloud_connect::config::PREVIOUS_CLOUD_MANAGED_SPICEPOD_FILE,
            ),
        ] {
            let path = blocking_config_dir.join(file);
            super::state::remove_durable_file(&path).map_err(|source| Error::CloudConnectIo {
                message: format!("remove the {label}: {source}"),
            })?;
        }
        ConnectOperation::delete(&blocking_config_dir).map_err(|source| Error::CloudConnectIo {
            message: format!("remove the enrollment journal: {source}"),
        })?;
        ProjectOperation::delete(&blocking_config_dir).map_err(|source| Error::CloudConnectIo {
            message: format!("remove the project attachment journal: {source}"),
        })?;
        super::state::remove_durable_file(
            &blocking_config_dir.join(CloudConnectConfig::ENDPOINT_OVERRIDE_FILE),
        )
        .map_err(|source| Error::CloudConnectIo {
            message: format!("remove the Cloud endpoint binding: {source}"),
        })?;
        remove_local_credential_debris(&blocking_config_dir)?;
        Ok(())
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("local Cloud Connect cleanup task panicked: {source}"),
    })??;

    enrollment_transaction
        .delete_draft_async()
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("remove the enrollment draft: {source}"),
        })?;
    IdentityStore::clear_with_transaction_async(identity_path, transaction)
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("clear the released instance identity: {source}"),
        })?;
    Ok(())
}

/// Remove only the secret-bearing temporary and quarantined file families
/// created by Cloud Connect writers. The caller holds the local state locks,
/// so every matching writer artifact is abandoned and removable.
fn remove_local_credential_debris(config_dir: &Path) -> Result<()> {
    let draft_path = runtime_cloud_connect::EnrollmentDraft::path_in(config_dir);
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let entries = match std::fs::read_dir(config_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "inspect local Cloud credential artifacts in {}: {source}",
                    config_dir.display()
                ),
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| Error::CloudConnectIo {
            message: format!(
                "inspect local Cloud credential artifacts in {}: {source}",
                config_dir.display()
            ),
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let generated_draft_quarantine = is_generated_draft_quarantine(&name);
        let generated_draft_temp =
            runtime_cloud_connect::identity::is_runtime_atomic_write_artifact(&draft_path, &name);
        let generated_identity_temp =
            runtime_cloud_connect::identity::is_runtime_atomic_write_artifact(
                &identity_path,
                &name,
            );
        if generated_draft_quarantine || generated_draft_temp || generated_identity_temp {
            super::state::remove_durable_file(&entry.path()).map_err(|source| {
                Error::CloudConnectIo {
                    message: format!(
                        "remove local Cloud credential artifact at {}: {source}",
                        entry.path().display()
                    ),
                }
            })?;
        }
    }
    Ok(())
}

/// Match the exact quarantine spelling emitted by [`super::state`].
fn is_generated_draft_quarantine(name: &str) -> bool {
    let Some(parts) = name
        .strip_prefix("enrollment-draft.quarantine.")
        .and_then(|name| name.strip_suffix(".json"))
    else {
        return false;
    };
    let mut parts = parts.split('.');
    let (Some(epoch), Some(pid), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    epoch
        .parse::<u128>()
        .is_ok_and(|value| value.to_string() == epoch)
        && pid
            .parse::<u32>()
            .is_ok_and(|value| value.to_string() == pid)
}

async fn persist_attachment(config_dir: &Path, attachment: &ProjectAttachment) -> Result<()> {
    let config_dir = config_dir.to_path_buf();
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let attachment = AppAttachment {
        app_id: attachment.project_id.to_string(),
        org_name: Some(attachment.organization.clone()),
        app_name: Some(attachment.project_name.clone()),
        monitor_url: Some(attachment.monitor_url.clone()),
    };
    tokio::task::spawn_blocking(move || {
        IdentityStore::set_attachment(&config_dir, &identity_path, Some(&attachment)).map_err(
            |source| Error::CloudConnectIo {
                message: format!("persist project attachment: {source}"),
            },
        )
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
                "the Cloud Connect identity at {} is unusable ({reason}); run `spice cloud unlink` before enrolling again",
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
            source: EndpointSource::Explicit,
        });
    }
    if let Some(value) = bound {
        return Ok(TransactionEndpoint {
            value,
            source: EndpointSource::Bound,
        });
    }

    let legacy = CloudConnectConfig::read_normalized_enroll_endpoint_override(config_dir).map_err(
        |source| Error::CloudConnectIo {
            message: source.to_string(),
        },
    )?;
    Ok(match legacy {
        Some(value) => TransactionEndpoint {
            value,
            source: EndpointSource::LegacyFile,
        },
        None => TransactionEndpoint {
            value: runtime_cloud_connect::config::DEFAULT_ENDPOINT.to_string(),
            source: EndpointSource::CompiledDefault,
        },
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
    let identity = identity.map(IdentityFacts::from);
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
    let identity = identity.map(IdentityFacts::from);
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
    let path = config_dir.join(CloudConnectConfig::ENDPOINT_OVERRIDE_FILE);
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

fn print_unattached(identity: &Identity, recovery_url: Option<&str>) {
    let org = identity.org_name.as_deref().unwrap_or("Spice Cloud");
    println!("Spice Cloud Connect: enrolled in {org} — not yet attached to a project");
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

/// One rule decides which Cloud-provided link may be printed or opened, and it
/// lives with the identity that persists them.
fn safe_recovery_url(candidate: &str) -> Option<String> {
    runtime_cloud_connect::config::safe_portal_url(candidate)
}

/// Validate what this invocation asks for on its own terms, before any state is
/// read or any lock is taken.
///
/// Everything here is wrong whatever is on disk — a malformed project name, an
/// impossible region, an unusable endpoint — so rejecting it costs no I/O and
/// touches nothing. Rules that a pending operation can answer better are not
/// here: those wait until the draft has been read, so an operator with an
/// operation in flight is always told about *that* operation rather than about
/// the flags in the abstract.
fn preflight_request(request: &ConnectRequest) -> Result<()> {
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

fn validate_project_name(name: &str) -> std::result::Result<(), &'static str> {
    if !(4..=38).contains(&name.len()) {
        return Err("must be between 4 and 38 characters");
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err("must contain only lowercase letters, digits, and dashes");
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err("must start and end with a letter or digit");
    }
    Ok(())
}

fn print_attached(identity: &Identity) {
    let org = identity.org_name.as_deref().unwrap_or("Spice Cloud");
    let project = identity.app_name.as_deref().unwrap_or("attached project");
    println!("Spice Cloud Connect: enrollment complete; attached to {org} / {project}");
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
    use runtime_cloud_connect::{
        EnrollmentAuthorityBinding, EnrollmentDraft, EnrollmentRequestBinding,
    };

    use super::*;

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
        let cancelled = Arc::new(AtomicBool::new(false));
        let telemetry = FlowTelemetry::new(Some(Arc::clone(&cancelled)));
        telemetry.auth("login");
        telemetry.stage("project_assignment");
        assert_eq!(telemetry.auth_path.get(), "login");
        assert_eq!(telemetry.failure_stage.get(), "project_assignment");
        telemetry.complete("cancelled");
        assert!(cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn control_plane_endpoint_requires_https() {
        for valid in ["https://api.spice.ai", "https://cloud.example.test/api"] {
            validate_control_plane_endpoint(valid).expect("valid control-plane URL");
        }
        for invalid in [
            "not-a-url",
            "http://localhost:8090",
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
        assert_eq!(fresh.source, EndpointSource::CompiledDefault);
        assert!(!fresh.persist_file());
        assert!(fresh.permits_credentials());
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
        assert_eq!(resolved.source, EndpointSource::LegacyFile);
        assert!(!resolved.persist_file());
        assert!(!resolved.permits_credentials());
    }

    /// A prompter that answers only the enrollment-key prompt and fails the
    /// test if the authentication chooser — or anything past enrollment — is
    /// ever reached. Offering a mode the pending operation cannot be finished
    /// under is the regression this guards.
    struct ResumePrompter {
        interactive: bool,
        key: Option<String>,
        key_prompts: usize,
    }

    impl Prompter for ResumePrompter {
        fn interactive(&self) -> bool {
            self.interactive
        }

        async fn read_enrollment_key(
            &mut self,
            _portal_url: &str,
        ) -> Result<Option<Zeroizing<String>>> {
            self.key_prompts += 1;
            Ok(self.key.take().map(Zeroizing::new))
        }
    }

    /// A prompter that replaces the pending draft while the key prompt is open —
    /// the one place a resumed run waits on a human, and so the widest window a
    /// concurrent enrollment has to change the operation under it.
    struct RacingPrompter {
        key: Option<String>,
        config_dir: std::path::PathBuf,
        replacement: Option<EnrollmentRequestBinding>,
        /// Remove the pending draft without publishing another, the way a
        /// release does.
        remove: bool,
    }

    impl Prompter for RacingPrompter {
        fn interactive(&self) -> bool {
            true
        }

        async fn read_enrollment_key(
            &mut self,
            _portal_url: &str,
        ) -> Result<Option<Zeroizing<String>>> {
            // What another process does when it takes the transaction: the
            // operation this run routed on is gone, and either a different one is
            // pending in its place or nothing is.
            if self.remove || self.replacement.is_some() {
                std::fs::remove_file(EnrollmentDraft::path_in(&self.config_dir))
                    .expect("remove the pending draft");
            }
            if let Some(binding) = self.replacement.take() {
                EnrollmentDraft::load_or_create(
                    &self.config_dir,
                    &InstanceFacts::gather("v0.0.0-racing-test", &self.config_dir),
                    Some("lab-seoul"),
                    &binding,
                )
                .expect("publish the replacement draft");
            }
            Ok(self.key.take().map(Zeroizing::new))
        }
    }

    fn connect_request() -> ConnectRequest {
        ConnectRequest {
            org: None,
            project: None,
            token: None,
            region: None,
            dir: None,
            endpoint: None,
        }
    }

    fn pending_draft(config_dir: &Path, authority: EnrollmentAuthorityBinding) -> EnrollmentDraft {
        EnrollmentDraft::load_or_create(
            config_dir,
            &InstanceFacts::gather("v0.0.0-resume-test", config_dir),
            Some("lab-seoul"),
            &EnrollmentRequestBinding {
                endpoint: "https://api.spice.ai".to_string(),
                authority,
            },
        )
        .expect("publish a pending enrollment draft")
    }

    fn token_draft(config_dir: &Path, expected_org: Option<&str>) -> EnrollmentDraft {
        pending_draft(
            config_dir,
            EnrollmentAuthorityBinding::Token {
                expected_org: expected_org.map(str::to_string),
            },
        )
    }

    fn login_draft(config_dir: &Path, organization: &str) -> EnrollmentDraft {
        pending_draft(
            config_dir,
            EnrollmentAuthorityBinding::AuthenticatedSession {
                organization: organization.to_string(),
            },
        )
    }

    /// A loopback address nothing listens on: an enrollment attempt against it
    /// fails without reaching a control plane.
    fn closed_endpoint() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
        let address = listener.local_addr().expect("reserved port");
        drop(listener);
        format!("https://{address}")
    }

    /// A pending enrollment-key operation resumes token mode: the key prompt is
    /// the only question, the chooser is never opened, and the persisted
    /// organization assertion rides the replay even though no flag named it.
    #[test]
    fn a_token_draft_resumes_the_key_path_under_its_persisted_assertion() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let draft = token_draft(dir.path(), Some("acme"));

        for requested in [None, Some("ACME")] {
            let request = ConnectRequest {
                org: requested.map(str::to_string),
                ..connect_request()
            };
            let resumed = resumable_authority(&draft, &request, true).expect("resume token mode");
            assert_eq!(
                resumed,
                ResumableAuthority::EnrollmentKey {
                    expected_org: Some("acme".to_string())
                },
                "the draft's own assertion is what a resume replays"
            );
        }
    }

    /// An explicit input an enrollment-key operation cannot replay — a project it
    /// has no authority to create, a key it cannot be given — fails before
    /// anything is prompted for or sent, and says what finishes it instead.
    #[test]
    fn a_token_draft_rejects_inputs_it_cannot_replay() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let draft = token_draft(dir.path(), Some("acme"));

        let with_project = resumable_authority(
            &draft,
            &ConnectRequest {
                project: Some("retail".to_string()),
                ..connect_request()
            },
            true,
        )
        .expect_err("an enrollment-key operation cannot create a project");
        assert!(
            with_project.to_string().contains("--project"),
            "{with_project}"
        );

        let headless = resumable_authority(&draft, &connect_request(), false)
            .expect_err("a key cannot be prompted for without a terminal");
        assert!(
            headless.to_string().contains("--token <enrollment-key>"),
            "{headless}"
        );

        // The unasserted case still resumes token mode rather than the chooser.
        let unasserted = token_draft(tempfile::tempdir().expect("create tempdir").path(), None);
        assert_eq!(
            resumable_authority(&unasserted, &connect_request(), true).expect("resume token mode"),
            ResumableAuthority::EnrollmentKey { expected_org: None }
        );
    }

    /// `--org` on a pending token operation corrects the assertion rather than
    /// contradicting it.
    ///
    /// Spice Cloud checks `expected_org` before it consumes the key, so an
    /// operation that failed on a mistyped organization still holds an unspent
    /// key and is replayable with the right one. Refusing the correction would
    /// leave abandoning the draft as the only way forward.
    #[test]
    fn a_token_draft_takes_a_corrected_organization_assertion() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let draft = token_draft(dir.path(), Some("acme-typo"));

        let corrected = resumable_authority(
            &draft,
            &ConnectRequest {
                org: Some("acme".to_string()),
                ..connect_request()
            },
            true,
        )
        .expect("a corrected assertion replays the pending operation");
        assert_eq!(
            corrected,
            ResumableAuthority::EnrollmentKey {
                expected_org: Some("acme".to_string())
            },
            "the corrected organization is what the replay asserts"
        );

        // And one is supplied where the draft asserted nothing at all.
        let unasserted = token_draft(tempfile::tempdir().expect("create tempdir").path(), None);
        assert_eq!(
            resumable_authority(
                &unasserted,
                &ConnectRequest {
                    org: Some("acme".to_string()),
                    ..connect_request()
                },
                true,
            )
            .expect("resume token mode"),
            ResumableAuthority::EnrollmentKey {
                expected_org: Some("acme".to_string())
            }
        );
    }

    /// A pending login operation resumes login mode for the organization its
    /// binding names, with or without a matching `--org`.
    #[test]
    fn a_login_draft_resumes_the_organization_it_is_bound_to() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let draft = login_draft(dir.path(), "acme");

        for requested in [None, Some("acme"), Some("ACME")] {
            let request = ConnectRequest {
                org: requested.map(str::to_string),
                project: Some("retail".to_string()),
                ..connect_request()
            };
            assert_eq!(
                resumable_authority(&draft, &request, true).expect("resume login mode"),
                ResumableAuthority::Login {
                    organization: "acme".to_string()
                }
            );
        }
    }

    /// A login-mode operation cannot be finished with an enrollment key or
    /// under another organization: both fail closed, naming the command that
    /// finishes it and the one that abandons it explicitly.
    #[test]
    fn a_login_draft_rejects_a_key_or_another_organization() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let draft = login_draft(dir.path(), "acme");

        let with_key = resumable_authority(
            &draft,
            &ConnectRequest {
                token: Some(
                    EnrollmentKey::parse("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                        .expect("fixture enrollment key"),
                ),
                project: Some("retail".to_string()),
                ..connect_request()
            },
            true,
        )
        .expect_err("a key must not replace the pending login authority");
        let rendered = with_key.to_string();
        assert!(
            rendered.contains("--token")
                && rendered.contains("re-run `spice cloud link` interactively")
                && rendered.contains("spice cloud unlink"),
            "{rendered}"
        );
        // The advertised command has to work in the mode the operator is in. A
        // login-mode resume needs a project name off a terminal and accepts one
        // on it, while the organization comes from the binding either way — so
        // naming --org instead would fail the caller it was written for.
        assert!(
            !rendered.contains("spice cloud link --org"),
            "the remedy must not advertise a command a non-interactive caller cannot finish: {rendered}"
        );

        let another_org = resumable_authority(
            &draft,
            &ConnectRequest {
                org: Some("globex".to_string()),
                project: Some("retail".to_string()),
                ..connect_request()
            },
            true,
        )
        .expect_err("another organization must not redirect the pending operation");
        assert!(
            another_org
                .to_string()
                .contains("bound to organization acme"),
            "{another_org}"
        );

        let headless = resumable_authority(
            &draft,
            &ConnectRequest {
                org: Some("acme".to_string()),
                ..connect_request()
            },
            false,
        )
        .expect_err("a project name cannot be prompted for without a terminal");
        assert!(
            headless.to_string().contains("--project <name>"),
            "{headless}"
        );
    }

    /// The pending draft binds the control plane: it is used when no flag names
    /// one, and a flag naming another is refused rather than replayed there.
    #[test]
    fn a_pending_draft_binds_the_control_plane() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let draft = token_draft(dir.path(), Some("acme"));

        let bound =
            resolve_transaction_endpoint_with_env(dir.path(), None, None, None, Some(&draft))
                .expect("the draft binds the endpoint");
        assert_eq!(bound.value, "https://api.spice.ai");
        assert_eq!(bound.source, EndpointSource::Bound);

        resolve_transaction_endpoint_with_env(
            dir.path(),
            Some("https://other.example"),
            None,
            None,
            Some(&draft),
        )
        .expect_err("a pending operation must not be replayed to another control plane");
    }

    /// The end-to-end resume: a plain interactive `spice cloud link` over a pending
    /// enrollment-key draft asks for a key exactly once, never opens the
    /// chooser, and leaves the retry-safe state intact when the control plane
    /// cannot be reached.
    #[tokio::test(start_paused = true)]
    async fn a_pending_token_draft_resumes_without_the_chooser() {
        let instance = tempfile::tempdir().expect("create instance directory");
        let directory = instance.path().canonicalize().expect("canonical tempdir");
        let config_dir = CloudConnectConfig::resolve_config_dir(Some(&directory));
        let endpoint = closed_endpoint();
        let published = EnrollmentDraft::load_or_create(
            &config_dir,
            &InstanceFacts::gather("v0.0.0-resume-test", &config_dir),
            Some("lab-seoul"),
            &EnrollmentRequestBinding {
                endpoint: endpoint.clone(),
                authority: EnrollmentAuthorityBinding::Token {
                    expected_org: Some("acme".to_string()),
                },
            },
        )
        .expect("publish a pending enrollment draft");

        let mut prompter = ResumePrompter {
            interactive: true,
            key: Some("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string()),
            key_prompts: 0,
        };
        let error = execute_with(
            &RuntimeContext::new().expect("runtime context"),
            ConnectRequest {
                dir: Some(directory.clone()),
                // Named explicitly so an ambient SPICE_CLOUD_ENDPOINT cannot
                // decide what this exercise resumes against.
                endpoint: Some(endpoint.clone()),
                ..connect_request()
            },
            &mut prompter,
        )
        .await
        .expect_err("a closed control plane cannot complete an enrollment");

        assert_eq!(
            prompter.key_prompts, 1,
            "resuming a key operation asks for exactly one key"
        );
        assert!(
            matches!(error, Error::CloudConnectEnroll { .. }),
            "the resume must reach the enrollment request: {error}"
        );
        let preserved = EnrollmentDraft::load_optional(&config_dir)
            .expect("read the preserved draft")
            .expect("a failed attempt keeps the retry-safe state");
        assert_eq!(
            preserved.enrollment_operation_id, published.enrollment_operation_id,
            "a resume replays the durable operation ID"
        );
        assert_eq!(preserved.public_key_pem, published.public_key_pem);
        assert_eq!(preserved.binding, published.binding);
        let contents =
            std::fs::read_to_string(EnrollmentDraft::path_in(&config_dir)).expect("read draft");
        assert!(
            !contents.contains("spice-enroll-"),
            "the enrollment key must never be persisted"
        );
    }

    /// The authority is routed from a draft read before the enrollment
    /// transaction is held, so another process can replace the operation while
    /// the key prompt is open — the one place a resumed run waits on a human.
    ///
    /// What refuses is the operation's identity, whatever else the replacement
    /// changed. The binding cannot stand in for it: every token binding matches
    /// every other so a corrected organization assertion can still recover its
    /// operation, which leaves a *different* operation under the same binding
    /// indistinguishable. Both replacements below are therefore refused the same
    /// way, with nothing sent, nothing enrolled, and the operation the other
    /// process published left exactly as it wrote it.
    #[tokio::test(start_paused = true)]
    async fn an_operation_replaced_while_prompting_fails_closed() {
        let endpoint = closed_endpoint();
        let routed_binding = EnrollmentRequestBinding {
            endpoint: endpoint.clone(),
            authority: EnrollmentAuthorityBinding::Token {
                expected_org: Some("acme".to_string()),
            },
        };
        let replacements = [
            // Indistinguishable by binding: only the operation ID differs.
            routed_binding.clone(),
            // A different authority entirely.
            EnrollmentRequestBinding {
                endpoint: endpoint.clone(),
                authority: EnrollmentAuthorityBinding::AuthenticatedSession {
                    organization: "globex".to_string(),
                },
            },
        ];

        for replacement in replacements {
            let instance = tempfile::tempdir().expect("create instance directory");
            let directory = instance.path().canonicalize().expect("canonical tempdir");
            let config_dir = CloudConnectConfig::resolve_config_dir(Some(&directory));
            let routed = EnrollmentDraft::load_or_create(
                &config_dir,
                &InstanceFacts::gather("v0.0.0-racing-test", &config_dir),
                Some("lab-seoul"),
                &routed_binding,
            )
            .expect("publish a pending enrollment draft");

            let mut prompter = RacingPrompter {
                key: Some("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string()),
                config_dir: config_dir.clone(),
                replacement: Some(replacement.clone()),
                remove: false,
            };
            let error = execute_with(
                &RuntimeContext::new().expect("runtime context"),
                ConnectRequest {
                    dir: Some(directory.clone()),
                    endpoint: Some(endpoint.clone()),
                    ..connect_request()
                },
                &mut prompter,
            )
            .await
            .expect_err("an operation this run never routed on must not be enrolled");
            let rendered = error.to_string();
            assert!(
                rendered.contains("changed while this run was preparing")
                    && rendered.contains("Nothing was sent"),
                "the transaction must refuse the replaced operation: {rendered}"
            );

            let surviving = EnrollmentDraft::load_optional(&config_dir)
                .expect("read the draft state")
                .expect("the replacement is still pending");
            assert_eq!(
                surviving.binding, replacement,
                "the operation the other process published must be left exactly as written"
            );
            assert_ne!(
                surviving.enrollment_operation_id, routed.enrollment_operation_id,
                "the replacement is a different operation, which is what refuses"
            );
            assert!(
                !config_dir
                    .join(runtime_cloud_connect::config::IDENTITY_FILE)
                    .exists(),
                "a refused resume must enroll nothing"
            );
        }
    }

    /// The conflict rules are asserted through the command, not only through the
    /// decision function, because their value is which message an operator
    /// actually gets — and that depends on the order the command applies them in.
    ///
    /// `--token` with `--project` is invalid in every mode, but over a pending
    /// login-mode operation the useful answer is the one about that operation, so
    /// the flag rule must not pre-empt it.
    #[tokio::test(start_paused = true)]
    async fn a_pending_login_draft_answers_before_the_flag_rules_do() {
        let instance = tempfile::tempdir().expect("create instance directory");
        let directory = instance.path().canonicalize().expect("canonical tempdir");
        let config_dir = CloudConnectConfig::resolve_config_dir(Some(&directory));
        let endpoint = closed_endpoint();
        let published = EnrollmentDraft::load_or_create(
            &config_dir,
            &InstanceFacts::gather("v0.0.0-ordering-test", &config_dir),
            Some("lab-seoul"),
            &EnrollmentRequestBinding {
                endpoint: endpoint.clone(),
                authority: EnrollmentAuthorityBinding::AuthenticatedSession {
                    organization: "acme".to_string(),
                },
            },
        )
        .expect("publish a pending enrollment draft");

        let mut prompter = ResumePrompter {
            interactive: true,
            key: None,
            key_prompts: 0,
        };
        let error = execute_with(
            &RuntimeContext::new().expect("runtime context"),
            ConnectRequest {
                token: Some(
                    EnrollmentKey::parse("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                        .expect("fixture enrollment key"),
                ),
                project: Some("retail".to_string()),
                dir: Some(directory.clone()),
                endpoint: Some(endpoint.clone()),
                ..connect_request()
            },
            &mut prompter,
        )
        .await
        .expect_err("a key cannot finish a pending login-mode operation");
        let rendered = error.to_string();
        assert!(
            rendered.contains("re-run `spice cloud link` interactively")
                && rendered.contains("spice cloud unlink"),
            "the pending operation must answer, naming what finishes and what abandons it: {rendered}"
        );
        assert_eq!(
            prompter.key_prompts, 0,
            "a refusal must not ask for anything first"
        );
        assert_eq!(
            EnrollmentDraft::load_optional(&config_dir)
                .expect("read the draft state")
                .expect("the operation is still pending")
                .enrollment_operation_id,
            published.enrollment_operation_id,
            "a refused invocation leaves the pending operation exactly as it was"
        );
    }

    /// A resumed run whose operation is gone refuses and publishes nothing. The
    /// prepare step creates a draft when none exists, and creating one here would
    /// answer a vanished operation by leaving a phantom for the next run to
    /// resume — so it is decided before anything can create.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_operation_that_vanished_publishes_nothing() {
        let instance = tempfile::tempdir().expect("create instance directory");
        let directory = instance.path().canonicalize().expect("canonical tempdir");
        let config_dir = CloudConnectConfig::resolve_config_dir(Some(&directory));
        let endpoint = closed_endpoint();
        EnrollmentDraft::load_or_create(
            &config_dir,
            &InstanceFacts::gather("v0.0.0-racing-test", &config_dir),
            Some("lab-seoul"),
            &EnrollmentRequestBinding {
                endpoint: endpoint.clone(),
                authority: EnrollmentAuthorityBinding::Token {
                    expected_org: Some("acme".to_string()),
                },
            },
        )
        .expect("publish a pending enrollment draft");

        let mut prompter = RacingPrompter {
            key: Some("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string()),
            config_dir: config_dir.clone(),
            // No replacement: the operation is simply removed, as a release does.
            replacement: None,
            remove: true,
        };
        let error = execute_with(
            &RuntimeContext::new().expect("runtime context"),
            ConnectRequest {
                dir: Some(directory.clone()),
                endpoint: Some(endpoint.clone()),
                ..connect_request()
            },
            &mut prompter,
        )
        .await
        .expect_err("a vanished operation must not be replaced by a new one");
        let rendered = error.to_string();
        assert!(
            rendered.contains("was removed while this run was preparing")
                && rendered.contains("was not redeemed"),
            "the failure must say the operation is gone and the key unspent: {rendered}"
        );
        assert!(
            EnrollmentDraft::load_optional(&config_dir)
                .expect("read the draft state")
                .is_none(),
            "a refused resume must not publish a phantom operation"
        );
        assert!(
            !config_dir
                .join(runtime_cloud_connect::config::IDENTITY_FILE)
                .exists(),
            "a refused resume must enroll nothing"
        );
    }

    #[tokio::test]
    async fn unlink_cleanup_removes_cloud_state_and_preserves_operator_backups() {
        let root = tempfile::tempdir().expect("create tempdir");
        let config_dir = root.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config directory");
        let artifacts = [
            runtime_cloud_connect::config::IDENTITY_FILE,
            "enrollment-draft.json",
            "enrollment-draft.quarantine.1.2.json",
            ".enrollment-draft.json.93f1f89a-e2b7-4597-b01d-6e955efb8de8.tmp",
            ".identity.json.ae2284bb-7147-4a82-8816-37fe41f401d8.tmp",
            runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE,
            runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE,
            "spicepod-cloud-managed.incoming.yml",
            "spicepod-cloud-managed.bak",
            runtime_cloud_connect::config::DEPLOYMENT_TRANSACTION_FILE,
            runtime_cloud_connect::config::DEPLOYMENT_TRANSACTION_INCOMING_FILE,
            runtime_cloud_connect::config::PREVIOUS_CLOUD_MANAGED_SPICEPOD_FILE,
            runtime_cloud_connect::config::PREVIOUS_SECRET_CACHE_FILE,
            runtime_cloud_connect::config::INCOMING_SECRET_CACHE_FILE,
            CloudConnectConfig::ENDPOINT_OVERRIDE_FILE,
            runtime_cloud_connect::config::CloudConnectConfig::CONNECT_OPERATION_FILE,
            runtime_cloud_connect::config::CloudConnectConfig::PROJECT_OPERATION_FILE,
        ]
        .map(|file| config_dir.join(file));
        for artifact in &artifacts {
            std::fs::write(artifact, b"Cloud state").expect("write Cloud artifact");
        }
        let operator_owned = [
            ".identity.json.manual.bak",
            ".enrollment-draft.json.notes.bak",
            "enrollment-draft.quarantine.notes.2.json",
        ]
        .map(|file| config_dir.join(file));
        for path in &operator_owned {
            std::fs::write(path, b"operator backup").expect("write operator backup");
        }

        clear_local_state(
            &config_dir,
            &config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE),
        )
        .await
        .expect("clear Cloud state");

        for artifact in artifacts {
            assert!(
                !artifact.exists(),
                "unlink cleanup must remove {}",
                artifact.display()
            );
        }
        for path in operator_owned {
            assert!(
                path.exists(),
                "unlink cleanup must preserve {}",
                path.display()
            );
        }
    }

    #[test]
    fn unsafe_cloud_recovery_urls_are_never_opened_or_printed() {
        assert_eq!(
            safe_recovery_url("https://spice.ai/connect").as_deref(),
            Some("https://spice.ai/connect")
        );
        for unsafe_url in [
            "javascript:alert(1)",
            "http://127.0.0.1:8080/connect",
            "http://attacker.example/connect",
            "https://user:secret@spice.ai/connect",
        ] {
            assert_eq!(safe_recovery_url(unsafe_url), None);
        }
    }
}
