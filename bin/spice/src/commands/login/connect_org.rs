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

//! Explicit organization selection for connecting an instance.
//!
//! Connecting an instance enrolls it into exactly one organization, and that
//! choice is part of the connection — not a side effect of whichever org the
//! login happens to treat as default. This module resolves the organization
//! for **one invocation**: it enumerates the organizations the login belongs
//! to, keeps only those whose membership role may enroll (owner or admin),
//! validates an explicit `--org`, and asks the user to choose when more than
//! one qualifies. The stored active org and the login's own org are only ever
//! *highlighted* as the chooser's starting position; neither is selected on
//! its own when alternatives exist.
//!
//! Nothing here reads the choice into, or writes it out of, the machine-wide
//! `spice cloud org use` state — the resolution lives and dies with the
//! invocation that asked for it.

use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Select};
use spice_cloud_client::types::Org;

use crate::commands::cloud::{CloudClient, format_org_list, org as cloud_org};
use crate::error::{CloudErrorCode, Error, Result};

use super::session::AuthenticatedSession;

/// The membership roles allowed to connect an instance into an organization.
///
/// Compared case-insensitively against the role the API reports: this filter
/// is a permission boundary, and formatting drift on the wire must not widen
/// or narrow it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectRole {
    Owner,
    Admin,
}

impl ConnectRole {
    /// Parse a reported membership role, `None` for any role that may not
    /// connect an instance (member, viewer, deleted, or anything unknown).
    fn parse(role: &str) -> Option<Self> {
        let role = role.trim();
        if role.eq_ignore_ascii_case("owner") {
            Some(Self::Owner)
        } else if role.eq_ignore_ascii_case("admin") {
            Some(Self::Admin)
        } else {
            None
        }
    }

    #[must_use]
    fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Admin => "admin",
        }
    }
}

/// An organization resolved for one connect invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectOrg {
    /// The organization name, as `--org` and the API address it.
    pub name: String,
    /// Human-readable display name, when the API reports one.
    pub display_name: Option<String>,
    /// The membership role that made the org eligible. `None` when discovery
    /// could not report roles — the server still enforces the role on every
    /// mutation.
    pub role: Option<ConnectRole>,
}

impl std::fmt::Display for ConnectOrg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(display_name) = self
            .display_name
            .as_ref()
            .filter(|display| !display.is_empty() && !display.eq_ignore_ascii_case(&self.name))
        {
            write!(f, " — {display_name}")?;
        }
        if let Some(role) = self.role {
            write!(f, " ({})", role.as_str())?;
        }
        Ok(())
    }
}

/// How the organization resolution ended.
#[derive(Debug)]
pub enum OrgResolution {
    /// The organization this invocation connects into.
    Selected(ConnectOrg),
    /// The user cancelled the chooser (Esc, `q`, Ctrl-C, or EOF). A normal
    /// exit for the caller to unwind cleanly, not an error.
    Cancelled,
}

/// Resolve the organization one connect invocation acts on, using the
/// credential a live login session carries.
///
/// - An explicit `org` is validated against the login's memberships and must
///   carry the owner or admin role; it skips the chooser.
/// - With exactly one eligible organization, it is printed and used.
/// - With several, an interactive chooser is required: the stored active org
///   (or the login's own org) is highlighted but never chosen silently. When
///   `interactive` is false the resolution fails and names `--org`.
/// - When organization discovery is unavailable, an explicit `org` is
///   membership-checked server-side and the unverifiable role is reported;
///   otherwise only the organization the credential itself proves can be
///   offered, and only interactively.
///
/// The choice is scoped to this invocation. Nothing here mutates the
/// machine-wide active org.
///
/// # Errors
///
/// Returns an error if the organizations cannot be listed, if `org` is not an
/// eligible membership, if no membership is eligible, or if a choice is
/// required and the invocation is not interactive.
pub async fn resolve_connect_organization(
    session: &AuthenticatedSession,
    org: Option<&str>,
    interactive: bool,
) -> Result<OrgResolution> {
    let client = session.management_client()?;
    resolve_connect_organization_with_client(&client, org, Some(session.org_name()), interactive)
        .await
}

/// [`resolve_connect_organization`] for a caller that already holds an
/// authenticated management client — the already-logged-in path, where no
/// browser flow ran and no [`AuthenticatedSession`] exists.
///
/// `credential_org` is the organization the credential is known to belong to,
/// when the caller knows it. It is only a chooser highlight and the
/// discovery-unavailable confirmation candidate — never a silent choice.
///
/// # Errors
///
/// As [`resolve_connect_organization`].
pub async fn resolve_connect_organization_with_client(
    client: &CloudClient,
    org: Option<&str>,
    credential_org: Option<&str>,
    interactive: bool,
) -> Result<OrgResolution> {
    // `interactive` is the caller's intent; the terminal is a fact. Clamp on
    // the real stdin so a caller passing `true` under a pipe still gets the
    // non-interactive error shapes instead of a prompt nothing can answer.
    use std::io::IsTerminal as _;
    let interactive = interactive && std::io::stdin().is_terminal();

    resolve_with_stored_default(
        client,
        org,
        credential_org.filter(|org| !org.is_empty()),
        interactive,
        &mut TerminalPrompter,
    )
    .await
}

/// The layer that folds in the machine-wide highlight hint. Everything below
/// the terminal clamp and the client construction runs through here, so tests
/// can exercise it — including that it only ever *reads* the stored state.
async fn resolve_with_stored_default<D: OrgDiscovery, P: Prompter>(
    discovery: &D,
    explicit_org: Option<&str>,
    credential_org: Option<&str>,
    interactive: bool,
    prompter: &mut P,
) -> Result<OrgResolution> {
    // Highlight hint only. A context file that cannot be read must not block
    // the resolution — the hint moves the chooser's cursor, nothing else.
    // Read under spawn_blocking: it is filesystem I/O against the home
    // directory, which must not occupy an async runtime thread.
    let stored_default = tokio::task::spawn_blocking(cloud_org::active_org)
        .await
        .ok()
        .and_then(Result::ok)
        .flatten();

    resolve_with(
        discovery,
        explicit_org,
        stored_default.as_deref(),
        credential_org,
        interactive,
        prompter,
    )
    .await
}

/// Where the resolver asks about the login's organizations. Implemented by the
/// Spice Cloud client; tests substitute a scripted source.
trait OrgDiscovery {
    /// The organizations the login belongs to, or `None` when the API cannot
    /// enumerate them.
    async fn list_orgs(&self) -> Result<Option<Vec<Org>>>;

    /// Server-side membership check for one explicitly named organization.
    async fn confirm_membership(&self, org: &str) -> Result<()>;
}

impl OrgDiscovery for CloudClient {
    async fn list_orgs(&self) -> Result<Option<Vec<Org>>> {
        CloudClient::list_orgs(self).await
    }

    async fn confirm_membership(&self, org: &str) -> Result<()> {
        self.get_auth_context_for_org(org).await.map(drop)
    }
}

/// Where the chooser's cursor starts, and why. The *why* controls the label:
/// marking the login's own org "(default)" would claim `spice cloud org use`
/// state that does not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Highlight {
    index: usize,
    source: HighlightSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HighlightSource {
    /// The machine-wide active org selected by `spice cloud org use`.
    StoredDefault,
    /// The organization the login credential itself belongs to.
    LoginOrg,
}

/// How the resolver asks — and tells — the user. Implemented on the terminal;
/// tests substitute a scripted prompter (or one that panics, to prove no
/// prompt was reached).
trait Prompter {
    /// Choose among `options`, with the cursor starting on `highlight` when
    /// one of them deserves it. `None` means the user cancelled.
    async fn choose(
        &mut self,
        options: &[ConnectOrg],
        highlight: Option<Highlight>,
    ) -> Result<Option<usize>>;

    /// Confirm connecting into `org`, the only organization the credential
    /// proves. `None` means the user cancelled.
    async fn confirm(&mut self, org: &str) -> Result<Option<bool>>;

    /// Show the user one line of resolver output. Not a prompt — routed
    /// through this trait so tests can assert what a path reported.
    fn notify(&mut self, line: String);
}

struct TerminalPrompter;

impl Prompter for TerminalPrompter {
    async fn choose(
        &mut self,
        options: &[ConnectOrg],
        highlight: Option<Highlight>,
    ) -> Result<Option<usize>> {
        let items = chooser_items(options, highlight);
        let cursor = highlight.map_or(0, |highlight| highlight.index);

        // dialoguer blocks on terminal input for as long as the user thinks;
        // that wait must not occupy an async runtime thread.
        let selection = tokio::task::spawn_blocking(move || {
            Select::with_theme(&ColorfulTheme::default())
                .with_prompt("Organization")
                .items(&items)
                .default(cursor)
                .interact_opt()
        })
        .await
        .map_err(|err| Error::InvalidArgument {
            message: format!("Failed to read the organization selection: {err}"),
        })?;

        map_prompt_cancellation(selection, "organization selection")
    }

    async fn confirm(&mut self, org: &str) -> Result<Option<bool>> {
        let prompt =
            format!("Connect using organization '{org}' (the organization this login belongs to)?");
        let confirmation = tokio::task::spawn_blocking(move || {
            Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt(prompt)
                .default(false)
                .interact_opt()
        })
        .await
        .map_err(|err| Error::InvalidArgument {
            message: format!("Failed to read the organization confirmation: {err}"),
        })?;

        map_prompt_cancellation(confirmation, "organization confirmation")
    }

    fn notify(&mut self, line: String) {
        println!("{line}");
    }
}

/// The chooser rows: "(default)" marks only a highlight that really is the
/// stored `spice cloud org use` state — the login's own org gets the cursor
/// without a label it has not earned.
fn chooser_items(options: &[ConnectOrg], highlight: Option<Highlight>) -> Vec<String> {
    options
        .iter()
        .enumerate()
        .map(|(index, org)| match highlight {
            Some(highlight)
                if highlight.index == index
                    && highlight.source == HighlightSource::StoredDefault =>
            {
                format!("{org} (default)")
            }
            _ => org.to_string(),
        })
        .collect()
}

/// Fold the two shapes of "the user backed out" — `Esc`/`q` (`Ok(None)`) and
/// Ctrl-C/EOF (an interrupted read) — into `None`, so every way of declining a
/// prompt is the same clean cancellation.
fn map_prompt_cancellation<T>(
    outcome: dialoguer::Result<Option<T>>,
    what: &str,
) -> Result<Option<T>> {
    match outcome {
        Ok(selection) => Ok(selection),
        Err(dialoguer::Error::IO(err))
            if matches!(
                err.kind(),
                std::io::ErrorKind::Interrupted | std::io::ErrorKind::UnexpectedEof
            ) =>
        {
            Ok(None)
        }
        Err(err) => Err(Error::InvalidArgument {
            message: format!("Failed to read the {what}: {err}"),
        }),
    }
}

/// The resolver behind [`resolve_connect_organization`], with its two effect
/// channels — discovery and prompting — injectable.
async fn resolve_with<D: OrgDiscovery, P: Prompter>(
    discovery: &D,
    explicit_org: Option<&str>,
    stored_default: Option<&str>,
    credential_org: Option<&str>,
    interactive: bool,
    prompter: &mut P,
) -> Result<OrgResolution> {
    if let Some(requested) = explicit_org {
        cloud_org::validate_org_name(requested)?;
    }

    let Some(listing) = discovery.list_orgs().await? else {
        return resolve_without_discovery(
            discovery,
            explicit_org,
            credential_org,
            interactive,
            prompter,
        )
        .await;
    };

    let eligible = eligible_orgs(&listing);

    if let Some(requested) = explicit_org {
        return Ok(OrgResolution::Selected(validate_explicit(
            &listing, &eligible, requested,
        )?));
    }

    match eligible.as_slice() {
        [] => Err(no_eligible_org_error(&listing)),
        [only] => {
            // Show the org before anything acts on it: with no chooser and no
            // flag, this line is the only place the user sees the target.
            prompter.notify(format!("Organization: {only}"));
            Ok(OrgResolution::Selected(only.clone()))
        }
        _ => {
            if !interactive {
                return Err(ambiguous_without_terminal_error(&eligible));
            }
            let highlight = highlight(&eligible, stored_default, credential_org);
            match prompter.choose(&eligible, highlight).await? {
                Some(index) => {
                    let chosen =
                        eligible
                            .get(index)
                            .cloned()
                            .ok_or_else(|| Error::InvalidArgument {
                                message: format!(
                                    "Failed to read the organization selection: choice {index} \
                                     is out of range for {} organizations.",
                                    eligible.len()
                                ),
                            })?;
                    Ok(OrgResolution::Selected(chosen))
                }
                None => Ok(OrgResolution::Cancelled),
            }
        }
    }
}

/// Resolution when the API serves no organization listing.
///
/// Without a listing there are no roles to check, so the CLI can offer only
/// what it can prove: an explicit `--org` is membership-checked server-side
/// with the unverifiable role reported out loud, and the only organization
/// offerable interactively is the one the credential itself belongs to —
/// confirmed, never assumed.
async fn resolve_without_discovery<D: OrgDiscovery, P: Prompter>(
    discovery: &D,
    explicit_org: Option<&str>,
    credential_org: Option<&str>,
    interactive: bool,
    prompter: &mut P,
) -> Result<OrgResolution> {
    if let Some(requested) = explicit_org {
        discovery.confirm_membership(requested).await?;
        // Membership is proven; the role is not — there is no listing to read
        // it from. Say so rather than silently weakening the owner/admin
        // gate. Spice Cloud re-validates the role before any enrollment
        // mutation commits, so an ineligible role still fails before side
        // effects.
        prompter.notify(format!(
            "Organization '{requested}': membership confirmed, but your role could not be \
             verified because organization discovery is unavailable. Spice Cloud enforces the \
             owner or admin requirement at enrollment."
        ));
        return Ok(OrgResolution::Selected(ConnectOrg {
            name: requested.to_string(),
            display_name: None,
            role: None,
        }));
    }

    let Some(own_org) = credential_org.filter(|org| !org.is_empty()) else {
        return Err(discovery_unavailable_error(None));
    };

    if !interactive {
        return Err(discovery_unavailable_error(Some(own_org)));
    }

    // The same weakened guarantee the explicit-`--org` branch reports: with no
    // listing, no role can be verified locally.
    prompter.notify(
        "Organization roles could not be verified because organization discovery is \
         unavailable. Spice Cloud enforces the owner or admin requirement at enrollment."
            .to_string(),
    );
    match prompter.confirm(own_org).await? {
        Some(true) => Ok(OrgResolution::Selected(ConnectOrg {
            name: own_org.to_string(),
            display_name: None,
            role: None,
        })),
        Some(false) | None => Ok(OrgResolution::Cancelled),
    }
}

/// The organizations whose membership role may connect an instance.
fn eligible_orgs(listing: &[Org]) -> Vec<ConnectOrg> {
    listing
        .iter()
        .filter_map(|org| {
            let role = org.role.as_deref().and_then(ConnectRole::parse)?;
            Some(ConnectOrg {
                name: org.name.clone(),
                display_name: org.display_name.clone(),
                role: Some(role),
            })
        })
        .collect()
}

/// Validate an explicitly named organization against the listing.
///
/// Distinguishes "member, but the role may not connect" from "not a member at
/// all" — the fix for the first is a role grant, for the second an invitation
/// or a corrected name — and fails before anything is minted or mutated.
fn validate_explicit(
    listing: &[Org],
    eligible: &[ConnectOrg],
    requested: &str,
) -> Result<ConnectOrg> {
    if let Some(org) = eligible
        .iter()
        .find(|org| org.name.eq_ignore_ascii_case(requested))
    {
        return Ok(org.clone());
    }

    if let Some(org) = listing
        .iter()
        .find(|org| org.name.eq_ignore_ascii_case(requested))
    {
        let role = org
            .role
            .as_deref()
            .map_or_else(|| "not reported".to_string(), str::to_string);
        return Err(Error::cloud_with_hint(
            CloudErrorCode::Forbidden,
            format!(
                "Failed to select organization {requested}: your membership role is '{role}', \
                 and connecting an instance requires owner or admin."
            ),
            format!(
                "Ask an owner of '{requested}' to grant you the admin role, or pass --org <name> \
                 for an organization where you already have it."
            ),
        ));
    }

    let hint = if eligible.is_empty() {
        "Run 'spice cloud orgs' to list the organizations you can access.".to_string()
    } else {
        format!(
            "Eligible organizations for this login: {}.",
            format_org_list(&org_names(eligible))
        )
    };
    // `OrgNotFound` matches what the membership probe answers for an org that
    // does not exist or is invisible to this login. (The probe can also answer
    // `OrgForbidden` for a real org the login is not a member of — a
    // distinction the listing cannot make, so callers must handle both.)
    Err(Error::cloud_with_hint(
        CloudErrorCode::OrgNotFound,
        format!("Failed to select organization {requested}: this login is not a member of it."),
        hint,
    ))
}

/// The chooser position to start on: the stored active org when it is
/// eligible, else the login's own org when it is. Position only — the user
/// still makes the choice.
fn highlight(
    eligible: &[ConnectOrg],
    stored_default: Option<&str>,
    credential_org: Option<&str>,
) -> Option<Highlight> {
    let position = |hint: Option<&str>| {
        hint.and_then(|hint| {
            eligible
                .iter()
                .position(|org| org.name.eq_ignore_ascii_case(hint))
        })
    };

    if let Some(index) = position(stored_default) {
        return Some(Highlight {
            index,
            source: HighlightSource::StoredDefault,
        });
    }
    position(credential_org).map(|index| Highlight {
        index,
        source: HighlightSource::LoginOrg,
    })
}

fn org_names(orgs: &[ConnectOrg]) -> Vec<String> {
    orgs.iter().map(|org| org.name.clone()).collect()
}

fn no_eligible_org_error(listing: &[Org]) -> Error {
    let memberships = if listing.is_empty() {
        "this login belongs to no organizations".to_string()
    } else {
        format!(
            "your memberships ({}) all carry roles that may not enroll",
            format_org_list(
                &listing
                    .iter()
                    .map(|org| org.name.clone())
                    .collect::<Vec<_>>()
            )
        )
    };
    Error::cloud_with_hint(
        CloudErrorCode::Forbidden,
        format!(
            "Failed to resolve an organization for this login: connecting an instance requires \
             the owner or admin role, and {memberships}."
        ),
        "Ask an organization owner to grant you the admin role, then re-run the command.",
    )
}

// The two errors below say "non-interactive" rather than blaming the
// terminal: the resolution is non-interactive when stdin is not a terminal
// *or* when the caller asked for a non-interactive run, and the fix — `--org`
// — is the same either way.

fn ambiguous_without_terminal_error(eligible: &[ConnectOrg]) -> Error {
    Error::cloud_with_hint(
        CloudErrorCode::InvalidRequest,
        format!(
            "Failed to resolve an organization for this login: {} are eligible, and this run is \
             non-interactive, so the choice cannot be prompted for.",
            format_org_list(&org_names(eligible))
        ),
        "Pass --org <name> to choose the organization non-interactively.",
    )
}

fn discovery_unavailable_error(credential_org: Option<&str>) -> Error {
    let hint = match credential_org {
        Some(org) => format!(
            "Pass --org <name> to name the organization explicitly (this login belongs to \
             '{org}')."
        ),
        None => "Pass --org <name> to name the organization explicitly.".to_string(),
    };
    Error::cloud_with_hint(
        CloudErrorCode::InvalidRequest,
        "Failed to resolve an organization for this login: Spice Cloud did not serve the \
         organization listing, and this run is non-interactive, so the organization cannot be \
         confirmed interactively.",
        hint,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scripted discovery source: a fixed listing (or `None` for
    /// "unavailable") and a set of orgs the membership probe accepts.
    struct FakeDiscovery {
        listing: Option<Vec<Org>>,
        members: Vec<&'static str>,
    }

    impl FakeDiscovery {
        fn serving(listing: Vec<Org>) -> Self {
            Self {
                listing: Some(listing),
                members: Vec::new(),
            }
        }

        fn unavailable(members: Vec<&'static str>) -> Self {
            Self {
                listing: None,
                members,
            }
        }
    }

    impl OrgDiscovery for FakeDiscovery {
        async fn list_orgs(&self) -> Result<Option<Vec<Org>>> {
            Ok(self.listing.clone())
        }

        async fn confirm_membership(&self, org: &str) -> Result<()> {
            if self.members.iter().any(|m| m.eq_ignore_ascii_case(org)) {
                Ok(())
            } else {
                Err(Error::cloud(
                    CloudErrorCode::OrgForbidden,
                    format!("You are not a member of organization '{org}'."),
                ))
            }
        }
    }

    /// A prompter that must never be asked anything. Proves a path resolved
    /// (or failed) without prompting. Notifications are output, not prompts,
    /// and are ignored here.
    struct NoPrompt;

    impl Prompter for NoPrompt {
        async fn choose(
            &mut self,
            _options: &[ConnectOrg],
            _highlight: Option<Highlight>,
        ) -> Result<Option<usize>> {
            panic!("this path must not prompt");
        }

        async fn confirm(&mut self, _org: &str) -> Result<Option<bool>> {
            panic!("this path must not prompt");
        }

        fn notify(&mut self, _line: String) {}
    }

    /// A prompter that records what it was asked and told, and answers from a
    /// script.
    struct ScriptedPrompt {
        answer: Option<usize>,
        confirm_answer: Option<bool>,
        asked: Vec<(Vec<ConnectOrg>, Option<Highlight>)>,
        confirmed: Vec<String>,
        notices: Vec<String>,
    }

    impl ScriptedPrompt {
        fn choosing(answer: Option<usize>) -> Self {
            Self {
                answer,
                confirm_answer: None,
                asked: Vec::new(),
                confirmed: Vec::new(),
                notices: Vec::new(),
            }
        }

        fn confirming(answer: Option<bool>) -> Self {
            Self {
                answer: None,
                confirm_answer: answer,
                asked: Vec::new(),
                confirmed: Vec::new(),
                notices: Vec::new(),
            }
        }
    }

    impl Prompter for ScriptedPrompt {
        async fn choose(
            &mut self,
            options: &[ConnectOrg],
            highlight: Option<Highlight>,
        ) -> Result<Option<usize>> {
            self.asked.push((options.to_vec(), highlight));
            Ok(self.answer)
        }

        async fn confirm(&mut self, org: &str) -> Result<Option<bool>> {
            self.confirmed.push(org.to_string());
            Ok(self.confirm_answer)
        }

        fn notify(&mut self, line: String) {
            self.notices.push(line);
        }
    }

    fn org(name: &str, role: Option<&str>) -> Org {
        Org {
            id: None,
            name: name.to_string(),
            display_name: None,
            role: role.map(str::to_string),
        }
    }

    async fn resolve<P: Prompter>(
        discovery: &FakeDiscovery,
        explicit: Option<&str>,
        stored_default: Option<&str>,
        credential_org: Option<&str>,
        interactive: bool,
        prompter: &mut P,
    ) -> Result<OrgResolution> {
        resolve_with(
            discovery,
            explicit,
            stored_default,
            credential_org,
            interactive,
            prompter,
        )
        .await
    }

    fn selected(resolution: Result<OrgResolution>) -> ConnectOrg {
        match resolution.expect("resolution should succeed") {
            OrgResolution::Selected(org) => org,
            OrgResolution::Cancelled => panic!("expected a selection, got a cancellation"),
        }
    }

    // ------------------------------------------------------------------
    // Role filtering
    // ------------------------------------------------------------------

    /// Only owner and admin may enroll, compared case-insensitively; member,
    /// viewer, deleted, unknown, and role-less memberships are excluded.
    #[test]
    fn eligibility_keeps_owner_and_admin_case_insensitively() {
        let listing = vec![
            org("a-owner", Some("owner")),
            org("b-admin", Some("Admin")),
            org("c-owner-caps", Some("OWNER")),
            org("d-member", Some("member")),
            org("e-viewer", Some("viewer")),
            org("f-deleted", Some("deleted")),
            org("g-unknown", Some("superuser")),
            org("h-none", None),
        ];

        let eligible = eligible_orgs(&listing);

        let names = org_names(&eligible);
        assert_eq!(names, vec!["a-owner", "b-admin", "c-owner-caps"]);
        assert_eq!(eligible[0].role, Some(ConnectRole::Owner));
        assert_eq!(eligible[1].role, Some(ConnectRole::Admin));
    }

    /// Padded role strings still parse — the filter must not narrow on
    /// formatting drift.
    #[test]
    fn padded_roles_still_parse() {
        assert_eq!(ConnectRole::parse(" owner "), Some(ConnectRole::Owner));
        assert_eq!(ConnectRole::parse("ADMIN"), Some(ConnectRole::Admin));
        assert_eq!(ConnectRole::parse("member"), None);
        assert_eq!(ConnectRole::parse(""), None);
    }

    // ------------------------------------------------------------------
    // Explicit --org
    // ------------------------------------------------------------------

    /// An explicit eligible org resolves without prompting, whatever the
    /// terminal is.
    #[tokio::test]
    async fn an_explicit_eligible_org_skips_the_chooser() {
        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("owner")),
            org("globex", Some("admin")),
        ]);

        for interactive in [true, false] {
            let resolved = selected(
                resolve(
                    &discovery,
                    Some("globex"),
                    None,
                    Some("acme"),
                    interactive,
                    &mut NoPrompt,
                )
                .await,
            );
            assert_eq!(resolved.name, "globex");
            assert_eq!(resolved.role, Some(ConnectRole::Admin));
        }
    }

    /// `--org` is matched case-insensitively, but the resolved name is the
    /// API's spelling — the one every later request must carry.
    #[tokio::test]
    async fn an_explicit_org_matches_case_insensitively() {
        let discovery = FakeDiscovery::serving(vec![org("Acme", Some("owner"))]);

        let resolved =
            selected(resolve(&discovery, Some("ACME"), None, None, false, &mut NoPrompt).await);
        assert_eq!(resolved.name, "Acme");
    }

    /// A membership whose role may not enroll is rejected before anything is
    /// minted or mutated, and the error names the actual role.
    #[tokio::test]
    async fn an_explicit_ineligible_org_fails_before_any_mutation() {
        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("owner")),
            org("globex", Some("member")),
        ]);

        let err = resolve(
            &discovery,
            Some("globex"),
            None,
            Some("acme"),
            true,
            &mut NoPrompt,
        )
        .await
        .expect_err("a member role must not connect an instance");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::Forbidden));
        let rendered = err.to_string();
        assert!(
            rendered.contains("'member'") && rendered.contains("owner or admin"),
            "the error should name the actual and the required roles: {rendered}"
        );
    }

    /// Naming an org the login does not belong to at all is its own failure,
    /// pointing at the eligible alternatives.
    #[tokio::test]
    async fn an_explicit_unknown_org_is_rejected_by_name() {
        let discovery = FakeDiscovery::serving(vec![org("acme", Some("owner"))]);

        let err = resolve(
            &discovery,
            Some("initech"),
            None,
            Some("acme"),
            true,
            &mut NoPrompt,
        )
        .await
        .expect_err("an unknown org must not resolve");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::OrgNotFound));
        let rendered = err.to_string();
        assert!(
            rendered.contains("initech") && rendered.contains("'acme'"),
            "the error should name the request and the eligible alternative: {rendered}"
        );
    }

    /// An org name unsafe for URLs and headers is rejected before any request
    /// carries it.
    #[tokio::test]
    async fn an_invalid_org_name_is_rejected_before_discovery() {
        let discovery = FakeDiscovery::serving(vec![org("acme", Some("owner"))]);

        let err = resolve(
            &discovery,
            Some("bad org name"),
            None,
            None,
            true,
            &mut NoPrompt,
        )
        .await
        .expect_err("an invalid org name must not resolve");
        assert_eq!(err.cloud_code(), Some(CloudErrorCode::InvalidRequest));
    }

    // ------------------------------------------------------------------
    // No explicit org
    // ------------------------------------------------------------------

    /// Exactly one eligible org resolves without prompting, on and off a
    /// terminal — and it is printed, because with no chooser and no flag that
    /// line is the only place the user sees the target.
    #[tokio::test]
    async fn a_single_eligible_org_resolves_without_prompting_and_is_printed() {
        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("owner")),
            org("globex", Some("member")),
        ]);

        for interactive in [true, false] {
            let mut prompt = ScriptedPrompt::choosing(None);
            let resolved = selected(
                resolve(
                    &discovery,
                    None,
                    None,
                    Some("acme"),
                    interactive,
                    &mut prompt,
                )
                .await,
            );
            assert_eq!(resolved.name, "acme");
            assert!(
                prompt.asked.is_empty(),
                "a single eligible org must not open a chooser"
            );
            assert_eq!(
                prompt.notices,
                vec!["Organization: acme (owner)".to_string()],
                "the single eligible org must be shown before anything acts on it"
            );
        }
    }

    /// No eligible org is an actionable failure, not an empty chooser.
    #[tokio::test]
    async fn no_eligible_org_is_an_actionable_error() {
        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("member")),
            org("globex", Some("viewer")),
        ]);

        let err = resolve(&discovery, None, None, Some("acme"), true, &mut NoPrompt)
            .await
            .expect_err("no eligible org must not resolve");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::Forbidden));
        let rendered = err.to_string();
        assert!(
            rendered.contains("owner or admin"),
            "the error should say which roles qualify: {rendered}"
        );
    }

    /// Multiple eligible orgs on a terminal require a real choice; the answer
    /// is the org the user picked.
    #[tokio::test]
    async fn multiple_eligible_orgs_require_a_choice_on_a_terminal() {
        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("owner")),
            org("globex", Some("admin")),
        ]);

        let mut prompt = ScriptedPrompt::choosing(Some(1));
        let resolved =
            selected(resolve(&discovery, None, None, Some("acme"), true, &mut prompt).await);

        assert_eq!(resolved.name, "globex");
        assert_eq!(prompt.asked.len(), 1, "exactly one chooser must be shown");
        let (options, _) = &prompt.asked[0];
        assert_eq!(org_names(options), vec!["acme", "globex"]);
    }

    /// The stored default org moves the chooser's cursor and nothing else: it
    /// is never selected on the user's behalf.
    #[tokio::test]
    async fn the_stored_default_is_highlighted_but_never_chosen() {
        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("owner")),
            org("globex", Some("admin")),
        ]);

        // The user cancels: the stored default must NOT be selected for them.
        let mut prompt = ScriptedPrompt::choosing(None);
        let resolution = resolve(
            &discovery,
            None,
            Some("globex"),
            Some("acme"),
            true,
            &mut prompt,
        )
        .await
        .expect("a cancelled chooser is not an error");

        assert!(
            matches!(resolution, OrgResolution::Cancelled),
            "cancelling must not fall back to the stored default: {resolution:?}"
        );
        let (_, highlight) = &prompt.asked[0];
        assert_eq!(
            *highlight,
            Some(Highlight {
                index: 1,
                source: HighlightSource::StoredDefault
            }),
            "the stored default should be where the cursor starts"
        );
    }

    /// Without a stored default the login's own org is the highlight — marked
    /// as such, so the chooser does not call it a default it is not; with
    /// neither eligible there is no highlight at all.
    #[test]
    fn the_highlight_falls_back_from_stored_default_to_credential_org() {
        let eligible = eligible_orgs(&[org("acme", Some("owner")), org("globex", Some("admin"))]);

        assert_eq!(
            highlight(&eligible, Some("globex"), Some("acme")),
            Some(Highlight {
                index: 1,
                source: HighlightSource::StoredDefault
            })
        );
        assert_eq!(
            highlight(&eligible, None, Some("acme")),
            Some(Highlight {
                index: 0,
                source: HighlightSource::LoginOrg
            })
        );
        assert_eq!(highlight(&eligible, Some("initech"), None), None);
        assert_eq!(highlight(&eligible, None, None), None);
    }

    /// The "(default)" chooser label appears only when the highlight really is
    /// the stored `spice cloud org use` state — never for the login's own org.
    #[test]
    fn the_default_label_marks_only_the_stored_default() {
        let eligible = eligible_orgs(&[org("acme", Some("owner")), org("globex", Some("admin"))]);

        let stored = chooser_items(
            &eligible,
            Some(Highlight {
                index: 1,
                source: HighlightSource::StoredDefault,
            }),
        );
        assert_eq!(stored, vec!["acme (owner)", "globex (admin) (default)"]);

        let login_org = chooser_items(
            &eligible,
            Some(Highlight {
                index: 1,
                source: HighlightSource::LoginOrg,
            }),
        );
        assert_eq!(
            login_org,
            vec!["acme (owner)", "globex (admin)"],
            "the login's own org must not be labelled a default it is not"
        );

        assert_eq!(
            chooser_items(&eligible, None),
            vec!["acme (owner)", "globex (admin)"]
        );
    }

    /// A chooser answer outside the option list is an error, never a panic —
    /// the prompter is an injectable boundary and cannot be trusted blindly.
    #[tokio::test]
    async fn an_out_of_range_chooser_answer_is_an_error() {
        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("owner")),
            org("globex", Some("admin")),
        ]);

        let mut prompt = ScriptedPrompt::choosing(Some(99));
        let err = resolve(&discovery, None, None, None, true, &mut prompt)
            .await
            .expect_err("an out-of-range choice must not resolve");
        assert!(
            err.to_string().contains("out of range"),
            "unexpected error: {err}"
        );
    }

    /// Off a terminal, ambiguity must fail fast and name `--org` — never hang
    /// on a prompt no one can answer.
    #[tokio::test]
    async fn multiple_eligible_orgs_without_a_terminal_name_the_flag() {
        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("owner")),
            org("globex", Some("admin")),
        ]);

        let err = resolve(&discovery, None, None, Some("acme"), false, &mut NoPrompt)
            .await
            .expect_err("ambiguity off a terminal must not resolve");

        let rendered = err.to_string();
        assert!(
            rendered.contains("--org"),
            "the error must name the flag that fixes it: {rendered}"
        );
        assert!(
            rendered.contains("'acme'") && rendered.contains("'globex'"),
            "the error should name the candidates: {rendered}"
        );
    }

    // ------------------------------------------------------------------
    // Discovery unavailable
    // ------------------------------------------------------------------

    /// With no listing to check roles against, an explicit org is
    /// membership-checked server-side and carries no locally proven role.
    #[tokio::test]
    async fn discovery_unavailable_with_explicit_org_probes_membership() {
        let discovery = FakeDiscovery::unavailable(vec!["acme"]);

        let mut prompt = ScriptedPrompt::choosing(None);
        let resolved = selected(
            resolve(
                &discovery,
                Some("acme"),
                None,
                Some("acme"),
                false,
                &mut prompt,
            )
            .await,
        );
        assert_eq!(resolved.name, "acme");
        assert_eq!(
            resolved.role, None,
            "no role can be proven without the listing"
        );
        // The weakened guarantee must be said out loud, not silently deferred
        // to the server.
        assert!(
            prompt
                .notices
                .iter()
                .any(|line| line.contains("could not be verified")),
            "the unverifiable role must be reported: {:?}",
            prompt.notices
        );
    }

    /// The membership probe's rejection propagates: a non-member org does not
    /// resolve just because discovery is down.
    #[tokio::test]
    async fn discovery_unavailable_rejects_a_non_member_org() {
        let discovery = FakeDiscovery::unavailable(vec!["acme"]);

        let err = resolve(
            &discovery,
            Some("initech"),
            None,
            Some("acme"),
            false,
            &mut NoPrompt,
        )
        .await
        .expect_err("a non-member org must not resolve");
        assert_eq!(err.cloud_code(), Some(CloudErrorCode::OrgForbidden));
    }

    /// Interactively, the only offerable org is the one the credential
    /// proves — and it is confirmed, never assumed, with the unverifiable
    /// role reported the same way the explicit-`--org` branch reports it.
    #[tokio::test]
    async fn discovery_unavailable_confirms_the_credential_org_interactively() {
        let discovery = FakeDiscovery::unavailable(vec![]);

        let mut prompt = ScriptedPrompt::confirming(Some(true));
        let resolved =
            selected(resolve(&discovery, None, None, Some("acme"), true, &mut prompt).await);

        assert_eq!(resolved.name, "acme");
        assert_eq!(prompt.confirmed, vec!["acme"]);
        assert!(
            prompt
                .notices
                .iter()
                .any(|line| line.contains("could not be verified")),
            "the unverifiable role must be reported: {:?}",
            prompt.notices
        );
    }

    /// Declining (or cancelling) the confirmation is a clean cancellation.
    #[tokio::test]
    async fn discovery_unavailable_confirmation_declined_is_cancelled() {
        let discovery = FakeDiscovery::unavailable(vec![]);

        for answer in [Some(false), None] {
            let mut prompt = ScriptedPrompt::confirming(answer);
            let resolution = resolve(&discovery, None, None, Some("acme"), true, &mut prompt)
                .await
                .expect("declining a confirmation is not an error");
            assert!(matches!(resolution, OrgResolution::Cancelled));
        }
    }

    /// Off a terminal with no listing and no flag, the resolution fails and
    /// names `--org` — it must not guess the credential's org.
    #[tokio::test]
    async fn discovery_unavailable_without_a_terminal_names_the_flag() {
        let discovery = FakeDiscovery::unavailable(vec![]);

        let err = resolve(&discovery, None, None, Some("acme"), false, &mut NoPrompt)
            .await
            .expect_err("no listing off a terminal must not resolve");

        let rendered = err.to_string();
        assert!(
            rendered.contains("--org") && rendered.contains("'acme'"),
            "the error must name the flag and the login's own org: {rendered}"
        );
    }

    // ------------------------------------------------------------------
    // Global org state
    // ------------------------------------------------------------------

    /// Resolving an organization must not touch the machine-wide active-org
    /// state: the choice is scoped to the invocation. Guards every path that
    /// completes a selection.
    #[tokio::test]
    async fn resolving_never_mutates_the_global_active_org() {
        let context_path = dirs::home_dir()
            .expect("home dir should resolve in tests")
            .join(".spice")
            .join("cloud-context.json");
        let before = std::fs::read(&context_path).ok();

        let discovery = FakeDiscovery::serving(vec![
            org("acme", Some("owner")),
            org("globex", Some("admin")),
        ]);

        // Explicit selection, single-org auto-selection, and a prompted
        // choice — all through the layer that also reads the stored default,
        // so a mutation slipped in beside that read is caught too.
        let _ = selected(
            resolve_with_stored_default(&discovery, Some("acme"), None, false, &mut NoPrompt).await,
        );
        let single = FakeDiscovery::serving(vec![org("acme", Some("owner"))]);
        let _ =
            selected(resolve_with_stored_default(&single, None, None, false, &mut NoPrompt).await);
        let mut prompt = ScriptedPrompt::choosing(Some(0));
        let _ =
            selected(resolve_with_stored_default(&discovery, None, None, true, &mut prompt).await);

        let after = std::fs::read(&context_path).ok();
        assert_eq!(
            before, after,
            "resolution must never write the machine-wide cloud context"
        );
    }

    // ------------------------------------------------------------------
    // Rendering
    // ------------------------------------------------------------------

    #[test]
    fn a_connect_org_renders_name_display_name_and_role() {
        let full = ConnectOrg {
            name: "acme".to_string(),
            display_name: Some("Acme Corp".to_string()),
            role: Some(ConnectRole::Owner),
        };
        assert_eq!(full.to_string(), "acme — Acme Corp (owner)");

        let bare = ConnectOrg {
            name: "acme".to_string(),
            display_name: None,
            role: None,
        };
        assert_eq!(bare.to_string(), "acme");

        // A display name that only repeats the name is noise, not signal.
        let echoed = ConnectOrg {
            name: "acme".to_string(),
            display_name: Some("Acme".to_string()),
            role: Some(ConnectRole::Admin),
        };
        assert_eq!(echoed.to_string(), "acme (admin)");
    }
}
