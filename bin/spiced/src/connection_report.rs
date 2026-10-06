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

//! The one line that says this instance is connected, and where to look at it.
//!
//! It is emitted by the runtime rather than by whatever started it, because
//! every way of starting one has to say the same thing: a plain `spice run` or
//! `spiced` in an enrolled directory and a service the supervisor started all
//! reach the same state and print the same block.
//!
//! Two facts have to be true before it is honest, and they arrive in either
//! order: the runtime has finished its initial load and bound its HTTP listener
//! (it is serving), and Spice Cloud has answered this process (it is managed). Whichever is second
//! releases the report; if the process shuts down before both, there is nothing
//! true to say and nothing is printed.

use std::borrow::Cow;
use std::sync::Arc;

use runtime_cloud_connect::{AcknowledgedSession, SessionAck};
use tokio_util::sync::CancellationToken;

/// The completion report's two lines: what this instance is connected to, and
/// where to go next.
///
/// Every destination is a Cloud-constructed URL carried in the identity — the
/// runtime never assembles a portal route, because the portal owns its
/// environments and routes and a URL invented here is one that quietly 404s.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CompletionReport {
    /// What this instance is attached to.
    pub(crate) headline: String,
    /// Where to see or finish it, when the Cloud supplied a destination.
    pub(crate) link: Option<String>,
}

impl CompletionReport {
    /// The report for an acknowledged session.
    pub(crate) fn of(session: &AcknowledgedSession) -> Self {
        // An org name is what makes the headline mean anything, so an instance
        // whose control plane never named one is reported by its identifier —
        // still true, still enough to find it in the portal.
        let org = session.org_name.as_deref().map_or_else(
            || format!("instance {}", terminal_text(&session.identifier)),
            |org| terminal_text(org).into_owned(),
        );

        // `app_id` is the attachment; the name only labels it, and an
        // attachment can arrive without one. So the name falls back the way
        // `org` does, rather than an instance that holds a project being told it
        // has none and offered a link to create the one it already has.
        if session.app_id.is_some() {
            let app = session
                .app_name
                .as_deref()
                .map_or(Cow::Borrowed("attached project"), terminal_text);
            Self {
                headline: format!("Spice Cloud Connect: connected to {org} / {app}"),
                link: session
                    .monitor_url
                    .as_deref()
                    .map(|url| format!("Monitor: {url}")),
            }
        } else {
            Self {
                headline: format!(
                    "Spice Cloud Connect: connected to {org} — not yet attached to a project"
                ),
                link: session
                    .new_project_url
                    .as_deref()
                    .map(|url| format!("Create one: {url}")),
            }
        }
    }

    fn emit(&self) {
        tracing::info!("{}", self.headline);
        if let Some(link) = &self.link {
            tracing::info!("{link}");
        }
    }
}

/// Make control-plane metadata safe to interpolate into a terminal log line.
/// Printable Unicode is preserved; control characters become a visible
/// replacement rather than injecting lines or escape sequences.
fn terminal_text(value: &str) -> Cow<'_, str> {
    if !value.chars().any(char::is_control) {
        return Cow::Borrowed(value);
    }
    Cow::Owned(
        value
            .chars()
            .map(|character| {
                if character.is_control() {
                    '�'
                } else {
                    character
                }
            })
            .collect(),
    )
}

/// Report the connection once the runtime is serving and Spice Cloud has
/// answered, on a task of its own.
///
/// `serving` is cancelled when the initial load settles and a listener binds; `withdrawn`
/// when this process may no longer report itself as connected — the servers
/// have stopped, or the process is going down. The report is emitted exactly
/// once, and only if `withdrawn` is not the first of them to fire.
pub(crate) fn spawn(
    session_ack: Arc<SessionAck>,
    serving: CancellationToken,
    withdrawn: CancellationToken,
) {
    if already_reported() {
        tracing::debug!(
            "The process that started this runtime has already reported the Cloud connection; not repeating it"
        );
        return;
    }

    tokio::spawn(async move {
        if let Some(session) = await_completion(&session_ack, &serving, &withdrawn).await {
            // Resolved now rather than at acknowledgement: an `AttachApp` can
            // land between the two, and the block has to name the project this
            // instance is attached to when it prints — not the one it was
            // attached to when Spice Cloud first answered.
            CompletionReport::of(&session.refreshed().await).emit();
        }
    });
}

/// Whether the process that started this runtime has already told the operator
/// that this instance is connected.
///
/// A launcher can print the block before handing the terminal to the runtime,
/// so repeating it here would put the same two lines on the same terminal
/// twice. It is a private handshake from that launcher — never a documented
/// setting, and absent from direct `spiced` and `spice run` starts.
fn already_reported() -> bool {
    std::env::var_os("SPICE_CONNECTION_REPORTED").is_some_and(|value| !value.is_empty())
}

/// Wait for both halves of the completion, or for the report to be withdrawn.
///
/// `None` means the servers stopped or the process went down before both were
/// true, which is the one case where there is nothing honest to report: either
/// the runtime never finished coming up, or Spice Cloud never answered it.
async fn await_completion(
    session_ack: &SessionAck,
    serving: &CancellationToken,
    withdrawn: &CancellationToken,
) -> Option<AcknowledgedSession> {
    let session_withdrawn = session_ack.withdrawal();
    let session = tokio::select! {
        biased;
        () = withdrawn.cancelled() => return None,
        () = session_withdrawn.cancelled() => return None,
        session = session_ack.wait() => session,
    };
    tokio::select! {
        biased;
        () = withdrawn.cancelled() => return None,
        () = session_withdrawn.cancelled() => return None,
        () = serving.cancelled() => {},
    }
    if withdrawn.is_cancelled() || session_withdrawn.is_cancelled() {
        None
    } else {
        Some(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// `app_id` is what makes the instance attached; `app_name` only labels the
    /// attachment, and the two are supplied separately because the control plane
    /// can deliver the first without the second.
    fn session(app_id: Option<&str>, app_name: Option<&str>) -> AcknowledgedSession {
        AcknowledgedSession {
            identifier: "inst_abc".to_string(),
            // No identity on disk: `refreshed` then falls back to this
            // snapshot, which is what these cases are about.
            identity_path: std::path::PathBuf::from("/nonexistent/identity.json"),
            org_name: Some("acme".to_string()),
            app_id: app_id.map(str::to_string),
            app_name: app_name.map(str::to_string),
            monitor_url: Some("https://spice.ai/acme/edge/monitor".to_string()),
            new_project_url: Some("https://spice.ai/acme/new?instance=inst_abc".to_string()),
        }
    }

    #[test]
    fn an_attached_instance_is_reported_with_its_project_and_monitor_page() {
        let report = CompletionReport::of(&session(Some("14034"), Some("edge")));
        assert_eq!(
            report.headline,
            "Spice Cloud Connect: connected to acme / edge"
        );
        assert_eq!(
            report.link.as_deref(),
            Some("Monitor: https://spice.ai/acme/edge/monitor")
        );
    }

    #[test]
    fn a_detached_instance_is_sent_to_create_a_project() {
        let report = CompletionReport::of(&session(None, None));
        assert_eq!(
            report.headline,
            "Spice Cloud Connect: connected to acme — not yet attached to a project"
        );
        assert_eq!(
            report.link.as_deref(),
            Some("Create one: https://spice.ai/acme/new?instance=inst_abc")
        );
        // The monitor URL belongs to an attachment this instance does not have.
        assert!(
            !report
                .link
                .as_deref()
                .unwrap_or_default()
                .contains("monitor")
        );
    }

    /// An attachment the control plane delivered without its display metadata.
    /// The instance holds a project, so reporting it as unattached — and
    /// offering to create the project it already has — is the failure this
    /// guards. `monitor_url` is absent here because it arrives with the name it
    /// is missing.
    #[test]
    fn an_attachment_the_cloud_named_nothing_for_is_still_reported_as_attached() {
        let mut unlabelled = session(Some("14034"), None);
        unlabelled.monitor_url = None;
        let report = CompletionReport::of(&unlabelled);

        assert_eq!(
            report.headline,
            "Spice Cloud Connect: connected to acme / attached project"
        );
        assert!(
            !report.headline.contains("not yet attached"),
            "an instance holding a project must never be told it has none: {}",
            report.headline
        );
        // No monitor page was supplied, and the create-a-project link is for an
        // instance without one — so there is nothing honest to offer.
        assert_eq!(report.link, None);
    }

    #[test]
    fn control_plane_names_cannot_inject_terminal_lines_or_escapes() {
        let mut acknowledged = session(Some("14034"), Some("edge\n\u{1b}[31m"));
        acknowledged.org_name = Some("acme\radmin".to_string());
        let report = CompletionReport::of(&acknowledged);

        assert!(!report.headline.contains('\n'));
        assert!(!report.headline.contains('\r'));
        assert!(!report.headline.contains('\u{1b}'));
        assert_eq!(
            report.headline,
            "Spice Cloud Connect: connected to acme�admin / edge��[31m"
        );
    }

    #[test]
    fn a_destination_is_never_invented_when_the_cloud_supplied_none() {
        let mut detached = session(None, None);
        detached.new_project_url = None;
        assert!(CompletionReport::of(&detached).link.is_none());

        let mut attached = session(Some("14034"), Some("edge"));
        attached.monitor_url = None;
        assert!(CompletionReport::of(&attached).link.is_none());
    }

    #[test]
    fn an_instance_whose_org_was_never_named_is_still_identified() {
        let mut anonymous = session(None, None);
        anonymous.org_name = None;
        assert_eq!(
            CompletionReport::of(&anonymous).headline,
            "Spice Cloud Connect: connected to instance inst_abc — not yet attached to a project"
        );
    }

    /// What order the three signals fire in.
    enum Order {
        AcknowledgedThenServing,
        ServingThenAcknowledged,
        AcknowledgedOnly,
        ServingOnly,
        Neither,
    }

    /// Drive the signals in one order and report whether a completion resolved.
    async fn completion_under(order: Order) -> Option<AcknowledgedSession> {
        let ack = Arc::new(SessionAck::new());
        let serving = CancellationToken::new();
        let shutdown = CancellationToken::new();

        let waiter = tokio::spawn({
            let ack = Arc::clone(&ack);
            let serving = serving.clone();
            let shutdown = shutdown.clone();
            async move { await_completion(&ack, &serving, &shutdown).await }
        });

        match order {
            Order::AcknowledgedThenServing => {
                ack.record(session(Some("14034"), Some("edge")));
                tokio::task::yield_now().await;
                serving.cancel();
            }
            Order::ServingThenAcknowledged => {
                serving.cancel();
                tokio::task::yield_now().await;
                ack.record(session(Some("14034"), Some("edge")));
            }
            // Each half alone is not a completion: the process goes down with
            // one of the two facts never established.
            Order::AcknowledgedOnly => {
                ack.record(session(Some("14034"), Some("edge")));
                tokio::task::yield_now().await;
                shutdown.cancel();
            }
            Order::ServingOnly => {
                serving.cancel();
                tokio::task::yield_now().await;
                shutdown.cancel();
            }
            Order::Neither => shutdown.cancel(),
        }

        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the waiter must settle")
            .expect("the waiter task must not panic")
    }

    #[tokio::test]
    async fn the_report_waits_for_both_latches_in_either_order() {
        // Spice Cloud can answer before the load settles or after it, and
        // neither ordering may lose the report.
        assert!(
            completion_under(Order::AcknowledgedThenServing)
                .await
                .is_some()
        );
        assert!(
            completion_under(Order::ServingThenAcknowledged)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn shutdown_before_both_latches_reports_nothing() {
        assert!(completion_under(Order::AcknowledgedOnly).await.is_none());
        assert!(completion_under(Order::ServingOnly).await.is_none());
        assert!(completion_under(Order::Neither).await.is_none());
    }

    #[tokio::test]
    async fn withdrawal_wins_when_every_signal_is_already_ready() {
        let ack = SessionAck::new();
        let serving = CancellationToken::new();
        let withdrawn = CancellationToken::new();
        ack.record(session(Some("14034"), Some("edge")));
        serving.cancel();
        withdrawn.cancel();

        assert!(await_completion(&ack, &serving, &withdrawn).await.is_none());
    }
}
