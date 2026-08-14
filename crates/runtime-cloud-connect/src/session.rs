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

//! The latch that reports when Spice Cloud first answered this process.
//!
//! A connection is not established when the transport opens: the gateway
//! accepts the mTLS connection before it has decided anything about the
//! instance, so a stream that is up says only that a socket exists. What says
//! the control plane has this session is the first message it sends back — the
//! `Ack` for the `Hello`, or any command dispatched to it, both of which are
//! reachable only through a session the control plane holds.
//!
//! Held as a latch rather than delivered as a log line inside the client
//! because the completion an operator waits for is *two* facts: the runtime is
//! serving, and Spice Cloud is attached to it. The two arrive in either order,
//! and either can fail to arrive at all — so the process that reports them
//! waits on both and prints once.
//!
//! The latch records *that* the control plane answered, and a snapshot of the
//! portal metadata at that moment. It deliberately does not fix that metadata:
//! the first message can be the very `AttachApp` that attaches this instance,
//! and more can land while the runtime is still loading, so a report is
//! resolved from the durable identity when it prints
//! ([`AcknowledgedSession::refreshed`]).

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tokio::sync::Notify;

use crate::identity::{Identity, IdentityStore};

/// The instance as Spice Cloud acknowledged it: the identifier the control
/// plane answered, plus the portal metadata this instance holds for it.
///
/// The metadata is a *snapshot* taken when the acknowledgement arrived. It can
/// be superseded within the same session — the control plane's first message is
/// often the `AttachApp` that attaches this instance to a project — so a
/// consumer that reports it later re-reads it with [`Self::refreshed`] instead
/// of presenting the snapshot as current.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgedSession {
    /// The cloud-assigned instance identifier this session authenticated as.
    pub identifier: String,
    /// The durable identity this session's metadata came from, and where
    /// [`Self::refreshed`] re-reads it.
    pub identity_path: PathBuf,
    /// The organization this instance is enrolled in, when one is recorded.
    pub org_name: Option<String>,
    /// The attached project, when this instance is attached to one.
    pub app_name: Option<String>,
    /// The portal page for the attached project. Cloud-constructed; never
    /// derived locally.
    pub monitor_url: Option<String>,
    /// The portal page for creating a project for this instance, when the
    /// enrollment reported one. Cloud-constructed; never derived locally.
    pub new_project_url: Option<String>,
}

impl AcknowledgedSession {
    /// The session an acknowledgement on `identity` describes.
    #[must_use]
    pub(crate) fn of_identity(identity: &Identity, identity_path: &Path) -> Self {
        Self {
            identifier: identity.identifier.clone(),
            identity_path: identity_path.to_path_buf(),
            org_name: identity.org_name.clone(),
            app_name: identity.app_name.clone(),
            monitor_url: identity.monitor_url.clone(),
            new_project_url: identity.new_project_url.clone(),
        }
    }

    /// The same session with its portal metadata re-read from the durable
    /// identity.
    ///
    /// An `AttachApp` persists the attachment as it is handled, which can be
    /// after this session was acknowledged and before anything reports it. The
    /// file is therefore the current answer and the snapshot is only the
    /// fallback — used when the identity cannot be read, or when the identifier
    /// on disk is not this session's any more (a removal and re-enrollment),
    /// where mixing the two would describe an instance that never existed.
    #[must_use]
    pub async fn refreshed(&self) -> Self {
        match IdentityStore::load_optional_async(self.identity_path.clone()).await {
            Ok(Some(identity)) if identity.identifier == self.identifier => {
                Self::of_identity(&identity, &self.identity_path)
            }
            _ => self.clone(),
        }
    }
}

/// One-shot record of the first control-plane acknowledgement of this process.
///
/// First write wins: reconnects re-acknowledge the same instance, and a
/// completion an operator has already read must not be re-announced (nor
/// rewritten) every time the stream flaps.
#[derive(Debug, Default)]
pub struct SessionAck {
    acknowledged: Mutex<Option<AcknowledgedSession>>,
    notify: Notify,
}

impl SessionAck {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The acknowledgement, if one has already arrived.
    #[must_use]
    pub fn get(&self) -> Option<AcknowledgedSession> {
        self.acknowledged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Wait for the control plane's first acknowledgement.
    ///
    /// Never resolves if none arrives — an instance the control plane does not
    /// answer has nothing to report — so callers race this against shutdown.
    pub async fn wait(&self) -> AcknowledgedSession {
        loop {
            // Registered before the check, so an acknowledgement that lands in
            // the window between them still wakes this waiter.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(session) = self.get() {
                return session;
            }
            notified.await;
        }
    }

    /// Record the first acknowledgement. Later calls are ignored.
    ///
    /// Filled by the control client on the messages Spice Cloud sends back.
    /// Public so the process that reports the connection can also exercise the
    /// latch it waits on.
    pub fn record(&self, session: AcknowledgedSession) {
        {
            let mut held = self
                .acknowledged
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if held.is_some() {
                return;
            }
            *held = Some(session);
        }
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn session(identifier: &str) -> AcknowledgedSession {
        AcknowledgedSession {
            identifier: identifier.to_string(),
            identity_path: PathBuf::from("/nonexistent/identity.json"),
            org_name: Some("acme".to_string()),
            app_name: None,
            monitor_url: None,
            new_project_url: None,
        }
    }

    #[tokio::test]
    async fn a_waiter_registered_before_the_acknowledgement_is_woken() {
        let ack = Arc::new(SessionAck::new());
        let waiter = tokio::spawn({
            let ack = Arc::clone(&ack);
            async move { ack.wait().await }
        });

        // Yield until the waiter is parked, then acknowledge.
        tokio::task::yield_now().await;
        ack.record(session("inst_a"));

        let observed = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("the waiter must be woken")
            .expect("the waiter task must not panic");
        assert_eq!(observed.identifier, "inst_a");
    }

    #[tokio::test]
    async fn an_acknowledgement_that_already_arrived_resolves_immediately() {
        let ack = SessionAck::new();
        ack.record(session("inst_b"));
        let observed = tokio::time::timeout(std::time::Duration::from_secs(5), ack.wait())
            .await
            .expect("an already-recorded acknowledgement must not wait");
        assert_eq!(observed.identifier, "inst_b");
    }

    #[tokio::test]
    async fn a_report_names_an_attachment_that_landed_after_the_acknowledgement() {
        // The control plane's first message is often the `AttachApp` that
        // attaches this instance, and more can land while the runtime is still
        // loading. A report resolved from the acknowledgement snapshot would
        // say "not yet attached" about an instance that is.
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let detached = crate::Identity {
            identifier: "inst_attach".to_string(),
            identity_cert_pem: "cert".to_string(),
            private_key_pem: "key".to_string(),
            public_key_pem: "public".to_string(),
            ca_bundle_pem: String::new(),
            gateway_addr: "gateway.example:443".to_string(),
            not_after_unix: None,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
            app_id: None,
            org_name: Some("acme".to_string()),
            app_name: None,
            monitor_url: None,
            new_project_url: Some("https://spice.ai/acme/new".to_string()),
        };
        IdentityStore::store(&path, &detached).expect("store the enrolled identity");

        let acknowledged = AcknowledgedSession::of_identity(&detached, &path);
        assert!(acknowledged.app_name.is_none());

        IdentityStore::set_attachment(
            &path,
            Some(&crate::AppAttachment {
                app_id: "4002".to_string(),
                org_name: Some("acme".to_string()),
                app_name: Some("edge".to_string()),
                monitor_url: Some("https://spice.ai/acme/edge/monitor".to_string()),
            }),
        )
        .expect("attach after the acknowledgement");

        let refreshed = acknowledged.refreshed().await;
        assert_eq!(refreshed.app_name.as_deref(), Some("edge"));
        assert_eq!(
            refreshed.monitor_url.as_deref(),
            Some("https://spice.ai/acme/edge/monitor")
        );
    }

    #[tokio::test]
    async fn a_replaced_identity_never_supplies_a_report_for_the_old_session() {
        // A removal and re-enrollment leaves a different instance at the same
        // path; mixing its metadata into this session's report would describe
        // an instance that never existed.
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let acknowledged = AcknowledgedSession {
            identifier: "inst_old".to_string(),
            identity_path: path.clone(),
            org_name: Some("acme".to_string()),
            app_name: None,
            monitor_url: None,
            new_project_url: None,
        };

        // Nothing on disk at all, and then a different instance.
        assert_eq!(acknowledged.refreshed().await, acknowledged);
        let replacement = crate::Identity {
            identifier: "inst_new".to_string(),
            identity_cert_pem: "cert".to_string(),
            private_key_pem: "key".to_string(),
            public_key_pem: "public".to_string(),
            ca_bundle_pem: String::new(),
            gateway_addr: "gateway.example:443".to_string(),
            not_after_unix: None,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
            app_id: Some("9001".to_string()),
            org_name: Some("other-org".to_string()),
            app_name: Some("other-project".to_string()),
            monitor_url: Some("https://spice.ai/other-org/other-project/monitor".to_string()),
            new_project_url: None,
        };
        IdentityStore::store(&path, &replacement).expect("store the replacement identity");
        assert_eq!(acknowledged.refreshed().await, acknowledged);
    }

    #[test]
    fn only_the_first_acknowledgement_is_kept() {
        // Reconnects re-acknowledge the same instance; the completion block is
        // printed once, from the first.
        let ack = SessionAck::new();
        ack.record(session("inst_first"));
        ack.record(session("inst_second"));
        assert_eq!(
            ack.get().expect("recorded").identifier,
            "inst_first".to_string()
        );
    }
}
