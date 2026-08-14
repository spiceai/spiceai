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

use std::sync::Mutex;

use tokio::sync::Notify;

use crate::identity::Identity;

/// The instance as Spice Cloud acknowledged it: the identifier the control
/// plane answered, plus the portal metadata this instance holds for it.
///
/// The metadata is read at acknowledgement time from the identity in force,
/// which carries what the last `AttachApp` (or the enrollment) recorded. An
/// attachment that lands later is a state change, not a correction to this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgedSession {
    /// The cloud-assigned instance identifier this session authenticated as.
    pub identifier: String,
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
    pub(crate) fn of_identity(identity: &Identity) -> Self {
        Self {
            identifier: identity.identifier.clone(),
            org_name: identity.org_name.clone(),
            app_name: identity.app_name.clone(),
            monitor_url: identity.monitor_url.clone(),
            new_project_url: identity.new_project_url.clone(),
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
