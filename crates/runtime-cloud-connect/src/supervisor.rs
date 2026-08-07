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

//! What will relaunch this process after a deployment exits it.
//!
//! A deployment applies by persisting the spicepod and exiting, so whatever
//! supervises the process is the mechanism, not a convenience: with nothing
//! watching, a deployment *stops* the instance instead of updating it. The
//! runtime cannot install a supervisor, so it detects one and says what it
//! found — at startup, in `get_status`, and therefore in the portal — before
//! the first deployment is the way the operator finds out.
//!
//! Detection is evidence, not proof. A container is detectable; its restart
//! policy is not visible from inside it, so [`Supervisor::Container`] carries a
//! caveat rather than a guarantee.

use std::path::Path;

/// What was found watching this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Supervisor {
    /// A systemd unit: `Restart=always` brings the process back.
    Systemd,
    /// A Kubernetes pod: the kubelet restarts the container.
    Kubernetes,
    /// A container outside Kubernetes (Docker, Podman). Whether it comes back
    /// depends on a restart policy this process cannot read.
    Container,
    /// Nothing detected. A deployment will stop this instance.
    #[default]
    Undetected,
}

impl Supervisor {
    /// Detect from this process's environment.
    #[must_use]
    pub fn detect() -> Self {
        Self::classify(
            &|key| std::env::var_os(key).is_some_and(|value| !value.is_empty()),
            &|path| Path::new(path).exists(),
        )
    }

    /// The detection itself, over injected probes so it can be tested without
    /// a systemd unit or a container to run inside.
    ///
    /// Ordered by how specific the evidence is: `INVOCATION_ID` names a systemd
    /// unit outright, a Kubernetes pod is restarted by the kubelet rather than
    /// by the container runtime's own policy, and the container markers are the
    /// weakest signal — they say "something built this filesystem", not "something
    /// will restart it".
    #[must_use]
    pub fn classify(
        env_var_set: &dyn Fn(&str) -> bool,
        path_exists: &dyn Fn(&str) -> bool,
    ) -> Self {
        // Set by systemd for every unit it starts (v232+); JOURNAL_STREAM is the
        // fallback for a unit whose stdout is journald on an older systemd.
        if env_var_set("INVOCATION_ID") || env_var_set("JOURNAL_STREAM") {
            return Self::Systemd;
        }
        if env_var_set("KUBERNETES_SERVICE_HOST") {
            return Self::Kubernetes;
        }
        if path_exists("/.dockerenv") || path_exists("/run/.containerenv") {
            return Self::Container;
        }
        Self::Undetected
    }

    /// Stable identifier for the `get_status` document.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Systemd => "systemd",
            Self::Kubernetes => "kubernetes",
            Self::Container => "container",
            Self::Undetected => "undetected",
        }
    }

    /// Whether anything was found that is expected to relaunch the process.
    ///
    /// `true` for [`Supervisor::Container`] on the evidence available: a
    /// container this runtime is deployed in is nearly always run under a
    /// restart policy, and [`Supervisor::caveat`] carries the qualification the
    /// bare answer cannot.
    #[must_use]
    pub fn is_supervised(self) -> bool {
        !matches!(self, Self::Undetected)
    }

    /// What an operator needs to be told, or `None` when the answer needs no
    /// qualification.
    #[must_use]
    pub fn caveat(self) -> Option<&'static str> {
        match self {
            Self::Systemd | Self::Kubernetes => None,
            Self::Container => Some(
                "Running in a container: a deployment exits this process, so the container must \
                 have a restart policy (`docker run --restart unless-stopped`) or the deployment \
                 will stop the instance instead of updating it. See: https://spiceai.org/docs",
            ),
            Self::Undetected => Some(
                "No process supervisor detected: a deployment exits this process to apply, so it \
                 will stop this instance instead of restarting it. Install the service with \
                 `sudo spice connect --install`, or run spiced under your own supervisor \
                 (systemd, or a container with a restart policy). See: https://spiceai.org/docs",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn none(_: &str) -> bool {
        false
    }

    #[test]
    fn systemd_is_detected_from_either_marker() {
        assert_eq!(
            Supervisor::classify(&|key| key == "INVOCATION_ID", &none),
            Supervisor::Systemd
        );
        assert_eq!(
            Supervisor::classify(&|key| key == "JOURNAL_STREAM", &none),
            Supervisor::Systemd
        );
    }

    #[test]
    fn kubernetes_outranks_the_container_markers() {
        // A pod is a container, but it is the kubelet that restarts it — and
        // unlike a bare container, that restart is guaranteed.
        let supervisor = Supervisor::classify(&|key| key == "KUBERNETES_SERVICE_HOST", &|path| {
            path == "/.dockerenv"
        });
        assert_eq!(supervisor, Supervisor::Kubernetes);
        assert!(supervisor.caveat().is_none());
    }

    #[test]
    fn a_bare_container_is_supervised_with_a_caveat() {
        for marker in ["/.dockerenv", "/run/.containerenv"] {
            let supervisor = Supervisor::classify(&none, &|path| path == marker);
            assert_eq!(supervisor, Supervisor::Container, "marker {marker}");
            assert!(
                supervisor.is_supervised(),
                "a container is treated as supervised…"
            );
            assert!(
                supervisor.caveat().is_some(),
                "…but the restart policy is not readable from inside it, so say so"
            );
        }
    }

    #[test]
    fn nothing_detected_reports_unsupervised_with_the_fix() {
        let supervisor = Supervisor::classify(&none, &none);
        assert_eq!(supervisor, Supervisor::Undetected);
        assert!(!supervisor.is_supervised());
        assert_eq!(supervisor.as_str(), "undetected");
        let caveat = supervisor
            .caveat()
            .expect("an unsupervised host is told so");
        assert!(
            caveat.contains("spice connect --install"),
            "the warning must name the fix: {caveat}"
        );
    }

    #[test]
    fn detect_reads_the_real_environment_without_panicking() {
        // The value depends on where the tests run; the contract under test is
        // that detection is total and its label is one of the known ones.
        let detected = Supervisor::detect();
        assert!(
            ["systemd", "kubernetes", "container", "undetected"].contains(&detected.as_str()),
            "unexpected label {}",
            detected.as_str()
        );
    }
}
