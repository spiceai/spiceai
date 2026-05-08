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

//! Public-endpoint TLS configuration for HTTP, Flight, and Metrics.
//!
//! [`TlsConfig`] is built once at startup and shared as `Arc<TlsConfig>`
//! across all three servers. The contained [`rustls::ServerConfig`] uses a
//! [`reload::ReloadableServerCerts`] resolver so the certificate can be
//! hot-swapped from disk without rebuilding the `ServerConfig` or
//! restarting any of the servers.

pub mod flight_incoming;
pub mod reload;

use std::{path::PathBuf, sync::Arc};

use rustls::ServerConfig;
use x509_certificate::X509Certificate;

pub use reload::{CertWatcher, ReloadError, ReloadScope, ReloadableServerCerts};
/// Process-wide TLS reload control plane.
///
/// `TlsControl` is the single owner of the underlying [`CertWatcher`] for
/// the whole process. Every TLS subsystem (public TLS, cluster mTLS, future
/// outbound mTLS clients) takes a `&TlsControl` at construction time so all
/// reload activity flows through one filesystem watcher — one dispatcher
/// thread, one poll loop, one place to wire SIGHUP.
///
/// The binary owns the [`TlsControl`]; subsystems borrow it. Operators
/// trigger a manual reload of every registered material via
/// [`TlsControl::reload_all`] (typically wired from `SIGHUP` in the
/// `spiced` binary).
pub struct TlsControl {
    watcher: Arc<CertWatcher>,
}

impl TlsControl {
    /// Spawn a fresh process-wide TLS control plane. Spawns the
    /// underlying [`CertWatcher`] dispatcher thread.
    pub fn new() -> Result<Self, ReloadError> {
        Ok(Self {
            watcher: Arc::new(CertWatcher::spawn()?),
        })
    }

    /// Force a synchronous reload of every TLS material currently being
    /// watched. Bypasses the filesystem-event debounce. Intended for
    /// SIGHUP-driven manual rotation pickup.
    pub fn reload_all(&self) -> Result<(), ReloadError> {
        self.watcher.trigger_reload_all()
    }

    /// Underlying watcher handle. Crate-private so the runtime
    /// boundary stays opaque — binary callers go through the high-level
    /// `reload_all` API.
    pub(crate) fn watcher(&self) -> &Arc<CertWatcher> {
        &self.watcher
    }
}

/// Public-endpoint TLS state.
///
/// Construct via [`TlsConfig::try_new`] for inline / secret-sourced PEMs,
/// or [`TlsConfig::try_new_from_paths`] when the cert + key live on disk
/// and should be hot-reloaded on rotation.
pub struct TlsConfig {
    /// rustls config installed on every server. Shared by reference; the
    /// contained `ResolvesServerCert` is the swap point for rotated certs.
    pub server_config: Arc<ServerConfig>,

    /// Resolver kept around so callers can introspect / force a reload from
    /// tests. Production callers do not need to touch this directly.
    resolver: Arc<ReloadableServerCerts>,

    /// Filesystem watcher kept alive for the lifetime of the TLS config.
    /// `None` for inline-bytes configs that cannot rotate. The watcher is
    /// owned process-wide by [`TlsControl`]; this `Arc` is purely a
    /// drop-guard so the dispatcher outlives this config if `TlsControl`
    /// is dropped first.
    watcher_keepalive: Option<Arc<CertWatcher>>,
}

impl TlsConfig {
    /// Build from in-memory PEM bytes. Used for inline `runtime.tls.certificate`
    /// and `${secrets:...}`-sourced material. The returned config will not
    /// hot-reload (no path is being watched).
    pub fn try_new(
        cert_bytes: &[u8],
        key_bytes: &[u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let resolver = ReloadableServerCerts::from_pem(cert_bytes, key_bytes, ReloadScope::Public)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        let mut server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::clone(&resolver) as Arc<_>);
        configure_alpn(&mut server_config);
        Ok(Self {
            server_config: Arc::new(server_config),
            resolver,
            watcher_keepalive: None,
        })
    }

    /// Build from on-disk PEMs and register file-change watching with
    /// `watcher`. When either path changes, both files are re-read and the
    /// rustls cert resolver is swapped atomically. In-flight TLS connections
    /// are unaffected; new handshakes pick up the new material.
    pub fn try_new_from_paths(
        cert_path: PathBuf,
        key_path: PathBuf,
        control: &TlsControl,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let resolver = ReloadableServerCerts::from_paths(
            cert_path,
            key_path,
            ReloadScope::Public,
            control.watcher(),
        )
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        let mut server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::clone(&resolver) as Arc<_>);
        configure_alpn(&mut server_config);
        Ok(Self {
            server_config: Arc::new(server_config),
            resolver,
            // Hold an Arc to the watcher so the dispatcher thread is kept
            // alive even if `TlsControl` is later dropped before us.
            watcher_keepalive: Some(Arc::clone(control.watcher())),
        })
    }
}

/// Advertise both `h2` (for tonic / Flight) and `http/1.1` (for axum HTTP &
/// metrics) via ALPN. tonic requires `h2` to be in the ALPN list to accept
/// gRPC connections; hyper transparently downgrades to HTTP/1.1 if `h2` is
/// not negotiated. Order matters: rustls picks the first server-listed
/// protocol that the client also offers, so prefer `h2`.
fn configure_alpn(config: &mut ServerConfig) {
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
}

impl TlsConfig {
    /// Subject CN of the currently-loaded leaf cert, for the startup log
    /// line. Reads the live (post-reload) chain.
    #[must_use]
    pub fn subject_name(&self) -> Option<String> {
        let chain = self.resolver.current_cert_chain();
        let leaf = chain.first()?;
        let x509 = X509Certificate::from_der(leaf.as_ref()).ok()?;
        x509.subject_name().user_friendly_str().ok()
    }

    /// For tests: force an immediate reload of the cert + key files.
    /// No-op for inline-bytes configs.
    pub fn force_reload_for_tests(&self) {
        self.resolver.reload_now();
    }

    /// Watcher kept alive by this config, exposed only for tests that
    /// need to introspect the watcher directly. Production callers go
    /// through [`TlsControl::reload_all`].
    #[doc(hidden)]
    #[must_use]
    pub fn watcher(&self) -> Option<Arc<CertWatcher>> {
        self.watcher_keepalive.as_ref().map(Arc::clone)
    }
}

impl AsRef<TlsConfig> for TlsConfig {
    fn as_ref(&self) -> &TlsConfig {
        self
    }
}
