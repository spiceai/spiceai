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
///
/// When `client_auth_mode: required` is configured, two `ServerConfig`s are
/// built off the same cert resolver and CA bundle:
///
/// - [`Self::flight_server_config`] uses a strict
///   [`rustls::server::WebPkiClientVerifier`]: the TLS handshake fails
///   if the client does not present a cert. The Flight (gRPC) listener
///   uses this.
///
/// - [`Self::http_server_config`] uses an `allow_unauthenticated`
///   verifier: the handshake succeeds without a client cert
///   (presented-but-invalid certs are still rejected at the handshake).
///   The HTTP and metrics listeners use this. HTTP route middleware
///   gates non-probe routes on the presence of a verified client cert
///   so `/health` and `/v1/ready` remain reachable for Kubernetes
///   liveness/readiness probes that cannot mount a client certificate.
///
/// Under `client_auth_mode: request`, both listeners use the
/// `allow_unauthenticated` verifier and the HTTP route layer does
/// **not** gate non-probe routes — anonymous and identified clients
/// both reach the application. Presented certs are still verified
/// against the configured CA bundle and promoted to a request
/// principal under [`runtime_auth::IdentitySource::Channel`].
///
/// When `client_auth_mode` is unset, both fields point at an identical
/// `with_no_client_auth()` config.
pub struct TlsConfig {
    /// rustls config used by the Flight listener. Under
    /// `client_auth_mode: required` this enforces client-cert
    /// presence at the handshake. Under `request` it admits no-cert
    /// handshakes (presented certs are verified). Shared by
    /// reference; the contained `ResolvesServerCert` is the swap
    /// point for rotated certs.
    pub flight_server_config: Arc<ServerConfig>,

    /// rustls config used by the HTTP and metrics listeners. Under
    /// `client_auth_mode: required` and `request` this admits
    /// no-cert handshakes. Identical to [`Self::flight_server_config`]
    /// when `client_auth_mode` is unset.
    pub http_server_config: Arc<ServerConfig>,

    /// Whether the HTTP route layer should reject non-probe requests
    /// that arrived without a verified client cert.
    pub client_auth: ClientAuthEnforcement,

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

/// Whether and how the public TLS listeners enforce client
/// certificates. Drives both the per-listener rustls verifier choice
/// inside [`build_server_configs`] and the per-request gate on
/// [`crate::http::routes`] that 401s non-probe HTTP requests when the
/// connection presented no client cert. The Flight listener
/// independently consults its own (strict vs lax)
/// `WebPkiClientVerifier` baked into [`TlsConfig::flight_server_config`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuthEnforcement {
    /// `client_auth_mode` was not configured. Both listeners run with
    /// `with_no_client_auth()`. No route gate.
    Disabled,
    /// `client_auth_mode: request`. Both listeners use the
    /// `allow_unauthenticated` verifier so no-cert handshakes are
    /// admitted; presented certs are verified against the CA bundle.
    /// No route gate.
    Requested,
    /// `client_auth_mode: required`. Flight listener uses the strict
    /// verifier (no-cert handshakes rejected at TLS). HTTP and
    /// metrics listeners use the `allow_unauthenticated` verifier so
    /// probes work without a client cert; the HTTP route layer 401s
    /// any non-probe request whose connection has no
    /// [`runtime_auth::ChannelIdentity`].
    Required,
}

impl TlsConfig {
    /// Build from in-memory PEM bytes. Used for inline `runtime.tls.certificate`
    /// and `${secrets:...}`-sourced material. The returned config will not
    /// hot-reload (no path is being watched).
    pub fn try_new(
        cert_bytes: &[u8],
        key_bytes: &[u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        Self::try_new_with_client_auth(cert_bytes, key_bytes, None)
    }

    /// Like [`Self::try_new`] but additionally builds a static client
    /// cert verifier from `client_ca_pem` when supplied. Inline
    /// material; no hot-reload.
    pub fn try_new_with_client_auth(
        cert_bytes: &[u8],
        key_bytes: &[u8],
        client_ca_pem: Option<&[u8]>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let enforcement = enforcement_from(client_ca_pem.is_some());
        Self::try_new_with_client_auth_mode(cert_bytes, key_bytes, client_ca_pem, enforcement)
    }

    /// Like [`Self::try_new_with_client_auth`] but accepts an explicit
    /// [`ClientAuthEnforcement`] so callers (typically the binary
    /// entrypoint) can distinguish [`ClientAuthEnforcement::Requested`]
    /// from [`ClientAuthEnforcement::Required`] — both pass a CA
    /// bundle, only the per-listener verifier strictness and HTTP
    /// route gate differ.
    pub fn try_new_with_client_auth_mode(
        cert_bytes: &[u8],
        key_bytes: &[u8],
        client_ca_pem: Option<&[u8]>,
        enforcement: ClientAuthEnforcement,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        check_client_auth_invariant(client_ca_pem.is_some(), enforcement)?;
        let resolver = ReloadableServerCerts::from_pem_with_client_auth(
            cert_bytes,
            key_bytes,
            client_ca_pem,
            ReloadScope::Public,
        )
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        let (flight_cfg, http_cfg) = build_server_configs(&resolver, enforcement);
        Ok(Self {
            flight_server_config: Arc::new(flight_cfg),
            http_server_config: Arc::new(http_cfg),
            client_auth: enforcement,
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
        Self::try_new_from_paths_with_client_auth(cert_path, key_path, None, control)
    }

    /// Like [`Self::try_new_from_paths`] but additionally registers an
    /// optional client CA path. When `client_ca_path` is `Some`, the
    /// CA file participates in the same watcher as the cert + key:
    /// every change re-reads the whole bundle and the resulting
    /// `WebPkiClientVerifier` is atomically swapped under the live
    /// `ServerConfig` without rebuilding it.
    pub fn try_new_from_paths_with_client_auth(
        cert_path: PathBuf,
        key_path: PathBuf,
        client_ca_path: Option<PathBuf>,
        control: &TlsControl,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let enforcement = enforcement_from(client_ca_path.is_some());
        Self::try_new_from_paths_with_client_auth_mode(
            cert_path,
            key_path,
            client_ca_path,
            enforcement,
            control,
        )
    }

    /// Like [`Self::try_new_from_paths_with_client_auth`] but accepts
    /// an explicit [`ClientAuthEnforcement`] so callers can
    /// distinguish [`ClientAuthEnforcement::Requested`] from
    /// [`ClientAuthEnforcement::Required`].
    pub fn try_new_from_paths_with_client_auth_mode(
        cert_path: PathBuf,
        key_path: PathBuf,
        client_ca_path: Option<PathBuf>,
        enforcement: ClientAuthEnforcement,
        control: &TlsControl,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        check_client_auth_invariant(client_ca_path.is_some(), enforcement)?;
        let resolver = ReloadableServerCerts::from_paths_with_client_auth(
            cert_path,
            key_path,
            client_ca_path,
            ReloadScope::Public,
            control.watcher(),
        )
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        let (flight_cfg, http_cfg) = build_server_configs(&resolver, enforcement);
        Ok(Self {
            flight_server_config: Arc::new(flight_cfg),
            http_server_config: Arc::new(http_cfg),
            client_auth: enforcement,
            resolver,
            // Hold an Arc to the watcher so the dispatcher thread is kept
            // alive even if `TlsControl` is later dropped before us.
            watcher_keepalive: Some(Arc::clone(control.watcher())),
        })
    }
}

/// Build the (Flight, HTTP) `ServerConfig` pair from a single
/// [`ReloadableServerCerts`]. Wires the hot-reloadable cert resolver
/// into both, and — when client-cert auth is configured — attaches
/// a per-listener verifier shim chosen from `enforcement`:
///
/// - [`ClientAuthEnforcement::Disabled`] → both listeners run
///   `with_no_client_auth()`.
/// - [`ClientAuthEnforcement::Requested`] → both listeners use the
///   `allow_unauthenticated` (lax) verifier so no-cert handshakes
///   are admitted; presented certs are still verified.
/// - [`ClientAuthEnforcement::Required`] → Flight uses the strict
///   verifier (no-cert handshakes rejected at TLS); HTTP uses the
///   lax verifier so probes work without a client cert (the route
///   layer enforces presence on non-probe routes).
///
/// Both reads go through the same `ArcSwap` machinery so a cert or
/// CA rotation does not require rebuilding either `ServerConfig`.
fn build_server_configs(
    resolver: &Arc<ReloadableServerCerts>,
    enforcement: ClientAuthEnforcement,
) -> (ServerConfig, ServerConfig) {
    let flight_verifier = match enforcement {
        ClientAuthEnforcement::Disabled => None,
        ClientAuthEnforcement::Requested => resolver.client_verifier_lax(),
        ClientAuthEnforcement::Required => resolver.client_verifier(),
    };
    let http_verifier = match enforcement {
        ClientAuthEnforcement::Disabled => None,
        ClientAuthEnforcement::Requested | ClientAuthEnforcement::Required => {
            resolver.client_verifier_lax()
        }
    };
    let flight = match flight_verifier {
        Some(verifier) => ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(Arc::clone(resolver) as Arc<_>),
        None => ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::clone(resolver) as Arc<_>),
    };
    let http = match http_verifier {
        Some(verifier) => ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(Arc::clone(resolver) as Arc<_>),
        None => ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::clone(resolver) as Arc<_>),
    };
    let mut flight = flight;
    let mut http = http;
    configure_alpn(&mut flight);
    configure_alpn(&mut http);
    (flight, http)
}

fn enforcement_from(has_client_auth: bool) -> ClientAuthEnforcement {
    if has_client_auth {
        ClientAuthEnforcement::Required
    } else {
        ClientAuthEnforcement::Disabled
    }
}

/// Runtime check (not just `debug_assert`) that a client CA bundle
/// was supplied iff the operator asked for non-`Disabled` client
/// authentication. Misconfiguring this on the call path would
/// otherwise produce one of two silently-wrong outcomes:
///
/// - `Disabled` + a CA: the server quietly drops the operator's
///   trust roots and every client is admitted unauthenticated.
/// - `Requested` / `Required` + no CA: same outcome via the other
///   side of the match — [`build_server_configs`] would fall through
///   to `with_no_client_auth()` because no verifier could be built.
///
/// Either shape would silently weaken the security posture in
/// release builds, where `debug_assert!` is a no-op. Returning a
/// `Box<dyn Error>` keeps the existing constructor signature and
/// surfaces the misuse at startup instead.
fn check_client_auth_invariant(
    has_ca: bool,
    enforcement: ClientAuthEnforcement,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let wants_ca = !matches!(enforcement, ClientAuthEnforcement::Disabled);
    if has_ca == wants_ca {
        return Ok(());
    }
    Err(if wants_ca {
        format!(
            "TlsConfig: ClientAuthEnforcement::{enforcement:?} requires a client CA bundle, but none was supplied"
        )
        .into()
    } else {
        "TlsConfig: a client CA bundle was supplied but ClientAuthEnforcement::Disabled was requested; pass Requested or Required, or drop the CA"
            .into()
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invariant_ok_required_with_ca() {
        check_client_auth_invariant(true, ClientAuthEnforcement::Required)
            .expect("Required + CA is valid");
    }

    #[test]
    fn invariant_ok_requested_with_ca() {
        check_client_auth_invariant(true, ClientAuthEnforcement::Requested)
            .expect("Requested + CA is valid");
    }

    #[test]
    fn invariant_ok_disabled_without_ca() {
        check_client_auth_invariant(false, ClientAuthEnforcement::Disabled)
            .expect("Disabled + no CA is valid");
    }

    #[test]
    fn invariant_rejects_required_without_ca() {
        let err = check_client_auth_invariant(false, ClientAuthEnforcement::Required)
            .expect_err("Required without CA must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("Required") && msg.contains("requires a client CA"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn invariant_rejects_requested_without_ca() {
        let err = check_client_auth_invariant(false, ClientAuthEnforcement::Requested)
            .expect_err("Requested without CA must be rejected");
        assert!(err.to_string().contains("Requested"));
    }

    #[test]
    fn invariant_rejects_disabled_with_ca() {
        let err = check_client_auth_invariant(true, ClientAuthEnforcement::Disabled)
            .expect_err("Disabled with CA must be rejected");
        assert!(err.to_string().contains("Disabled"));
    }
}
