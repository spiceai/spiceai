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

//! On-disk certificate hot-reload for TLS endpoints.
//!
//! Lets `spiced` swap TLS certificates (and the client-cert trust
//! roots and certificate revocation lists used for mTLS) at runtime
//! without a restart.
//!
//! ## Pieces
//!
//! - [`ReloadableServerCerts`] implements [`rustls::server::ResolvesServerCert`]
//!   over an [`arc_swap::ArcSwap<rustls::sign::CertifiedKey>`]. Plug it into a
//!   `ServerConfig::builder().with_no_client_auth().with_cert_resolver(...)`
//!   pipeline once at startup; new TLS handshakes pick up the swapped cert
//!   automatically. In-flight connections keep using the cert they were
//!   established with.
//!
//! - Cluster mTLS uses `ClusterPkiBundle` (in the runtime cluster PKI), which
//!   atomically swaps server cert + client verifier + outbound
//!   `ClientTlsConfig` together via a single [`ArcSwap`] of a snapshot.
//!
//! - [`CertWatcher`] watches a set of file paths via the `notify` crate,
//!   debounces filesystem events, and invokes a registered reload callback
//!   when any of them changes. Atomic-rename writes (`mv tmp dst`) and
//!   in-place modifications are both handled; the watcher re-arms after a
//!   `Remove` since SPIRE / cert-manager / kubelet all rotate via rename.
//!
//! ## Failure mode
//!
//! If a reload sees malformed PEM, the old material keeps serving and a
//! `warn!` is logged with the file path. The `runtime_tls_reload_total`
//! metric increments with `result="parse_error"` so operators can alert.
//! `spiced` never goes down because a rotation produced a bad file. A bad
//! CRL rotation keeps the previous CRLs, so a broken CRL publisher can
//! never turn revocation checking off.

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use notify::{Config as NotifyConfig, Event, EventKind, PollWatcher, RecursiveMode, Watcher};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Meter},
};
use rustls::{
    DistinguishedName, RootCertStore, SignatureScheme,
    client::danger::HandshakeSignatureValid,
    pki_types::{
        CertificateDer, CertificateRevocationListDer, PrivateKeyDer, UnixTime,
        pem::{self, PemObject},
    },
    server::{
        ClientHello, ResolvesServerCert, VerifierBuilderError, WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
    sign::CertifiedKey,
};
use snafu::Snafu;
use tokio::sync::mpsc;

const RELOAD_DEBOUNCE: Duration = Duration::from_millis(250);

const TLS_DOCS_URL: &str = "https://spiceai.org/docs/api/tls";

/// Trust material for verifying client certificates, held in memory: the
/// CA bundle a client certificate must chain to and, optionally, the
/// certificate revocation lists (CRLs) that revoke individual certificates.
#[derive(Clone, Copy)]
pub struct ClientTrustPem<'a> {
    /// PEM-encoded CA certificates.
    pub ca: &'a [u8],
    /// PEM-encoded (`X509 CRL`) CRLs, or one DER-encoded CRL. `None`
    /// disables revocation checking.
    pub crl: Option<&'a [u8]>,
}

impl<'a> ClientTrustPem<'a> {
    /// Trust `ca` without checking revocation.
    #[must_use]
    pub fn ca_only(ca: &'a [u8]) -> Self {
        Self { ca, crl: None }
    }
}

/// On-disk counterpart of [`ClientTrustPem`]. Every path is watched, and a
/// change to any of them rebuilds the client verifiers from all of them.
#[derive(Clone, Debug)]
pub struct ClientTrustPaths {
    /// PEM-encoded CA certificates.
    pub ca: PathBuf,
    /// PEM-encoded CRLs, or one DER-encoded CRL. `None` disables
    /// revocation checking.
    pub crl: Option<PathBuf>,
}

impl ClientTrustPaths {
    /// Trust the CA bundle at `ca` without checking revocation.
    #[must_use]
    pub fn ca_only(ca: PathBuf) -> Self {
        Self { ca, crl: None }
    }
}

/// Operational scope of a reloadable cert/verifier. Used as a metric label so
/// operators can distinguish public-endpoint rotations from cluster rotations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReloadScope {
    /// Public TLS (HTTP / Flight / Metrics).
    Public,
    /// Cluster mTLS (scheduler <-> executor).
    Cluster,
}

impl ReloadScope {
    fn as_str(self) -> &'static str {
        match self {
            ReloadScope::Public => "public",
            ReloadScope::Cluster => "cluster",
        }
    }
}

/// A `ResolvesServerCert` implementation backed by an [`ArcSwap`] so the
/// certificate can be hot-swapped without rebuilding the parent
/// [`rustls::ServerConfig`].
///
/// This type can also optionally own swappable
/// [`WebPkiClientVerifier`]s for client-cert authentication. When the
/// caller configures a client CA path (and optionally a CRL path), the
/// server cert/key, the CA file, and the CRL file are watched together;
/// on rotation, the reload callback rebuilds whichever materials changed
/// and stores them on the respective swap. Cert+key and verifier each
/// have their own swap so rustls can read them independently per
/// handshake without locking; the reload callback is the only writer.
///
/// Two verifiers are built off the same CA bundle and CRLs: a *strict* one
/// (no-cert handshake fails) used by the Flight listener, and a *lax*
/// one (`allow_unauthenticated()`, no-cert handshake succeeds and
/// `peer_certificates()` is empty) used by the HTTP and metrics
/// listeners. The HTTP route layer enforces presence on non-probe
/// routes; `/health` and `/v1/ready` are reachable without a client
/// cert by design so Kubernetes liveness/readiness probes work over
/// TLS without mounting a probe certificate.
#[derive(Debug)]
pub struct ReloadableServerCerts {
    inner: ArcSwap<CertifiedKey>,
    /// Strict verifier — used by the Flight server. `None` when
    /// the caller never configured client-cert authentication.
    verifier_swap: Option<Arc<ArcSwap<Arc<dyn ClientCertVerifier>>>>,
    /// Lax (`allow_unauthenticated`) verifier — used by the HTTP
    /// and metrics servers. `None` mirrors `verifier_swap`.
    verifier_swap_lax: Option<Arc<ArcSwap<Arc<dyn ClientCertVerifier>>>>,
    scope: ReloadScope,
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
    /// Paths to the client CA bundle and CRLs when their hot-reload is
    /// enabled. `None` when the verifier was built from inline bytes (or
    /// when no client auth is configured).
    client_trust_paths: Option<ClientTrustPaths>,
}

impl ReloadableServerCerts {
    /// Build from in-memory PEM. Used for inline / `${secrets:...}`-sourced
    /// material that cannot rotate at runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if `cert_pem`/`key_pem` cannot be parsed into a
    /// valid certificate chain and private key.
    pub fn from_pem(
        cert_pem: &[u8],
        key_pem: &[u8],
        scope: ReloadScope,
    ) -> Result<Arc<Self>, ReloadError> {
        Self::from_pem_with_client_auth(cert_pem, key_pem, None, scope)
    }

    /// Like [`Self::from_pem`] but additionally builds a static
    /// [`WebPkiClientVerifier`] from `client_trust` when supplied.
    /// The verifier is held under the same `ArcSwap` machinery as the
    /// hot-reload path uses, but — since this constructor takes inline
    /// bytes — it never gets rebuilt for the lifetime of the process.
    ///
    /// # Errors
    ///
    /// Returns an error if `cert_pem`/`key_pem` cannot be parsed into a
    /// valid certificate chain and private key, or if `client_trust` is
    /// supplied but its CA bundle or CRLs cannot be parsed into a client
    /// cert verifier.
    pub fn from_pem_with_client_auth(
        cert_pem: &[u8],
        key_pem: &[u8],
        client_trust: Option<ClientTrustPem<'_>>,
        scope: ReloadScope,
    ) -> Result<Arc<Self>, ReloadError> {
        let certified = build_certified_key(cert_pem, key_pem)?;
        let (verifier_swap, verifier_swap_lax) = match client_trust {
            Some(trust) => {
                let verifiers = ClientVerifiers::build(trust.ca, trust.crl)?;
                verifiers.coverage.log(&CrlSource::Inline);
                (
                    Some(Arc::new(ArcSwap::from_pointee(verifiers.strict))),
                    Some(Arc::new(ArcSwap::from_pointee(verifiers.lax))),
                )
            }
            None => (None, None),
        };
        Ok(Arc::new(Self {
            inner: ArcSwap::from_pointee(certified),
            verifier_swap,
            verifier_swap_lax,
            scope,
            cert_path: None,
            key_path: None,
            client_trust_paths: None,
        }))
    }

    /// Build from on-disk PEM and register reload via `watcher`. The watcher
    /// fires when either path changes; both files are then re-read together
    /// since cert + key must swap atomically.
    ///
    /// # Errors
    ///
    /// Returns an error if the cert/key files cannot be read or parsed, or
    /// if registering the watch with `watcher` fails.
    pub fn from_paths(
        cert_path: PathBuf,
        key_path: PathBuf,
        scope: ReloadScope,
        watcher: &CertWatcher,
    ) -> Result<Arc<Self>, ReloadError> {
        Self::from_paths_with_client_auth(cert_path, key_path, None, scope, watcher)
    }

    /// Like [`Self::from_paths`] but additionally registers optional
    /// client trust paths (CA bundle and CRLs) for client-cert
    /// authentication. When `client_trust` is `Some`, the watcher monitors
    /// every file; on any change the reload callback re-reads and
    /// re-validates *all* materials in scope and atomically stores the new
    /// server cert/key and (separately) the new client verifier. Cert+key
    /// and verifier are independent swaps so reads on one cannot block
    /// reads on the other; the dispatcher thread is the only writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the cert/key (or, when supplied, client CA and
    /// CRL) files cannot be read or parsed, or if registering the watch
    /// with `watcher` fails.
    pub fn from_paths_with_client_auth(
        cert_path: PathBuf,
        key_path: PathBuf,
        client_trust: Option<ClientTrustPaths>,
        scope: ReloadScope,
        watcher: &CertWatcher,
    ) -> Result<Arc<Self>, ReloadError> {
        let cert_pem = read_file(&cert_path)?;
        let key_pem = read_file(&key_path)?;
        let certified = build_certified_key(&cert_pem, &key_pem).map_err(|err| match err {
            ReloadError::KeyMismatch => ReloadError::KeyMismatchFiles {
                cert_path: cert_path.clone(),
                key_path: key_path.clone(),
            },
            other => other,
        })?;
        let (verifier_swap, verifier_swap_lax) = if let Some(trust) = &client_trust {
            let ca_pem = read_file(&trust.ca)?;
            let crl = trust.crl.as_deref().map(read_file).transpose()?;
            let verifiers = ClientVerifiers::build(&ca_pem, crl.as_deref())?;
            if let Some(crl_path) = &trust.crl {
                verifiers.coverage.log(&CrlSource::File(crl_path));
            }
            (
                Some(Arc::new(ArcSwap::from_pointee(verifiers.strict))),
                Some(Arc::new(ArcSwap::from_pointee(verifiers.lax))),
            )
        } else {
            (None, None)
        };
        let this = Arc::new(Self {
            inner: ArcSwap::from_pointee(certified),
            verifier_swap,
            verifier_swap_lax,
            scope,
            cert_path: Some(cert_path.clone()),
            key_path: Some(key_path.clone()),
            client_trust_paths: client_trust.clone(),
        });

        let weak = Arc::downgrade(&this);
        // Single registration covering every path that should trigger a
        // reload. The callback re-reads its own paths from `self`, so a
        // change to any one of them produces one consistent re-read of
        // the whole bundle (cert+key and, when applicable, CA and CRLs).
        let mut cb_paths = vec![cert_path, key_path];
        if let Some(trust) = client_trust {
            cb_paths.push(trust.ca);
            cb_paths.extend(trust.crl);
        }
        watcher.register(cb_paths, move |_changed| {
            let Some(this) = weak.upgrade() else {
                return;
            };
            this.reload_now();
        })?;

        Ok(this)
    }

    /// Re-read the configured cert + key files (and client CA and CRLs,
    /// when configured) and swap atomically. Each material is independent;
    /// a parse failure on one keeps the previous version in place
    /// without affecting the others. Errors keep the old material in
    /// place rather than taking the server down on a bad rotation.
    pub fn reload_now(&self) {
        // ---- server cert + key ----
        if let (Some(cert_path), Some(key_path)) = (&self.cert_path, &self.key_path) {
            let cert_pem = match fs::read(cert_path) {
                Ok(b) => b,
                Err(err) => {
                    tracing::warn!(
                        path = %cert_path.display(),
                        "TLS reload: failed to read cert file: {err}"
                    );
                    metrics().reload(self.scope, "io_error");
                    return;
                }
            };
            let key_pem = match fs::read(key_path) {
                Ok(b) => b,
                Err(err) => {
                    tracing::warn!(
                        path = %key_path.display(),
                        "TLS reload: failed to read key file: {err}"
                    );
                    metrics().reload(self.scope, "io_error");
                    return;
                }
            };
            match build_certified_key(&cert_pem, &key_pem) {
                // A rotation touches both the cert and the key file and the
                // watcher fires once per changed file, so the second pass
                // usually re-reads exactly what is already served. Don't
                // swap, log, or count it again.
                Ok(new_ck) if same_material(&new_ck, &self.inner.load()) => {
                    tracing::debug!(
                        cert_path = %cert_path.display(),
                        scope = self.scope.as_str(),
                        "TLS reload: server certificate and key unchanged, not swapping"
                    );
                }
                Ok(new_ck) => {
                    let fp = cert_fingerprint_short(&new_ck);
                    self.inner.store(Arc::new(new_ck));
                    tracing::info!(
                        cert_path = %cert_path.display(),
                        fingerprint = %fp,
                        scope = self.scope.as_str(),
                        "TLS reload: swapped server certificate"
                    );
                    metrics().reload(self.scope, "ok");
                }
                Err(ReloadError::KeyMismatch) => {
                    // Typically a rotation that replaced only one of the two
                    // files. Serving it would fail every handshake, so keep
                    // the old pair; a later write of the matching file
                    // triggers another reload that picks up the new pair.
                    tracing::warn!(
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        scope = self.scope.as_str(),
                        "TLS reload: new certificate {} does not match private key {}, keeping the current certificate",
                        cert_path.display(),
                        key_path.display(),
                    );
                    metrics().reload(self.scope, "key_mismatch");
                }
                Err(err) => {
                    tracing::warn!(
                        cert_path = %cert_path.display(),
                        "TLS reload: rejected new material, keeping old: {err}"
                    );
                    metrics().reload(self.scope, "parse_error");
                }
            }
        }

        // ---- client CA and CRLs (mTLS verifiers) ----
        // Independent from the cert+key swap: a bad CA or CRL rotation
        // must not affect the server cert, and vice versa. The strict and
        // lax verifiers are rebuilt from the same CA and CRL bytes and
        // stored back-to-back; rustls reads each independently per
        // handshake, so a connection only ever sees one consistent
        // verifier. Any failure keeps the previous CA *and* CRLs, so a
        // bad CRL rotation never disables revocation checking.
        if let (Some(trust), Some(strict_swap), Some(lax_swap)) = (
            &self.client_trust_paths,
            &self.verifier_swap,
            &self.verifier_swap_lax,
        ) {
            let ca_pem = match fs::read(&trust.ca) {
                Ok(b) => b,
                Err(err) => {
                    tracing::warn!(
                        path = %trust.ca.display(),
                        "TLS reload: failed to read client CA file: {err}"
                    );
                    metrics().reload(self.scope, "io_error");
                    return;
                }
            };
            let crl = match &trust.crl {
                Some(crl_path) => match fs::read(crl_path) {
                    Ok(b) => Some(b),
                    Err(err) => {
                        tracing::warn!(
                            path = %crl_path.display(),
                            "TLS reload: failed to read client CRL file, keeping the previous CRLs: {err}"
                        );
                        metrics().reload(self.scope, "io_error");
                        return;
                    }
                },
                None => None,
            };
            match (ClientVerifiers::build(&ca_pem, crl.as_deref()), &trust.crl) {
                (Ok(verifiers), None) => {
                    strict_swap.store(Arc::new(verifiers.strict));
                    lax_swap.store(Arc::new(verifiers.lax));
                    tracing::info!(
                        ca_path = %trust.ca.display(),
                        scope = self.scope.as_str(),
                        "TLS reload: swapped client CA bundle"
                    );
                    metrics().reload(self.scope, "ok");
                }
                (Ok(verifiers), Some(crl_path)) => {
                    strict_swap.store(Arc::new(verifiers.strict));
                    lax_swap.store(Arc::new(verifiers.lax));
                    tracing::info!(
                        ca_path = %trust.ca.display(),
                        crl_path = %crl_path.display(),
                        scope = self.scope.as_str(),
                        "TLS reload: swapped client CA bundle and certificate revocation lists"
                    );
                    verifiers.coverage.log(&CrlSource::File(crl_path));
                    metrics().reload(self.scope, "ok");
                }
                (Err(err), None) => {
                    tracing::warn!(
                        ca_path = %trust.ca.display(),
                        "TLS reload: rejected new client CA bundle, keeping old: {err}"
                    );
                    metrics().reload(self.scope, "parse_error");
                }
                (Err(err), Some(crl_path)) => {
                    tracing::warn!(
                        ca_path = %trust.ca.display(),
                        crl_path = %crl_path.display(),
                        "TLS reload: rejected new client CA bundle or certificate revocation lists, keeping the previous ones: {err}"
                    );
                    metrics().reload(self.scope, "parse_error");
                }
            }
        }
    }
}

impl ResolvesServerCert for ReloadableServerCerts {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.inner.load_full())
    }
}

impl ReloadableServerCerts {
    /// Returns the *strict* [`ClientCertVerifier`] suitable for
    /// [`ServerConfig::with_client_cert_verifier`] when this resolver was
    /// built with a client CA configured. "Strict" means the rustls
    /// handshake fails if the client does not present a cert. Used by
    /// the Flight listener.
    ///
    /// The returned verifier is a thin shim that reads `verifier_swap`
    /// on every handshake — so a CA rotation lands on the next
    /// handshake without rebuilding the `ServerConfig`.
    ///
    /// `None` when the resolver was built without client-cert auth
    /// (the caller should wire `with_no_client_auth()` instead).
    #[must_use]
    pub fn client_verifier(&self) -> Option<Arc<dyn ClientCertVerifier>> {
        Some(shim_for(self.verifier_swap.as_ref()?))
    }

    /// Returns the *lax* [`ClientCertVerifier`] for the HTTP and
    /// metrics listeners. "Lax" means a client that presents no cert
    /// completes the handshake (and `peer_certificates()` is empty);
    /// a client that presents an invalid / wrong-CA cert is still
    /// rejected at the handshake. Per-route HTTP middleware enforces
    /// presence for non-probe routes; `/health` and `/v1/ready` are
    /// reachable without a cert by design for Kubernetes probes.
    ///
    /// `None` mirrors [`Self::client_verifier`].
    #[must_use]
    pub fn client_verifier_lax(&self) -> Option<Arc<dyn ClientCertVerifier>> {
        Some(shim_for(self.verifier_swap_lax.as_ref()?))
    }
}

fn shim_for(swap: &Arc<ArcSwap<Arc<dyn ClientCertVerifier>>>) -> Arc<dyn ClientCertVerifier> {
    // Capture `root_hint_subjects` from the live verifier once,
    // here, because `ClientCertVerifier::root_hint_subjects`
    // returns `&[DistinguishedName]` — a borrow that cannot point
    // into an `ArcSwap::load` result. This matches the design
    // choice in `ClusterPkiBundle`: the set
    // of advertised CA subjects is pinned at startup and a
    // rotated CA file does **not** change the
    // `CertificateRequest` hint list. Operators rotating a CA in
    // a way that adds or removes subject DNs need to restart the
    // process to re-advertise; in practice CA rotations swap the
    // signing key under the same Subject, so the hint list stays
    // valid.
    let hint_subjects = swap.load_full().root_hint_subjects().to_vec();
    Arc::new(ReloadableClientVerifier {
        inner: Arc::clone(swap),
        hint_subjects,
    })
}

/// Thin shim that delegates every [`ClientCertVerifier`] call to the
/// live verifier inside an [`ArcSwap`]. Reads happen on the rustls
/// handshake hot path and are wait-free (`ArcSwap::load_full` is a
/// single atomic read + clone of the inner `Arc`). The dispatcher
/// thread is the only writer and only stores fully-validated
/// verifiers, so no handshake can ever observe a torn / partially
/// rebuilt verifier.
///
/// Methods other than `verify_client_cert` (TLS 1.2 / 1.3 signature
/// verification, supported schemes) also delegate to the live verifier
/// so a CA rotation that, for example, narrows the supported
/// signature schemes is reflected immediately. `root_hint_subjects`
/// is the exception — see the doc comment in
/// [`ReloadableServerCerts::client_verifier`] for why it's pinned at
/// startup.
#[derive(Debug)]
struct ReloadableClientVerifier {
    inner: Arc<ArcSwap<Arc<dyn ClientCertVerifier>>>,
    /// Subject DN list captured at construction time and returned
    /// from every `root_hint_subjects` call. See the constructor for
    /// the rotation trade-off.
    hint_subjects: Vec<DistinguishedName>,
}

impl ReloadableClientVerifier {
    fn live(&self) -> Arc<dyn ClientCertVerifier> {
        // `inner` is an `Arc<ArcSwap<Arc<dyn ClientCertVerifier>>>`,
        // so `load_full()` returns `Arc<Arc<dyn ClientCertVerifier>>`
        // (the outer `Arc` is the snapshot guard, the inner is the
        // verifier itself). Deref one layer and clone the inner Arc
        // explicitly so the return type isn't relying on a subtle
        // `Deref` coercion.
        let snapshot = self.inner.load_full();
        Arc::clone(&*snapshot)
    }
}

impl ClientCertVerifier for ReloadableClientVerifier {
    fn offer_client_auth(&self) -> bool {
        self.live().offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        // Forwarded from the live verifier so the
        // strict-vs-`allow_unauthenticated` distinction baked into
        // each underlying `WebPkiClientVerifier` actually reaches
        // rustls. The default trait impl returns `true`, which would
        // override `allow_unauthenticated()` on the lax verifier and
        // make the handshake reject no-cert clients — not what we
        // want for the HTTP listener that serves `/health`.
        self.live().client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.hint_subjects
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.live()
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.live().verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.live().verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.live().supported_verify_schemes()
    }
}

impl ReloadableServerCerts {
    /// Snapshot of the currently-loaded leaf + chain. Used for startup
    /// logging (`TlsConfig::subject_name`).
    #[must_use]
    pub fn current_cert_chain(&self) -> Vec<CertificateDer<'static>> {
        self.inner.load().cert.clone()
    }
}

/// Watches a collection of file paths and dispatches debounced reload
/// callbacks. One watcher serves the whole runtime.
pub struct CertWatcher {
    tx: mpsc::UnboundedSender<WatchOp>,
    _watcher_thread: std::thread::JoinHandle<()>,
}

enum WatchOp {
    Register {
        paths: Vec<PathBuf>,
        callback: Box<dyn Fn(&Path) + Send + Sync + 'static>,
        result_tx: std::sync::mpsc::SyncSender<Result<(), notify::Error>>,
    },
    Event(notify::Result<Event>),
    /// Synchronously fire every registered callback. Used by the SIGHUP
    /// reload path so operators can trigger a manual rotation pickup
    /// without waiting for the poll interval.
    TriggerReloadAll,
}

impl CertWatcher {
    /// Spawn the watcher loop. The loop owns the OS-level watcher and a
    /// debounce table. Returns a handle that can be cloned cheaply via `Arc`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying platform watcher or the
    /// dispatcher thread fails to start.
    pub fn spawn() -> Result<Self, ReloadError> {
        type ReloadCallback = Box<dyn Fn(&Path) + Send + Sync>;
        let (tx, mut rx) = mpsc::unbounded_channel::<WatchOp>();
        let event_tx = tx.clone();
        // Use the polling watcher rather than the platform-native one
        // (FSEvents on macOS, inotify on Linux). Trade-offs:
        //   - Polling is platform-uniform and doesn't suffer from
        //     FSEvents per-process resource limits / event coalescing,
        //     which we observed dropping events in heavy parallel test
        //     runs.
        //   - 2s poll interval is well below the SVID rotation cadence
        //     SPIRE uses (~30 minutes), so cert pickup latency is fine.
        //   - Polling cost: a `stat` per watched path every 2s. With
        //     ~5 cert files in a worst-case spiced deployment, this is
        //     not measurable.
        let poll_config = NotifyConfig::default()
            .with_poll_interval(std::time::Duration::from_secs(2))
            .with_compare_contents(true);
        let mut watcher = PollWatcher::new(
            move |res: notify::Result<Event>| {
                let _ = event_tx.send(WatchOp::Event(res));
            },
            poll_config,
        )
        .map_err(|source| ReloadError::Watcher { source })?;

        // The notify watcher runs its own thread; we drive the dispatch loop
        // on a dedicated std::thread so we do not need to be inside a tokio
        // runtime to use the watcher (this matters for the cluster path,
        // which is constructed before the runtime in some test contexts).
        let dispatcher = std::thread::Builder::new()
            .name("tls-cert-watcher".into())
            .spawn(move || {
                let mut callbacks: Vec<(Vec<PathBuf>, ReloadCallback)> = Vec::new();
                let mut last_fire = std::collections::HashMap::<PathBuf, std::time::Instant>::new();

                while let Some(op) = rx.blocking_recv() {
                    match op {
                        WatchOp::Register {
                            paths,
                            callback,
                            result_tx,
                        } => {
                            let mut first_err: Option<notify::Error> = None;
                            for p in &paths {
                                // Watching the parent dir is more robust to
                                // atomic-rename rotations (notify on the file
                                // itself stops firing after the inode is
                                // replaced).
                                let watch_target = p.parent().unwrap_or(p);
                                if let Err(err) =
                                    watcher.watch(watch_target, RecursiveMode::NonRecursive)
                                {
                                    tracing::warn!(
                                        path = %watch_target.display(),
                                        "TLS reload: failed to watch path: {err}"
                                    );
                                    if first_err.is_none() {
                                        first_err = Some(err);
                                    }
                                }
                            }
                            if let Some(err) = first_err {
                                // Surface the failure to the caller so
                                // misconfiguration (missing dir, no
                                // permissions) is detected at startup
                                // instead of silently disabling reload.
                                let _ = result_tx.send(Err(err));
                            } else {
                                callbacks.push((paths, callback));
                                let _ = result_tx.send(Ok(()));
                            }
                        }
                        WatchOp::Event(Ok(event)) => {
                            tracing::trace!(
                                kind = ?event.kind,
                                paths = ?event.paths,
                                "TLS reload: notify event"
                            );
                            if !is_reload_event(event.kind) {
                                continue;
                            }
                            for changed in &event.paths {
                                let now = std::time::Instant::now();
                                if let Some(prev) = last_fire.get(changed)
                                    && now.duration_since(*prev) < RELOAD_DEBOUNCE
                                {
                                    continue;
                                }
                                last_fire.insert(changed.clone(), now);

                                for (paths, cb) in &callbacks {
                                    if paths.iter().any(|p| paths_match(p, changed)) {
                                        tracing::debug!(
                                            path = %changed.display(),
                                            "TLS reload: dispatching to callback"
                                        );
                                        cb(changed);
                                    }
                                }
                            }
                        }
                        WatchOp::Event(Err(err)) => {
                            tracing::debug!("TLS reload: watcher error: {err}");
                        }
                        WatchOp::TriggerReloadAll => {
                            tracing::info!(
                                callbacks = callbacks.len(),
                                "TLS reload: SIGHUP triggered manual reload of all registered material"
                            );
                            for (paths, cb) in &callbacks {
                                // Fire with the first registered path as a
                                // sentinel; the callback re-reads its own
                                // paths from disk anyway.
                                if let Some(p) = paths.first() {
                                    cb(p);
                                }
                            }
                            // Bypass debounce on the next notify event
                            // for these paths so a SIGHUP followed by a
                            // legitimate file change still fires the
                            // callbacks rather than being eaten by the
                            // 250ms window.
                            last_fire.clear();
                        }
                    }
                }
            })
            .map_err(|source| ReloadError::Spawn { source })?;

        Ok(Self {
            tx,
            _watcher_thread: dispatcher,
        })
    }

    /// Register `callback` to fire whenever any of `paths` changes.
    /// Blocks briefly while the watcher dispatcher arms each parent
    /// directory, so misconfiguration (missing dir, no permissions) is
    /// surfaced as a `ReloadError::Watcher` at the call site rather than
    /// silently disabling rotation.
    ///
    /// # Errors
    ///
    /// Returns an error if any of `paths` cannot be watched (missing
    /// parent directory, insufficient permissions), or if the dispatcher
    /// loop has already exited.
    pub fn register<F>(&self, paths: Vec<PathBuf>, callback: F) -> Result<(), ReloadError>
    where
        F: Fn(&Path) + Send + Sync + 'static,
    {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.tx
            .send(WatchOp::Register {
                paths,
                callback: Box::new(callback),
                result_tx,
            })
            .map_err(|_| ReloadError::WatcherClosed)?;
        match result_rx.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(source)) => Err(ReloadError::Watcher { source }),
            Err(_) => Err(ReloadError::WatcherClosed),
        }
    }

    /// Enqueue a reload of every registered callback, regardless of
    /// whether the underlying file has changed. This is the SIGHUP /
    /// `kill -HUP` path: it lets operators force a TLS material pickup
    /// instead of waiting for the next poll tick.
    ///
    /// **Best-effort, asynchronous.** This call returns as soon as the
    /// op has been queued on the dispatcher channel; the actual
    /// callbacks (parse + validate + `ArcSwap::store`) run on the
    /// dispatcher thread shortly after. The only error returned
    /// (`ReloadError::WatcherClosed`) means the dispatcher has already
    /// exited, so the request was definitively not delivered. There is
    /// no completion acknowledgement; callers that need to know when
    /// the rotation has actually landed should observe the
    /// `tls_reload_total{result="ok"}` counter (or the
    /// `reload_count_for_tests` helper in tests).
    ///
    /// # Errors
    ///
    /// Returns [`ReloadError::WatcherClosed`] if the dispatcher loop has
    /// already exited.
    pub fn trigger_reload_all(&self) -> Result<(), ReloadError> {
        self.tx
            .send(WatchOp::TriggerReloadAll)
            .map_err(|_| ReloadError::WatcherClosed)
    }
}

impl std::fmt::Debug for CertWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertWatcher").finish_non_exhaustive()
    }
}

fn is_reload_event(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    )
}

fn paths_match(registered: &Path, changed: &Path) -> bool {
    if registered == changed {
        return true;
    }
    // Match on canonicalized form when possible — handles `./foo/bar` vs
    // `/abs/foo/bar` style differences.
    match (fs::canonicalize(registered), fs::canonicalize(changed)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

fn build_certified_key(cert_pem: &[u8], key_pem: &[u8]) -> Result<CertifiedKey, ReloadError> {
    let cert_chain = load_certs(cert_pem)?;
    if cert_chain.is_empty() {
        return Err(ReloadError::EmptyCertChain);
    }
    let key = load_key(key_pem)?;
    let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
        .map_err(|source| ReloadError::Sign { source })?;
    let certified = CertifiedKey::new(cert_chain, signing_key);
    // rustls does not check that the leaf certificate belongs to the private
    // key; a mismatched pair loads fine and then fails every handshake.
    // `Unknown` (the key can't report its public key) is accepted, matching
    // rustls' own `CertifiedKey::from_der`.
    match certified.keys_match() {
        Ok(()) | Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::Unknown)) => {
            Ok(certified)
        }
        Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::KeyMismatch)) => {
            Err(ReloadError::KeyMismatch)
        }
        Err(source) => Err(ReloadError::Rustls { source }),
    }
}

/// Whether `new` serves exactly what `current` already serves: the same
/// chain and the same private key (compared by its public half).
fn same_material(new: &CertifiedKey, current: &CertifiedKey) -> bool {
    new.cert == current.cert
        && new.key.public_key().is_some()
        && new.key.public_key() == current.key.public_key()
}

/// The strict (rejects no-cert at handshake) and lax
/// (`allow_unauthenticated`) [`WebPkiClientVerifier`]s built from one CA
/// bundle and its optional CRLs. Both share the same trust roots and
/// revocation checks; only the no-cert behavior differs. Built together so
/// no listener ever pairs a new CA or CRL with another listener's old one.
struct ClientVerifiers {
    strict: Arc<dyn ClientCertVerifier>,
    lax: Arc<dyn ClientCertVerifier>,
    coverage: CrlCoverage,
}

impl ClientVerifiers {
    /// Build both verifiers from PEM-encoded CA certificates and, when
    /// `crl` is supplied, the CRLs that revoke client certificates. Used
    /// both at startup and on every CA- or CRL-file rotation.
    ///
    /// Treats an empty CA bundle as an error — a verifier with no roots
    /// would silently reject every client cert and operators would have a
    /// hard time figuring out why mTLS "stopped working". Likewise an empty
    /// or unparseable CRL file is an error rather than "no CRLs", which
    /// would silently stop checking revocation.
    ///
    /// With CRLs, every certificate in the client's chain is checked
    /// (except the trust anchor), and a certificate whose issuer published
    /// no CRL in the bundle is rejected: its revocation status is unknown.
    /// The revocations an expired CRL lists are still enforced (rustls'
    /// default expiration policy); [`CrlCoverage`] reports the expiry.
    fn build(ca_pem: &[u8], crl: Option<&[u8]>) -> Result<Self, ReloadError> {
        let ca_certs = load_certs(ca_pem)?;
        if ca_certs.is_empty() {
            return Err(ReloadError::EmptyClientCa);
        }
        let crls = crl.map(load_crls).transpose()?.unwrap_or_default();
        let coverage = if crls.is_empty() {
            CrlCoverage::default()
        } else {
            CrlCoverage::of(&ca_certs, &crls, x509_parser::time::ASN1Time::now())
        };
        let mut roots = RootCertStore::empty();
        for ca in ca_certs {
            roots
                .add(ca)
                .map_err(|source| ReloadError::Rustls { source })?;
        }
        let roots = Arc::new(roots);
        let builder = || WebPkiClientVerifier::builder(Arc::clone(&roots)).with_crls(crls.clone());
        Ok(Self {
            strict: builder().build().map_err(verifier_build_error)?,
            lax: builder()
                .allow_unauthenticated()
                .build()
                .map_err(verifier_build_error)?,
            coverage,
        })
    }
}

fn verifier_build_error(err: VerifierBuilderError) -> ReloadError {
    match err {
        VerifierBuilderError::InvalidCrl(reason) => ReloadError::InvalidCrl { reason },
        err => ReloadError::ClientVerifierBuild {
            message: err.to_string(),
        },
    }
}

/// Parse a CRL bundle: any number of PEM `X509 CRL` blocks, or a single
/// DER-encoded CRL (the form CRL distribution points publish). The CRLs
/// themselves are validated when the verifier is built.
fn load_crls(bytes: &[u8]) -> Result<Vec<CertificateRevocationListDer<'static>>, ReloadError> {
    // A DER CRL is an ASN.1 SEQUENCE, so its first byte is 0x30; a PEM file
    // is text and starts with a `-----BEGIN` line or an explanatory header.
    if bytes.first() == Some(&0x30) {
        return Ok(vec![CertificateRevocationListDer::from(bytes.to_vec())]);
    }
    match CertificateRevocationListDer::pem_slice_iter(bytes).collect::<Result<Vec<_>, _>>() {
        Ok(crls) if !crls.is_empty() => Ok(crls),
        Ok(_) | Err(pem::Error::NoItemsFound) => Err(ReloadError::EmptyClientCrl),
        Err(err) => Err(ReloadError::ParseCrl {
            source: io::Error::new(io::ErrorKind::InvalidData, err),
        }),
    }
}

/// Where the CRLs being reported on came from, for the operator-facing
/// diagnostics.
enum CrlSource<'a> {
    File(&'a Path),
    Inline,
}

impl std::fmt::Display for CrlSource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CrlSource::File(path) => write!(f, "the CRL file '{}'", path.display()),
            CrlSource::Inline => f.write_str("the inline CRLs"),
        }
    }
}

/// What a client CA bundle's CRLs leave unchecked, reported each time the
/// verifiers are built so an operator learns of it before a client is
/// rejected (or a revoked one admitted).
#[derive(Debug, Default, PartialEq, Eq)]
struct CrlCoverage {
    /// Subjects of trusted CAs with no CRL in the bundle. Every client
    /// certificate such a CA issued directly is rejected, because its
    /// revocation status is unknown.
    cas_without_crl: Vec<String>,
    /// `(issuer, nextUpdate)` of each CRL whose `nextUpdate` has passed.
    expired: Vec<(String, String)>,
}

impl CrlCoverage {
    /// Compare the trusted CAs against the CRLs' issuers and `nextUpdate`.
    ///
    /// A CRL covers a CA when its issuer name is byte-for-byte the CA's
    /// subject name — the same comparison webpki makes to select a CRL.
    /// Certificates and CRLs rustls accepts but x509-parser cannot read are
    /// skipped, and if any CRL is unreadable no CA is reported as uncovered,
    /// since that CRL may be the one covering it.
    fn of(
        ca_certs: &[CertificateDer<'_>],
        crls: &[CertificateRevocationListDer<'_>],
        now: x509_parser::time::ASN1Time,
    ) -> Self {
        let mut coverage = Self::default();
        let mut crl_issuers = Vec::with_capacity(crls.len());
        let mut every_crl_read = true;
        for crl in crls {
            let Ok((_, parsed)) = x509_parser::parse_x509_crl(crl.as_ref()) else {
                every_crl_read = false;
                continue;
            };
            crl_issuers.push(parsed.issuer().as_raw().to_vec());
            if let Some(next_update) = parsed.next_update()
                && next_update <= now
            {
                coverage
                    .expired
                    .push((parsed.issuer().to_string(), format_asn1_time(next_update)));
            }
        }
        if every_crl_read {
            for ca in ca_certs {
                let Ok((_, parsed)) = x509_parser::parse_x509_certificate(ca.as_ref()) else {
                    continue;
                };
                let subject = parsed.subject();
                if !crl_issuers.iter().any(|issuer| issuer == subject.as_raw()) {
                    coverage.cas_without_crl.push(subject.to_string());
                }
            }
        }
        coverage
    }

    fn log(&self, source: &CrlSource<'_>) {
        for ca in &self.cas_without_crl {
            tracing::warn!("{}", ca_without_crl_warning(ca, source));
        }
        for (issuer, next_update) in &self.expired {
            tracing::warn!("{}", expired_crl_warning(issuer, next_update, source));
        }
    }
}

fn ca_without_crl_warning(ca_subject: &str, source: &CrlSource<'_>) -> String {
    format!(
        "Client certificate authority '{ca_subject}' has no certificate revocation list in {source}, so every client certificate it issued directly is rejected. Add the CRL that '{ca_subject}' publishes to {source}. See {TLS_DOCS_URL}"
    )
}

fn expired_crl_warning(issuer: &str, next_update: &str, source: &CrlSource<'_>) -> String {
    format!(
        "The certificate revocation list from '{issuer}' in {source} expired at {next_update}, so client certificates '{issuer}' revoked since it was issued are still accepted. Replace it with the CRL '{issuer}' currently publishes. See {TLS_DOCS_URL}"
    )
}

fn format_asn1_time(instant: x509_parser::time::ASN1Time) -> String {
    instant
        .to_datetime()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| instant.to_string())
}

fn read_file(path: &Path) -> Result<Vec<u8>, ReloadError> {
    fs::read(path).map_err(|source| ReloadError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn load_certs(pem_bytes: &[u8]) -> Result<Vec<CertificateDer<'static>>, ReloadError> {
    // `pem_slice_iter` yields no items for empty/non-PEM input (rather than
    // always erroring). Map `NoItemsFound` to an empty vec so callers can
    // distinguish empty bundles (`EmptyCertChain` / `EmptyClientCa`) from
    // real parse failures.
    match CertificateDer::pem_slice_iter(pem_bytes).collect::<Result<Vec<_>, _>>() {
        Ok(certs) => Ok(certs),
        Err(pem::Error::NoItemsFound) => Ok(Vec::new()),
        Err(err) => Err(ReloadError::ParseCert {
            source: io::Error::new(io::ErrorKind::InvalidData, err),
        }),
    }
}

fn load_key(pem_bytes: &[u8]) -> Result<PrivateKeyDer<'static>, ReloadError> {
    match PrivateKeyDer::from_pem_slice(pem_bytes) {
        Ok(key) => Ok(key),
        Err(pem::Error::NoItemsFound) => Err(ReloadError::MissingKey),
        Err(err) => Err(ReloadError::ParseKey {
            source: io::Error::new(io::ErrorKind::InvalidData, err),
        }),
    }
}

fn cert_fingerprint_short(ck: &CertifiedKey) -> String {
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};
    let leaf = ck
        .cert
        .first()
        .map(|c| Sha256::digest(c.as_ref()).to_vec())
        .unwrap_or_default();
    let mut s = String::with_capacity(16);
    for b in leaf.iter().take(8) {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[derive(Debug, Snafu)]
pub enum ReloadError {
    #[snafu(display("failed to read TLS file at {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("failed to parse TLS certificate: {source}"))]
    ParseCert { source: std::io::Error },
    #[snafu(display("failed to parse TLS private key: {source}"))]
    ParseKey { source: std::io::Error },
    #[snafu(display("TLS PEM contained no private key"))]
    MissingKey,
    #[snafu(display("TLS PEM contained no certificates"))]
    EmptyCertChain,
    #[snafu(display("TLS certificate does not match the private key"))]
    KeyMismatch,
    #[snafu(display(
        "TLS certificate {} does not match private key {}",
        cert_path.display(),
        key_path.display()
    ))]
    KeyMismatchFiles {
        cert_path: PathBuf,
        key_path: PathBuf,
    },
    #[snafu(display("client CA bundle contained no certificates"))]
    EmptyClientCa,
    #[snafu(display(
        "client certificate revocation list file contained no CRLs: expected PEM `-----BEGIN X509 CRL-----` blocks or one DER-encoded CRL"
    ))]
    EmptyClientCrl,
    #[snafu(display("failed to parse client certificate revocation list: {source}"))]
    ParseCrl { source: std::io::Error },
    #[snafu(display("invalid client certificate revocation list: {reason:?}"))]
    InvalidCrl {
        reason: rustls::CertRevocationListError,
    },
    #[snafu(display("failed to build rustls client cert verifier: {message}"))]
    ClientVerifierBuild { message: String },
    #[snafu(display("rustls signing key error: {source}"))]
    Sign { source: rustls::Error },
    #[snafu(display("rustls error: {source}"))]
    Rustls { source: rustls::Error },
    #[snafu(display("filesystem watcher error: {source}"))]
    Watcher { source: notify::Error },
    #[snafu(display("failed to spawn watcher thread: {source}"))]
    Spawn { source: std::io::Error },
    #[snafu(display("watcher dispatch loop closed"))]
    WatcherClosed,
}

// ---------- metrics ----------

struct ReloadMetrics {
    counter: Counter<u64>,
    /// Monotonic counter exposed for tests via `reload_total_for_tests()`.
    test_total: AtomicU64,
    /// Per-(scope, result) breakdown for tests so they can wait on a
    /// specific outcome (e.g. `parse_error` for the public scope) without
    /// being confused by unrelated reload activity in another scope.
    test_buckets: std::sync::Mutex<std::collections::HashMap<(ReloadScope, &'static str), u64>>,
}

static METRICS: OnceLock<ReloadMetrics> = OnceLock::new();

fn metrics() -> &'static ReloadMetrics {
    METRICS.get_or_init(|| {
        let meter: Meter = opentelemetry::global::meter("spiced_runtime");
        ReloadMetrics {
            counter: meter
                .u64_counter("runtime_tls_reload_total")
                .with_description(
                    "Total TLS material reload attempts, labeled by scope and result.",
                )
                .build(),
            test_total: AtomicU64::new(0),
            test_buckets: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    })
}

impl ReloadMetrics {
    fn reload(&self, scope: ReloadScope, result: &'static str) {
        self.counter.add(
            1,
            &[
                KeyValue::new("scope", scope.as_str()),
                KeyValue::new("result", result),
            ],
        );
        self.test_total.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut buckets) = self.test_buckets.lock() {
            *buckets.entry((scope, result)).or_insert(0) += 1;
        }
    }
}

/// Total reload attempts since process start (success + failure). Intended
/// for tests; production should rely on the `OTel` counter.
#[must_use]
pub fn reload_total_for_tests() -> u64 {
    metrics().test_total.load(Ordering::Relaxed)
}

/// Reload attempts for a specific (scope, result) bucket since process
/// start. Lets tests wait for and assert a specific outcome without
/// racing with unrelated reload activity in other scopes.
#[must_use]
pub fn reload_count_for_tests(scope: ReloadScope, result: &'static str) -> u64 {
    metrics()
        .test_buckets
        .lock()
        .ok()
        .and_then(|m| m.get(&(scope, result)).copied())
        .unwrap_or(0)
}

/// Increment the reload metric for the given (scope, result). Used by
/// out-of-module reload paths (e.g. cluster outbound `ClientTlsConfig`
/// rebuild) so all reload activity is observed under one metric.
pub fn record_reload_metric(scope: ReloadScope, result: &'static str) {
    metrics().reload(scope, result);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install_crypto_provider() {
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
    }

    #[test]
    fn build_from_pem_round_trips() {
        use std::fmt::Write as _;

        use sha2::{Digest, Sha256};

        install_crypto_provider();
        let cert = include_bytes!("../../../test/tls/spiced_cert.pem");
        let key = include_bytes!("../../../test/tls/spiced_key.pem");
        let r = ReloadableServerCerts::from_pem(cert, key, ReloadScope::Public)
            .expect("build from pem");

        // The served chain is the fixture's chain, certificate for certificate.
        let expected = CertificateDer::pem_slice_iter(cert)
            .collect::<Result<Vec<_>, _>>()
            .expect("the fixture is a PEM certificate chain");
        let served = r.current_cert_chain();
        assert_eq!(served, expected);

        // Pinned without any PEM parser: the fixture holds one certificate,
        // and these are the length and SHA-256 of its base64-decoded DER.
        let [leaf] = served.as_slice() else {
            panic!(
                "expected a single-certificate chain, got {} certificates",
                served.len()
            );
        };
        assert_eq!(leaf.as_ref().len(), 1275);
        let digest = Sha256::digest(leaf.as_ref())
            .iter()
            .fold(String::new(), |mut hex, byte| {
                let _ = write!(hex, "{byte:02x}");
                hex
            });
        assert_eq!(
            digest,
            "a4563153133eb4b263d1e801ab4599e20cc28f1637819365810b4f952e8cde8f"
        );
    }

    #[test]
    fn build_rejects_malformed_pem() {
        install_crypto_provider();
        let err = ReloadableServerCerts::from_pem(
            b"not a pem",
            include_bytes!("../../../test/tls/spiced_key.pem"),
            ReloadScope::Public,
        )
        .expect_err("must reject malformed cert");
        assert!(matches!(err, ReloadError::EmptyCertChain), "got: {err:?}");
    }

    const CERT: &[u8] = include_bytes!("../../../test/tls/spiced_cert.pem");
    const KEY: &[u8] = include_bytes!("../../../test/tls/spiced_key.pem");
    /// A valid certificate whose public key is *not* the one in `KEY`.
    const OTHER_CERT: &[u8] = include_bytes!("../../../test/tls/spiced_root_cert.pem");

    fn pem_chain(pem: &[u8]) -> Vec<CertificateDer<'static>> {
        CertificateDer::pem_slice_iter(pem)
            .collect::<Result<Vec<_>, _>>()
            .expect("fixture is a PEM certificate chain")
    }

    /// Cert and key on disk under a tempdir, served by a resolver wired to a
    /// live watcher (the watcher must outlive the resolver's registration).
    fn on_disk(
        scope: ReloadScope,
    ) -> (
        tempfile::TempDir,
        PathBuf,
        CertWatcher,
        Arc<ReloadableServerCerts>,
    ) {
        install_crypto_provider();
        let dir = tempfile::TempDir::new().expect("tempdir");
        let cert_path = dir.path().join("server.pem");
        let key_path = dir.path().join("server.key");
        fs::write(&cert_path, CERT).expect("write cert");
        fs::write(&key_path, KEY).expect("write key");
        let watcher = CertWatcher::spawn().expect("spawn watcher");
        let certs = ReloadableServerCerts::from_paths(cert_path.clone(), key_path, scope, &watcher)
            .expect("build from paths");
        (dir, cert_path, watcher, certs)
    }

    #[test]
    fn build_rejects_key_that_does_not_match_certificate() {
        install_crypto_provider();
        let err = ReloadableServerCerts::from_pem(OTHER_CERT, KEY, ReloadScope::Public)
            .expect_err("a certificate whose public key isn't the private key's must be rejected");
        assert!(
            matches!(err, ReloadError::KeyMismatch),
            "expected KeyMismatch, got: {err}"
        );
    }

    #[test]
    fn startup_from_mismatched_files_names_both_files() {
        install_crypto_provider();
        let dir = tempfile::TempDir::new().expect("tempdir");
        let cert_path = dir.path().join("server.pem");
        let key_path = dir.path().join("server.key");
        fs::write(&cert_path, OTHER_CERT).expect("write cert");
        fs::write(&key_path, KEY).expect("write key");
        let watcher = CertWatcher::spawn().expect("spawn watcher");
        let err = ReloadableServerCerts::from_paths(
            cert_path.clone(),
            key_path.clone(),
            ReloadScope::Public,
            &watcher,
        )
        .expect_err("a mismatched pair must not start serving");
        let msg = err.to_string();
        assert!(
            msg.contains(&cert_path.display().to_string())
                && msg.contains(&key_path.display().to_string()),
            "startup error should name both files, got: {msg}"
        );
    }

    /// A rotation that replaces the cert but not the key (or vice versa)
    /// must not be served: every handshake would fail. The old pair stays
    /// and the failure is counted.
    #[test]
    fn reload_keeps_old_certificate_when_new_one_does_not_match_key() {
        let (_dir, cert_path, _watcher, certs) = on_disk(ReloadScope::Public);
        let mismatches_before = reload_count_for_tests(ReloadScope::Public, "key_mismatch");

        fs::write(&cert_path, OTHER_CERT).expect("replace cert, keep key");
        certs.reload_now();

        assert!(
            certs.current_cert_chain() == pem_chain(CERT),
            "the mismatched certificate was swapped in"
        );
        assert_eq!(
            reload_count_for_tests(ReloadScope::Public, "key_mismatch"),
            mismatches_before + 1,
            "the rejected reload was not counted"
        );
    }

    /// One rotation touches both the cert and the key file, so the watcher
    /// fires the reload once per file. The second pass re-reads material
    /// that is already being served and must not swap (or log) again.
    #[test]
    fn reload_of_unchanged_material_does_not_swap_again() {
        // Cluster scope: no other test in this crate reloads it, so the
        // `ok` bucket isn't moved by unrelated tests running in parallel.
        let (_dir, _cert_path, _watcher, certs) = on_disk(ReloadScope::Cluster);
        let served = certs.inner.load_full();
        let ok_before = reload_count_for_tests(ReloadScope::Cluster, "ok");

        certs.reload_now();

        assert!(
            Arc::ptr_eq(&certs.inner.load_full(), &served),
            "unchanged cert and key were swapped in again"
        );
        assert_eq!(
            reload_count_for_tests(ReloadScope::Cluster, "ok"),
            ok_before,
            "an unchanged reload was counted as a swap"
        );
    }

    #[test]
    fn trigger_reload_all_fires_registered_callbacks() {
        // Verifies the SIGHUP path: trigger_reload_all() must fire every
        // registered callback synchronously, regardless of whether the
        // underlying file changed. Uses a temp file we never modify.
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{Duration, Instant};

        let dir = tempfile::TempDir::new().expect("tempdir");
        let p1 = dir.path().join("a.pem");
        let p2 = dir.path().join("b.pem");
        std::fs::write(&p1, b"placeholder a").expect("write a");
        std::fs::write(&p2, b"placeholder b").expect("write b");

        let watcher = CertWatcher::spawn().expect("spawn watcher");
        let hits_a = Arc::new(AtomicUsize::new(0));
        let hits_b = Arc::new(AtomicUsize::new(0));
        let cb_a = Arc::clone(&hits_a);
        let cb_b = Arc::clone(&hits_b);
        watcher
            .register(vec![p1], move |_| {
                cb_a.fetch_add(1, Ordering::SeqCst);
            })
            .expect("register a");
        watcher
            .register(vec![p2], move |_| {
                cb_b.fetch_add(1, Ordering::SeqCst);
            })
            .expect("register b");

        watcher.trigger_reload_all().expect("trigger");

        // Dispatcher runs on a dedicated thread; give it a brief moment
        // to drain the op queue and fire the callbacks.
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline
            && (hits_a.load(Ordering::SeqCst) == 0 || hits_b.load(Ordering::SeqCst) == 0)
        {
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(hits_a.load(Ordering::SeqCst), 1, "callback a not fired");
        assert_eq!(hits_b.load(Ordering::SeqCst), 1, "callback b not fired");
    }

    fn test_ca(
        name: &str,
    ) -> (
        CertificateDer<'static>,
        rcgen::Issuer<'static, rcgen::KeyPair>,
    ) {
        let mut params = rcgen::CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let key = rcgen::KeyPair::generate().expect("generate CA key");
        let cert = params.self_signed(&key).expect("self-sign CA");
        (cert.der().clone(), rcgen::Issuer::new(params, key))
    }

    fn test_crl(
        issuer: &rcgen::Issuer<'static, rcgen::KeyPair>,
        next_update: time::OffsetDateTime,
    ) -> CertificateRevocationListDer<'static> {
        rcgen::CertificateRevocationListParams {
            this_update: time::macros::datetime!(2020-01-01 00:00 UTC),
            next_update,
            crl_number: rcgen::SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs: Vec::new(),
            key_identifier_method: rcgen::KeyIdMethod::Sha256,
        }
        .signed_by(issuer)
        .expect("sign CRL")
        .into()
    }

    #[test]
    fn crl_coverage_reports_cas_without_a_crl_and_expired_crls() {
        let (current_ca, current_issuer) = test_ca("current");
        let (uncovered_ca, _) = test_ca("uncovered");
        let (expired_ca, expired_issuer) = test_ca("expired");
        let crls = [
            test_crl(
                &current_issuer,
                time::macros::datetime!(2999-01-01 00:00 UTC),
            ),
            test_crl(
                &expired_issuer,
                time::macros::datetime!(2026-01-02 03:04:05 UTC),
            ),
        ];
        let now = x509_parser::time::ASN1Time::from_timestamp(
            time::macros::datetime!(2026-10-22 00:00 UTC).unix_timestamp(),
        )
        .expect("valid timestamp");

        let coverage = CrlCoverage::of(&[current_ca, uncovered_ca, expired_ca], &crls, now);

        assert_eq!(
            coverage,
            CrlCoverage {
                cas_without_crl: vec!["CN=uncovered".to_string()],
                expired: vec![("CN=expired".to_string(), "2026-01-02T03:04:05Z".to_string())],
            }
        );
    }

    #[test]
    fn crl_coverage_is_empty_when_every_ca_has_a_current_crl() {
        let (ca, issuer) = test_ca("current");
        let crls = [test_crl(
            &issuer,
            time::macros::datetime!(2999-01-01 00:00 UTC),
        )];
        assert_eq!(
            CrlCoverage::of(&[ca], &crls, x509_parser::time::ASN1Time::now()),
            CrlCoverage::default()
        );
    }

    #[test]
    fn crl_warnings_name_the_ca_the_source_the_consequence_and_the_fix() {
        let file = CrlSource::File(Path::new("/etc/spice/tls/client.crl"));
        assert_eq!(
            ca_without_crl_warning("CN=Partner CA", &file),
            "Client certificate authority 'CN=Partner CA' has no certificate revocation list in the CRL file '/etc/spice/tls/client.crl', so every client certificate it issued directly is rejected. Add the CRL that 'CN=Partner CA' publishes to the CRL file '/etc/spice/tls/client.crl'. See https://spiceai.org/docs/api/tls"
        );
        assert_eq!(
            expired_crl_warning("CN=Partner CA", "2026-01-02T03:04:05Z", &CrlSource::Inline),
            "The certificate revocation list from 'CN=Partner CA' in the inline CRLs expired at 2026-01-02T03:04:05Z, so client certificates 'CN=Partner CA' revoked since it was issued are still accepted. Replace it with the CRL 'CN=Partner CA' currently publishes. See https://spiceai.org/docs/api/tls"
        );
    }
}
