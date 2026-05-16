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
//! roots used for mTLS) at runtime without a restart.
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
//! - Cluster mTLS uses [`crate::cluster::pki::ClusterPkiBundle`], which
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
//! `spiced` never goes down because a rotation produced a bad file.

use std::{
    fs,
    io::{self, Cursor},
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
    pki_types::{CertificateDer, PrivateKeyDer, UnixTime},
    server::{
        ClientHello, ResolvesServerCert, WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
    sign::CertifiedKey,
};
use rustls_pemfile::{certs, private_key};
use snafu::Snafu;
use tokio::sync::mpsc;

const RELOAD_DEBOUNCE: Duration = Duration::from_millis(250);

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
/// caller configures a client CA path, both the server cert/key and
/// the CA file are watched together; on rotation, the reload callback
/// rebuilds whichever materials changed and stores them on the
/// respective swap. Cert+key and verifier each have their own swap so
/// rustls can read them independently per handshake without locking;
/// the reload callback is the only writer.
///
/// Two verifiers are built off the same CA bundle: a *strict* one
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
    /// Path to the client CA bundle when CA hot-reload is enabled.
    /// `None` when the verifier was built from inline bytes (or when
    /// no client auth is configured).
    client_ca_path: Option<PathBuf>,
}

impl ReloadableServerCerts {
    /// Build from in-memory PEM. Used for inline / `${secrets:...}`-sourced
    /// material that cannot rotate at runtime.
    pub fn from_pem(
        cert_pem: &[u8],
        key_pem: &[u8],
        scope: ReloadScope,
    ) -> Result<Arc<Self>, ReloadError> {
        Self::from_pem_with_client_auth(cert_pem, key_pem, None, scope)
    }

    /// Like [`Self::from_pem`] but additionally builds a static
    /// [`WebPkiClientVerifier`] from `client_ca_pem` when supplied.
    /// The verifier is held under the same `ArcSwap` machinery as the
    /// hot-reload path uses, but — since this constructor takes inline
    /// bytes — it never gets rebuilt for the lifetime of the process.
    pub fn from_pem_with_client_auth(
        cert_pem: &[u8],
        key_pem: &[u8],
        client_ca_pem: Option<&[u8]>,
        scope: ReloadScope,
    ) -> Result<Arc<Self>, ReloadError> {
        let certified = build_certified_key(cert_pem, key_pem)?;
        let (verifier_swap, verifier_swap_lax) = match client_ca_pem {
            Some(ca) => {
                let strict = build_client_verifier(ca, ClientVerifierKind::Strict)?;
                let lax = build_client_verifier(ca, ClientVerifierKind::Lax)?;
                (
                    Some(Arc::new(ArcSwap::from_pointee(strict))),
                    Some(Arc::new(ArcSwap::from_pointee(lax))),
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
            client_ca_path: None,
        }))
    }

    /// Build from on-disk PEM and register reload via `watcher`. The watcher
    /// fires when either path changes; both files are then re-read together
    /// since cert + key must swap atomically.
    pub fn from_paths(
        cert_path: PathBuf,
        key_path: PathBuf,
        scope: ReloadScope,
        watcher: &CertWatcher,
    ) -> Result<Arc<Self>, ReloadError> {
        Self::from_paths_with_client_auth(cert_path, key_path, None, scope, watcher)
    }

    /// Like [`Self::from_paths`] but additionally registers an optional
    /// client CA path for client-cert authentication. When `client_ca_path`
    /// is `Some`, the watcher monitors all three files; on any change the
    /// reload callback re-reads and re-validates *all* materials in
    /// scope and atomically stores the new server cert/key and
    /// (separately) the new client verifier. Cert+key and verifier are
    /// independent swaps so reads on one cannot block reads on the
    /// other; the dispatcher thread is the only writer.
    pub fn from_paths_with_client_auth(
        cert_path: PathBuf,
        key_path: PathBuf,
        client_ca_path: Option<PathBuf>,
        scope: ReloadScope,
        watcher: &CertWatcher,
    ) -> Result<Arc<Self>, ReloadError> {
        let cert_pem = fs::read(&cert_path).map_err(|source| ReloadError::Io {
            path: cert_path.clone(),
            source,
        })?;
        let key_pem = fs::read(&key_path).map_err(|source| ReloadError::Io {
            path: key_path.clone(),
            source,
        })?;
        let certified = build_certified_key(&cert_pem, &key_pem)?;
        let (verifier_swap, verifier_swap_lax) = if let Some(ca_path) = &client_ca_path {
            let ca_pem = fs::read(ca_path).map_err(|source| ReloadError::Io {
                path: ca_path.clone(),
                source,
            })?;
            let strict = build_client_verifier(&ca_pem, ClientVerifierKind::Strict)?;
            let lax = build_client_verifier(&ca_pem, ClientVerifierKind::Lax)?;
            (
                Some(Arc::new(ArcSwap::from_pointee(strict))),
                Some(Arc::new(ArcSwap::from_pointee(lax))),
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
            client_ca_path: client_ca_path.clone(),
        });

        let weak = Arc::downgrade(&this);
        // Single registration covering every path that should trigger a
        // reload. The callback re-reads its own paths from `self`, so a
        // change to any one of them produces one consistent re-read of
        // the whole bundle (cert+key and, when applicable, CA).
        let mut cb_paths = vec![cert_path, key_path];
        if let Some(ca_path) = client_ca_path {
            cb_paths.push(ca_path);
        }
        watcher.register(cb_paths, move |_changed| {
            let Some(this) = weak.upgrade() else {
                return;
            };
            this.reload_now();
        })?;

        Ok(this)
    }

    /// Re-read the configured cert + key files (and client CA, when
    /// configured) and swap atomically. Each material is independent;
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
                Err(err) => {
                    tracing::warn!(
                        cert_path = %cert_path.display(),
                        "TLS reload: rejected new material, keeping old: {err}"
                    );
                    metrics().reload(self.scope, "parse_error");
                }
            }
        }

        // ---- client CA (mTLS verifiers) ----
        // Independent from the cert+key swap: a bad CA rotation must
        // not affect the server cert, and vice versa. The strict and
        // lax verifiers are rebuilt from the same CA bytes and stored
        // back-to-back; rustls reads each independently per handshake,
        // so a connection only ever sees one consistent verifier.
        if let (Some(ca_path), Some(strict_swap), Some(lax_swap)) = (
            &self.client_ca_path,
            &self.verifier_swap,
            &self.verifier_swap_lax,
        ) {
            let ca_pem = match fs::read(ca_path) {
                Ok(b) => b,
                Err(err) => {
                    tracing::warn!(
                        path = %ca_path.display(),
                        "TLS reload: failed to read client CA file: {err}"
                    );
                    metrics().reload(self.scope, "io_error");
                    return;
                }
            };
            let strict = build_client_verifier(&ca_pem, ClientVerifierKind::Strict);
            let lax = build_client_verifier(&ca_pem, ClientVerifierKind::Lax);
            match (strict, lax) {
                (Ok(new_strict), Ok(new_lax)) => {
                    strict_swap.store(Arc::new(new_strict));
                    lax_swap.store(Arc::new(new_lax));
                    tracing::info!(
                        ca_path = %ca_path.display(),
                        scope = self.scope.as_str(),
                        "TLS reload: swapped client CA bundle"
                    );
                    metrics().reload(self.scope, "ok");
                }
                (Err(err), _) | (_, Err(err)) => {
                    tracing::warn!(
                        ca_path = %ca_path.display(),
                        "TLS reload: rejected new client CA bundle, keeping old: {err}"
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
    // choice in `crate::cluster::pki::ClusterPkiBundle`: the set
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
    Ok(CertifiedKey::new(cert_chain, signing_key))
}

/// Whether to build the strict (rejects no-cert at handshake) or lax
/// (`allow_unauthenticated`) variant of the `WebPkiClientVerifier`.
/// Both variants share the same trust roots; only the no-cert
/// behavior differs.
#[derive(Debug, Clone, Copy)]
enum ClientVerifierKind {
    Strict,
    Lax,
}

/// Build a [`WebPkiClientVerifier`] from one or more PEM-encoded CA
/// certificates. Used both at startup and on every CA-file rotation.
///
/// Treats an empty CA bundle as an error — a verifier with no roots
/// would silently reject every client cert and operators would have a
/// hard time figuring out why mTLS "stopped working".
fn build_client_verifier(
    ca_pem: &[u8],
    kind: ClientVerifierKind,
) -> Result<Arc<dyn ClientCertVerifier>, ReloadError> {
    let mut roots = RootCertStore::empty();
    let ca_certs = certs(&mut Cursor::new(ca_pem))
        .collect::<io::Result<Vec<_>>>()
        .map_err(|source| ReloadError::ParseCert { source })?;
    if ca_certs.is_empty() {
        return Err(ReloadError::EmptyClientCa);
    }
    for ca in ca_certs {
        roots
            .add(ca)
            .map_err(|source| ReloadError::Rustls { source })?;
    }
    let builder = WebPkiClientVerifier::builder(Arc::new(roots));
    let builder = match kind {
        ClientVerifierKind::Strict => builder,
        ClientVerifierKind::Lax => builder.allow_unauthenticated(),
    };
    builder
        .build()
        .map_err(|err| ReloadError::ClientVerifierBuild {
            message: err.to_string(),
        })
}

fn load_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, ReloadError> {
    let mut cursor = Cursor::new(pem);
    certs(&mut cursor)
        .collect::<io::Result<Vec<_>>>()
        .map_err(|source| ReloadError::ParseCert { source })
}

fn load_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, ReloadError> {
    let mut cursor = Cursor::new(pem);
    private_key(&mut cursor)
        .map_err(|source| ReloadError::ParseKey { source })?
        .ok_or(ReloadError::MissingKey)
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
    #[snafu(display("client CA bundle contained no certificates"))]
    EmptyClientCa,
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
        install_crypto_provider();
        let cert = include_bytes!("../../../../test/tls/spiced_cert.pem");
        let key = include_bytes!("../../../../test/tls/spiced_key.pem");
        let r = ReloadableServerCerts::from_pem(cert, key, ReloadScope::Public)
            .expect("build from pem");
        let resolved = r
            .inner
            .load()
            .cert
            .first()
            .map(|c| c.as_ref().to_vec())
            .unwrap_or_default();
        assert!(!resolved.is_empty(), "cert chain should be non-empty");
    }

    #[test]
    fn build_rejects_malformed_pem() {
        install_crypto_provider();
        let err = ReloadableServerCerts::from_pem(
            b"not a pem",
            include_bytes!("../../../../test/tls/spiced_key.pem"),
            ReloadScope::Public,
        )
        .expect_err("must reject malformed cert");
        assert!(matches!(err, ReloadError::EmptyCertChain), "got: {err:?}");
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
}
