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
//! This module is the foundation for [milestone 1 of the mTLS plan]
//! (`plans/mtls-public-endpoints.md`). It adds the ability to swap
//! TLS certificates and (in a follow-up milestone) client-cert trust roots
//! without restarting `spiced`.
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
    pki_types::{CertificateDer, PrivateKeyDer},
    server::{ClientHello, ResolvesServerCert},
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
#[derive(Debug)]
pub struct ReloadableServerCerts {
    inner: ArcSwap<CertifiedKey>,
    scope: ReloadScope,
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
}

impl ReloadableServerCerts {
    /// Build from in-memory PEM. Used for inline / `${secrets:...}`-sourced
    /// material that cannot rotate at runtime.
    pub fn from_pem(
        cert_pem: &[u8],
        key_pem: &[u8],
        scope: ReloadScope,
    ) -> Result<Arc<Self>, ReloadError> {
        let certified = build_certified_key(cert_pem, key_pem)?;
        Ok(Arc::new(Self {
            inner: ArcSwap::from_pointee(certified),
            scope,
            cert_path: None,
            key_path: None,
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
        let cert_pem = fs::read(&cert_path).map_err(|source| ReloadError::Io {
            path: cert_path.clone(),
            source,
        })?;
        let key_pem = fs::read(&key_path).map_err(|source| ReloadError::Io {
            path: key_path.clone(),
            source,
        })?;
        let certified = build_certified_key(&cert_pem, &key_pem)?;
        let this = Arc::new(Self {
            inner: ArcSwap::from_pointee(certified),
            scope,
            cert_path: Some(cert_path.clone()),
            key_path: Some(key_path.clone()),
        });

        let weak = Arc::downgrade(&this);
        let cb_paths = vec![cert_path, key_path];
        watcher.register(cb_paths, move |_changed| {
            let Some(this) = weak.upgrade() else {
                return;
            };
            this.reload_now();
        })?;

        Ok(this)
    }

    /// Re-read the configured cert + key files and swap atomically. Errors
    /// keep the old material in place.
    pub fn reload_now(&self) {
        let (Some(cert_path), Some(key_path)) = (&self.cert_path, &self.key_path) else {
            return;
        };
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
}

impl ResolvesServerCert for ReloadableServerCerts {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.inner.load_full())
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
