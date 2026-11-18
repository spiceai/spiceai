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

//! Atomic cluster mTLS PKI bundle.
//!
//! `ClusterPkiBundle` is the single rotation point for the three pieces
//! of cluster mTLS material that move together:
//!
//! 1. The server cert + key (consumed by inbound rustls handshakes via
//!    the [`rustls::server::ResolvesServerCert`] impl).
//! 2. The CA bundle for verifying inbound client certs (consumed by the
//!    [`rustls::server::danger::ClientCertVerifier`] impl).
//! 3. The outbound `tonic::transport::ClientTlsConfig` used when this
//!    node dials peers (consumed via [`Self::client_tls_config`]).
//!
//! On a file-change event the bundle re-reads **all three** files,
//! validates the chain (server cert issued by CA, signature verifies),
//! and only then publishes a new snapshot via a single
//! [`arc_swap::ArcSwap`] store. If any step fails the previous snapshot
//! stays in place \u2014 the runtime is never left with a half-rotated PKI
//! (e.g. new server cert paired with the old CA verifier).

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use arc_swap::{ArcSwap, ArcSwapOption};
use rustls::{
    DigitallySignedStruct, DistinguishedName, SignatureScheme,
    client::{WebPkiServerVerifier, danger::HandshakeSignatureValid},
    pki_types::{
        CertificateDer, PrivateKeyDer, ServerName, UnixTime,
        pem::{self, PemObject},
    },
    server::{
        ClientHello, ParsedCertificate, ResolvesServerCert, WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
    sign::CertifiedKey,
};
use sha2::{Digest, Sha256};
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use x509_certificate::CapturedX509Certificate;

use runtime_tls::{CertWatcher, ReloadScope, reload::record_reload_metric};
use tokio::io::AsyncWriteExt;

use crate::peer::PeerIdentity;

/// Set of paths the bundle re-reads on every reload.
#[derive(Clone, Debug)]
pub struct ClusterPkiPaths {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// One coherent snapshot of the cluster PKI. Constructed atomically;
/// every reload either commits a fully-validated new snapshot or keeps
/// the previous one.
struct ClusterPkiSnapshot {
    /// Server-side keypair for inbound TLS handshakes.
    cert: Arc<CertifiedKey>,
    /// CA-bundle-backed verifier for inbound client certs.
    verifier: Arc<dyn ClientCertVerifier>,
    /// Pre-built `ClientTlsConfig` snapshot for outbound dials. Tonic
    /// builds its own internal rustls config from this when each channel
    /// is constructed; storing the immutable input is sufficient.
    client_tls: Arc<ClientTlsConfig>,
    /// The same outbound trust and client identity as `client_tls`, as a
    /// rustls config [`ClusterPkiBundle::observe_peer_identity`] connects with.
    observe_tls: Arc<rustls::ClientConfig>,
    /// SHA-256 over (CA || cert || key) for change detection. The
    /// watcher already debounces, but on slow filesystems `notify` can
    /// surface modify events for files whose contents are byte-identical
    /// to the last load (e.g. `touch`); skip those.
    fingerprint: [u8; 32],
}

/// Atomic, hot-reloadable bundle of cluster mTLS material.
///
/// Implements both [`ResolvesServerCert`] and [`ClientCertVerifier`] so a
/// single `Arc<ClusterPkiBundle>` slots into a `rustls::ServerConfig`
/// builder twice without any wrapping types. Outbound callers grab a
/// snapshot via [`Self::client_tls_config`].
#[derive(Debug)]
pub struct ClusterPkiBundle {
    inner: ArcSwap<ClusterPkiSnapshot>,
    paths: ClusterPkiPaths,
    /// Subject DNs of the trusted CAs, captured **once** at
    /// construction. Returned from `root_hint_subjects()` so the TLS
    /// `CertificateRequest` we send to peers carries a non-empty
    /// acceptable-CA list — multi-identity clients (e.g. browsers,
    /// SPIRE workloads with several SVIDs) need this to pick the right
    /// client cert.
    ///
    /// We don't refresh this on rotation: the rustls trait demands a
    /// `&[DistinguishedName]` whose lifetime is tied to `&self`, and an
    /// [`ArcSwap`] guard cannot satisfy that. Cert/key rotations don't
    /// touch the CA's DN, so the hint stays correct. CA *replacement*
    /// (rare) requires a restart for the hint to refresh; verification
    /// itself uses the current snapshot and remains correct.
    hint_subjects: Vec<DistinguishedName>,
    /// The node's kind (`executor`/`scheduler`), instance identity, and the host
    /// it advertises to peers, set once cluster configuration resolves them (see
    /// [`Self::set_advertise_identity`]). When set, a reloaded certificate that
    /// does not name the host is rejected, because peers reach this node there
    /// and verify its certificate against the host.
    advertise: ArcSwapOption<AdvertiseIdentity>,
}

/// How this node is reached by its peers, for the certificate reload check.
#[derive(Debug)]
struct AdvertiseIdentity {
    node_kind: String,
    node_id: String,
    host: String,
}

impl std::fmt::Debug for ClusterPkiSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterPkiSnapshot")
            .field("fingerprint_prefix", &hex8(&self.fingerprint))
            .finish_non_exhaustive()
    }
}

fn hex8(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(16);
    for b in bytes.iter().take(8) {
        use std::fmt::Write;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

impl ClusterPkiBundle {
    /// Read + validate the bundle from disk and register a reload
    /// callback on `watcher`. On any file change the three files are
    /// re-read together; the bundle only swaps if the parse + chain
    /// validation succeeds for all three.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if any file cannot be read, any PEM is
    /// malformed, the chain does not validate, or the watcher refuses
    /// to register the reload callback.
    pub fn try_new(paths: &ClusterPkiPaths, watcher: &CertWatcher) -> io::Result<Arc<Self>> {
        let snapshot = parse_and_validate(paths)?;
        // Snapshot the hint subjects from the initial verifier into an
        // owned `Vec` we can hand out as `&[DistinguishedName]` from
        // `root_hint_subjects()`. See `hint_subjects` field docs.
        let hint_subjects = snapshot.verifier.root_hint_subjects().to_vec();
        let this = Arc::new(Self {
            inner: ArcSwap::from_pointee(snapshot),
            paths: paths.clone(),
            hint_subjects,
            advertise: ArcSwapOption::empty(),
        });

        let weak = Arc::downgrade(&this);
        let watch_paths = vec![paths.ca.clone(), paths.cert.clone(), paths.key.clone()];
        watcher
            .register(watch_paths, move |_changed| {
                let Some(this) = weak.upgrade() else {
                    return;
                };
                this.reload_now();
            })
            .map_err(|err| {
                io::Error::other(format!(
                    "Cluster mTLS: failed to register reload watcher: {err}"
                ))
            })?;

        Ok(this)
    }

    /// Outbound `ClientTlsConfig` snapshot. Cheap: clones the inner
    /// `Arc<ClientTlsConfig>` view. Callers that need the value (tonic's
    /// channel builder takes by value) can `(*snap).clone()`.
    #[must_use]
    pub fn client_tls_config(&self) -> Arc<ClientTlsConfig> {
        Arc::clone(&self.inner.load().client_tls)
    }

    /// Connects to the cluster node at `host:port` the way this node connects
    /// to its peers — verifying the node's certificate against the cluster CA
    /// and `host`, and presenting this node's certificate — and returns the
    /// identity the node's certificate gives it. The connection is closed once
    /// the TLS handshake completes; no request is sent.
    ///
    /// # Errors
    ///
    /// Returns an error if the node cannot be reached within `timeout`, `host`
    /// is not a DNS name or IP address, or the handshake fails, for example
    /// because the node's certificate is not valid for `host`.
    pub async fn observe_peer_identity(
        &self,
        host: &str,
        port: u16,
        timeout: Duration,
    ) -> io::Result<PeerIdentity> {
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        let server_name = ServerName::try_from(host.to_string()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("'{host}' is not a DNS name or IP address: {err}"),
            )
        })?;
        let connector =
            tokio_rustls::TlsConnector::from(Arc::clone(&self.inner.load().observe_tls));
        let observe = async {
            let tcp = tokio::net::TcpStream::connect((host, port)).await?;
            let mut tls = connector.connect(server_name, tcp).await?;
            let identity = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(<[CertificateDer<'static>]>::first)
                .and_then(PeerIdentity::from_certificate);
            // Best effort: the handshake is all this connection is for.
            let _ = tls.shutdown().await;
            identity.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("the node at {host}:{port} presented no readable certificate"),
                )
            })
        };
        tokio::time::timeout(timeout, observe).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no TLS handshake with {host}:{port} within {timeout:?}"),
            )
        })?
    }

    /// Records how this node is reached by its peers — its kind, instance
    /// identity, and advertise host — so a later certificate reload that drops
    /// the host name is rejected (see [`Self::reload_now`]). Each scheduler and
    /// executor sets it once its cluster configuration resolves
    /// (spiceai/spiceai#16481).
    pub fn set_advertise_identity(
        &self,
        node_kind: impl Into<String>,
        node_id: impl Into<String>,
        host: impl Into<String>,
    ) {
        self.advertise.store(Some(Arc::new(AdvertiseIdentity {
            node_kind: node_kind.into(),
            node_id: node_id.into(),
            host: host.into(),
        })));
    }

    /// This node's own identity, from the leaf of the certificate it currently
    /// serves. `None` only if that leaf does not parse, which construction
    /// already rejects.
    #[must_use]
    pub fn self_identity(&self) -> Option<PeerIdentity> {
        let snapshot = self.inner.load();
        let leaf = snapshot.cert.end_entity_cert().ok()?;
        PeerIdentity::from_certificate(leaf)
    }

    /// The subject of this node's own certificate, for messages (for example
    /// `CN=executor-1`); empty when the subject is empty.
    #[must_use]
    pub fn self_subject(&self) -> String {
        self.self_identity()
            .map(|identity| identity.subject().to_string())
            .unwrap_or_default()
    }

    /// Whether this node's own certificate is valid for `host` — the check a TLS
    /// client applies when it dials this node there. A node advertises only an
    /// address its certificate names.
    #[must_use]
    pub fn self_certificate_names_host(&self, host: &str) -> bool {
        let snapshot = self.inner.load();
        let Ok(leaf) = snapshot.cert.end_entity_cert() else {
            return false;
        };
        crate::peer::certificate_names_host(leaf, host)
    }

    /// Fingerprint of the currently-loaded bundle, for tests.
    #[doc(hidden)]
    #[must_use]
    pub fn fingerprint(&self) -> [u8; 32] {
        self.inner.load().fingerprint
    }

    /// For tests: force a synchronous reload from the configured paths.
    #[doc(hidden)]
    pub fn force_reload_for_tests(&self) {
        self.reload_now();
    }

    fn reload_now(&self) {
        match parse_and_validate(&self.paths) {
            Ok(new_snap) => {
                if new_snap.fingerprint == self.inner.load().fingerprint {
                    tracing::debug!("Cluster mTLS reload: file changed but content identical");
                    return;
                }
                // A rotated certificate that no longer names the address this
                // node advertises is not loaded: peers reach this node there and
                // verify its certificate against the host, so loading it would
                // make the node unreachable.
                if let Some(advertise) = self.advertise.load_full() {
                    let names = new_snap.cert.end_entity_cert().is_ok_and(|leaf| {
                        crate::peer::certificate_names_host(leaf, &advertise.host)
                    });
                    if !names {
                        let subject = new_snap
                            .cert
                            .end_entity_cert()
                            .ok()
                            .and_then(PeerIdentity::from_certificate)
                            .map(|identity| identity.subject().to_string())
                            .unwrap_or_default();
                        tracing::error!(
                            "{}",
                            crate::peer::advertise_address_mismatch_message(
                                crate::peer::MismatchSite::Reload,
                                &advertise.node_kind,
                                &advertise.node_id,
                                &subject,
                                &advertise.host,
                            )
                        );
                        // Counted with the other rotations whose material fails
                        // validation and is not loaded; the log line names the
                        // cause.
                        record_reload_metric(ReloadScope::Cluster, "parse_error");
                        return;
                    }
                }
                self.inner.store(Arc::new(new_snap));
                tracing::info!(
                    ca = %self.paths.ca.display(),
                    cert = %self.paths.cert.display(),
                    "Cluster mTLS: rotated PKI bundle (server cert + verifier + outbound) atomically"
                );
                record_reload_metric(ReloadScope::Cluster, "ok");
            }
            Err(err)
                if err.kind() == io::ErrorKind::NotFound
                    || err.kind() == io::ErrorKind::PermissionDenied
                    || err.raw_os_error().is_some() =>
            {
                tracing::warn!(
                    "Cluster mTLS reload: I/O error reading rotated material, keeping previous: {err}"
                );
                record_reload_metric(ReloadScope::Cluster, "io_error");
            }
            Err(err) => {
                tracing::warn!(
                    "Cluster mTLS reload: rotated material failed validation, keeping previous: {err}"
                );
                record_reload_metric(ReloadScope::Cluster, "parse_error");
            }
        }
    }
}

impl ResolvesServerCert for ClusterPkiBundle {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.inner.load().cert))
    }
}

impl ClientCertVerifier for ClusterPkiBundle {
    fn offer_client_auth(&self) -> bool {
        self.inner.load().verifier.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.load().verifier.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // Stable owned snapshot captured at construction time. See
        // `hint_subjects` field docs for the lifetime / rotation
        // trade-off.
        &self.hint_subjects
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.inner
            .load()
            .verifier
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner
            .load()
            .verifier
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner
            .load()
            .verifier
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.load().verifier.supported_verify_schemes()
    }
}

/// Reads the three files, validates them as a coherent chain, and
/// produces a new snapshot. All-or-nothing: if anything fails we return
/// an error and the caller keeps the previous snapshot.
fn parse_and_validate(paths: &ClusterPkiPaths) -> io::Result<ClusterPkiSnapshot> {
    let ca_pem = std::fs::read(&paths.ca)?;
    let cert_pem = std::fs::read(&paths.cert)?;
    let key_pem = std::fs::read(&paths.key)?;

    validate_chain(&paths.ca, &paths.cert, &ca_pem, &cert_pem)?;

    // Server-side keypair.
    let cert = build_certified_key(&cert_pem, &key_pem)
        .map_err(|err| io::Error::other(format!("server cert/key parse failed: {err}")))?;

    // CA-backed client verifier.
    let mut roots = rustls::RootCertStore::empty();
    let ca_certs = load_certs(&ca_pem).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "failed to parse CA certificates from {}: {err}",
                paths.ca.display()
            ),
        )
    })?;
    if ca_certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no CA certificates parsed from {}", paths.ca.display()),
        ));
    }
    for ca in ca_certs {
        roots.add(ca).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("rustls rejected CA certificate: {err}"),
            )
        })?;
    }
    let roots = Arc::new(roots);
    let verifier = WebPkiClientVerifier::builder(Arc::clone(&roots))
        .build()
        .map_err(|err| {
            io::Error::other(format!(
                "rustls failed to build cluster client verifier: {err}"
            ))
        })?;

    let observe_tls = build_observe_config(roots, &cert_pem, &key_pem)?;

    // Outbound ClientTlsConfig.
    let client_tls = Arc::new(
        ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&ca_pem))
            .identity(Identity::from_pem(&cert_pem, &key_pem)),
    );

    let fingerprint = bundle_fingerprint(&ca_pem, &cert_pem, &key_pem);

    Ok(ClusterPkiSnapshot {
        cert: Arc::new(cert),
        verifier,
        client_tls,
        observe_tls,
        fingerprint,
    })
}

/// The rustls client config [`ClusterPkiBundle::observe_peer_identity`] uses:
/// the cluster CA as the only trust anchor, this node's certificate as its
/// client identity, and h2, the protocol every cluster listener serves.
fn build_observe_config(
    roots: Arc<rustls::RootCertStore>,
    cert_pem: &[u8],
    key_pem: &[u8],
) -> io::Result<Arc<rustls::ClientConfig>> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let server_verifier = WebPkiServerVerifier::builder_with_provider(roots, Arc::clone(&provider))
        .build()
        .map_err(|err| {
            io::Error::other(format!(
                "rustls failed to build cluster server verifier: {err}"
            ))
        })?;
    let chain = load_certs(cert_pem)
        .map_err(|err| io::Error::other(format!("node cert parse failed: {err}")))?;
    let key = load_key(key_pem).map_err(io::Error::other)?;
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| io::Error::other(format!("rustls protocol versions: {err}")))?
        .with_webpki_verifier(server_verifier)
        .with_client_auth_cert(chain, key)
        .map_err(|err| io::Error::other(format!("node cert/key not usable: {err}")))?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

fn validate_chain(
    ca_path: &Path,
    cert_path: &Path,
    ca_pem: &[u8],
    cert_pem: &[u8],
) -> io::Result<()> {
    let ca_x509 = CapturedX509Certificate::from_pem(ca_pem).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Failed to parse cluster CA certificate at {}: {err}",
                ca_path.display()
            ),
        )
    })?;
    let node_x509 = CapturedX509Certificate::from_pem(cert_pem).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Failed to parse cluster node certificate at {}: {err}",
                cert_path.display()
            ),
        )
    })?;

    let ca_name = ca_x509.subject_name().user_friendly_str().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Failed to read subject name from cluster CA certificate at {}: {err}",
                ca_path.display()
            ),
        )
    })?;
    let node_issuer = node_x509.issuer_name().user_friendly_str().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Failed to read issuer name from cluster node certificate at {}: {err}",
                cert_path.display()
            ),
        )
    })?;

    if node_issuer != ca_name {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "The node certificate was not issued by the provided CA, expected {ca_name} but found issuer {node_issuer}"
            ),
        ));
    }

    if let Err(err) = node_x509.verify_signed_by_certificate(&ca_x509) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "The node certificate was not issued by the provided CA, signature verification failed for issuer {node_issuer}: {err}"
            ),
        ));
    }

    let node_cn = node_x509
        .subject_common_name()
        .unwrap_or_else(|| "unknown".to_string());
    tracing::info!("Cluster mTLS configured with CA {ca_name} and node certificate CN {node_cn}");
    Ok(())
}

fn build_certified_key(cert_pem: &[u8], key_pem: &[u8]) -> Result<CertifiedKey, String> {
    let cert_chain = load_certs(cert_pem).map_err(|e| format!("cert parse error: {e}"))?;
    if cert_chain.is_empty() {
        return Err("empty certificate chain".into());
    }
    // Sanity-check the leaf parses as X.509.
    let _ = ParsedCertificate::try_from(&cert_chain[0])
        .map_err(|e| format!("leaf cert parse error: {e}"))?;

    let key = load_key(key_pem)?;
    let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
        .map_err(|e| format!("private key not usable: {e}"))?;
    Ok(CertifiedKey::new(cert_chain, signing_key))
}

fn load_certs(pem_bytes: &[u8]) -> Result<Vec<CertificateDer<'static>>, pem::Error> {
    // Empty / non-PEM input should surface as an empty vec so callers can emit
    // the stable "no CA certificates" / "empty certificate chain" errors,
    // rather than a generic parse failure.
    match CertificateDer::pem_slice_iter(pem_bytes).collect::<Result<Vec<_>, _>>() {
        Ok(certs) => Ok(certs),
        Err(pem::Error::NoItemsFound) => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

fn load_key(pem_bytes: &[u8]) -> Result<PrivateKeyDer<'static>, String> {
    match PrivateKeyDer::from_pem_slice(pem_bytes) {
        Ok(key) => Ok(key),
        Err(pem::Error::NoItemsFound) => Err("no private key found in PEM".into()),
        Err(err) => Err(format!("key parse error: {err}")),
    }
}

fn bundle_fingerprint(ca: &[u8], cert: &[u8], key: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"ca:");
    h.update((ca.len() as u64).to_le_bytes());
    h.update(ca);
    h.update(b"cert:");
    h.update((cert.len() as u64).to_le_bytes());
    h.update(cert);
    h.update(b"key:");
    h.update((key.len() as u64).to_le_bytes());
    h.update(key);
    h.finalize().into()
}
