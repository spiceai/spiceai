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
};

use arc_swap::ArcSwap;
use rustls::{
    DigitallySignedStruct, DistinguishedName, SignatureScheme,
    client::danger::HandshakeSignatureValid,
    pki_types::{CertificateDer, UnixTime},
    server::{
        ClientHello, ParsedCertificate, ResolvesServerCert, WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
    sign::CertifiedKey,
};
use sha2::{Digest, Sha256};
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use x509_certificate::CapturedX509Certificate;

use crate::tls::{CertWatcher, ReloadScope, reload::record_reload_metric};

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
    let ca_certs = rustls_pemfile::certs(&mut ca_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
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
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|err| {
            io::Error::other(format!(
                "rustls failed to build cluster client verifier: {err}"
            ))
        })?;

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
        fingerprint,
    })
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
    let cert_chain = rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cert parse error: {e}"))?;
    if cert_chain.is_empty() {
        return Err("empty certificate chain".into());
    }
    // Sanity-check the leaf parses as X.509.
    let _ = ParsedCertificate::try_from(&cert_chain[0])
        .map_err(|e| format!("leaf cert parse error: {e}"))?;

    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(key_pem))
        .map_err(|e| format!("key parse error: {e}"))?
        .ok_or("no private key found in PEM")?;
    let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
        .map_err(|e| format!("private key not usable: {e}"))?;
    Ok(CertifiedKey::new(cert_chain, signing_key))
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
