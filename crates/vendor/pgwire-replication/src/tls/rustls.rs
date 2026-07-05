//! TLS support using rustls.
//!
//! This module provides TLS/SSL connection upgrade for PostgreSQL connections
//! using the rustls library. It supports:
//!
//! - All PostgreSQL SSL modes (disable, prefer, require, verify-ca, verify-full)
//! - Custom CA certificates
//! - Client certificate authentication (mTLS)
//! - SNI hostname override
//!
//! # SSL Modes
//!
//! | Mode | Chain Verified | Hostname Verified | Falls back to plain |
//! |------|----------------|-------------------|---------------------|
//! | `Disable` | - | - | N/A (never uses TLS) |
//! | `Prefer` | No | No | Yes |
//! | `Require` | No | No | No |
//! | `VerifyCa` | Yes | No | No |
//! | `VerifyFull` | Yes | Yes | No |
//!
//! # Security Considerations
//!
//! - `Prefer` and `Require` modes are vulnerable to MITM attacks
//! - `VerifyCa` protects against MITM but allows any hostname
//! - `VerifyFull` provides full protection (recommended for production)
//!
//! # Example
//!
//! ```no_run
//! use pgwire_replication::config::TlsConfig;
//! use pgwire_replication::tls::rustls::{maybe_upgrade_to_tls, MaybeTlsStream};
//! use tokio::net::TcpStream;
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let tls_config = TlsConfig::verify_full(Some(PathBuf::new()))
//!         .with_sni_hostname("db.example.com");
//!
//!     let tcp_stream = TcpStream::connect(("db.example.com", 5432)).await?;
//!
//!     let stream = maybe_upgrade_to_tls(tcp_stream, &tls_config, "db.example.com").await?;
//!     match stream {
//!         MaybeTlsStream::Plain(_) => {}
//!         MaybeTlsStream::Tls(_) => {}
//!     }
//!
//!     Ok(())
//! }
//! ```

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};

use crate::config::{SslMode, TlsConfig};
use crate::error::{PgWireError, Result};
use crate::protocol::framing::write_ssl_request;

/// A stream that may or may not be TLS-encrypted.
///
/// This enum allows code to work with both plain TCP and TLS connections
/// through a unified interface via `AsyncRead` and `AsyncWrite` implementations.
#[derive(Debug)]
pub enum MaybeTlsStream {
    /// Unencrypted TCP connection
    Plain(TcpStream),
    /// TLS-encrypted connection (boxed to reduce enum size)
    Tls(Box<TlsStream<TcpStream>>),
}

impl MaybeTlsStream {
    /// Returns `true` if this is a TLS-encrypted stream.
    #[inline]
    pub fn is_tls(&self) -> bool {
        matches!(self, MaybeTlsStream::Tls(_))
    }

    /// Returns `true` if this is a plain (unencrypted) stream.
    #[inline]
    pub fn is_plain(&self) -> bool {
        matches!(self, MaybeTlsStream::Plain(_))
    }

    /// Returns a reference to the underlying `TcpStream`.
    ///
    /// For TLS streams, this returns the inner TCP stream.
    pub fn get_ref(&self) -> &TcpStream {
        match self {
            MaybeTlsStream::Plain(s) => s,
            MaybeTlsStream::Tls(s) => s.get_ref().0,
        }
    }
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Attempt to upgrade a TCP connection to TLS based on configuration.
///
/// This function implements PostgreSQL's SSL negotiation protocol:
/// 1. Send SSLRequest message
/// 2. Read single-byte response ('S' = proceed, 'N' = rejected)
/// 3. If proceeding, perform TLS handshake
///
/// # Arguments
/// * `tcp` - The TCP connection to potentially upgrade
/// * `tls` - TLS configuration specifying mode and certificates
/// * `host` - Target hostname (used for SNI and verification)
///
/// # Returns
/// A `MaybeTlsStream` that is either the original TCP stream or a TLS-wrapped stream.
///
/// # Errors
/// - Server rejects TLS when mode requires it
/// - TLS handshake failure
/// - Certificate verification failure (for VerifyCa/VerifyFull)
/// - Invalid certificate/key files
pub async fn maybe_upgrade_to_tls(
    mut tcp: TcpStream,
    tls: &TlsConfig,
    host: &str,
) -> Result<MaybeTlsStream> {
    match tls.mode {
        SslMode::Disable => return Ok(MaybeTlsStream::Plain(tcp)),
        SslMode::Prefer | SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {}
    }

    // Install crypto provider (required for rustls 0.23+)
    // This is idempotent - safe to call multiple times
    let _ = rustls::crypto::ring::default_provider().install_default();

    // PostgreSQL TLS negotiation: send SSLRequest, expect 'S' or 'N'
    write_ssl_request(&mut tcp).await?;

    let mut resp = [0u8; 1];
    use tokio::io::AsyncReadExt;
    tcp.read_exact(&mut resp).await?;

    if resp[0] != b'S' {
        // Server refused TLS
        return match tls.mode {
            SslMode::Prefer => Ok(MaybeTlsStream::Plain(tcp)),
            _ => Err(PgWireError::Tls(
                "server does not support TLS (SSLRequest rejected)".into(),
            )),
        };
    }

    // Determine verification requirements based on SSL mode
    let verify_chain = matches!(tls.mode, SslMode::VerifyCa | SslMode::VerifyFull);
    let verify_hostname = matches!(tls.mode, SslMode::VerifyFull);

    let cfg = build_rustls_config(tls, verify_chain, verify_hostname, host)?;
    let connector = TlsConnector::from(Arc::new(cfg));

    // SNI hostname: use override if provided, otherwise use connection host
    let sni = tls.sni_hostname.as_deref().unwrap_or(host);
    let server_name = rustls::pki_types::ServerName::try_from(sni.to_string())
        .map_err(|_| PgWireError::Tls(format!("invalid SNI hostname '{sni}'")))?;

    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| PgWireError::Tls(format!("TLS handshake failed: {e}")))?;

    Ok(MaybeTlsStream::Tls(Box::new(tls_stream)))
}

/// Build rustls ClientConfig based on TLS settings.
fn build_rustls_config(
    tls: &TlsConfig,
    verify_chain: bool,
    verify_hostname: bool,
    host: &str,
) -> Result<ClientConfig> {
    // ---- mTLS config validation ----
    let has_cert = tls.client_cert_pem_path.is_some();
    let has_key = tls.client_key_pem_path.is_some();
    if has_cert ^ has_key {
        return Err(PgWireError::Tls(format!(
            "TLS config error: mTLS requires both client_cert_pem_path and client_key_pem_path \
             (got cert={has_cert} key={has_key})"
        )));
    }

    // Operator hint: VerifyFull + IP literal is a common misconfiguration
    if verify_hostname && host.parse::<std::net::IpAddr>().is_ok() && tls.sni_hostname.is_none() {
        return Err(PgWireError::Tls(format!(
            "TLS config error: VerifyFull enabled but host '{host}' is an IP address. \
             Hint: use a DNS name matching the certificate, or set tls.sni_hostname, \
             or use VerifyCa mode."
        )));
    }

    // ---- Root certificate store ----
    let roots = build_root_store(tls)?;
    let roots_arc = Arc::new(roots.clone());

    // ---- Base config builder ----
    let builder = ClientConfig::builder().with_root_certificates(roots);

    // ---- Client authentication (mTLS) ----
    let mut cfg: ClientConfig = if has_cert {
        let cert_path = tls.client_cert_pem_path.as_ref().unwrap();
        let key_path = tls.client_key_pem_path.as_ref().unwrap();

        let cert_chain = load_cert_chain(cert_path)?;
        let key = load_private_key(key_path)?;

        builder
            .with_client_auth_cert(cert_chain, key)
            .map_err(|e| {
                PgWireError::Tls(format!("TLS config error: invalid client cert/key: {e}"))
            })?
    } else {
        builder.with_no_client_auth()
    };

    // ---- Custom verification policy ----
    if !verify_chain {
        // Prefer/Require: skip all verification (dangerous but matches PostgreSQL behavior)
        cfg.dangerous()
            .set_certificate_verifier(Arc::new(NoVerifier));
        return Ok(cfg);
    }

    if verify_chain && !verify_hostname {
        // VerifyCa: verify certificate chain but ignore hostname mismatch
        let inner = rustls::client::WebPkiServerVerifier::builder(roots_arc)
            .build()
            .map_err(|e| PgWireError::Tls(format!("TLS config error: build verifier: {e}")))?;

        cfg.dangerous()
            .set_certificate_verifier(Arc::new(VerifyChainOnly { inner }));
    }

    // VerifyFull: rustls default behavior already verifies chain + hostname
    Ok(cfg)
}

/// Build root certificate store from config or system defaults.
fn build_root_store(tls: &TlsConfig) -> Result<RootCertStore> {
    use rustls::pki_types::CertificateDer;

    let mut roots = RootCertStore::empty();

    if let Some(path) = &tls.ca_pem_path {
        // Load custom CA certificates
        use rustls::pki_types::pem::PemObject;

        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
            .map_err(|e| {
                PgWireError::Tls(format!(
                    "TLS config error: failed to open CA PEM '{}': {e}",
                    path.display()
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                PgWireError::Tls(format!(
                    "TLS config error: failed to parse CA PEM '{}': {e}",
                    path.display()
                ))
            })?;

        let (added, _ignored) = roots.add_parsable_certificates(certs);
        if added == 0 {
            return Err(PgWireError::Tls(format!(
                "TLS config error: no valid CA certificates found in '{}'",
                path.display()
            )));
        }
    } else {
        // Use Mozilla's root certificates (webpki-roots)
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    Ok(roots)
}

/// Load certificate chain from PEM file.
fn load_cert_chain(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::CertificateDer;

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(|e| {
            PgWireError::Tls(format!(
                "TLS config error: failed to open client certificate '{}': {e}",
                path.display()
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            PgWireError::Tls(format!(
                "TLS config error: failed to parse client certificate '{}': {e}",
                path.display()
            ))
        })?;

    if certs.is_empty() {
        return Err(PgWireError::Tls(format!(
            "TLS config error: no certificates found in '{}'",
            path.display()
        )));
    }

    Ok(certs)
}

/// Load private key from PEM file.
///
/// Supports PKCS#8, PKCS#1 (RSA), and SEC1 (EC) key formats.
fn load_private_key(path: &std::path::Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::PrivateKeyDer;

    PrivateKeyDer::from_pem_file(path).map_err(|e| {
        PgWireError::Tls(format!(
            "TLS config error: failed to load private key from '{}': {e}. \
             Supported formats: PKCS#8, PKCS#1 (RSA), SEC1 (EC)",
            path.display()
        ))
    })
}

// ==================== Custom Certificate Verifiers ====================

/// Verifier that accepts any certificate without verification.
///
/// Used for `SslMode::Prefer` and `SslMode::Require`.
///
/// # Security Warning
/// This provides NO protection against man-in-the-middle attacks.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        // Support all common signature schemes
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

/// Verifier that validates the certificate chain but ignores hostname mismatch.
///
/// Used for `SslMode::VerifyCa`.
#[derive(Debug)]
struct VerifyChainOnly {
    inner: Arc<dyn rustls::client::danger::ServerCertVerifier>,
}

impl rustls::client::danger::ServerCertVerifier for VerifyChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Ok(ok) => Ok(ok),
            // VerifyCa: ignore hostname mismatch but enforce chain validation
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // ==================== mTLS misconfiguration detection ====================
    // These catch a common mistake: providing cert without key or vice versa

    #[test]
    fn mtls_requires_both_cert_and_key() {
        // Cert without key
        let tls = TlsConfig {
            client_cert_pem_path: Some("/path/to/cert.pem".into()),
            client_key_pem_path: None,
            ..Default::default()
        };
        let err = build_rustls_config(&tls, false, false, "localhost").unwrap_err();
        assert!(err.to_string().contains("mTLS requires both"));

        // Key without cert
        let tls = TlsConfig {
            client_cert_pem_path: None,
            client_key_pem_path: Some("/path/to/key.pem".into()),
            ..Default::default()
        };
        let err = build_rustls_config(&tls, false, false, "localhost").unwrap_err();
        assert!(err.to_string().contains("mTLS requires both"));
    }

    // ==================== VerifyFull + IP address detection ====================
    // Catches a common mistake: VerifyFull requires hostname, not IP

    #[test]
    fn verify_full_rejects_ip_without_sni_override() {
        let tls = TlsConfig {
            mode: SslMode::VerifyFull,
            ..Default::default()
        };

        // Should fail early: IP address can't match certificate hostname
        let err = build_rustls_config(&tls, true, true, "192.168.1.1").unwrap_err();
        assert!(err.to_string().contains("IP address"));
    }

    // Note: Testing that SNI override allows IP addresses requires a CryptoProvider
    // which isn't available in unit tests. This is covered by integration tests.

    // ==================== File error handling ====================
    // Ensures clear error messages for common file issues

    #[test]
    fn missing_ca_file_gives_clear_error() {
        let tls = TlsConfig {
            ca_pem_path: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        };

        let err = build_root_store(&tls).unwrap_err().to_string();
        assert!(err.contains("failed to open"));
        assert!(err.contains("ca.pem"));
    }

    #[test]
    fn empty_ca_file_gives_clear_error() {
        let f = NamedTempFile::new().unwrap();
        let tls = TlsConfig {
            ca_pem_path: Some(f.path().to_path_buf()),
            ..Default::default()
        };

        let err = build_root_store(&tls).unwrap_err().to_string();
        assert!(err.contains("no valid CA certificates"));
    }

    #[test]
    fn empty_key_file_gives_clear_error() {
        let f = NamedTempFile::new().unwrap();

        let err = load_private_key(f.path()).unwrap_err().to_string();
        assert!(err.contains("failed to load private key"));
    }

    #[test]
    fn invalid_pem_gives_clear_error() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"this is not a valid PEM file").unwrap();

        // Should fail gracefully, not panic
        assert!(load_private_key(f.path()).is_err());
        assert!(load_cert_chain(f.path()).is_err());
    }
}
