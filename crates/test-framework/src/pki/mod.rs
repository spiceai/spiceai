/*
Copyright 2026 The Spice.ai OSS Authors

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

//! PKI (Public Key Infrastructure) test helpers for clustered Spice instances.
//!
//! This module provides utilities to initialize a test PKI infrastructure
//! including CA certificates and client certificates for testing clustered
//! Spice runtime configurations.
//!
//! # Example
//!
//! ```no_run
//! use test_framework::pki::{PkiConfig, init_pki};
//! use tempfile::TempDir;
//!
//! # fn main() -> anyhow::Result<()> {
//! let temp_dir = TempDir::new()?;
//! let pki = init_pki(temp_dir.path())?;
//!
//! // Use the generated certificates
//! println!("CA cert: {}", pki.ca_cert_path.display());
//! println!("CA key: {}", pki.ca_key_path.display());
//! # Ok(())
//! # }
//! ```

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use time::{Duration, OffsetDateTime};

/// Default validity period for CA certificates (10 years).
const CA_VALIDITY_YEARS: i64 = 10;

/// Default validity period for client certificates (1 year).
const CLIENT_VALIDITY_YEARS: i64 = 1;

/// Default CA certificate filename.
const CA_CERT_FILENAME: &str = "ca.crt";

/// Default CA private key filename.
const CA_KEY_FILENAME: &str = "ca.key";

/// Common Name for test CA certificates.
const CA_CN: &str = "Spice.ai Test CA - DO NOT USE IN PRODUCTION";

/// Default Organizational Unit for test certificates.
const DEFAULT_OU: &str = "test-framework";

/// Result of PKI initialization containing paths to generated files.
#[derive(Debug, Clone)]
pub struct PkiConfig {
    /// Path to the directory containing all PKI files.
    pub pki_dir: PathBuf,
    /// Path to the CA certificate file.
    pub ca_cert_path: PathBuf,
    /// Path to the CA private key file.
    pub ca_key_path: PathBuf,
}

impl PkiConfig {
    /// Get the path where a client certificate would be stored.
    #[must_use]
    pub fn client_cert_path(&self, client_name: &str) -> PathBuf {
        self.pki_dir.join(format!("{client_name}.crt"))
    }

    /// Get the path where a client private key would be stored.
    #[must_use]
    pub fn client_key_path(&self, client_name: &str) -> PathBuf {
        self.pki_dir.join(format!("{client_name}.key"))
    }

    /// Create a client certificate signed by the CA.
    ///
    /// Generates a new client certificate and private key signed by this CA.
    /// The client certificate is suitable for both client authentication and
    /// server authentication in mutual TLS configurations.
    ///
    /// The generated client certificate:
    /// - Uses ECDSA P-256 for key generation
    /// - Is valid for 1 year
    /// - Has key usages: Digital Signature, Key Encipherment
    /// - Has extended key usages: Client Auth, Server Auth
    /// - Includes "localhost" and "127.0.0.1" as Subject Alternative Names (SANs)
    ///
    /// # Arguments
    ///
    /// * `client_name` - Name for the client certificate. Used as the Common Name (CN)
    ///   and determines the output filenames (`{client_name}.crt`, `{client_name}.key`).
    ///
    /// # Returns
    ///
    /// Returns a [`ClientCertConfig`] containing paths to the generated certificate
    /// and private key.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The client name is empty or contains invalid characters
    /// - CA certificate or key cannot be read
    /// - Certificate generation fails
    /// - File I/O fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use test_framework::pki::init_pki;
    /// use tempfile::TempDir;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let temp_dir = TempDir::new()?;
    /// let pki = init_pki(temp_dir.path())?;
    ///
    /// let client = pki.create_client_cert("node1")?;
    /// assert!(client.cert_path.exists());
    /// assert!(client.key_path.exists());
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_client_cert(&self, client_name: &str) -> anyhow::Result<ClientCertConfig> {
        self.create_client_cert_with_hosts(client_name, &[])
    }

    /// Create a client certificate with additional Subject Alternative Names.
    ///
    /// This is the same as [`Self::create_client_cert`] but allows specifying
    /// additional hosts (DNS names or IP addresses) to include in the
    /// certificate's SANs.
    ///
    /// # Arguments
    ///
    /// * `client_name` - Name for the client certificate.
    /// * `additional_hosts` - Additional hostnames or IP addresses to include in
    ///   the certificate's Subject Alternative Names. Each entry is automatically
    ///   detected as either an IP address or DNS name.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The client name is empty or contains invalid characters
    /// - CA certificate or key cannot be read
    /// - Certificate generation fails
    /// - File I/O fails
    pub fn create_client_cert_with_hosts(
        &self,
        client_name: &str,
        additional_hosts: &[&str],
    ) -> anyhow::Result<ClientCertConfig> {
        validate_client_name(client_name)?;

        let client_cert_path = self.client_cert_path(client_name);
        let client_key_path = self.client_key_path(client_name);

        // Load CA certificate and key
        let ca_cert_pem = fs::read_to_string(&self.ca_cert_path)?;
        let ca_key_pem = fs::read_to_string(&self.ca_key_path)?;

        let ca_key_pair = KeyPair::from_pem(&ca_key_pem)?;
        let ca_issuer = Issuer::from_ca_cert_pem(&ca_cert_pem, ca_key_pair)?;

        // Generate client certificate
        let not_before = OffsetDateTime::now_utc();
        let not_after = not_before + Duration::days(CLIENT_VALIDITY_YEARS * 365);

        // Build SANs - always include localhost and 127.0.0.1
        let mut dns_names = vec!["localhost".to_string()];
        let mut ip_addresses: Vec<IpAddr> = vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)];

        // Add additional hosts to SANs
        for host in additional_hosts {
            if let Ok(ip) = host.parse::<IpAddr>() {
                if !ip_addresses.contains(&ip) {
                    ip_addresses.push(ip);
                }
            } else if !dns_names.contains(&(*host).to_string()) {
                dns_names.push((*host).to_string());
            }
        }

        let mut client_params = CertificateParams::default();
        client_params
            .distinguished_name
            .push(DnType::CommonName, client_name);
        client_params.not_before = not_before;
        client_params.not_after = not_after;
        client_params.is_ca = IsCa::NoCa;
        client_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        client_params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];

        // Add SANs
        for dns in &dns_names {
            client_params
                .subject_alt_names
                .push(SanType::DnsName(dns.clone().try_into()?));
        }
        for ip in &ip_addresses {
            client_params
                .subject_alt_names
                .push(SanType::IpAddress(*ip));
        }

        let client_key_pair = KeyPair::generate()?;
        let client_cert = client_params.signed_by(&client_key_pair, &ca_issuer)?;

        // Write client certificate and key
        fs::write(&client_cert_path, client_cert.pem())?;
        fs::write(&client_key_path, client_key_pair.serialize_pem())?;

        // Set secure permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file_permissions = fs::Permissions::from_mode(0o600);
            fs::set_permissions(&client_cert_path, file_permissions.clone())?;
            fs::set_permissions(&client_key_path, file_permissions)?;
        }

        Ok(ClientCertConfig {
            cert_path: client_cert_path,
            key_path: client_key_path,
            common_name: client_name.to_string(),
        })
    }
}

/// Result of client certificate creation.
#[derive(Debug, Clone)]
pub struct ClientCertConfig {
    /// Path to the client certificate file.
    pub cert_path: PathBuf,
    /// Path to the client private key file.
    pub key_path: PathBuf,
    /// The Common Name (CN) of the client certificate.
    pub common_name: String,
}

/// Initialize a test PKI infrastructure in the specified directory.
///
/// Creates a Certificate Authority (CA) certificate and private key suitable
/// for testing clustered Spice instances with mutual TLS.
///
/// The generated CA:
/// - Uses ECDSA P-256 for key generation
/// - Is valid for 10 years
/// - Has key usages: Digital Signature, Key Cert Sign, CRL Sign
/// - Uses "Spice.ai Test CA - DO NOT USE IN PRODUCTION" as Common Name
///
/// # Arguments
///
/// * `output_dir` - Directory where PKI files should be created. The directory
///   will be created if it doesn't exist.
///
/// # Returns
///
/// Returns a [`PkiConfig`] containing paths to the generated CA certificate
/// and private key.
///
/// # Errors
///
/// Returns an error if:
/// - Directory creation fails
/// - Certificate generation fails
/// - File I/O fails
///
/// # Example
///
/// ```no_run
/// use test_framework::pki::init_pki;
/// use tempfile::TempDir;
///
/// # fn main() -> anyhow::Result<()> {
/// let temp_dir = TempDir::new()?;
/// let pki = init_pki(temp_dir.path())?;
///
/// assert!(pki.ca_cert_path.exists());
/// assert!(pki.ca_key_path.exists());
/// # Ok(())
/// # }
/// ```
pub fn init_pki(output_dir: &Path) -> anyhow::Result<PkiConfig> {
    init_pki_with_ou(output_dir, DEFAULT_OU)
}

/// Initialize a test PKI infrastructure with a custom Organizational Unit.
///
/// This is the same as [`init_pki`] but allows specifying a custom OU
/// for the CA certificate's distinguished name.
///
/// # Arguments
///
/// * `output_dir` - Directory where PKI files should be created.
/// * `organizational_unit` - The Organizational Unit (OU) to include in the
///   CA certificate's distinguished name.
///
/// # Errors
///
/// Returns an error if:
/// - Directory creation fails
/// - Certificate generation fails
/// - File I/O fails
pub fn init_pki_with_ou(output_dir: &Path, organizational_unit: &str) -> anyhow::Result<PkiConfig> {
    // Create PKI directory
    fs::create_dir_all(output_dir)?;

    let ca_cert_path = output_dir.join(CA_CERT_FILENAME);
    let ca_key_path = output_dir.join(CA_KEY_FILENAME);

    // Generate CA certificate and key
    let not_before = OffsetDateTime::now_utc();
    let not_after = not_before + Duration::days(CA_VALIDITY_YEARS * 365);

    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name.push(DnType::CommonName, CA_CN);
    ca_params
        .distinguished_name
        .push(DnType::OrganizationalUnitName, organizational_unit);
    ca_params.not_before = not_before;
    ca_params.not_after = not_after;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];

    let ca_key_pair = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key_pair)?;

    // Write CA certificate and key
    fs::write(&ca_cert_path, ca_cert.pem())?;
    fs::write(&ca_key_path, ca_key_pair.serialize_pem())?;

    // Set secure permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let dir_permissions = fs::Permissions::from_mode(0o700);
        fs::set_permissions(output_dir, dir_permissions)?;

        let file_permissions = fs::Permissions::from_mode(0o600);
        fs::set_permissions(&ca_cert_path, file_permissions.clone())?;
        fs::set_permissions(&ca_key_path, file_permissions)?;
    }

    Ok(PkiConfig {
        pki_dir: output_dir.to_path_buf(),
        ca_cert_path,
        ca_key_path,
    })
}

/// Validate that a client name contains only allowed characters.
///
/// Client names must be non-empty and contain only ASCII alphanumeric
/// characters, hyphens, or underscores.
fn validate_client_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("client name cannot be empty");
    }

    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        anyhow::bail!(
            "client name can only contain letters, numbers, hyphens, and underscores: '{name}'"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_init_pki_creates_ca_files() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let pki_dir = temp_dir.path().join("pki");

        let pki = init_pki(&pki_dir).expect("failed to init PKI");

        assert!(pki.ca_cert_path.exists(), "CA certificate should exist");
        assert!(pki.ca_key_path.exists(), "CA key should exist");

        // Verify files are readable
        let cert_pem = fs::read_to_string(&pki.ca_cert_path).expect("failed to read CA cert");
        let key_pem = fs::read_to_string(&pki.ca_key_path).expect("failed to read CA key");

        assert!(
            cert_pem.contains("BEGIN CERTIFICATE"),
            "CA cert should be PEM format"
        );
        assert!(
            key_pem.contains("BEGIN PRIVATE KEY"),
            "CA key should be PEM format"
        );
    }

    #[test]
    fn test_create_client_cert() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let pki = init_pki(temp_dir.path()).expect("failed to init PKI");

        let client = pki
            .create_client_cert("node1")
            .expect("failed to create client cert");

        assert!(client.cert_path.exists(), "client cert should exist");
        assert!(client.key_path.exists(), "client key should exist");
        assert_eq!(client.common_name, "node1");

        // Verify files are readable
        let cert_pem = fs::read_to_string(&client.cert_path).expect("failed to read client cert");
        let key_pem = fs::read_to_string(&client.key_path).expect("failed to read client key");

        assert!(
            cert_pem.contains("BEGIN CERTIFICATE"),
            "client cert should be PEM format"
        );
        assert!(
            key_pem.contains("BEGIN PRIVATE KEY"),
            "client key should be PEM format"
        );
    }

    #[test]
    fn test_create_multiple_client_certs() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let pki = init_pki(temp_dir.path()).expect("failed to init PKI");

        let client1 = pki
            .create_client_cert("node1")
            .expect("failed to create client1 cert");
        let client2 = pki
            .create_client_cert("node2")
            .expect("failed to create client2 cert");

        assert!(client1.cert_path.exists());
        assert!(client2.cert_path.exists());
        assert_ne!(client1.cert_path, client2.cert_path);
    }

    #[test]
    fn test_create_client_cert_with_additional_hosts() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let pki = init_pki(temp_dir.path()).expect("failed to init PKI");

        let client = pki
            .create_client_cert_with_hosts("node1", &["192.168.1.100", "myhost.local"])
            .expect("failed to create client cert with hosts");

        assert!(client.cert_path.exists());
    }

    #[test]
    fn test_validate_client_name_empty() {
        let result = validate_client_name("");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_client_name_invalid_chars() {
        let result = validate_client_name("node@1");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_client_name_valid() {
        validate_client_name("node1").expect("should be valid");
        validate_client_name("node-1").expect("should be valid");
        validate_client_name("node_1").expect("should be valid");
        validate_client_name("Node-1_Test").expect("should be valid");
    }

    #[test]
    fn test_pki_config_paths() {
        let pki = PkiConfig {
            pki_dir: PathBuf::from("/tmp/pki"),
            ca_cert_path: PathBuf::from("/tmp/pki/ca.crt"),
            ca_key_path: PathBuf::from("/tmp/pki/ca.key"),
        };

        assert_eq!(
            pki.client_cert_path("node1"),
            PathBuf::from("/tmp/pki/node1.crt")
        );
        assert_eq!(
            pki.client_key_path("node1"),
            PathBuf::from("/tmp/pki/node1.key")
        );
    }
}
