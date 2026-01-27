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

//! `spice cluster` command - Cluster operations for Spice runtime.

use crate::error::{
    ConfigIoSnafu, CreateDirectorySnafu, HomeDirectoryNotFoundSnafu, InvalidArgumentSnafu, Result,
};
use ansi_colors::Color;
use clap::{Args, Subcommand};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use snafu::{ResultExt, ensure};
use std::fs;
use std::io::{self, Write};
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Command;
use time::{Duration, OffsetDateTime};

const CA_VALIDITY_YEARS: i64 = 10;
const CLIENT_VALIDITY_YEARS: i64 = 1;
const CA_CERT_FILENAME: &str = "ca.crt";
const CA_KEY_FILENAME: &str = "ca.key";
const PKI_DIR_NAME: &str = "pki";
const DOT_SPICE: &str = ".spice";
const CA_CN: &str = "Spice.ai CLI Root CA - DO NOT USE IN PRODUCTION";
const DEFAULT_OU: &str = "unknown";

/// Arguments for the `cluster` command.
#[derive(Args, Debug)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub command: ClusterCommands,
}

/// Cluster subcommands.
#[derive(Subcommand, Debug)]
pub enum ClusterCommands {
    /// TLS certificate operations for clustered mode
    Tls(TlsArgs),
}

/// TLS subcommand arguments.
#[derive(Args, Debug)]
pub struct TlsArgs {
    #[command(subcommand)]
    pub command: TlsCommands,
}

/// TLS subcommands.
#[derive(Subcommand, Debug)]
pub enum TlsCommands {
    /// Initialize a test PKI infrastructure for clustered mode
    #[command(
        long_about = "Initialize a test PKI infrastructure by generating a new Certificate Authority (CA).\n\n\
        This command creates:\n\
        - A CA certificate (~/.spice/pki/ca.crt)\n\
        - A CA private key (~/.spice/pki/ca.key)\n\n\
        The CA certificate is valid for 10 years and uses ECDSA P-256.\n\n\
        WARNING: This CA is for development and testing purposes only.\n\
                 DO NOT use these certificates in production environments."
    )]
    Init,

    /// Create a new client certificate signed by the CA
    #[command(
        long_about = "Create a new client certificate and private key signed by the CA.\n\n\
        This command creates:\n\
        - A client certificate (~/.spice/pki/<client-name>.crt)\n\
        - A client private key (~/.spice/pki/<client-name>.key)\n\n\
        The client certificate is valid for 1 year and uses ECDSA P-256.\n\
        The certificate includes localhost and 127.0.0.1 as Subject Alternative Names (SANs).\n\n\
        The CA must be initialized first using 'spice cluster tls init'."
    )]
    Add {
        /// Name of the client/node
        client_name: String,

        /// Additional host to include in Subject Alternative Names
        #[arg(long)]
        host: Option<String>,
    },
}

/// Execute the `cluster` command.
///
/// # Errors
///
/// Returns an error if certificate generation or file I/O fails.
pub fn execute(args: &ClusterArgs) -> Result<()> {
    match &args.command {
        ClusterCommands::Tls(tls_args) => match &tls_args.command {
            TlsCommands::Init => execute_tls_init(),
            TlsCommands::Add { client_name, host } => execute_tls_add(client_name, host.as_deref()),
        },
    }
}

/// Get the PKI directory (~/.spice/pki).
fn get_pki_dir() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| HomeDirectoryNotFoundSnafu.build())?;
    Ok(home_dir.join(DOT_SPICE).join(PKI_DIR_NAME))
}

/// Get the organizational unit from git config email, or default.
fn get_organizational_unit() -> String {
    Command::new("git")
        .args(["config", "--get", "user.email"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            } else {
                None
            }
        })
        .unwrap_or_else(|| DEFAULT_OU.to_string())
}

/// Prompt user for confirmation.
fn confirm_overwrite(what: &str) -> bool {
    print!("{what} already exists. Overwrite (y/n)? ");
    let _ = io::stdout().flush();

    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_ok() {
        input.trim().eq_ignore_ascii_case("y")
    } else {
        false
    }
}

/// Validate client name (alphanumeric, hyphens, underscores).
fn validate_client_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty(),
        InvalidArgumentSnafu {
            message: "client name cannot be empty"
        }
    );

    ensure!(
        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        InvalidArgumentSnafu {
            message: "client name can only contain letters, numbers, hyphens, and underscores"
        }
    );

    Ok(())
}

/// Initialize the PKI infrastructure with a new CA.
fn execute_tls_init() -> Result<()> {
    let pki_dir = get_pki_dir()?;
    let ca_cert_path = pki_dir.join(CA_CERT_FILENAME);
    let ca_key_path = pki_dir.join(CA_KEY_FILENAME);

    // Check if CA already exists
    if ca_cert_path.exists() && !confirm_overwrite("CA certificate") {
        println!("Aborted.");
        return Ok(());
    }

    // Create PKI directory
    fs::create_dir_all(&pki_dir).context(CreateDirectorySnafu {
        path: pki_dir.clone(),
    })?;

    // Set secure permissions on PKI directory
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o700);
        fs::set_permissions(&pki_dir, permissions)
            .context(CreateDirectorySnafu { path: pki_dir })?;
    }

    let ou = get_organizational_unit();

    // Generate CA certificate and key
    let not_before = OffsetDateTime::now_utc();
    let not_after = not_before + Duration::days(CA_VALIDITY_YEARS * 365);

    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name.push(DnType::CommonName, CA_CN);
    ca_params
        .distinguished_name
        .push(DnType::OrganizationalUnitName, &ou);
    ca_params.not_before = not_before;
    ca_params.not_after = not_after;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];

    let ca_key_pair = KeyPair::generate().map_err(|e| crate::error::Error::InvalidArgument {
        message: format!("Failed to generate CA key pair: {e}"),
    })?;

    let ca_cert =
        ca_params
            .self_signed(&ca_key_pair)
            .map_err(|e| crate::error::Error::InvalidArgument {
                message: format!("Failed to create CA certificate: {e}"),
            })?;

    // Write CA certificate
    fs::write(&ca_cert_path, ca_cert.pem()).context(ConfigIoSnafu {
        operation: "write",
        path: ca_cert_path.clone(),
    })?;

    // Write CA private key
    fs::write(&ca_key_path, ca_key_pair.serialize_pem()).context(ConfigIoSnafu {
        operation: "write",
        path: ca_key_path.clone(),
    })?;

    // Set secure permissions on files
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o600);
        fs::set_permissions(&ca_cert_path, permissions.clone()).context(ConfigIoSnafu {
            operation: "set permissions on",
            path: ca_cert_path.clone(),
        })?;
        fs::set_permissions(&ca_key_path, permissions).context(ConfigIoSnafu {
            operation: "set permissions on",
            path: ca_key_path.clone(),
        })?;
    }

    println!();
    println!(
        "{}",
        Color::Green.paint("Test PKI infrastructure initialized successfully!")
    );
    println!();
    println!("CA Certificate: {}", ca_cert_path.display());
    println!("CA Private Key: {}", ca_key_path.display());
    println!(
        "Validity:       {CA_VALIDITY_YEARS} years (until {})",
        not_after.date()
    );
    println!("OU:             {ou}");
    println!();
    println!(
        "{}",
        Color::Yellow.paint("WARNING: This CA is for development and testing only.")
    );
    println!(
        "{}",
        Color::Yellow.paint("         DO NOT use these certificates in production!")
    );
    println!();
    println!("Next steps:");
    println!(
        "  Run {} to create a certificate for a cluster member.",
        Color::Cyan.paint("spice cluster tls add <client-name>")
    );

    Ok(())
}

/// Create a client certificate signed by the CA.
fn execute_tls_add(client_name: &str, host: Option<&str>) -> Result<()> {
    validate_client_name(client_name)?;

    let pki_dir = get_pki_dir()?;
    let ca_cert_path = pki_dir.join(CA_CERT_FILENAME);
    let ca_key_path = pki_dir.join(CA_KEY_FILENAME);
    let client_cert_path = pki_dir.join(format!("{client_name}.crt"));
    let client_key_path = pki_dir.join(format!("{client_name}.key"));

    // Check if CA exists
    ensure!(
        ca_cert_path.exists(),
        InvalidArgumentSnafu {
            message: "CA certificate not found. Run 'spice cluster tls init' first."
        }
    );
    ensure!(
        ca_key_path.exists(),
        InvalidArgumentSnafu {
            message: "CA private key not found. Run 'spice cluster tls init' first."
        }
    );

    // Check if client cert already exists
    if client_cert_path.exists() && !confirm_overwrite(&format!("Certificate for '{client_name}'"))
    {
        println!("Aborted.");
        return Ok(());
    }

    // Load CA certificate and key
    let ca_cert_pem = fs::read_to_string(&ca_cert_path).context(ConfigIoSnafu {
        operation: "read",
        path: ca_cert_path,
    })?;
    let ca_key_pem = fs::read_to_string(&ca_key_path).context(ConfigIoSnafu {
        operation: "read",
        path: ca_key_path,
    })?;

    // Parse the CA key
    let ca_key_pair =
        KeyPair::from_pem(&ca_key_pem).map_err(|e| crate::error::Error::InvalidArgument {
            message: format!("Failed to parse CA private key: {e}"),
        })?;

    // Parse the CA certificate from PEM to reconstruct it for signing
    let ca_params = CertificateParams::from_ca_cert_pem(&ca_cert_pem).map_err(|e| {
        crate::error::Error::InvalidArgument {
            message: format!("Failed to parse CA certificate: {e}"),
        }
    })?;

    let ca_cert =
        ca_params
            .self_signed(&ca_key_pair)
            .map_err(|e| crate::error::Error::InvalidArgument {
                message: format!("Failed to reconstruct CA certificate: {e}"),
            })?;

    // Generate client certificate
    let not_before = OffsetDateTime::now_utc();
    let not_after = not_before + Duration::days(CLIENT_VALIDITY_YEARS * 365);

    // Build SANs - always include localhost and 127.0.0.1
    let mut dns_names = vec!["localhost".to_string()];
    let mut ip_addresses: Vec<IpAddr> = vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)];

    // Add optional host to SANs
    if let Some(host) = host {
        if let Ok(ip) = host.parse::<IpAddr>() {
            ip_addresses.push(ip);
        } else {
            dns_names.push(host.to_string());
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
            .push(SanType::DnsName(dns.clone().try_into().map_err(|e| {
                crate::error::Error::InvalidArgument {
                    message: format!("Invalid DNS name '{dns}': {e}"),
                }
            })?));
    }
    for ip in &ip_addresses {
        client_params
            .subject_alt_names
            .push(SanType::IpAddress(*ip));
    }

    let client_key_pair =
        KeyPair::generate().map_err(|e| crate::error::Error::InvalidArgument {
            message: format!("Failed to generate client key pair: {e}"),
        })?;

    let client_cert = client_params
        .signed_by(&client_key_pair, &ca_cert, &ca_key_pair)
        .map_err(|e| crate::error::Error::InvalidArgument {
            message: format!("Failed to create client certificate: {e}"),
        })?;

    // Write client certificate
    fs::write(&client_cert_path, client_cert.pem()).context(ConfigIoSnafu {
        operation: "write",
        path: client_cert_path.clone(),
    })?;

    // Write client private key
    fs::write(&client_key_path, client_key_pair.serialize_pem()).context(ConfigIoSnafu {
        operation: "write",
        path: client_key_path.clone(),
    })?;

    // Set secure permissions on files
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o600);
        fs::set_permissions(&client_cert_path, permissions.clone()).context(ConfigIoSnafu {
            operation: "set permissions on",
            path: client_cert_path.clone(),
        })?;
        fs::set_permissions(&client_key_path, permissions).context(ConfigIoSnafu {
            operation: "set permissions on",
            path: client_key_path.clone(),
        })?;
    }

    println!();
    println!(
        "{}",
        Color::Green.paint(format!(
            "Certificate for '{client_name}' created successfully!"
        ))
    );
    println!();
    println!("Certificate: {}", client_cert_path.display());
    println!("Private Key: {}", client_key_path.display());
    println!(
        "Validity:    {CLIENT_VALIDITY_YEARS} year (until {})",
        not_after.date()
    );
    println!("CN:          {client_name}");
    println!("DNS SANs:    {}", dns_names.join(", "));
    println!(
        "IP SANs:     {}",
        ip_addresses
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );

    Ok(())
}
