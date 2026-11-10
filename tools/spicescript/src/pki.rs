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

//! The node mTLS credentials a cluster run uses: a CA and one certificate and
//! key per node role, generated for each run into its work directory, so no
//! private key is kept in the repository.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, SanType,
};

/// The node roles that get a certificate: `<role>.crt` and `<role>.key`.
pub const ROLES: [&str; 2] = ["scheduler", "executor"];

/// Writes `ca.crt` and, for each of [`ROLES`], `<role>.crt` and `<role>.key`
/// into `dir`, and returns `dir`. Each node certificate is valid for
/// `localhost` and `127.0.0.1`, for both server and client authentication,
/// since a cluster node is both.
pub fn generate(dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).with_context(|| {
        format!(
            "Failed to create the cluster PKI directory {}",
            dir.display()
        )
    })?;
    let mut ca_name = DistinguishedName::new();
    ca_name.push(DnType::CommonName, "spicescript test CA");
    let mut ca = CertificateParams::default();
    ca.distinguished_name = ca_name;
    ca.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().context("Failed to generate the cluster CA key")?;
    let ca_cert = ca
        .self_signed(&ca_key)
        .context("Failed to sign the cluster CA certificate")?;
    write(dir, "ca.crt", &ca_cert.pem())?;
    let issuer = Issuer::new(ca, ca_key);
    for role in ROLES {
        let mut name = DistinguishedName::new();
        name.push(DnType::CommonName, role);
        let mut node = CertificateParams::default();
        node.distinguished_name = name;
        node.subject_alt_names = vec![
            SanType::DnsName("localhost".try_into()?),
            SanType::IpAddress(std::net::Ipv4Addr::LOCALHOST.into()),
        ];
        node.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        node.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key =
            KeyPair::generate().with_context(|| format!("Failed to generate the {role} key"))?;
        let cert = node
            .signed_by(&key, &issuer)
            .with_context(|| format!("Failed to sign the {role} certificate"))?;
        write(dir, &format!("{role}.crt"), &cert.pem())?;
        write(dir, &format!("{role}.key"), &key.serialize_pem())?;
    }
    Ok(dir.to_path_buf())
}

fn write(dir: &Path, name: &str, pem: &str) -> Result<()> {
    let path = dir.join(name);
    std::fs::write(&path, pem).with_context(|| format!("Failed to write {}", path.display()))
}
