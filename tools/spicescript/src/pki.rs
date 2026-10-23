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

//! The test-only CA and node certificates a cluster's node mTLS uses.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

/// The nodes a certificate is issued for: `<node>.crt` and `<node>.key`.
const NODES: [&str; 2] = ["scheduler", "executor"];

/// Writes `ca.crt` and a certificate and key per node into a directory of
/// this runner's own under `work_dir`, once per run, and returns it: runners
/// sharing a work directory never replace each other's certificates. Each
/// node certificate names 127.0.0.1 and localhost, where every node listens.
pub fn cluster_pki(work_dir: &Path) -> Result<PathBuf> {
    static DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    static CREATING: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    let _guard = CREATING.lock();
    if let Some(dir) = DIR.get() {
        return Ok(dir.clone());
    }
    let dir = std::path::absolute(work_dir.join(format!("cluster-pki-{}", std::process::id())))?;
    std::fs::create_dir_all(&dir)?;
    let mut ca_params = CertificateParams::default();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "spicescript test CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    std::fs::write(dir.join("ca.crt"), ca_cert.pem())?;
    let issuer = Issuer::new(ca_params, ca_key);
    for node in NODES {
        let mut params = CertificateParams::new(vec!["127.0.0.1".into(), "localhost".into()])?;
        params.distinguished_name.push(DnType::CommonName, node);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate()?;
        let cert = params.signed_by(&key, &issuer)?;
        std::fs::write(dir.join(format!("{node}.crt")), cert.pem())?;
        std::fs::write(dir.join(format!("{node}.key")), key.serialize_pem())
            .with_context(|| format!("Failed to write the {node} key"))?;
    }
    Ok(DIR.get_or_init(|| dir).clone())
}
