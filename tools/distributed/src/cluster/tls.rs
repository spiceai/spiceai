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

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;

/// Get the PKI directory path.
pub fn get_pki_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().context("Failed to get home directory")?;
    Ok(home.join(".spice/pki"))
}

/// Check if TLS certificates exist for the given node names.
#[expect(dead_code)]
pub fn certificates_exist(node_names: &[&str]) -> Result<bool> {
    let pki_dir = get_pki_dir()?;
    for name in node_names {
        let cert_path = pki_dir.join(format!("{name}.crt"));
        let key_path = pki_dir.join(format!("{name}.key"));
        if !cert_path.exists() || !key_path.exists() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Initialize TLS by running `spice cluster tls init`.
pub fn ensure_tls_initialized() -> Result<()> {
    let pki_dir = get_pki_dir()?;

    // Check if CA certificate exists
    let ca_cert = pki_dir.join("ca.crt");
    let ca_key = pki_dir.join("ca.key");

    if ca_cert.exists() && ca_key.exists() {
        return Ok(());
    }

    // Run spice cluster tls init
    let spice_path = get_spice_cli_path()?;
    let output = Command::new(&spice_path)
        .arg("cluster")
        .arg("tls")
        .arg("init")
        .output()
        .context("Failed to execute 'spice cluster tls init'")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "Failed to initialize TLS certificates: {stderr}"
        ));
    }

    Ok(())
}

/// Generate certificates for the given node names.
pub fn ensure_certificates(node_names: &[&str]) -> Result<()> {
    let spice_path = get_spice_cli_path()?;

    for name in node_names {
        // Check if certificate already exists
        let pki_dir = get_pki_dir()?;
        let cert_path = pki_dir.join(format!("{name}.crt"));
        let key_path = pki_dir.join(format!("{name}.key"));

        if cert_path.exists() && key_path.exists() {
            continue;
        }

        // Generate certificate using 'spice cluster tls add'
        let output = Command::new(&spice_path)
            .arg("cluster")
            .arg("tls")
            .arg("add")
            .arg(name)
            .output()
            .context(format!("Failed to generate certificate for {name}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "Failed to generate certificate for {name}: {stderr}"
            ));
        }
    }

    Ok(())
}

/// Validate that all required TLS files exist for the cluster.
/// Called when --no-tls-init is used to ensure certificates are already set up.
pub fn validate_tls_files(num_executors: usize) -> Result<()> {
    let pki_dir = get_pki_dir()?;

    // Check CA certificate
    let ca_cert = pki_dir.join("ca.crt");
    let ca_key = pki_dir.join("ca.key");
    if !ca_cert.exists() || !ca_key.exists() {
        return Err(anyhow::anyhow!(
            "CA certificate files not found in {}. Run 'spice cluster tls init' first.",
            pki_dir.display()
        ));
    }

    // Check scheduler certificate
    let scheduler_cert = pki_dir.join("scheduler1.crt");
    let scheduler_key = pki_dir.join("scheduler1.key");
    if !scheduler_cert.exists() || !scheduler_key.exists() {
        return Err(anyhow::anyhow!(
            "Scheduler certificate files not found in {}. Run 'spice cluster tls add scheduler1' first.",
            pki_dir.display()
        ));
    }

    // Check executor certificates
    for i in 0..num_executors {
        let executor_name = format!("executor{}", i + 1);
        let executor_cert = pki_dir.join(format!("{executor_name}.crt"));
        let executor_key = pki_dir.join(format!("{executor_name}.key"));
        if !executor_cert.exists() || !executor_key.exists() {
            return Err(anyhow::anyhow!(
                "Executor certificate files not found for {} in {}. Run 'spice cluster tls add {executor_name}' first.",
                executor_name,
                pki_dir.display()
            ));
        }
    }

    Ok(())
}

/// Get the path to the spice CLI binary.
fn get_spice_cli_path() -> Result<PathBuf> {
    // Try to find spice in PATH
    if let Ok(output) = Command::new("which").arg("spice").output()
        && output.status.success()
    {
        let path = String::from_utf8_lossy(&output.stdout);
        return Ok(PathBuf::from(path.trim()));
    }

    // Fallback to default location
    let home = dirs::home_dir().context("Failed to get home directory")?;
    let default_path = home.join(".spice/bin/spice");

    if default_path.exists() {
        Ok(default_path)
    } else {
        Err(anyhow::anyhow!(
            "Could not find 'spice' CLI. Please ensure it's installed and in your PATH."
        ))
    }
}
