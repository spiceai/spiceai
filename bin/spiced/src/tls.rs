/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;

use app::spicepod::component::runtime::TlsConfig as SpicepodTlsConfig;
use runtime::secrets::{ExposeSecret, ParamStr, Secrets};
use runtime::tls::TlsConfig;
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::Args;

pub(crate) async fn load_tls_config(
    args: &Args,
    spicepod_tls_config: Option<&SpicepodTlsConfig>,
    secrets: Arc<RwLock<Secrets>>,
) -> std::result::Result<Option<Arc<TlsConfig>>, Box<dyn std::error::Error>> {
    let tls_enabled = args.tls_enabled || spicepod_tls_config.as_ref().is_some_and(|c| c.enabled);
    if !tls_enabled {
        return Ok(None);
    }

    let secrets = secrets.read().await;

    let app_cert_bytes = load_spicepod_tls_param(
        &secrets,
        spicepod_tls_config,
        |tls| &tls.certificate_file,
        |tls| &tls.certificate,
        "certificate",
        "certificate_path",
    )
    .await?;

    let app_key_bytes = load_spicepod_tls_param(
        &secrets,
        spicepod_tls_config,
        |tls| &tls.key_file,
        |tls| &tls.key,
        "key",
        "key_path",
    )
    .await?;

    let cert_bytes: Vec<u8> = match (
        &args.tls_certificate_file,
        &args.tls_certificate,
        app_cert_bytes,
    ) {
        (Some(cert_path), _, _) => load_file(Path::new(cert_path))?,
        (_, Some(cert), _) => cert.as_bytes().to_vec(),
        (_, _, Some(cert)) => cert,
        (None, None, None) => return Err("TLS certificate is required (--tls-certificate)".into()),
    };
    let key_bytes: Vec<u8> = match (&args.tls_key_file, &args.tls_key, app_key_bytes) {
        (Some(key_path), _, _) => load_file(Path::new(key_path))?,
        (_, Some(key), _) => key.as_bytes().to_vec(),
        (_, _, Some(key)) => key,
        (None, None, None) => return Err("TLS key is required (--tls-key)".into()),
    };

    let tls_config = TlsConfig::try_new(cert_bytes, key_bytes)?;

    Ok(Some(Arc::new(tls_config)))
}

async fn load_spicepod_tls_param(
    secrets: &RwLockReadGuard<'_, Secrets>,
    spicepod_tls_config: Option<&SpicepodTlsConfig>,
    file_field: impl Fn(&SpicepodTlsConfig) -> &Option<String>,
    secret_field: impl Fn(&SpicepodTlsConfig) -> &Option<String>,
    secret_name: &str,
    param_name: &str,
) -> std::result::Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let Some(tls) = spicepod_tls_config else {
        return Ok(None);
    };

    let bytes = match (file_field(tls), secret_field(tls)) {
        (Some(file_path), _) => {
            tracing::debug!("Loading TLS {} from file: {}", secret_name, file_path);
            let injected_path = secrets
                .inject_secrets(param_name, ParamStr(file_path))
                .await;
            Some(load_file(Path::new(injected_path.expose_secret()))?)
        }
        (_, Some(secret)) => {
            let injected_secret = secrets.inject_secrets(secret_name, ParamStr(secret)).await;
            Some(injected_secret.expose_secret().as_bytes().to_vec())
        }
        _ => None,
    };

    Ok(bytes)
}

fn load_file(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}
