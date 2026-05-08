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

use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use app::spicepod::component::runtime::TlsConfig as SpicepodTlsConfig;
use runtime::secrets::{ExposeSecret, ParamStr, Secrets};
use runtime::tls::{TlsConfig, TlsControl};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::Args;

/// A resolved TLS material source. `File` paths are eligible for hot-reload;
/// `Inline` is a static byte blob that cannot rotate.
enum TlsMaterial {
    File(PathBuf),
    Inline(Vec<u8>),
}

impl TlsMaterial {
    fn into_bytes(self) -> io::Result<Vec<u8>> {
        match self {
            TlsMaterial::File(p) => load_file(&p),
            TlsMaterial::Inline(b) => Ok(b),
        }
    }
}

pub(crate) async fn load_tls_config(
    args: &Args,
    spicepod_tls_config: Option<&SpicepodTlsConfig>,
    secrets: Arc<RwLock<Secrets>>,
    control: &TlsControl,
) -> std::result::Result<Option<Arc<TlsConfig>>, Box<dyn std::error::Error>> {
    let tls_enabled = args.tls_enabled || spicepod_tls_config.as_ref().is_some_and(|c| c.enabled);
    if !tls_enabled {
        return Ok(None);
    }

    let secrets = secrets.read().await;

    let app_cert_material = load_spicepod_tls_param(
        &secrets,
        spicepod_tls_config,
        |tls| &tls.certificate_file,
        |tls| &tls.certificate,
        "certificate",
        "certificate_path",
    )
    .await?;

    let app_key_material = load_spicepod_tls_param(
        &secrets,
        spicepod_tls_config,
        |tls| &tls.key_file,
        |tls| &tls.key,
        "key",
        "key_path",
    )
    .await?;

    let cert_material: TlsMaterial = match (
        &args.tls_certificate_file,
        &args.tls_certificate,
        app_cert_material,
    ) {
        (Some(cert_path), _, _) => TlsMaterial::File(PathBuf::from(cert_path)),
        (_, Some(cert), _) => TlsMaterial::Inline(cert.as_bytes().to_vec()),
        (_, _, Some(cert)) => cert,
        (None, None, None) => {
            return Err(
                "TLS certificate is required: provide --tls-certificate-file (recommended for hot-reload), --tls-certificate (inline PEM), or runtime.tls.{certificate_file,certificate} in the spicepod"
                    .into(),
            );
        }
    };
    let key_material: TlsMaterial = match (&args.tls_key_file, &args.tls_key, app_key_material) {
        (Some(key_path), _, _) => TlsMaterial::File(PathBuf::from(key_path)),
        (_, Some(key), _) => TlsMaterial::Inline(key.as_bytes().to_vec()),
        (_, _, Some(key)) => key,
        (None, None, None) => {
            return Err(
                "TLS key is required: provide --tls-key-file (recommended for hot-reload), --tls-key (inline PEM), or runtime.tls.{key_file,key} in the spicepod"
                    .into(),
            );
        }
    };

    // Both file => hot-reload via watcher. Anything else => inline bytes.
    let tls_config = match (cert_material, key_material) {
        (TlsMaterial::File(cert_path), TlsMaterial::File(key_path)) => {
            TlsConfig::try_new_from_paths(cert_path, key_path, control)?
        }
        (cert, key) => {
            let cert_bytes = cert.into_bytes()?;
            let key_bytes = key.into_bytes()?;
            TlsConfig::try_new(&cert_bytes, &key_bytes)?
        }
    };

    Ok(Some(Arc::new(tls_config)))
}

async fn load_spicepod_tls_param(
    secrets: &RwLockReadGuard<'_, Secrets>,
    spicepod_tls_config: Option<&SpicepodTlsConfig>,
    file_field: impl Fn(&SpicepodTlsConfig) -> &Option<String>,
    secret_field: impl Fn(&SpicepodTlsConfig) -> &Option<String>,
    secret_name: &str,
    param_name: &str,
) -> std::result::Result<Option<TlsMaterial>, Box<dyn std::error::Error>> {
    let Some(tls) = spicepod_tls_config else {
        return Ok(None);
    };

    let material = match (file_field(tls), secret_field(tls)) {
        (Some(file_path), _) => {
            tracing::debug!("Loading TLS {} from file: {}", secret_name, file_path);
            let injected_path = secrets
                .inject_secrets(param_name, ParamStr(file_path))
                .await;
            Some(TlsMaterial::File(PathBuf::from(
                injected_path.expose_secret(),
            )))
        }
        (_, Some(secret)) => {
            let injected_secret = secrets.inject_secrets(secret_name, ParamStr(secret)).await;
            Some(TlsMaterial::Inline(
                injected_secret.expose_secret().as_bytes().to_vec(),
            ))
        }
        _ => None,
    };

    Ok(material)
}

fn load_file(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}
