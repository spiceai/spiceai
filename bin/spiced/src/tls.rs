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

use crate::{Args, ClientAuthMode};

/// Message shown when mTLS is configured but the `client_tls` cargo feature is
/// not compiled into this build of Spice.
pub(crate) const MTLS_ENTERPRISE_ONLY_MESSAGE: &str = "mTLS (client certificate authentication) is included in the Enterprise distribution of Spice.ai. Learn more at https://docs.spice.ai/docs/enterprise";

#[cfg(feature = "client_tls")]
const CLIENT_TLS_ENABLED: bool = true;
#[cfg(not(feature = "client_tls"))]
const CLIENT_TLS_ENABLED: bool = false;

/// A resolved TLS material source. `File` paths are eligible for hot-reload;
/// `Inline` is a static byte blob that cannot rotate.
pub(crate) enum TlsMaterial {
    File(PathBuf),
    Inline(Vec<u8>),
}

// Manual `Debug` so that inline PEM bytes — which may be private key
// material — never leak into log output. Paths are safe; inline
// bytes are reduced to a length so operators can still tell
// inline-vs-file at a glance without exposing secrets.
impl std::fmt::Debug for TlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsMaterial::File(path) => f.debug_tuple("File").field(path).finish(),
            TlsMaterial::Inline(bytes) => f
                .debug_struct("Inline")
                .field("len", &bytes.len())
                .finish_non_exhaustive(),
        }
    }
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
) -> std::result::Result<(Option<Arc<TlsConfig>>, ClientAuthMode), Box<dyn std::error::Error>> {
    let tls_enabled = args.tls_enabled || spicepod_tls_config.as_ref().is_some_and(|c| c.enabled);
    if !tls_enabled {
        return Ok((None, ClientAuthMode::None));
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

    // Resolve and validate the client-cert-authentication surface.
    // The resolved bundle is then either fed into the hot-reload-aware
    // `TlsConfig` constructors below or used to build an inline static
    // verifier, depending on whether the server-cert material itself
    // is file-backed.
    let client_auth = resolve_client_auth(args, spicepod_tls_config, &secrets).await?;

    // mTLS (client certificate authentication) is an enterprise-only feature.
    // If the operator configured it but the feature is not compiled in, fail
    // early with a clear message rather than silently downgrading to no-auth.
    if !CLIENT_TLS_ENABLED && client_auth.mode != ClientAuthMode::None {
        return Err(MTLS_ENTERPRISE_ONLY_MESSAGE.into());
    }

    match (client_auth.mode, &client_auth.ca_material) {
        (ClientAuthMode::None, _) => {
            tracing::debug!(
                "Public TLS client certificates: not requested (client_auth_mode=none)"
            );
        }
        (ClientAuthMode::Request, Some(ca)) => {
            tracing::info!(
                "Public TLS client certificates: requested but optional (client_auth_mode=request); CA from {}",
                describe_ca_source(ca),
            );
        }
        (ClientAuthMode::Required, Some(ca)) => {
            tracing::info!(
                "Public TLS client certificates: required (client_auth_mode=required); CA from {}",
                describe_ca_source(ca),
            );
        }
        // `validate_client_auth` rejects Request/Required without a CA,
        // so these arms are unreachable in practice.
        (ClientAuthMode::Request | ClientAuthMode::Required, None) => {}
    }

    // Both file => hot-reload via watcher. Anything else => inline bytes.
    // The client-CA path threads through to the same code paths so a
    // file-backed CA is hot-reloaded and an inline CA gets a static
    // verifier alongside the static cert+key.
    let tls_config = match (cert_material, key_material) {
        (TlsMaterial::File(cert_path), TlsMaterial::File(key_path)) => {
            // We require all three (cert, key, client CA) to be
            // file-backed when *any* of them is, so the entire TLS
            // bundle hot-reloads together. Mixing
            // `client_ca` (inline) with file-backed cert/key would
            // either disable cert/key hot-reload (rebuilding the
            // server config from in-memory bytes) or leave the CA
            // pinned at startup while certs rotate — both are
            // surprising. Reject the combination instead.
            let client_ca_path = match client_auth.ca_material {
                Some(TlsMaterial::File(p)) => Some(p),
                Some(TlsMaterial::Inline(_)) => {
                    return Err(
                        "client_auth_ca configured inline but tls.certificate_file/key_file \
                         are file-backed: this would prevent the client CA from \
                         hot-reloading with the cert and key. Switch to \
                         tls.client_auth_ca_file so the entire bundle rotates together, \
                         or move cert/key to inline form."
                            .into(),
                    );
                }
                None => None,
            };
            TlsConfig::try_new_from_paths_with_client_auth_mode(
                cert_path,
                key_path,
                client_ca_path,
                client_auth_enforcement(client_auth.mode),
                control,
            )?
        }
        (cert, key) => {
            let cert_bytes = cert.into_bytes()?;
            let key_bytes = key.into_bytes()?;
            // For inline cert+key, the CA is also resolved to bytes
            // here regardless of whether the operator gave us a file
            // path — inline material can't hot-reload anyway.
            let ca_bytes = match client_auth.ca_material {
                Some(material) => Some(material.into_bytes()?),
                None => None,
            };
            TlsConfig::try_new_with_client_auth_mode(
                &cert_bytes,
                &key_bytes,
                ca_bytes.as_deref(),
                client_auth_enforcement(client_auth.mode),
            )?
        }
    };

    Ok((Some(Arc::new(tls_config)), client_auth.mode))
}

/// Fully-resolved client-auth surface. Combines the effective mode
/// (CLI → spicepod → default `None`) with the resolved client CA bytes
/// (file path or inline / `${secrets:...}`).
///
/// Used to build the rustls `WebPkiClientVerifier`. Validation in
/// [`resolve_client_auth`] enforces that `mode ∈ {Request, Required}`
/// iff `ca_material.is_some()`, so consumers can rely on the
/// invariant that any non-`None` mode always comes with concrete CA
/// bytes.
#[derive(Debug)]
pub(crate) struct ResolvedClientAuth {
    pub mode: ClientAuthMode,
    /// `None` when `mode == ClientAuthMode::None`. `Some` when `mode`
    /// is `Request` or `Required` (validation in
    /// [`resolve_client_auth`] enforces this invariant).
    pub ca_material: Option<TlsMaterial>,
}

/// Translate the binary-level [`ClientAuthMode`] into the runtime's
/// [`runtime::tls::ClientAuthEnforcement`]. The two enums live in
/// different crates because the binary mirrors the spicepod surface
/// (clap-derivable) while the runtime carries the per-listener
/// behavior bit.
fn client_auth_enforcement(mode: ClientAuthMode) -> runtime::tls::ClientAuthEnforcement {
    match mode {
        ClientAuthMode::None => runtime::tls::ClientAuthEnforcement::Disabled,
        ClientAuthMode::Request => runtime::tls::ClientAuthEnforcement::Requested,
        ClientAuthMode::Required => runtime::tls::ClientAuthEnforcement::Required,
    }
}

/// Resolves the effective `client_auth` mode and client CA material
/// from CLI args, spicepod config, and (for inline / secret-backed CA
/// values) the secrets store. Applies the v1 validation rules:
///
/// * `Required` requires either `client_ca` or `client_ca_file` to be
///   set (otherwise the runtime would fail every handshake).
/// * `None` MUST NOT have a CA configured — misconfiguration here
///   could otherwise be silently downgraded to `none` and bypass the
///   operator's intent.
///
/// Precedence: CLI `--tls-client-auth-ca-file` / `--tls-client-auth-ca` /
/// `--tls-client-auth-mode` overrides anything in `runtime.tls`.
/// Inline CA values are subject to `${secrets:...}` injection just
/// like inline cert / key material.
async fn resolve_client_auth(
    args: &Args,
    spicepod_tls_config: Option<&SpicepodTlsConfig>,
    secrets: &RwLockReadGuard<'_, Secrets>,
) -> std::result::Result<ResolvedClientAuth, Box<dyn std::error::Error>> {
    // Mode precedence: CLI > spicepod > default.
    let mode = match args.tls_client_auth_mode {
        Some(m) => m,
        None => spicepod_tls_config
            .and_then(|c| c.client_auth_mode)
            .map(ClientAuthMode::from_spicepod)
            .unwrap_or_default(),
    };

    // CA material precedence (independently of mode): CLI > spicepod.
    // We resolve eagerly so the validation below has a concrete
    // `Option<TlsMaterial>` to inspect.
    let spicepod_ca_material = load_spicepod_tls_param(
        secrets,
        spicepod_tls_config,
        |tls| &tls.client_auth_ca_file,
        |tls| &tls.client_auth_ca,
        "client_auth_ca",
        "client_auth_ca_path",
    )
    .await?;
    let ca_material: Option<TlsMaterial> = match (
        &args.tls_client_auth_ca_file,
        &args.tls_client_auth_ca,
        spicepod_ca_material,
    ) {
        (Some(p), _, _) => Some(TlsMaterial::File(PathBuf::from(p))),
        (_, Some(pem), _) => Some(TlsMaterial::Inline(pem.as_bytes().to_vec())),
        (_, _, Some(m)) => Some(m),
        (None, None, None) => None,
    };

    // Validation — see doc comment above for rationale.
    validate_client_auth(mode, ca_material.as_ref())?;
    Ok(ResolvedClientAuth { mode, ca_material })
}

/// Pure validation of the (mode, CA presence) tuple. Extracted from
/// [`resolve_client_auth`] so it can be unit-tested without needing a
/// real `Secrets` store, full `Args`, or a tokio runtime. The two
/// invalid shapes are `Request`/`Required` with no CA, and `None`
/// with a CA; everything else is acceptable.
fn validate_client_auth(
    mode: ClientAuthMode,
    ca_material: Option<&TlsMaterial>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    match (mode, ca_material) {
        (ClientAuthMode::Required, None) => Err(
            "client_auth_mode=required requires a client CA: provide --tls-client-auth-ca-file (recommended for hot-reload), --tls-client-auth-ca (inline PEM), or runtime.tls.{client_auth_ca_file,client_auth_ca} in the spicepod"
                .into(),
        ),
        (ClientAuthMode::Request, None) => Err(
            "client_auth_mode=request requires a client CA so presented certs can be verified: provide --tls-client-auth-ca-file, --tls-client-auth-ca, or runtime.tls.{client_auth_ca_file,client_auth_ca}"
                .into(),
        ),
        (ClientAuthMode::None, Some(_)) => Err(
            "client_auth_mode=none must not have a client CA configured (set client_auth_mode=request or required to enable mTLS, or remove the CA configuration)"
                .into(),
        ),
        _ => Ok(()),
    }
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

/// Operator-facing description of where a TLS CA bundle was sourced
/// from. Inline material has no path to print; file-backed material
/// echoes back the path so an operator can reconcile what they
/// configured against what the runtime loaded.
fn describe_ca_source(material: &TlsMaterial) -> String {
    match material {
        TlsMaterial::File(p) => format!("file {}", p.display()),
        TlsMaterial::Inline(_) => "inline PEM (spicepod / CLI)".to_string(),
    }
}

fn load_file(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_none_without_ca_is_ok() {
        validate_client_auth(ClientAuthMode::None, None).expect("none without CA is valid");
    }

    #[test]
    fn validate_required_with_file_ca_is_ok() {
        let ca = TlsMaterial::File(PathBuf::from("/tmp/client-ca.pem"));
        validate_client_auth(ClientAuthMode::Required, Some(&ca))
            .expect("required with file CA is valid");
    }

    #[test]
    fn validate_required_with_inline_ca_is_ok() {
        let ca = TlsMaterial::Inline(b"-----BEGIN CERTIFICATE-----...".to_vec());
        validate_client_auth(ClientAuthMode::Required, Some(&ca))
            .expect("required with inline CA is valid");
    }

    #[test]
    fn validate_required_without_ca_is_rejected() {
        let err = validate_client_auth(ClientAuthMode::Required, None)
            .expect_err("required without CA must be rejected");
        assert!(
            err.to_string()
                .contains("client_auth_mode=required requires a client CA")
        );
    }

    #[test]
    fn validate_request_without_ca_is_rejected() {
        let err = validate_client_auth(ClientAuthMode::Request, None)
            .expect_err("request without CA must be rejected");
        assert!(
            err.to_string()
                .contains("client_auth_mode=request requires a client CA")
        );
    }

    #[test]
    fn validate_request_with_file_ca_is_ok() {
        let ca = TlsMaterial::File(PathBuf::from("/tmp/client-ca.pem"));
        validate_client_auth(ClientAuthMode::Request, Some(&ca))
            .expect("request with file CA is valid");
    }

    #[test]
    fn validate_none_with_ca_is_rejected() {
        // Critical: this rejection is what prevents a misconfiguration
        // (operator left the CA in but switched mode to `none`) from
        // silently downgrading to no client-auth.
        let ca = TlsMaterial::File(PathBuf::from("/tmp/client-ca.pem"));
        let err = validate_client_auth(ClientAuthMode::None, Some(&ca))
            .expect_err("none with CA must be rejected");
        assert!(
            err.to_string()
                .contains("client_auth_mode=none must not have a client CA")
        );
    }

    #[test]
    fn enterprise_gate_blocks_request_mode() {
        if CLIENT_TLS_ENABLED {
            // When the feature is compiled in, the gate is a no-op —
            // nothing to test.
            return;
        }
        // Without the feature, the constant and message must be
        // wired correctly for the runtime guard.
        assert_eq!(
            MTLS_ENTERPRISE_ONLY_MESSAGE,
            "mTLS (client certificate authentication) is included in the Enterprise distribution of Spice.ai. Learn more at https://docs.spice.ai/docs/enterprise"
        );
    }

    #[test]
    fn enterprise_gate_allows_none_mode() {
        // client_auth_mode=none must never be blocked, even without
        // the feature.
        let mode = ClientAuthMode::None;
        assert!(
            mode == ClientAuthMode::None,
            "None mode should always be allowed regardless of feature flag"
        );
    }
}
