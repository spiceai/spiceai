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

use std::path::PathBuf;

pub const TLS_CLIENT_CERTIFICATE_FILE: &str = "tls_client_certificate_file";
pub const TLS_CLIENT_KEY_FILE: &str = "tls_client_key_file";
pub const TLS_CLIENT_CERTIFICATE: &str = "tls_client_certificate";
pub const TLS_CLIENT_KEY: &str = "tls_client_key";
pub const TLS_CLIENT_IDENTITY_PARAM_NAMES: [&str; 4] = [
    TLS_CLIENT_CERTIFICATE_FILE,
    TLS_CLIENT_KEY_FILE,
    TLS_CLIENT_CERTIFICATE,
    TLS_CLIENT_KEY,
];

#[derive(Debug)]
pub enum ClientIdentityConfig {
    FromFiles {
        cert_path: PathBuf,
        key_path: PathBuf,
    },
    FromPem {
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
    },
}

#[derive(Debug)]
pub enum ClientIdentityConfigError {
    Incomplete {
        set_field: &'static str,
        missing_field: &'static str,
    },
    Ambiguous,
}

/// Resolves the mTLS client identity from the file-based parameter pair or the
/// inline PEM parameter pair.
pub fn resolve_client_identity_config(
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
    cert_inline: Option<Vec<u8>>,
    key_inline: Option<Vec<u8>>,
) -> Result<Option<ClientIdentityConfig>, ClientIdentityConfigError> {
    let has_file_cert = cert_file.is_some();
    let has_file_key = key_file.is_some();
    let has_inline_cert = cert_inline.is_some();
    let has_inline_key = key_inline.is_some();

    if (has_file_cert || has_file_key) && (has_inline_cert || has_inline_key) {
        return Err(ClientIdentityConfigError::Ambiguous);
    }

    if has_file_cert || has_file_key {
        return match (cert_file, key_file) {
            (Some(cert_path), Some(key_path)) => Ok(Some(ClientIdentityConfig::FromFiles {
                cert_path,
                key_path,
            })),
            (Some(_), None) => Err(ClientIdentityConfigError::Incomplete {
                set_field: TLS_CLIENT_CERTIFICATE_FILE,
                missing_field: TLS_CLIENT_KEY_FILE,
            }),
            (None, Some(_)) => Err(ClientIdentityConfigError::Incomplete {
                set_field: TLS_CLIENT_KEY_FILE,
                missing_field: TLS_CLIENT_CERTIFICATE_FILE,
            }),
            (None, None) => Ok(None),
        };
    }

    if has_inline_cert || has_inline_key {
        return match (cert_inline, key_inline) {
            (Some(cert_pem), Some(key_pem)) => {
                Ok(Some(ClientIdentityConfig::FromPem { cert_pem, key_pem }))
            }
            (Some(_), None) => Err(ClientIdentityConfigError::Incomplete {
                set_field: TLS_CLIENT_CERTIFICATE,
                missing_field: TLS_CLIENT_KEY,
            }),
            (None, Some(_)) => Err(ClientIdentityConfigError::Incomplete {
                set_field: TLS_CLIENT_KEY,
                missing_field: TLS_CLIENT_CERTIFICATE,
            }),
            (None, None) => Ok(None),
        };
    }

    Ok(None)
}
