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

use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    ServerConfig,
};
use rustls_pemfile::{certs, private_key};
use secrecy::{ExposeSecret, Secret};
use std::{
    io::{self, Cursor},
    sync::Arc,
};
use x509_certificate::X509Certificate;

pub struct TlsConfig {
    pub cert: Secret<Vec<u8>>,
    pub key: Secret<Vec<u8>>,
    pub server_config: Arc<ServerConfig>,
}

impl TlsConfig {
    pub fn try_new(
        cert_bytes: Vec<u8>,
        key_bytes: Vec<u8>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let certs = load_certs(&cert_bytes)?;
        let key = load_key(&key_bytes)?;

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

        Ok(Self {
            cert: Secret::new(cert_bytes),
            key: Secret::new(key_bytes),
            server_config: Arc::new(config),
        })
    }

    #[must_use]
    pub fn subject_name(&self) -> Option<String> {
        let x509_cert = X509Certificate::from_pem(self.cert.expose_secret()).ok()?;
        x509_cert.subject_name().user_friendly_str().ok()
    }
}

fn load_certs(cert_bytes: &[u8]) -> io::Result<Vec<CertificateDer<'static>>> {
    let mut cursor = Cursor::new(cert_bytes);
    certs(&mut cursor).collect()
}

fn load_key(
    key_bytes: &[u8],
) -> std::result::Result<PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(key_bytes);
    private_key(&mut cursor)?.ok_or_else(|| "No private key found in provided TLS key".into())
}
