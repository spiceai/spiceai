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

use base64::{engine::general_purpose, Engine as _};
use snafu::prelude::*;
use std::io::Write;
use std::str::FromStr;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load all system TLS certificates: {errors:?}"))]
    FailedToLoadCerts {
        errors: Vec<rustls_native_certs::Error>,
    },

    #[snafu(display("Unable to convert PEMs to string: {source}"))]
    FailedToConvertPems { source: std::string::FromUtf8Error },

    #[snafu(display("Unable to connect to endpoint: {source}"))]
    UnableToConnectToEndpoint { source: tonic::transport::Error },

    #[snafu(display("IO error: {source}"))]
    Io { source: std::io::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// # Errors
///
/// Will return `Err` if:
///     - `rustls_native_certs` could not load native certificates.
///     - It couldn't convert the PEMs to a string.
pub fn system_tls_certificate() -> Result<tonic::transport::Certificate> {
    // Load root certificates found in the platform's native certificate store.
    let cert_result = rustls_native_certs::load_native_certs();
    if !cert_result.errors.is_empty() {
        return Err(Error::FailedToLoadCerts {
            errors: cert_result.errors,
        });
    }

    let mut pem = Vec::new();
    for cert in cert_result.certs {
        pem.write_all(b"-----BEGIN CERTIFICATE-----\n")
            .context(IoSnafu)?;
        pem.write_all(general_purpose::STANDARD.encode(cert.as_ref()).as_bytes())
            .context(IoSnafu)?;
        pem.write_all(b"\n-----END CERTIFICATE-----\n")
            .context(IoSnafu)?;
    }

    Ok(tonic::transport::Certificate::from_pem(pem))
}

/// # Errors
///
/// Will return `Err` if:
///    - It couldn't connect to the endpoint.
///    - It couldn't load the system TLS certificate.
pub async fn new_tls_flight_channel(endpoint_str: &str) -> Result<Channel> {
    let mut endpoint = Endpoint::from_str(endpoint_str).context(UnableToConnectToEndpointSnafu)?;

    let mut tls_domain_name = None;
    let tls_prefixes = ["https://", "grpc+tls://"];
    for prefix in &tls_prefixes {
        if endpoint_str.starts_with(prefix) {
            tls_domain_name = Some(endpoint_str.trim_start_matches(prefix));
            break;
        }
    }

    if let Some(tls_domain_name) = tls_domain_name {
        let cert = system_tls_certificate()?;
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(cert)
            .domain_name(tls_domain_name);
        endpoint = endpoint
            .tls_config(tls_config)
            .context(UnableToConnectToEndpointSnafu)?;
    }

    endpoint
        .connect()
        .await
        .context(UnableToConnectToEndpointSnafu)
}
