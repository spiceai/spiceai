/*
Copyright 2024 The Spice.ai OSS Authors

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

use snafu::prelude::*;
use std::str::FromStr;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load system TLS certificate: {source}"))]
    FailedToLoadCerts { source: std::io::Error },

    #[snafu(display("Unable to convert PEMs to string: {source}"))]
    FailedToConvertPems { source: std::string::FromUtf8Error },

    #[snafu(display("Unable to connect to endpoint: {source}"))]
    UnableToConnectToEndpoint { source: tonic::transport::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// # Errors
///
/// Will return `Err` if:
///     - `rustls_native_certs` could not load native certificates.
///     - It couldn't convert the PEMs to a string.
pub fn system_tls_certificate() -> Result<tonic::transport::Certificate> {
    // Load root certificates found in the platform's native certificate store.
    let certs = rustls_native_certs::load_native_certs().context(FailedToLoadCertsSnafu)?;

    let concatenated_pems = certs
        .iter()
        .filter_map(|cert| {
            let mut buf = cert.as_ref();
            rustls_pemfile::certs(&mut buf).ok()?.pop()
        })
        .map(String::from_utf8)
        .collect::<Result<String, _>>()
        .context(FailedToConvertPemsSnafu)?;

    Ok(tonic::transport::Certificate::from_pem(concatenated_pems))
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
