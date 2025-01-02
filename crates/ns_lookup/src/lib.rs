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

use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use snafu::prelude::*;
use trust_dns_resolver::AsyncResolver;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to {host}:{port}, are the host and port correct?"))]
    UnableToConnect { host: String, port: u16 },

    #[snafu(display("Failed to parse endpoint {endpoint}: {source}"))]
    UnableToParseUrl {
        endpoint: String,
        source: url::ParseError,
    },

    #[snafu(display("Invalid endpoint (no host provided): {endpoint}"))]
    InvalidHost { endpoint: String },

    #[snafu(display("Invalid endpoint (no port specified): {endpoint}"))]
    InvalidPort { endpoint: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Verify NS lookup and TCP connect for the provided `endpoint`.
///
/// # Arguments
///
/// * `endpoint` - The endpoint to lookup.
///
/// # Errors
///
/// Returns `Error` if unable to parse endpoint or if the NS lookup or TCP connect fails.
pub async fn verify_endpoint_connection(endpoint: &str) -> Result<()> {
    let url = url::Url::parse(endpoint).context(UnableToParseUrlSnafu {
        endpoint: endpoint.to_string(),
    })?;

    let host = url.host_str().context(InvalidHostSnafu {
        endpoint: endpoint.to_string(),
    })?;

    let port = url.port_or_known_default().context(InvalidPortSnafu {
        endpoint: endpoint.to_string(),
    })?;

    verify_ns_lookup_and_tcp_connect(host, port).await
}

/// Verify NS lookup and TCP connect of the provided `host` and `port`.
///
/// # Arguments
///
/// * `host` - The host to lookup.
/// * `port` - The port to connect to.
///
/// # Errors
///
/// Returns an `Error` if the NS lookup or TCP connect fails.
pub async fn verify_ns_lookup_and_tcp_connect(host: &str, port: u16) -> Result<()> {
    // DefaultConfig uses google as upstream nameservers which won't work for kubernetes name
    // resolving
    let resolver = AsyncResolver::tokio_from_system_conf().map_err(|_| Error::UnableToConnect {
        host: host.to_string(),
        port,
    })?;
    match resolver.lookup_ip(host).await {
        Ok(ips) => {
            for ip in ips.iter() {
                let addr = SocketAddr::new(ip, port);
                if TcpStream::connect_timeout(&addr, Duration::from_secs(30)).is_ok() {
                    return Ok(());
                }
            }

            tracing::debug!("Failed to connect to {host}:{port}, connection timed out");

            UnableToConnectSnafu {
                host: host.to_string(),
                port,
            }
            .fail()
        }
        Err(err) => {
            tracing::debug!("Failed to resolve host: {err}");
            UnableToConnectSnafu {
                host: host.to_string(),
                port,
            }
            .fail()
        }
    }
}
