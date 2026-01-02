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

use std::net::SocketAddr;
use std::time::Duration;

use hickory_resolver::Resolver;
use snafu::prelude::*;
use tokio::net::TcpStream;
use tokio::time::timeout;

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

    #[snafu(display("Failed to perform SRV lookup for {name}: {message}"))]
    SrvLookupFailed { name: String, message: String },

    #[snafu(display("Failed to create DNS resolver: {message}"))]
    ResolverCreationFailed { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Result of an SRV record lookup.
#[derive(Debug, Clone)]
pub struct SrvRecord {
    /// The target hostname from the SRV record.
    pub target: String,
    /// The port from the SRV record.
    pub port: u16,
    /// The priority of the SRV record (lower is higher priority).
    pub priority: u16,
    /// The weight for load balancing among records with the same priority.
    pub weight: u16,
}

/// Perform a DNS SRV lookup for the given service name.
///
/// This is useful for discovering services in Kubernetes headless services,
/// where the SRV record returns the individual pod hostnames and ports.
///
/// # Arguments
///
/// * `name` - The DNS name to query for SRV records (e.g., `my-service.default.svc.cluster.local`)
///
/// # Returns
///
/// A vector of `SrvRecord` containing the discovered services.
///
/// # Errors
///
/// Returns an error if the DNS lookup fails.
pub async fn lookup_srv(name: &str) -> Result<Vec<SrvRecord>> {
    let resolver = Resolver::builder_tokio()
        .map_err(|e| Error::ResolverCreationFailed {
            message: e.to_string(),
        })?
        .build();

    let srv_lookup = resolver
        .srv_lookup(name)
        .await
        .map_err(|e| Error::SrvLookupFailed {
            name: name.to_string(),
            message: e.to_string(),
        })?;

    let records: Vec<SrvRecord> = srv_lookup
        .iter()
        .map(|srv| {
            // Remove trailing dot from target hostname
            let target = srv.target().to_string();
            let target = target.trim_end_matches('.').to_string();

            SrvRecord {
                target,
                port: srv.port(),
                priority: srv.priority(),
                weight: srv.weight(),
            }
        })
        .collect();

    Ok(records)
}

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
    let resolver = Resolver::builder_tokio()
        .map_err(|_| Error::UnableToConnect {
            host: host.to_string(),
            port,
        })?
        .build();
    match resolver.lookup_ip(host).await {
        Ok(ips) => {
            for ip in ips.iter() {
                let addr = SocketAddr::new(ip, port);
                match timeout(Duration::from_secs(30), TcpStream::connect(addr)).await {
                    Ok(Ok(stream)) => {
                        drop(stream);
                        return Ok(());
                    }
                    Ok(Err(err)) => {
                        tracing::debug!("Failed to connect to {addr}: {err}");
                    }
                    Err(_) => {
                        tracing::debug!("Failed to connect to {addr}, connection timed out");
                    }
                }
            }

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
