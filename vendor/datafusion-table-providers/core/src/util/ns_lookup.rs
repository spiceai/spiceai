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
