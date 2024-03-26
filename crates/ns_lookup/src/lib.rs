use std::net::{SocketAddr, TcpStream};

use snafu::prelude::*;
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    AsyncResolver,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to {host}:{port}, are the host and port correct?"))]
    UnableToConnect { host: String, port: u16 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Test NS lookup and TCP connect of the provided `host` and `port`.
///
/// # Arguments
///
/// * `host` - The host to lookup.
/// * `port` - The port to connect to.
///
/// # Errors
///
/// Returns an `Error` if the NS lookup or TCP connect fails.
pub async fn test_ns_lookup_and_tcp_connect(host: &str, port: u16) -> Result<()> {
    let resolver = AsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
    match resolver.lookup_ip(host).await {
        Ok(ips) => {
            for ip in ips.iter() {
                let addr = SocketAddr::new(ip, port);
                if TcpStream::connect(addr).is_ok() {
                    return Ok(());
                }
            }

            UnableToConnectSnafu {
                host: host.to_string(),
                port,
            }
            .fail()
        }
        Err(_) => UnableToConnectSnafu {
            host: host.to_string(),
            port,
        }
        .fail(),
    }
}
