/*
Copyright 2021-2024 The Spice Authors

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
