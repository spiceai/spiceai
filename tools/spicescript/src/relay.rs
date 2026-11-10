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

//! A TCP relay that makes a host-network container reachable behind a
//! rootless Docker daemon.
//!
//! A rootless daemon's `host` network is its own namespace, not this
//! machine's, and `docker run -p` does nothing for a container on it. So a
//! container executor there cannot be reached at the ports it serves. The
//! daemon does publish a bridge-network container's ports, on this machine's
//! loopback and on its namespace's loopback alike, so a relay container on
//! the bridge network publishes the executor's ports and passes each
//! connection on to the executor, which listens on the bridge's gateway
//! address in the daemon's namespace. The relay is this binary, run in a
//! container with [`ARG`] as its first argument, so it needs no image of its
//! own.

use std::net::{IpAddr, SocketAddr};

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};

/// The first argument that runs this binary as a relay rather than a script
/// runner: `spicescript __relay <to> <port>...`.
pub const ARG: &str = "__relay";

/// The relay's arguments after [`ARG`]: the address to pass connections on to,
/// then the ports, each listened on and relayed to the same port there.
pub fn relay_args(to: IpAddr, ports: &[u16]) -> Vec<String> {
    std::iter::once(ARG.to_string())
        .chain(std::iter::once(to.to_string()))
        .chain(ports.iter().map(ToString::to_string))
        .collect()
}

/// Runs the relay `args` (after [`ARG`]) describe until it is killed.
pub async fn run(args: &[String]) -> Result<()> {
    let (to, ports) = args
        .split_first()
        .context("usage: spicescript __relay <to> <port>...")?;
    let to: IpAddr = to
        .parse()
        .with_context(|| format!("'{to}' is not an IP address"))?;
    let mut listeners = Vec::new();
    for port in ports {
        let port: u16 = port
            .parse()
            .with_context(|| format!("'{port}' is not a port"))?;
        let listener = TcpListener::bind(("0.0.0.0", port))
            .await
            .with_context(|| format!("Failed to listen on port {port}"))?;
        listeners.push(tokio::spawn(serve(listener, SocketAddr::new(to, port))));
    }
    anyhow::ensure!(!listeners.is_empty(), "the relay has no ports to relay");
    for listener in listeners {
        listener.await??;
    }
    Ok(())
}

/// Passes every connection `listener` accepts on to `to`.
async fn serve(listener: TcpListener, to: SocketAddr) -> Result<()> {
    loop {
        let (mut inbound, _) = listener.accept().await?;
        tokio::spawn(async move {
            // A connection refused here (the executor is not up yet, or was
            // killed) closes the inbound one, which its client sees as the
            // executor being down.
            if let Ok(mut outbound) = TcpStream::connect(to).await {
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn passes_a_connection_on_to_the_target_both_ways() {
        // The target: an echo server that prefixes what it sends back.
        let target = TcpListener::bind("127.0.0.1:0").await.expect("bind target");
        let to = target.local_addr().expect("target address");
        tokio::spawn(async move {
            let (mut stream, _) = target.accept().await.expect("accept");
            let mut buf = [0; 5];
            stream.read_exact(&mut buf).await.expect("read");
            stream.write_all(b"echo:").await.expect("write");
            stream.write_all(&buf).await.expect("write");
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind relay");
        let relay = listener.local_addr().expect("relay address");
        tokio::spawn(serve(listener, to));
        let mut client = TcpStream::connect(relay).await.expect("connect");
        client.write_all(b"hello").await.expect("send");
        let mut reply = Vec::new();
        client.read_to_end(&mut reply).await.expect("reply");
        assert_eq!(reply, b"echo:hello");
    }

    #[test]
    fn its_arguments_name_the_target_then_the_ports() {
        let to: IpAddr = "172.17.0.1".parse().expect("address");
        assert_eq!(
            relay_args(to, &[41001, 41002]),
            ["__relay", "172.17.0.1", "41001", "41002"]
        );
    }
}
