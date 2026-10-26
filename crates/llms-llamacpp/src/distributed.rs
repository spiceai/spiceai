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

//! A model split tensor-parallel across the nodes of a `distributed_backend` topology. Every
//! node serves its GPU through llama.cpp's RPC backend; rank 0, the head, also serves the API:
//! it reaches every node's GPU, its own included, and splits each weight across them. The
//! nodes add up their partial results over direct connections between every pair of them.

use std::net::{IpAddr, TcpStream, ToSocketAddrs};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use llama_cpp::Backend;
use llama_cpp::rpc::{self, Device};
use llms::chat::{DistributedConfig, Error as ChatError, Result};

/// Rank `r` serves its GPU on this port plus `r`; llama.cpp's pairwise connections between the
/// ranks use the ports 1000 above.
pub(crate) const RPC_PORT_BASE: u16 = 12345;

/// How long the head waits for every other rank to serve its GPU.
const RANK_WAIT: Duration = Duration::from_secs(600);

/// How often the head says which ranks it is still waiting for.
const RANK_WAIT_NOTE: Duration = Duration::from_secs(30);

/// Threads llama.cpp's RPC server uses for work it runs on the CPU.
const RPC_THREADS: usize = 4;

/// How long one probe of a rank's port waits for the connection.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Whether something accepts connections at `endpoint`. llama.cpp's RPC client aborts the
/// process when it cannot connect, so a rank is reached only once this says it listens.
fn listening(endpoint: &str) -> bool {
    endpoint.to_socket_addrs().is_ok_and(|mut addrs| {
        addrs.any(|addr| TcpStream::connect_timeout(&addr, PROBE_TIMEOUT).is_ok())
    })
}

fn port(rank: usize) -> Result<u16> {
    u16::try_from(rank)
        .ok()
        .and_then(|rank| RPC_PORT_BASE.checked_add(rank))
        .ok_or_else(|| failed(format!("node_rank {rank} is too large for a port")))
}

fn failed(message: String) -> ChatError {
    ChatError::FailedToLoadModel {
        source: message.into(),
    }
}

/// The host part of a `nodes` entry, which may carry a port.
fn host(node: &str) -> &str {
    let node = node.trim();
    match node.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') && port.parse::<u16>().is_ok() => host,
        _ => node,
    }
}

/// The IPv4 address a node listens on: llama.cpp binds its RPC server to an address, not a name.
fn ipv4(node: &str) -> Result<String> {
    let host = host(node);
    if let Ok(IpAddr::V4(addr)) = host.parse::<IpAddr>() {
        return Ok(addr.to_string());
    }
    (host, 0)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.find(std::net::SocketAddr::is_ipv4))
        .map(|addr| addr.ip().to_string())
        .ok_or_else(|| {
            failed(format!(
                "'{host}' in `nodes` does not resolve to an IPv4 address on this node. List each node by an IPv4 address or a name this node resolves to one."
            ))
        })
}

/// This node's GPUs, served once per process: a model that loads again reuses the server.
static SERVING: OnceLock<std::result::Result<String, String>> = OnceLock::new();

/// Serves this node's GPUs at `endpoint` on a thread of its own, once per process.
fn serve_gpus(name: &str, endpoint: &str) -> Result<()> {
    let served = SERVING.get_or_init(|| {
        let backend = Backend::get();
        let gpus = Device::local_gpus(backend);
        if gpus.is_empty() {
            return Err(
                "this node has no GPU llama.cpp can use, so it cannot hold a share of a distributed model"
                    .to_string(),
            );
        }
        let at = endpoint.to_string();
        thread::Builder::new()
            .name("llama-cpp-rpc".to_string())
            .spawn(move || {
                // Runs for the life of the process; it returns only when it cannot serve.
                if let Err(e) = rpc::serve(Backend::get(), &at, &gpus, None, RPC_THREADS) {
                    tracing::error!("{e}");
                }
            })
            .map_err(|e| format!("could not start the thread that serves this node's GPU: {e}"))?;
        Ok(endpoint.to_string())
    });
    match served {
        Ok(at) if *at == endpoint => Ok(()),
        Ok(at) => Err(failed(format!(
            "Model '{name}' would serve this node's GPU at {endpoint}, but this spiced already serves it at {at} for another model. Restart spiced to change the node's address or rank."
        ))),
        Err(reason) => Err(failed(format!(
            "Model '{name}' could not serve this node's GPU: {reason}"
        ))),
    }
}

/// A rank other than 0 lends its GPU to the head and holds its share of the model there; it
/// serves no requests itself. The load does not complete: the GPU is served for the life of
/// the process, and the call returns only when that cannot start.
pub(crate) async fn lend_gpu(
    cfg: &DistributedConfig,
    name: &str,
) -> Result<std::convert::Infallible> {
    let rank = cfg.node_rank;
    let endpoint = format!("{}:{}", ipv4(&cfg.nodes[rank])?, port(rank)?);
    serve_gpus(name, &endpoint)?;
    tracing::info!(
        "Model '{name}': this node is rank {rank} of {} and holds its share of the model for the head at '{}', which serves the model's requests; this node serves its GPU at {endpoint}.",
        cfg.world_size(),
        host(&cfg.nodes[0])
    );
    // The share stays loaded while the process runs.
    Ok(std::future::pending::<std::convert::Infallible>().await)
}

/// The address the head serves and reaches its own GPU at: the one this node sends from to
/// reach rank 1. Between nodes on RDMA that address belongs to the RDMA NIC, so the head's
/// own GPU is driven over the NIC's loopback like every other rank's GPU instead of over TCP
/// on 127.0.0.1, which on DGX Sparks decoded GLM-5.3 3% slower. Loopback when this node has no
/// route to rank 1 or reaches it from a loopback address.
fn head_gpu_address(cfg: &DistributedConfig) -> String {
    cfg.nodes
        .get(1)
        .and_then(|node| ipv4(node).ok())
        .and_then(|peer| source_toward(&peer))
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

/// The local IPv4 address this node's routes send from to reach `peer`. A UDP socket's
/// connect only resolves the route; nothing is sent.
fn source_toward(peer: &str) -> Option<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect((peer, RPC_PORT_BASE)).ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(addr) if !addr.is_loopback() && !addr.is_unspecified() => Some(addr.to_string()),
        _ => None,
    }
}

/// The head serves its own GPU at the address it reaches rank 1 from, waits for every other rank
/// to serve theirs, and returns all of them in rank order, for the model to be split across.
pub(crate) async fn reach_ranks(cfg: &DistributedConfig, name: &str) -> Result<Vec<Device>> {
    let world = cfg.world_size();
    let head_gpu = format!("{}:{}", head_gpu_address(cfg), port(0)?);
    serve_gpus(name, &head_gpu)?;
    configure_comm(cfg);

    let mut endpoints = Vec::with_capacity(world);
    endpoints.push(head_gpu);
    for (rank, node) in cfg.nodes.iter().enumerate().skip(1) {
        endpoints.push(format!("{}:{}", host(node), port(rank)?));
    }

    let started = Instant::now();
    let mut noted = Instant::now();
    let mut devices: Vec<Option<Vec<Device>>> = vec![None; world];
    loop {
        for (rank, endpoint) in endpoints.iter().enumerate() {
            if devices[rank].is_none() {
                let at = endpoint.clone();
                let reached = tokio::task::spawn_blocking(move || {
                    listening(&at)
                        .then(|| rpc::connect(Backend::get(), &at).ok())
                        .flatten()
                })
                .await
                .ok()
                .flatten();
                devices[rank] = reached;
            }
        }
        let missing: Vec<usize> = (0..world).filter(|&rank| devices[rank].is_none()).collect();
        if missing.is_empty() {
            break;
        }
        let waiting = missing
            .iter()
            .map(|&rank| format!("rank {rank} at {}", endpoints[rank]))
            .collect::<Vec<_>>()
            .join(", ");
        if started.elapsed() > RANK_WAIT {
            return Err(failed(format!(
                "Model '{name}' waited {}s for every node to serve its GPU, and {waiting} never did. Start spiced with the same spicepod and that node's `node_rank` on each node, and check the head reaches them on port {} and up.",
                RANK_WAIT.as_secs(),
                RPC_PORT_BASE
            )));
        }
        if noted.elapsed() > RANK_WAIT_NOTE {
            tracing::info!(
                "Model '{name}' is waiting for {waiting} to serve its GPU before it loads; start spiced with that `node_rank` there."
            );
            noted = Instant::now();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let devices: Vec<Device> = devices.into_iter().flatten().flatten().collect();
    tracing::info!(
        "Model '{name}' is split across {} GPUs on {world} nodes: {}.",
        devices.len(),
        devices
            .iter()
            .map(Device::description)
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(devices)
}

/// Where the ranks reach each other for the all-reduce: the split's device `i` first reaches device
/// `j` at the node `nodes` lists for it, and llama.cpp moves the pair to the fastest link the two
/// share. Device `d` is rank `d`. Every rank sums the partials in the same order in F32, so all
/// hold the same sum. A setting already in the environment wins.
fn configure_comm(cfg: &DistributedConfig) {
    let set = |key: &str, value: String| {
        if std::env::var_os(key).is_none() {
            // SAFETY: called during model init, before llama.cpp reads it, as the ring's
            // `RING_CONFIG` is set.
            unsafe { std::env::set_var(key, value) };
        }
    };
    set("GGML_RPC_COMM_MESH", "1".to_string());
    let mut hosts = Vec::new();
    for i in 1..cfg.world_size() {
        for j in 0..i {
            hosts.push(format!("{i}:{j}={}", host(&cfg.nodes[j])));
        }
    }
    set("GGML_RPC_COMM_HOSTS", hosts.join(","));
}

#[cfg(test)]
mod tests {
    use super::host;

    #[test]
    fn a_node_entry_may_carry_a_port() {
        assert_eq!(host("10.42.9.100"), "10.42.9.100");
        assert_eq!(host(" 10.42.9.100:8090 "), "10.42.9.100");
        assert_eq!(host("spark-a"), "spark-a");
        assert_eq!(host("spark-a:8090"), "spark-a");
        // an IPv6 literal is not split at its colons
        assert_eq!(host("fe80::1"), "fe80::1");
    }
}
