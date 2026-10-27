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

use std::net::{IpAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::chat::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
};
use async_trait::async_trait;
use llama_cpp::Backend;
use llama_cpp::rpc::{self, Device};
use llms::chat::nsql::SqlGeneration;
use llms::chat::{Chat, DistributedConfig, Error as ChatError, Result};

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

/// How long this node's GPU server has to start accepting connections.
const SERVE_START: Duration = Duration::from_secs(30);

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

/// This node's GPU server: the endpoint it serves at, and whether its thread still runs.
struct Serving {
    endpoint: String,
    running: Arc<AtomicBool>,
}

/// The server of this node's GPUs, one per process: a model that loads again reuses it.
static SERVING: Mutex<Option<Serving>> = Mutex::new(None);

/// Serves this node's GPUs at `endpoint` on a thread of its own, once per process, and returns
/// whether that server still runs. The GPU counts as served only once the server accepts
/// connections; a server that could not start, or has stopped, is not reused, so a later load
/// starts it again. Blocks until the server accepts connections or fails.
fn serve_gpus(name: &str, endpoint: &str) -> Result<Arc<AtomicBool>> {
    let mut serving = SERVING.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(served) = serving.as_ref()
        && served.running.load(Ordering::Acquire)
    {
        return if served.endpoint == endpoint {
            Ok(Arc::clone(&served.running))
        } else {
            Err(failed(format!(
                "Model '{name}' would serve this node's GPU at {endpoint}, but this spiced already serves it at {} for another model. Restart spiced to change the node's address or rank.",
                served.endpoint
            )))
        };
    }
    let cannot = |reason: String| {
        failed(format!(
            "Model '{name}' could not serve this node's GPU at {endpoint}: {reason}"
        ))
    };
    let gpus = Device::local_gpus(Backend::get());
    if gpus.is_empty() {
        return Err(cannot(
            "this node has no GPU llama.cpp can use, so it cannot hold a share of a distributed model"
                .to_string(),
        ));
    }
    // llama.cpp's server fails to bind only inside its own thread; another process on the port
    // is found here, before the load reports the GPU served.
    drop(TcpListener::bind(endpoint).map_err(|e| {
        cannot(format!(
            "{e}. Stop what listens on that port, or give this node another address in `nodes`"
        ))
    })?);
    let running = Arc::new(AtomicBool::new(true));
    let flag = Arc::clone(&running);
    let at = endpoint.to_string();
    thread::Builder::new()
        .name("llama-cpp-rpc".to_string())
        .spawn(move || {
            // Runs for the life of the process; it returns only when it cannot serve.
            if let Err(e) = rpc::serve(Backend::get(), &at, &gpus, None, RPC_THREADS) {
                tracing::error!("{e}");
            }
            flag.store(false, Ordering::Release);
        })
        .map_err(|e| cannot(format!("could not start the thread that serves it: {e}")))?;
    let started = Instant::now();
    loop {
        if !running.load(Ordering::Acquire) {
            return Err(cannot(
                "llama.cpp's RPC server stopped; see the lines it logged before this".to_string(),
            ));
        }
        if listening(endpoint) {
            break;
        }
        if started.elapsed() > SERVE_START {
            return Err(cannot(format!(
                "llama.cpp's RPC server did not accept connections within {}s",
                SERVE_START.as_secs()
            )));
        }
        thread::sleep(Duration::from_millis(100));
    }
    *serving = Some(Serving {
        endpoint: endpoint.to_string(),
        running: Arc::clone(&running),
    });
    Ok(running)
}

/// Refuses to split the model `name` across nodes unless spiced runs with
/// `--allow-insecure-connections`: llama.cpp's RPC connections between the nodes are neither
/// authenticated nor encrypted, so a node's GPU server would serve any host that reaches it.
pub(crate) fn ensure_insecure_allowed(cfg: &DistributedConfig, name: &str) -> Result<()> {
    if cfg.allow_insecure_connections {
        Ok(())
    } else {
        Err(ChatError::InsecureConnectionsNotAllowed {
            model: name.to_string(),
        })
    }
}

/// A rank other than 0 lends its GPU to the head and holds its share of the model there. Its
/// load completes once it serves the GPU, which it does for the life of the process, so the
/// models listed after it load too; it serves no requests itself and refuses them, naming
/// the head.
pub(crate) async fn lend_gpu(cfg: &DistributedConfig, name: &str) -> Result<Share> {
    ensure_insecure_allowed(cfg, name)?;
    let rank = cfg.node_rank;
    let (node, model) = (cfg.nodes[rank].clone(), name.to_string());
    // Resolving a node named by host name can block on DNS.
    let (endpoint, running) = tokio::task::spawn_blocking(move || {
        let endpoint = format!("{}:{}", ipv4(&node)?, port(rank)?);
        let running = serve_gpus(&model, &endpoint)?;
        Ok::<_, ChatError>((endpoint, running))
    })
    .await
    .map_err(|e| {
        failed(format!(
            "Model '{name}' could not serve this node's GPU: {e}"
        ))
    })??;
    let head = host(&cfg.nodes[0]);
    tracing::info!(
        "Model '{name}': this node is rank {rank} of {} and holds its share of the model for the head at '{head}', which serves the model's requests; this node serves its GPU at {endpoint}.",
        cfg.world_size(),
    );
    Ok(Share {
        refusal: served_by_head(name, head),
        stopped: format!(
            "Model '{name}' no longer holds its share on this node: the server of this node's GPU at {endpoint} stopped. Restart spiced on this node."
        ),
        running,
    })
}

/// What a request to a rank other than 0 is answered with.
fn served_by_head(name: &str, head: &str) -> String {
    format!(
        "Model '{name}' is served by the first machine in 'nodes' ({head}); this machine holds part of it."
    )
}

/// A model on a rank other than 0: this node's GPU holds a share of it for the head, which
/// serves every request.
pub(crate) struct Share {
    refusal: String,
    /// The health check's error once the GPU server has stopped.
    stopped: String,
    /// Whether this node's GPU server still runs.
    running: Arc<AtomicBool>,
}

impl Share {
    fn refuse(&self) -> OpenAIError {
        OpenAIError::ApiError(ApiError {
            message: self.refusal.clone(),
            r#type: Some("invalid_request_error".to_string()),
            param: None,
            code: None,
        })
    }
}

#[async_trait]
impl Chat for Share {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    /// Healthy while this node's GPU server runs.
    async fn health(&self) -> Result<()> {
        if self.running.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(ChatError::HealthCheckError {
                source: self.stopped.clone().into(),
            })
        }
    }

    async fn chat_stream(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> std::result::Result<ChatCompletionResponseStream, OpenAIError> {
        Err(self.refuse())
    }

    async fn chat_request(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> std::result::Result<CreateChatCompletionResponse, OpenAIError> {
        Err(self.refuse())
    }
}

/// The address the head serves and reaches its own GPU at: its own entry in `nodes`, as every
/// rank serves at its own, so no GPU server listens on an interface `nodes` does not name.
/// Between nodes on RDMA that address belongs to the RDMA NIC, so the head's own GPU is driven
/// over the NIC's loopback like every other rank's GPU instead of over TCP on 127.0.0.1, which
/// on DGX Sparks decoded GLM-5.3 3% slower.
fn head_gpu_address(cfg: &DistributedConfig) -> Result<String> {
    ipv4(&cfg.nodes[0])
}

/// The head serves its own GPU at its own `nodes` address, waits for every other rank
/// to serve theirs, and returns all of them in rank order, for the model to be split across.
pub(crate) async fn reach_ranks(cfg: &DistributedConfig, name: &str) -> Result<Vec<Device>> {
    ensure_insecure_allowed(cfg, name)?;
    let world = cfg.world_size();
    let head_gpu = {
        let (cfg, model) = (cfg.clone(), name.to_string());
        // Finding the head's address and starting its GPU server both block.
        tokio::task::spawn_blocking(move || {
            let head_gpu = format!("{}:{}", head_gpu_address(&cfg)?, port(0)?);
            serve_gpus(&model, &head_gpu)?;
            Ok::<_, ChatError>(head_gpu)
        })
        .await
        .map_err(|e| {
            failed(format!(
                "Model '{name}' could not serve this node's GPU: {e}"
            ))
        })??
    };
    configure_comm(cfg);

    let mut endpoints = Vec::with_capacity(world);
    endpoints.push(head_gpu);
    for (rank, node) in cfg.nodes.iter().enumerate().skip(1) {
        endpoints.push(format!("{}:{}", host(node), port(rank)?));
    }

    let started = Instant::now();
    let mut noted = Instant::now();
    let mut devices: Vec<Option<Vec<Device>>> = vec![None; world];
    // Each rank's connection attempt runs on a blocking thread, and the loop never waits on one:
    // llama.cpp's handshake has no deadline, so a port that accepts and never answers would
    // otherwise hold the loop past `RANK_WAIT`. A rank gets a new attempt only once its last one
    // has ended.
    let mut attempts: Vec<Option<tokio::task::JoinHandle<Option<Vec<Device>>>>> =
        (0..world).map(|_| None).collect();
    loop {
        for (rank, endpoint) in endpoints.iter().enumerate() {
            if devices[rank].is_some() {
                continue;
            }
            match attempts[rank].take() {
                Some(attempt) if attempt.is_finished() => {
                    devices[rank] = attempt.await.ok().flatten();
                }
                Some(attempt) => attempts[rank] = Some(attempt),
                None => {
                    let at = endpoint.clone();
                    attempts[rank] = Some(tokio::task::spawn_blocking(move || {
                        listening(&at)
                            .then(|| rpc::connect(Backend::get(), &at).ok())
                            .flatten()
                    }));
                }
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
    use super::{head_gpu_address, host, served_by_head};
    use llms::chat::{DistributedBackend, DistributedConfig};

    #[test]
    fn the_head_serves_its_gpu_only_at_its_own_nodes_address() {
        // rank 1 at a documentation address, which this node reaches through another interface
        let cfg = DistributedConfig {
            backend: DistributedBackend::Ring,
            node_rank: 0,
            nodes: vec!["127.0.0.1".to_string(), "192.0.2.1".to_string()],
            allow_insecure_connections: true,
        };
        assert_eq!(
            head_gpu_address(&cfg).expect("127.0.0.1 is an IPv4 address"),
            "127.0.0.1"
        );
    }

    #[test]
    fn a_request_to_another_rank_names_the_node_that_serves_the_model() {
        assert_eq!(
            served_by_head("deepseek-v4.1-flash", "10.42.9.100"),
            "Model 'deepseek-v4.1-flash' is served by the first machine in 'nodes' (10.42.9.100); this machine holds part of it."
        );
    }

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
