/*
Copyright 2026 The Spice.ai OSS Authors
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

use snafu::ResultExt;

use crate::chat::{Error as ChatError, FailedToLoadModelSnafu};

/// Base TCP port for the ring transport. Rank `r` binds `RING_PORT_BASE + r` to
/// accept its left neighbour; its right neighbour is rank `(r + 1) % world_size`.
const RING_PORT_BASE: u16 = 12345;
/// Port for the ring's request-replication channel (rank 0 listens; others dial).
const RING_MASTER_PORT: u16 = 12344;

/// mistral.rs `RingConfig` as serialized to the `RING_CONFIG` JSON file. The
/// field names/shape must match `mistralrs_quant::distributed::RingConfig`.
#[derive(serde::Serialize)]
struct RingConfigFile {
    #[serde(skip_serializing_if = "Option::is_none")]
    master_ip: Option<String>,
    master_port: u16,
    port: u16,
    right_port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    right_ip: Option<String>,
    rank: usize,
    world_size: usize,
}

/// Backend used for multi-node distributed (tensor-parallel) inference of a local model.
/// Currently only mistral.rs's pure-TCP `ring` all-reduce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributedBackend {
    /// mistral.rs ring all-reduce over plain TCP. No system dependency. The reduction
    /// rotates the ring once per peer, so it is correct at any world size >= 2.
    Ring,
}

/// Topology for running one local model across multiple nodes (tensor-parallel).
///
/// The same `nodes` list is given to every node; only `node_rank` differs.
/// Spice derives the per-node transport wiring (ports, ring neighbour, master
/// address) from this. Rank 0 is the head and serves the API; the other ranks
/// run as compute replicas.
#[derive(Debug, Clone)]
pub struct DistributedConfig {
    pub backend: DistributedBackend,
    /// This node's rank in `[0, world_size)`. Rank 0 is the head/server.
    pub node_rank: usize,
    /// Ordered node addresses (host or IP); index == rank, length == world size.
    pub nodes: Vec<String>,
}

impl DistributedConfig {
    /// Number of nodes participating (the tensor-parallel world size).
    #[must_use]
    pub fn world_size(&self) -> usize {
        self.nodes.len()
    }

    /// Validate the topology. On failure, returns the offending param name
    /// (`"nodes"` or `"node_rank"`) alongside a human-readable message, so the
    /// caller can attribute the error to the field the user actually set wrong.
    ///
    /// Any world size of 2 or more is accepted. These two rules mirror the engine's own
    /// (`RingComm::from_device`), and must stay a *subset* of them: Spice rejecting a
    /// topology the engine would happily run is the failure mode to avoid, whereas being
    /// looser only costs a worse error message from deeper in the load. Whether a given
    /// model can be split this many ways is the engine's call: loaders differ in whether
    /// they shard heads and experts evenly, and it reports the specifics when the model
    /// loads.
    pub fn validate(&self) -> std::result::Result<(), (&'static str, String)> {
        let world_size = self.world_size();
        if world_size < 2 {
            return Err((
                "nodes",
                format!("distributed inference needs at least 2 nodes; `nodes` lists {world_size}"),
            ));
        }
        if self.node_rank >= world_size {
            return Err((
                "node_rank",
                format!(
                    "`node_rank` {} is out of range for world size {world_size} (valid: 0..{world_size})",
                    self.node_rank
                ),
            ));
        }
        Ok(())
    }
}

/// Translate a [`DistributedConfig`] into a per-node mistral.rs ring topology,
/// write it to a temp `RING_CONFIG` file, and point the `RING_CONFIG` env var at
/// it. mistral.rs reads that env var while building the pipeline (there is no
/// builder API for rank/world size), so this MUST run before `load_model_from_hf`.
/// Returns the temp-file guard, which must outlive the model.
pub(crate) fn configure_ring_distributed(
    cfg: &DistributedConfig,
) -> Result<tempfile::TempPath, ChatError> {
    match cfg.backend {
        DistributedBackend::Ring => {}
    }

    if !cfg!(feature = "distributed") {
        return Err(ChatError::FailedToLoadModel {
            source: "`distributed_backend: ring` was requested, but this build does not include multi-node distributed inference — a Spice enterprise feature (build with the `distributed` Cargo feature to enable it).".into(),
        });
    }

    if let Err((param, message)) = cfg.validate() {
        return Err(ChatError::InvalidParamValueError {
            param: param.to_string(),
            message,
        });
    }

    let world_size = cfg.world_size();
    let rank = cfg.node_rank;
    let right = (rank + 1) % world_size;

    let ring = RingConfigFile {
        // Rank 0 listens for the replication channel on all interfaces (the
        // mistral.rs default of 0.0.0.0); other ranks must dial the head.
        master_ip: if rank == 0 {
            None
        } else {
            Some(cfg.nodes[0].trim().to_string())
        },
        master_port: RING_MASTER_PORT,
        port: ring_port(rank)?,
        right_port: ring_port(right)?,
        right_ip: Some(cfg.nodes[right].trim().to_string()),
        rank,
        world_size,
    };

    let json = serde_json::to_string_pretty(&ring)
        .boxed()
        .context(FailedToLoadModelSnafu)?;

    let mut file = tempfile::Builder::new()
        .prefix("spice-ring-")
        .suffix(".json")
        .tempfile()
        .boxed()
        .context(FailedToLoadModelSnafu)?;
    std::io::Write::write_all(&mut file, json.as_bytes())
        .boxed()
        .context(FailedToLoadModelSnafu)?;
    let path = file.into_temp_path();

    // SAFETY: set once during model initialization, before mistral.rs reads
    // `RING_CONFIG` while constructing the pipeline. Loading is not concurrent
    // with other environment access here, so no other thread races this write.
    unsafe {
        std::env::set_var("RING_CONFIG", path.as_os_str());
    }

    tracing::info!(
        rank,
        world_size,
        right = %cfg.nodes[right],
        "Configured distributed ring inference"
    );
    if rank != 0 {
        tracing::warn!(
            "This node is rank {rank} (not the head): it runs as a tensor-parallel compute replica, blocking inside model load instead of serving its own API. Send inference requests to rank 0 ({}).",
            cfg.nodes[0]
        );
    }

    Ok(path)
}

/// Map a ring rank to its TCP port (`RING_PORT_BASE + rank`), erroring rather
/// than silently truncating if it cannot fit in a `u16`.
fn ring_port(rank: usize) -> Result<u16, ChatError> {
    u16::try_from(rank)
        .ok()
        .and_then(|r| RING_PORT_BASE.checked_add(r))
        .ok_or_else(|| ChatError::InvalidParamValueError {
            param: "node_rank".to_string(),
            message: format!(
                "rank {rank} cannot be mapped to a TCP port (base {RING_PORT_BASE} + rank overflows u16)"
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_cfg(node_rank: usize, n: usize) -> DistributedConfig {
        DistributedConfig {
            backend: DistributedBackend::Ring,
            node_rank,
            nodes: (0..n).map(|i| format!("10.0.0.{i}")).collect(),
        }
    }

    #[test]
    fn validate_accepts_two_node_ring() {
        ring_cfg(0, 2)
            .validate()
            .expect("2-node ring rank 0 is valid");
        ring_cfg(1, 2)
            .validate()
            .expect("2-node ring rank 1 is valid");
    }

    #[test]
    fn validate_rejects_fewer_than_two_nodes() {
        assert!(ring_cfg(0, 1).validate().is_err());
    }

    #[test]
    fn validate_accepts_world_sizes_that_are_not_powers_of_two() {
        // Three nodes is the smallest world size that pools a model too large for two,
        // and the smallest that is not a power of two.
        for world_size in [3usize, 5, 6, 7] {
            for rank in 0..world_size {
                ring_cfg(rank, world_size)
                    .validate()
                    .unwrap_or_else(|e| panic!("world size {world_size} rank {rank}: {e:?}"));
            }
        }
    }

    #[test]
    fn validate_rejects_rank_out_of_range() {
        for (rank, world_size) in [(2usize, 2usize), (3, 3)] {
            let err = ring_cfg(rank, world_size)
                .validate()
                .expect_err("a rank equal to the world size is out of range");
            // The error must be attributed to `node_rank`, not `nodes`.
            assert_eq!(err.0, "node_rank", "world size {world_size}");
        }
    }
}
