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

use llms::chat::{DistributedBackendSetting, PagedAttentionMode, PickleTrust};
use runtime_parameters::TypedParams;

/// Parameters for `from: file` (local) chat models.
#[derive(Debug, TypedParams)]
#[params(
    prefix = "file",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct FileModelParams {
    /// Customizes the transformation of `OpenAI` chat messages into a character stream for the model.
    #[param(runtime)]
    pub chat_template: Option<String>,
    /// Allow loading pickle-based weight files (.pt / .pth / .ckpt / .bin). These formats execute arbitrary code on load and are disabled by default. Set to 'true' only when the model weights come from a fully trusted source.
    #[param(runtime, default = "false")]
    pub trust_pickle: PickleTrust,
    /// Run the model tensor-parallel across multiple nodes (a Spice enterprise feature; standard builds are single-node only). Set to 'ring' to pool the model over the `nodes` list using the pure-TCP ring all-reduce, or 'nccl' to use NCCL's on-device collectives; omit or 'none' for single-node. The backend is fixed when the binary is built, so a build accepts only the one it carries.
    #[param(runtime, default = "none")]
    pub distributed_backend: DistributedBackendSetting,
    /// This node's 0-indexed rank in the distributed `nodes` list. Rank 0 is the head and serves the API; other ranks are compute replicas.
    #[param(runtime)]
    pub node_rank: Option<String>,
    /// Comma-separated, rank-ordered node addresses for distributed inference (e.g. '10.0.0.1,10.0.0.2'). Identical on every node; only `node_rank` differs. Any node count of 2 or more is accepted; whether a given model splits that many ways is the engine's call.
    #[param(runtime)]
    pub nodes: Option<String>,
    /// Sequence-length budget, in tokens, for a locally served model: it plans cross-device layer placement and sizes the KV cache. Defaults to the engine default (4096) when unset. It does not raise the context the weights were trained for, and larger values need proportionally more KV-cache memory.
    #[param(runtime)]
    pub context_length: Option<String>,
    /// Attention implementation for a locally served model. 'auto' (the default) uses `PagedAttention` wherever the build supports it, and the engine falls back to dense attention for architectures with no paged kernel, such as the Multi-head Latent Attention GGUFs. 'disabled' forces dense attention with a contiguous KV cache.
    #[param(runtime, default = "auto")]
    pub paged_attention: PagedAttentionMode,
}
