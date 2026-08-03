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

use util::concat_arrays;

use super::{COMMON_MODEL_PARAMETERS_WITH_DEPRECATED, PARAM_WITH_DEPRE_LEN};
use crate::parameters::ParameterSpec;

pub const PARAMETERS: &[ParameterSpec] =
    &concat_arrays::<
        ParameterSpec,
        FILE_PARAM_LEN,
        PARAM_WITH_DEPRE_LEN,
        { FILE_PARAM_LEN + PARAM_WITH_DEPRE_LEN },
    >(FILE_PARAMETERS, COMMON_MODEL_PARAMETERS_WITH_DEPRECATED);

const FILE_PARAM_LEN: usize = 7;

pub(crate) const FILE_PARAMETERS: [ParameterSpec; FILE_PARAM_LEN] = [
    ParameterSpec::runtime("chat_template").description(
        "Customizes the transformation of OpenAI chat messages into a character stream for the model.",
    ),
    ParameterSpec::runtime("trust_pickle").description(
        "Allow loading pickle-based weight files (.pt / .pth / .ckpt / .bin). \
        These formats execute arbitrary code on load and are disabled by default. \
        Set to 'true' only when the model weights come from a fully trusted source.",
    ).one_of_ignore_ascii_case(&["true", "false"]),
    ParameterSpec::runtime("distributed_backend")
        .description("Run the model tensor-parallel across multiple nodes (a Spice enterprise feature; standard builds are single-node only). Set to 'ring' to pool the model over the `nodes` list; omit or 'none' for single-node.")
        .default("none")
        .one_of_ignore_ascii_case(&["none", "ring"]),
    ParameterSpec::runtime("node_rank")
        .description("This node's 0-indexed rank in the distributed `nodes` list. Rank 0 is the head and serves the API; other ranks are compute replicas."),
    ParameterSpec::runtime("nodes")
        .description("Comma-separated, rank-ordered node addresses for distributed inference (e.g. '10.0.0.1,10.0.0.2,10.0.0.3'). Identical on every node; only `node_rank` differs. Two or more nodes. Whether a given model can be split across that many nodes depends on the model and is reported when it loads."),
    ParameterSpec::runtime("context_length")
        .description("Sequence-length budget, in tokens, for a locally served model: it plans cross-device layer placement and sizes the KV cache. Defaults to the engine default (4096) when unset. It does not raise the context the weights were trained for, and larger values need proportionally more KV-cache memory."),
    ParameterSpec::runtime("paged_attention")
        .description("Attention implementation for a locally served model. 'auto' (the default) uses PagedAttention wherever the build supports it, and the engine falls back to dense attention for architectures with no paged kernel, such as the Multi-head Latent Attention GGUFs. 'disabled' forces dense attention with a contiguous KV cache.")
        .default("auto")
        .one_of_ignore_ascii_case(llms::chat::PagedAttentionMode::VALUES),
];
