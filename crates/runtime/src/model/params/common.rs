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

//! Parameters common to every model provider, expressed as
//! [`PassthroughParam`] tables consumed by the per-source `#[derive(TypedParams)]`
//! structs via `#[params(passthrough = ...)]`.
//!
//! These are not bound to typed fields: the runtime tunables
//! (`tools`, `system_prompt`, `max_concurrency`, …) and the OpenAI-compatible
//! chat-completion overrides (`temperature`, `top_p`, …) are read from the raw
//! params map elsewhere (`get_openai_request_overrides`, `rate_limit`, the
//! `extract_secret!` reads). Their presence here exists to suppress unknown-key
//! warnings, emit deprecation warnings for the legacy `openai_`-prefixed forms,
//! and feed schema generation — the same roles the old `COMMON_MODEL_PARAMETERS`
//! `ParameterSpec` lists played.

use runtime_parameters_typed::PassthroughParam;

/// Deprecation note for the legacy `openai_<param>` override forms accepted by
/// non-OpenAI providers.
pub const DEPRECATED_MESSAGE: &str = "The `openai_<param>` language model overrides parameter is deprecated and will be removed in a future release. Please use `<model_prefix>_<param>` parameter name instead.";

/// Runtime tunables every provider accepts (unprefixed), shared by both tables.
const RUNTIME_TUNABLES: [PassthroughParam; 6] = [
    PassthroughParam::runtime("tools").description("Which tools should be made available to the model. Set to 'auto' to automatically choose between direct tools and searchable discovery without data sampling tools, 'all' to use built-in and Spicepod-configured tools directly, or 'search_registry' to require searchable tool discovery."),
    PassthroughParam::runtime("tool_embedding_model").description("Embedding model name to use for searchable tool discovery. tools: search_registry requires a model configured in the embeddings section and uses it when only one embedding model is configured; tools: auto falls back to direct tools if embeddings are unavailable."),
    PassthroughParam::runtime("system_prompt").description("An additional system prompt used for all chat completions to this model."),
    PassthroughParam::runtime("parameterized_prompt"),
    PassthroughParam::runtime("max_concurrency").description("Maximum number of concurrent requests for this model. Overrides provider defaults."),
    PassthroughParam::runtime("requests_per_minute_limit").description("Maximum requests per minute for this model. Overrides provider defaults."),
];

/// OpenAI-compatible chat-completion override params, in canonical order. The
/// tables below render these in each provider's accepted forms.
const OVERRIDES: [&str; 22] = [
    "frequency_penalty",
    "logit_bias",
    "logprobs",
    "top_logprobs",
    "max_completion_tokens",
    "reasoning_effort",
    "store",
    "metadata",
    "n",
    "presence_penalty",
    "response_format",
    "seed",
    "stop",
    "stream",
    "stream_options",
    "temperature",
    "top_p",
    "tool_choice",
    "parallel_tool_calls",
    "prompt_cache_key",
    "prompt_cache_retention",
    "user",
];

/// Common params for the `OpenAI` provider: the runtime tunables, the override
/// params accepted unprefixed (runtime), and the `openai_`-prefixed forms
/// (`openai_temperature`, …) kept as deprecated aliases for back-compat.
pub const OPENAI_COMMON: &[PassthroughParam] = &openai_common();

/// Common params for every non-OpenAI provider: the runtime tunables, the
/// override params accepted `{prefix}_`-prefixed (component), and the literal
/// `openai_`-prefixed forms kept as deprecated aliases.
pub const PREFIXED_COMMON: &[PassthroughParam] = &prefixed_common();

// OpenAI: 6 runtime tunables + 22 runtime overrides + (`openai_tools` + 22
// `openai_*` overrides) deprecated component forms = 51 (legacy PARAM_LEN).
const OPENAI_LEN: usize = RUNTIME_TUNABLES.len() + OVERRIDES.len() + (1 + OVERRIDES.len());
// Prefixed: 6 runtime tunables + (`tools` + 22 overrides) component forms +
// (`openai_tools` + 22 `openai_*` overrides) deprecated runtime forms = 52
// (legacy PARAM_WITH_DEPRE_LEN).
const PREFIXED_LEN: usize = RUNTIME_TUNABLES.len() + (1 + OVERRIDES.len()) + (1 + OVERRIDES.len());

const fn openai_common() -> [PassthroughParam; OPENAI_LEN] {
    let mut out = [PassthroughParam::runtime(""); OPENAI_LEN];
    let mut i = 0;

    // Runtime tunables.
    let mut j = 0;
    while j < RUNTIME_TUNABLES.len() {
        out[i] = RUNTIME_TUNABLES[j];
        i += 1;
        j += 1;
    }

    // Override params accepted unprefixed.
    j = 0;
    while j < OVERRIDES.len() {
        out[i] = PassthroughParam::runtime(OVERRIDES[j]);
        i += 1;
        j += 1;
    }

    // Deprecated `openai_`-prefixed override forms (component-scoped under the
    // `openai` prefix), plus the deprecated `tools` component form. The note
    // names the unprefixed key, matching the legacy specs.
    out[i] = PassthroughParam::component("tools").deprecated("Use 'tools' without prefix");
    i += 1;
    j = 0;
    while j < OVERRIDES.len() {
        out[i] = PassthroughParam::component(OVERRIDES[j]).deprecated(WITHOUT_PREFIX_NOTES[j]);
        i += 1;
        j += 1;
    }

    out
}

const fn prefixed_common() -> [PassthroughParam; PREFIXED_LEN] {
    let mut out = [PassthroughParam::runtime(""); PREFIXED_LEN];
    let mut i = 0;

    // Runtime tunables.
    let mut j = 0;
    while j < RUNTIME_TUNABLES.len() {
        out[i] = RUNTIME_TUNABLES[j];
        i += 1;
        j += 1;
    }

    // Override params accepted `{prefix}_`-prefixed (component), plus `tools`.
    out[i] = PassthroughParam::component("tools");
    i += 1;
    j = 0;
    while j < OVERRIDES.len() {
        out[i] = PassthroughParam::component(OVERRIDES[j]);
        i += 1;
        j += 1;
    }

    // Deprecated literal `openai_`-prefixed forms (runtime, so the `openai_`
    // name is not re-prefixed by the provider's own prefix), including
    // `openai_tools`.
    out[i] = PassthroughParam::runtime("openai_tools").deprecated(DEPRECATED_MESSAGE);
    i += 1;
    j = 0;
    while j < OVERRIDES.len() {
        out[i] = PassthroughParam::runtime(OPENAI_PREFIXED[j]).deprecated(DEPRECATED_MESSAGE);
        i += 1;
        j += 1;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_lengths_match_legacy_specs() {
        // Parity with the retired COMMON_MODEL_PARAMETERS (51) and
        // COMMON_MODEL_PARAMETERS_WITH_DEPRECATED (52) arrays.
        assert_eq!(OPENAI_COMMON.len(), 51);
        assert_eq!(PREFIXED_COMMON.len(), 52);
    }

    fn user_keys(table: &[PassthroughParam], prefix: &str) -> Vec<(String, bool)> {
        table
            .iter()
            .map(|p| (p.user_key(prefix), p.deprecated.is_some()))
            .collect()
    }

    #[test]
    fn openai_accepts_unprefixed_and_deprecated_prefixed_overrides() {
        let keys = user_keys(OPENAI_COMMON, "openai");
        assert!(keys.contains(&("temperature".to_string(), false)));
        assert!(keys.contains(&("openai_temperature".to_string(), true)));
        assert!(keys.contains(&("tools".to_string(), false)));
        assert!(keys.contains(&("openai_tools".to_string(), true)));
    }

    #[test]
    fn prefixed_accepts_prefixed_and_deprecated_openai_overrides() {
        let keys = user_keys(PREFIXED_COMMON, "hf");
        assert!(keys.contains(&("hf_temperature".to_string(), false)));
        assert!(keys.contains(&("openai_temperature".to_string(), true)));
        assert!(keys.contains(&("hf_tools".to_string(), false)));
        assert!(keys.contains(&("openai_tools".to_string(), true)));
        // Runtime tunables stay unprefixed.
        assert!(keys.contains(&("system_prompt".to_string(), false)));
    }
}

/// `openai_`-prefixed literal names, index-aligned with [`OVERRIDES`].
const OPENAI_PREFIXED: [&str; 22] = [
    "openai_frequency_penalty",
    "openai_logit_bias",
    "openai_logprobs",
    "openai_top_logprobs",
    "openai_max_completion_tokens",
    "openai_reasoning_effort",
    "openai_store",
    "openai_metadata",
    "openai_n",
    "openai_presence_penalty",
    "openai_response_format",
    "openai_seed",
    "openai_stop",
    "openai_stream",
    "openai_stream_options",
    "openai_temperature",
    "openai_top_p",
    "openai_tool_choice",
    "openai_parallel_tool_calls",
    "openai_prompt_cache_key",
    "openai_prompt_cache_retention",
    "openai_user",
];

/// `Use '<name>' without prefix` notes, index-aligned with [`OVERRIDES`].
const WITHOUT_PREFIX_NOTES: [&str; 22] = [
    "Use 'frequency_penalty' without prefix",
    "Use 'logit_bias' without prefix",
    "Use 'logprobs' without prefix",
    "Use 'top_logprobs' without prefix",
    "Use 'max_completion_tokens' without prefix",
    "Use 'reasoning_effort' without prefix",
    "Use 'store' without prefix",
    "Use 'metadata' without prefix",
    "Use 'n' without prefix",
    "Use 'presence_penalty' without prefix",
    "Use 'response_format' without prefix",
    "Use 'seed' without prefix",
    "Use 'stop' without prefix",
    "Use 'stream' without prefix",
    "Use 'stream_options' without prefix",
    "Use 'temperature' without prefix",
    "Use 'top_p' without prefix",
    "Use 'tool_choice' without prefix",
    "Use 'parallel_tool_calls' without prefix",
    "Use 'prompt_cache_key' without prefix",
    "Use 'prompt_cache_retention' without prefix",
    "Use 'user' without prefix",
];
