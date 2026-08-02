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

//! How a locally served model implements attention: `PagedAttention` (a paged KV cache,
//! which lets the scheduler overcommit and share cache blocks) or dense attention with
//! a contiguous cache.

use std::str::FromStr;

/// GGUF architectures the local engine evaluates with dense attention only. Their
/// loaders reject a `PagedAttentionConfig` outright, so `auto` must not hand them one.
///
/// These are Multi-head Latent Attention models: the paged kernels have no MLA path, and
/// the GGUF loader reconstructs full per-head K/V from the compressed latent and caches
/// those instead.
const DENSE_ATTENTION_ONLY_GGUF_ARCHITECTURES: &[&str] = &[
    // GLM-4.x / GLM-5.x MoE (MLA + sigmoid-gated MoE, plus a sparse-attention indexer
    // the loader skips).
    "glm-dsa",
    // DeepSeek-V4 (MLA with an output LoRA, sqrt-softplus routing, hash layers). Listed
    // ahead of its loader so that when the loader lands, `auto` already routes it to
    // dense attention rather than a config the loader would reject.
    "deepseek4",
];

/// Operator-selected attention implementation for a locally served model, from the
/// `paged_attention` model param.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PagedAttentionMode {
    /// Use `PagedAttention` where the build and the model support it, and fall back to
    /// dense attention where they do not.
    #[default]
    Auto,
    /// Never use `PagedAttention`: always serve with dense attention and a contiguous
    /// KV cache. The escape hatch for a model whose paged path misbehaves.
    Disabled,
}

impl PagedAttentionMode {
    /// The values accepted in a Spicepod, for the parameter spec and error messages.
    pub const VALUES: [&'static str; 2] = ["auto", "disabled"];
}

impl FromStr for PagedAttentionMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // An empty value means the operator wrote the key but left it blank; treat that
        // as unset rather than as a typo, matching the other model params.
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "disabled" => Ok(Self::Disabled),
            other => Err(other.to_string()),
        }
    }
}

impl std::fmt::Display for PagedAttentionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Disabled => f.write_str("disabled"),
        }
    }
}

/// Whether a GGUF `general.architecture` value is one the local engine serves with dense
/// attention only. Matching is case-insensitive: GGUF writers are not consistent about case.
#[must_use]
pub fn gguf_requires_dense_attention(architecture: &str) -> bool {
    DENSE_ATTENTION_ONLY_GGUF_ARCHITECTURES
        .iter()
        .any(|known| known.eq_ignore_ascii_case(architecture.trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_documented_values_case_insensitively() {
        for (raw, expected) in [
            ("auto", PagedAttentionMode::Auto),
            ("AUTO", PagedAttentionMode::Auto),
            ("  auto ", PagedAttentionMode::Auto),
            // Blank is unset, not invalid.
            ("", PagedAttentionMode::Auto),
            ("disabled", PagedAttentionMode::Disabled),
            ("Disabled", PagedAttentionMode::Disabled),
        ] {
            assert_eq!(
                raw.parse::<PagedAttentionMode>()
                    .unwrap_or_else(|e| panic!("{raw:?} should parse: {e}")),
                expected,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn rejects_values_outside_the_spec() {
        // Booleans in particular: a Spicepod that spells this `true`/`false` has to fail
        // loudly rather than have one of them quietly read as a mode.
        for raw in ["true", "false", "1", "0", "on", "off", "eager", "maybe"] {
            assert!(
                raw.parse::<PagedAttentionMode>().is_err(),
                "{raw:?} should be rejected"
            );
        }
    }

    #[test]
    fn round_trips_through_display() {
        for mode in [PagedAttentionMode::Auto, PagedAttentionMode::Disabled] {
            assert_eq!(
                mode.to_string()
                    .parse::<PagedAttentionMode>()
                    .expect("Display output is a valid value"),
                mode
            );
        }
        // Every variant is listed in the spec's accepted values.
        for mode in [PagedAttentionMode::Auto, PagedAttentionMode::Disabled] {
            assert!(
                PagedAttentionMode::VALUES.contains(&mode.to_string().as_str()),
                "{mode} missing from VALUES"
            );
        }
    }

    #[test]
    fn recognizes_the_dense_attention_only_architectures() {
        // GLM-4.x/5.x and DeepSeek-V4 GGUFs: MLA models with no paged kernel.
        assert!(gguf_requires_dense_attention("glm-dsa"));
        assert!(gguf_requires_dense_attention("GLM-DSA"));
        assert!(gguf_requires_dense_attention("deepseek4"));
        // Ordinary architectures keep paged attention.
        assert!(!gguf_requires_dense_attention("llama"));
        assert!(!gguf_requires_dense_attention("qwen3moe"));
        assert!(!gguf_requires_dense_attention(""));
    }
}
