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
//! A checkpoint's `strands_decider_config.json`: what the readout needs from it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use snafu::{ResultExt, ensure};

use crate::schema::Kind;
use crate::{ConfigSnafu, ReadFileSnafu, Result};

/// The config file of a checkpoint.
pub const CONFIG_NAME: &str = "strands_decider_config.json";
/// Its name in checkpoints saved before the rename.
pub const LEGACY_CONFIG_NAME: &str = "hobson_config.json";

/// The settings of a checkpoint that the readout uses. Keys it does not use are ignored.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct DeciderConfig {
    /// The Hugging Face id of the base model the adapter was trained on.
    pub base_model: String,
    /// The base model's revision; [`DeciderConfig::from_checkpoint_dir`] falls back to the
    /// checkpoint's `provenance.json`.
    pub base_revision: Option<String>,
    /// The longest prompt, state and one question, in tokens.
    pub max_length: usize,
    /// The head's query and key width. A soup of N heads has N times one head's.
    pub pointer_dim: usize,
    /// The calibrated temperature for a kind with none of its own.
    pub temperature: f64,
    /// Calibrated temperatures by kind (`noul`, `choice`, `score`).
    pub temperature_by_kind: HashMap<String, f64>,
    /// The mass training put on a score's neighbouring levels.
    pub ordinal_smoothing: f64,
    pub use_lora: bool,
    /// Start every prompt with the tokenizer's BOS token even when it adds none itself.
    pub force_bos: bool,
    head_type: Option<String>,
    full_weight_targets: Vec<String>,
}

impl Default for DeciderConfig {
    /// Upstream's defaults, for keys a config leaves out.
    fn default() -> Self {
        Self {
            base_model: "Qwen/Qwen3-1.7B-Base".to_string(),
            base_revision: None,
            max_length: 3072,
            pointer_dim: 256,
            temperature: 1.0,
            temperature_by_kind: HashMap::new(),
            ordinal_smoothing: 0.0,
            use_lora: true,
            force_bos: false,
            head_type: None,
            full_weight_targets: Vec::new(),
        }
    }
}

impl DeciderConfig {
    /// Parses a config, refusing checkpoints the pointer readout cannot serve: a slot head,
    /// or a full-weight checkpoint.
    pub fn from_json(text: &str) -> Result<Self> {
        let config: Self = serde_json::from_str(text).map_err(|e| {
            ConfigSnafu {
                message: e.to_string(),
            }
            .build()
        })?;
        ensure!(
            config.head_type.as_deref().is_none_or(|t| t == "pointer"),
            ConfigSnafu {
                message: format!(
                    "head_type is {:?}; only pointer-head checkpoints are supported",
                    config.head_type
                ),
            }
        );
        ensure!(
            config.full_weight_targets.is_empty(),
            ConfigSnafu {
                message: "full_weight_targets is set; only LoRA checkpoints are supported",
            }
        );
        ensure!(
            config.pointer_dim > 0,
            ConfigSnafu {
                message: "pointer_dim must be positive",
            }
        );
        Ok(config)
    }

    /// Reads the config of the checkpoint in `dir` ([`CONFIG_NAME`], or [`LEGACY_CONFIG_NAME`]
    /// when only it exists), filling in the base revision from `provenance.json` when the config
    /// has none and the provenance names the same base model.
    pub fn from_checkpoint_dir(dir: &Path) -> Result<Self> {
        let mut path = dir.join(CONFIG_NAME);
        if !path.exists() && dir.join(LEGACY_CONFIG_NAME).exists() {
            path = dir.join(LEGACY_CONFIG_NAME);
        }
        let mut config = Self::from_json(&read(&path)?)?;
        let provenance = dir.join("provenance.json");
        if config.base_revision.is_none() && provenance.exists() {
            let meta: serde_json::Value =
                serde_json::from_str(&read(&provenance)?).map_err(|e| {
                    ConfigSnafu {
                        message: format!("provenance.json: {e}"),
                    }
                    .build()
                })?;
            if meta.get("base_model").and_then(|m| m.as_str()) == Some(config.base_model.as_str()) {
                config.base_revision = meta
                    .get("base_model_revision")
                    .and_then(|r| r.as_str())
                    .map(str::to_string);
            }
        }
        Ok(config)
    }

    /// The temperature that `kind`'s logits are divided by.
    #[must_use]
    pub fn temperature(&self, kind: Kind) -> f64 {
        self.temperature_by_kind
            .get(kind.as_str())
            .copied()
            .unwrap_or(self.temperature)
    }
}

fn read(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).context(ReadFileSnafu {
        path: PathBuf::from(path),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_released_config() {
        // strands_decider_config.json of StrandsAgents/strands-decider-2B-qwen3.5-v1-2610,
        // less the LoRA keys.
        let config = DeciderConfig::from_json(
            r#"{"base_model": "Qwen/Qwen3.5-2B-Base", "num_slots": 24, "head_type": "pointer",
                "max_length": 4096, "pointer_dim": 768, "temperature": 1.0545210242263665,
                "temperature_by_kind": {"noul": 1.0704394404346953, "choice": 0.8629556148274228,
                "score": 1.834409163182587}, "ordinal_smoothing": 0.1, "use_lora": true}"#,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(config.pointer_dim, 768);
        assert!((config.temperature(Kind::Score) - 1.834_409_163_182_587).abs() < 1e-15);
        assert!(!config.force_bos);
        assert!(matches!(
            DeciderConfig::from_json(r#"{"head_type": "slot"}"#),
            Err(crate::Error::Config { .. })
        ));
        assert!(matches!(
            DeciderConfig::from_json(r#"{"full_weight_targets": ["q_proj"]}"#),
            Err(crate::Error::Config { .. })
        ));
        // A kind with no fitted temperature falls back to the global one.
        let config =
            DeciderConfig::from_json(r#"{"temperature": 2.0}"#).unwrap_or_else(|e| panic!("{e}"));
        assert!((config.temperature(Kind::Noul) - 2.0).abs() < 1e-15);
    }
}
