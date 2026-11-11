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
//! A checkpoint's config (`strands_decider_config.json`): what the readout needs from it, and
//! how to tell a decider config from any other JSON file.

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
/// Its name where upstream's `save_assets` writes the head beside a converted model.
pub const HEAD_CONFIG_NAME: &str = "head_config.json";

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
    /// The readout a config written by `save_assets` names, where others name `head_type`.
    readout: Option<String>,
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
            readout: None,
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
            config.readout.as_deref().is_none_or(|r| r == "pointer"),
            ConfigSnafu {
                message: format!(
                    "readout is {:?}; only pointer-head checkpoints are supported",
                    config.readout
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

    /// Tells a decider config from any other JSON by its content, as a file's name differs
    /// between upstream's writers ([`CONFIG_NAME`], [`LEGACY_CONFIG_NAME`], [`HEAD_CONFIG_NAME`]).
    ///
    /// A decider config is a JSON object with an integer `pointer_dim` that names its head, by
    /// `head_type` or by `readout`. `Ok(None)` for any other text, and for a training recipe
    /// (`train_config.json`, which names `train_files`): it has the same keys, but they are the
    /// settings training started from, not those of the exported head (a soup of heads has a
    /// wider `pointer_dim`). A decider config that the
    /// pointer readout cannot serve (another head, or a full-weight checkpoint) is an error, not
    /// `None`, so such a checkpoint is refused rather than taken for something else.
    pub fn detect(text: &str) -> Result<Option<Self>> {
        let Ok(serde_json::Value::Object(map)) = serde_json::from_str(text) else {
            return Ok(None);
        };
        let has_pointer_dim = map
            .get("pointer_dim")
            .is_some_and(|dim| dim.is_u64() || dim.is_i64());
        let names_head = map.contains_key("head_type") || map.contains_key("readout");
        let is_training_recipe = map.contains_key("train_files");
        if !(has_pointer_dim && names_head) || is_training_recipe {
            return Ok(None);
        }
        Self::from_json(text).map(Some)
    }

    /// Reads the config of the checkpoint in `dir` ([`CONFIG_NAME`], or [`LEGACY_CONFIG_NAME`]
    /// when only it exists), filling in the base revision as [`DeciderConfig::from_file`] does.
    pub fn from_checkpoint_dir(dir: &Path) -> Result<Self> {
        Self::from_file(&config_path(dir))
    }

    /// Reads the config at `path`, filling in the base revision from the `provenance.json`
    /// beside it when the config has none and the provenance names the same base model.
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut config = Self::from_json(&read(path)?)?;
        let provenance = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("provenance.json");
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

/// The config file of the checkpoint in `dir`: [`CONFIG_NAME`], or [`LEGACY_CONFIG_NAME`] when
/// only it exists.
pub(crate) fn config_path(dir: &Path) -> PathBuf {
    let path = dir.join(CONFIG_NAME);
    if !path.exists() && dir.join(LEGACY_CONFIG_NAME).exists() {
        return dir.join(LEGACY_CONFIG_NAME);
    }
    path
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

    #[test]
    fn detects_every_config_shape_by_content() {
        let detected = |text: &str| {
            DeciderConfig::detect(text)
                .unwrap_or_else(|e| panic!("{text}: {e}"))
                .map(|config| config.pointer_dim)
        };
        // `strands_decider_config.json` and the legacy `hobson_config.json` name `head_type`.
        assert_eq!(
            detected(
                r#"{"base_model": "Qwen/Qwen3.5-2B-Base", "head_type": "pointer", "pointer_dim": 768}"#
            ),
            Some(768)
        );
        // `head_config.json` from `save_assets` names `readout`.
        assert_eq!(
            detected(r#"{"readout": "pointer", "pointer_dim": 256, "max_length": 4096}"#),
            Some(256)
        );
    }

    #[test]
    fn other_json_is_not_a_decider_config() {
        for text in [
            // A model's config.json, a tokenizer config, and not JSON at all.
            r#"{"architectures": ["Qwen3_5ForCausalLM"], "hidden_size": 2048}"#,
            r#"{"bos_token": null, "eos_token": "<|endoftext|>"}"#,
            "not json",
            "[1, 2, 3]",
            // `pointer_dim` without a head named, or not an integer.
            r#"{"pointer_dim": 768}"#,
            r#"{"head_type": "pointer", "pointer_dim": "768"}"#,
            r#"{"head_type": "pointer", "pointer_dim": 76.8}"#,
            // A head named without `pointer_dim`.
            r#"{"head_type": "pointer"}"#,
            // The training recipe published beside the checkpoint, abridged.
            r#"{"base_model": "Qwen/Qwen3.5-2B-Base", "head_type": "pointer", "pointer_dim": 256,
                "epochs": 1, "lr": 0.0002, "train_files": ["train.jsonl"]}"#,
        ] {
            assert!(
                matches!(DeciderConfig::detect(text), Ok(None)),
                "{text} is not a decider config"
            );
        }
    }

    #[test]
    fn a_decider_config_the_readout_cannot_serve_is_refused() {
        for (text, why) in [
            (r#"{"head_type": "slot", "pointer_dim": 768}"#, "head_type"),
            (r#"{"readout": "slot", "pointer_dim": 768}"#, "readout"),
            (
                r#"{"head_type": "pointer", "pointer_dim": 768, "full_weight_targets": ["q_proj"]}"#,
                "full_weight_targets",
            ),
        ] {
            match DeciderConfig::detect(text) {
                Err(crate::Error::Config { message }) => {
                    assert!(message.contains(why), "{text}: {message}");
                }
                other => panic!("{text}: {other:?}"),
            }
        }
        // An empty list of full-weight targets is a LoRA checkpoint.
        assert!(matches!(
            DeciderConfig::detect(
                r#"{"head_type": "pointer", "pointer_dim": 768, "full_weight_targets": []}"#
            ),
            Ok(Some(_))
        ));
    }
}
