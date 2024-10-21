use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::embeddings::FailedToInstantiateEmbeddingModelSnafu;

use super::{ModelConfig, Result};
use serde::Deserialize;
use snafu::ResultExt;
use tei_backend_core::Pool;

/*
Copyright 2024 The Spice.ai OSS Authors

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

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PoolConfig {
    pooling_mode_cls_token: bool,
    pooling_mode_mean_tokens: bool,
    #[serde(default)]
    pooling_mode_lasttoken: bool,
}

impl From<PoolConfig> for Option<Pool> {
    fn from(value: PoolConfig) -> Self {
        if value.pooling_mode_cls_token {
            return Some(Pool::Cls);
        }
        if value.pooling_mode_mean_tokens {
            return Some(Pool::Mean);
        }
        if value.pooling_mode_lasttoken {
            return Some(Pool::LastToken);
        }
        None
    }
}

pub(crate) fn pool_from_str(p: &str) -> Option<Pool> {
    match p {
        "cls" => Some(Pool::Cls),
        "mean" => Some(Pool::Mean),
        "splade" => Some(Pool::Splade),
        "last_token" => Some(Pool::LastToken),
        _ => None,
    }
}

/// Determine the type of pooling for an embedding model based on the `1_Pooling/config.json`.
pub(crate) fn pooling_from_pooling_config(
    model_root: &Path,
) -> Result<Option<Pool>, Box<dyn std::error::Error + Send + Sync>> {
    let config_path = model_root.join("1_Pooling/config.json");
    if !config_path.exists() {
        return Ok(None);
    }
    let config = fs::read_to_string(config_path).boxed()?;
    let pool_config: PoolConfig = serde_json::from_str(&config).boxed()?;

    let z: Option<Pool> = pool_config.into();
    Ok(z)
}

/// Loads the model config from the `config.json` file in the model root.
pub(crate) fn model_config(model_root: &Path) -> Result<ModelConfig> {
    tracing::trace!(
        "Loading model config from {:?}",
        model_root.join("config.json")
    );
    let config_str = fs::read_to_string(model_root.join("config.json"))
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    tracing::trace!("Model config loaded.");

    let config: ModelConfig = serde_json::from_str(&config_str)
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    tracing::trace!("Model config parsed: {:?}", config);

    Ok(config)
}
