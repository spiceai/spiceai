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

use async_trait::async_trait;
use secrets::Secret;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub mod huggingface;
pub mod local;
pub mod spiceai;

#[derive(Debug, Snafu)]
pub enum Error {
    UnableToCreateModelSource {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to load the model: {source}"))]
    UnableToFetchModel { source: reqwest::Error },

    #[snafu(display("Unable to download model file"))]
    UnableToDownloadModelFile {},

    #[snafu(display("Unable to parse metadata"))]
    UnableToParseMetadata {},

    #[snafu(display("Unable to find home directory"))]
    UnableToFindHomeDir {},

    #[snafu(display("Unable to create model path: {source}"))]
    UnableToCreateModelPath { source: std::io::Error },

    #[snafu(display("Unable to load the configuration: {reason}"))]
    UnableToLoadConfig { reason: String },

    #[snafu(display("Unknown data source: {model_source}"))]
    UnknownModelSource { model_source: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `ModelSource` pulls a model from a source into a local directory
///
/// Implementing `pull` is required, which will fetch the model from the source (either local or
/// remote) and store it in the local directory. The local directory is returned for further
/// processing by `ModelRuntime`.
#[async_trait]
pub trait ModelSource {
    async fn pull(
        &self,
        secret: Secret,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<String>;
}

pub fn ensure_model_path(name: &str) -> Result<String> {
    let mut model_path = dirs::home_dir().context(UnableToFindHomeDirSnafu)?;
    model_path.push(".spice/models");
    model_path.push(name);

    if !model_path.exists() {
        std::fs::create_dir_all(&model_path).context(UnableToCreateModelPathSnafu)?;
    }

    let Some(model_path) = model_path.to_str() else {
        return Err(Error::UnableToCreateModelSource {
            source: "Unable to create model path".into(),
        });
    };

    Ok(model_path.to_string())
}

pub fn create_source_from(source: &str) -> Result<Box<dyn ModelSource>> {
    match source {
        "localhost" => Ok(Box::new(local::Local {})),
        "spiceai" => Ok(Box::new(spiceai::SpiceAI {})),
        "huggingface" => Ok(Box::new(huggingface::Huggingface {})),
        _ => UnknownModelSourceSnafu {
            model_source: source,
        }
        .fail(),
    }
}
