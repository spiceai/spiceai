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

#![allow(clippy::missing_errors_doc)]

use async_trait::async_trait;
use secrets::Secret;
use snafu::prelude::*;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModelSourceType {
    Huggingface,
    Local,
    SpiceAI,
}

impl fmt::Display for ModelSourceType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ModelSourceType::Huggingface => write!(f, "huggingface"),
            ModelSourceType::Local => write!(f, "file"),
            ModelSourceType::SpiceAI => write!(f, "spiceai"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ParseError {
    message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl FromStr for ModelSourceType {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.starts_with("spiceai:") => Ok(ModelSourceType::SpiceAI),
            s if s.starts_with("huggingface:") => Ok(ModelSourceType::Huggingface),
            s if s.starts_with("file:/") => Ok(ModelSourceType::Local),
            _ => Err(ParseError {
                message: "Unrecognized model source type prefix".to_string(),
            }),
        }
    }
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

impl From<ModelSourceType> for Box<dyn ModelSource> {
    fn from(source: ModelSourceType) -> Self {
        match source {
            ModelSourceType::Local => Box::new(local::Local {}),
            ModelSourceType::SpiceAI => Box::new(spiceai::SpiceAI {}),
            ModelSourceType::Huggingface => Box::new(huggingface::Huggingface {}),
        }
    }
}

#[must_use]
pub fn source(from: &str) -> ModelSourceType {
    match from.parse::<ModelSourceType>() {
        Ok(source) => source,
        Err(_) => ModelSourceType::SpiceAI,
    }
}

#[must_use]
pub fn path(from: &str) -> String {
    let sources = vec!["spiceai:"];

    for source in &sources {
        if from.starts_with(source) {
            match from.find(':') {
                Some(index) => return from[index + 1..].to_string(),
                None => return from.to_string(),
            }
        }
    }

    from.to_string()
}

#[must_use]
pub fn version(from: &str) -> String {
    let path = path(from);
    path.split(':').last().unwrap_or("").to_string()
}
