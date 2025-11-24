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

#![allow(clippy::missing_errors_doc)]

use async_trait::async_trait;
use secrecy::SecretString;
use snafu::prelude::*;
use std::collections::HashMap;
use std::fmt;
use std::path::{Component, Path};
use std::str::FromStr;
use std::sync::Arc;

use crate::modelformat::ModelFormat;

#[cfg(feature = "full")]
pub mod huggingface;
#[cfg(feature = "full")]
pub mod local;
#[cfg(feature = "full")]
pub mod spiceai;

#[derive(Debug, Snafu)]
pub enum Error {
    UnableToCreateModelSource {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Unable to load the model. {source} Verify that the model is available and accessible."
    ))]
    UnableToFetchModel { source: reqwest::Error },

    #[snafu(display(
        "Unable to download model file. Verify that the model is available and accessible."
    ))]
    UnableToDownloadModelFile {},

    #[snafu(display(
        "Unable to parse metadata. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToParseMetadata {},

    #[snafu(display(
        "Unable to find home directory. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToFindHomeDir {},

    #[snafu(display(
        "Unable to create model path. {source} Verify you have the necessary permissions to access the model path."
    ))]
    UnableToCreateModelPath { source: std::io::Error },

    #[snafu(display(
        "Invalid model name '{name}'. Model names must not be absolute paths or contain parent directory segments."
    ))]
    InvalidModelName { name: String },

    #[snafu(display(
        "Invalid model file path '{path}'. File paths must stay within the model directory."
    ))]
    InvalidModelFilePath { path: String },

    #[snafu(display(
        "Unable to load the configuration. {reason} Verify the configuration is valid, and try again."
    ))]
    UnableToLoadConfig { reason: String },

    #[snafu(display(
        "An unsupported model source was specified in the 'from' parameter: '{from}'. Specify a valid source, like 'openai', and try again. For details, visit: https://spiceai.org/docs/components/models"
    ))]
    UnknownModelSource { from: String },

    #[snafu(display(
        "The specified model format, '{model_format}', is not supported. Specify a supported model format and try again. For details, visit: https://spiceai.org/docs/components/models"
    ))]
    UnsupportedModelFormat { model_format: ModelFormat },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `ModelSource` pulls a model from a source into a local directory
///
/// Implementing `pull` is required, which will fetch the model from the source (either local or
/// remote) and store it in the local directory. The local directory is returned for further
/// processing by `ModelRuntime`.
#[async_trait]
pub trait ModelSource: Send + Sync {
    async fn pull(&self, params: Arc<HashMap<String, SecretString>>) -> Result<String>;
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
            s if s.starts_with("huggingface:")
                || s.starts_with("huggingface.co")
                || s.starts_with("hf:") =>
            {
                Ok(ModelSourceType::Huggingface)
            }
            s if s.starts_with("file:/") => Ok(ModelSourceType::Local),
            _ => Err(ParseError {
                message: "Unrecognized model source type prefix".to_string(),
            }),
        }
    }
}

pub fn ensure_model_path(name: &str) -> Result<String> {
    let mut base_path = dirs::home_dir().context(UnableToFindHomeDirSnafu)?;
    base_path.push(".spice/models");

    let candidate = Path::new(name);
    ensure!(
        !candidate.is_absolute()
            && candidate
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        InvalidModelNameSnafu {
            name: name.to_string(),
        }
    );

    let model_path = base_path.join(candidate);
    ensure!(
        model_path.starts_with(&base_path),
        InvalidModelNameSnafu {
            name: name.to_string(),
        }
    );

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

impl From<ModelSourceType> for Option<Box<dyn ModelSource>> {
    #[allow(unused_variables)]
    fn from(source: ModelSourceType) -> Self {
        #[cfg(feature = "full")]
        if source == ModelSourceType::Local {
            return Some(Box::new(local::Local {}));
        }

        #[cfg(feature = "full")]
        if source == ModelSourceType::SpiceAI {
            return Some(Box::new(spiceai::SpiceAI {}));
        }

        #[cfg(feature = "full")]
        if source == ModelSourceType::Huggingface {
            return Some(Box::new(huggingface::Huggingface {}));
        }
        None
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
    path.split(':').next_back().unwrap_or("").to_string()
}
