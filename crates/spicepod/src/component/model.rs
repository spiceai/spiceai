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

use regex::Regex;
use std::{collections::HashMap, fmt::Display, path::Path, sync::LazyLock};

use super::{Nameable, WithDependsOn};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Model {
    pub from: String,
    pub name: String,

    pub description: Option<String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub metadata: HashMap<String, Value>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "files", default)]
    pub files: Vec<ModelFile>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, Value>,

    #[serde(rename = "datasets", default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl Nameable for Model {
    fn name(&self) -> &str {
        &self.name
    }
}

impl WithDependsOn<Model> for Model {
    fn depends_on(&self, depends_on: &[String]) -> Model {
        Model {
            from: self.from.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            metadata: self.metadata.clone(),
            files: self.files.clone(),
            params: self.params.clone(),
            datasets: self.datasets.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}

/// Describe where the [`Model`] is sourced from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub enum ModelSource {
    OpenAi,
    Azure,
    Anthropic,
    Xai,
    HuggingFace,
    Perplexity,
    SpiceAI,
    File,
}

// Matches model paths in these formats:
// - organization/model-name
// - organization/model-name:revision
// - huggingface:organization/model-name
// - hf:organization/model-name
// - huggingface:organization/model-name:revision
// - hf:organization/model-name:revision
// - huggingface.co/organization/model-name
// - huggingface.co/organization/model-name:revision
// - huggingface:huggingface.co/organization/model-name
// - hf:huggingface.co/organization/model-name
//
// Captures three named groups:
// - org: Organization name (allows word chars and hyphens)
// - model: Model name (allows word chars, hyphens, and dots)
// - revision: Optional revision/version (allows word chars, digits, hyphens, and dots)
pub static HUGGINGFACE_PATH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    match Regex::new(
        r"\A(?:(?:huggingface|hf):)?(huggingface\.co\/)?(?<org>[\w\-]+)\/(?<model>[\w\-\.]+)(:(?<revision>[\w\d\-\.]+))?\z",
    ) {
        Ok(regex) => regex,
        Err(_) => {
            unreachable!("Regex is checked at compile time");
        }
    }
});

/// Implement the [`TryFrom<&str>`] trait for [`ModelSource`]. Should be the inverse of [`ModelSource`]'s [`Display`].
impl TryFrom<&str> for ModelSource {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.starts_with("huggingface") || value.starts_with("hf") {
            Ok(ModelSource::HuggingFace)
        } else if value.starts_with("file") {
            Ok(ModelSource::File)
        } else if value.starts_with("anthropic") {
            Ok(ModelSource::Anthropic)
        } else if value.starts_with("perplexity") {
            Ok(ModelSource::Perplexity)
        } else if value.starts_with("openai") {
            Ok(ModelSource::OpenAi)
        } else if value.starts_with("azure") {
            Ok(ModelSource::Azure)
        } else if value.starts_with("xai") {
            Ok(ModelSource::Xai)
        } else if value.starts_with("spiceai") {
            Ok(ModelSource::SpiceAI)
        } else {
            Err("Unknown prefix")
        }
    }
}

/// Implement the [`Display`] trait for [`ModelSource`]. Should be the inverse of [`TryFrom<&str>`].
impl Display for ModelSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelSource::OpenAi => write!(f, "openai"),
            ModelSource::Azure => write!(f, "azure"),
            ModelSource::Xai => write!(f, "xai"),
            ModelSource::Anthropic => write!(f, "anthropic"),
            ModelSource::Perplexity => write!(f, "perplexity"),
            ModelSource::HuggingFace => write!(f, "huggingface"),
            ModelSource::File => write!(f, "file"),
            ModelSource::SpiceAI => write!(f, "spiceai"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub enum ModelType {
    Llm,
    Ml,
}

impl Display for ModelType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelType::Llm => write!(f, "Llm"),
            ModelType::Ml => write!(f, "Ml"),
        }
    }
}

impl Model {
    #[must_use]
    pub fn new(from: impl Into<String>, name: impl Into<String>) -> Self {
        Model {
            from: from.into(),
            name: name.into(),
            description: None,
            metadata: HashMap::default(),
            files: Vec::default(),
            params: HashMap::default(),
            datasets: Vec::default(),
            depends_on: Vec::default(),
        }
    }

    #[must_use]
    pub fn get_all_file_paths(&self) -> Vec<String> {
        self.get_all_files()
            .iter()
            .map(|f| f.path.clone())
            .collect()
    }

    /// Finds at most one model file with the given [`ModelFileType`].
    #[must_use]
    pub fn find_any_file_path(&self, file_type: ModelFileType) -> Option<String> {
        self.get_all_files()
            .iter()
            .find(|f| f.file_type() == Some(file_type))
            .map(|f| f.path.clone())
    }

    /// Finds all models with a given [`ModelFileType`].
    #[must_use]
    pub fn find_all_file_path(&self, file_type: ModelFileType) -> Vec<String> {
        self.get_all_files()
            .iter()
            .filter(|f| f.file_type() == Some(file_type))
            .map(|f| f.path.clone())
            .collect()
    }

    /// Get all files for the model component, if a [`ModelFile`] is a directory, include all files in the directory too.
    #[must_use]
    pub fn get_all_files(&self) -> Vec<ModelFile> {
        let mut component_files = self.files.clone();

        // If `from:file:...` then add the model_id as a possible source of files.
        if matches!(
            ModelSource::try_from(self.from.as_str()),
            Ok(ModelSource::File)
        ) {
            if let Some(id) = self.get_model_id() {
                component_files.push(ModelFile {
                    path: id,
                    name: Some("from_id".to_string()),
                    r#type: Some(ModelFileType::Weights),
                    params: None,
                });
            }
        }
        component_files
            .iter()
            .flat_map(|f| {
                if Path::new(&f.path).is_dir() {
                    tracing::debug!("Loading model files from: '{}'.", f.path);

                    if let Ok(read_dir) = Path::new(&f.path).read_dir() {
                        read_dir
                            .filter_map(|a| {
                                if let Ok(r) = a {
                                    r.path().to_str().map(|s| ModelFile {
                                        path: s.to_string(),
                                        name: None,
                                        r#type: determine_type_from_path(s),
                                        params: f.params.clone(),
                                    })
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        vec![]
                    }
                } else {
                    vec![f.clone()]
                }
            })
            .collect()
    }

    #[must_use]
    pub fn get_source(&self) -> Option<ModelSource> {
        ModelSource::try_from(self.from.as_str()).ok()
    }

    /// Get the model id from the `from` field. The model id is the part of the `from` field after the source.
    ///
    /// # Example
    /// - `spice.ai/taxi_tech_co/taxi_drives/models/drive_stats:latest`
    ///     - Prefix: `spice.ai`
    ///     - Source: `taxi_tech_co/taxi_drives/models/drive_stats:latest`
    /// - `huggingface:huggingface.co/transformers/gpt-2:latest`
    ///    - Prefix: `huggingface:huggingface.co`
    ///    - Source: `transformers/gpt-2:latest`
    /// - `file://absolute/path/to/my/model.onnx`
    ///     - Prefix: `file:`
    ///     - Source: `/absolute/path/to/my/model.onnx`
    /// - `openai`
    ///    - Prefix: `openai`
    ///    - Source: None
    /// - `openai:gpt-4o`
    ///    - Prefix: `openai`
    ///    - Source: `gpt-4o`
    #[must_use]
    pub fn get_model_id(&self) -> Option<String> {
        match self.get_source() {
            Some(ModelSource::HuggingFace) => {
                HUGGINGFACE_PATH_REGEX.captures(&self.from).map(|caps| {
                    let model = format!("{}/{}", &caps["org"], &caps["model"]);
                    if let Some(revision) = caps.name("revision") {
                        format!("{}:{}", model, revision.as_str())
                    } else {
                        model
                    }
                })
            }
            Some(p) => {
                let from = &self.from;
                if let Some(stripped) = from.strip_prefix(&format!("{p}:")) {
                    Some(stripped.to_string())
                } else {
                    from.strip_prefix(&format!("{p}/"))
                        .map(std::string::ToString::to_string)
                }
            }
            None => None,
        }
    }

    /// Attempts to determine the model's type based on its `from` field and, `files` and `params`.
    ///
    /// ### Current support/checks
    ///
    /// | ModelType | OpenAI  |      Hugging Face       | Spice   | Local          |
    /// | --------- | ------- | ----------------------- | ------- | -------------- |
    /// | Llm       | Default | `params.model_type` set | N/A     | File Specified |
    /// | Ml        |  N/A    | ONNX file specified     | Default | File specified |
    pub fn model_type(&self) -> Option<ModelType> {
        let Ok(source) = ModelSource::try_from(self.from.as_str()) else {
            tracing::error!("Unknown model source from model: {}", self.from);
            return None;
        };

        // Some providers only support either ML or LLMs.
        if matches!(
            source,
            ModelSource::Perplexity
                | ModelSource::Azure
                | ModelSource::OpenAi
                | ModelSource::Anthropic
                | ModelSource::Xai
        ) {
            return Some(ModelType::Llm);
        }

        if source == ModelSource::SpiceAI {
            return Some(ModelType::Ml);
        };

        let files = self.get_all_files();

        // TODO: Need to scan filenames from HF for [`ModelSource::HuggingFace`]. Below is a hack
        // to determine if it's an LLM from HF by check if an ML files are set manually.
        let no_ml_files = files.iter().all(|f| !is_ml_file(Path::new(&f.path)));
        if source == ModelSource::HuggingFace && no_ml_files {
            return Some(ModelType::Llm);
        }

        let is_llm = files.iter().any(|f| {
            match f.file_type() {
                // Only true since embeddings aren't [`Model`]s.
                Some(
                    ModelFileType::Tokenizer
                    | ModelFileType::Config
                    | ModelFileType::TokenizerConfig,
                ) => true,
                _ => is_llm_file(Path::new(&f.path)),
            }
        });
        if is_llm {
            return Some(ModelType::Llm);
        }

        if files.iter().any(|f| is_ml_file(Path::new(&f.path))) {
            return Some(ModelType::Ml);
        }

        None
    }

    /// Returns the subset of [`Self::params`] that are `OpenAI` request overrides. These will be
    /// used as the default values in the request payload, instead of the provider's defaults.
    ///
    /// `OpenAI` request overrides are parameters that start with `openai_`. The returned keys will be
    /// without the `openai_` prefix.
    #[must_use]
    pub fn get_openai_request_overrides(&self) -> Vec<(String, Value)> {
        self.params
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("openai_")
                    .map(|new_k| (new_k.to_string(), v.clone()))
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct ModelFile {
    pub path: String,
    pub name: Option<String>,

    /// Should use [`Self::file_type`] to access.
    pub(crate) r#type: Option<ModelFileType>,

    pub params: Option<HashMap<String, String>>,
}

impl ModelFile {
    #[must_use]
    pub fn from_path(p: &Path) -> Self {
        Self {
            path: p.display().to_string(),
            name: None,
            r#type: None,
            params: None,
        }
    }

    /// Returns the [`ModelFileType`] if explicitly set, otherwise attempts to determine the file
    /// type for the [`ModelFile`] based on the file path.
    #[must_use]
    pub fn file_type(&self) -> Option<ModelFileType> {
        match self.r#type {
            Some(t) => Some(t),
            None => {
                if let Some(t) = self.r#type {
                    Some(t)
                } else {
                    let typ = determine_type_from_path(&self.path);
                    tracing::trace!("Determined model file type for {}: {:?}", self.path, typ);
                    typ
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum ModelFileType {
    Weights,
    Config,
    Tokenizer,
    TokenizerConfig,
    GenerationConfig,
}

/// Attempts to determine the file type for the [`ModelFile`] based on the file path. If
/// [`determine_type_from_path`] is None, the file may be one of [`ModelFileType`], but the type
/// could not be determined.
pub(crate) fn determine_type_from_path(p: &str) -> Option<ModelFileType> {
    let path = Path::new(p);

    if is_ml_file(path) || is_llm_file(path) {
        return Some(ModelFileType::Weights);
    }

    let filename = path.file_name().map(|f| f.to_string_lossy().to_string())?;

    if filename == "config.json" {
        return Some(ModelFileType::Config);
    }

    if filename == "tokenizer.json" {
        return Some(ModelFileType::Tokenizer);
    }

    if filename == "tokenizer_config.json" {
        return Some(ModelFileType::TokenizerConfig);
    }

    if filename == "generation_config.json" {
        return Some(ModelFileType::GenerationConfig);
    }

    None
}

/// Returns true if the file is an ML model file. Possible false negatives, but attempts to be positively certain (i.e. avoid false positives).
pub(crate) fn is_ml_file(p: &Path) -> bool {
    let extension = p
        .extension()
        .map(|e| e.to_string_lossy().to_string())
        .unwrap_or_default();

    extension == "onnx"
}

/// Returns true if the file is an LLM model file. Possible false negatives, but attempts to be positively certain (i.e. avoid false positives).
pub(crate) fn is_llm_file(p: &Path) -> bool {
    let Some(filename) = p.file_name().map(|f| f.to_string_lossy().to_string()) else {
        return false;
    };
    let extension = p
        .extension()
        .map(|e| e.to_string_lossy().to_string())
        .unwrap_or_default();

    // `extension == "safetensors" || filename == "pytorch_model.bin"` also true for embeddings.
    extension == "gguf"
        || extension == "ggml"
        || extension == "safetensors"
        || filename == "pytorch_model.bin"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_huggingface_path_regex() {
        let test_cases = vec![
            // Basic format
            (
                "organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With revision
            (
                "organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With huggingface: prefix
            (
                "huggingface:organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With hf: prefix
            (
                "hf:organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With huggingface: prefix and revision
            (
                "huggingface:organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With hf: prefix and revision
            (
                "hf:organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With huggingface.co domain
            (
                "huggingface.co/organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With huggingface.co domain and revision
            (
                "huggingface.co/organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With huggingface: prefix and huggingface.co domain
            (
                "huggingface:huggingface.co/organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With hf: prefix and huggingface.co domain
            (
                "hf:huggingface.co/organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With huggingface: prefix, huggingface.co domain, and revision
            (
                "huggingface:huggingface.co/organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With hf: prefix, huggingface.co domain, and revision
            (
                "hf:huggingface.co/organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // Test hyphens in organization name
            ("my-org/model-name", ("my-org", "model-name", "")),
            // Test hyphens and dots in model name
            (
                "organization/my-model.v2",
                ("organization", "my-model.v2", ""),
            ),
            // Test complex revision with hyphens, dots, and numbers
            (
                "organization/model-name:v1.2-beta.3",
                ("organization", "model-name", "v1.2-beta.3"),
            ),
            // Test 'latest' revision (handled in code)
            (
                "organization/model-name:latest",
                ("organization", "model-name", "latest"),
            ),
        ];

        for (input, expected) in test_cases {
            let caps = HUGGINGFACE_PATH_REGEX
                .captures(input)
                .unwrap_or_else(|| panic!("Failed to match valid input: {input}"));

            assert_eq!(&caps["org"], expected.0, "org mismatch for input: {input}");
            assert_eq!(
                &caps["model"], expected.1,
                "model mismatch for input: {input}"
            );

            let revision = caps.name("revision").map_or("", |m| m.as_str());
            assert_eq!(revision, expected.2, "revision mismatch for input: {input}");
        }
    }

    #[test]
    fn test_invalid_huggingface_paths() {
        let invalid_paths = vec![
            "",                   // Empty string
            "invalid",            // No slash
            "/",                  // Just a slash
            "org/",               // Missing model name
            "/model",             // Missing organization
            "org/model:",         // Empty revision
            "org/model::",        // Double colon
            "huggingface:",       // Missing path
            "hf:",                // Missing path
            "huggingface:/",      // Invalid path
            "hf:/",               // Invalid path
            "huggingface.co",     // Missing path
            "huggingface.co/",    // Missing org and model
            "org/model/extra",    // Extra path component
            "@org/model",         // Invalid character in org
            "org/@model",         // Invalid character in model
            "org/model:@version", // Invalid character in revision
        ];

        for path in invalid_paths {
            assert!(
                HUGGINGFACE_PATH_REGEX.captures(path).is_none(),
                "Should not match invalid path: {path}"
            );
        }
    }
}
