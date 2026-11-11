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

//! Strands decider models served locally: found by the content of a model's files, and
//! answered by a registered engine.
//!
//! A decider is a base model, a `LoRA` adapter and a small pointer head
//! ([`strands_decider`]). A Spicepod names its files; nothing in the Spicepod says it is a
//! decider. A model is one when a JSON file among its files is a decider config
//! ([`DeciderConfig::detect`]):
//!
//! ```text
//!  model files ──► *.json ──content──► decider config? ── no ──► chat model, as before
//!                                            │ yes
//!                  *.gguf ──general.type──► "model"   ──► base   ┐
//!                                       └─► "adapter" ──► adapter├─► DeciderFiles ──► engine
//!                  config's directory ───► head.safetensors,      ┘
//!                                          tokenizer.json
//! ```
//!
//! The base model and the adapter stay separate files; the engine applies the adapter when it
//! decodes. An engine crate registers itself with [`register_local_decider_engine!`], as chat
//! engines do ([`crate::chat::engine`]), so `llms` never depends on one.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use evaluate_api::{
    Answer, EntryType, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState, NonNullEntry,
    NullableEntry, Question, Usage,
};
use futures::future::BoxFuture;
use huggingface_hub::{DownloadError, Snapshot};
use runtime_rate_control::RateController;
use snafu::{ResultExt, Snafu};
use strands_decider::{
    ChoiceOptions, Content, DeciderConfig, Evaluation, Meaning, NoulCriteria, ScoreLevels,
};

use crate::chat::gguf_header::{self, GENERAL_TYPE};
use crate::hf::HubParams;

/// Larger JSON files are not read for a decider config: a config is well under a kilobyte, and
/// a tokenizer is many megabytes.
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

/// The files beside a decider config that its readout reads; the tokenizer config and the
/// provenance are optional.
const BESIDE_CONFIG: [&str; 4] = [
    "head.safetensors",
    "tokenizer.json",
    "tokenizer_config.json",
    "provenance.json",
];

const DOCS_URL: &str = "https://spiceai.org/docs/components/models";

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "Model '{model}' is a Strands decider ('{}'), but no base model GGUF was found among its files{hint}. A decider needs the base model as a GGUF whose `general.type` is 'model' (convert it with llama.cpp's convert_hf_to_gguf.py). See: {DOCS_URL}",
        config.display()
    ))]
    NoBaseGguf {
        model: String,
        config: PathBuf,
        hint: String,
    },

    #[snafu(display(
        "Model '{model}' is a Strands decider ('{}'), but no LoRA adapter GGUF was found among its files{hint}. A decider needs its adapter as a GGUF whose `general.type` is 'adapter' (convert it with llama.cpp's convert_lora_to_gguf.py). See: {DOCS_URL}",
        config.display()
    ))]
    NoAdapterGguf {
        model: String,
        config: PathBuf,
        hint: String,
    },

    #[snafu(display(
        "Model '{model}' is a Strands decider, but more than one {kind} GGUF was found ({}). Name one in the model's `files`.",
        files.join(", ")
    ))]
    SeveralGgufs {
        model: String,
        kind: GgufKind,
        files: Vec<String>,
    },

    #[snafu(display(
        "Model '{model}' has more than one Strands decider config ({}). Give it the files of one decider.",
        files.join(", ")
    ))]
    SeveralConfigs { model: String, files: Vec<String> },

    #[snafu(display(
        "Model '{model}' is a Strands decider that Spice cannot serve ('{}'): {source}",
        path.display()
    ))]
    UnsupportedDecider {
        model: String,
        path: PathBuf,
        source: strands_decider::Error,
    },

    #[snafu(display("Model '{model}' could not read '{}': {source}", path.display()))]
    ReadFile {
        model: String,
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("{source}"))]
    Download { source: DownloadError },

    #[snafu(display(
        "Model '{model}' is a Strands decider, which this build of spiced cannot serve: it needs the llama.cpp engine (the `llamacpp` feature)."
    ))]
    NoEngine { model: String },

    #[snafu(display("Model '{model}' failed to load on {engine}: {source}"))]
    Load {
        model: String,
        engine: &'static str,
        source: BoxError,
    },
}

impl Error {
    /// Whether the load cannot succeed until the Spicepod or the build changes.
    #[must_use]
    pub fn is_configuration_error(&self) -> bool {
        match self {
            Self::NoBaseGguf { .. }
            | Self::NoAdapterGguf { .. }
            | Self::SeveralGgufs { .. }
            | Self::SeveralConfigs { .. }
            | Self::UnsupportedDecider { .. }
            | Self::NoEngine { .. } => true,
            Self::Download { source } => source.is_configuration_error(),
            Self::ReadFile { .. } | Self::Load { .. } => false,
        }
    }
}

/// What a GGUF file holds, by its header's `general.type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GgufKind {
    /// A model: `general.type` is `model`, or absent, as in a file written before the key
    /// existed (llama.cpp loads such a file as a model).
    Model,
    /// A `LoRA` adapter: `general.type` is `adapter`.
    Adapter,
}

impl GgufKind {
    /// The kind a header's `general.type` names; `None` for any other file, such as a
    /// multimodal projector.
    #[must_use]
    pub fn of(general_type: Option<&str>) -> Option<Self> {
        match general_type {
            None | Some("model") => Some(Self::Model),
            Some("adapter") => Some(Self::Adapter),
            Some(_) => None,
        }
    }
}

impl std::fmt::Display for GgufKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Model => "base model",
            Self::Adapter => "LoRA adapter",
        })
    }
}

/// The files a local decider loads from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeciderFiles {
    /// The base model's GGUF.
    pub base: PathBuf,
    /// The `LoRA` adapter's GGUF, applied to the base model when it decodes.
    pub adapter: PathBuf,
    /// The decider config; `head.safetensors` and `tokenizer.json` are beside it.
    pub config: PathBuf,
}

/// Chooses a decider's base model and adapter from its GGUF files, each given with its
/// header's `general.type`: exactly one of each.
///
/// # Errors
///
/// Returns an error if either is missing or found more than once. `hint` follows the
/// missing-file error, to say what was found instead.
pub fn choose_ggufs<P: AsRef<Path>>(
    model: &str,
    config: &Path,
    ggufs: &[(P, Option<String>)],
    hint: &str,
) -> Result<(PathBuf, PathBuf), Error> {
    let of_kind = |kind: GgufKind| -> Vec<PathBuf> {
        ggufs
            .iter()
            .filter(|(_, general_type)| GgufKind::of(general_type.as_deref()) == Some(kind))
            .map(|(path, _)| path.as_ref().to_path_buf())
            .collect()
    };
    let one = |kind: GgufKind, missing: Error| -> Result<PathBuf, Error> {
        match of_kind(kind).as_slice() {
            [] => Err(missing),
            [only] => Ok(only.clone()),
            several => SeveralGgufsSnafu {
                model,
                kind,
                files: several
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>(),
            }
            .fail(),
        }
    };
    let base = one(
        GgufKind::Model,
        Error::NoBaseGguf {
            model: model.to_string(),
            config: config.to_path_buf(),
            hint: hint.to_string(),
        },
    )?;
    let adapter = one(
        GgufKind::Adapter,
        Error::NoAdapterGguf {
            model: model.to_string(),
            config: config.to_path_buf(),
            hint: hint.to_string(),
        },
    )?;
    Ok((base, adapter))
}

/// What follows a missing-GGUF error when the files hold weights in another format: a
/// checkpoint as Hugging Face publishes it has its adapter as safetensors.
fn other_weights_hint<'a>(paths: impl Iterator<Item = &'a str>) -> String {
    let others: Vec<&str> = paths
        .filter(|path| {
            Path::new(path)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("safetensors"))
                && !path.ends_with("head.safetensors")
        })
        .collect();
    if others.is_empty() {
        String::new()
    } else {
        format!(
            " (found safetensors weights: {}; serving a decider from safetensors is not supported yet, see https://github.com/spiceai/spiceai/issues/16198)",
            others.join(", ")
        )
    }
}

fn is_extension(path: &str, extension: &str) -> bool {
    Path::new(path)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case(extension))
}

/// The decider config among `configs`, each a path with its text; `None` when none is one.
fn find_config<'a>(
    model: &str,
    configs: impl Iterator<Item = (&'a Path, &'a str)>,
) -> Result<Option<PathBuf>, Error> {
    let mut found = Vec::new();
    for (path, text) in configs {
        if DeciderConfig::detect(text)
            .context(UnsupportedDeciderSnafu { model, path })?
            .is_some()
        {
            found.push(path.to_path_buf());
        }
    }
    match found.as_slice() {
        [] => Ok(None),
        [only] => Ok(Some(only.clone())),
        several => SeveralConfigsSnafu {
            model,
            files: several
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>(),
        }
        .fail(),
    }
}

/// The decider files among a local model's `files`, or `None` when none of its JSON files is a
/// decider config, so the model is not a decider.
///
/// # Errors
///
/// Returns an error if a decider config cannot be served, or the decider lacks a base model or
/// adapter GGUF, or has more than one.
pub async fn detect_local(model: &str, files: &[PathBuf]) -> Result<Option<DeciderFiles>, Error> {
    let mut configs = Vec::new();
    for path in files
        .iter()
        .filter(|path| path.to_str().is_some_and(|path| is_extension(path, "json")))
    {
        let metadata = tokio::fs::metadata(path)
            .await
            .context(ReadFileSnafu { model, path })?;
        if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
            continue;
        }
        let text = tokio::fs::read_to_string(path)
            .await
            .context(ReadFileSnafu { model, path })?;
        configs.push((path.clone(), text));
    }
    let Some(config) = find_config(
        model,
        configs
            .iter()
            .map(|(path, text)| (path.as_path(), text.as_str())),
    )?
    else {
        return Ok(None);
    };

    let mut ggufs = Vec::new();
    for path in files
        .iter()
        .filter(|path| path.to_str().is_some_and(|path| is_extension(path, "gguf")))
    {
        let general_type = gguf_header::local_string(path, GENERAL_TYPE)
            .await
            .context(ReadFileSnafu { model, path })?;
        ggufs.push((path.clone(), general_type));
    }
    let hint = other_weights_hint(files.iter().filter_map(|path| path.to_str()));
    let (base, adapter) = choose_ggufs(model, &config, &ggufs, &hint)?;
    Ok(Some(DeciderFiles {
        base,
        adapter,
        config,
    }))
}

/// The decider files of the Hugging Face model `model_id` (`org/name[:revision]`), downloaded,
/// or `None` when none of its top-level JSON files is a decider config. Only those JSON files
/// are downloaded to tell; a decider's GGUF files are told apart by headers read from the Hub.
/// `selectors` are the model's `files`, which name the GGUF files to choose from when the
/// repository holds several of a kind.
///
/// # Errors
///
/// As [`detect_local`], and if the repository cannot be read or a download fails.
pub async fn detect_hf(
    model_id: &str,
    selectors: &[&str],
    hub: HubParams<'_>,
    model: &str,
) -> Result<Option<DeciderFiles>, Error> {
    let snapshot = crate::chat::hf_files::open(model_id, hub, model)
        .await
        .context(DownloadSnafu)?;
    let json: Vec<&str> = snapshot
        .files()
        .filter(|file| {
            !file.path.contains('/')
                && is_extension(&file.path, "json")
                && file.size <= MAX_CONFIG_BYTES
        })
        .map(|file| file.path.as_str())
        .collect();
    if json.is_empty() {
        return Ok(None);
    }
    let fetched = snapshot.fetch_quietly(&json).await.context(DownloadSnafu)?;
    let mut configs = Vec::new();
    for (name, path) in json.iter().zip(&fetched) {
        let text = tokio::fs::read_to_string(path)
            .await
            .context(ReadFileSnafu { model, path })?;
        configs.push((PathBuf::from(name), text));
    }
    let Some(config) = find_config(
        model,
        configs
            .iter()
            .map(|(path, text)| (path.as_path(), text.as_str())),
    )?
    else {
        return Ok(None);
    };

    let (base, adapter) = hf_ggufs(&snapshot, selectors, model, &config).await?;
    let mut paths = vec![base.as_str(), adapter.as_str()];
    let config_name = config.to_string_lossy().to_string();
    paths.push(&config_name);
    paths.extend(
        BESIDE_CONFIG
            .iter()
            .copied()
            .filter(|name| snapshot.file(name).is_some()),
    );
    snapshot.download(&paths).await.context(DownloadSnafu)?;
    let dir = snapshot.dir();
    Ok(Some(DeciderFiles {
        base: dir.join(base),
        adapter: dir.join(adapter),
        config: dir.join(config),
    }))
}

/// The repository paths of a decider's base model and adapter GGUFs, chosen by the headers
/// read from the Hub, among those `selectors` name when they name any.
async fn hf_ggufs(
    snapshot: &Snapshot,
    selectors: &[&str],
    model: &str,
    config: &Path,
) -> Result<(String, String), Error> {
    let named = |path: &str| {
        selectors.is_empty()
            || selectors.iter().any(|selector| {
                let selector = selector.trim_start_matches("./");
                path == selector || path.ends_with(&format!("/{selector}"))
            })
    };
    let mut ggufs = Vec::new();
    for path in snapshot
        .files()
        .map(|file| file.path.as_str())
        .filter(|path| is_extension(path, "gguf") && named(path))
    {
        let general_type = gguf_header::read_string(GENERAL_TYPE, |len| {
            let path = path.to_string();
            async move { snapshot.read_prefix(&path, len).await }
        })
        .await
        .context(DownloadSnafu)?;
        ggufs.push((PathBuf::from(path), general_type));
    }
    let hint = other_weights_hint(snapshot.files().map(|file| file.path.as_str()));
    let (base, adapter) = choose_ggufs(model, config, &ggufs, &hint)?;
    Ok((
        base.to_string_lossy().to_string(),
        adapter.to_string_lossy().to_string(),
    ))
}

/// What an engine gets to load a decider.
pub struct DeciderLoad<'a> {
    /// The Spicepod model name, which errors and responses name.
    pub name: &'a str,
    pub files: &'a DeciderFiles,
    /// Every request takes a permit from it.
    pub rate_controller: Arc<RateController>,
}

/// Loads a decider from its files.
pub type LoadDecider =
    for<'a> fn(DeciderLoad<'a>) -> BoxFuture<'a, Result<Arc<dyn Evaluate>, BoxError>>;

/// An engine that serves local deciders.
pub struct LocalDeciderEngine {
    /// The engine's name, as logs and errors give it.
    pub name: &'static str,
    pub load: LoadDecider,
}

#[linkme::distributed_slice]
pub static LOCAL_DECIDER_ENGINES: [LocalDeciderEngine];

/// Loads the decider `load` names on the first registered engine.
///
/// # Errors
///
/// Returns an error if no engine is registered, or the engine fails to load it.
pub async fn load(load: DeciderLoad<'_>) -> Result<Arc<dyn Evaluate>, Error> {
    let model = load.name.to_string();
    let Some(engine) = LOCAL_DECIDER_ENGINES.first() else {
        return NoEngineSnafu { model }.fail();
    };
    tracing::info!(
        "Model '{model}' is a Strands decider: base '{}', adapter '{}', config '{}'. It answers decisions on {} and serves no chat.",
        load.files.base.display(),
        load.files.adapter.display(),
        load.files.config.display(),
        engine.name
    );
    (engine.load)(load).await.context(LoadSnafu {
        model,
        engine: engine.name,
    })
}

/// Registers a [`LocalDeciderEngine`] as the static `$name`, as
/// [`crate::register_local_chat_engine!`] registers a chat engine.
#[macro_export]
macro_rules! register_local_decider_engine {
    ($name:ident, $engine:expr $(,)?) => {
        #[$crate::linkme::distributed_slice($crate::decider::LOCAL_DECIDER_ENGINES)]
        #[linkme(crate = $crate::linkme)]
        static $name: $crate::decider::LocalDeciderEngine = $engine;
    };
}

/// A `/v1/decisions` request as a decider asks it.
#[derive(Debug, Clone, PartialEq)]
pub struct DeciderRequest {
    pub state: Content,
    /// The questions the decider answers, in the request's order.
    pub questions: Vec<(String, strands_decider::Question)>,
    /// Answers that need no model: a choice with a single option.
    foregone: BTreeMap<String, Answer>,
    /// Each score question's levels as the request gave them, which its legend reports.
    score_levels: BTreeMap<String, Vec<NonNullEntry>>,
}

/// A request a decider cannot ask.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidQuestion(pub String);

impl DeciderRequest {
    /// The decider's questions for `request`.
    ///
    /// A choice's options reach the prompt in the request's map order, which is sorted by
    /// option name, not the order the caller wrote them in.
    ///
    /// # Errors
    ///
    /// Returns an error if a question breaks the decider's limits, such as too many options.
    pub fn new(request: &EvaluateRequest) -> Result<Self, InvalidQuestion> {
        let invalid = |id: &str, e: strands_decider::Error| InvalidQuestion(format!("{id}: {e}"));
        let mut questions = Vec::new();
        let mut foregone = BTreeMap::new();
        let mut score_levels = BTreeMap::new();
        for (id, question) in &request.questions {
            if let Some(answer) = question.foregone_answer() {
                foregone.insert(id.clone(), answer);
                continue;
            }
            let asked = match question {
                Question::Noul {
                    instructions,
                    criteria,
                } => strands_decider::Question::Noul {
                    instructions: instructions_content(instructions),
                    criteria: criteria
                        .as_ref()
                        .map(|criteria| NoulCriteria {
                            true_meaning: meaning(&criteria.true_meaning),
                            false_meaning: meaning(&criteria.false_meaning),
                        })
                        .unwrap_or_default(),
                },
                Question::Choice {
                    instructions,
                    criteria,
                } => strands_decider::Question::Choice {
                    instructions: instructions_content(instructions),
                    options: ChoiceOptions::new(
                        criteria
                            .iter()
                            .map(|(name, description)| (name.clone(), entry_content(description)))
                            .collect(),
                    )
                    .map_err(|e| invalid(id, e))?,
                },
                Question::Score {
                    instructions,
                    criteria,
                } => {
                    score_levels.insert(id.clone(), criteria.clone());
                    strands_decider::Question::Score {
                        instructions: instructions_content(instructions),
                        levels: ScoreLevels::new(
                            criteria
                                .iter()
                                .map(|level| level_text(&EntryType::from(level)))
                                .collect(),
                        )
                        .map_err(|e| invalid(id, e))?,
                    }
                }
            };
            questions.push((id.clone(), asked));
        }
        Ok(Self {
            state: match &request.state {
                EvaluateState::String(text) => Content::Text(text.clone()),
                EvaluateState::Array(items) => {
                    Content::Structured(serde_json::Value::Array(items.clone()))
                }
                EvaluateState::Object(fields) => {
                    Content::Structured(serde_json::Value::Object(fields.clone()))
                }
            },
            questions,
            foregone,
            score_levels,
        })
    }

    /// The answers when every question is foregone, which need no model; `None` when the
    /// decider has a question to answer.
    #[must_use]
    pub fn foregone_only(&self) -> Option<BTreeMap<String, Answer>> {
        self.questions.is_empty().then(|| self.foregone.clone())
    }

    /// The response for the decider's `evaluation` of this request, from the model `name`.
    #[must_use]
    pub fn response(self, name: &str, evaluation: Evaluation) -> EvaluateResponse {
        let Self {
            mut foregone,
            score_levels,
            ..
        } = self;
        for (id, answer) in evaluation.answers {
            let answer = match answer {
                strands_decider::Answer::Noul { noul } => Answer::Noul { noul },
                strands_decider::Answer::Choice {
                    choice,
                    probabilities,
                    confidence,
                } => Answer::Choice {
                    choice,
                    probabilities: probabilities.into_iter().collect(),
                    confidence,
                },
                strands_decider::Answer::Score {
                    score,
                    legend,
                    probabilities,
                    confidence,
                } => {
                    let levels = score_levels.get(&id);
                    Answer::Score {
                        score,
                        // The levels as the request gave them, as other models report them.
                        legend: legend
                            .into_iter()
                            .enumerate()
                            .map(|(i, (label, text))| {
                                let level = levels
                                    .and_then(|levels| levels.get(i))
                                    .map_or(EntryType::String(text), EntryType::from);
                                (label, level)
                            })
                            .collect(),
                        probabilities: probabilities.into_iter().collect(),
                        confidence,
                    }
                }
            };
            foregone.insert(id, answer);
        }
        EvaluateResponse {
            model: name.to_string(),
            answers: foregone,
            usage: Some(Usage {
                input_tokens: u64::try_from(evaluation.input_tokens).unwrap_or(u64::MAX),
                output_tokens: 0,
                ..Usage::default()
            }),
        }
    }
}

/// Instructions as the decider renders them; absent or null instructions render as no text.
fn instructions_content(instructions: &NullableEntry) -> Content {
    match instructions {
        NullableEntry::Absent | NullableEntry::Null => Content::Text(String::new()),
        NullableEntry::Value(entry) => {
            entry_content(entry).unwrap_or_else(|| Content::Text(String::new()))
        }
    }
}

/// A description: `None` for null.
fn entry_content(entry: &EntryType) -> Option<Content> {
    match entry {
        EntryType::String(text) => Some(Content::Text(text.clone())),
        EntryType::Array(items) => {
            Some(Content::Structured(serde_json::Value::Array(items.clone())))
        }
        EntryType::Object(fields) => Some(Content::Structured(serde_json::Value::Object(
            fields.clone(),
        ))),
        EntryType::Null => None,
    }
}

/// One side of a noul question: upstream's default text when absent, no text when null.
fn meaning(entry: &NullableEntry) -> Meaning {
    match entry {
        NullableEntry::Absent => Meaning::Default,
        NullableEntry::Null | NullableEntry::Value(EntryType::Null) => Meaning::Blank,
        NullableEntry::Value(entry) => entry_content(entry).map_or(Meaning::Blank, Meaning::Given),
    }
}

/// A score level as rubric text: its text, or structured data as indented JSON.
fn level_text(entry: &EntryType) -> String {
    strands_decider::render_content(entry_content(entry).as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::chat::gguf_header::tests::string_header;

    const CONFIG: &str = r#"{"base_model": "Qwen/Qwen3.5-2B-Base", "head_type": "pointer", "pointer_dim": 768, "max_length": 4096}"#;

    fn typed(path: &str, general_type: Option<&str>) -> (PathBuf, Option<String>) {
        (PathBuf::from(path), general_type.map(ToString::to_string))
    }

    #[test]
    fn ggufs_are_told_apart_by_their_type_not_their_names() {
        let config = Path::new("decider/strands_decider_config.json");
        // Named against their type, and listed adapter first.
        let ggufs = [
            typed("base.gguf", Some("adapter")),
            typed("lora.gguf", Some("model")),
        ];
        assert_eq!(
            choose_ggufs("triage", config, &ggufs, "").expect("one of each"),
            (PathBuf::from("lora.gguf"), PathBuf::from("base.gguf"))
        );
        // A file with no type is a model; another type is neither.
        let ggufs = [
            typed("old-base.gguf", None),
            typed("mmproj.gguf", Some("mmproj")),
            typed("adapter.gguf", Some("adapter")),
        ];
        assert_eq!(
            choose_ggufs("triage", config, &ggufs, "").expect("one of each"),
            (
                PathBuf::from("old-base.gguf"),
                PathBuf::from("adapter.gguf")
            )
        );
    }

    #[test]
    fn a_missing_or_repeated_gguf_is_refused() {
        let config = Path::new("strands_decider_config.json");
        let err = choose_ggufs(
            "triage",
            config,
            &[typed("adapter.gguf", Some("adapter"))],
            "",
        )
        .expect_err("no base");
        assert!(matches!(err, Error::NoBaseGguf { .. }), "{err}");
        assert!(err.to_string().contains("no base model GGUF"), "{err}");

        let err = choose_ggufs("triage", config, &[typed("base.gguf", Some("model"))], "")
            .expect_err("no adapter");
        assert!(matches!(err, Error::NoAdapterGguf { .. }), "{err}");
        assert!(err.to_string().contains("no LoRA adapter GGUF"), "{err}");

        let err = choose_ggufs(
            "triage",
            config,
            &[
                typed("base-F16.gguf", Some("model")),
                typed("base-Q8_0.gguf", Some("model")),
                typed("adapter.gguf", Some("adapter")),
            ],
            "",
        )
        .expect_err("two bases");
        assert!(
            matches!(
                err,
                Error::SeveralGgufs {
                    kind: GgufKind::Model,
                    ..
                }
            ),
            "{err}"
        );
        assert!(err.is_configuration_error());
    }

    #[test]
    fn a_safetensors_adapter_is_named_in_the_missing_gguf_error() {
        let hint = other_weights_hint(
            [
                "strands_decider_config.json",
                "head.safetensors",
                "lora/adapter_model.safetensors",
            ]
            .into_iter(),
        );
        assert!(hint.contains("lora/adapter_model.safetensors"), "{hint}");
        assert!(!hint.contains("head.safetensors,"), "{hint}");
        assert!(hint.contains("16198"), "{hint}");
        assert_eq!(other_weights_hint(["head.safetensors"].into_iter()), "");
    }

    /// The files of a decider as a Spicepod lists them, with the config under `name`.
    fn decider_dir(name: &str, config: &str) -> (tempfile::TempDir, Vec<PathBuf>) {
        let dir = tempfile::tempdir().expect("a temp dir");
        let write = |file: &str, bytes: &[u8]| {
            let path = dir.path().join(file);
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("dirs");
            std::fs::write(&path, bytes).expect("written");
            path
        };
        let files = vec![
            write(
                "Qwen3.5-2B-Base-F16.gguf",
                &string_header(&[
                    ("general.architecture", "qwen35"),
                    ("general.type", "model"),
                ]),
            ),
            write(
                "decider-lora-F32.gguf",
                &string_header(&[
                    ("general.architecture", "qwen35"),
                    ("general.type", "adapter"),
                ]),
            ),
            write(&format!("decider/{name}"), config.as_bytes()),
            write("decider/head.safetensors", b"not read here"),
            write("decider/tokenizer.json", b"{\"model\": {}}"),
        ];
        (dir, files)
    }

    #[tokio::test]
    async fn every_config_name_is_detected_by_content() {
        for name in [
            "strands_decider_config.json",
            "hobson_config.json",
            "head_config.json",
            "anything.json",
        ] {
            let (dir, files) = decider_dir(name, CONFIG);
            let found = detect_local("triage", &files)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"))
                .unwrap_or_else(|| panic!("{name} is a decider"));
            assert_eq!(
                found,
                DeciderFiles {
                    base: dir.path().join("Qwen3.5-2B-Base-F16.gguf"),
                    adapter: dir.path().join("decider-lora-F32.gguf"),
                    config: dir.path().join("decider").join(name),
                }
            );
        }
        // `save_assets` names the readout instead of the head type.
        let (_dir, files) = decider_dir(
            "head_config.json",
            r#"{"readout": "pointer", "pointer_dim": 768}"#,
        );
        assert!(
            detect_local("triage", &files)
                .await
                .expect("a decider")
                .is_some()
        );
    }

    #[tokio::test]
    async fn a_training_recipe_beside_the_config_is_passed_over() {
        let (dir, mut files) = decider_dir("strands_decider_config.json", CONFIG);
        let recipe = dir.path().join("decider/train_config.json");
        std::fs::write(
            &recipe,
            r#"{"head_type": "pointer", "pointer_dim": 256, "train_files": ["train.jsonl"]}"#,
        )
        .expect("written");
        files.push(recipe);
        let found = detect_local("triage", &files)
            .await
            .expect("one decider config")
            .expect("a decider");
        assert_eq!(
            found.config,
            dir.path().join("decider/strands_decider_config.json")
        );
    }

    #[tokio::test]
    async fn a_chat_model_is_not_a_decider() {
        // A GGUF chat model with a tokenizer config, and one with only its GGUF.
        let (_dir, mut files) = decider_dir("tokenizer_config.json", r#"{"bos_token": null}"#);
        assert_eq!(detect_local("chat", &files).await.expect("readable"), None);
        files.retain(|path| path.extension().is_some_and(|ext| ext == "gguf"));
        assert_eq!(detect_local("chat", &files).await.expect("readable"), None);
    }

    #[tokio::test]
    async fn an_unservable_decider_is_refused_not_loaded_as_chat() {
        let (_dir, files) = decider_dir(
            "strands_decider_config.json",
            r#"{"head_type": "slot", "pointer_dim": 768}"#,
        );
        let err = detect_local("triage", &files)
            .await
            .expect_err("a slot head");
        assert!(matches!(err, Error::UnsupportedDecider { .. }), "{err}");
        assert!(err.to_string().contains("head_type"), "{err}");

        // The adapter only as safetensors, as the Hugging Face checkpoint publishes it.
        let (_dir, mut files) = decider_dir("strands_decider_config.json", CONFIG);
        files.retain(|path| !path.ends_with("decider-lora-F32.gguf"));
        let err = detect_local("triage", &files)
            .await
            .expect_err("no adapter");
        assert!(matches!(err, Error::NoAdapterGguf { .. }), "{err}");
    }

    fn request(questions: &serde_json::Value) -> EvaluateRequest {
        serde_json::from_value(json!({
            "model": "triage",
            "state": "Help! My payouts have been failing for 3 days! ",
            "questions": questions,
        }))
        .expect("a valid request")
    }

    #[test]
    fn questions_map_to_the_deciders() {
        let asked = DeciderRequest::new(&request(&json!({
            "urgent": {"type": "noul", "instructions": "Does this convey urgency?"},
            "team": {
                "type": "choice",
                "instructions": "Which team should handle this?",
                "criteria": {"billing": "", "sales": null, "retail": {"desk": 3}}
            },
            "mood": {
                "type": "score",
                "instructions": "How frustrated is the writer?",
                "criteria": ["calm", "frustrated", {"level": "depressed"}]
            },
            "only": {"type": "choice", "criteria": {"one": "the only option"}},
            "blank": {
                "type": "noul",
                "criteria": {"true": null, "false": "it does not hold"}
            }
        })))
        .expect("valid questions");

        assert_eq!(
            asked.state,
            Content::Text("Help! My payouts have been failing for 3 days! ".into())
        );
        let questions: BTreeMap<_, _> = asked.questions.iter().cloned().collect();
        assert_eq!(
            questions.get("urgent"),
            Some(&strands_decider::Question::Noul {
                instructions: "Does this convey urgency?".into(),
                criteria: NoulCriteria::default(),
            })
        );
        // Options reach the prompt sorted by name.
        assert_eq!(
            questions.get("team"),
            Some(&strands_decider::Question::Choice {
                instructions: "Which team should handle this?".into(),
                options: ChoiceOptions::new(vec![
                    ("billing".into(), Some("".into())),
                    (
                        "retail".into(),
                        Some(Content::Structured(json!({"desk": 3})))
                    ),
                    ("sales".into(), None),
                ])
                .expect("valid options"),
            })
        );
        assert_eq!(
            questions.get("mood"),
            Some(&strands_decider::Question::Score {
                instructions: "How frustrated is the writer?".into(),
                levels: ScoreLevels::new(vec![
                    "calm".into(),
                    "frustrated".into(),
                    "{\n  \"level\": \"depressed\"\n}".into(),
                ])
                .expect("valid levels"),
            })
        );
        assert_eq!(
            questions.get("blank"),
            Some(&strands_decider::Question::Noul {
                instructions: Content::Text(String::new()),
                criteria: NoulCriteria {
                    true_meaning: Meaning::Blank,
                    false_meaning: Meaning::Given("it does not hold".into()),
                },
            })
        );
        // A single-option choice is answered without the model.
        assert!(!questions.contains_key("only"));
        assert_eq!(asked.foregone_only(), None);
    }

    #[test]
    fn a_question_past_the_deciders_limits_is_invalid() {
        let options: serde_json::Map<String, serde_json::Value> = (0
            ..=strands_decider::MAX_CHOICE_OPTIONS)
            .map(|i| (format!("option{i}"), json!(null)))
            .collect();
        let err = DeciderRequest::new(&request(&json!({
            "many": {"type": "choice", "criteria": options}
        })))
        .expect_err("too many options");
        assert!(err.0.starts_with("many: "), "{}", err.0);
    }

    #[test]
    fn answers_map_back_with_the_requests_levels_and_usage() {
        let asked = DeciderRequest::new(&request(&json!({
            "mood": {"type": "score", "criteria": ["calm", {"level": "upset"}]},
            "only": {"type": "choice", "criteria": {"one": null}},
        })))
        .expect("valid questions");
        let response = asked.response(
            "triage",
            Evaluation {
                answers: vec![(
                    "mood".into(),
                    strands_decider::Answer::Score {
                        score: 0.25,
                        legend: vec![
                            ("0".into(), "calm".into()),
                            ("1".into(), "{ \"level\": \"upset\" }".into()),
                        ],
                        probabilities: vec![("0".into(), 0.75), ("1".into(), 0.25)],
                        confidence: 0.5,
                    },
                )],
                input_tokens: 42,
            },
        );
        assert_eq!(response.model, "triage");
        assert_eq!(
            response.answers.get("mood"),
            Some(&Answer::Score {
                score: 0.25,
                legend: BTreeMap::from([
                    ("0".into(), EntryType::String("calm".into())),
                    (
                        "1".into(),
                        EntryType::Object(
                            json!({"level": "upset"})
                                .as_object()
                                .cloned()
                                .expect("an object")
                        )
                    ),
                ]),
                probabilities: BTreeMap::from([("0".into(), 0.75), ("1".into(), 0.25)]),
                confidence: 0.5,
            })
        );
        assert!(matches!(
            response.answers.get("only"),
            Some(Answer::Choice { choice, .. }) if choice == "one"
        ));
        assert_eq!(
            response
                .usage
                .map(|usage| (usage.input_tokens, usage.output_tokens)),
            Some((42, 0))
        );
    }
}
