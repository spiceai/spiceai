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

// The contract lives in `chat-api`, below every provider; re-exported so existing
// `llms::chat::…` paths resolve, including SNAFU's generated context selectors.
pub use chat_api::*;
#[cfg(feature = "local_llm")]
use snafu::ResultExt;
use std::path::Path;
#[cfg(any(feature = "local_llm", test))]
use std::path::PathBuf;
#[cfg(feature = "local_llm")]
use std::str::FromStr;
#[cfg(feature = "local_llm")]
use std::sync::Arc;

// Only `message_to_mistral` below still names these, and it is `local_llm`-gated: every
// other user moved to `chat-api` with the `Chat` trait.
#[cfg(feature = "local_llm")]
use async_openai::types::chat::{
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestDeveloperMessage, ChatCompletionRequestDeveloperMessageContent,
    ChatCompletionRequestDeveloperMessageContentPart, ChatCompletionRequestFunctionMessage,
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestToolMessage, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, ReasoningEffort,
};

#[cfg(feature = "local_llm")]
pub mod engine;
#[cfg(feature = "local_llm")]
pub mod gguf_files;
#[cfg(feature = "local_llm")]
mod gguf_header;
#[cfg(feature = "local_llm")]
pub mod hf_files;
#[cfg(feature = "local_llm")]
pub mod mistral;
pub mod nsql;
#[cfg(feature = "local_llm")]
mod template_content;
#[cfg(feature = "local_llm")]
pub use crate::chat::distributed::{DistributedBackend, DistributedConfig};
#[cfg(feature = "local_llm")]
use crate::chat::distributed::{configure_ring_distributed, ensure_distributed_build};
#[cfg(feature = "local_llm")]
pub use crate::chat::template_content::ContentSupport;
#[cfg(feature = "local_llm")]
use indexmap::IndexMap;
#[cfg(feature = "local_llm")]
use mistralrs::MessageContent;

// Distributed inference only matters for local mistral.rs models, so the module
// (and its `configure_ring_distributed` helper) is gated on `local_llm` to avoid
// dead-code in `--no-default-features` / feature-matrix builds.
#[cfg(feature = "local_llm")]
pub mod distributed;

// The attention-implementation knob is plain config with no engine dependency, so it
// stays ungated: the model parameter spec compiles unconditionally and shares its
// accepted values.
mod paged_attention;
pub use crate::chat::paged_attention::PagedAttentionMode;

// Whether pickle-format weights are trusted is plain config with no engine
// dependency, so it stays ungated alongside `paged_attention` above.
mod pickle_trust;
pub use crate::chat::pickle_trust::PickleTrust;

// The `distributed_backend` setting itself is plain config; kept ungated and
// separate from the `local_llm`-gated `distributed` module above.
mod distributed_backend_setting;
pub use crate::chat::distributed_backend_setting::DistributedBackendSetting;

static WEIGHTS_EXTENSIONS: [&str; 7] = [
    ".safetensors",
    ".pth",
    ".pt",
    ".bin",
    ".onyx",
    ".gguf",
    ".ggml",
];

/// Attempts to string match a model error to a known error type.
/// Returns None if no match is found.
#[must_use]
pub fn try_map_boxed_error(e: &(dyn std::error::Error + Send + Sync)) -> Option<Error> {
    let err_string = e.to_string().to_ascii_lowercase();
    if err_string.contains("expected file with extension")
        && WEIGHTS_EXTENSIONS
            .iter()
            .any(|ext| err_string.contains(ext))
    {
        Some(Error::ModelMissingWeights {
            extensions: WEIGHTS_EXTENSIONS.join(", "),
        })
    } else if err_string.contains("hf api error") && err_string.contains("status: 404") {
        let file_url = err_string
            .split("url: ")
            .last()
            .map(|url| {
                url.split(' ')
                    .next()
                    .unwrap_or_default()
                    .replace([']', ')'], "")
            })
            .unwrap_or_default();

        if file_url.is_empty() {
            None
        } else {
            Some(Error::ModelFileMissing { file_url })
        }
    } else {
        None
    }
}

/// Re-writes a boxed error to a known error type, if possible.
/// Always returns a boxed error. Returns the original error if no match is found.
#[must_use]
pub fn try_map_boxed_error_to_box(
    e: Box<dyn std::error::Error + Send + Sync>,
) -> Box<dyn std::error::Error + Send + Sync> {
    try_map_boxed_error(&*e).map_or_else(|| e, std::convert::Into::into)
}

/// A part of a user message's content that a local model cannot read: the engines give its
/// chat template only text.
#[cfg(feature = "local_llm")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedContentPart {
    /// The part's `type`, as the request names it.
    pub part: &'static str,
    /// The input the part carries, as a model that accepts it would name it.
    pub input: &'static str,
}

#[cfg(feature = "local_llm")]
impl UnsupportedContentPart {
    /// Why a request to `model` that carries this part is refused.
    #[must_use]
    pub fn refusal(&self, model: &str) -> String {
        format!(
            "Model '{model}' reads only text, so it cannot take the `{}` part of a user message. Send the message's content as text, or use a model that accepts {} input.",
            self.part, self.input
        )
    }
}

/// Converts a [`ChatCompletionRequestMessage`] to the message a chat template renders, as the
/// local engines render it. Content sent as an array becomes text parts,
/// `[{"type": "text", "text": …}]`, which [`ContentSupport::rewrite`] then fits to the
/// template; an empty array is empty text, as llama-server takes it.
///
/// # Errors
///
/// A user message's part other than text: an image, audio or a file, which no local engine
/// passes to its model.
#[cfg(feature = "local_llm")]
pub fn message_to_mistral(
    message: &ChatCompletionRequestMessage,
) -> std::result::Result<IndexMap<String, MessageContent>, UnsupportedContentPart> {
    use async_openai::types::chat::{
        ChatCompletionMessageToolCalls, ChatCompletionRequestAssistantMessageContentPart,
        ChatCompletionRequestSystemMessageContent, ChatCompletionRequestSystemMessageContentPart,
        ChatCompletionRequestToolMessageContent, ChatCompletionRequestToolMessageContentPart,
        ChatCompletionRequestUserMessageContentPart,
    };
    use either::Either;
    use serde_json::Value;

    let role = |role: &str| (String::from("role"), Either::Left(role.to_string()));
    let text = |text: &str| Either::Left(text.to_string());

    Ok(match message {
        ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
            content, ..
        }) => {
            let content = match content {
                ChatCompletionRequestUserMessageContent::Text(t) => text(t),
                ChatCompletionRequestUserMessageContent::Array(parts) => {
                    let texts = parts
                        .iter()
                        .map(|part| match part {
                            ChatCompletionRequestUserMessageContentPart::Text(t) => {
                                Ok(t.text.as_str())
                            }
                            ChatCompletionRequestUserMessageContentPart::ImageUrl(_) => {
                                Err(UnsupportedContentPart {
                                    part: "image_url",
                                    input: "image",
                                })
                            }
                            ChatCompletionRequestUserMessageContentPart::InputAudio(_) => {
                                Err(UnsupportedContentPart {
                                    part: "input_audio",
                                    input: "audio",
                                })
                            }
                            ChatCompletionRequestUserMessageContentPart::File(_) => {
                                Err(UnsupportedContentPart {
                                    part: "file",
                                    input: "file",
                                })
                            }
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    parts_content(&texts)
                }
            };
            IndexMap::from([role("user"), (String::from("content"), content)])
        }
        ChatCompletionRequestMessage::Developer(ChatCompletionRequestDeveloperMessage {
            content,
            ..
        }) => {
            let content = match content {
                ChatCompletionRequestDeveloperMessageContent::Text(t) => text(t),
                ChatCompletionRequestDeveloperMessageContent::Array(parts) => parts_content(
                    &parts
                        .iter()
                        .map(
                            |ChatCompletionRequestDeveloperMessageContentPart::Text(t)| {
                                t.text.as_str()
                            },
                        )
                        .collect::<Vec<_>>(),
                ),
            };
            IndexMap::from([role("developer"), (String::from("content"), content)])
        }
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content,
            ..
        }) => {
            let content = match content {
                ChatCompletionRequestSystemMessageContent::Text(t) => text(t),
                ChatCompletionRequestSystemMessageContent::Array(parts) => parts_content(
                    &parts
                        .iter()
                        .map(|ChatCompletionRequestSystemMessageContentPart::Text(t)| {
                            t.text.as_str()
                        })
                        .collect::<Vec<_>>(),
                ),
            };
            IndexMap::from([role("system"), (String::from("content"), content)])
        }
        ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            content,
            tool_call_id,
        }) => {
            let content = match content {
                ChatCompletionRequestToolMessageContent::Text(t) => text(t),
                ChatCompletionRequestToolMessageContent::Array(parts) => parts_content(
                    &parts
                        .iter()
                        .map(|ChatCompletionRequestToolMessageContentPart::Text(t)| t.text.as_str())
                        .collect::<Vec<_>>(),
                ),
            };
            IndexMap::from([
                role("tool"),
                (String::from("content"), content),
                (String::from("tool_call_id"), text(tool_call_id)),
            ])
        }
        ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
            content,
            name,
            tool_calls,
            ..
        }) => {
            let content = match content {
                Some(ChatCompletionRequestAssistantMessageContent::Text(t)) => text(t),
                // A refusal is what the assistant said, so the template reads it as its text.
                Some(ChatCompletionRequestAssistantMessageContent::Array(parts)) => parts_content(
                    &parts
                        .iter()
                        .map(|part| match part {
                            ChatCompletionRequestAssistantMessageContentPart::Text(t) => {
                                t.text.as_str()
                            }
                            ChatCompletionRequestAssistantMessageContentPart::Refusal(r) => {
                                r.refusal.as_str()
                            }
                        })
                        .collect::<Vec<_>>(),
                ),
                // Empty text, not none, which more open-source chat templates accept.
                None => text(""),
            };
            let mut map = IndexMap::from([role("assistant"), (String::from("content"), content)]);
            if let Some(name) = name {
                map.insert("name".to_string(), text(name));
            }
            if let Some(tool_calls) = tool_calls {
                let tool_call_results: Vec<IndexMap<String, Value>> = tool_calls
                    .iter()
                    .filter_map(|t| {
                        let ChatCompletionMessageToolCalls::Function(func_call) = t else {
                            return None;
                        };
                        let Ok(function) = serde_json::to_value(&func_call.function) else {
                            tracing::warn!("Invalid function call: {:#?}", func_call.function);
                            return None;
                        };

                        let mut map = IndexMap::new();
                        map.insert("id".to_string(), Value::String(func_call.id.clone()));
                        map.insert("function".to_string(), function);
                        map.insert("type".to_string(), Value::String("function".to_string()));

                        Some(map)
                    })
                    .collect();

                map.insert("tool_calls".to_string(), Either::Right(tool_call_results));
            }
            map
        }
        ChatCompletionRequestMessage::Function(ChatCompletionRequestFunctionMessage {
            content,
            name,
        }) => IndexMap::from([
            role("function"),
            (
                "content".to_string(),
                text(content.as_deref().unwrap_or_default()),
            ),
            ("name".to_string(), text(name)),
        ]),
    })
}

/// A message's content sent as an array of text parts: the parts, or empty text when there are
/// none, as llama-server takes an empty array.
#[cfg(feature = "local_llm")]
fn parts_content(texts: &[&str]) -> MessageContent {
    if texts.is_empty() {
        either::Either::Left(String::new())
    } else {
        template_content::text_parts(texts.iter().copied())
    }
}

#[cfg(feature = "local_llm")]
use crate::hf::HubParams;

/// Create a model to run locally, from a Hugging Face model repository.
///
/// `model_id` names the repository, `org/name`, with an optional `:revision`. Spice downloads
/// the files the engine loads into the Hugging Face cache ([`hf_files`]), and the engine loads
/// the cache's snapshot directory: it never downloads anything itself.
/// `model_type` is the type of model, if needed to be explicit. Often this can
///    be inferred from the `.model_type` key in a HF's `config.json`, or from the GGUF metadata.
/// `files` are the model's `files`, as the Spicepod lists them: GGUF files within the repo,
///    or a quantization by name (`UD-Q4_K_XL`), resolved against the repo
///    ([`gguf_files::resolve`]); with none, a GGUF repo's largest quantization that fits the
///    memory the engine reports. A registered engine that serves the GGUF files gets them all,
///    every shard and any draft head; mistral.rs loads the first.
/// `hub` is the model's `hf_token` and `hf_endpoint`.
/// `distributed` optionally runs the model tensor-parallel across multiple nodes.
/// `reasoning` names the model and its `reasoning_effort` default for the load-time check.
#[cfg(feature = "local_llm")]
pub async fn create_hf_model(
    model_id: &str,
    model_type: Option<&str>,
    files: &[&str],
    hub: HubParams<'_>,
    chat_template_literal: Option<&str>,
    distributed: Option<DistributedConfig>,
    reasoning: ReasoningDefault<'_>,
) -> Result<Arc<dyn Chat>> {
    let download_error = |error: huggingface_hub::DownloadError| Error::FailedToDownloadModel {
        configuration: error.is_configuration_error(),
        source: Box::new(error),
    };
    // Every node of a split model resolves the files itself, and each would choose by its own
    // memory, so a split model takes only a repository's one quantization: every node must name
    // the same files.
    let budget = if distributed.is_some() {
        hf_files::Budget::Split
    } else {
        engine::free_memory().map_or(hf_files::Budget::Unmeasured, hf_files::Budget::Free)
    };
    // A rank other than 0 of a model an engine splits across nodes never reads the weights (the
    // head sends each rank its share), so it doesn't download them. It resolves the files from
    // the Hub's listing and reads the first shard's header instead, to decide as the head does.
    if let Some(cfg) = distributed.as_ref()
        && cfg.node_rank > 0
    {
        let resolved =
            hf_files::model_architecture(model_id, files, budget, hub, reasoning.model_name)
                .await
                .map_err(download_error)?;
        let gguf_files: Vec<PathBuf> = resolved.gguf.iter().map(PathBuf::from).collect();
        if let Some(engine) = engine::engine_for(&gguf_files)
            && engine.distributed.is_some()
            && engine.splits(resolved.architecture.as_deref())
        {
            ensure_distributed_build()?;
            let options = LocalModelOptions {
                reasoning,
                distributed: Some(cfg),
                ..LocalModelOptions::default()
            };
            return (engine.load)(&gguf_files, options).await;
        }
    }
    let hf_files::Downloaded { dir, gguf } =
        hf_files::download_model(model_id, files, budget, hub, reasoning.model_name)
            .await
            .map_err(download_error)?;
    let gguf_files: Vec<PathBuf> = gguf.iter().map(PathBuf::from).collect();
    let weights: Vec<PathBuf> = gguf_files.iter().map(|file| dir.join(file)).collect();
    if let Some(engine) = engine::engine_for(&gguf_files)
        && (distributed.is_none() || engine.splits_weights(&weights).await)
    {
        if distributed.is_some() {
            ensure_distributed_build()?;
        }
        let template_file = hf_files::chat_template_file(&dir).await;
        let options = LocalModelOptions {
            chat_template_literal,
            chat_template_file: template_file.as_deref(),
            reasoning,
            distributed: distributed.as_ref(),
            ..LocalModelOptions::default()
        };
        return (engine.load)(&weights, options).await;
    }
    // Configure multi-node distributed (ring) inference before loading: the
    // loader reads `RING_CONFIG` from the environment while building the
    // pipeline. The returned guard keeps the temp file alive for the model.
    let ring_config = if let Some(cfg) = distributed {
        Some(configure_ring_distributed(&cfg)?)
    } else {
        // Defensive: clear any `RING_CONFIG` left set by a prior distributed
        // load in this process, so this single-node load can't accidentally
        // run distributed.
        // SAFETY: touched only during model init, before mistral.rs reads it.
        unsafe { std::env::remove_var("RING_CONFIG") };
        None
    };
    mistral::MistralLlama::from_snapshot(
        &dir,
        model_type,
        gguf.first().map(String::as_str),
        chat_template_literal,
        ring_config,
        reasoning,
    )
    .await
    .map(|x| Arc::new(x) as Arc<dyn Chat>)
}

/// Knobs pushed into the loader for a locally served model. Grouped so that adding one is
/// a field rather than another positional argument on two signatures and every call site —
/// and so a caller cannot transpose two of the several optional arguments and still
/// type-check. Multi-node topology is deliberately not here: it is a side effect applied
/// before the loader runs, not something the loader reads.
#[cfg(feature = "local_llm")]
#[derive(Debug, Default, Clone, Copy)]
pub struct LocalModelOptions<'a> {
    /// Overrides the chat template; ignored for GGUF when the file carries its own.
    pub chat_template_literal: Option<&'a str>,
    /// The model's `tokenizer_config.json`, or a `.jinja` file, whose chat template wins over a
    /// GGUF's own, as it does on the mistral.rs engine.
    pub chat_template_file: Option<&'a Path>,
    /// Sequence-length budget for layer placement and KV-cache sizing.
    pub context_length: Option<usize>,
    /// Attention implementation to request from the engine.
    pub paged_attention: PagedAttentionMode,
    /// The model's name and `reasoning_effort` default, checked against its chat template.
    pub reasoning: ReasoningDefault<'a>,
    /// The nodes the model is split across, for an engine that serves distributed models
    /// ([`engine::LocalChatEngine::distributed`]); mistral.rs reads its ring from the environment.
    pub distributed: Option<&'a DistributedConfig>,
}

/// A local model's Spicepod name and its `reasoning_effort` default. The loader checks the
/// default against the model's chat template, so a default the template can't honor fails
/// the load instead of every request. The name is what the refusal names.
#[cfg(feature = "local_llm")]
#[derive(Debug, Default, Clone, Copy)]
pub struct ReasoningDefault<'a> {
    pub model_name: &'a str,
    pub effort: Option<&'a ReasoningEffort>,
}

#[cfg(feature = "local_llm")]
pub async fn create_local_model(
    model_weights: &[String],
    config: Option<&str>,
    tokenizer: Option<&str>,
    tokenizer_config: Option<&str>,
    generation_config: Option<&str>,
    distributed: Option<DistributedConfig>,
    options: LocalModelOptions<'_>,
) -> Result<Arc<dyn Chat>> {
    // A model goes to a registered engine that serves its weights; a distributed one only when
    // that engine splits models of its architecture, and to mistral.rs's ring otherwise.
    let weights = model_weights.iter().map(PathBuf::from).collect::<Vec<_>>();
    let engine = match engine::engine_for(&weights) {
        Some(engine) => Some(engine),
        None if distributed.as_ref().is_some_and(|cfg| cfg.node_rank > 0) => {
            engine::engine_without_weights(&weights).await
        }
        None => None,
    };
    if let Some(engine) = engine
        && (distributed.is_none() || engine.splits_weights(&weights).await)
    {
        if distributed.is_some() {
            ensure_distributed_build()?;
        }
        let options = LocalModelOptions {
            chat_template_file: tokenizer_config.map(Path::new),
            distributed: distributed.as_ref(),
            ..options
        };
        return (engine.load)(&weights, options).await;
    }
    // Configure multi-node distributed (ring) inference before loading: the
    // loader reads `RING_CONFIG` from the environment while building the
    // pipeline. The returned guard keeps the temp file alive for the model.
    let ring_config = if let Some(cfg) = distributed {
        Some(configure_ring_distributed(&cfg)?)
    } else {
        // Defensive: clear any `RING_CONFIG` left set by a prior distributed
        // load in this process, so this single-node load can't accidentally
        // run distributed.
        // SAFETY: touched only during model init, before mistral.rs reads it.
        unsafe { std::env::remove_var("RING_CONFIG") };
        None
    };
    mistral::MistralLlama::from(
        model_weights
            .iter()
            .map(|p| PathBuf::from_str(p))
            .collect::<Result<Vec<_>, _>>()
            .boxed()
            .map_err(|e| Error::FailedToLoadModel { source: e })?
            .as_slice(),
        config.map(Path::new),
        tokenizer.map(Path::new),
        tokenizer_config.map(Path::new),
        generation_config.map(Path::new),
        options,
        ring_config,
    )
    .await
    .map(|x| Arc::new(x) as Arc<dyn Chat>)
}

/// File extensions that are conventionally Python pickle by `PyTorch`'s
/// ecosystem and that are unsafe to load from any source the operator
/// does not fully trust. Pickle deserialization is RCE by design.
const PICKLE_WEIGHT_EXTENSIONS: &[&str] = &["bin", "pt", "pth", "ckpt"];

/// Reject any weight path whose extension lands in [`PICKLE_WEIGHT_EXTENSIONS`]
/// unless the caller has explicitly opted in via `trust_pickle = true`.
///
/// `weights` is expected to be the same slice of paths that will be handed
/// to the model loader. Returns [`Error::UnsafePickleWeight`] on the first
/// match, so the message identifies the offending file.
///
/// # Errors
///
/// Returns [`Error::UnsafePickleWeight`] when `trust_pickle` is `false`
/// and any path has a pickle-class extension.
pub fn reject_unsafe_weight_formats<P: AsRef<Path>>(
    weights: &[P],
    trust_pickle: bool,
) -> Result<()> {
    if trust_pickle {
        return Ok(());
    }
    for w in weights {
        let path = w.as_ref();
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            let ext = ext.to_ascii_lowercase();
            if PICKLE_WEIGHT_EXTENSIONS.contains(&ext.as_str()) {
                return Err(Error::UnsafePickleWeight {
                    path: path.to_string_lossy().into_owned(),
                    extension: ext,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_pickle_extensions_by_default() {
        for ext in PICKLE_WEIGHT_EXTENSIONS {
            let path = PathBuf::from(format!("/models/pytorch_model.{ext}"));
            let err = reject_unsafe_weight_formats(&[path], false)
                .expect_err(&format!("expected rejection for .{ext}"));
            assert!(matches!(err, Error::UnsafePickleWeight { .. }));
        }
    }

    #[test]
    fn allows_safe_extensions() {
        for ext in ["safetensors", "gguf", "ggml", "onnx"] {
            let path = PathBuf::from(format!("/models/weights.{ext}"));
            reject_unsafe_weight_formats(&[path], false)
                .unwrap_or_else(|e| panic!("expected ok for .{ext}, got {e:?}"));
        }
    }

    #[test]
    fn opt_in_allows_pickle_extensions() {
        let paths = [
            PathBuf::from("/m/a.pt"),
            PathBuf::from("/m/pytorch_model.bin"),
        ];
        reject_unsafe_weight_formats(&paths, true).expect("opt-in should allow pickle");
    }

    #[test]
    fn extension_check_is_case_insensitive() {
        let path = PathBuf::from("/m/Pytorch_Model.PT");
        let err = reject_unsafe_weight_formats(&[path], false)
            .expect_err("case-insensitive .PT must be rejected");
        match err {
            Error::UnsafePickleWeight { extension, .. } => assert_eq!(extension, "pt"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn extensionless_path_is_ignored() {
        let path = PathBuf::from("/m/no_extension_here");
        reject_unsafe_weight_formats(&[path], false).expect("no extension → no rejection");
    }
}
