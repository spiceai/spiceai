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
use secrecy::SecretString;
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
    ChatCompletionRequestUserMessageContent,
};

#[cfg(feature = "local_llm")]
pub mod mistral;
pub mod nsql;
#[cfg(feature = "local_llm")]
use crate::chat::distributed::{clear_distributed_env, configure_distributed};
#[cfg(feature = "local_llm")]
pub use crate::chat::distributed::{DistributedBackend, DistributedConfig};
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

/// Convert a structured [`ChatCompletionRequestMessage`] to the mistral.rs compatible [`RequestMessage`] type.
#[cfg(feature = "local_llm")]
#[must_use]
pub fn message_to_mistral(
    message: &ChatCompletionRequestMessage,
) -> IndexMap<String, MessageContent> {
    use async_openai::types::chat::{
        ChatCompletionMessageToolCalls, ChatCompletionRequestSystemMessageContent,
        ChatCompletionRequestToolMessageContent,
    };
    use either::Either;
    use serde_json::{Value, json};

    match message {
        ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
            content, ..
        }) => {
            let body: MessageContent = match content {
                ChatCompletionRequestUserMessageContent::Text(text) => {
                    either::Either::Left(text.clone())
                }
                ChatCompletionRequestUserMessageContent::Array(array) => {
                    let index_map = array.iter().map(|p| {
                        match p {
                            async_openai::types::chat::ChatCompletionRequestUserMessageContentPart::Text(t) => {
                                ("content".to_string(), Value::String(t.text.clone()))
                            }
                            async_openai::types::chat::ChatCompletionRequestUserMessageContentPart::ImageUrl(i) => {
                                ("image_url".to_string(), Value::String(i.image_url.url.clone()))
                            }
                            async_openai::types::chat::ChatCompletionRequestUserMessageContentPart::InputAudio(a) => {
                                ("input_audio".to_string(), Value::String(a.input_audio.data.clone()))
                            }
                            async_openai::types::chat::ChatCompletionRequestUserMessageContentPart::File(f) => {
                                ("file".to_string(), serde_json::to_value(&f.file).unwrap_or_default())
                            }
                        }

                    }).collect();
                    either::Either::Right(vec![index_map])
                }
            };
            IndexMap::from([
                (String::from("role"), Either::Left(String::from("user"))),
                (String::from("content"), body),
            ])
        }
        ChatCompletionRequestMessage::Developer(ChatCompletionRequestDeveloperMessage {
            content: ChatCompletionRequestDeveloperMessageContent::Text(text),
            ..
        }) => IndexMap::from([
            (
                String::from("role"),
                Either::Left(String::from("developer")),
            ),
            (String::from("content"), Either::Left(text.clone())),
        ]),
        ChatCompletionRequestMessage::Developer(ChatCompletionRequestDeveloperMessage {
            content: ChatCompletionRequestDeveloperMessageContent::Array(parts),
            ..
        }) => {
            // TODO: This will cause issue for some chat_templates. Tracking: https://github.com/EricLBuehler/mistral.rs/issues/793
            let content_json = parts
                .iter()
                .map(|p| {
                    let ChatCompletionRequestDeveloperMessageContentPart::Text(t) = p;
                    t.text.clone()
                })
                .collect::<Vec<_>>();
            IndexMap::from([
                (
                    String::from("role"),
                    Either::Left(String::from("developer")),
                ),
                (
                    String::from("content"),
                    Either::Left(json!(content_json).to_string()),
                ),
            ])
        }
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content: ChatCompletionRequestSystemMessageContent::Text(text),
            ..
        }) => IndexMap::from([
            (String::from("role"), Either::Left(String::from("system"))),
            (String::from("content"), Either::Left(text.clone())),
        ]),
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content: ChatCompletionRequestSystemMessageContent::Array(parts),
            ..
        }) => {
            // TODO: This will cause issue for some chat_templates. Tracking: https://github.com/EricLBuehler/mistral.rs/issues/793
            let content_json = parts
                .iter()
                .map(|p| match p {
                    async_openai::types::chat::ChatCompletionRequestSystemMessageContentPart::Text(t) => {
                        ("text".to_string(), t.text.clone())
                    }
                })
                .collect::<Vec<_>>();
            IndexMap::from([
                (String::from("role"), Either::Left(String::from("system"))),
                (
                    String::from("content"),
                    Either::Left(json!(content_json).to_string()),
                ),
            ])
        }
        ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            content: ChatCompletionRequestToolMessageContent::Text(text),
            tool_call_id,
        }) => IndexMap::from([
            (String::from("role"), Either::Left(String::from("tool"))),
            (String::from("content"), Either::Left(text.clone())),
            (
                String::from("tool_call_id"),
                Either::Left(tool_call_id.clone()),
            ),
        ]),
        ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            content: ChatCompletionRequestToolMessageContent::Array(parts),
            tool_call_id,
        }) => {
            // TODO: This will cause issue for some chat_templates. Tracking: https://github.com/EricLBuehler/mistral.rs/issues/793
            let content_json = parts
                .iter()
                .map(|p| match p {
                    async_openai::types::chat::ChatCompletionRequestToolMessageContentPart::Text(t) => {
                        ("text".to_string(), t.text.clone())
                    }
                })
                .collect::<Vec<_>>();

            IndexMap::from([
                (String::from("role"), Either::Left(String::from("tool"))),
                (
                    String::from("content"),
                    Either::Left(json!(content_json).to_string()),
                ),
                (
                    String::from("tool_call_id"),
                    Either::Left(tool_call_id.clone()),
                ),
            ])
        }
        ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
            content,
            name,
            tool_calls,
            ..
        }) => {
            let mut map: IndexMap<String, MessageContent> = IndexMap::from([(
                String::from("role"),
                Either::Left(String::from("assistant")),
            )]);
            match content {
                Some(ChatCompletionRequestAssistantMessageContent::Text(s)) => {
                    map.insert("content".to_string(), Either::Left(s.clone()));
                }
                Some(ChatCompletionRequestAssistantMessageContent::Array(parts)) => {
                    // TODO: This will cause issue for some chat_templates. Tracking: https://github.com/EricLBuehler/mistral.rs/issues/793
                    let content_json= parts.iter().map(|p| match p {
                        async_openai::types::chat::ChatCompletionRequestAssistantMessageContentPart::Text(t) => {
                            ("text".to_string(), t.text.clone())
                        }
                        async_openai::types::chat::ChatCompletionRequestAssistantMessageContentPart::Refusal(i) => {
                            ("refusal".to_string(), i.refusal.clone())
                        }
                    }).collect::<Vec<_>>();
                    map.insert(
                        String::from("content"),
                        Either::Left(json!(content_json).to_string()),
                    );
                }
                None => {
                    // Use Some(""), not None as it is more compatible with many open source `chat_template`s.
                    map.insert("content".to_string(), Either::Left(String::new()));
                }
            }
            if let Some(name) = name {
                map.insert("name".to_string(), Either::Left(name.clone()));
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
            (String::from("role"), Either::Left(String::from("function"))),
            (
                "content".to_string(),
                Either::Left(content.clone().unwrap_or_default()),
            ),
            ("name".to_string(), Either::Left(name.clone())),
        ]),
    }
}

/// Create a model to run locally, via files from Huggingface.
///
/// `model_id` uniquely refers to a Huggingface model.
/// `model_type` is the type of model, if needed to be explicit. Often this can
///    be inferred from the `.model_type` key in a HF's `config.json`, or from the GGUF metadata.
/// `from_gguf` is a path to a GGUF file within the huggingface model repo. If provided, the model will be loaded from this GGUF. This is useful for loading quantized models.
/// `hf_token_literal` is a literal string of the Huggingface API token. If not provided, the token will be read from the HF token cache (i.e. `~/.cache/huggingface/token` or set via `HF_TOKEN_PATH`).
/// `distributed` optionally runs the model tensor-parallel across multiple nodes.
#[cfg(feature = "local_llm")]
pub async fn create_hf_model(
    model_id: &str,
    model_type: Option<&str>,
    from_gguf: Option<PathBuf>,
    hf_token_literal: Option<&SecretString>,
    chat_template_literal: Option<&str>,
    distributed: Option<DistributedConfig>,
) -> Result<Arc<dyn Chat>> {
    // Configure multi-node distributed inference before loading: the loader reads the
    // topology (`RING_CONFIG`, or the `MISTRALRS_MN_*` set for NCCL) from the environment
    // while building the pipeline. Any returned guard keeps the temp file alive for the
    // model.
    let ring_config = if let Some(cfg) = distributed {
        configure_distributed(&cfg)?
    } else {
        // Clear any topology left by a prior distributed load in this process, so this
        // single-node load cannot inherit a rank and world size and try to join a
        // communicator that is not there.
        clear_distributed_env();
        None
    };
    mistral::MistralLlama::from_hf(
        model_id,
        model_type,
        hf_token_literal,
        from_gguf,
        chat_template_literal,
        ring_config,
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
    /// Sequence-length budget for layer placement and KV-cache sizing.
    pub context_length: Option<usize>,
    /// Attention implementation to request from the engine.
    pub paged_attention: PagedAttentionMode,
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
    // Configure multi-node distributed (ring) inference before loading: the
    // loader reads `RING_CONFIG` from the environment while building the
    // pipeline. The returned guard keeps the temp file alive for the model.
    let ring_config = if let Some(cfg) = distributed {
        configure_distributed(&cfg)?
    } else {
        // Clear any topology left by a prior distributed load in this process, so this
        // single-node load cannot inherit a rank and world size and try to join a
        // communicator that is not there.
        clear_distributed_env();
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
