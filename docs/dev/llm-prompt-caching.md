# LLM Prompt Caching

This note documents how prompt-cache intent flows through Spice's LLM runtime and provider adapters. It is internal maintainer guidance, not a user-facing API reference.

## Core Model

`prompt_cache_key` and `prompt_cache_retention` describe cache intent. They do not describe a portable serialized KV-cache format, and Spice should not attempt to store live LLM KV tensors in the Arrow accelerator or hash indexes. KV cache state is model-engine, provider, device, and scheduler specific, so it belongs in the provider or local model engine.

Provider adapters should map cache intent to the provider-native mechanism when one exists. If a provider does not support explicit prompt caching, preserve provider correctness and request semantics rather than fabricating cache behavior.

## Runtime Entry Points

- Chat model defaults are collected in `crates/runtime/src/model/chat.rs` and applied by `crates/runtime/src/model/wrapper/mod.rs`.
- Responses model defaults are collected in `crates/runtime/src/model/responses.rs` and applied by `crates/runtime/src/model/wrapper/responses.rs`.
- Model parameter specs live in `crates/runtime/src/model/params/mod.rs`; update the parameter counts when adding entries.
- `/v1/nsql` accepts `prompt_cache_key` and forwards it to the configured NSQL chat model in `crates/runtime/src/http/v1/nsql.rs`.

Defaults must not override request-provided values. Keep default parsing on the request path where warnings can identify the model and field that failed to parse.

## Provider Mappings

| Provider path                         | Cache mapping                                                                                                                                                         |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| OpenAI-compatible chat and Azure chat | Pass `prompt_cache_key` through the OpenAI-compatible chat request field.                                                                                             |
| OpenAI-compatible Responses           | Pass `prompt_cache_key` and `prompt_cache_retention` through the Responses request fields.                                                                            |
| xAI chat                              | Move `prompt_cache_key` out of the request body and send it as the `x-grok-conv-id` request header.                                                                   |
| xAI Responses                         | Leave Responses fields in the request body.                                                                                                                           |
| Google Gemini                         | Map `prompt_cache_key` to `GenerateContentRequest.cached_content.name`; callers must provide a valid cached-content resource name.                                    |
| Anthropic                             | Set top-level ephemeral `cache_control` when cache intent is present and preserve cache usage fields in OpenAI-compatible usage.                                      |
| Bedrock Converse                      | Append a native `CachePoint` to the last message, or to system content when no messages exist.                                                                        |
| Databricks hosted Claude              | Use BYOT JSON and add Claude-style `cache_control` to the last text content part.                                                                                     |
| Local HuggingFace/file models         | Use `mistral-rs` native KV cache and paged-attention scheduling when the backend and pipeline support it. Request-level cache keys are not portable to local tensors. |

## Usage Accounting

When providers return cache-token usage, keep totals data-correct:

- Include provider-reported cache creation and cache read input tokens in `prompt_tokens` and `total_tokens` when the provider's accounting reports them separately from normal input tokens.
- Populate `prompt_tokens_details.cached_tokens` only with cache-read tokens. Cache-creation tokens are prompt work, but they were not read from cache.
- Do not invent cached-token counts when a provider omits them.

## Local Model Notes

`mistral-rs` regular KV caching is enabled by keeping `no_kv_cache` false. Paged attention is requested only on supported CUDA Unix backends, then used only if the loaded pipeline exposes cache metadata. If metadata is missing, fall back to the default scheduler and log at debug level. Metal keeps the default scheduler because the current paged-attention path can panic in the underlying Metal kernels.

Keep scheduler constants non-zero at compile time; do not use `unwrap` or `expect` in the production scheduler path.

## Maintenance Checklist

When changing prompt caching behavior:

- Add or update provider-specific unit tests in `crates/llms`.
- Add runtime wrapper/default extraction tests in `crates/runtime` when model parameters or defaults change.
- Ensure NSQL forwards only cache intent, not provider-specific behavior.
- Keep provider mappings explicit; avoid one-size-fits-all request mutation across providers.
- Do not store live KV tensors in data accelerators, Arrow arrays, or hash indexes.
- Run `cargo fmt --all`.
- Run `cargo test -p llms prompt_cache --features local_llm`.
- Run `cargo test -p runtime prompt_cache --features models`.
- Run `make lint` before resolving review threads or handing off the PR.