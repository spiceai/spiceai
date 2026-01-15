# google-genai

A Rust client library for the Google Generative AI (Gemini) REST API.

## Features

- ✅ Text generation and chat
- ✅ Multi-turn conversations
- ✅ Streaming responses (SSE)
- ✅ Text embeddings with configurable dimensions
- ✅ Function calling (tools)
- ✅ Tool configuration (function calling modes)
- ✅ Thinking configuration for complex reasoning
- ✅ Cached content support
- ✅ Structured output (response schema)
- ✅ Safety settings
- ✅ Batch embeddings

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
google-genai = { path = "path/to/google-genai" }
```

## Usage
See [examples](./examples)

## API Reference

See the [Google AI documentation](https://ai.google.dev/api) for detailed API specifications.

### Supported Models

- **Chat/Text Generation**: `gemini-2.0-flash`, `gemini-1.5-pro`, `gemini-1.5-flash`
- **Embeddings**: `text-embedding-004`

## Examples

The `examples/` directory contains complete working examples:

- **`simple_chat.rs`** - Basic text generation with token usage
- **`streaming.rs`** - Streaming responses with Server-Sent Events
- **`embeddings.rs`** - Generate text embeddings with multiple inputs
- **`function_calling.rs`** - Function calling with weather API example
- **`tool_config_modes.rs`** - Demonstrates different `ToolConfig` modes (AUTO, NONE, ANY with restrictions)
- **`thinking.rs`** - Using thinking mode for complex reasoning tasks
- **`cached_content.rs`** - Using cached content for optimized queries

Run examples with:
```bash
cargo run --example simple_chat
cargo run --example function_calling
cargo run --example thinking
```

## Authentication

Get an API key from [Google AI Studio](https://aistudio.google.com/app/apikey).

## License

Licensed under the Apache License, Version 2.0. See LICENSE for details.
