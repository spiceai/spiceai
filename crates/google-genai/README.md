# google-genai

A Rust client library for Google's Gemini models on Vertex AI.

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

See the [Vertex AI generative AI documentation](https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest)
for detailed API specifications.

### Supported Models

- **Chat/Text Generation**: `gemini-2.0-flash`, `gemini-1.5-pro`, `gemini-1.5-flash`
- **Embeddings**: `text-embedding-004`

## Examples

The `examples/` directory contains:

- **`cached_content.rs`** - The request structure for cached content, printed rather than sent

Run it with:
```bash
cargo run --example cached_content
```

## Authentication

Requests are authenticated with an `Authorization: Bearer` token sourced from a
`token_provider::TokenProvider` — for Vertex AI, a GCP service account JWT-bearer exchange via
`token_provider::gcp_service_account_token::GcpServiceAccountTokenProvider`.

```rust
let token_provider = GcpServiceAccountTokenProvider::try_new(
    &service_account_json,
    "https://www.googleapis.com/auth/cloud-platform",
).await?;
// `location: global` uses the non-regional host (`aiplatform.googleapis.com`, no
// `{location}-` prefix); every other region uses the regional host.
let host = if location == "global" {
    "https://aiplatform.googleapis.com".to_string()
} else {
    format!("https://{location}-aiplatform.googleapis.com")
};
let base_url = format!(
    "{host}/v1/projects/{project}/locations/{location}/publishers/google"
);
let client = google_genai::Client::with_bearer_token(Arc::new(token_provider), base_url)?;
```

## License

Licensed under the Apache License, Version 2.0. See LICENSE for details.
