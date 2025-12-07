# AI UDF Tests Quick Start Guide

This guide explains how to run the AI UDF (User-Defined Function) tests.

## Prerequisites

### Required API Keys

Set the following environment variables with your API keys:

```bash
export SPICE_OPENAI_API_KEY="your-openai-api-key"
export SPICE_ANTHROPIC_API_KEY="your-anthropic-api-key"
export SPICE_XAI_API_KEY="your-xai-api-key"
export SPICE_HUGGINGFACE_API_KEY="your-huggingface-api-key"  # Required for local model test
```

### Models Used

The tests use the following models:

- **OpenAI**: `gpt-4o-mini`
- **xAI**: `grok-4-fast-non-reasoning`
- **Anthropic**: `claude-3-5-haiku-latest`
- **Local**: `llama3` (Phi-3.5-mini-instruct from HuggingFace)

## Running the Tests

> **Important**: When multiple models are configured in the spicepod, you must specify the model name in the UDF call. For example: `ai('hi', 'gpt-4o-mini')`. The tests configure multiple models, so all queries explicitly specify which model to use.

### Run All AI UDF Tests

From the repository root:

```bash
cargo test -p runtime --test integration_models ai_udf --features models
```

Or from the `crates/runtime` directory:

```bash
cd crates/runtime
cargo test --test integration_models ai_udf --features models
```

### Run Individual Tests

**Basic AI UDF test** (OpenAI + Anthropic):

```bash
cargo test -p runtime --test integration_models test_ai_udf_basic --features models
```

**AI UDF with dataset** (OpenAI + xAI + Anthropic):

```bash
cargo test -p runtime --test integration_models test_ai_udf_with_dataset --features models
```

**LEFT truncation test** (OpenAI + xAI + Anthropic):

```bash
cargo test -p runtime --test integration_models test_ai_udf_left_truncate --features models
```

**Local model test** (Llama 3):

```bash
cargo test -p runtime --test integration_models test_ai_udf_with_local_model --features models
```

> **Note**: The local model test downloads the model on first run and may take several minutes.

## Test Descriptions

### `test_ai_udf_basic`

- Tests basic `ai()` function calls with explicit model specification
- Tests model selection with specific model names (e.g., `ai('hi', 'gpt-4o-mini')`)
- Tests column aliases
- Tests LEFT() function on ai() results
- Tests multiple ai() calls in a single query
- **Duration**: ~30-60 seconds
- **Models**: gpt-4o-mini, claude-3-5-haiku-latest
- **Note**: Model name is required in all calls since multiple models are configured

### `test_ai_udf_with_dataset`

- Tests ai() function with real dataset (MegaScience Q&A dataset)
- Tests AI answering questions across 3 providers
- Each model answers questions in 10 words or less
- **Duration**: ~60-120 seconds
- **Models**: gpt-4o-mini, grok-4, claude-haiku

### `test_ai_udf_left_truncate`

- Tests LEFT() truncation with multiple ai() calls
- Verifies output length constraints
- **Duration**: ~30-60 seconds
- **Models**: gpt-4o-mini, grok-4, claude-haiku

### `test_ai_udf_with_local_model`

- Tests locally-hosted model via HuggingFace (Phi-3.5-mini)
- Verifies synchronous execution of local models
- **Duration**: ~3-5 minutes (first run with model download)
- **Models**: llama3 (alias for Phi-3.5-mini-instruct)

## Troubleshooting

### Missing API Keys

If you see errors about missing secrets, ensure all required environment variables are set:

```bash
echo $SPICE_OPENAI_API_KEY
echo $SPICE_ANTHROPIC_API_KEY
echo $SPICE_XAI_API_KEY
echo $SPICE_HUGGINGFACE_API_KEY  # For local model test
```

### Timeout Errors

Tests have built-in timeouts:

- Basic tests: 60 seconds
- Dataset tests: 120 seconds
- Local model tests: 180 seconds

If tests timeout, check your network connection and API rate limits.

### Local Model Issues

The local model test requires:

- Sufficient disk space (~7.6GB for Phi-3.5-mini)
- HuggingFace API key (`SPICE_HUGGINGFACE_API_KEY`)
- First run will download the model (this takes time!)

## Quick Example

```bash
# Set your API keys
export SPICE_OPENAI_API_KEY="sk-..."
export SPICE_ANTHROPIC_API_KEY="sk-ant-..."
export SPICE_XAI_API_KEY="xai-..."
export SPICE_HUGGINGFACE_API_KEY="hf_..."

# Run all AI UDF tests (from repo root)
cargo test -p runtime --test integration_models ai_udf --features models

# Or run just the basic test
cargo test -p runtime --test integration_models test_ai_udf_basic --features models
```

## Additional Options

### Run with output logs

```bash
cargo test -p runtime --test integration_models ai_udf --features models -- --nocapture
```

### Run with specific log level

```bash
RUST_LOG=info cargo test -p runtime --test integration_models ai_udf --features models
```
