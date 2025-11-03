# HuggingFace Model Downloader

A reusable Rust crate for downloading models from HuggingFace Hub with support for authentication, caching, and various repository types.

## Features

- Download models from HuggingFace Hub (models, datasets, spaces)
- Support for private repositories with authentication tokens
- Automatic caching using HuggingFace's cache directory
- Support for specific revisions/branches
- Async API built on `tokio`
- Progress tracking support

## Usage

### Basic Example

```rust
use hf_model_downloader::{DownloadConfig, HfDownloader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a download configuration
    let config = DownloadConfig::new("sentence-transformers/all-MiniLM-L6-v2")
        .with_revision("main")
        .with_progress(true);

    // Create the downloader
    let downloader = HfDownloader::new(config)?;

    // Download a specific file
    let tokenizer_path = downloader.download_file("tokenizer.json").await?;
    println!("Downloaded tokenizer to: {:?}", tokenizer_path);

    // Download multiple files
    let paths = downloader.download_files(&[
        "config.json",
        "model.safetensors"
    ]).await?;

    Ok(())
}
```

### With Authentication

```rust
use hf_model_downloader::DownloadConfig;
use secrecy::SecretString;

let config = DownloadConfig::new("private-org/private-model")
    .with_token(SecretString::from("hf_...".to_string()));

let downloader = HfDownloader::new(config)?;
let path = downloader.download_file("model.safetensors").await?;
```

### Convenience Function

For simple use cases, use the `download_file` helper:

```rust
use hf_model_downloader::download_file;

let path = download_file(
    "sentence-transformers/all-MiniLM-L6-v2",
    "tokenizer.json",
    Some("main"),  // revision
    None,          // token
).await?;
```

## Environment Variables

- `HF_HUB_CACHE`: Custom cache directory for downloaded models
- `HF_TOKEN_PATH`: Path to HuggingFace token file

## Auto-Discovery of GGUF Files

The downloader includes intelligent GGUF file discovery for llama.cpp models:

```rust
use hf_model_downloader::{DownloadConfig, HfDownloader};

let config = DownloadConfig::new("TheBloke/Llama-2-7B-GGUF");
let downloader = HfDownloader::new(config)?;

// Automatically find and download the best GGUF file
let model_path = downloader.download_best_gguf().await?;

// Or list all available GGUF files
let gguf_files = downloader.find_gguf_files().await?;
```

The auto-discovery prefers Q4_K_M quantization for optimal balance of quality and size.

## Integration with Spice

This crate is used by the Spice runtime to enable automatic model downloads from HuggingFace, particularly for:

- **llama.cpp engine**: Automatically discovers and downloads GGUF files from HuggingFace repositories
- **Embedding models**: Downloads tokenizers and model weights for local embedding inference

### Example: llama.cpp with Auto-Discovery

```yaml
models:
  - from: huggingface:TheBloke/Llama-2-7B-GGUF
    name: my-model
    engine: llama.cpp
    # No files needed - automatically discovers and downloads the best GGUF!
```

The model ID is parsed from the `from:` field. All of these formats are supported:

- `huggingface:TheBloke/Llama-2-7B-GGUF`
- `huggingface:huggingface.co/TheBloke/Llama-2-7B-GGUF`
- `hf:TheBloke/Llama-2-7B-GGUF`
- `TheBloke/Llama-2-7B-GGUF`

The downloader automatically finds and downloads the optimal GGUF file.

## License

Apache License 2.0 - See LICENSE file for details
