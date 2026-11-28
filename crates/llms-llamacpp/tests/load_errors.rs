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

//! Load failures, with no network: a file that is not a GGUF, a GGUF of an architecture
//! llama.cpp does not know, and a chat template file that cannot be read.

use std::io::Write;
use std::path::PathBuf;

use llms::chat::{Error, LocalModelOptions};

async fn load(path: PathBuf) -> Error {
    let weights = [path];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    match (engine.load)(&weights, LocalModelOptions::default()).await {
        Ok(_) => panic!("{} loaded", weights[0].display()),
        Err(error) => error,
    }
}

#[tokio::test]
async fn a_file_that_is_not_a_gguf_names_what_llama_cpp_found() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("broken.gguf");
    std::fs::write(&path, b"not a model at all").expect("the file is written");

    let error = load(path.clone()).await.to_string();

    assert!(
        error.starts_with(&format!(
            "Failed to load the model. An error occurred: llama.cpp could not load the model '{}': ",
            path.display()
        )),
        "{error}"
    );
    assert!(
        error.contains("invalid magic"),
        "llama.cpp's reason is kept: {error}"
    );
}

/// A GGUF v3 header with one string key, `general.architecture`, and no tensors.
fn gguf_with_architecture(architecture: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"GGUF");
    bytes.extend_from_slice(&3u32.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes()); // tensors
    bytes.extend_from_slice(&1u64.to_le_bytes()); // key-value pairs
    let key = b"general.architecture";
    bytes.extend_from_slice(&(key.len() as u64).to_le_bytes());
    bytes.extend_from_slice(key);
    bytes.extend_from_slice(&8u32.to_le_bytes()); // string
    bytes.extend_from_slice(&(architecture.len() as u64).to_le_bytes());
    bytes.extend_from_slice(architecture.as_bytes());
    bytes
}

#[tokio::test]
async fn an_unknown_architecture_says_this_release_does_not_support_it() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("foo-model.gguf");
    std::fs::File::create(&path)
        .and_then(|mut f| f.write_all(&gguf_with_architecture("foo")))
        .expect("the file is written");

    let error = load(path).await;

    assert!(
        matches!(&error, Error::UnsupportedArchitecture { architecture, file }
            if architecture == "foo" && file == "foo-model.gguf"),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "Architecture 'foo' in foo-model.gguf is not supported by this Spice release. Upgrade Spice, or use a GGUF of a supported architecture: https://spiceai.org/docs/components/models#gguf-architectures"
    );
}

/// A model's `tokenizer_config.json` is read before the model, so one that is not JSON fails
/// the load, naming the file, before llama.cpp reads the weights: here not a GGUF, which
/// llama.cpp would refuse with an error of its own.
#[tokio::test]
async fn a_tokenizer_config_that_is_not_json_names_the_file() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config = dir.path().join("tokenizer_config.json");
    std::fs::write(&config, b"{ not json").expect("the file is written");
    let weights = [dir.path().join("model.gguf")];
    std::fs::write(&weights[0], b"not a model at all").expect("the file is written");
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    let options = LocalModelOptions {
        chat_template_file: Some(&config),
        ..LocalModelOptions::default()
    };
    let Err(error) = (engine.load)(&weights, options).await else {
        panic!("a tokenizer_config.json that is not JSON should fail the load");
    };
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to load the model. An error occurred: could not read the chat template in '{}': key must be a string at line 1 column 3 Report a bug on GitHub: https://github.com/spiceai/spiceai/issues",
            config.display()
        )
    );
}
