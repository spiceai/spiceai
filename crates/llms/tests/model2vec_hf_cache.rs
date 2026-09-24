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

//! Guards that a static-embedding model is resolved out of the shared Hugging
//! Face cache when `HF_HUB_CACHE` names one.
//!
//! Its own test binary because it sets a process-wide environment variable, and
//! `std::env::set_var` is only sound while no other thread is touching the
//! environment. One test per binary is what makes that true here.
//!
//! Registered in the `nextest` gate by name (see `NEXTEST_FILTER` in the
//! Makefile), like the other credential-free `llms` integration binaries.

use std::io::Write;
use std::path::Path;

use llms::model2vec::Model2Vec;
use tokenizers::Tokenizer;
use tokenizers::models::wordpiece::WordPiece;

/// The repo id the guard asks for. It does not exist on the Hub, so a lookup
/// that ignores the cache cannot accidentally succeed by downloading it.
const REPO_ID: &str = "spiceai-fork-patch-guard/model2vec-not-on-the-hub";

/// The two files `model2vec-rs` reads, written into `dir`.
///
/// Written by hand so the guard needs no network and no committed fixture:
/// `safetensors` is a length-prefixed JSON header followed by raw tensor bytes,
/// and the loader takes the first of `embeddings`, `embedding.weight` or `0`.
fn write_static_model(dir: &Path) {
    let vocab_path = dir.join("vocab.txt");
    std::fs::write(&vocab_path, "[UNK]\nan\napple\nday\n").expect("writes the fixture vocabulary");

    // The loader resolves the tokenizer's `unk_token` and requires it to be in
    // the vocabulary, so the fixture needs a real one.
    let model = WordPiece::from_file(
        vocab_path
            .to_str()
            .expect("the fixture vocabulary path is UTF-8"),
    )
    .unk_token("[UNK]".to_string())
    .build()
    .expect("builds the fixture WordPiece model");
    Tokenizer::new(model)
        .save(dir.join("tokenizer.json"), false)
        .expect("writes the fixture tokenizer");

    let rows: usize = 4;
    let cols: usize = 2;
    let mut data = Vec::with_capacity(rows * cols * 4);
    for i in 0..rows * cols {
        #[expect(
            clippy::cast_precision_loss,
            reason = "the fixture's values are small and arbitrary"
        )]
        data.extend_from_slice(&(i as f32).to_le_bytes());
    }
    let header = format!(
        r#"{{"embeddings":{{"dtype":"F32","shape":[{rows},{cols}],"data_offsets":[0,{}]}}}}"#,
        data.len()
    );
    let mut safetensors =
        std::fs::File::create(dir.join("model.safetensors")).expect("creates the tensor file");
    safetensors
        .write_all(&(header.len() as u64).to_le_bytes())
        .expect("writes the safetensors header length");
    safetensors
        .write_all(header.as_bytes())
        .expect("writes the safetensors header");
    safetensors
        .write_all(&data)
        .expect("writes the safetensors tensor data");

    // `config.json` is present so only the cache lookup is under test here.
    std::fs::write(dir.join("config.json"), r#"{"normalize":true}"#)
        .expect("writes the fixture config");
}

/// A model already in the shared cache must be read from it, not fetched again.
///
/// `HF_HUB_CACHE` is how a deployment points every model at one warm directory —
/// a mounted volume, an image layer, a sidecar's download. Only a Spice patch to
/// the `spiceai/model2vec-rs` fork reads it: upstream builds the client with
/// `Api::new()`, which takes the default cache under the home directory and
/// ignores the variable entirely. Losing the patch therefore does not fail
/// loudly; it re-downloads a model that was already on disk, on every start, for
/// every replica — and where there is no egress, or the model is gated, it does
/// not come back at all.
///
/// Asserted with a repo id that exists only in the cache, so the load can only
/// succeed by looking there. The passing path is offline — hf-hub returns a
/// cached file without a request — and only the failing path reaches the network,
/// where the id resolves to nothing (measured: `status code 401`).
#[test]
fn a_cached_model_is_read_from_the_directory_hf_hub_cache_names() {
    let cache = tempfile::tempdir().expect("creates a cache directory");

    // hf-hub's on-disk layout: `refs/<revision>` holds the commit hash, and the
    // files live under `snapshots/<commit hash>/`.
    let repo_dir = cache
        .path()
        .join(format!("models--{}", REPO_ID.replace('/', "--")));
    let commit = "0000000000000000000000000000000000000000";
    let snapshot = repo_dir.join("snapshots").join(commit);
    std::fs::create_dir_all(&snapshot).expect("creates the snapshot directory");
    std::fs::create_dir_all(repo_dir.join("refs")).expect("creates the refs directory");
    std::fs::write(repo_dir.join("refs").join("main"), commit).expect("writes the ref");
    write_static_model(&snapshot);

    // SAFETY: this binary holds exactly one test, so no other thread is reading
    // or writing the environment while this runs.
    unsafe {
        std::env::set_var("HF_HUB_CACHE", cache.path());
    }

    Model2Vec::from_params(REPO_ID, None, None, None, None, None, None).unwrap_or_else(|e| {
        panic!(
            "a model already present under HF_HUB_CACHE must be loaded from there; ignoring the \
             variable re-downloads it on every start, and fails outright with no egress: {e}"
        )
    });
}
