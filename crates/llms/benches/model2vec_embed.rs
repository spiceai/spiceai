/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Benchmarks real (non-stub) embedding compute via `Model2Vec`
//! (`crates/llms/src/model2vec.rs`) -- the only hermetic local-inference
//! embedding path in the codebase (`embed_sync` runs synchronously, no
//! network, no GPU, once a model is staged locally).
//!
//! `Model2Vec::from_params` resolves a bare Hugging Face repo id (e.g.
//! `minishlab/potion-base-2M`, the tiny model already used by
//! `crates/runtime/tests/models/{hnsw_index,hf}.rs`) via a live download at
//! model-load time -- it is not committed or vendored anywhere in this
//! repo. For a hermetic, network-free run, point `MODEL2VEC_LOCAL_PATH` at
//! a pre-staged local copy of the model directory (a `/`, `./`, `../`, or
//! `~/`-prefixed name resolves to a local path with no HF lookup, per
//! `local_model_path` in `model2vec.rs`). Without it this bench prints
//! staging instructions and exits successfully, so it is safe under
//! `cargo bench` in CI.

#![allow(clippy::expect_used)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use llms::embeddings::{Embed, EmbeddingInput};
use llms::model2vec::Model2Vec;

fn print_instructions(reason: &str) {
    println!(
        "model2vec_embed: skipped ({reason}).\n\
         \n\
         This bench runs real embedding compute via Model2Vec and needs a\n\
         local model directory (no network at bench time). To stage one:\n\
         \n\
         1. Download the tiny model already used by the test suite once:\n\
            huggingface-cli download minishlab/potion-base-2M \\\n\
              --local-dir /path/to/potion-base-2M\n\
         2. Run the bench:\n\
            MODEL2VEC_LOCAL_PATH=/path/to/potion-base-2M \\\n\
              cargo bench -p llms --bench model2vec_embed --features local_embed"
    );
}

fn make_texts(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!("benchmark document number {i} with representative sentence-length text")
        })
        .collect()
}

fn bench_model2vec_embed(c: &mut Criterion) {
    let Ok(local_path) = std::env::var("MODEL2VEC_LOCAL_PATH") else {
        print_instructions("MODEL2VEC_LOCAL_PATH is not set");
        return;
    };

    let model = match Model2Vec::from_params(&local_path, None, None, None, None, None, None) {
        Ok(model) => model,
        Err(e) => {
            print_instructions(&format!("failed to load model at {local_path}: {e}"));
            return;
        }
    };

    let mut group = c.benchmark_group("model2vec_embed");
    group.sample_size(20);

    for &n in &[1usize, 16, 64, 256] {
        let texts = make_texts(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("embed_sync", n), &texts, |b, texts| {
            b.iter(|| {
                let input = EmbeddingInput::StringArray(texts.clone());
                let embeddings = model.embed_sync(input).expect("embed_sync succeeds");
                black_box(embeddings);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_model2vec_embed);
criterion_main!(benches);
