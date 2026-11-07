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

//! What it costs to put one streamed chat completion chunk on the wire: serialized
//! straight to a string, through [`chat_api::wire::chat_completion_json`], which omits
//! unreported usage counts first, or through [`chat_api::wire::stream_chunk_json`], which
//! cleans only a chunk that carries usage. `/v1/chat/completions` pays this once per
//! streamed token.
//!
//! Prints each arm's latency distribution, P50 through the maximum, from two runs of
//! each arm in mirrored order (A, B, C, C, B, A):
//!
//! ```text
//! cargo bench -p chat-api --bench wire
//! ```

#![expect(clippy::expect_used, reason = "a benchmark fails loudly on bad input")]

use std::hint::black_box;
use std::time::Instant;

use async_openai::types::chat::CreateChatCompletionStreamResponse;
use chat_api::wire::{chat_completion_json, stream_chunk_json};
use serde_json::json;

/// Calls timed per run; each arm runs twice.
const SAMPLES: usize = 500_000;

/// A chunk carrying one token, as nearly every chunk of a stream does.
fn token_chunk() -> CreateChatCompletionStreamResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 1_793_000_000,
        "model": "qwen",
        "choices": [{"index": 0, "delta": {"content": " the"}, "finish_reason": null}],
    }))
    .expect("a token chunk")
}

/// The final chunk, carrying usage with unreported detail counts.
fn usage_chunk() -> CreateChatCompletionStreamResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 1_793_000_000,
        "model": "qwen", "choices": [],
        "usage": {
            "prompt_tokens": 20_758, "completion_tokens": 95, "total_tokens": 20_853,
            "prompt_tokens_details": {"audio_tokens": null, "cached_tokens": 20_480},
            "completion_tokens_details": {"reasoning_tokens": null, "audio_tokens": null},
        },
    }))
    .expect("a usage chunk")
}

/// The latency of each of [`SAMPLES`] calls to `serialize`, in nanoseconds.
fn time(serialize: &dyn Fn() -> String) -> Vec<u64> {
    (0..SAMPLES)
        .map(|_| {
            let start = Instant::now();
            black_box(serialize());
            u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
        })
        .collect()
}

fn report(name: &str, mut samples: Vec<u64>) {
    samples.sort_unstable();
    let at = |quantile: f64| {
        // Nearest rank; the sample count is far past the 10,000 a P99.9 needs.
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "a rank within a vector of at most a few million samples"
        )]
        let rank = ((quantile * samples.len() as f64).ceil() as usize).clamp(1, samples.len());
        samples[rank - 1]
    };
    println!(
        "{name:<34} n={:>9}  p50={:>6} ns  p99={:>6} ns  p99.9={:>6} ns  max={:>8} ns",
        samples.len(),
        at(0.50),
        at(0.99),
        at(0.999),
        samples.last().copied().unwrap_or_default()
    );
}

fn main() {
    for (chunk_name, chunk) in [
        ("token chunk", token_chunk()),
        ("usage chunk", usage_chunk()),
    ] {
        let direct = || serde_json::to_string(&chunk).expect("serialize the chunk");
        let cleaned = || {
            serde_json::to_string(&chat_completion_json(&chunk).expect("clean the chunk"))
                .expect("serialize the cleaned chunk")
        };
        let streamed = || {
            serde_json::to_string(&stream_chunk_json(&chunk).expect("prepare the chunk"))
                .expect("serialize the prepared chunk")
        };
        // Warm every path before timing any.
        time(&direct);
        time(&cleaned);
        time(&streamed);
        let mut direct_samples = time(&direct);
        let mut cleaned_samples = time(&cleaned);
        let mut streamed_samples = time(&streamed);
        streamed_samples.extend(time(&streamed));
        cleaned_samples.extend(time(&cleaned));
        direct_samples.extend(time(&direct));
        report(&format!("{chunk_name}, direct"), direct_samples);
        report(
            &format!("{chunk_name}, chat_completion_json"),
            cleaned_samples,
        );
        report(
            &format!("{chunk_name}, stream_chunk_json"),
            streamed_samples,
        );
    }
}
