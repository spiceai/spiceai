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

//! A model split tensor-parallel across two ranks on this machine: this test binary runs
//! again as rank 1, which lends this machine's GPU over loopback, and this process is rank 0,
//! which serves the model. Its greedy replies must equal the single-node model's: the ranks
//! sum their partials exactly, so the split changes only the order of float additions, which
//! one-answer prompts don't feel.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};

use llms::chat::{
    Chat, DistributedBackend, DistributedConfig, LocalModelOptions, ReasoningDefault,
};
use serde_json::json;

use crate::{FILE, MODEL, REVISION, request};

/// Set in rank 1's environment: there `rank_one` lends the GPU instead of returning.
const RANK_ONE: &str = "SPICE_LLAMACPP_TEST_RANK_ONE";

fn topology(node_rank: usize) -> DistributedConfig {
    DistributedConfig {
        backend: DistributedBackend::Ring,
        node_rank,
        nodes: vec!["127.0.0.1".to_string(), "127.0.0.1".to_string()],
    }
}

fn options(distributed: Option<&DistributedConfig>) -> LocalModelOptions<'_> {
    LocalModelOptions {
        reasoning: ReasoningDefault {
            model_name: "qwen3-0.6b",
            effort: None,
        },
        distributed,
        ..LocalModelOptions::default()
    }
}

/// Rank 1, run only by `a_model_split_across_two_ranks_answers_as_on_one` in a process of
/// its own: rank 1's load lends this machine's GPU and never completes, so this pends until
/// that test kills it. Without `RANK_ONE` it does nothing.
#[tokio::test]
async fn rank_one() {
    if std::env::var_os(RANK_ONE).is_none() {
        return;
    }
    let cfg = topology(1);
    // rank 1 never reads the weights; the head sends them
    let weights = [PathBuf::from(FILE)];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    let error = (engine.load)(&weights, options(Some(&cfg))).await.err();
    panic!("rank 1 stopped lending its GPU: {error:?}");
}

/// A `tracing` writer into a shared buffer.
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("the log").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Kills rank 1 when the test ends, whichever way it ends.
struct RankOne(Child);

impl Drop for RankOne {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn local_gguf() -> PathBuf {
    llms::chat::hf_files::download_model(
        &format!("{MODEL}:{REVISION}"),
        &[FILE],
        llms::hf::HubParams::default(),
        "qwen3-0.6b",
    )
    .await
    .expect("the GGUF downloads")
    .join(FILE)
}

const PROMPTS: &[&str] = &[
    "What is the capital of France? Reply with one word.",
    "What is 12 times 12? Reply with only the number.",
    "Count from 1 to 12, separated by commas.",
    "Which planet is closest to the Sun? Reply with one word.",
];

async fn replies(model: &dyn Chat) -> Vec<String> {
    let mut out = Vec::new();
    for prompt in PROMPTS {
        let reply = model
            .chat_request(request(json!({
                "model": "qwen3",
                "messages": [{ "role": "user", "content": prompt }],
                "temperature": 0,
                "reasoning_effort": "none",
                "max_tokens": 48,
            })))
            .await
            .expect("a reply");
        out.push(reply.choices[0].message.content.clone().expect("content"));
    }
    out
}

#[tokio::test]
async fn a_model_split_across_two_ranks_answers_as_on_one() {
    let gguf = local_gguf().await;
    let weights = [gguf];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    assert!(
        engine.distributed,
        "the llama.cpp engine serves distributed models"
    );

    let single: Arc<dyn Chat> = (engine.load)(&weights, options(None))
        .await
        .expect("the model loads on one node");
    let expected = replies(&*single).await;
    drop(single);

    let _rank_one = RankOne(
        Command::new(std::env::current_exe().expect("the test binary's path"))
            .args(["--exact", "distributed::rank_one", "--nocapture"])
            .env(RANK_ONE, "1")
            .spawn()
            .expect("rank 1 starts"),
    );
    let cfg = topology(0);
    // the head waits for rank 1 to serve its GPU before it loads; its log says what it split
    // the model across, which a load that fell back to one node would not
    let log = Arc::new(Mutex::new(Vec::<u8>::new()));
    let writer = Arc::clone(&log);
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || Capture(Arc::clone(&writer)))
        .with_ansi(false)
        .finish();
    let split: Arc<dyn Chat> = {
        let _capture = tracing::subscriber::set_default(subscriber);
        (engine.load)(&weights, options(Some(&cfg)))
            .await
            .expect("the model loads split across the two ranks")
    };
    let log = String::from_utf8_lossy(&log.lock().expect("the log")).into_owned();
    assert!(
        log.contains("Model 'qwen3-0.6b' is split across 2 GPUs on 2 nodes"),
        "the head's log: {log}"
    );
    let got = replies(&*split).await;

    assert_eq!(
        got, expected,
        "the split model's replies against one node's"
    );
    assert!(
        got.iter().any(|reply| reply.contains("Paris")),
        "the replies answer the prompts: {got:?}"
    );
}
