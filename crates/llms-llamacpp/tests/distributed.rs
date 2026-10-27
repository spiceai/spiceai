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
//! again as rank 0, which serves the model, and again as rank 1, which lends this machine's GPU
//! over loopback. Every rank runs in a process of its own, as the GPU server a process starts
//! lives as long as the process. Its greedy replies must equal the single-node model's: the ranks
//! sum their partials exactly, so the split changes only the order of float additions, which
//! one-answer prompts don't feel.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_openai::error::OpenAIError;
use llms::chat::{
    Chat, DistributedBackend, DistributedConfig, Error, LocalModelOptions, ReasoningDefault,
};
use serde_json::json;

use crate::{FILE, MODEL, REVISION, request};

/// Set in rank 1's environment: there `rank_one` lends the GPU instead of returning.
const RANK_ONE: &str = "SPICE_LLAMACPP_TEST_RANK_ONE";

/// Set in the environment of the process `rank_two_of_three` runs in.
const RANK_TWO: &str = "SPICE_LLAMACPP_TEST_RANK_TWO";

/// Set in the environment of the process `head_of_two_ranks` runs in.
const HEAD_OF_TWO: &str = "SPICE_LLAMACPP_TEST_HEAD_OF_TWO";

/// Set in the environment of the process `head_with_a_silent_rank` runs in.
const SILENT_RANK: &str = "SPICE_LLAMACPP_TEST_SILENT_RANK";

/// Set in the environment of the process `rank_three_on_a_taken_port` runs in.
const PORT_TAKEN: &str = "SPICE_LLAMACPP_TEST_PORT_TAKEN";

/// `world` nodes on this machine, as spiced sees them with `--allow-insecure-connections`,
/// which a model split over llama.cpp RPC needs.
fn topology(node_rank: usize, world: usize) -> DistributedConfig {
    DistributedConfig {
        backend: DistributedBackend::Ring,
        node_rank,
        nodes: vec!["127.0.0.1".to_string(); world],
        allow_insecure_connections: true,
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

/// Rank 1, run only by `head_of_two_ranks` in a process of its own: rank 1's load lends this
/// machine's GPU, which stays served after the load, so this pends until the head kills it.
/// Without `RANK_ONE` it does nothing.
#[tokio::test]
async fn rank_one() {
    if std::env::var_os(RANK_ONE).is_none() {
        return;
    }
    let cfg = topology(1, 2);
    // rank 1 never reads the weights; the head sends them
    let weights = [PathBuf::from(FILE)];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    let _share = (engine.load)(&weights, options(Some(&cfg)))
        .await
        .expect("rank 1 lends its GPU");
    std::future::pending::<()>().await;
}

/// Rank 2 of three, run only by `a_rank_other_than_0_loads_its_share_and_names_the_head` in a
/// process of its own, as the GPU a process serves is the process's own. Without `RANK_TWO` it
/// does nothing.
#[tokio::test]
async fn rank_two_of_three() {
    if std::env::var_os(RANK_TWO).is_none() {
        return;
    }
    let cfg = topology(2, 3);
    let weights = [PathBuf::from(FILE)];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    // no head ever reaches this rank, and its load still finishes
    let share = tokio::time::timeout(
        Duration::from_secs(60),
        (engine.load)(&weights, options(Some(&cfg))),
    )
    .await
    .expect("rank 2's load finishes without waiting for the head")
    .expect("rank 2 lends its GPU");

    share.health().await.expect("a lent GPU's share is healthy");
    std::net::TcpStream::connect("127.0.0.1:12347")
        .expect("rank 2 serves its GPU at port 12345 + 2");
    let refusal = share
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "What is the capital of France?" }],
        })))
        .await
        .expect_err("rank 2 serves no requests");
    let OpenAIError::ApiError(refusal) = refusal else {
        panic!("rank 2 refuses with an API error: {refusal:?}");
    };
    assert_eq!(
        refusal.message,
        "Model 'qwen3-0.6b' is served by the first machine in 'nodes' (127.0.0.1); this machine holds part of it."
    );
    assert_eq!(refusal.r#type.as_deref(), Some("invalid_request_error"));

    // the share is the engine's to return: loaded as a model of its own, a rank is refused
    let Err(own) = llms_llamacpp::LlamaCpp::from(&weights, options(Some(&cfg))).await else {
        panic!("rank 2 loaded a model of its own");
    };
    assert_eq!(
        own.to_string(),
        "Failed to load the model. An error occurred: Model 'qwen3-0.6b' is a share held for the first node in `nodes` on this node (rank 2), so it loads through the engine, not as a model of its own. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    );
}

/// Rank 3 of four, run only by `a_rank_whose_port_is_taken_fails_until_it_is_free` in a process
/// of its own, as the GPU a process serves is the process's own. Without `PORT_TAKEN` it does
/// nothing.
#[tokio::test]
async fn rank_three_on_a_taken_port() {
    if std::env::var_os(PORT_TAKEN).is_none() {
        return;
    }
    let cfg = topology(3, 4);
    let weights = [PathBuf::from(FILE)];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    let taken = std::net::TcpListener::bind("127.0.0.1:12348").expect("port 12345 + 3 is free");
    let Err(error) = (engine.load)(&weights, options(Some(&cfg))).await else {
        panic!("rank 3 reported its GPU served while another listener held its port");
    };
    let message = error.to_string();
    assert!(
        message.contains(
            "Model 'qwen3-0.6b' could not serve this node's GPU at 127.0.0.1:12348: Address already in use"
        ),
        "{message}"
    );
    drop(taken);
    // a failed start is not remembered: once the port is free, the same load serves the GPU
    let share = (engine.load)(&weights, options(Some(&cfg)))
        .await
        .expect("rank 3 serves its GPU once its port is free");
    share
        .health()
        .await
        .expect("a served GPU's share is healthy");
}

/// A rank other than 0 whose port another process holds fails its load, rather than report
/// its GPU served while llama.cpp's server could not bind, and loads once the port is free.
#[tokio::test]
async fn a_rank_whose_port_is_taken_fails_until_it_is_free() {
    let status = Command::new(std::env::current_exe().expect("the test binary's path"))
        .args([
            "--exact",
            "distributed::rank_three_on_a_taken_port",
            "--nocapture",
        ])
        .env(PORT_TAKEN, "1")
        .status()
        .expect("rank 3 runs");
    assert!(status.success(), "rank 3's checks failed: {status}");
}

/// A rank other than 0 finishes loading once it serves its GPU: a load that never finished
/// kept every model the Spicepod lists after it from loading.
#[tokio::test]
async fn a_rank_other_than_0_loads_its_share_and_names_the_head() {
    let status = Command::new(std::env::current_exe().expect("the test binary's path"))
        .args(["--exact", "distributed::rank_two_of_three", "--nocapture"])
        .env(RANK_TWO, "1")
        .status()
        .expect("rank 2 runs");
    assert!(status.success(), "rank 2's checks failed: {status}");
}

/// Neither the head nor another rank serves or reaches a GPU over llama.cpp RPC unless spiced
/// runs with `--allow-insecure-connections`, as the connections between them are not
/// authenticated. The refusal comes before either one listens or connects, so it is quick.
#[tokio::test]
async fn a_split_model_needs_insecure_connections_allowed() {
    let weights = [local_gguf().await];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    for rank in [0, 1] {
        let cfg = DistributedConfig {
            allow_insecure_connections: false,
            ..topology(rank, 2)
        };
        let Err(error) = tokio::time::timeout(
            Duration::from_secs(10),
            (engine.load)(&weights, options(Some(&cfg))),
        )
        .await
        .unwrap_or_else(|_| panic!("rank {rank} is refused before it serves or reaches a GPU")) else {
            panic!("rank {rank} loaded without `--allow-insecure-connections`");
        };
        assert!(
            matches!(&error, Error::InsecureConnectionsNotAllowed { model } if model == "qwen3-0.6b"),
            "rank {rank}: {error:?}"
        );
        assert_eq!(
            error.to_string(),
            "Model 'qwen3-0.6b' is split across nodes over connections that are neither authenticated nor encrypted, so it loads only when spiced runs with `--allow-insecure-connections`. Start spiced with `--allow-insecure-connections` on every node in `nodes`, and only on a trusted private network: any host that reaches a node's GPU server can use that GPU.",
            "rank {rank}"
        );
        // only restarting spiced with the flag clears it, so the runtime does not retry it
        assert!(error.is_configuration_error(), "rank {rank}");
    }
}

/// A GGUF holding only a header that names `arch`, as a model's first shard starts.
fn header_only(dir: &std::path::Path, name: &str, arch: &str) -> PathBuf {
    let string = |bytes: &mut Vec<u8>, text: &str| {
        let len = u64::try_from(text.len()).expect("a short string");
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(text.as_bytes());
    };
    let mut bytes = b"GGUF".to_vec();
    bytes.extend_from_slice(&3u32.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes());
    string(&mut bytes, "general.architecture");
    bytes.extend_from_slice(&8u32.to_le_bytes());
    string(&mut bytes, arch);
    let path = dir.join(name);
    std::fs::write(&path, bytes).expect("the GGUF is written");
    path
}

/// A model split across nodes goes to llama.cpp only when llama.cpp splits its architecture
/// by tensor, which every node decides from the first shard's header: `glm-dsa`, which it
/// cannot split, stays on mistral.rs's ring, as it was before llama.cpp served split models.
#[tokio::test]
async fn only_an_architecture_llama_cpp_splits_is_split_by_it() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let glm = [
        header_only(dir.path(), "GLM-UD-Q2_K_XL-00001-of-00002.gguf", "glm-dsa"),
        dir.path().join("GLM-UD-Q2_K_XL-00002-of-00002.gguf"),
    ];
    let qwen = [header_only(dir.path(), "Qwen3-Q8_0.gguf", "qwen3")];
    let engine = llms::chat::engine::engine_for(&qwen).expect("a .gguf goes to llama.cpp");
    assert_eq!(engine.name, "llama.cpp");

    assert!(
        !engine.splits_weights(&glm).await,
        "glm-dsa stays on the ring"
    );
    assert!(
        engine.splits_weights(&qwen).await,
        "qwen3 is split by llama.cpp"
    );
    // a rank without the weights cannot be a ring member, which reads them on every node
    let absent = [dir.path().join("absent-00001-of-00002.gguf")];
    assert!(
        engine.splits_weights(&absent).await,
        "a rank without the weights"
    );
}

/// A rank other than 0 of a Hugging Face model reads the first shard's architecture from the
/// Hub with a range read, so it decides as the head does without downloading the weights.
#[tokio::test]
async fn a_rank_reads_the_architecture_from_the_hub() {
    let arch = llms::chat::hf_files::model_architecture(
        "unsloth/Qwen3-0.6B-GGUF:50968a4468ef4233ed78cd7c3de230dd1d61a56b",
        &["Qwen3-0.6B-Q6_K.gguf"],
        llms::hf::HubParams::default(),
        "qwen3-0.6b",
    )
    .await
    .expect("the header is read");
    assert_eq!(arch.as_deref(), Some("qwen3"));
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

/// The head of `a_model_split_across_two_ranks_answers_as_on_one`, run only by it in a process
/// of its own. Without `HEAD_OF_TWO` it does nothing.
#[tokio::test]
async fn head_of_two_ranks() {
    if std::env::var_os(HEAD_OF_TWO).is_none() {
        return;
    }
    let gguf = local_gguf().await;
    let weights = [gguf];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    assert!(
        engine.splits_weights(&weights).await,
        "the llama.cpp engine splits Qwen3 across nodes"
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
    let cfg = topology(0, 2);
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

/// Serializes the tests whose head runs in a process of its own: a head serves its own GPU at
/// port 12345 and reaches rank 1 at 12346, which only one head at a time can use.
static HEADS: Mutex<()> = Mutex::new(());

/// Runs the test `name` in a process of its own with `env` set, holding [`HEADS`].
fn run_head(name: &str, env: &str) -> std::process::ExitStatus {
    let _head = HEADS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Command::new(std::env::current_exe().expect("the test binary's path"))
        .args(["--exact", name, "--nocapture"])
        .env(env, "1")
        .status()
        .expect("the head runs")
}

#[tokio::test]
async fn a_model_split_across_two_ranks_answers_as_on_one() {
    let status = run_head("distributed::head_of_two_ranks", HEAD_OF_TWO);
    assert!(status.success(), "the head's checks failed: {status}");
}

/// The head of two ranks whose rank 1 port accepts connections and never answers, run only by
/// `a_rank_that_never_answers_does_not_stall_the_head` in a process of its own. It ends the
/// process itself, with the outcome as its exit status: the connection attempt stuck in
/// llama.cpp's handshake never returns, and the runtime would wait for it. Without
/// `SILENT_RANK` it does nothing.
#[tokio::test]
async fn head_with_a_silent_rank() {
    if std::env::var_os(SILENT_RANK).is_none() {
        return;
    }
    let silent = std::net::TcpListener::bind("127.0.0.1:12346").expect("port 12345 + 1 is free");
    std::thread::spawn(move || {
        // holds every connection open without a byte in reply
        let held: Vec<_> = silent.incoming().collect();
        drop(held);
    });
    let weights = [local_gguf().await];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    let cfg = topology(0, 2);
    let log = Arc::new(Mutex::new(Vec::<u8>::new()));
    let writer = Arc::clone(&log);
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || Capture(Arc::clone(&writer)))
        .with_ansi(false)
        .finish();
    let outcome = {
        let _capture = tracing::subscriber::set_default(subscriber);
        tokio::time::timeout(
            Duration::from_secs(40),
            (engine.load)(&weights, options(Some(&cfg))),
        )
        .await
    };
    let Err(_) = outcome else {
        panic!("the head loaded without rank 1 answering");
    };
    let log = String::from_utf8_lossy(&log.lock().expect("the log")).into_owned();
    // the note the wait loop logs every 30 s, which a loop stuck in the handshake never reaches;
    // a failure exits too, as the runtime would wait for the stuck attempt before a panic ends
    // the test
    let noted = log.contains(
        "Model 'qwen3-0.6b' is waiting for rank 1 at 127.0.0.1:12346 to serve its GPU before it loads",
    );
    if !noted {
        eprintln!("the head never noted what it waits for; its log: {log}");
    }
    std::process::exit(i32::from(!noted));
}

/// A rank whose port accepts connections and never answers llama.cpp's handshake does not stop
/// the head's wait loop, which goes on noting what it waits for and fails at its deadline.
#[tokio::test]
async fn a_rank_that_never_answers_does_not_stall_the_head() {
    let status = run_head("distributed::head_with_a_silent_rank", SILENT_RANK);
    assert!(status.success(), "the head's checks failed: {status}");
}
