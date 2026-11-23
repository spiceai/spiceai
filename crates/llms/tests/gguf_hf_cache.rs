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

//! A GGUF model's `files` resolved against what the Hugging Face cache holds, with the Hub out
//! of reach: the repository is listed from the cached files alone.
//!
//! Selected in the `nextest` gate (see `NEXTEST_FILTER` in the Makefile), like the other
//! credential-free `llms` integration tests.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use llms::chat::gguf_files::{self, ListedFile};
use llms::chat::hf_files::{Budget, download_model, model_architecture};
use llms::chat::{DistributedBackend, DistributedConfig, ReasoningDefault, create_hf_model};
use llms::hf::HubParams;

const REPO_ID: &str = "spiceai-test/gguf-not-on-the-hub";

const WHY: &str =
    "this test sets the process-wide `HF_HUB_CACHE` and `HF_HUB_OFFLINE` environment variables";

/// A quantization pinned while the Hub listed the whole repository loads again from a cache
/// that holds only what the pin downloaded.
///
/// Two quantizations in one folder go by their tags, so the pin names `Q4_K_M`. With the Hub
/// out of reach the repository is listed from the cache, where the folder holds only that
/// quantization; the pin must still name it, or a restart without egress cannot load a model
/// it already downloaded.
#[tokio::test]
async fn a_quantization_pinned_online_loads_from_a_cache_holding_only_it() {
    crate::require_process_per_test(WHY);
    let repository = listing(&[("gguf/Model-Q4_K_M.gguf", 4), ("gguf/Model-Q8_0.gguf", 8)]);
    let q4 = gguf_files::quantizations(&repository)
        .into_iter()
        .find(|q| q.files == ["gguf/Model-Q4_K_M.gguf"])
        .expect("the repository offers Q4_K_M");
    let pinned = gguf_files::pin(&repository, &q4);
    assert_eq!(pinned, ["Q4_K_M"]);

    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "gguf/Model-Q4_K_M.gguf");

    let files: Vec<&str> = pinned.iter().map(String::as_str).collect();
    let downloaded = download_model(
        REPO_ID,
        &files,
        Budget::Unmeasured,
        HubParams::default(),
        "test",
    )
    .await
    .unwrap_or_else(|e| panic!("the pin {pinned:?} must load from the cache: {e}"));
    assert_eq!(downloaded.gguf, ["gguf/Model-Q4_K_M.gguf"]);
}

/// A quantization pinned while the Hub listed the whole repository loads again from a cache
/// that also holds part of another folder, one named like the quantization's tag.
///
/// Online, `Q4_K_M/` holds two quantizations, so the folder names neither; in a cache holding
/// only one of them it would name that one, and a pin of the tag `Q4_K_M` would load it
/// instead of the file the pin downloaded.
#[tokio::test]
async fn a_pin_is_not_taken_by_a_folder_another_quantization_has_alone_in_the_cache() {
    crate::require_process_per_test(WHY);
    let repository = listing(&[
        ("gguf/Model-Q4_K_M.gguf", 4),
        ("gguf/Model-Q8_0.gguf", 8),
        ("Q4_K_M/Other-Q2_K.gguf", 2),
        ("Q4_K_M/Other-Q8_0.gguf", 8),
    ]);
    let q4 = gguf_files::quantizations(&repository)
        .into_iter()
        .find(|q| q.files == ["gguf/Model-Q4_K_M.gguf"])
        .expect("the repository offers gguf/Model-Q4_K_M.gguf");
    let pinned = gguf_files::pin(&repository, &q4);

    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "gguf/Model-Q4_K_M.gguf");
    write_file(&snapshot, "Q4_K_M/Other-Q2_K.gguf");

    let files: Vec<&str> = pinned.iter().map(String::as_str).collect();
    let downloaded = download_model(
        REPO_ID,
        &files,
        Budget::Unmeasured,
        HubParams::default(),
        "test",
    )
    .await
    .unwrap_or_else(|e| panic!("the pin {pinned:?} must load from the cache: {e}"));
    assert_eq!(
        downloaded.gguf,
        ["gguf/Model-Q4_K_M.gguf"],
        "pin {pinned:?}"
    );
}

/// Every shard of a draft head published as a series is downloaded, shard 1 first, which the
/// engine opens: whether `files` names every shard, one shard, or a quantization that brings
/// the head.
#[tokio::test]
async fn every_shard_of_a_sharded_draft_head_is_downloaded() {
    const HEAD_1: &str = "MTP/mtp-Model-Q8_0-00001-of-00002.gguf";
    const HEAD_2: &str = "MTP/mtp-Model-Q8_0-00002-of-00002.gguf";
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    for file in ["Model-Q4_K_M.gguf", HEAD_1, HEAD_2] {
        write_file(&snapshot, file);
    }

    for files in [
        &["Model-Q4_K_M.gguf", HEAD_1, HEAD_2][..],
        &["Model-Q4_K_M.gguf", HEAD_2],
        &["Q4_K_M"],
    ] {
        let downloaded = download_model(
            REPO_ID,
            files,
            Budget::Unmeasured,
            HubParams::default(),
            "test",
        )
        .await
        .unwrap_or_else(|e| panic!("{files:?} must load from the cache: {e}"));
        assert_eq!(
            downloaded.gguf,
            ["Model-Q4_K_M.gguf", HEAD_1, HEAD_2],
            "{files:?}"
        );
    }
}

/// A repository that offers safetensors loads them when `files` names nothing, offline as
/// online, even when the cache holds one of its GGUFs and the safetensors index without the
/// shards: loading the GGUF there would serve other weights than the same model serves online.
#[tokio::test]
async fn a_model_with_no_files_keeps_its_safetensors_offline() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "Model-Q4_K_M.gguf");
    std::fs::write(
        snapshot.join("model.safetensors.index.json"),
        br#"{"metadata":{},"weight_map":{"embed":"model-00001-of-00001.safetensors"}}"#,
    )
    .expect("writes the safetensors index");
    std::fs::write(snapshot.join("config.json"), b"{}").expect("writes the config");
    std::fs::write(snapshot.join("tokenizer.json"), b"{}").expect("writes the tokenizer");

    let error = download_model(
        REPO_ID,
        &[],
        Budget::Unmeasured,
        HubParams::default(),
        "test",
    )
    .await
    .expect_err("the safetensors shards are not in the cache");
    // the shard the safetensors index names is what is missing, not a GGUF
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download model 'test' from Hugging Face repo '{REPO_ID}' (revision 'main'): `HF_HUB_OFFLINE` is set and the Hugging Face cache at '{}' does not hold 'model-00001-of-00001.safetensors', which 'model.safetensors.index.json' names. Run `hf download {REPO_ID} --revision main` while online, or unset `HF_HUB_OFFLINE`. See: https://spiceai.org/docs/components/models/huggingface",
            cache.path().display()
        )
    );
}

/// With the Hub out of reach, what the cache lacks is a gap in the cache, not a mistake in the
/// Spicepod: the error says only the cache was read and how to fill it, and Spice retries the
/// load. A file `files` names that the cache lacks, and, for a model with no `files`, a cache
/// whose quantizations none fit the memory, where the Hub may offer a smaller one.
#[tokio::test]
async fn what_the_cache_lacks_is_retried_and_says_how_to_fill_it() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "Model-Q4_K_M.gguf");
    let fill = format!(
        "Run `hf download {REPO_ID} --revision main` while online, or unset `HF_HUB_OFFLINE`. See: https://spiceai.org/docs/components/models/huggingface"
    );
    let read = format!(
        "Failed to download model 'test' from Hugging Face repo '{REPO_ID}' (revision 'main'): `HF_HUB_OFFLINE` is set, so Spice read only the Hugging Face cache at '{}', where",
        cache.path().display()
    );

    write_file(&snapshot, "Model-Q2_K-00001-of-00002.gguf");
    write_file(&snapshot, "dspark-Model-Q8_0.gguf");

    // each says what is not in the cache, and nothing of the repository it did not read
    for (files, budget, why) in [
        (
            &["Model-Q8_0.gguf"][..],
            Budget::Unmeasured,
            "there is no file 'Model-Q8_0.gguf'",
        ),
        (
            &["Model-Q2_K-00001-of-00002.gguf"][..],
            Budget::Unmeasured,
            "'Model-Q2_K-00001-of-00002.gguf' is a shard of a series whose shards Model-Q2_K-00002-of-00002.gguf are not there",
        ),
        (
            &["Q3_K_M"][..],
            Budget::Unmeasured,
            "there is no file or quantization 'Q3_K_M'; the quantizations there are Model-Q4_K_M.gguf",
        ),
        // the repository may hold a Q8_0 of the model beside its drafter's
        (
            &["Q8_0"][..],
            Budget::Unmeasured,
            "'Q8_0' names only dspark-Model-Q8_0.gguf, which Spice, like llama.cpp, takes by name for an importance matrix or a drafter, not the model's weights; to load one as the weights, name its path in `files`",
        ),
        (
            &[],
            Budget::Free(1),
            "none of its GGUF quantizations fits the 0.0 GiB of memory free for the model with room for the context; the smallest, Model-Q4_K_M.gguf, is 0.0 GB. Name one in the model's `files` to load it anyway, across machines with `nodes` if one cannot hold it: Model-Q4_K_M.gguf (0.0 GB)",
        ),
    ] {
        let error = download_model(REPO_ID, files, budget, HubParams::default(), "test")
            .await
            .expect_err("the cache holds no file the model can load");
        assert_eq!(
            error.to_string(),
            format!("{read} {why}. {fill}"),
            "{files:?}"
        );
        assert!(!error.is_configuration_error(), "{files:?}");
    }
}

/// A `DFlash` drafter, which the engine tells by its header, can be named beside the model's
/// weights under a name that does not say it is one: its header is read before two sets of
/// weights are refused, and it goes after the weights, which a model opens from. Two sets of
/// weights the headers do not tell apart are refused.
#[tokio::test]
async fn a_drafter_told_by_its_header_can_be_named_beside_the_weights() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_gguf(&snapshot, "Model-Q4_K_M.gguf", "qwen3");
    write_gguf(&snapshot, "Model-Q8_0.gguf", "qwen3");
    write_gguf(&snapshot, "assistant.gguf", "dflash");

    for files in [
        &["Q4_K_M", "assistant.gguf"][..],
        &["assistant.gguf", "Q4_K_M"],
    ] {
        let downloaded = download_model(
            REPO_ID,
            files,
            Budget::Unmeasured,
            HubParams::default(),
            "test",
        )
        .await
        .unwrap_or_else(|e| panic!("{files:?} must load the weights and the drafter: {e}"));
        assert_eq!(
            downloaded.gguf,
            ["Model-Q4_K_M.gguf", "assistant.gguf"],
            "{files:?}"
        );
    }

    let error = download_model(
        REPO_ID,
        &["Q4_K_M", "Model-Q8_0.gguf"],
        Budget::Unmeasured,
        HubParams::default(),
        "test",
    )
    .await
    .expect_err("two sets of weights");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download model 'test' from Hugging Face repo '{REPO_ID}' (revision 'main'): `HF_HUB_OFFLINE` is set, so Spice read only the Hugging Face cache at '{}', where `files` names 2 sets of model weights there, Model-Q4_K_M.gguf, Model-Q8_0.gguf, and a model loads one; name one of them. Run `hf download {REPO_ID} --revision main` while online, or unset `HF_HUB_OFFLINE`. See: https://spiceai.org/docs/components/models/huggingface",
            cache.path().display()
        )
    );
    assert!(!error.is_configuration_error());
}

/// A file named beside the weights whose name says it is no weights, as a `dspark-` drafter's, is
/// told by its header: a `dflash` one drafts, in place of the MTP head the quantization would
/// bring, and one whose header names a model's architecture leaves the head to draft.
#[tokio::test]
async fn a_drafter_named_beside_the_weights_is_told_by_its_header() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_gguf(&snapshot, "Model-Q4_K_M.gguf", "qwen3");
    write_gguf(&snapshot, "mtp-Model-Q8_0.gguf", "qwen3");
    write_gguf(&snapshot, "dspark-Model-Q8_0.gguf", "dflash");
    write_gguf(&snapshot, "dspark-Other-Q8_0.gguf", "qwen3");

    for (files, gguf) in [
        (
            &["Q4_K_M", "dspark-Model-Q8_0.gguf"][..],
            &["Model-Q4_K_M.gguf", "dspark-Model-Q8_0.gguf"][..],
        ),
        (
            &["Q4_K_M", "dspark-Other-Q8_0.gguf"],
            &[
                "Model-Q4_K_M.gguf",
                "dspark-Other-Q8_0.gguf",
                "mtp-Model-Q8_0.gguf",
            ],
        ),
    ] {
        let downloaded = download_model(
            REPO_ID,
            files,
            Budget::Unmeasured,
            HubParams::default(),
            "test",
        )
        .await
        .unwrap_or_else(|e| panic!("{files:?} must load: {e}"));
        assert_eq!(downloaded.gguf, gguf, "{files:?}");
    }
}

/// A name several quantizations in the cache answer to says so of the cache, and to name one of
/// them, and is retried: the Hub's listing, once read, can make the name answer to one
/// (`gguf_files::tests::a_name_can_be_ambiguous_in_the_cache_alone`).
#[tokio::test]
async fn a_name_several_quantizations_in_the_cache_answer_to_says_to_name_one() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "gguf/Model-Q8_0.gguf");
    write_file(&snapshot, "gguf/Model-Q4_K_M.gguf");

    let error = download_model(
        REPO_ID,
        &["gguf"],
        Budget::Unmeasured,
        HubParams::default(),
        "test",
    )
    .await
    .expect_err("two quantizations answer to the folder");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download model 'test' from Hugging Face repo '{REPO_ID}' (revision 'main'): `HF_HUB_OFFLINE` is set, so Spice read only the Hugging Face cache at '{}', where 'gguf' matches 2 quantizations there, gguf/Model-Q4_K_M.gguf, gguf/Model-Q8_0.gguf; name one of them. Run `hf download {REPO_ID} --revision main` while online, or unset `HF_HUB_OFFLINE`. See: https://spiceai.org/docs/components/models/huggingface",
            cache.path().display()
        )
    );
    assert!(!error.is_configuration_error());
}

/// A split model with no `files` loads, with the Hub out of reach, the default of what the
/// cache holds, which need not be the repository's: here its `Q8_0`, the cache holding no
/// `Q4_K_M`.
#[tokio::test]
async fn a_model_with_no_files_loads_the_default_of_what_the_cache_holds() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "Model-Q8_0.gguf");
    write_file(&snapshot, "Model-Q2_K.gguf");

    let downloaded = download_model(REPO_ID, &[], Budget::Split, HubParams::default(), "test")
        .await
        .unwrap_or_else(|e| panic!("the cached Q8_0 loads: {e}"));
    assert_eq!(downloaded.gguf, ["Model-Q8_0.gguf"]);
}

/// A model split across nodes, with the Hub out of reach, resolves its files from each node's
/// own cache. The node that loads the weights logs what the model loads. A node that reads only
/// their architecture, as a rank other than 0 does, logs what it resolved at debug, never as
/// what the model loads: the head sends it its share of the head's weights, which here are
/// `Q4_K_M` while this node's cache holds only `Q8_0`.
#[tokio::test]
async fn only_the_node_that_loads_the_weights_says_what_the_model_loads() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "Model-Q8_0.gguf");
    let line = "llms::chat::hf_files: Model 'test' loads quantization Q8_0 (1 file, 0.0 GB) from Hugging Face repo 'spiceai-test/gguf-not-on-the-hub', the quantization a model with no `files` loads: its Q8_0, as llama.cpp loads it when a repository has no Q4_K_M. `HF_HUB_OFFLINE` is set, so Spice chose among what the Hugging Face cache holds, and a load that reads the Hub may choose another. The cache holds no other complete GGUF quantization of the repo.";

    let (downloaded, log) = logged(download_model(
        REPO_ID,
        &[],
        Budget::Split,
        HubParams::default(),
        "test",
    ))
    .await;
    assert_eq!(
        downloaded.expect("the cached Q8_0 loads").gguf,
        ["Model-Q8_0.gguf"]
    );
    assert_eq!(resolution_lines(&log), [format!(" INFO {line}")], "{log}");

    let (resolved, log) = logged(model_architecture(
        REPO_ID,
        &[],
        Budget::Split,
        HubParams::default(),
        "test",
    ))
    .await;
    assert_eq!(
        resolved.expect("the cached Q8_0 is read").gguf,
        ["Model-Q8_0.gguf"]
    );
    assert_eq!(resolution_lines(&log), [format!("DEBUG {line}")], "{log}");
}

/// A model split across nodes that each load its weights, as mistral.rs splits one, does not
/// choose its quantization among what one node's cache holds: another node's cache need not
/// hold the same, and the nodes would then each load different weights. Named in `files`, the
/// quantization gets past the refusal, to the engine. No engine that splits a model by sending
/// each node its share is linked here, so the model goes to mistral.rs.
#[tokio::test]
async fn a_split_model_whose_nodes_each_load_the_weights_does_not_choose_from_a_cache() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "Model-Q8_0.gguf");

    let error = load_split(&[], HubParams::default())
        .await
        .err()
        .expect("a quantization chosen from the cache is refused");
    assert_eq!(
        error.to_string(),
        "Model 'test' is split across nodes by mistral.rs, which has each node load the weights itself, and `HF_HUB_OFFLINE` is set, so this node chose quantization Q8_0 among what its Hugging Face cache holds, and another node's cache need not hold the same. Name the quantization in the model's `files`, so that every node loads the same quantization: `files: [{path: Model-Q8_0.gguf}]`. See: https://spiceai.org/docs/components/models/huggingface"
    );
    assert!(
        error.is_configuration_error(),
        "with `HF_HUB_OFFLINE` set, no retry reads the Hub"
    );

    let named = load_split(&["Q8_0"], HubParams::default())
        .await
        .err()
        .expect("the placeholder file is not a model mistral.rs loads");
    assert!(
        !matches!(named, llms::chat::Error::SplitModelChoseFromCache { .. }),
        "a quantization named in `files` is loaded as named: {named}"
    );
}

/// With the Hub out of reach, the same refusal is retried: a later load can read the Hub's
/// listing, which every node reads alike.
#[tokio::test]
async fn a_split_model_that_chose_from_the_cache_in_an_outage_is_retried() {
    crate::require_process_per_test(WHY);
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    write_file(&snapshot, "Model-Q8_0.gguf");
    // SAFETY: `require_process_per_test` established that no other thread reads or writes the
    // environment.
    unsafe { std::env::remove_var("HF_HUB_OFFLINE") };
    // Nothing listens on the discard port of the loopback address.
    let hub = HubParams {
        endpoint: Some("http://127.0.0.1:9"),
        ..HubParams::default()
    };

    let error = load_split(&[], hub)
        .await
        .err()
        .expect("a quantization chosen from the cache is refused");
    let message = error.to_string();
    assert!(
        message.starts_with("Model 'test' is split across nodes by mistral.rs, which has each node load the weights itself, and the Hugging Face Hub could not be reached ("),
        "{message}"
    );
    assert!(
        message.ends_with("), so this node chose quantization Q8_0 among what its Hugging Face cache holds, and another node's cache need not hold the same. Name the quantization in the model's `files`, so that every node loads the same quantization: `files: [{path: Model-Q8_0.gguf}]`. See: https://spiceai.org/docs/components/models/huggingface"),
        "{message}"
    );
    assert!(
        !error.is_configuration_error(),
        "a later load can reach the Hub"
    );
}

/// Loads [`REPO_ID`] with `files`, split across two nodes as the head, the model named `test`.
async fn load_split(
    files: &[&str],
    hub: HubParams<'_>,
) -> llms::chat::Result<Arc<dyn llms::chat::Chat>> {
    create_hf_model(
        REPO_ID,
        None,
        files,
        hub,
        None,
        Some(DistributedConfig {
            backend: DistributedBackend::Ring,
            node_rank: 0,
            nodes: vec!["127.0.0.1".to_string(), "127.0.0.2".to_string()],
            allow_insecure_connections: true,
            head_url: None,
        }),
        ReasoningDefault {
            model_name: "test",
            effort: None,
        },
    )
    .await
}

/// `future`'s output, and what it logged at debug and above, without times or colours.
async fn logged<T>(future: impl Future<Output = T>) -> (T, String) {
    let log = Arc::new(Mutex::new(Vec::<u8>::new()));
    let writer = Arc::clone(&log);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || Capture(Arc::clone(&writer)))
        .with_ansi(false)
        .without_time()
        .finish();
    let output = {
        let _capture = tracing::subscriber::set_default(subscriber);
        future.await
    };
    let log = String::from_utf8_lossy(&log.lock().expect("the log")).into_owned();
    (output, log)
}

/// The lines of `log` the resolver wrote.
fn resolution_lines(log: &str) -> Vec<String> {
    log.lines()
        .filter(|line| line.contains("llms::chat::hf_files:"))
        .map(ToString::to_string)
        .collect()
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

fn listing(files: &[(&str, u64)]) -> Vec<ListedFile> {
    files
        .iter()
        .map(|&(path, size)| ListedFile {
            path: path.to_string(),
            size,
        })
        .collect()
}

/// Writes a placeholder for `path` in the snapshot: resolving reads only the listing.
fn write_file(snapshot: &Path, path: &str) {
    let file = snapshot.join(path);
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir).expect("creates the file's folder");
    }
    std::fs::write(&file, b"GGUF").expect("writes the cached file");
}

/// Writes a GGUF for `path` in the snapshot whose header names `architecture`, as a drafter's
/// (`dflash`) or a model's, and holds nothing else.
fn write_gguf(snapshot: &Path, path: &str, architecture: &str) {
    let string = |bytes: &mut Vec<u8>, text: &str| {
        bytes.extend_from_slice(&(text.len() as u64).to_le_bytes());
        bytes.extend_from_slice(text.as_bytes());
    };
    let mut bytes = b"GGUF".to_vec();
    bytes.extend_from_slice(&3u32.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes());
    string(&mut bytes, "general.architecture");
    bytes.extend_from_slice(&8u32.to_le_bytes());
    string(&mut bytes, architecture);
    let file = snapshot.join(path);
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir).expect("creates the file's folder");
    }
    std::fs::write(&file, bytes).expect("writes the cached GGUF");
}

/// Lays out an empty snapshot of [`REPO_ID`] at `main` in the Hugging Face cache `cache`,
/// points `HF_HUB_CACHE` at it and sets `HF_HUB_OFFLINE`, and returns the snapshot directory.
///
/// Call it only after `require_process_per_test`, which establishes that no other thread
/// reads or writes the environment.
fn offline_snapshot(cache: &Path) -> PathBuf {
    // `huggingface_hub`'s on-disk layout: `refs/<revision>` holds the commit hash, and the
    // files live under `snapshots/<commit hash>/`.
    let repo_dir = cache.join(format!("models--{}", REPO_ID.replace('/', "--")));
    let commit = "0000000000000000000000000000000000000000";
    let snapshot = repo_dir.join("snapshots").join(commit);
    std::fs::create_dir_all(&snapshot).expect("creates the snapshot directory");
    std::fs::create_dir_all(repo_dir.join("refs")).expect("creates the refs directory");
    std::fs::write(repo_dir.join("refs").join("main"), commit).expect("writes the ref");

    // SAFETY: the caller has established, through `require_process_per_test`, that this test
    // runs in a process of its own, so no other thread is reading or writing the environment
    // while this runs.
    unsafe {
        std::env::set_var("HF_HUB_CACHE", cache);
        std::env::set_var("HF_HUB_OFFLINE", "1");
    }
    snapshot
}
