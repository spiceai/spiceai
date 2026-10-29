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

use std::path::{Path, PathBuf};

use llms::chat::gguf_files::{self, ListedFile};
use llms::chat::hf_files::{Budget, download_model};
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
