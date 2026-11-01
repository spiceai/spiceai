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

//! A sharded safetensors model loaded from the Hugging Face cache with the Hub out of reach.
//!
//! Selected in the `nextest` gate (see `NEXTEST_FILTER` in the Makefile), like the other
//! credential-free `llms` integration tests.

use std::path::{Path, PathBuf};

use llms::chat::hf_files::{Budget, download_model};
use llms::hf::HubParams;

const REPO_ID: &str = "spiceai-test/safetensors-not-on-the-hub";

/// A shard the cached safetensors index names but the cache does not hold is reported as
/// missing from the cache, with the fix for an offline load, not as missing from the
/// repository: the repository may well have it, and only the cache is read.
#[tokio::test]
async fn a_shard_the_cache_lacks_is_reported_as_not_cached() {
    crate::require_process_per_test(
        "this test sets the process-wide `HF_HUB_CACHE` and `HF_HUB_OFFLINE` environment variables",
    );
    let cache = tempfile::tempdir().expect("creates a cache directory");
    let snapshot = offline_snapshot(cache.path());
    for (file, contents) in [
        ("config.json", "{}"),
        ("tokenizer.json", "{}"),
        (
            "model.safetensors.index.json",
            r#"{"metadata":{},"weight_map":{"embed":"model-00001-of-00002.safetensors","head":"model-00002-of-00002.safetensors"}}"#,
        ),
        ("model-00001-of-00002.safetensors", ""),
    ] {
        std::fs::write(snapshot.join(file), contents).expect("writes the cached file");
    }

    let error = download_model(
        REPO_ID,
        &[],
        Budget::Unmeasured,
        HubParams::default(),
        "test",
    )
    .await
    .expect_err("the second shard is not in the cache");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download model 'test' from Hugging Face repo '{REPO_ID}' (revision 'main'): `HF_HUB_OFFLINE` is set and the Hugging Face cache at '{}' does not hold 'model-00002-of-00002.safetensors', which 'model.safetensors.index.json' names. Run `hf download {REPO_ID} --revision main` while online, or unset `HF_HUB_OFFLINE`. See: https://spiceai.org/docs/components/models/huggingface",
            cache.path().display()
        )
    );
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
