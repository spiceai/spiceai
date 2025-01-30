/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use rand::Rng;
use regex::Regex;
use std::{
    future::Future,
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    sync::LazyLock,
    time::Duration,
};

pub async fn wait_until_true<F, Fut>(max_wait: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait {
        if f().await {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

pub(crate) fn get_random_element<T>(vec: &[T]) -> Option<&T> {
    if vec.is_empty() {
        None
    } else {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..vec.len());
        Some(&vec[index])
    }
}

pub fn hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

// replace insta headers with an empty string
const INSTA_HEADER_REGEX: &str = r"^---\n(([\w\W]*\n)+)---\n";
static INSTA_HEADER_RE: LazyLock<Regex> = LazyLock::new(|| {
    #[allow(clippy::expect_used)] // the regex is valid
    Regex::new(INSTA_HEADER_REGEX).expect("Insta header replacement regex should build")
});

/// Compare two insta snapshots by hashing their contents.
/// Returns true if the snapshots are the same.
///
/// This doesn't use ``assert_snapshot!`` because:
/// - insta might update the snapshots which we don't want
/// - we want to return a boolean instead of any other kind of error/panic
#[must_use]
pub fn snapshots_are_equal(snapshot_a: &str, snapshot_b: &str) -> bool {
    // remove insta headers
    let snapshot_a = INSTA_HEADER_RE.replace(snapshot_a, "");
    let snapshot_b = INSTA_HEADER_RE.replace(snapshot_b, "");

    let hash_a = hash(&snapshot_a);
    let hash_b = hash(&snapshot_b);

    hash_a == hash_b
}

/// Recursively scan a directory for YAML files
pub fn scan_directory_for_yamls(path: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = vec![];

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            files.append(&mut scan_directory_for_yamls(&path)?);
        } else if path.is_file() && path.extension().is_some_and(|ext| ext == "yaml") {
            files.push(path);
        }
    }

    Ok(files)
}
