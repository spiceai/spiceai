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

//! `data_components`' integration tests, linked into one binary.
//!
//! Each `tests/<name>.rs` is a module here rather than its own test target, so
//! the crate's dependencies are linked once instead of once per file.

#[cfg(feature = "elasticsearch")]
mod elasticsearch_test;
mod hadoop_catalog_test;
mod http_provider_test;

/// `autotests = false` means cargo compiles only the test targets `Cargo.toml`
/// names, so a new `tests/<name>.rs` that is neither a module of a target nor its own
/// `[[test]]` would build nothing and its tests would never run, silently.
#[test]
fn every_test_file_is_compiled() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest =
        std::fs::read_to_string(dir.join("Cargo.toml")).expect("Cargo.toml should be readable");
    let target_paths: Vec<&str> = manifest
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("path = \"tests/")
                .and_then(|path| path.strip_suffix(".rs\""))
        })
        .collect();
    let roots: Vec<String> = target_paths
        .iter()
        .map(|stem| {
            std::fs::read_to_string(dir.join(format!("tests/{stem}.rs")))
                .expect("test target root should be readable")
        })
        .collect();
    let mut missing: Vec<String> = std::fs::read_dir(dir.join("tests"))
        .expect("tests/ should be readable")
        .map(|entry| entry.expect("tests/ entry should be readable").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .filter_map(|path| Some(path.file_stem()?.to_str()?.to_string()))
        .filter(|stem| {
            !target_paths.contains(&stem.as_str())
                && !roots.iter().any(|root| {
                    root.lines()
                        .any(|line| line.trim() == format!("mod {stem};"))
                })
        })
        .collect();
    missing.sort();
    assert!(
        missing.is_empty(),
        "these files in crates/data_components/tests are not compiled into any test binary: \
         {missing:?}. Add `mod <name>;` to tests/integration.rs, or a `[[test]]` \
         entry in crates/data_components/Cargo.toml if the file needs its own binary"
    );
}
