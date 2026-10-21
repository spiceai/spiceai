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

//! `spiced`'s integration tests, linked into one binary.
//!
//! Each `tests/<name>.rs` is a module here rather than its own test target, so
//! the crate's dependencies are linked once instead of once per file.
//!
//! nextest runs every test in a process of its own, so a test here may change
//! process-global state as long as it calls [`require_process_per_test`] first;
//! under plain `cargo test` that call fails the test before it changes anything
//! its neighbours would see.

#![expect(
    clippy::expect_used,
    reason = "set-up and helpers outside `#[test]` functions fail fast with `expect`"
)]

mod build_logging;
mod cloud_connect_bootstrap;
mod cpu_budget;
mod dependency_logging;
mod otel_logs;

/// Fails the calling test unless it runs in a process of its own.
///
/// nextest runs every test in its own process; plain `cargo test` runs a
/// binary's tests on threads of one process. A test that changes process-global
/// state calls this before changing it, so under `cargo test` it fails with
/// instructions instead of changing the state every neighbouring test runs under.
/// `why` names that state.
fn require_process_per_test(why: &str) {
    assert!(
        std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test"),
        "{why}, so it must run in a process of its own. Run it with \
         `cargo nextest run -p spiced <test name>`: plain \
         `cargo test` runs every test in the binary on threads of one process"
    );
}
