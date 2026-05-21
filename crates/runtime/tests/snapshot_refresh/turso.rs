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

use super::{EngineKind, run_bootstrap_then_refresh_cycle};

// Turso (libsql) WAL flush via rusqlite currently fails with
// "file is not a database" — the libsql on-disk file isn't byte-compatible
// with the rusqlite reader. Tracked in spiceai/spiceai#10657.
//
// The WAL-flush hook (commit 1 of #10651) is therefore a no-op for Turso
// today; data loss can occur on snapshot creation if writes are still in
// the WAL. The integration test stays \#\[ignore\]d until #10657 swaps in
// a turso-native checkpoint path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "snapshot_refresh for turso requires a turso-native WAL checkpoint; see spiceai/spiceai#10657"]
async fn snapshot_refresh_turso_bootstrap_then_refresh() -> Result<(), anyhow::Error> {
    run_bootstrap_then_refresh_cycle("snapshot_refresh_turso", EngineKind::Turso).await
}
