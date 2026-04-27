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

// Turso (libsql 0.5.x, MVCC) almost certainly suffers from the same class
// of issue as SQLite: a plain `fs::copy` of the engine's primary file
// captures only the page header, not the recently-written rows that live
// in the journal/transaction log. The fix shape mirrors SQLite:
// engine-specific snapshot preparation. Tracked alongside the SQLite
// failure in spiceai/spiceai#10643.
#[ignore = "blocked on spiceai/spiceai#10643 (libsql/Turso primary file likely mirrors SQLite WAL behavior)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_refresh_turso_bootstrap_then_refresh() -> Result<(), anyhow::Error> {
    run_bootstrap_then_refresh_cycle("snapshot_refresh_turso", EngineKind::Turso).await
}
