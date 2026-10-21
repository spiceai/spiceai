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

// SQLite acceleration ran in WAL journaling mode without flushing before
// `fs::copy`, so the snapshot uploaded only the page-zero header. Fixed by
// the WAL-checkpoint hook on `SnapshotEngine` (see commit `fix(snapshot):
// flush SQLite/Turso WAL before snapshotting`); spiceai/spiceai#10643.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_refresh_sqlite_bootstrap_then_refresh() -> Result<(), anyhow::Error> {
    run_bootstrap_then_refresh_cycle("snapshot_refresh_sqlite", EngineKind::Sqlite).await
}
