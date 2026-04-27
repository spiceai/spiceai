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

// SQLite acceleration snapshots are currently broken: in WAL journaling
// mode (the default for SQLite acceleration), the writer's `.sqlite` file
// stays at 4096 bytes (page-zero header only) until a checkpoint flushes
// the `-wal` sidecar into the main file, but the snapshot pipeline only
// performs a plain `fs::copy` of the `.sqlite` file. The uploaded
// snapshot is therefore an empty database. Tracked in spiceai/spiceai#10643.
//
// `refresh_mode: full`/`append` happen to recover via federated fallback,
// but `refresh_mode: snapshot` has no fallback by design and surfaces the
// underlying bug. This test is enabled once #10643 is fixed.
#[ignore = "blocked on spiceai/spiceai#10643 (SQLite WAL not flushed before snapshot)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_refresh_sqlite_bootstrap_then_refresh() -> Result<(), anyhow::Error> {
    run_bootstrap_then_refresh_cycle("snapshot_refresh_sqlite", EngineKind::Sqlite).await
}
