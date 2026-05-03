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

// Cayenne snapshot refresh exercises the per-dataset metastore-slice format
// shipped by `CayenneSnapshotEngine`: the writer's snapshot tar contains
// `metadata/<dataset>.slice.json` instead of the raw `cayenne.db` file, and
// the reader's bootstrap path imports the slice into its local metastore.
//
// The wholesale-replace import correctly rebuilds the cayenne-domain tables
// (`cayenne_table`, `cayenne_partition`, `cayenne_delete_file`), but does
// **not** populate the spice_sys `_dataset_checkpoint` table that
// `download_latest_snapshot` queries via `Checkpointer::get_schema`. The
// resulting `MissingSchema` error is the last blocker to enabling this
// test by default. Tracked in spiceai/spiceai#10658.
//
// The supporting code (per-dataset slice format, `CayenneSnapshotEngine`
// pipeline wiring, multi-dataset shared metastore validation lift) all
// lands in this PR; only the schema-handoff to spice_sys is deferred.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "snapshot_refresh for cayenne requires checkpoint schema handoff after slice import; see spiceai/spiceai#10658"]
async fn snapshot_refresh_cayenne_bootstrap_then_refresh() -> Result<(), anyhow::Error> {
    run_bootstrap_then_refresh_cycle("snapshot_refresh_cayenne", EngineKind::Cayenne).await
}
