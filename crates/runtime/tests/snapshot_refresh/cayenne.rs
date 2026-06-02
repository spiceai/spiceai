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
// (`cayenne_table`, `cayenne_partition`, `cayenne_delete_file`); the
// spice_sys `_dataset_checkpoint` schema row is bootstrapped from the
// snapshot metadata by `download_latest_snapshot` (closes
// spiceai/spiceai#10658), so this test now runs by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_refresh_cayenne_bootstrap_then_refresh() -> Result<(), anyhow::Error> {
    run_bootstrap_then_refresh_cycle("snapshot_refresh_cayenne", EngineKind::Cayenne).await
}
