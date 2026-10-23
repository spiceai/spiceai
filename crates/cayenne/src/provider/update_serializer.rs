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

//! Autocommit `UPDATE` reads, deletes, and inserts inside `write_lock`.
//!
//! An UPDATE that reads its rows from a scan frozen at `TableProvider::update`
//! and takes `write_lock` only for the later delete and insert loses rows under
//! contention. Two plans built against the same version then execute as: the
//! first writes a new version; the second materializes zero rows (live
//! tombstones hide the captured version, the replacement is not in the frozen
//! listing), tombstones the live key, and inserts nothing.
//!
//! [`CayenneUpdateSerializer`] runs the whole UPDATE inside one `write_lock`
//! hold, so the read is judged by the same table the delete is — the #13828
//! pattern applied to `UPDATE`.

use arrow::array::RecordBatch;
use data_components::update::{UpdateRead, UpdateSerializer, UpdateWrite};
use datafusion_common::DataFusionError;

use super::table::CayenneTableProvider;
use super::write_lock_held;

/// Runs a Cayenne table's autocommit `UPDATE` inside its `write_lock`.
pub(crate) struct CayenneUpdateSerializer {
    table: CayenneTableProvider,
}

impl CayenneUpdateSerializer {
    pub(crate) fn new(table: CayenneTableProvider) -> Self {
        Self { table }
    }
}

impl std::fmt::Debug for CayenneUpdateSerializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneUpdateSerializer")
            .field("table", &self.table.table_name())
            .finish()
    }
}

#[async_trait::async_trait]
impl UpdateSerializer for CayenneUpdateSerializer {
    async fn run(
        &self,
        read: UpdateRead,
        write: UpdateWrite,
    ) -> Result<RecordBatch, DataFusionError> {
        let write_guard = self.table.write_lock_arc().lock_owned().await;
        self.table
            .ensure_publication_outcome_known()
            .map_err(DataFusionError::from)?;
        // The read changes nothing, so a caller cancelled here drops the lock and
        // leaves the table as it was.
        let rows = write_lock_held::scope(read).await?;
        // The delete and insert run on a task that owns the lock. A caller
        // cancelled between them would otherwise leave the matched rows deleted
        // with nothing inserted, and the insert hands its commit to a task that
        // outlives its caller: dropping the lock with the caller would let the
        // next writer start while that commit publishes.
        let table = self.table.table_name().to_string();
        tokio::spawn(async move {
            let _write_guard = write_guard;
            write_lock_held::scope(write(rows)).await
        })
        .await
        .map_err(|source| DataFusionError::from(super::Error::TaskPanicked { table, source }))?
    }
}
