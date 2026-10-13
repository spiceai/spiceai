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

use std::sync::Arc;

use datafusion_execution::TaskContext;
use tokio::sync::Mutex;

use super::table::CayenneTableProvider;

/// Execution-scoped permission to lose uncheckpointed writes to one table.
///
/// Install in the write session's configuration only when the caller can rebuild
/// the data. It does not authorize source acknowledgement or change other tables'
/// writes. The shared write lock identifies the exact storage owner and its clones.
#[derive(Debug)]
pub struct RebuildableWrite {
    owner: Arc<Mutex<()>>,
}

impl RebuildableWrite {
    /// Permit rebuildable writes to this table's storage owner and its clones.
    #[must_use]
    pub fn new(table: &CayenneTableProvider) -> Self {
        Self {
            owner: table.write_lock_arc(),
        }
    }

    pub(crate) fn permits(table: &CayenneTableProvider, context: &TaskContext) -> bool {
        context
            .session_config()
            .get_extension::<Self>()
            .is_some_and(|write| Arc::ptr_eq(&write.owner, &table.write_lock_arc()))
    }
}
