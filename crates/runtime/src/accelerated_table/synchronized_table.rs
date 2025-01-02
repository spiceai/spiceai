/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;

use crate::accelerated_table::refresh::Refresher;
use crate::accelerated_table::AcceleratedTable;

#[derive(Clone)]
pub struct SynchronizedTable {
    parent_dataset_name: TableReference,
    child_dataset_name: TableReference,
    child_accelerator: Arc<dyn TableProvider>,
    refresher: Arc<Refresher>,
}

impl std::fmt::Debug for SynchronizedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SynchronizedTable")
            .field("parent_dataset_name", &self.parent_dataset_name)
            .field("child_dataset_name", &self.child_dataset_name)
            .field("child_accelerator", &self.child_accelerator)
            .finish_non_exhaustive()
    }
}

impl SynchronizedTable {
    pub fn from(
        accelerated_table: &AcceleratedTable,
        child_accelerator: Arc<dyn TableProvider>,
        child_dataset_name: TableReference,
    ) -> Self {
        Self {
            parent_dataset_name: accelerated_table.dataset_name.clone(),
            child_dataset_name,
            child_accelerator,
            refresher: accelerated_table.refresher(),
        }
    }

    pub fn child_dataset_name(&self) -> TableReference {
        self.child_dataset_name.clone()
    }

    pub fn parent_dataset_name(&self) -> TableReference {
        self.parent_dataset_name.clone()
    }

    pub fn child_accelerator(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.child_accelerator)
    }

    pub fn refresher(&self) -> Arc<Refresher> {
        Arc::clone(&self.refresher)
    }
}
