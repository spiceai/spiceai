/*
Copyright 2024 The Spice.ai OSS Authors

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
use tokio::sync::mpsc;

use crate::accelerated_table::refresh::{RefreshOverrides, Refresher};
use crate::accelerated_table::AcceleratedTable;

#[derive(Clone)]
pub struct SynchronizedTable {
    child_dataset_name: TableReference,
    child_accelerator: Arc<dyn TableProvider>,
    refresh_trigger: Option<mpsc::Sender<Option<RefreshOverrides>>>,
    refresher: Arc<Refresher>,
}

impl SynchronizedTable {
    pub fn from(
        accelerated_table: &AcceleratedTable,
        child_accelerator: Arc<dyn TableProvider>,
        child_dataset_name: TableReference,
    ) -> Self {
        Self {
            child_dataset_name,
            child_accelerator,
            refresh_trigger: accelerated_table.refresh_trigger.clone(),
            refresher: accelerated_table.refresher(),
        }
    }

    pub fn child_dataset_name(&self) -> TableReference {
        self.child_dataset_name.clone()
    }

    pub fn child_accelerator(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.child_accelerator)
    }

    pub fn refresher(&self) -> Arc<Refresher> {
        Arc::clone(&self.refresher)
    }

    pub fn refresh_trigger(&self) -> Option<&mpsc::Sender<Option<RefreshOverrides>>> {
        self.refresh_trigger.as_ref()
    }
}
