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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use tokio::sync::{Mutex, OwnedRwLockWriteGuard};

use crate::accelerated::AcceleratedTable;
use crate::accelerated::caching::{
    CacheRefreshHelper, InFlightRevalidations, SynchronizedCacheTarget, SynchronizedChildren,
};
use crate::accelerated::refresh::Refresher;

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct SynchronizedTable {
    parent_dataset_name: TableReference,
    child_dataset_name: TableReference,
    child_accelerator: Arc<dyn TableProvider>,
    refresher: Arc<Refresher>,
    /// Reference to parent's synchronized children list (for caching mode registration)
    parent_synchronized_children: SynchronizedChildren,
    parent_cache_children_closed: Arc<AtomicBool>,
    /// Reference to parent's accelerator (for initializing child from existing data)
    parent_accelerator: Arc<dyn TableProvider>,
    parent_write_mutex: Arc<Mutex<()>>,
    parent_change_sink: Option<runtime_acceleration::change_sink::ChangeSink>,
}

/// Retains the parent fanout fence between the child snapshot and publication.
pub(crate) struct PreparedCacheChild {
    parent: SynchronizedTable,
    children: OwnedRwLockWriteGuard<Vec<SynchronizedCacheTarget>>,
    child: SynchronizedCacheTarget,
    rows: usize,
}

impl PreparedCacheChild {
    /// The child must have a drain owner before it becomes a fanout target.
    pub(crate) fn publish(mut self) -> datafusion::error::Result<usize> {
        self.parent.ensure_parent_accepts_children()?;
        self.children.push(self.child);
        Ok(self.rows)
    }
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
            parent_synchronized_children: accelerated_table.synchronized_children(),
            parent_cache_children_closed: Arc::clone(&accelerated_table.cache_children_closed),
            parent_accelerator: accelerated_table.get_accelerator(),
            parent_write_mutex: Arc::clone(&accelerated_table.accelerator_write_mutex),
            parent_change_sink: accelerated_table.change_sink().cloned(),
        }
    }

    #[must_use]
    pub fn child_dataset_name(&self) -> TableReference {
        self.child_dataset_name.clone()
    }

    #[must_use]
    pub fn parent_dataset_name(&self) -> TableReference {
        self.parent_dataset_name.clone()
    }

    #[must_use]
    pub fn child_accelerator(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.child_accelerator)
    }

    #[must_use]
    pub fn parent_accelerator(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.parent_accelerator)
    }

    #[must_use]
    pub fn refresher(&self) -> Arc<Refresher> {
        Arc::clone(&self.refresher)
    }

    /// Remove only the retiring child's generation after its writes drain.
    /// Waiting for the registry also orders this removal after parent jobs that
    /// already selected the child. No storage lock may be held while waiting.
    pub(crate) async fn unregister_cache_child(&self, claims: &InFlightRevalidations) {
        self.parent_synchronized_children
            .write()
            .await
            .retain(|child| !Arc::ptr_eq(&child.in_flight, claims));
    }

    /// Initialize a child while fencing parent propagation until publication.
    /// The registry fence precedes the write fence: earlier parent writes are
    /// included in the snapshot, and later fanout sees the registered child.
    pub(crate) async fn prepare_cache_child(
        &self,
        child: SynchronizedCacheTarget,
    ) -> datafusion::error::Result<PreparedCacheChild> {
        let children = Arc::clone(&self.parent_synchronized_children)
            .write_owned()
            .await;
        self.ensure_parent_accepts_children()?;
        if let Some(sink) = &self.parent_change_sink {
            sink.flush().await?;
        }
        let _write_guard = self.parent_write_mutex.lock().await;
        let rows = CacheRefreshHelper::initialize_child_from_parent(
            &self.parent_accelerator,
            &child,
            &self.child_dataset_name.to_string(),
        )
        .await?;
        self.ensure_parent_accepts_children()?;
        Ok(PreparedCacheChild {
            parent: self.clone(),
            children,
            child,
            rows,
        })
    }

    fn ensure_parent_accepts_children(&self) -> datafusion::error::Result<()> {
        if self.parent_cache_children_closed.load(Ordering::Acquire) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Cannot initialize cache dataset '{}' because parent '{}' is stopping",
                self.child_dataset_name, self.parent_dataset_name,
            )));
        }
        Ok(())
    }
}
