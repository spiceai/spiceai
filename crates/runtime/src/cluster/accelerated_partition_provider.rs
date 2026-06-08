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

//! Runtime-side wrapper that lifts `AcceleratedTable` downcasting out of the
//! `runtime-cluster` crate — the trait impl must live here because both
//! `TablePartitionProvider` and `ExecutorRegistry` are external to this crate
//! (orphan rule).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{catalog::TableProvider, datasource::DefaultTableSource, sql::TableReference};
use datafusion_expr::TableScan;
use runtime_cluster::{ExecutorRegistry, PartitionValue};
use runtime_datafusion::analyzer_rule::TablePartitionProvider;

use crate::accelerated_table::AcceleratedTable;
use crate::search::util::find_concrete_table_provider;

/// Wraps an [`ExecutorRegistry`] with the `AcceleratedTable`-specific
/// `should_partition` logic so it can be installed as a `TablePartitionProvider`.
#[derive(Debug)]
pub struct AcceleratedPartitionProvider(Arc<ExecutorRegistry>);

impl AcceleratedPartitionProvider {
    #[must_use]
    pub fn from_registry(registry: Arc<ExecutorRegistry>) -> Self {
        Self(registry)
    }
}

/// Whether `table_provider` is (or wraps) an [`AcceleratedTable`].
///
/// The registered provider for an accelerated dataset is decorated depending on its
/// configuration: a `FederatedTableProviderAdaptor` (`PolyTableProvider` engines),
/// a `MetadataEnrichedTableProvider` (datasets with column/table metadata such as
/// descriptions), an `IndexedTableProvider` / `EmbeddingTable` (embedding or
/// full-text-search columns), or any nesting of these. We must see through all of
/// them — otherwise the coordinator skips partition distribution and silently
/// federates the read to the source. [`find_concrete_table_provider`] already knows
/// how to unwrap every such decorator.
fn is_accelerated_table_provider(table_provider: &Arc<dyn TableProvider>) -> bool {
    find_concrete_table_provider::<AcceleratedTable>(table_provider).is_some()
}

impl TablePartitionProvider for AcceleratedPartitionProvider {
    fn should_partition(&self, tbl: &TableScan) -> bool {
        let Some(default) = tbl.source.as_any().downcast_ref::<DefaultTableSource>() else {
            return false;
        };
        is_accelerated_table_provider(&default.table_provider)
    }

    fn get_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        self.0
            .resolve_accelerated_partitions(table, schema)
            .into_iter()
            // Executors only materialize data for their assigned partitions; bucket filters are
            // redundant and expensive to evaluate per-row.
            .map(|(provider, _)| (provider, vec![]))
            .collect()
    }
}
