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

//! A `TableProvider` wrapper that makes Iceberg table scans serializable for
//! distributed (Ballista) execution.
//!
//! Iceberg's `IcebergTableScan` holds a live, non-serializable `Table`, and its
//! planned `FileScanTask`s carry fields the iceberg crate intentionally refuses
//! to serialize. So this wrapper sits in front of the Iceberg provider and, **only
//! when the query is planned in a distributed session**, wraps the produced
//! `IcebergTableScan` in an [`IcebergScanExec`] that carries the table's
//! [`TableReference`] plus the scan's projection/filters/limit. The physical
//! codec serializes that recipe, and the executor re-derives an equivalent scan
//! by resolving this same provider and replaying `scan()` — reusing its catalog,
//! so no secrets cross the wire.
//!
//! In a single-node session the wrapper is a transparent pass-through: it returns
//! the inner scan unchanged, so non-distributed plans are unaffected.

use spice_table::{LayerWalk, SpiceTable, TableLayer};
use std::borrow::Cow;
use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::{Constraints, Result as DFResult, Statistics};
use datafusion::datasource::TableType;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use iceberg_datafusion::physical_plan::IcebergTableScan;

use crate::execution_plan::{IcebergScanExec, session_is_distributed};

/// Wraps an Iceberg `TableProvider` so its scans can cross Ballista node
/// boundaries. Carries the `DataFusion` [`TableReference`] used to resolve this
/// provider on a remote executor and replay the scan there.
pub struct IcebergClusterTableProvider {
    table_ref: TableReference,
    inner: Arc<dyn TableProvider>,
}

impl std::fmt::Debug for IcebergClusterTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergClusterTableProvider")
            .field("table_ref", &self.table_ref.to_string())
            .finish_non_exhaustive()
    }
}

impl IcebergClusterTableProvider {
    /// Presents this layer over the table it wraps.
    #[must_use]
    pub fn into_table(self: Arc<Self>) -> Arc<SpiceTable> {
        let below = Arc::clone(&self.inner);
        SpiceTable::over(self, below)
    }

    /// Wraps `inner` with the reference used to resolve it for distributed
    /// execution.
    #[must_use]
    pub fn new(table_ref: TableReference, inner: Arc<dyn TableProvider>) -> Self {
        Self { table_ref, inner }
    }

    /// The wrapped provider (an `IcebergTableProvider`, or an
    /// `IcebergDeletionProvider` around one). Exposed so wrapper-peeling helpers
    /// can see through this layer to the concrete Iceberg provider.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

// Deny missing trait methods so that a newly added `TableProvider` method (even
// one with a default) forces an explicit decision here, rather than silently
// bypassing this wrapper's distributed-scan handling.
#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableLayer for IcebergClusterTableProvider {
    /// Wraps a cluster-visible Iceberg scan so it can cross node boundaries. It
    /// carries no schema, CDC or write semantics of its own, so only read
    /// discovery needs to see past it — which is exactly the step that used to
    /// require `runtime` to ship this type's accessor down to the accelerated
    /// table.
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        match walk {
            LayerWalk::Read | LayerWalk::Index => Some(below),
            _ => None,
        }
    }









    async fn scan(
        &self,
        _below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;

        // Only wrap for distributed planning. In a single-node session the scan
        // is returned unchanged, so non-distributed plans are untouched. The
        // downcast guards against the inner provider returning a non-Iceberg
        // plan (it never should), in which case we also leave it untouched.
        // The scan arguments are captured so the executor can replay this exact
        // `scan()` call to re-derive an equivalent (identically bucketed) scan.
        if session_is_distributed(state.config()) && plan.is::<IcebergTableScan>() {
            return Ok(Arc::new(IcebergScanExec::new(
                self.table_ref.clone(),
                plan,
                projection.cloned(),
                filters.to_vec(),
                limit,
            )));
        }

        Ok(plan)
    }

    async fn scan_with_args<'a>(
        &self,
        _below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        // Route through our own `scan` (mirroring the trait's default) so the
        // distributed `IcebergScanExec` wrapping is applied. Forwarding straight
        // to `self.inner` would bypass it and make Iceberg scans unserializable.
        let projection = args.projection().map(<[usize]>::to_vec);
        let plan = self
            .scan(
                state,
                projection.as_ref(),
                args.filters().unwrap_or(&[]),
                args.limit(),
            )
            .await?;
        Ok(plan.into())
    }




}
