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
impl TableProvider for IcebergClusterTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
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
        if session_is_distributed(state.config())
            && plan.downcast_ref::<IcebergTableScan>().is_some()
        {
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

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, overwrite).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    async fn truncate(&self, state: &dyn Session) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
    }
}
