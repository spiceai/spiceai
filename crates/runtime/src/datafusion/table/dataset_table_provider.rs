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

//! `DatasetTableProvider` — a placeholder `TableProvider` registered for
//! every deferred dataset at t=0.
//!
//! The placeholder publishes a known schema to the catalog (so SQL
//! parsing and federation analysis succeed without any source-facing
//! I/O) and parks a [`DatasetInitialization`] that can be consumed at
//! most once to build the real provider.
//!
//! When `ensure_ready` runs (from the query-planning resolver
//! hook on first reference) it consumes the parked initialization,
//! materialises the real provider via
//! [`DatasetInitialization::initialize`], and the caller swaps this
//! placeholder out of the catalog with the real provider so federation
//! downcasts on subsequent queries hit the underlying
//! `FederatedTableProviderAdaptor`.
//!
//! ## Hot-path contract
//!
//! `scan` returns [`DataFusionError::Internal`] unconditionally. The
//! runtime guarantees the placeholder is replaced before any query is
//! planned against it (the resolver hook is wired into
//! `create_logical_plan`). A scan reaching the placeholder is a runtime
//! invariant violation and is reported loudly rather than papered over
//! with a silent late init.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use snafu::Snafu;
use tokio::sync::OnceCell;

use crate::init::dataset_initialization::{DatasetInitialization, DatasetReady};

/// Errors surfaced from `DatasetTableProvider::ensure_ready`.
///
/// Wrapped in `Arc` because they are stored in a `OnceCell` and any
/// number of concurrent callers may observe the same failed
/// initialization.
#[derive(Debug, Snafu)]
pub enum InitError {
    #[snafu(display(
        "Dataset `{name}` initialization has already been consumed. \
         Each `DatasetTableProvider` is intended to initialize at most once."
    ))]
    AlreadyConsumed { name: TableReference },

    #[snafu(display("Dataset `{name}` failed to initialize after registration: {source}"))]
    InitializationFailed {
        name: TableReference,
        #[snafu(source(from(crate::Error, Box::new)))]
        source: Box<crate::Error>,
    },

    #[snafu(display("Dataset `{name}` placeholder swap failed: {message}"))]
    CatalogSwapFailed {
        name: TableReference,
        message: String,
    },
}

/// A placeholder `TableProvider` for a deferred dataset whose real
/// provider has not yet been built.
///
/// Construction is I/O-free. The provider is registered with the
/// `DataFusion` catalog at dataset registration time using the
/// pre-known schema (declared `columns[]` or supplied by the connector
/// factory's `static_schema`). The first call to `ensure_ready`
/// consumes the parked `DatasetInitialization`, performs the full
/// bring-up, and the caller swaps this provider in the catalog with
/// the real one.
///
/// `scan` returns `DataFusionError::Internal` — the runtime is
/// expected to swap the real provider in *before* any query plan
/// references this dataset. A scan reaching this
/// type is a runtime invariant violation.
pub struct DatasetTableProvider {
    name: TableReference,
    schema: SchemaRef,
    init: Mutex<Option<DatasetInitialization>>,
    ready: OnceCell<Result<Arc<DatasetReady>, Arc<InitError>>>,
}

impl DatasetTableProvider {
    /// Build a placeholder for the given dataset. `schema` is published
    /// to the catalog and used for SQL planning until the real provider
    /// takes over.
    #[must_use]
    pub fn new(name: TableReference, schema: SchemaRef, init: DatasetInitialization) -> Self {
        Self {
            name,
            schema,
            init: Mutex::new(Some(init)),
            ready: OnceCell::new(),
        }
    }

    /// The dataset table reference this placeholder will resolve to.
    #[must_use]
    pub fn name(&self) -> &TableReference {
        &self.name
    }

    /// At-most-once initialization. Concurrent callers wait for the
    /// in-flight call and observe its result.
    pub async fn ensure_ready(&self) -> Result<Arc<DatasetReady>, Arc<InitError>> {
        let result = self.ready.get_or_init(|| self.initialize_ready()).await;
        result.clone()
    }

    fn initialize_ready(&self) -> BoxFuture<'_, Result<Arc<DatasetReady>, Arc<InitError>>> {
        Box::pin(async move {
            let init = {
                self.init.lock().take().ok_or_else(|| {
                    Arc::new(InitError::AlreadyConsumed {
                        name: self.name.clone(),
                    })
                })?
            };

            let ready = Box::pin(init.initialize()).await.map_err(|err| {
                Arc::new(InitError::InitializationFailed {
                    name: self.name.clone(),
                    source: Box::new(err),
                })
            })?;

            Ok(Arc::new(ready))
        })
    }

    /// True iff `ensure_ready` has been called and produced a result
    /// (success or failure).
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.ready.initialized()
    }
}

impl std::fmt::Debug for DatasetTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetTableProvider")
            .field("name", &self.name)
            .field("initialized", &self.ready.initialized())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for DatasetTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Intentionally unreachable.
    ///
    /// The runtime guarantees this provider is replaced in the catalog
    /// before any query plan references it. A `scan` call here means
    /// the swap path was bypassed (e.g. a non-`create_logical_plan`
    /// SQL entry point) and is treated as a runtime invariant
    /// violation rather than a transient condition to silently
    /// re-resolve.
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(format!(
            "DatasetTableProvider for `{}` was scanned without being initialized first. \
             This indicates a SQL entry point that bypassed the dataset initialization \
             resolver. Please file a bug.",
            self.name
        )))
    }

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        None
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(
        &self,
    ) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> Result<datafusion::catalog::ScanResult, DataFusionError> {
        let filters = args.filters().unwrap_or(&[]);
        let projection = args.projection().map(<[usize]>::to_vec);
        let limit = args.limit();
        let plan = self
            .scan(state, projection.as_ref(), filters, limit)
            .await?;
        Ok(datafusion::catalog::ScanResult::new(plan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        None
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(format!(
            "DatasetTableProvider for `{}` does not support inserts before initialization.",
            self.name
        )))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(format!(
            "DatasetTableProvider for `{}` does not support deletes before initialization.",
            self.name
        )))
    }

    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(format!(
            "DatasetTableProvider for `{}` does not support updates before initialization.",
            self.name
        )))
    }

    async fn truncate(
        &self,
        _state: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(format!(
            "DatasetTableProvider for `{}` does not support truncate before initialization.",
            self.name
        )))
    }
}
