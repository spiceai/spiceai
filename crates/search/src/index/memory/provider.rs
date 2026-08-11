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

//! [`TableProvider`]s over a [`MemoryVectorStore`].
//!
//! Both providers read the store lazily at `scan()` time, so rows written
//! after plan construction are visible — `VectorScanTableProvider` builds the
//! list plan once at construction, and `query_table_provider` is a sync fn.

use std::{borrow::Cow, sync::Arc};

use arrow_schema::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{MemTable, Session},
    common::{Column, Constraints, not_impl_err},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use datafusion_expr::{
    Expr as LogicalExpr, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF,
    TableProviderFilterPushDown, binary_expr, col, lit,
};
use parking_lot::RwLock;
use runtime_datafusion_udfs::{
    cosine_distance::CosineDistance, inner_product::InnerProduct, l2_distance::L2Distance,
};

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::memory::{MemoryDistanceMetric, store::MemoryVectorStore};

/// Enumerates the store contents for [`crate::index::VectorIndex::list_table_provider`].
#[derive(Debug)]
pub(crate) struct MemoryVectorListTable {
    store: Arc<RwLock<MemoryVectorStore>>,
    schema: SchemaRef,
}

impl MemoryVectorListTable {
    pub(crate) fn new(store: Arc<RwLock<MemoryVectorStore>>) -> Self {
        let schema = Arc::clone(&store.read().stored_schema);
        Self { store, schema }
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for MemoryVectorListTable {
    fn schema(&self) -> SchemaRef {
        SchemaRef::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let batches = self.store.read().batches();
        MemTable::try_new(self.schema(), vec![batches])?
            .scan(state, projection, filters, limit)
            .await
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        None
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> DataFusionResult<datafusion::catalog::ScanResult> {
        let filters = args.filters().unwrap_or(&[]);
        let projection = args.projection().map(<[usize]>::to_vec);
        let limit = args.limit();
        let plan = self
            .scan(state, projection.as_ref(), filters, limit)
            .await?;
        Ok(plan.into())
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _op: datafusion::logical_expr::dml::InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Insert into not supported for 'MemoryVectorListTable' table")
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("DELETE not supported for 'MemoryVectorListTable' table")
    }

    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("UPDATE not supported for 'MemoryVectorListTable' table")
    }

    async fn truncate(&self, _state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("TRUNCATE not supported for 'MemoryVectorListTable' table")
    }
}

/// Brute-force exact k-NN scan for [`crate::index::SearchIndex::query_table_provider`].
///
/// Builds a logical plan over [`MemoryVectorListTable`] that scores every
/// stored row against the query with the shared distance UDFs.
#[derive(Debug)]
pub(crate) struct MemoryVectorQueryTable {
    index_name: String,
    store: Arc<RwLock<MemoryVectorStore>>,
    embed_udf: Arc<ScalarUDF>,
    model_name: String,
    query: String,
    metric: MemoryDistanceMetric,
    embedding_column_name: String,
    schema: SchemaRef,
}

impl MemoryVectorQueryTable {
    pub(crate) fn new(
        index_name: String,
        store: Arc<RwLock<MemoryVectorStore>>,
        embed_udf: Arc<ScalarUDF>,
        model_name: String,
        query: String,
        metric: MemoryDistanceMetric,
        embedding_column_name: String,
    ) -> Self {
        let mut fields = store.read().stored_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            DataType::Float64,
            true,
        )));
        Self {
            index_name,
            store,
            embed_udf,
            model_name,
            query,
            metric,
            embedding_column_name,
            schema: Arc::new(arrow_schema::Schema::new(fields)),
        }
    }

    fn score_expr(&self) -> LogicalExpr {
        let query_embedding =
            LogicalExpr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                Arc::clone(&self.embed_udf),
                vec![lit(self.query.clone()), lit(self.model_name.clone())],
            ));

        let embedding_col =
            LogicalExpr::Column(Column::new_unqualified(self.embedding_column_name.clone()));
        match self.metric {
            MemoryDistanceMetric::Cosine => binary_expr(
                lit(1.0),
                Operator::Minus,
                LogicalExpr::ScalarFunction(datafusion_expr::expr::ScalarFunction {
                    func: Arc::new(CosineDistance::new().into()) as Arc<ScalarUDF>,
                    args: vec![query_embedding, embedding_col],
                }),
            ),
            MemoryDistanceMetric::L2 => {
                -LogicalExpr::ScalarFunction(datafusion_expr::expr::ScalarFunction {
                    func: Arc::new(L2Distance::new().into()) as Arc<ScalarUDF>,
                    args: vec![query_embedding, embedding_col],
                })
            }
            MemoryDistanceMetric::Dot => {
                LogicalExpr::ScalarFunction(datafusion_expr::expr::ScalarFunction {
                    func: Arc::new(InnerProduct::new().into()) as Arc<ScalarUDF>,
                    args: vec![query_embedding, embedding_col],
                })
            }
        }
        .alias(SEARCH_SCORE_COLUMN_NAME)
    }

    fn projection_exprs(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> DataFusionResult<Vec<LogicalExpr>> {
        let schema = match projection {
            Some(indices) => Arc::new(
                self.schema()
                    .project(indices)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            ),
            None => self.schema(),
        };
        Ok(schema
            .fields()
            .iter()
            .map(|field| LogicalExpr::Column(Column::new_unqualified(field.name())))
            .collect())
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for MemoryVectorQueryTable {
    fn schema(&self) -> SchemaRef {
        SchemaRef::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let list_table =
            Arc::new(MemoryVectorListTable::new(Arc::clone(&self.store))) as Arc<dyn TableProvider>;
        let mut builder = LogicalPlanBuilder::scan(
            self.index_name.as_str(),
            Arc::new(DefaultTableSource::new(list_table)),
            None,
        )?;

        let mut score_projection = builder
            .schema()
            .columns()
            .iter()
            .map(|column| LogicalExpr::Column(column.clone()))
            .collect::<Vec<_>>();
        score_projection.push(self.score_expr());
        builder = builder.project(score_projection)?;

        if let Some(filter) = filters.iter().cloned().reduce(LogicalExpr::and) {
            builder = builder.filter(filter)?;
        }

        builder = builder.sort(vec![col(SEARCH_SCORE_COLUMN_NAME).sort(false, false)])?;

        if limit.is_some() {
            builder = builder.limit(0, limit)?;
        }

        if projection.is_some() {
            builder = builder.project(self.projection_exprs(projection)?)?;
        }

        let logical_plan = builder.build()?;
        state.create_physical_plan(&logical_plan).await
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // We explicitly add filters atop `LogicalPlanBuilder` during `MemoryVectorQueryTable::scan`.
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        None
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> DataFusionResult<datafusion::catalog::ScanResult> {
        let filters = args.filters().unwrap_or(&[]);
        let projection = args.projection().map(<[usize]>::to_vec);
        let limit = args.limit();
        let plan = self
            .scan(state, projection.as_ref(), filters, limit)
            .await?;
        Ok(plan.into())
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _op: datafusion::logical_expr::dml::InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Insert into not supported for 'MemoryVectorQueryTable' table")
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("DELETE not supported for 'MemoryVectorQueryTable' table")
    }

    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("UPDATE not supported for 'MemoryVectorQueryTable' table")
    }

    async fn truncate(&self, _state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("TRUNCATE not supported for 'MemoryVectorQueryTable' table")
    }
}
