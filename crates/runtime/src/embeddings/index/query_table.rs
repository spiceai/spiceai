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

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use arrow_schema::{ArrowError, Field, Schema};
use async_trait::async_trait;

use data_components::s3_vectors::{
    MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
};

use datafusion::{
    catalog::Session,
    common::{Column, Constraints, DFSchema, DFSchemaRef, JoinConstraint, JoinType},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        BinaryExpr, Cast, Expr, Filter, Join, LogicalPlan, Operator, Projection, Sort, SortExpr,
        TableProviderFilterPushDown, TableScan,
        expr::{Alias, InList},
    },
    physical_plan::ExecutionPlan,
    prelude::lit,
    scalar::ScalarValue,
    sql::TableReference,
};

use runtime_datafusion_index::IndexedTableProvider;
use search::SEARCH_SCORE_COLUMN_NAME;

use tokio_stream::StreamExt;

use crate::{embedding_col, embeddings::index::VectorIndex};
use crate::{embeddings::udtf::append_fields, search::util::find_concrete_table_provider};

/// An [`IndexedTableProvider`] embued with a [`VectorIndex`] that can order results in the underlying [`IndexedTableProvider::get_underlying`] by vector similarity to a query (similarity with respect to associated embedded column in [`VectorIndex`]).
#[derive(Debug, Clone)]
pub struct VectorQueryTableProvider {
    /// Base [`TableProvider`] associated with the vector index query.
    /// Note: [`TableProvider::schema`] will contain vector embedding columns that may need to be
    /// recomputed at query time. As such full projections on this [`TableProvider`] are not advised.
    ///
    /// To get the underlying schema (i.e. without any calculated columns), downcast to, and use [`runtime_datafusion_index::IndexedTableProvider::get_underlying`].
    pub table_provider: Arc<dyn TableProvider>,
    pub vector_index: Arc<dyn VectorIndex>,

    pub query: String,

    /// If Some(N), will only retrieve `N` results from the index. If filters are provided that are
    /// unsupported by the index (i.e. via its[`TableProvider::supports_filters_pushdown`] ), then
    ///  `< N` will be returned in the overall SQL query.
    pub pre_limit: Option<usize>,
}

impl VectorQueryTableProvider {
    /// Execute the given physical plan of a vector index query, extract the primary key column and convert the values: {v1, v2, ..., vn} into a filter predicate: `WHERE primary_key_column IN (v1, v2,...,vn)`.
    async fn base_table_query_filter(
        &self,
        state: &dyn Session,
        physical_plan: Arc<dyn ExecutionPlan>,
        primary_key_column: String,
    ) -> DataFusionResult<Expr> {
        let mut expr = vec![];

        let mut strm = physical_plan.execute(0, state.task_ctx())?;
        while let Some(Ok(rb)) = strm.next().await {
            if let Some(arr) = rb.column_by_name(primary_key_column.as_str()) {
                for i in 0..arr.len() {
                    expr.push(Expr::Literal(ScalarValue::try_from_array(arr, i)?));
                }
            }
        }
        Ok(Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name(primary_key_column.clone()))),
            expr,
            false,
        )))
    }

    /// Returns all filters that can be handled by the given vector index columns.
    ///
    /// This does not require that associated [`TableProvider::supports_filters_pushdown`] is
    /// [`TableProviderFilterPushDown::Unsupported`] for all filters, only that the columns
    /// referenced in the filters, are those available in the `vector_index_table`.
    fn vector_index_filters(vector_index_columns: &HashSet<String>, filters: &[Expr]) -> Vec<Expr> {
        filters
            .iter()
            .filter(|f| {
                let filter_columns = f
                    .column_refs()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect::<HashSet<_>>();
                vector_index_columns.is_superset(&filter_columns)
            })
            .cloned()
            .collect()
    }

    /// Returns true if the projection (relative to [`VectorQueryTableProvider`]) can be handled by the given vector index schema.
    fn vector_index_has_full_projection(
        &self,
        vector_index_columns: &HashSet<String>,
        projection: Option<&Vec<usize>>,
    ) -> Result<bool, ArrowError> {
        let schema = match projection {
            None => self.schema(),
            Some(indices) => Arc::new(self.schema().project(indices)?),
        };
        let columns_requested: HashSet<String> =
            schema.fields().iter().map(|f| f.name().clone()).collect();

        Ok(vector_index_columns.is_superset(&columns_requested))
    }

    fn qualified_schema(&self, projection: Option<&Vec<usize>>) -> DFSchemaRef {
        let base = self.get_underlying_schema();
        let mut qualified_fields: Vec<_> = base
            .fields()
            .iter()
            .map(|f| (Some(TableReference::parse_str("tbl")), Arc::clone(f)))
            .collect();
        qualified_fields.push((
            Some(TableReference::parse_str("vector_index")),
            Arc::new(Field::new(
                SEARCH_SCORE_COLUMN_NAME.to_string(),
                arrow_schema::DataType::Float64,
                false,
            )),
        ));

        let projected_qualified_fields = match projection {
            None => qualified_fields,
            Some(proj) => qualified_fields
                .into_iter()
                .enumerate()
                .filter_map(|(i, f)| if proj.contains(&i) { Some(f) } else { None })
                .collect(),
        };

        let Ok(df_schema) =
            DFSchema::new_with_metadata(projected_qualified_fields, HashMap::default())
        else {
            unreachable!("DFSchema::try_from is infallible as of DataFusion 38")
        };

        Arc::new(df_schema)
    }

    /// Returns a [`TableScan`] with associated parameters restricted to those relevant on the underlying table (i.e. restrict projection indices to within bounds).
    fn underlying_table_scan(
        &self,
        filters: &[Expr],
        embedded_column: &str,
        metadata_columns: &[String],
    ) -> DataFusionResult<LogicalPlan> {
        // Remove embedding column and metadata columns of vector index.
        let base_proj = (0..self.get_underlying_schema().fields().len()).collect::<Vec<_>>();
        let base_proj =
            self.underlying_projection_without_metadata(metadata_columns, Some(&base_proj));
        let base_proj = self.remove_embedding_column(base_proj, embedded_column);

        let filter_refs: Vec<_> = filters.iter().collect();
        let supported_filters = self
            .table_provider
            .supports_filters_pushdown(filter_refs.as_slice())?;
        let underlying_filters: Vec<Expr> = filters
            .iter()
            .zip(supported_filters.iter())
            .filter_map(|(f, supp)| {
                if matches!(supp, TableProviderFilterPushDown::Unsupported) {
                    None
                } else {
                    Some(f)
                }
            })
            .cloned()
            .collect();

        let scan = LogicalPlan::TableScan(TableScan::try_new(
            TableReference::parse_str("tbl"), // This name is just useful for picking columns during JOIN. kinda
            Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
            Some(base_proj),
            vec![],
            None, // Cannot restrict, as dependent on vector query scan.
        )?);
        let plan = if let Some(filter) = fold_binary(underlying_filters.as_slice(), Operator::And) {
            LogicalPlan::Filter(Filter::try_new(filter, scan.into())?)
        } else {
            scan
        };

        Ok(plan)
    }

    fn remove_embedding_column(&self, projection: Vec<usize>, col: &str) -> Vec<usize> {
        match self.schema().column_with_name(col) {
            Some((idx, _)) => projection.into_iter().filter(|p| *p != idx).collect(),
            None => projection,
        }
    }

    fn get_underlying_schema(&self) -> Arc<Schema> {
        let Some(indexed) =
            find_concrete_table_provider::<IndexedTableProvider>(&self.table_provider)
        else {
            tracing::debug!(
                "'VectorQueryTableProvider' instantiated without using a 'IndexedTableProvider'. Cannot get underlying schema, defaulting to TableProvider. TableProvider is {:?}",
                self.table_provider
            );
            return self.table_provider.schema();
        };
        indexed.get_underlying().schema()
    }

    // Remove any fields that can be returned from the vector indexes `metadata_columns`.
    fn underlying_projection_without_metadata(
        &self,
        metadata_columns: &[String],
        projection: Option<&Vec<usize>>,
    ) -> Vec<usize> {
        self.schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                // Don't include columns from vector index
                if metadata_columns.contains(f.name()) {
                    return None;
                }

                // Don't include if not requested by user
                if let Some(p) = projection.as_ref() {
                    if !p.contains(&i) {
                        return None;
                    }
                }
                Some(i)
            })
            .collect()
    }

    async fn vector_index_table(
        &self,
        pk: &Field,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<LogicalPlan, DataFusionError> {
        let query_table = self
            .vector_index
            .query_table_provider(self.query.as_str())
            .await?;
        let query_table_ref = TableReference::parse_str("vector_index");

        let mut query_table_projection_exprs = vec![
            Expr::Alias(Alias::new(
                Expr::Cast(Cast::new(
                    Box::new(Expr::Column(Column::new_unqualified(
                        S3_VECTOR_PRIMARY_KEY_NAME,
                    ))),
                    pk.data_type().clone(),
                )),
                Some(query_table_ref.clone()),
                pk.name().to_string(),
            )),
            Expr::Alias(Alias::new(
                Expr::Column(Column::new_unqualified(S3_VECTOR_EMBEDDING_NAME)),
                None::<TableReference>,
                embedding_col!(self.vector_index.embedded_column()),
            )),
            Expr::Alias(Alias::new(
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(lit(1.0)),
                    Operator::Minus,
                    Box::new(Expr::Column(Column::new_unqualified("distance"))),
                )),
                Some(query_table_ref.clone()),
                SEARCH_SCORE_COLUMN_NAME,
            )),
        ];

        query_table_projection_exprs.extend(metadata_columns_to_exprs(
            self.vector_index.metadata_columns(),
        ));

        let query_table_scan = TableScan::try_new(
            query_table_ref.clone(),
            Arc::new(DefaultTableSource::new(query_table)),
            None,
            Self::vector_index_filters(
                &self
                    .vector_index
                    .metadata_columns()
                    .filterable()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
                filters,
            ),
            self.pre_limit.or(limit),
        )?;

        Ok(LogicalPlan::Projection(Projection::try_new(
            query_table_projection_exprs.clone(),
            Arc::new(LogicalPlan::TableScan(query_table_scan)),
        )?))
    }

    // Returns true if the vector index table has all requested columns and can handle all filters (i.e. filters pertain to vector index column, even if they must be post-applied in DataFusion).
    fn vector_index_table_is_sufficient(
        &self,
        vector_index_table: &LogicalPlan,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<bool, DataFusionError> {
        let vector_index_columns: HashSet<String> = vector_index_table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        let full_projection =
            self.vector_index_has_full_projection(&vector_index_columns, projection)?;
        let vector_index_filters = Self::vector_index_filters(&vector_index_columns, filters);

        Ok(full_projection && vector_index_filters.len() == filters.len())
    }
}

#[async_trait]
impl TableProvider for VectorQueryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        append_fields(
            &self.get_underlying_schema(),
            vec![Arc::new(Field::new(
                SEARCH_SCORE_COLUMN_NAME.to_string(),
                arrow_schema::DataType::Float64,
                false,
            ))],
        )
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let base_table = self.table_provider.supports_filters_pushdown(filters)?;

        // For vector index, any filter that is only on vector index will be marked as support (if not supported by engine, we will manually apply thereafter).
        let vector_index_columns: HashSet<String> = self
            .vector_index
            .metadata_columns()
            .filterable()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let vector_index: Vec<_> = filters
            .iter()
            .map(|f| {
                let filter_columns = f
                    .column_refs()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect();
                if vector_index_columns.is_superset(&filter_columns) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        // If one of the two has a pushdown threshold, it can be used.
        Ok(base_table
            .iter()
            .zip(vector_index.iter())
            .map(|(a, b)| match (a, b) {
                (TableProviderFilterPushDown::Exact, _)
                | (_, TableProviderFilterPushDown::Exact) => TableProviderFilterPushDown::Exact,
                (TableProviderFilterPushDown::Inexact, _)
                | (_, TableProviderFilterPushDown::Inexact) => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let primary_key_fields = self.vector_index.primary_fields();
        let Some(pk) = primary_key_fields.first() else {
            return Err(DataFusionError::Execution("Vector search index was successfully created without a primary key available during physical planning.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues".to_string()));
        };
        let vector_index_table = self.vector_index_table(pk, filters, limit).await?;

        // Only join on base table if required.
        let base_logical_plan: LogicalPlan =
            if self.vector_index_table_is_sufficient(&vector_index_table, projection, filters)? {
                // Let DataFusion handle pushing filters.
                if let Some(filter) = fold_binary(filters, Operator::And) {
                    LogicalPlan::Filter(Filter::try_new(filter, vector_index_table.into())?)
                } else {
                    vector_index_table
                }
            } else {
                // DataFusion does not support equi-JOIN predicate pushdown, so by default the full underlying table will be scanned.
                // To improve performance, pre-call the vector index to find the relevant primary keys.
                // Add these primary keys as a filter to the underlying table.
                // Add primary_key as filter `WHERE primary_key_column in ('a_pk', 'another one',...)`.
                let mut underlying_filters = filters.to_vec();
                underlying_filters.push(
                    self.base_table_query_filter(
                        state,
                        state.create_physical_plan(&vector_index_table).await?,
                        pk.name().to_string(),
                    )
                    .await?,
                );

                let underlying_table_scan = self.underlying_table_scan(
                    underlying_filters.as_slice(),
                    embedding_col!(self.vector_index.embedded_column()).as_str(),
                    self.vector_index.metadata_columns().all_names().as_slice(),
                )?;

                let join_schema = vector_index_table
                    .schema()
                    .join(underlying_table_scan.schema())?;

                // If the filter affects the primary key, we must apply after we have removed the duplicate primary key column.
                let (post_join_filters, pre_join_filters): (Vec<Expr>, Vec<Expr>) =
                    filters.iter().cloned().partition(|f| {
                        f.column_refs()
                            .contains(&Column::new_unqualified(pk.name().clone()))
                    });

                let join = LogicalPlan::Join(Join {
                    left: Arc::new(vector_index_table),
                    right: Arc::new(underlying_table_scan),
                    join_type: JoinType::Left,
                    join_constraint: JoinConstraint::On,
                    on: vec![(
                        Expr::Column(Column::new_unqualified(pk.name().clone())),
                        Expr::Column(Column::new_unqualified(pk.name().clone())),
                    )],
                    filter: fold_binary(pre_join_filters.as_slice(), Operator::And),
                    schema: join_schema.into(),
                    null_equals_null: false,
                });

                // DataFusion will not deduplicate the `Join::on` key. For simplicity with non-join
                // case, we will remove first.
                let deduped_schema = DFSchema::new_with_metadata(
                    join.schema()
                        .iter()
                        .filter(|(tbl, f)| {
                            !(f.name() == pk.name()
                                && tbl.is_some_and(|t| *t == TableReference::parse_str("tbl")))
                        })
                        .map(|(tbl, f)| (tbl.cloned(), Arc::clone(f)))
                        .collect(),
                    HashMap::default(),
                )?;

                let proj = LogicalPlan::Projection(Projection::new_from_schema(
                    join.into(),
                    deduped_schema.into(),
                ));

                if let Some(filter) = fold_binary(post_join_filters.as_slice(), Operator::And) {
                    LogicalPlan::Filter(Filter::try_new(filter, proj.into())?)
                } else {
                    proj
                }
            };

        let sort = LogicalPlan::Sort(Sort {
            expr: vec![SortExpr {
                expr: Expr::Column(Column::new_unqualified(SEARCH_SCORE_COLUMN_NAME)),
                asc: false,
                nulls_first: false,
            }],
            input: Arc::new(base_logical_plan),
            fetch: limit,
        });

        let final_proj = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(sort),
            Arc::new(DFSchema::from_unqualified_fields(
                self.qualified_schema(projection)
                    .as_arrow()
                    .fields()
                    .clone(),
                HashMap::default(),
            )?),
        ));

        state.create_physical_plan(&final_proj).await
    }
}

/// For a set of binary filter [`Expr`] = {f1, f2, .., fn} and binary operation op, return expression: `(((f1 op f2) op ...) op fn)`.
#[must_use]
pub fn fold_binary(exprs: &[Expr], op: Operator) -> Option<Expr> {
    let mut iter = exprs.iter();
    let first = iter.next()?.clone();
    Some(iter.fold(first, |acc, expr| {
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(acc),
            op,
            Box::new(expr.clone()),
        ))
    }))
}

/// Convert a [`MetadataColumns`] into a set of [`Expr`]s suitable for a projection.
#[must_use]
fn metadata_columns_to_exprs(metadata_columns: &MetadataColumns) -> Vec<Expr> {
    metadata_columns
        .iter()
        .map(|c| Expr::Column(Column::new_unqualified(c.name())))
        .collect()
}

#[cfg(test)]
mod tests {

    use std::{any::Any, collections::HashMap, sync::Arc};

    use arrow::{
        array::{
            ArrayData, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
            Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array,
            UInt16Array, UInt32Array, UInt64Array, new_null_array,
        },
        buffer::Buffer,
        util::pretty,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use data_components::s3_vectors::{
        MetadataColumn, MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
        query_provider::S3_VECTOR_DISTANCE_NAME,
    };
    use datafusion::{
        catalog::{MemTable, Session, TableProvider},
        datasource::TableType,
        error::DataFusionError,
        logical_expr::TableProviderFilterPushDown,
        physical_plan::{DisplayAs, ExecutionPlan},
        prelude::{Expr, SessionConfig, SessionContext},
        sql::TableReference,
    };
    use snafu::ResultExt;

    use crate::embeddings::{
        index::{VectorIndex, VectorQueryTableProvider},
        udtf::append_fields,
    };

    /// This is just a [`MemTable`] that pretends it can support all filter pushdowns.
    /// This is useful for testing explain plans.
    #[derive(Debug)]
    pub struct ExplainMemTable(MemTable);

    /// Wraps a [`ExecutionPlan`] with a new [`DisplayAs`] to show what filters have been pushed down.
    /// This is useful for testing explain plans.
    #[derive(Debug)]
    pub struct ExplainExecutionPlan(
        Arc<dyn ExecutionPlan>,
        Vec<Expr>,
        Option<usize>,
        Option<Vec<usize>>,
    );

    impl ExecutionPlan for ExplainExecutionPlan {
        fn name(&self) -> &'static str {
            "ExplainExecutionPlan"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
            self.0.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.0.children()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(ExplainExecutionPlan(
                Arc::clone(&self.0).with_new_children(children)?,
                self.1.clone(),
                self.2,
                self.3.clone(),
            )))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
            self.0.execute(partition, context)
        }
    }

    impl DisplayAs for ExplainExecutionPlan {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            let columns: Vec<String> = self
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .cloned()
                .collect();

            write!(
                f,
                "ExplainExecutionPlan: projection={columns:?} filter={:?} limit={:?}",
                self.1, self.2,
            )?;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl TableProvider for ExplainMemTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }

        fn table_type(&self) -> TableType {
            self.0.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            Ok(Arc::new(ExplainExecutionPlan(
                self.0.scan(state, projection, filters, limit).await?,
                filters.to_vec(),
                limit,
                projection.cloned(),
            )) as Arc<dyn ExecutionPlan>)
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
    }

    /// An implementation of [`VectorIndex`] that has one row. Useful for testing explain plans.
    #[derive(Debug)]
    pub struct PretendVectorIndex {
        embedded_column: String,
        primary_columns: Vec<Field>,
        schema: Schema,
        metadata: MetadataColumns,
    }
    impl PretendVectorIndex {
        pub fn new(embedded_column: String, primary_columns: Vec<Field>, schema: Schema) -> Self {
            let cols = schema
                .fields()
                .iter()
                .filter_map(|f| {
                    if f.name() == S3_VECTOR_PRIMARY_KEY_NAME
                        || f.name() == S3_VECTOR_EMBEDDING_NAME
                    {
                        return None;
                    }
                    if f.metadata().get("filterable") == Some(&"true".to_string()) {
                        Some(MetadataColumn::Filterable(Arc::clone(f)))
                    } else {
                        Some(MetadataColumn::NonFilterable(Arc::clone(f)))
                    }
                })
                .collect::<Vec<_>>();

            Self {
                embedded_column,
                primary_columns,
                schema,
                metadata: MetadataColumns::from(cols),
            }
        }
    }

    #[async_trait::async_trait]
    impl VectorIndex for PretendVectorIndex {
        fn embedded_column(&self) -> String {
            self.embedded_column.clone()
        }

        fn primary_fields(&self) -> Vec<Field> {
            self.primary_columns.clone()
        }

        fn list_table_provider(&self) -> Arc<dyn TableProvider> {
            Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::new(self.schema.clone()),
                    vec![vec![one_row_default_record_batch_for_schema(&Arc::new(
                        self.schema.clone(),
                    ))]],
                )
                .expect("Could not build PretendVectorIndex::list_table_provider"),
            ))
        }

        fn metadata_columns(&self) -> &MetadataColumns {
            &self.metadata
        }

        fn augment_table(self: Arc<Self>, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
            table
        }

        async fn write(&self, _record: &RecordBatch) {}
        async fn query_table_provider(
            &self,
            _query: &str,
        ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
            let schema = append_fields(
                &Arc::new(self.schema.clone()),
                vec![Arc::new(Field::new(
                    S3_VECTOR_DISTANCE_NAME,
                    DataType::Float64,
                    false,
                ))],
            );
            println!("In query_table_provider schema={:?}", schema);
            Ok(Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .boxed()?,
            )) as Arc<dyn TableProvider>)
        }
    }

    pub async fn test_explain(
        provider: Arc<dyn TableProvider>,
        tbl: TableReference,
        sql: &str,
        snapshot_name: &str,
    ) -> Result<(), String> {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(3));
        session
            .register_table(tbl, provider)
            .map_err(|e| e.to_string())?;

        let df = session
            .sql(format!("EXPLAIN {sql}").as_str())
            .await
            .map_err(|e| e.to_string())?;

        let col = df.collect().await.map_err(|e| e.to_string())?;
        insta::assert_snapshot!(
            snapshot_name,
            format!(
                "{}",
                pretty::pretty_format_batches(&col).map_err(|e| e.to_string())?
            )
        );
        Ok(())
    }

    #[allow(clippy::cast_sign_loss, clippy::cast_precision_loss)]
    fn default_value_array(dt: &DataType) -> ArrayRef {
        match dt {
            DataType::Int8 => Arc::new(Int8Array::from(vec![0])) as ArrayRef,
            DataType::Int16 => Arc::new(Int16Array::from(vec![0])) as ArrayRef,
            DataType::Int32 => Arc::new(Int32Array::from(vec![0])) as ArrayRef,
            DataType::Int64 => Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            DataType::UInt8 => Arc::new(UInt8Array::from(vec![0])) as ArrayRef,
            DataType::UInt16 => Arc::new(UInt16Array::from(vec![0])) as ArrayRef,
            DataType::UInt32 => Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
            DataType::Float32 => Arc::new(Float32Array::from(vec![0.0])) as ArrayRef,
            DataType::Float64 => Arc::new(Float64Array::from(vec![0.0])) as ArrayRef,
            DataType::Boolean => Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            DataType::Utf8 => Arc::new(StringArray::from(vec![""])) as ArrayRef,
            DataType::FixedSizeList(_, length) => {
                let list_data_type = DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, false)),
                    *length,
                );
                Arc::new(FixedSizeListArray::from(
                    ArrayData::builder(list_data_type.clone())
                        .len(1)
                        .add_child_data(
                            ArrayData::builder(DataType::Float32)
                                .len(*length as usize)
                                .add_buffer(Buffer::from_slice_ref(
                                    (0..(*length as usize))
                                        .map(|s| s as f32)
                                        .collect::<Vec<_>>(),
                                ))
                                .build()
                                .expect("unable to build FixedSizeListArray's ArrayData"),
                        )
                        .build()
                        .expect("unable to build FixedSizeListArray"),
                ))
            }
            _ => new_null_array(dt, 1),
        }
    }

    /// Creates a [`RecordBatch`] with a single row that has default value of types, as per the [`Schema`].
    fn one_row_default_record_batch_for_schema(schema: &Arc<Schema>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| default_value_array(field.data_type()))
            .collect();

        RecordBatch::try_new(Arc::clone(schema), arrays)
            .expect("could not build RecordBatch with one row")
    }

    #[tokio::test]
    pub async fn test_vector_query_basic() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
        ]));
        let p = VectorQueryTableProvider {
            table_provider: Arc::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
            ),
            vector_index: Arc::new(PretendVectorIndex::new(
                "body".to_string(),
                vec![Field::new("pk", DataType::Int64, false)],
                Schema::new(vec![
                    Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
                    Field::new(
                        S3_VECTOR_EMBEDDING_NAME,
                        DataType::new_fixed_size_list(DataType::Float32, 10, false),
                        false,
                    ),
                ]),
            )),
            query: "just a query".to_string(),
            pre_limit: None,
        };
        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "basic",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "join_for_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, score from my_vectored_table WHERE another_column != 'something' ORDER BY score desc LIMIT 5",
            "join_for_filter",
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    pub async fn test_vector_query_index_metadata() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
            Field::new("a_number", DataType::Int64, false),
            Field::new("not_where", DataType::Utf8, false),
        ]));
        let p = VectorQueryTableProvider {
            table_provider: Arc::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
            ),
            vector_index: Arc::new(PretendVectorIndex::new(
                "body".to_string(),
                vec![Field::new("pk", DataType::Int64, false)],
                Schema::new(vec![
                    Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
                    Field::new(
                        S3_VECTOR_EMBEDDING_NAME,
                        DataType::new_fixed_size_list(DataType::Float32, 10, false),
                        false,
                    ),
                    Field::new("a_number", DataType::Int64, false).with_metadata(HashMap::from([
                        ("filterable".to_string(), "true".to_string()),
                    ])),
                    Field::new("not_where", DataType::Utf8, false).with_metadata(HashMap::from([
                        ("filterable".to_string(), "false".to_string()),
                    ])),
                ]),
            )),
            query: "just a query".to_string(),
            pre_limit: None,
        };
        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "basic",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "join_for_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, not_where, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "join_for_projection_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, score from my_vectored_table WHERE another_column != 'something' AND a_number > 0 ORDER BY score desc LIMIT 5",
            "join_for_filter_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, not_where, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "no_join_for_metadata_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, score from my_vectored_table WHERE a_number > 0 ORDER BY score desc LIMIT 5",
            "no_join_for_metadata_filter",
        )
        .await?;

        Ok(())
    }
}
