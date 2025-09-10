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
    cmp::min,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use arrow_schema::{Field, Schema};
use async_trait::async_trait;

use data_components::s3_vectors::MetadataColumns;

use datafusion::{
    catalog::Session,
    common::{Column, Constraints, DFSchema, DFSchemaRef, JoinConstraint, JoinType, NullEquality},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        Expr, Filter, Join, LogicalPlan, Projection, Sort, SortExpr, TableProviderFilterPushDown,
        TableScan,
        expr::{InList, ScalarFunction},
    },
    physical_plan::ExecutionPlan,
    prelude::col,
    scalar::ScalarValue,
    sql::TableReference,
};

use runtime_datafusion_index::IndexedTableProvider;
use search::SEARCH_SCORE_COLUMN_NAME;

use tokio_stream::StreamExt;

use crate::{
    embedding_col,
    embeddings::index::{
        VectorIndex, projection_without_columns, vector_index_table_is_sufficient,
    },
};
use crate::{embeddings::index::vector_index_filters, search::util::find_concrete_table_provider};
use search::generation::util::append_fields;

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
    /// If a `limit` is provided such that `limit` < `pre_limit`, `limit` will be used.
    pub pre_limit: Option<usize>,
}

impl VectorQueryTableProvider {
    /// Execute the given physical plan of a vector index query, extract the primary key columns and convert the values: {(`v1_1`, `v1_2`, ...), (`v2_1`, `v2_2`, ...), ..., (`vn_1`, `vn_2`, ...)} into a filter predicate: `WHERE (primary_key_col1, primary_key_col2, ...) IN ((v1_1, v1_2, ...), (v2_1, v2_2, ...), ...)`.
    ///
    /// When `primary_key_fields.len() == 1`, use a simplified expression `WHERE primary_key_col1 IN (v1_1, v2_1, ...)`.
    ///
    /// If no primary-keys/rows are produced from the `physical_plan`, returns `Ok(None)`.
    async fn base_table_query_filter(
        &self,
        state: &dyn Session,
        physical_plan: Arc<dyn ExecutionPlan>,
        primary_key_fields: &[Field],
    ) -> DataFusionResult<Option<Expr>> {
        if primary_key_fields.is_empty() {
            return Err(DataFusionError::Execution(
                "No primary key columns provided".to_string(),
            ));
        }

        // For single column primary key, maintain the existing behavior
        if primary_key_fields.len() == 1 {
            let primary_key_column = &primary_key_fields[0];
            let mut expr = vec![];

            let mut strm = physical_plan.execute(0, state.task_ctx())?;
            while let Some(Ok(rb)) = strm.next().await {
                if let Some(arr) = rb.column_by_name(primary_key_column.name()) {
                    for i in 0..arr.len() {
                        expr.push(Expr::Literal(ScalarValue::try_from_array(arr, i)?, None));
                    }
                }
            }
            return Ok(Some(Expr::InList(InList::new(
                Box::new(Expr::Column(Column::from_name(primary_key_column.name()))),
                expr,
                false,
            ))));
        }

        // For composite primary keys, collect struct values for IN expression
        let mut struct_exprs = vec![];

        let mut strm = physical_plan.execute(0, state.task_ctx())?;
        while let Some(Ok(rb)) = strm.next().await {
            // Get all arrays for primary key columns
            let mut arrays = vec![];
            let pk_names: Vec<String> = primary_key_fields
                .iter()
                .map(|pk| pk.name().clone())
                .collect();
            for pk_col in primary_key_fields {
                if let Some(arr) = rb.column_by_name(pk_col.name()) {
                    arrays.push(arr);
                } else {
                    return Err(DataFusionError::Execution(format!(
                        "Primary key column '{}' not found in query result",
                        pk_col.name()
                    )));
                }
            }

            // Build struct values for each row
            let num_rows = arrays.first().map_or(0, |arr| arr.len());
            if num_rows == 0 {
                return Ok(None);
            }
            for i in 0..num_rows {
                let mut field_values: Vec<(&str, ScalarValue)> = vec![];
                for (j, arr) in arrays.iter().enumerate() {
                    field_values.push((
                        pk_names
                            .get(j)
                            .map(std::string::String::as_str)
                            .unwrap_or_default(),
                        ScalarValue::try_from_array(arr, i)?,
                    ));
                }
                struct_exprs.push(Expr::Literal(field_values.into(), None));
            }
        }

        // Create struct expression for LHS of IN: struct(col1, col2, ...)
        let struct_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                datafusion::functions::core::r#struct::StructFunc::new(),
            )),
            primary_key_fields.iter().map(|f| col(f.name())).collect(),
        ));

        Ok(Some(Expr::InList(InList::new(
            Box::new(struct_expr),
            struct_exprs,
            false,
        ))))
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
            projection_without_columns(&self.schema().fields, metadata_columns, Some(&base_proj));
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

        let plan = if let Some(filter) = underlying_filters.into_iter().reduce(Expr::and) {
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

    async fn vector_index_table(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<LogicalPlan, DataFusionError> {
        let query_table = self
            .vector_index
            .query_table_provider(self.query.as_str())
            .await?;

        let query_table_scan = TableScan::try_new(
            TableReference::parse_str("vector_index"),
            Arc::new(DefaultTableSource::new(query_table)),
            None,
            vector_index_filters(
                &self
                    .vector_index
                    .metadata_columns()
                    .filterable()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
                filters,
            ),
            self.limit_to_use(limit),
        )?;

        Ok(LogicalPlan::TableScan(query_table_scan))
    }

    /// Determine whether and how to pick between
    ///   1. The query-provided limit (i.e. passed through in the SQL/Logical plan)
    ///   2. The pre-limit configured in [`VectorQueryTableProvider::pre_limit`].
    fn limit_to_use(&self, limit: Option<usize>) -> Option<usize> {
        match (self.pre_limit, limit) {
            (Some(l), None) | (None, Some(l)) => Some(l),
            (None, None) => None,

            // Equivalent to using always using pre_limit, unless `limit` < `pre_limit`.
            (Some(a), Some(b)) => Some(min(a, b)),
        }
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

    #[allow(clippy::too_many_lines)]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let primary_key_fields = self.vector_index.primary_fields();
        if primary_key_fields.is_empty() {
            return Err(DataFusionError::Execution("The vector search index was created successfuly without a primary key.\nEnsure a primary key is available in the dataset source, or specified in the column configuration.\nFor details, visit: https://spiceai.org/docs/reference/spicepod/datasets#columnsembeddingsrow_id".to_string()));
        }
        let vector_index_table = self.vector_index_table(filters, limit).await?;

        // Only join on base table if required.
        let base_logical_plan: LogicalPlan = if vector_index_table_is_sufficient(
            self.schema(),
            &vector_index_table,
            projection,
            filters,
        )? {
            // Let DataFusion handle pushing filters.
            if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
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
            if let Some(filter) = self
                .base_table_query_filter(
                    state,
                    state.create_physical_plan(&vector_index_table).await?,
                    &primary_key_fields,
                )
                .await?
            {
                underlying_filters.push(filter);
            }

            let underlying_table_scan = self.underlying_table_scan(
                underlying_filters.as_slice(),
                embedding_col!(self.vector_index.embedded_column()).as_str(),
                self.vector_index.metadata_columns().all_names().as_slice(),
            )?;

            let join_schema = vector_index_table
                .schema()
                .join(underlying_table_scan.schema())?;

            // If the filter affects any primary key column, we must apply after we have removed the duplicate primary key columns.
            let primary_key_column_names: std::collections::HashSet<String> = primary_key_fields
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let (post_join_filters, pre_join_filters): (Vec<Expr>, Vec<Expr>) =
                filters.iter().cloned().partition(|f| {
                    f.column_refs()
                        .iter()
                        .any(|col| primary_key_column_names.contains(col.name()))
                });

            let join_conditions: Vec<(Expr, Expr)> = primary_key_fields
                .iter()
                .map(|pk_field| {
                    (
                        Expr::Column(Column::new_unqualified(pk_field.name())),
                        Expr::Column(Column::new_unqualified(pk_field.name())),
                    )
                })
                .collect();

            let join = LogicalPlan::Join(Join {
                left: Arc::new(vector_index_table),
                right: Arc::new(underlying_table_scan),
                join_type: JoinType::Left,
                join_constraint: JoinConstraint::On,
                on: join_conditions,
                filter: pre_join_filters.into_iter().reduce(Expr::and),
                schema: join_schema.into(),
                null_equality: NullEquality::NullEqualsNothing,
            });

            // DataFusion will not deduplicate the `Join::on` keys. For simplicity with non-join
            // case, we will remove duplicate primary key columns from the right table.
            let deduped_schema = DFSchema::new_with_metadata(
                join.schema()
                    .iter()
                    .filter(|(tbl, f)| {
                        !(primary_key_column_names.contains(f.name())
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

            if let Some(filter) = post_join_filters.into_iter().reduce(Expr::and) {
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

/// Convert a [`MetadataColumns`] into a set of [`Expr`]s suitable for a projection.
#[must_use]
pub(super) fn metadata_columns_to_exprs(metadata_columns: &MetadataColumns) -> Vec<Expr> {
    metadata_columns
        .iter()
        .map(|c| Expr::Column(Column::new_unqualified(c.name())))
        .collect()
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        catalog::{MemTable, TableProvider},
        sql::TableReference,
    };

    use crate::embeddings::index::VectorQueryTableProvider;
    use crate::embeddings::index::tests::{
        PretendVectorIndex, one_row_default_record_batch_for_schema, test_explain,
    };

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
                    Field::new("pk", DataType::Int64, false),
                    Field::new(
                        "body_embedding",
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
            "query_table_basic",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "query_table_join_for_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, score from my_vectored_table WHERE another_column != 'something' ORDER BY score desc LIMIT 5",
            "query_table_join_for_filter",
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
                    Field::new("pk", DataType::Int64, false),
                    Field::new(
                        "body_embedding",
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
            "query_table_basic_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "query_table_join_for_projection_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, not_where, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "query_table_join_for_projection_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, score from my_vectored_table WHERE another_column != 'something' AND a_number > 0 ORDER BY score desc LIMIT 5",
            "query_table_join_for_filter_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, not_where, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "query_table_no_join_for_metadata_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, score from my_vectored_table WHERE a_number > 0 ORDER BY score desc LIMIT 5",
            "query_table_no_join_for_metadata_filter",
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    pub async fn test_vector_query_index_multicolumn_pk() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk1", DataType::Int64, false),
            Field::new("pk2", DataType::Boolean, false),
            Field::new("pk3", DataType::Utf8, false),
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
                vec![
                    Field::new("pk1", DataType::Int64, false),
                    Field::new("pk2", DataType::Boolean, false),
                    Field::new("pk3", DataType::Utf8, false),
                ],
                Schema::new(vec![
                    Field::new("pk1", DataType::Int64, false),
                    Field::new("pk2", DataType::Boolean, false),
                    Field::new("pk3", DataType::Utf8, false),
                    Field::new(
                        "body_embedding",
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
            "SELECT pk1, pk2, pk3, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "query_table_basic_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, another_column, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "query_table_join_for_projection_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, another_column, not_where, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "query_table_join_for_projection_use_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, score from my_vectored_table WHERE another_column != 'something' AND a_number > 0 ORDER BY score desc LIMIT 5",
            "query_table_join_for_filter_use_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, not_where, score from my_vectored_table ORDER BY score desc LIMIT 5",
            "query_table_no_join_for_metadata_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, score from my_vectored_table WHERE a_number > 0 ORDER BY score desc LIMIT 5",
            "query_table_no_join_for_metadata_filter_multiple_pk",
        )
        .await?;

        Ok(())
    }
}
