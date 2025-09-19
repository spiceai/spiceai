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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_schema::{DataType, Field};
use async_trait::async_trait;

use datafusion::{
    catalog::Session,
    common::{Column, Constraints, DFSchema, DFSchemaRef, JoinConstraint, JoinType, NullEquality},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        Expr, Filter, Join, Limit, LogicalPlan, Projection, TableProviderFilterPushDown, TableScan,
    },
    physical_plan::ExecutionPlan,
    scalar::ScalarValue,
    sql::TableReference,
};
use datafusion_expr::SubqueryAlias;

use crate::{
    embedding_col,
    embeddings::index::{VectorIndex, search_index_table_is_sufficient},
};
use search::generation::util::append_fields;

/// A [`TableProvider`] that adds an embedding column to an underlying [`TableProvider`].
#[derive(Debug, Clone)]
pub struct VectorScanTableProvider {
    pub table_provider: Arc<dyn TableProvider>,
    pub index: Arc<dyn VectorIndex>,
}

impl VectorScanTableProvider {
    pub fn new(table_provider: Arc<dyn TableProvider>, index: Arc<dyn VectorIndex>) -> Self {
        Self {
            table_provider,
            index,
        }
    }

    /// Construct [`TableScan`] for underlying table for `projection` & `filters` relative to [`VectorScanTableProvider`].
    fn underlying_table_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> DataFusionResult<TableScan> {
        let num_underlying_columns = self.table_provider.schema().fields().len();
        let underlying_projection = projection.map(|proj| {
            proj.iter()
                .filter(|&idx| *idx < num_underlying_columns)
                .copied()
                .collect()
        });

        let filter_refs: Vec<&Expr> = filters.iter().collect();
        let underlying_filters = self
            .table_provider
            .supports_filters_pushdown(filter_refs.as_slice())?
            .into_iter()
            .zip(filters.iter())
            .filter_map(|(supported, filter)| {
                if matches!(supported, TableProviderFilterPushDown::Unsupported) {
                    None
                } else {
                    Some(filter.clone())
                }
            })
            .collect::<Vec<_>>();

        TableScan::try_new(
            TableReference::parse_str("base_table"),
            Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
            underlying_projection,
            underlying_filters,
            None,
        )
    }

    /// For a projection relative to [`VectorScanTableProvider`], check if the embedding column is being requested.
    fn need_vector_column(&self, projection: Option<&Vec<usize>>) -> bool {
        let Some(proj) = projection else {
            return true; // None projection -> "SELECT *".
        };

        let Some(idx) = index_of_column(
            &self.schema(),
            embedding_col!(self.index.search_column()).as_str(),
        ) else {
            return false; // Technically unreachable, but by definition not needed.
        };

        proj.contains(&idx)
    }

    fn qualified_schema(&self, projection: Option<&Vec<usize>>) -> DFSchemaRef {
        let base = self.table_provider.schema();
        let mut qualified_fields: Vec<_> = base
            .fields()
            .iter()
            .map(|f| (Some(TableReference::parse_str("base_table")), Arc::clone(f)))
            .collect();
        qualified_fields.push((
            Some(TableReference::parse_str("vector_index")),
            Arc::new(Field::new(
                embedding_col!(self.index.search_column()),
                DataType::new_list(DataType::Float32, false),
                true,
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
}

fn index_of_column(s: &SchemaRef, col: &str) -> Option<usize> {
    Some(s.column_with_name(col)?.0)
}

#[async_trait]
impl TableProvider for VectorScanTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        append_fields(
            &self.table_provider.schema(),
            vec![Arc::new(Field::new(
                embedding_col!(self.index.search_column()),
                DataType::new_list(DataType::Float32, false),
                true,
            ))],
        )
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.table_provider.constraints()
    }

    fn table_type(&self) -> TableType {
        self.table_provider.table_type()
    }

    #[allow(clippy::too_many_lines)]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Filter pushdown not supported for S3 vector listVectors. If vector is not needed in projection, do not need to join on this table.
        if !self.need_vector_column(projection) {
            return self
                .table_provider
                .scan(state, projection, filters, limit)
                .await;
        }

        let mut proj = self
            .index
            .primary_fields()
            .iter()
            .map(|f| {
                Expr::Column(Column::new(
                    Some(TableReference::parse_str("vector_index")),
                    f.name().clone(),
                ))
            })
            .collect::<Vec<_>>();
        proj.push(Expr::Column(Column::new(
            Some(TableReference::parse_str("vector_index")),
            embedding_col!(self.index.search_column()),
        )));

        let index_logical_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            self.index.list_table_provider()?.into(),
            TableReference::parse_str("vector_index"),
        )?);

        let primary_key_fields = self.index.primary_fields();
        if primary_key_fields.is_empty() {
            return Err(DataFusionError::Execution("The vector search index was created successfuly without a primary key.\nEnsure a primary key is available in the dataset source, or specified in the column configuration.\nFor details, visit: https://spiceai.org/docs/reference/spicepod/datasets#columnsembeddingsrow_id".to_string()));
        }

        let projection_schema: SchemaRef = match projection {
            None => self.schema(),
            Some(indices) => self.schema().project(indices)?.into(),
        };

        let output_plan = if search_index_table_is_sufficient(
            projection_schema.fields().iter().as_slice(),
            &index_logical_plan,
            filters,
        ) {
            // Let DataFusion handle pushing filters.
            if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
                LogicalPlan::Filter(Filter::try_new(filter, index_logical_plan.into())?)
            } else {
                index_logical_plan
            }
        } else {
            let underlying_table_scan =
                LogicalPlan::TableScan(self.underlying_table_scan(projection, filters)?);

            // Don't get metadata columns from index. Only vectors and primary key.
            let index_logical_projection =
                LogicalPlan::Projection(Projection::try_new(proj, index_logical_plan.into())?);
            let join_schema = index_logical_projection
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
                .map(|field| {
                    (
                        Expr::Column(Column::new(
                            Some(TableReference::parse_str("vector_index")),
                            field.name(),
                        )),
                        Expr::Column(Column::new(
                            Some(TableReference::parse_str("base_table")),
                            field.name(),
                        )),
                    )
                })
                .collect();

            // Right Join so that all rows in the underlying table are returned.
            // Rows may not have associated vectors periodically due to indexing delays.
            let join = LogicalPlan::Join(Join {
                left: Arc::new(index_logical_projection),
                right: Arc::new(underlying_table_scan),
                join_type: JoinType::Right,
                join_constraint: JoinConstraint::On,
                on: join_conditions,
                filter: pre_join_filters.into_iter().reduce(Expr::and),
                schema: join_schema.into(),
                null_equality: NullEquality::NullEqualsNothing,
            });

            // DataFusion will not deduplicate the `Join::on` keys. For simplicity with non-join
            // case, we will remove duplicate primary key columns from the right table.
            let deduped_join_proj_exprs: Vec<_> = join
                .schema()
                .iter()
                .filter(|(tbl, f)| {
                    !(primary_key_column_names.contains(f.name())
                        && tbl.is_some_and(|t| *t == TableReference::parse_str("vector_index")))
                })
                .map(|(tbl, field_ref)| match tbl {
                    Some(table_ref) => {
                        Expr::Column(Column::new(Some(table_ref.clone()), field_ref.name()))
                    }
                    None => Expr::Column(Column::new(None::<TableReference>, field_ref.name())),
                })
                .collect();

            let proj =
                LogicalPlan::Projection(Projection::try_new(deduped_join_proj_exprs, join.into())?);

            if let Some(filter) = post_join_filters.into_iter().reduce(Expr::and) {
                LogicalPlan::Filter(Filter::try_new(filter, proj.into())?)
            } else {
                proj
            }
        };

        let output_proj = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(output_plan),
            Arc::new(DFSchema::from_unqualified_fields(
                self.qualified_schema(projection)
                    .as_arrow()
                    .fields()
                    .clone(),
                HashMap::default(),
            )?),
        ));

        let limit = LogicalPlan::Limit(Limit {
            input: Arc::new(output_proj),
            fetch: Some(Box::new(Expr::Literal(
                ScalarValue::UInt64(limit.map(|l| l as u64)),
                None,
            ))),
            skip: None,
        });

        state.create_physical_plan(&limit).await
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        catalog::{MemTable, TableProvider},
        sql::TableReference,
    };

    use crate::embeddings::index::tests::{
        PretendVectorIndex, one_row_default_record_batch_for_schema, test_explain,
    };
    use crate::embeddings::index::{VectorScanTableProvider, tests::ExplainMemTable};

    #[tokio::test]
    pub async fn test_vector_scan_basic() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
        ]));

        let p = VectorScanTableProvider {
            table_provider: Arc::new(ExplainMemTable::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            index: Arc::new(PretendVectorIndex::new(
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
        };

        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_basic",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE another_column != 'something' ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_filter",
        )
        .await?;

        Ok(())
    }

    // [`VectorScanTableProvider`] cannot use metadata column to get data from vector index.
    #[tokio::test]
    pub async fn test_vector_scan_index_metadata() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
            Field::new("a_number", DataType::Int64, false),
            Field::new("not_where", DataType::Utf8, false),
        ]));
        let p = VectorScanTableProvider {
            table_provider: Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            index: Arc::new(PretendVectorIndex::new(
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
        };
        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_basic_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_projection_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, not_where, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_projection_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE another_column != 'something' AND a_number > 0 ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_filter_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, not_where, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_no_join_for_metadata_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE a_number > 0 ORDER BY pk desc LIMIT 5",
            "scan_table_no_join_for_metadata_filter",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, a_number from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_no_embedding_no_join",
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    pub async fn test_vector_scan_index_multicolumn_pk() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk1", DataType::Int64, false),
            Field::new("pk2", DataType::Boolean, false),
            Field::new("pk3", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
            Field::new("a_number", DataType::Int64, false),
            Field::new("not_where", DataType::Utf8, false),
        ]));
        let p = VectorScanTableProvider {
            table_provider: Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            index: Arc::new(PretendVectorIndex::new(
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
        };
        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, body_embedding from my_vectored_table LIMIT 5",
            "scan_table_basic_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, another_column, body_embedding from my_vectored_table LIMIT 5",
            "scan_table_join_for_projection_metadata_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, another_column, not_where, body_embedding from my_vectored_table LIMIT 5",
            "scan_table_join_for_projection_use_metadata_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, body_embedding from my_vectored_table WHERE another_column != 'something' AND a_number > 0 LIMIT 5",
            "scan_table_join_for_filter_use_metadata_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, not_where, body_embedding from my_vectored_table LIMIT 5",
            "scan_table_no_join_for_metadata_projection_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, body_embedding from my_vectored_table WHERE a_number > 0 LIMIT 5",
            "scan_table_no_join_for_metadata_filter_multiple_pk",
        )
        .await?;

        Ok(())
    }
}
