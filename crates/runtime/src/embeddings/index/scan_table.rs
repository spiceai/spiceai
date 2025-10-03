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
use arrow_schema::{FieldRef, Fields, Schema};
use async_trait::async_trait;

use datafusion::{
    catalog::Session,
    common::{Column, Constraints, JoinConstraint, JoinType, NullEquality},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{Expr, Filter, Join, Limit, LogicalPlan, Projection, TableScan},
    physical_plan::ExecutionPlan,
    scalar::ScalarValue,
    sql::TableReference,
};
use datafusion_expr::{SubqueryAlias, ident};

use itertools::Itertools;
use search::index::VectorIndex;

/// A [`TableProvider`] that adds an embedding column to an underlying [`TableProvider`].
#[derive(Debug, Clone)]
pub struct VectorScanTableProvider {
    pub table_provider: Arc<dyn TableProvider>,
    pub vector_index_list: Arc<LogicalPlan>,
    pub primary_key: Vec<String>,
}

impl VectorScanTableProvider {
    pub fn try_new(
        table_provider: Arc<dyn TableProvider>,
        index: &Arc<dyn VectorIndex>,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            table_provider,
            primary_key: index
                .primary_fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            vector_index_list: index.list_table_provider()?.into(),
        })
    }

    fn schema_is_sufficient(
        schema: &Fields,
        projection: &HashSet<String>,
        filters: &[Expr],
    ) -> bool {
        if !projection.is_subset(
            &schema
                .iter()
                .map(|f| f.name().to_string())
                .collect::<HashSet<String>>(),
        ) {
            // schema does not have all columns.
            return false;
        }
        // Ensure filters do not reference column not in the schema
        columns_missing_from(filters, schema).is_empty()
    }

    fn apply_proj_and_filter(
        input: Arc<LogicalPlan>,
        projection: &HashSet<String>,
        filters: &[Expr],
    ) -> Result<LogicalPlan, DataFusionError> {
        let filtered = if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            Arc::new(LogicalPlan::Filter(Filter::try_new(filter, input)?))
        } else {
            input
        };

        Ok(LogicalPlan::Projection(Projection::try_new(
            projection
                .iter()
                .sorted_unstable()
                .map(|p| Expr::Column(Column::new_unqualified(p.clone())))
                .collect(),
            filtered,
        )?))
    }

    fn columns_projected(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<HashSet<String>, DataFusionError> {
        let source_schema = match projection {
            None => self.schema(),
            Some(indices) => {
                let projected = self
                    .schema()
                    .project(indices)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Arc::new(projected)
            }
        };
        let columns_requested: HashSet<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        Ok(columns_requested)
    }
}

// Return the unqualified names of columns missing from those referenced by in `expr`.
fn columns_missing_from(expr: &[Expr], schema: &Fields) -> Vec<String> {
    let schema_cols = schema
        .iter()
        .map(|f| f.name().clone())
        .collect::<HashSet<_>>();

    expr.iter()
        .flat_map(|e| {
            let filter_cols = e
                .column_refs()
                .iter()
                .map(|c| c.name().to_string())
                .collect::<HashSet<_>>();
            filter_cols
                .difference(&schema_cols)
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

#[async_trait]
impl TableProvider for VectorScanTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut fields_map = self
            .table_provider
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), Arc::clone(f)))
            .collect::<HashMap<String, FieldRef>>();

        // Only add if key not in base table (we chose base table over index columns in `scan` afterall).
        for f in self.vector_index_list.schema().fields() {
            if !fields_map.contains_key(f.name()) {
                fields_map.insert(f.name().clone(), Arc::clone(f));
            }
        }

        let mut fields = fields_map.values().cloned().collect::<Vec<_>>();
        fields.sort_unstable();
        Arc::new(Schema::new(fields))
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
        let columns_requested = self.columns_projected(projection)?;

        if Self::schema_is_sufficient(
            self.table_provider.schema().fields(),
            &columns_requested,
            filters,
        ) {
            let lp = Self::apply_proj_and_filter(
                Arc::new(LogicalPlan::TableScan(TableScan::try_new(
                    "base_table",
                    Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
                    None,
                    vec![],
                    None,
                )?)),
                &columns_requested,
                filters,
            )?;

            return state.create_physical_plan(&lp).await;
        }

        // Reenable once we can distinguish between query and indexing `.scan()`.
        // See `<https://github.com/spiceai/spiceai/issues/7404>`
        // if Self::schema_is_sufficient(
        //     self.vector_index_list.schema().fields(),
        //     &columns_requested,
        //     filters,
        // ) {
        //     let lp = Self::apply_proj_and_filter(
        //         Arc::clone(&self.vector_index_list),
        //         &columns_requested,
        //         filters,
        //     )?;

        //     return state.create_physical_plan(&lp).await;
        // }
        let base_ts = LogicalPlan::TableScan(TableScan::try_new(
            TableReference::parse_str("base_table"),
            Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
            None,
            vec![],
            None,
        )?);

        // Only include fields from index that aren't in base table (including metadata), except primary key.
        let index_lp = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(LogicalPlan::Projection(Projection::try_new(
                self.vector_index_list
                    .schema()
                    .columns()
                    .iter()
                    .filter(|c| {
                        base_ts
                            .schema()
                            .columns_with_unqualified_name(&c.name)
                            .is_empty()
                            || self.primary_key.contains(&c.name)
                    })
                    .map(|c| Expr::Column(c.clone()))
                    .collect(),
                Arc::clone(&self.vector_index_list),
            )?)),
            TableReference::parse_str("vector_index"),
        )?);

        let join_schema = base_ts.schema().join(index_lp.schema())?;

        // If the filter affects any primary key column, we must apply after we have removed the duplicate primary key columns.
        let join_filters: Vec<Expr> = filters
            .iter()
            .filter(|f| {
                f.column_refs()
                    .iter()
                    .any(|col| !self.primary_key.contains(&col.name))
            })
            .cloned()
            .collect();

        let join_conditions: Vec<(Expr, Expr)> = self
            .primary_key
            .iter()
            .map(|pk| {
                (
                    Expr::Column(Column::new(
                        Some(TableReference::parse_str("base_table")),
                        pk.clone(),
                    )),
                    Expr::Column(Column::new(
                        Some(TableReference::parse_str("vector_index")),
                        pk.clone(),
                    )),
                )
            })
            .collect();

        // Left Join so that all rows in the underlying table are returned.
        // Rows may not have associated vectors periodically due to indexing delays.
        let join = LogicalPlan::Join(Join {
            left: Arc::new(base_ts),
            right: Arc::new(index_lp),
            join_type: JoinType::Left,
            join_constraint: JoinConstraint::On,
            on: join_conditions,
            filter: join_filters.into_iter().reduce(Expr::and),
            schema: join_schema.into(),
            null_equality: NullEquality::NullEqualsNothing,
        });

        // DataFusion will not deduplicate the `Join::on` keys. For simplicity with non-join
        // case, we will remove duplicate primary key columns from the right table.
        let deduped_join_proj_exprs: Vec<_> = join
            .schema()
            .iter()
            .filter(|(tbl, f)| {
                !(self.primary_key.contains(f.name())
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

        let filtered = if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            LogicalPlan::Filter(Filter::try_new(filter, proj.into())?)
        } else {
            proj
        };

        let output_proj = LogicalPlan::Projection(Projection::try_new(
            columns_requested
                .into_iter()
                .sorted_unstable()
                .map(ident)
                .collect(),
            Arc::new(filtered),
        )?);

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
    use search::index::VectorIndex;

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

        let p = VectorScanTableProvider::try_new(
            Arc::new(ExplainMemTable::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            &(Arc::new(PretendVectorIndex::new(
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
            )) as Arc<dyn VectorIndex>),
        )
        .expect("could not make 'VectorScanTableProvider'");

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
        let p = VectorScanTableProvider::try_new(
            Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            &(Arc::new(PretendVectorIndex::new(
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
            )) as Arc<dyn VectorIndex>),
        )
        .expect("could not make 'VectorScanTableProvider'");

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
        let p = VectorScanTableProvider::try_new(
            Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            &(Arc::new(PretendVectorIndex::new(
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
            )) as Arc<dyn VectorIndex>),
        )
        .expect("could not make 'VectorScanTableProvider'");

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
