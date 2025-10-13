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
    common::{Column, Constraints, JoinType},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{Expr, LogicalPlan},
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use datafusion_expr::{LogicalPlanBuilder, ident};

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
        input: LogicalPlanBuilder,
        projection: &HashSet<String>,
        filters: &[Expr],
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let filtered = if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            input.filter(filter)?
        } else {
            input
        };

        filtered.project(
            projection
                .iter()
                .sorted_unstable()
                .cloned()
                .map(ident)
                .collect::<Vec<Expr>>(),
        )
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

    /// Return all columns that appear in the [`Self::vector_index_list`] that are not in [`Self::table_provider`] as well as all primary keys.
    fn columns_needed_from_index(&self) -> Vec<Expr> {
        let table_schema = self.table_provider.schema();
        self.vector_index_list
            .schema()
            .columns()
            .into_iter()
            .filter(|c| {
                table_schema.column_with_name(&c.name).is_none()
                    || self.primary_key.contains(&c.name)
            })
            .map(Expr::Column)
            .collect()
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
                // Any field only present in vector index must be nullable since row may be in `self.table_provider` before `self.vector_index_list`.
                fields_map.insert(
                    f.name().clone(),
                    Arc::new(Arc::unwrap_or_clone(Arc::clone(f)).with_nullable(true)),
                );
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
                LogicalPlanBuilder::scan(
                    "base_table",
                    Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
                    None,
                )?,
                &columns_requested,
                filters,
            )?
            .build()?;

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
        //         LogicalPlanBuilder::new_from_arc(Arc::clone(&self.vector_index_list)),
        //         &columns_requested,
        //         filters,
        //     )?
        //     .build()?;

        //     return state.create_physical_plan(&lp).await;
        // }

        // Join on primary keys, prefer to use columns from base table, push down filters where we can.
        let mut join = LogicalPlanBuilder::scan(
            "base_table",
            Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
            None,
        )?
        .join(
            LogicalPlanBuilder::new_from_arc(Arc::clone(&self.vector_index_list))
                .project(self.columns_needed_from_index())?
                .alias("vector_index")?
                .build()?,
            JoinType::Left,
            self.primary_key
                .iter()
                .map(|pk| (Column::from_name(pk.clone()), Column::from_name(pk.clone())))
                .collect(),
            // If the filter affects any primary key column, we must apply after we have removed the duplicate primary key columns.
            filters
                .iter()
                .filter(|f| {
                    f.column_refs()
                        .iter()
                        .any(|col| !self.primary_key.contains(&col.name))
                })
                .cloned()
                .reduce(Expr::and),
        )?;

        let join_schema = Arc::clone(join.schema());
        join = join.project(
            // DataFusion will not deduplicate the `Join::on` keys. For simplicity with non-join
            // case, we will remove duplicate primary key columns from the right table.
            join_schema
                .iter()
                .filter(|(tbl, f)| {
                    !(self.primary_key.contains(f.name())
                        && tbl.is_some_and(|t| *t == TableReference::parse_str("vector_index")))
                })
                .map(|(tbl, field_ref)| match tbl {
                    Some(table_ref) => Column::new(Some(table_ref.clone()), field_ref.name()),
                    None => Column::new(None::<TableReference>, field_ref.name()),
                }),
        )?;

        if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            join = join.filter(filter)?;
        }

        join = join
            .project(
                columns_requested
                    .into_iter()
                    .sorted_unstable()
                    .map(ident)
                    .collect::<Vec<Expr>>(),
            )?
            .limit(0, limit)?;

        state.create_physical_plan(&join.build()?).await
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
