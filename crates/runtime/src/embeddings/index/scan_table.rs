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

use data_components::s3_vectors::{S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME};

use datafusion::{
    catalog::Session,
    common::{Column, Constraints, DFSchema, DFSchemaRef, JoinConstraint, JoinType},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        Cast, Expr, Join, Limit, LogicalPlan, Operator, Projection, TableProviderFilterPushDown,
        TableScan, expr::Alias,
    },
    physical_plan::ExecutionPlan,
    scalar::ScalarValue,
    sql::TableReference,
};

use crate::embeddings::index::query_table::fold_binary;
use crate::embeddings::udtf::append_fields;
use crate::{embedding_col, embeddings::index::VectorIndex};

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

    /// Construct [`TableScan`] for associated vector search index table for `projection` & `filters` relative to [`VectorScanTableProvider`].
    ///
    /// Ok(None), if no columns from table scan are required and no filters are needed.
    fn vector_table_scan(
        &self,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
    ) -> DataFusionResult<Option<LogicalPlan>> {
        // Filter pushdown not supported for S3 vector listVectors. If vector is not needed in projection, do not need this table.
        let need_vector_column = self.need_vector_column(projection);
        if !need_vector_column {
            return Ok(None);
        }

        let list_scan = self.index.list_table_provider();
        let list_scan_schema = list_scan.schema();
        let proj = [
            index_of_column(&list_scan_schema, S3_VECTOR_EMBEDDING_NAME),
            index_of_column(&list_scan_schema, S3_VECTOR_PRIMARY_KEY_NAME),
        ]
        .iter()
        .filter_map(|p| *p)
        .collect();

        let scan = TableScan::try_new(
            TableReference::parse_str("vector_index"),
            Arc::new(DefaultTableSource::new(list_scan)),
            Some(proj),
            vec![],
            None,
        )?;

        // Add expected column aliases.
        let primary_key = self
            .index
            .primary_fields()
            .first()
            .map_or(S3_VECTOR_PRIMARY_KEY_NAME.to_string(), |f| f.name().clone());

        let primary_key_datatype = self
            .index
            .primary_fields()
            .iter()
            .find_map(|f| {
                if *f.name() == primary_key {
                    Some(f.data_type().clone())
                } else {
                    None
                }
            })
            .unwrap_or(DataType::Utf8);

        let aliased = LogicalPlan::Projection(Projection::try_new(
            vec![
                Expr::Alias(Alias::new(
                    Expr::Column(Column::new_unqualified(S3_VECTOR_EMBEDDING_NAME)),
                    Some(TableReference::parse_str("vector_index")),
                    embedding_col!(self.index.embedded_column()),
                )),
                Expr::Alias(Alias::new(
                    Expr::Cast(Cast::new(
                        Box::new(Expr::Column(Column::new_unqualified(
                            S3_VECTOR_PRIMARY_KEY_NAME,
                        ))),
                        primary_key_datatype,
                    )),
                    Some(TableReference::parse_str("vector_index")),
                    primary_key,
                )),
            ],
            Arc::new(LogicalPlan::TableScan(scan)),
        )?);

        Ok(Some(aliased))
    }

    /// For a projection relative to [`VectorScanTableProvider`], check if the embedding column is being requested.
    fn need_vector_column(&self, projection: Option<&Vec<usize>>) -> bool {
        let Some(proj) = projection else {
            return true; // None projection -> "SELECT *".
        };

        let Some(idx) = index_of_column(
            &self.schema(),
            embedding_col!(self.index.embedded_column()).as_str(),
        ) else {
            return false; // Technically unreachable, but by definition not needed.
        };

        proj.contains(&idx)
    }

    /// Construct the required join on expressions as per the primary key.
    fn join_on_expr(&self) -> DataFusionResult<Vec<(Expr, Expr)>> {
        let primary_key_columns = self.index.primary_fields();
        let Some(pk) = primary_key_columns.first() else {
            return Err(DataFusionError::Execution("Vector search index was successfully created without a primary key available during physical planning.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues".to_string()));
        };
        Ok(vec![(
            Expr::Column(Column::new_unqualified(pk.name().clone())),
            Expr::Column(Column::new_unqualified(pk.name().clone())),
        )])
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
                embedding_col!(self.index.embedded_column()),
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
                embedding_col!(self.index.embedded_column()),
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // If vector table isn't needed (in either filters or projection)
        let Some(vector_table_scan) = self.vector_table_scan(projection, filters)? else {
            return self
                .table_provider
                .scan(state, projection, filters, limit)
                .await;
        };

        let underlying_table_scan = self.underlying_table_scan(projection, filters)?;

        // Right Join so that all rows in the underlying table are returned.
        // Rows may not have associated vectors periodically due to indexing delays.
        let join = LogicalPlan::Join(Join {
            left: Arc::new(vector_table_scan),
            right: Arc::new(LogicalPlan::TableScan(underlying_table_scan)),
            join_type: JoinType::Right,
            join_constraint: JoinConstraint::On,
            on: self.join_on_expr()?,
            filter: fold_binary(filters, Operator::And),
            schema: self.qualified_schema(projection),
            null_equals_null: false,
        });

        let output_proj = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(join),
            self.qualified_schema(projection),
        ));

        let limit = LogicalPlan::Limit(Limit {
            input: Arc::new(output_proj),
            fetch: Some(Box::new(Expr::Literal(ScalarValue::UInt64(
                limit.map(|l| l as u64),
            )))),
            skip: None,
        });

        state.create_physical_plan(&limit).await
    }
}
