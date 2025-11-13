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

use std::{any::Any, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field};
use async_trait::async_trait;
use data_components::s3_vectors::compute_query::{CachedQueryVector, ComputeQueryVector};
use data_components::s3_vectors::{
    S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME, S3VectorIdentifier, S3VectorsTable,
    list_provider::S3VectorsListTable, partition::PartitionedIndexName,
    query_provider::S3VectorsQueryTable,
};

use datafusion::common::DFSchema;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions::core::union_extract::UnionExtractFun;
use datafusion::physical_expr::create_physical_expr;
use datafusion::prelude::arrow_cast;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, binary_expr, cast, col};
use datafusion_functions_json::udfs::json_get_udf;
use futures::future::try_join_all;
use llms::embeddings::Embed;
use runtime_datafusion_index::Index;
use runtime_table_partition::insert::partition_batch;
use snafu::ResultExt;

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::s3_vectors::compute_query::EmbedQuery;
use crate::index::{SearchIndex, VectorIndex, embedding_col};
use crate::metadata::MetadataColumns;
use datafusion::{
    common::Column,
    error::DataFusionError,
    logical_expr::{LogicalPlan, Operator, expr::ScalarFunction},
    prelude::{Expr, lit},
};

mod compute_query;
mod write;

#[derive(Debug, Clone)]
pub struct S3Vector {
    pub table: S3VectorsTable,

    /// The name of the column in the associated [`TableProvider`] that produces the `data` column in [`S3VectorsTable`].
    pub embedded_column: String,

    /// The ordered fields that comprise the underlying unique `key` in [`S3VectorsTable`]
    pub primary_key: Vec<Field>,

    /// Additional columns to add as metadata to the S3 vector index from the original dataset columns.
    pub metadata_columns: MetadataColumns,

    pub compute_query: Arc<dyn Embed>,

    pub partition_by: Vec<Expr>,

    batch_write_rows: usize,
}

impl S3Vector {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::too_many_arguments
    )]
    #[must_use]
    pub fn new(
        table: S3VectorsTable,
        embedded_column: String,
        primary_key: Vec<Field>,
        metadata_columns: MetadataColumns,
        compute_query: Arc<dyn Embed>,
        partition_by: Vec<Expr>,
        batch_write_rows: usize,
    ) -> Self {
        Self {
            table,
            embedded_column,
            primary_key,
            metadata_columns,
            compute_query,
            partition_by,
            batch_write_rows,
        }
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.metadata_columns
    }
}

#[async_trait]
impl SearchIndex for S3Vector {
    fn search_column(&self) -> String {
        self.embedded_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_key.clone()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        match self.partition_by.first() {
            Some(partition_by) => {
                let input_dfschema = DFSchema::try_from(record.schema())?;
                let execution_props = ExecutionProps::new();
                let physical_expr =
                    create_physical_expr(partition_by, &input_dfschema, &execution_props)?;
                let partitions = partition_batch(&record, physical_expr.as_ref())?;

                for (partition_value, partition_record) in partitions.into_values() {
                    let id = self.table.current_index();
                    // change the index name to a partition name
                    let id = match &id {
                        S3VectorIdentifier::IndexArn(_) => {
                            tracing::warn!(
                                "Partitioning is not supported when index ARN is provided. Please provide the bucket and index name instead."
                            );
                            return write::write(self, &self.table, record, self.batch_write_rows)
                                .await
                                .boxed();
                        }
                        S3VectorIdentifier::Index {
                            bucket_name,
                            index_name,
                        } => {
                            let partitioned_index_name = PartitionedIndexName::new(
                                index_name,
                                &self.embedded_column,
                                &self.partition_by,
                                &partition_value,
                            )?;
                            let index_name = partitioned_index_name.to_index_name();
                            tracing::trace!(
                                "writing {} records to index: {index_name}",
                                partition_record.num_rows(),
                            );
                            S3VectorIdentifier::Index {
                                bucket_name: bucket_name.clone(),
                                index_name,
                            }
                        }
                    };

                    let table = S3VectorsTable::try_create_new_table(
                        id,
                        Arc::clone(&self.table.client),
                        self.table.dimension,
                        self.table.columns.clone(),
                        Some(self.table.distance_metric.clone()),
                    )
                    .await?
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "S3 vector index could not be read or created".to_string(),
                        )
                    })?;

                    write::write(self, &table, partition_record, self.batch_write_rows)
                        .await
                        .boxed()?;
                }
            }
            None => {
                return write::write(self, &self.table, record, self.batch_write_rows)
                    .await
                    .boxed();
            }
        }

        Ok(record)
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(Arc::clone(&self) as Arc<dyn VectorIndex>)
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        Ok(LogicalPlanBuilder::scan(
            "tbl",
            Arc::new(DefaultTableSource::new(Arc::new(S3VectorsQueryTable::new(
                self.table.clone(),
                // TODO: should be able to internalize the CachedQueryVector within S3VectorsQueryTable.
                Arc::new(CachedQueryVector::new(
                    Arc::new(EmbedQuery(Arc::clone(&self.compute_query))),
                    query.to_string(),
                )) as Arc<dyn ComputeQueryVector>,
                query.to_string(),
                self.embedded_column.clone(),
                self.partition_by.clone(),
            )))),
            None,
        )?
        .project(
            [
                s3_vectors_primary_key_cast(&self.primary_fields()),
                metadata_columns_to_exprs(&self.metadata_columns),
                vec![
                    col(S3_VECTOR_EMBEDDING_NAME).alias(embedding_col(&self.search_column())),
                    binary_expr(lit(1.0), Operator::Minus, col("distance"))
                        .alias(SEARCH_SCORE_COLUMN_NAME),
                ],
            ]
            .concat(),
        )?
        .build()?
        .into())
    }
}

impl VectorIndex for S3Vector {
    fn dimension(&self) -> i32 {
        self.table
            .schema
            .column_with_name(S3_VECTOR_EMBEDDING_NAME)
            .map(|(_, f)| {
                match f.data_type() {
                    DataType::FixedSizeList(_, dim) => *dim,
                    _ => unreachable!("S3 vector index schema is missing a 'FixedSizeList' field named '{S3_VECTOR_EMBEDDING_NAME}'")
                }
            })
            .unwrap_or_default()
    }

    /// Use a [`S3VectorsListTable`] and then:
    ///   1. Convert the primary key to its appropriate name and data type
    ///   2. Rename [`S3_VECTOR_EMBEDDING_NAME`] appropriately
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        LogicalPlanBuilder::scan(
            "tbl",
            Arc::new(DefaultTableSource::new(Arc::new(S3VectorsListTable::new(
                self.table.clone(),
                self.search_column(),
                self.partition_by.clone(),
            )))),
            None,
        )?
        .project(
            [
                s3_vectors_primary_key_cast(&self.primary_fields()),
                metadata_columns_to_exprs(&self.metadata_columns),
                vec![col(S3_VECTOR_EMBEDDING_NAME).alias(embedding_col(&self.search_column()))],
            ]
            .concat(),
        )?
        .build()
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

#[async_trait]
impl Index for S3Vector {
    fn name(&self) -> &'static str {
        "s3_vector_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        let mut pks: Vec<_> = self
            .primary_key
            .iter()
            .map(arrow_schema::Field::name)
            .cloned()
            .collect();
        pks.push(self.embedded_column.clone());
        pks.extend(
            self.metadata_columns
                .iter()
                .filter(|c| *c.name() != embedding_col(&self.embedded_column))
                .map(|c| c.name().to_string()),
        );

        pks
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let futs = batches
            .into_iter()
            .map(|rb| async { self.write(rb).await.map_err(DataFusionError::External) });
        try_join_all(futs).await
    }
}

/// For a given data type, determine the variant within the JSON `Union(_, Sparse)` that would be populated from the associated [`datafusion_functions_json::udfs::json_get_udf`].
fn data_type_to_union_variant(dt: &DataType) -> &str {
    match dt {
        DataType::Null => "null",
        DataType::Boolean => "bool",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "int",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "float",
        DataType::BinaryView | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "str",
        DataType::LargeList(_) | DataType::List(_) => "array",
        _ => "",
    }
}

#[must_use]
pub fn s3_vectors_primary_key_cast(primary_key: &[Field]) -> Vec<Expr> {
    match primary_key {
        [f] => vec![cast(col(S3_VECTOR_PRIMARY_KEY_NAME), f.data_type().clone()).alias(f.name())],
        [] => vec![],
        cols => cols
            .iter()
            .map(|f| {
                let col_name = f.name();
                let data_type = f.data_type().clone();
                cast(
                    arrow_cast(
                        Expr::ScalarFunction(ScalarFunction {
                            func: Arc::new(ScalarUDF::new_from_impl(UnionExtractFun::default())),
                            args: vec![
                                Expr::ScalarFunction(ScalarFunction {
                                    func: json_get_udf(),
                                    args: vec![
                                        col(S3_VECTOR_PRIMARY_KEY_NAME),
                                        lit(col_name.clone()),
                                    ],
                                }),
                                lit(data_type_to_union_variant(&data_type)),
                            ],
                        }),
                        lit(data_type.to_string()),
                    ),
                    data_type,
                )
                .alias(col_name)
            })
            .collect(),
    }
}
