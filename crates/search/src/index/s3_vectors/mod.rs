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

use std::sync::atomic::AtomicU8;
use std::{any::Any, sync::Arc};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow_schema::{DataType, Field};
use async_trait::async_trait;
use data_components::s3_vectors::compute_query::{CachedQueryVector, ComputeQueryVector};
use data_components::s3_vectors::partition::{
    S3VectorsPartitionedListTable, S3VectorsPartitionedQueryTable, all_indexes_in_partition,
};
use data_components::s3_vectors::query_provider::S3_VECTOR_DISTANCE_NAME;
use data_components::s3_vectors::spill::{
    all_existing_spill_tables, get_last_spill_index_for_virtual_index,
};
use data_components::s3_vectors::{
    S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME, S3VectorIdentifier, S3VectorsTable,
    list_provider::S3VectorsListTable, partition::PartitionedIndexName,
    query_provider::S3VectorsQueryTable,
};

use datafusion::catalog::TableProvider;
use datafusion::common::DFSchema;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions::core::union_extract::UnionExtractFun;
use datafusion::physical_expr::create_physical_expr;
use datafusion::prelude::arrow_cast;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, binary_expr, cast, col};
use datafusion_functions_json::udfs::json_get_udf;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt};
use llms::embeddings::Embed;
use runtime_table_partition::insert::partition_batch;
use snafu::ResultExt;
use spice_table::Index;

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::s3_vectors::compute_query::EmbedQuery;
use crate::index::write_util::extract_and_format_primary_key;
use crate::index::{MAX_CONCURRENT_INDEX_WRITES, SearchIndex, VectorIndex, embedding_col};
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

    spill_writes: bool,
}

impl S3Vector {
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
            spill_writes: false,
        }
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.metadata_columns
    }
    #[must_use]
    pub fn enable_spill_writes(mut self) -> Self {
        self.spill_writes = true;
        self
    }

    // If the index supports spill writes, retrieve the last spill index to commence writing from.
    pub async fn spill_index(
        &self,
    ) -> Result<Option<Arc<AtomicU8>>, data_components::s3_vectors::Error> {
        if !self.spill_writes {
            return Ok(None);
        }
        let (_, Some(bucket), Some(index)) = self.table.idx.index_identifier_variables() else {
            return Ok(None);
        };
        get_last_spill_index_for_virtual_index(&self.table.client, &bucket, &index)
            .await
            .map(|u| Some(Arc::new(AtomicU8::new(u))))
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
        let input_schema = record.schema();
        match self.partition_by.first() {
            Some(partition_by) => {
                let input_dfschema = DFSchema::try_from(Arc::clone(&input_schema))?;
                let execution_props = ExecutionProps::new();
                let physical_expr =
                    create_physical_expr(partition_by, &input_dfschema, &execution_props)?;
                let partitions = partition_batch(&record, physical_expr.as_ref())?;

                let mut data = vec![];
                for (partition_values, partition_record) in partitions.into_values() {
                    // S3 vector search only supports single-column partitioning
                    if partition_values.is_empty() {
                        return Err(Box::new(DataFusionError::Execution(
                            "Expected at least one partition value".to_string(),
                        )));
                    }
                    if partition_values.len() > 1 {
                        return Err(Box::new(DataFusionError::Configuration(format!(
                            "S3 vector search partitioning supports exactly one partition column, but {} values were provided",
                            partition_values.len()
                        ))));
                    }
                    let partition_value = &partition_values[0];
                    // change the index name to a partition name
                    let id = match Arc::unwrap_or_clone(Arc::clone(&self.table.idx)) {
                        S3VectorIdentifier::IndexArn(_) => {
                            tracing::warn!(
                                "Partitioning is not supported when index ARN is provided. Please provide the bucket and index name instead."
                            );
                            data.push(
                                write::write(
                                    self,
                                    &self.table,
                                    partition_record,
                                    self.batch_write_rows,
                                )
                                .await
                                .boxed()?,
                            );
                            continue;
                        }
                        S3VectorIdentifier::Index {
                            bucket_name,
                            index_name,
                        } => {
                            let partitioned_index_name = PartitionedIndexName::new(
                                &index_name,
                                &self.embedded_column,
                                &self.partition_by,
                                partition_value,
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

                    let rb = write::write(self, &table, partition_record, self.batch_write_rows)
                        .await
                        .boxed()?;
                    data.push(rb);
                }
                let schema = data
                    .first()
                    .map_or_else(|| Arc::clone(&input_schema), RecordBatch::schema);
                concat_batches(&schema, data.iter()).boxed()
            }
            None => write::write(self, &self.table, record, self.batch_write_rows)
                .await
                .boxed(),
        }
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(Arc::clone(&self) as Arc<dyn VectorIndex>)
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        // TODO: should be able to internalize the CachedQueryVector within S3VectorsQueryTable.
        let compute_vector = Arc::new(CachedQueryVector::new(
            Arc::new(EmbedQuery(Arc::clone(&self.compute_query))),
            query.to_string(),
        )) as Arc<dyn ComputeQueryVector>;
        let table: Arc<dyn TableProvider> = match (self.spill_writes, self.partition_by.len()) {
            (false, 0) => Arc::new(S3VectorsQueryTable::new(
                self.table.clone(),
                compute_vector,
                query.to_string(),
            )),
            (false, _) => Arc::new(S3VectorsPartitionedQueryTable::new(
                self.table.clone(),
                compute_vector,
                query.to_string(),
                self.embedded_column.clone(),
                self.partition_by.clone(),
            )),
            (true, _) => Arc::new(
                data_components::s3_vectors::spill::query_provider::S3VectorsSpillQueryTable::new(
                    self.table.clone(),
                    compute_vector,
                    query.to_string(),
                ),
            ),
        };
        Ok(
            LogicalPlanBuilder::scan("tbl", Arc::new(DefaultTableSource::new(table)), None)?
                .project(
                    [
                        s3_vectors_primary_key_cast(&self.primary_fields()),
                        metadata_columns_to_exprs(&self.metadata_columns),
                        vec![
                            col(S3_VECTOR_EMBEDDING_NAME)
                                .alias(embedding_col(&self.search_column())),
                            binary_expr(lit(1.0), Operator::Minus, col(S3_VECTOR_DISTANCE_NAME))
                                .alias(SEARCH_SCORE_COLUMN_NAME),
                        ],
                    ]
                    .concat(),
                )?
                .build()?
                .into(),
        )
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
        let table: Arc<dyn TableProvider> = match (self.spill_writes, self.partition_by.len()) {
            (false, 0) => Arc::new(S3VectorsListTable::new(self.table.clone())),
            (false, _) => Arc::new(S3VectorsPartitionedListTable::new(
                self.table.clone(),
                self.embedded_column.clone(),
                self.partition_by.clone(),
            )),
            (true, _) => Arc::new(
                data_components::s3_vectors::spill::list_provider::S3VectorsSpillListTable::new(
                    self.table.clone(),
                ),
            ),
        };
        LogicalPlanBuilder::scan("tbl", Arc::new(DefaultTableSource::new(table)), None)?
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
        futures::stream::iter(futs)
            .buffered(MAX_CONCURRENT_INDEX_WRITES)
            .try_collect()
            .await
    }

    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<(), DataFusionError> {
        let key_strings: Vec<String> =
            extract_and_format_primary_key(self.name(), &self.primary_key, &keys)
                .map_err(|e| DataFusionError::External(Box::new(*e)))?
                .into_iter()
                .flatten()
                .collect();

        let tables = self.delete_target_tables().await?;
        let num_tables = tables.len();

        let results = join_all(tables.into_iter().map(|table| {
            let key_strings = key_strings.clone();
            async move { table.delete_by_keys(key_strings).await }
        }))
        .await;

        let errors: Vec<String> = results
            .into_iter()
            .filter_map(Result::err)
            .map(|e| e.to_string())
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(DataFusionError::Execution(format!(
                "Failed to delete from {} of {num_tables} S3 Vectors index(es): {}",
                errors.len(),
                errors.join("; ")
            )))
        }
    }
}

impl S3Vector {
    /// Every physical S3 Vectors index a delete of `self` must reach.
    ///
    /// Broadcasts to every index that could hold a matching key rather than routing to the exact
    /// one, since:
    ///  - which spill index a key's vector landed in (see `enable_spill_writes`) depends on
    ///    write-time AWS quota state and isn't recoverable from the key alone;
    ///  - which partition index a key's vector landed in requires re-evaluating `partition_by`
    ///    against the row's original data, which a resolved delete-key batch doesn't carry.
    ///
    /// `DeleteVectors` against a key absent from a given index is a no-op, so broadcasting is
    /// safe — it costs one delete call per existing physical index rather than one overall.
    ///
    /// Mirrors the `(spill_writes, partition_by.len())` precedence used by
    /// [`Self::query_table_provider`]/[`VectorIndex::list_table_provider`]: spill-writes takes
    /// precedence over partitioning (a dataset combining both is only ever spill-routed for reads
    /// too).
    async fn delete_target_tables(&self) -> Result<Vec<S3VectorsTable>, DataFusionError> {
        if self.spill_writes {
            return all_existing_spill_tables(&self.table)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)));
        }

        if !self.partition_by.is_empty() {
            return match Arc::unwrap_or_clone(Arc::clone(&self.table.idx)) {
                S3VectorIdentifier::IndexArn(_) => {
                    tracing::warn!(
                        "Partitioning is not supported when index ARN is provided. Deleting only from the base index."
                    );
                    Ok(vec![self.table.clone()])
                }
                S3VectorIdentifier::Index { .. } => {
                    all_indexes_in_partition(&self.table, &self.embedded_column, &self.partition_by)
                        .await
                }
            };
        }

        Ok(vec![self.table.clone()])
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow_schema::Schema;
    use datafusion::scalar::ScalarValue;
    use llms::embeddings::EmbeddingInput;
    use s3_vectors::{
        CreateIndexInput, CreateVectorBucketInput, DataType as S3DataType, DistanceMetric,
        PutInputVector, PutVectorsInput, S3Vectors, mock::MockClient,
    };

    #[derive(Debug)]
    struct NoopEmbed;

    #[async_trait]
    impl Embed for NoopEmbed {
        async fn embed(&self, _input: EmbeddingInput) -> llms::embeddings::Result<Vec<Vec<f32>>> {
            Ok(vec![])
        }

        fn size(&self) -> i32 {
            3
        }
    }

    async fn create_index(client: &Arc<dyn S3Vectors + Send + Sync>, index_name: &str) {
        client
            .create_vector_bucket(
                &CreateVectorBucketInput::builder()
                    .vector_bucket_name("test-bucket")
                    .build()
                    .expect("valid input"),
            )
            .await
            .ok();
        client
            .create_index(
                &CreateIndexInput::builder()
                    .index_name(index_name)
                    .vector_bucket_name("test-bucket")
                    .data_type(S3DataType::Float32)
                    .dimension(3)
                    .distance_metric(DistanceMetric::Cosine)
                    .build()
                    .expect("valid input"),
            )
            .await
            .expect("create_index should succeed");
    }

    async fn test_s3_vector(client: Arc<dyn S3Vectors + Send + Sync>) -> S3Vector {
        create_index(&client, "virtual-index").await;
        let table = S3VectorsTable::try_create_new_table(
            S3VectorIdentifier::Index {
                bucket_name: "test-bucket".to_string(),
                index_name: "virtual-index".to_string(),
            },
            client,
            3,
            data_components::s3_vectors::MetadataColumns::none(),
            Some(DistanceMetric::Cosine),
        )
        .await
        .expect("try_create_new_table should succeed")
        .expect("index exists");

        S3Vector::new(
            table,
            "embedding".to_string(),
            vec![Field::new("id", DataType::Utf8, false)],
            MetadataColumns::none(),
            Arc::new(NoopEmbed) as Arc<dyn Embed>,
            vec![],
            100,
        )
    }

    #[tokio::test]
    async fn write_fails_when_embedding_source_column_is_missing() {
        let client = Arc::new(MockClient::new()) as Arc<dyn S3Vectors + Send + Sync>;
        let index = test_s3_vector(client).await;
        let record = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["row-1"]))],
        )
        .expect("record batch should be valid");

        let err = write::write(&index, &index.table, record, 100)
            .await
            .expect_err("missing embedding source column should fail indexing");

        assert_eq!(
            err.to_string(),
            "Cannot write to 's3_vector_index' index, data does not have column 'embedding'."
        );
    }

    fn index_names(tables: &[S3VectorsTable]) -> Vec<String> {
        tables
            .iter()
            .map(|t| {
                t.idx
                    .index_identifier_variables()
                    .2
                    .expect("index-backed identifier")
            })
            .collect()
    }

    #[tokio::test]
    async fn delete_target_tables_no_spill_no_partition_targets_just_the_base() {
        let client = Arc::new(MockClient::new()) as Arc<dyn S3Vectors + Send + Sync>;
        let index = test_s3_vector(client).await;

        let tables = index
            .delete_target_tables()
            .await
            .expect("should resolve targets");

        assert_eq!(index_names(&tables), vec!["virtual-index".to_string()]);
    }

    #[tokio::test]
    async fn delete_target_tables_spill_writes_broadcasts_to_every_spill_index() {
        let mock_client = Arc::new(MockClient::new());
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let mut index = test_s3_vector(Arc::clone(&client)).await;
        index = index.enable_spill_writes();

        create_index(&client, "virtual-index-01").await;
        create_index(&client, "virtual-index-02").await;

        let tables = index
            .delete_target_tables()
            .await
            .expect("should resolve targets");

        assert_eq!(
            index_names(&tables),
            vec![
                "virtual-index".to_string(),
                "virtual-index-01".to_string(),
                "virtual-index-02".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn delete_target_tables_partitioned_broadcasts_to_every_partition_index() {
        let client = Arc::new(MockClient::new()) as Arc<dyn S3Vectors + Send + Sync>;
        let mut index = test_s3_vector(Arc::clone(&client)).await;
        let column_name = "region";
        index.partition_by = vec![col(column_name)];

        for value in ["us", "eu"] {
            let partitioned_name = PartitionedIndexName::new(
                "virtual-index",
                &index.embedded_column,
                &index.partition_by,
                &ScalarValue::from(value),
            )
            .expect("valid partition name")
            .to_index_name();
            create_index(&client, &partitioned_name).await;
        }

        let tables = index
            .delete_target_tables()
            .await
            .expect("should resolve targets");

        assert_eq!(
            tables.len(),
            2,
            "one target per partition value written so far"
        );
    }

    #[tokio::test]
    async fn delete_target_tables_spill_writes_takes_precedence_over_partitioning() {
        // Mirrors the `(spill_writes, partition_by.len())` precedence used by
        // `query_table_provider`/`list_table_provider`: a dataset combining both is always
        // spill-routed, never partition-routed.
        let client = Arc::new(MockClient::new()) as Arc<dyn S3Vectors + Send + Sync>;
        let mut index = test_s3_vector(Arc::clone(&client)).await;
        index.partition_by = vec![col("region")];
        index = index.enable_spill_writes();

        let tables = index
            .delete_target_tables()
            .await
            .expect("should resolve targets");

        // No spill indexes exist and no partition indexes were ever created (since the
        // partitioned name is never computed on this path) — spill-routing over the unpartitioned
        // base index is the only target.
        assert_eq!(index_names(&tables), vec!["virtual-index".to_string()]);
    }

    #[tokio::test]
    async fn delete_target_tables_arn_identifier_with_partitioning_falls_back_to_base() {
        let client = Arc::new(MockClient::new()) as Arc<dyn S3Vectors + Send + Sync>;
        let mut index = test_s3_vector(Arc::clone(&client)).await;
        index.table = index.table.with_new_id(S3VectorIdentifier::IndexArn(
            "arn:aws:s3vectors:us-east-1:123:index/virtual".to_string(),
        ));
        index.partition_by = vec![col("region")];

        let tables = index
            .delete_target_tables()
            .await
            .expect("ARN + partitioning must fall back, not error");

        assert_eq!(tables.len(), 1);
    }

    async fn seed_keys(client: &Arc<dyn S3Vectors + Send + Sync>, index_name: &str, keys: &[&str]) {
        let vectors: Vec<PutInputVector> = keys
            .iter()
            .map(|k| {
                PutInputVector::builder()
                    .key(*k)
                    .build()
                    .expect("valid put input vector")
            })
            .collect();
        client
            .put_vectors(
                &PutVectorsInput::builder()
                    .index_name(index_name)
                    .vector_bucket_name("test-bucket")
                    .set_vectors(Some(vectors))
                    .build()
                    .expect("valid put vectors input"),
            )
            .await
            .expect("seed put_vectors should succeed");
    }

    fn id_key_batch(ids: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(ids.to_vec()))],
        )
        .expect("valid key batch")
    }

    #[tokio::test]
    async fn delete_by_keys_removes_only_matching_vectors() {
        let mock_client = Arc::new(MockClient::new());
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let index = test_s3_vector(Arc::clone(&client)).await;

        seed_keys(&client, "virtual-index", &["a", "b", "c"]).await;

        index
            .delete_by_keys(id_key_batch(&["b"]))
            .await
            .expect("delete should succeed");

        assert_eq!(mock_client.vector_keys("virtual-index"), vec!["a", "c"]);
    }

    #[tokio::test]
    async fn delete_by_keys_broadcasts_the_delete_to_every_spill_index() {
        let mock_client = Arc::new(MockClient::new());
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let mut index = test_s3_vector(Arc::clone(&client)).await;
        index = index.enable_spill_writes();

        create_index(&client, "virtual-index-01").await;
        seed_keys(&client, "virtual-index", &["a", "b"]).await;
        seed_keys(&client, "virtual-index-01", &["b", "c"]).await;

        index
            .delete_by_keys(id_key_batch(&["b"]))
            .await
            .expect("delete should succeed");

        // The delete broadcasts to every physical index because a resolved key does not carry
        // which spill index its vector landed in. Key "b" leaves both; the rest stay.
        assert_eq!(mock_client.vector_keys("virtual-index"), vec!["a"]);
        assert_eq!(mock_client.vector_keys("virtual-index-01"), vec!["c"]);
    }
}
