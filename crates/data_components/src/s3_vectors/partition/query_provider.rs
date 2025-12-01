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
    sync::{Arc, atomic::AtomicU8},
};

use crate::s3_vectors::{
    S3VectorIdentifier,
    compute_query::ComputeQueryVector,
    fetch_all_index_names,
    partition::{BelongsWith, PartitionedIndexName},
    query_provider::{S3VectorsQueryExec, S3VectorsQueryTable},
    vector_table::S3VectorsTable,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, exec_err, project_schema},
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::{ExecutionPlan, empty::EmptyExec, limit::GlobalLimitExec, union::UnionExec},
    prelude::Expr,
};
use s3_vectors::S3Vectors;

/// The JSON key within a `QueryVector` response that contains the distance to the query vector.
pub static S3_VECTOR_DISTANCE_NAME: &str = "distance";

/// Maximum topK results retrievable by a `QueryVector` operation. // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html>
pub static S3_VECTOR_MAX_TOPK: i32 = 30;

/// An S3 Vector index that implements [`TableProvider`] as a `QueryVector` API operation for a given query vector.
#[derive(Debug)]
pub struct S3VectorsPartitionedQueryTable {
    table: S3VectorsTable,
    compute_vector: Arc<dyn ComputeQueryVector>,
    query: String,
    column_name: String,
    partition_by: Vec<Expr>,
}

impl S3VectorsPartitionedQueryTable {
    #[must_use]
    pub fn new(
        table: S3VectorsTable,
        compute_vector: Arc<dyn ComputeQueryVector>,
        query: String,
        column_name: String,
        partition_by: Vec<Expr>,
    ) -> Self {
        Self {
            table,
            compute_vector,
            query,
            column_name,
            partition_by,
        }
    }
}

#[async_trait]
impl TableProvider for S3VectorsPartitionedQueryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut base_fields = self
            .table
            .schema
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        base_fields.push(Arc::new(Field::new(
            S3_VECTOR_DISTANCE_NAME,
            DataType::Float64,
            false,
        )));

        Arc::new(Schema::new(base_fields))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.table.constraints)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Filters can only possibly be pushed down for columns in underlying metadata (i.e. not derived columns like `S3_VECTOR_DISTANCE_NAME`).
        let columns: Vec<_> = self
            .table
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|c| self.table.is_filterable_column(c.as_str()))
            .collect();

        Ok(filters
            .iter()
            .map(|f| {
                if s3_vectors_metadata_filter::supports_filter_expr(columns.as_slice(), f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
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
        let query_vector = self
            .compute_vector
            .compute_vector(self.query.as_str())
            .await
            .map_err(DataFusionError::External)?;

        let current_index = self.table.current_index();
        let (_, bucket_name, index_name) = current_index.index_identifier_variables();

        let all_index_names = fetch_all_index_names(
            &self.table.client,
            bucket_name.as_deref(),
            index_name.as_deref(),
        )
        .await?;

        if self.partition_by.is_empty() {
            let limit_i32: i32 = match limit {
                Some(l) => {
                    // Safe conversion: check against i32::MAX first, then compare with limit
                    let l_i32 = i32::try_from(l).unwrap_or(i32::MAX);
                    if l_i32 > S3_VECTOR_MAX_TOPK {
                        tracing::warn!(
                            "S3VectorsPartitionedQueryTable: limit {l} exceeds maximum of {S3_VECTOR_MAX_TOPK}, truncating."
                        );
                        S3_VECTOR_MAX_TOPK
                    } else {
                        l_i32
                    }
                }
                None => S3_VECTOR_MAX_TOPK,
            };
            // TODO: use `S3VectorsQueryTable` to offload above limit checking logic .
            return Ok(Arc::new(S3VectorsQueryExec::new(
                &self.table,
                projection,
                i64::from(limit_i32),
                query_vector.clone(),
                filters.to_vec(),
            )));
        }

        let current_index = self.table.current_index();
        let (_, bucket_name, index_name) = current_index.index_identifier_variables();
        let (Some(bucket_name), Some(index_name)) = (bucket_name, index_name) else {
            return exec_err!("No bucket name or index name for bucket query");
        };

        let all_index_names = all_index_names.unwrap_or_default();

        let index_names: Vec<_> = all_index_names
            .iter()
            .filter_map(|idx_name| {
                let Ok(partitioned_index_name) =
                    PartitionedIndexName::from_index_name(idx_name)
                else {
                    return None;
                };

                if matches!(
                    partitioned_index_name.belongs_with(
                        &index_name,
                        &self.column_name,
                        &self.partition_by
                    ),
                    BelongsWith::ThisDataset
                ) {
                    Some(idx_name.clone())
                } else {
                    tracing::debug!(
                        "S3 index {idx_name} returned but does not belong with this dataset: {index_name}",
                    );
                    None
                }
            })
            .collect();

        if index_names.is_empty() {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &self.schema(),
                projection,
            )?)));
        }

        let mut index_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for index_name in index_names {
            let index_table = S3VectorsTable {
                client: Arc::clone(&self.table.client),
                schema: self.schema(),
                constraints: self.table.constraints.clone(),
                idx: Arc::new(S3VectorIdentifier::Index {
                    bucket_name: bucket_name.to_string(),
                    index_name,
                }),
                spill_index: Arc::new(AtomicU8::new(0)),
                dimension: self.table.dimension,
                columns: self.table.columns.clone(),
                distance_metric: self.table.distance_metric.clone(),
            };

            let query_table = S3VectorsQueryTable::new(
                index_table,
                Arc::clone(&self.compute_vector),
                self.query.clone(),
                self.column_name.clone(),
                vec![],
            );

            let index_plan = query_table.scan(state, projection, filters, limit).await?;
            index_plans.push(index_plan);
        }

        let union_plan = match index_plans.len() {
            0 => {
                return Ok(Arc::new(EmptyExec::new(project_schema(
                    &self.schema(),
                    projection,
                )?)));
            }
            1 => return Ok(Arc::clone(&index_plans[0])),
            _ => Arc::new(UnionExec::new(index_plans)),
        };

        Ok(Arc::new(GlobalLimitExec::new(union_plan, 0, limit)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::s3_vectors::{
        MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
    };

    use super::*;

    use arrow::datatypes::{DataType, Field};
    use datafusion::{
        prelude::{SessionContext, col},
        scalar::ScalarValue,
    };
    use s3_vectors::{DateTime, DistanceMetric, IndexSummary, mock::MockClient};

    #[derive(Debug)]
    struct MockComputeVector {
        vector: Vec<f32>,
    }

    impl MockComputeVector {
        fn new(vector: Vec<f32>) -> Self {
            Self { vector }
        }
    }

    #[async_trait]
    impl ComputeQueryVector for MockComputeVector {
        async fn compute_vector(
            &self,
            _query: &str,
        ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(self.vector.clone())
        }
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines)]
    async fn scan_plan_with_partitioned_index_spilling() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let base_index_name = "base-index";
        let column_name = "my-col";

        let partition_by = &[col(column_name)];

        let mut indexes = vec![];
        let mut vectors_map = HashMap::new();

        // Create 2 partitions, each with spilling
        for i in 1..=2 {
            let partition_value = ScalarValue::Int32(Some(i));
            let partition_index_name = PartitionedIndexName::new(
                base_index_name,
                column_name,
                partition_by,
                &partition_value,
            )?
            .to_index_name();

            // Main partition index
            indexes.push(
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(partition_index_name.clone())
                    .build()?,
            );
            vectors_map.insert(partition_index_name.clone(), vec![]);

            // Spill indexes for this partition
            for j in 1..=2 {
                let spill_index_name = format!("{partition_index_name}.{j:02}");
                indexes.push(
                    IndexSummary::builder()
                        .vector_bucket_name(bucket_name)
                        .set_index_arn(Some("arn".to_string()))
                        .creation_time(DateTime::from_secs(1))
                        .index_name(spill_index_name.clone())
                        .build()?,
                );
                vectors_map.insert(spill_index_name, vec![]);
            }
        }

        indexes.push(
            IndexSummary::builder()
                .vector_bucket_name(bucket_name)
                .set_index_arn(Some("arn".to_string()))
                .creation_time(DateTime::from_secs(1))
                .index_name("another-index")
                .build()?,
        ); // add unrelated index

        mock_client
            .data
            .lock()
            .expect("lock")
            .indexes
            .insert(bucket_name.to_string(), indexes);

        for (index, vectors) in vectors_map {
            mock_client
                .data
                .lock()
                .expect("lock")
                .vectors
                .insert(index, vectors);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
            Field::new(
                S3_VECTOR_EMBEDDING_NAME,
                DataType::new_list(DataType::Float32, true),
                false,
            ),
            Field::new(S3_VECTOR_DISTANCE_NAME, DataType::Float64, false),
            Field::new(column_name, DataType::Int32, true),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema,
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: base_index_name.to_string(),
            }),
            spill_index: Arc::new(AtomicU8::new(0)),
            dimension: 0,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table = S3VectorsPartitionedQueryTable::new(
            s3_table,
            compute_vector,
            "test query".to_string(),
            column_name.to_string(),
            vec![col(column_name)],
        );

        let session_state = SessionContext::new().state();
        let plan = query_table
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan");

        let limit_plan = plan
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .expect("downcast");
        let union_plan = limit_plan
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .expect("downcast");

        // There should be 2 partitions, each with 3 indexes (main + 2 spills), so 2 input plans to the UnionExec
        assert_eq!(union_plan.children().len(), 2);

        Ok(())
    }
}
