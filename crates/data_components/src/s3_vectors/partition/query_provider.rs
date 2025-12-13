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

use crate::s3_vectors::{
    compute_query::ComputeQueryVector, gather_and_limit_providers,
    partition::all_indexes_in_partition, query_provider::S3VectorsQueryTable,
    vector_table::S3VectorsTable,
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::Constraints,
    datasource::TableType,
    error::Result as DataFusionResult,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

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
        self.table.query_provider_schema()
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
        Ok(self.table.query_provider_supports_filters_pushdown(filters))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let query_tables =
            all_indexes_in_partition(&self.table, &self.column_name, &self.partition_by)
                .await?
                .into_iter()
                .map(|t| {
                    Arc::new(S3VectorsQueryTable::new(
                        t,
                        Arc::clone(&self.compute_vector),
                        self.query.clone(),
                    )) as Arc<dyn TableProvider>
                })
                .collect();

        gather_and_limit_providers(query_tables, state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::s3_vectors::{
        MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME, S3VectorIdentifier,
        partition::PartitionedIndexName, query_provider::S3_VECTOR_DISTANCE_NAME,
    };

    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        physical_plan::{limit::GlobalLimitExec, union::UnionExec},
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
