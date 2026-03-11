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
    gather_and_limit_providers, list_provider::S3VectorsListTable,
    partition::all_indexes_in_partition, vector_table::S3VectorsTable,
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

/// A [`TableProvider`] that performs a logical `/ListVectors` over a set of physical S3 Vector index.
///
/// Physical S3 Vector indexes are partitioned by predefined [`Expr`]s.
#[derive(Debug, Clone)]
pub struct S3VectorsPartitionedListTable {
    table: S3VectorsTable,
    column_name: String,
    partition_by: Vec<Expr>,
}

impl S3VectorsPartitionedListTable {
    /// Create a new [`S3VectorsPartitionedListTable`].
    ///
    /// Expects `partition_by` to be non-empty.
    #[must_use]
    pub fn new(table: S3VectorsTable, column_name: String, partition_by: Vec<Expr>) -> Self {
        Self {
            table,
            column_name,
            partition_by,
        }
    }
}

#[async_trait]
impl TableProvider for S3VectorsPartitionedListTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.table.constraints)
    }

    /// S3 vectors `ListVectors` API operation does not support filtering.
    ///
    /// TODO: Implement parititon based filter pushdown.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
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
                .map(|t| Arc::new(S3VectorsListTable::new(t)) as Arc<dyn TableProvider>)
                .collect();

        gather_and_limit_providers(query_tables, state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::s3_vectors::{
        MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME, S3VectorIdentifier,
        partition::PartitionedIndexName,
    };

    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        logical_expr::col,
        physical_plan::{limit::GlobalLimitExec, union::UnionExec},
        prelude::SessionContext,
        scalar::ScalarValue,
    };
    use s3_vectors::{DateTime, DistanceMetric, IndexSummary, mock::MockClient};

    #[tokio::test]
    async fn scan_plan_with_partitions() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let index_name_prefix = "test-index";
        let column_name = "my-col";

        let partition_by = &[col(column_name)];

        let mut indexes = vec![];
        let mut vectors_map = HashMap::new();

        // Create 2 partitions
        for i in 0..2 {
            let partition_value = ScalarValue::Int32(Some(i));
            let index_name = PartitionedIndexName::new(
                index_name_prefix,
                column_name,
                partition_by,
                &partition_value,
            )?
            .to_index_name();
            indexes.push(
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(index_name.clone())
                    .build()?,
            );
            vectors_map.insert(index_name, vec![]);
        }

        // Add an index that shouldn't be included
        indexes.push(
            IndexSummary::builder()
                .vector_bucket_name(bucket_name)
                .set_index_arn(Some("arn".to_string()))
                .creation_time(DateTime::from_secs(1))
                .index_name("another-index")
                .build()?,
        );

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
            Field::new(column_name, DataType::Utf8, true),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema,
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: index_name_prefix.to_string(),
            }),
            dimension: 0,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let list_table = S3VectorsPartitionedListTable::new(
            s3_table,
            column_name.to_string(),
            partition_by.to_vec(),
        );

        let session_state = SessionContext::new().state();
        let plan = list_table
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan");

        // The plan should be a UnionExec
        let global_limit_plan = plan
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .expect("downcast");
        let union_plan = global_limit_plan
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .expect("downcast");

        // There should be 2 partitions, so 2 input plans to the UnionExec
        assert_eq!(union_plan.children().len(), 2);

        Ok(())
    }
}
