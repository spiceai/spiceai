/*
Copyright 2026 The Spice.ai OSS Authors

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

use super::keys::extract_primary_keys;
use super::streaming_batch_write;
use crate::delete::DeletionSink;
use async_trait::async_trait;
use aws_sdk_dynamodb::Client as DbClient;
use aws_sdk_dynamodb::types::{DeleteRequest, WriteRequest};
use datafusion::error::DataFusionError;
use datafusion::prelude::Expr;
use std::collections::HashMap;
use std::sync::Arc;

pub struct DynamoDBDeletionSink {
    pub db_client: Arc<DbClient>,
    pub table_name: String,
    pub partition_key: String,
    pub sort_key: Option<String>,
    pub time_format: Arc<String>,
    pub filters: Vec<Expr>,
    pub parallelism: usize,
}

#[async_trait]
impl DeletionSink for DynamoDBDeletionSink {
    async fn delete_from(
        &self,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let keys = extract_primary_keys(
            &self.filters,
            &self.partition_key,
            self.sort_key.as_deref(),
            &self.time_format,
        )
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut write_requests: Vec<WriteRequest> = Vec::with_capacity(keys.len());
        for (pk_val, sk_val) in &keys {
            let mut key = HashMap::new();
            key.insert(self.partition_key.clone(), pk_val.clone());
            if let (Some(sk_name), Some(sk_val)) = (&self.sort_key, sk_val) {
                key.insert(sk_name.clone(), sk_val.clone());
            }

            let delete_request = DeleteRequest::builder()
                .set_key(Some(key))
                .build()
                .map_err(|e| {
                    Box::new(DataFusionError::Execution(format!(
                        "Failed to build DeleteRequest: {e}"
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?;
            write_requests.push(
                WriteRequest::builder()
                    .delete_request(delete_request)
                    .build(),
            );
        }

        let request_stream = futures::stream::iter(write_requests.into_iter().map(Ok));

        streaming_batch_write(
            &self.db_client,
            &self.table_name,
            Box::pin(request_stream),
            self.parallelism,
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}
