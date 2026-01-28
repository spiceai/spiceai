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

//! AWS `DynamoDB` Streams source implementation.
//!
//! This source connects to actual AWS `DynamoDB` (not a local Docker container).
//! It supports key-based authentication configured via environment variables.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, Date32Array, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use aws_config::{BehaviorVersion, Region, SdkConfig, retry::RetryConfig};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, PutRequest,
    ScalarAttributeType, StreamSpecification, StreamViewType, WriteRequest,
};
use futures::stream::{self, StreamExt};
use test_framework::anyhow::{self, Context, Result};
use tokio::time::sleep;

/// Maximum items per batch write request (DynamoDB limit).
const BATCH_SIZE: usize = 25;

/// Number of concurrent batch write requests.
const CONCURRENT_BATCHES: usize = 10;

use crate::commands::streaming::datasets::DatasetType;
use crate::commands::streaming::traits::StreamingSource;

/// Configuration for AWS `DynamoDB` source.
///
/// Configuration is read from environment variables:
/// - `DYNAMODB_AWS_REGION`: AWS region (required)
/// - `DYNAMODB_AWS_ACCESS_KEY_ID`: AWS access key ID (required)
/// - `DYNAMODB_AWS_SECRET_ACCESS_KEY`: AWS secret access key (required)
/// - `DYNAMODB_AWS_ENDPOINT_URL`: Custom endpoint URL (optional, for LocalStack)
#[derive(Debug, Clone)]
pub struct DynamoDbConfig {
    /// AWS region (e.g., "us-east-1")
    pub region: String,
    /// AWS access key ID
    pub access_key_id: String,
    /// AWS secret access key
    pub secret_access_key: String,
    /// Optional custom endpoint URL (for `LocalStack`, testing, etc.)
    pub endpoint_url: Option<String>,
}

impl DynamoDbConfig {
    /// Create configuration from environment variables.
    ///
    /// # Errors
    /// Returns an error if required environment variables are not set.
    pub fn from_env() -> Result<Self> {
        let region = std::env::var("DYNAMODB_AWS_REGION")
            .context("DYNAMODB_AWS_REGION environment variable is required")?;
        let access_key_id = std::env::var("DYNAMODB_AWS_ACCESS_KEY_ID")
            .context("DYNAMODB_AWS_ACCESS_KEY_ID environment variable is required")?;
        let secret_access_key = std::env::var("DYNAMODB_AWS_SECRET_ACCESS_KEY")
            .context("DYNAMODB_AWS_SECRET_ACCESS_KEY environment variable is required")?;
        let endpoint_url = std::env::var("DYNAMODB_AWS_ENDPOINT_URL").ok();

        Ok(Self {
            region,
            access_key_id,
            secret_access_key,
            endpoint_url,
        })
    }
}

/// AWS `DynamoDB` Streams source for streaming benchmarks.
///
/// Unlike the local Docker-based source, this connects to actual AWS `DynamoDB`.
pub struct DynamoDbStreamsSource {
    config: DynamoDbConfig,
    client: Option<Client>,
}

impl DynamoDbStreamsSource {
    /// Create a new AWS `DynamoDB` Streams source with the given configuration.
    #[must_use]
    pub fn new(config: DynamoDbConfig) -> Self {
        Self {
            config,
            client: None,
        }
    }

    /// Get the `DynamoDB` client.
    fn client(&self) -> Result<&Client> {
        self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("DynamoDB client not initialized - call prepare() first")
        })
    }

    /// Create a `DynamoDB` client with the configured authentication.
    fn create_client(config: &DynamoDbConfig) -> Client {
        let mut sdk_config_builder = SdkConfig::builder()
            .retry_config(RetryConfig::standard().with_max_attempts(5))
            .behavior_version(BehaviorVersion::latest())
            .region(Some(Region::new(config.region.clone())));

        // Configure endpoint URL if provided
        if let Some(ref endpoint_url) = config.endpoint_url {
            sdk_config_builder = sdk_config_builder.endpoint_url(endpoint_url.clone());
        }

        // Configure credentials from config (key-based auth only)
        let credentials = Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "testoperator-aws-dynamodb",
        );
        sdk_config_builder =
            sdk_config_builder.credentials_provider(SharedCredentialsProvider::new(credentials));

        let sdk_config = sdk_config_builder.build();
        Client::new(&sdk_config)
    }

    /// Convert an Arrow array value to a `DynamoDB` `AttributeValue`.
    fn array_to_attribute(array: &Arc<dyn Array>, row: usize) -> Result<AttributeValue> {
        match array.data_type() {
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int16Array"))?;
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int32Array"))?;
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
                Ok(AttributeValue::S(arr.value(row).to_string()))
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Date32Array"))?;
                // Store as days since epoch
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Failed to downcast to TimestampMicrosecondArray")
                    })?;
                // Store as microseconds since epoch
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            dt => Err(anyhow::anyhow!("Unsupported data type: {dt:?}")),
        }
    }

    /// Check if an error is a "resource not found" error.
    fn is_resource_not_found<E: std::fmt::Debug>(err: &aws_sdk_dynamodb::error::SdkError<E>) -> bool {
        // Check the raw response for 400 status with ResourceNotFoundException
        if let aws_sdk_dynamodb::error::SdkError::ServiceError(service_err) = err {
            let raw = service_err.raw();
            // ResourceNotFoundException returns 400 status
            if raw.status().as_u16() == 400 {
                // Check error code in the response
                let body = format!("{:?}", service_err.err());
                return body.contains("ResourceNotFoundException");
            }
        }
        // Fallback to string matching
        let err_str = format!("{err:?}");
        err_str.contains("ResourceNotFoundException") || err_str.contains("resource not found")
    }

    /// Delete a table if it exists and wait for deletion to complete.
    async fn delete_table_if_exists(client: &Client, table_name: &str) -> Result<()> {
        // Check if table exists
        match client.describe_table().table_name(table_name).send().await {
            Ok(_) => {
                println!("Table '{table_name}' exists, deleting...");
                client
                    .delete_table()
                    .table_name(table_name)
                    .send()
                    .await
                    .with_context(|| format!("Failed to delete table {table_name}"))?;

                // Wait for table to be deleted
                let timeout = Duration::from_secs(120);
                let start = std::time::Instant::now();

                loop {
                    match client.describe_table().table_name(table_name).send().await {
                        Ok(_) => {
                            if start.elapsed() > timeout {
                                return Err(anyhow::anyhow!(
                                    "Timeout waiting for table '{table_name}' to be deleted"
                                ));
                            }
                            sleep(Duration::from_secs(2)).await;
                        }
                        Err(e) => {
                            if Self::is_resource_not_found(&e) {
                                println!("Table '{table_name}' deleted successfully");
                                return Ok(());
                            }
                            // Some other error, keep waiting
                            if start.elapsed() > timeout {
                                return Err(anyhow::anyhow!(
                                    "Timeout waiting for table '{table_name}' to be deleted"
                                ));
                            }
                            sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
            }
            Err(e) => {
                if Self::is_resource_not_found(&e) {
                    // Table doesn't exist, nothing to delete
                    println!("Table '{table_name}' does not exist, skipping deletion");
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Failed to describe table {table_name}: {e}"))
                }
            }
        }
    }

    /// Wait for a table to become ACTIVE.
    async fn wait_for_table_active(client: &Client, table_name: &str) -> Result<()> {
        let timeout = Duration::from_secs(120);
        let start = std::time::Instant::now();

        loop {
            let response = client
                .describe_table()
                .table_name(table_name)
                .send()
                .await
                .with_context(|| format!("Failed to describe table {table_name}"))?;

            if let Some(table) = response.table()
                && let Some(status) = table.table_status()
            {
                if status.as_str() == "ACTIVE" {
                    println!("Table '{table_name}' is now ACTIVE");
                    return Ok(());
                }
                println!(
                    "Table '{table_name}' status: {}, waiting...",
                    status.as_str()
                );
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for table '{table_name}' to become ACTIVE"
                ));
            }

            sleep(Duration::from_secs(2)).await;
        }
    }

    /// Convert a record batch row to a HashMap of attribute values.
    fn row_to_item(batch: &RecordBatch, row: usize) -> Result<HashMap<String, AttributeValue>> {
        let schema = batch.schema();
        let mut item = HashMap::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let attr_value = Self::array_to_attribute(array, row)?;
            item.insert(field.name().clone(), attr_value);
        }

        Ok(item)
    }

    /// Perform batch writes with parallelization.
    #[expect(clippy::cast_precision_loss)]
    async fn batch_write_items(
        client: &Client,
        table: &str,
        records: &[RecordBatch],
    ) -> Result<()> {
        let total_rows: usize = records.iter().map(RecordBatch::num_rows).sum();
        println!("Inserting {total_rows} records into DynamoDB table '{table}' using batch writes");

        // Collect all items as WriteRequests
        let mut all_requests: Vec<WriteRequest> = Vec::with_capacity(total_rows);

        for batch in records {
            for row in 0..batch.num_rows() {
                let item = Self::row_to_item(batch, row)?;
                let put_request = PutRequest::builder().set_item(Some(item)).build()?;
                let write_request = WriteRequest::builder().put_request(put_request).build();
                all_requests.push(write_request);
            }
        }

        // Split into batches of BATCH_SIZE
        let batches: Vec<Vec<WriteRequest>> = all_requests
            .chunks(BATCH_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();

        let total_batches = batches.len();
        println!(
            "Split into {total_batches} batches of up to {BATCH_SIZE} items, processing {CONCURRENT_BATCHES} concurrently"
        );

        let inserted = std::sync::atomic::AtomicUsize::new(0);

        // Process batches concurrently
        let results: Vec<Result<()>> = stream::iter(batches.into_iter().enumerate())
            .map(|(batch_idx, batch)| {
                let client = client.clone();
                let table = table.to_string();
                let inserted = &inserted;
                async move {
                    let batch_len = batch.len();

                    // Retry logic for unprocessed items
                    let mut items_to_write = batch;
                    let mut retry_count = 0;
                    const MAX_RETRIES: usize = 5;

                    while !items_to_write.is_empty() && retry_count < MAX_RETRIES {
                        let mut request_items = HashMap::new();
                        request_items.insert(table.clone(), items_to_write.clone());

                        let response = client
                            .batch_write_item()
                            .set_request_items(Some(request_items))
                            .send()
                            .await
                            .with_context(|| {
                                format!("Failed to batch write items (batch {batch_idx})")
                            })?;

                        // Check for unprocessed items
                        if let Some(unprocessed) = response.unprocessed_items()
                            && let Some(remaining) = unprocessed.get(&table)
                            && !remaining.is_empty()
                        {
                            retry_count += 1;
                            let backoff = Duration::from_millis(100 * (1 << retry_count));
                            sleep(backoff).await;
                            items_to_write = remaining.clone();
                            continue;
                        }

                        // All items written successfully
                        items_to_write.clear();
                    }

                    if !items_to_write.is_empty() {
                        return Err(anyhow::anyhow!(
                            "Failed to write {} items after {MAX_RETRIES} retries",
                            items_to_write.len()
                        ));
                    }

                    let prev = inserted.fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
                    let current = prev + batch_len;

                    if current.is_multiple_of(1000) || current == total_rows {
                        println!(
                            "Inserted {current}/{total_rows} records ({:.1}%)",
                            (current as f64 / total_rows as f64) * 100.0
                        );
                    }

                    Ok(())
                }
            })
            .buffer_unordered(CONCURRENT_BATCHES)
            .collect()
            .await;

        // Check for any errors
        for result in results {
            result?;
        }

        let final_count = inserted.load(std::sync::atomic::Ordering::Relaxed);
        println!("Successfully inserted {final_count} records into '{table}'");
        Ok(())
    }

    /// Perform batch deletes with parallelization.
    #[expect(clippy::cast_precision_loss, dead_code)]
    async fn batch_delete_items(
        client: &Client,
        table: &str,
        keys: &[RecordBatch],
    ) -> Result<()> {
        use aws_sdk_dynamodb::types::DeleteRequest;

        let total_rows: usize = keys.iter().map(RecordBatch::num_rows).sum();
        println!("Deleting {total_rows} records from DynamoDB table '{table}' using batch deletes");

        // Collect all items as WriteRequests (for delete)
        let mut all_requests: Vec<WriteRequest> = Vec::with_capacity(total_rows);

        for batch in keys {
            for row in 0..batch.num_rows() {
                let item = Self::row_to_item(batch, row)?;
                let delete_request = DeleteRequest::builder().set_key(Some(item)).build()?;
                let write_request = WriteRequest::builder()
                    .delete_request(delete_request)
                    .build();
                all_requests.push(write_request);
            }
        }

        // Split into batches of BATCH_SIZE
        let batches: Vec<Vec<WriteRequest>> = all_requests
            .chunks(BATCH_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();

        let total_batches = batches.len();
        println!(
            "Split into {total_batches} batches of up to {BATCH_SIZE} items, processing {CONCURRENT_BATCHES} concurrently"
        );

        let deleted = std::sync::atomic::AtomicUsize::new(0);

        // Process batches concurrently
        let results: Vec<Result<()>> = stream::iter(batches.into_iter().enumerate())
            .map(|(batch_idx, batch)| {
                let client = client.clone();
                let table = table.to_string();
                let deleted = &deleted;
                async move {
                    let batch_len = batch.len();

                    // Retry logic for unprocessed items
                    let mut items_to_delete = batch;
                    let mut retry_count = 0;
                    const MAX_RETRIES: usize = 5;

                    while !items_to_delete.is_empty() && retry_count < MAX_RETRIES {
                        let mut request_items = HashMap::new();
                        request_items.insert(table.clone(), items_to_delete.clone());

                        let response = client
                            .batch_write_item()
                            .set_request_items(Some(request_items))
                            .send()
                            .await
                            .with_context(|| {
                                format!("Failed to batch delete items (batch {batch_idx})")
                            })?;

                        // Check for unprocessed items
                        if let Some(unprocessed) = response.unprocessed_items()
                            && let Some(remaining) = unprocessed.get(&table)
                            && !remaining.is_empty()
                        {
                            retry_count += 1;
                            let backoff = Duration::from_millis(100 * (1 << retry_count));
                            sleep(backoff).await;
                            items_to_delete = remaining.clone();
                            continue;
                        }

                        // All items deleted successfully
                        items_to_delete.clear();
                    }

                    if !items_to_delete.is_empty() {
                        return Err(anyhow::anyhow!(
                            "Failed to delete {} items after {MAX_RETRIES} retries",
                            items_to_delete.len()
                        ));
                    }

                    let prev = deleted.fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
                    let current = prev + batch_len;

                    if current.is_multiple_of(100) || current == total_rows {
                        println!(
                            "Deleted {current}/{total_rows} records ({:.1}%)",
                            (current as f64 / total_rows as f64) * 100.0
                        );
                    }

                    Ok(())
                }
            })
            .buffer_unordered(CONCURRENT_BATCHES)
            .collect()
            .await;

        // Check for any errors
        for result in results {
            result?;
        }

        let final_count = deleted.load(std::sync::atomic::Ordering::Relaxed);
        println!("Successfully deleted {final_count} records from '{table}'");
        Ok(())
    }

    /// Create a simple table with a single hash key.
    async fn create_simple_table(
        &self,
        client: &Client,
        table_name: &str,
        hash_key: &str,
    ) -> Result<()> {
        client
            .create_table()
            .table_name(table_name)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(hash_key)
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build attribute definition")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(hash_key)
                    .key_type(KeyType::Hash)
                    .build()
                    .context("Failed to build key schema")?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .stream_specification(
                StreamSpecification::builder()
                    .stream_enabled(true)
                    .stream_view_type(StreamViewType::NewAndOldImages)
                    .build()
                    .context("Failed to build stream specification")?,
            )
            .send()
            .await
            .with_context(|| format!("Failed to create {table_name} table"))?;

        println!("Created table '{table_name}' with DynamoDB Streams enabled");
        Ok(())
    }

    /// Create the lineitem table with composite key.
    async fn create_lineitem_table(&self, client: &Client) -> Result<()> {
        client
            .create_table()
            .table_name("lineitem")
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("l_orderkey")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build l_orderkey attribute definition")?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("l_linenumber")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build l_linenumber attribute definition")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("l_orderkey")
                    .key_type(KeyType::Hash)
                    .build()
                    .context("Failed to build l_orderkey key schema")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("l_linenumber")
                    .key_type(KeyType::Range)
                    .build()
                    .context("Failed to build l_linenumber key schema")?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .stream_specification(
                StreamSpecification::builder()
                    .stream_enabled(true)
                    .stream_view_type(StreamViewType::NewAndOldImages)
                    .build()
                    .context("Failed to build stream specification")?,
            )
            .send()
            .await
            .context("Failed to create lineitem table")?;

        println!("Created table 'lineitem' with DynamoDB Streams enabled");
        Ok(())
    }

    /// Create the partsupp table with composite key.
    async fn create_partsupp_table(&self, client: &Client) -> Result<()> {
        client
            .create_table()
            .table_name("partsupp")
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("ps_partkey")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build ps_partkey attribute definition")?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("ps_suppkey")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build ps_suppkey attribute definition")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("ps_partkey")
                    .key_type(KeyType::Hash)
                    .build()
                    .context("Failed to build ps_partkey key schema")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("ps_suppkey")
                    .key_type(KeyType::Range)
                    .build()
                    .context("Failed to build ps_suppkey key schema")?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .stream_specification(
                StreamSpecification::builder()
                    .stream_enabled(true)
                    .stream_view_type(StreamViewType::NewAndOldImages)
                    .build()
                    .context("Failed to build stream specification")?,
            )
            .send()
            .await
            .context("Failed to create partsupp table")?;

        println!("Created table 'partsupp' with DynamoDB Streams enabled");
        Ok(())
    }

    /// Delete a marker record with a single hash key.
    async fn delete_simple_marker(
        &self,
        client: &Client,
        table_name: &str,
        hash_key: &str,
    ) -> Result<()> {
        client
            .delete_item()
            .table_name(table_name)
            .key(hash_key, AttributeValue::N("-1".to_string()))
            .send()
            .await
            .with_context(|| format!("Failed to delete {table_name} marker record"))?;

        println!("Deleted marker record from '{table_name}'");
        Ok(())
    }

    /// Delete the lineitem marker record.
    async fn delete_lineitem_marker(&self, client: &Client) -> Result<()> {
        client
            .delete_item()
            .table_name("lineitem")
            .key("l_orderkey", AttributeValue::N("-1".to_string()))
            .key("l_linenumber", AttributeValue::N("-1".to_string()))
            .send()
            .await
            .context("Failed to delete lineitem marker record")?;

        println!("Deleted marker record from 'lineitem'");
        Ok(())
    }

    /// Delete the partsupp marker record.
    async fn delete_partsupp_marker(&self, client: &Client) -> Result<()> {
        client
            .delete_item()
            .table_name("partsupp")
            .key("ps_partkey", AttributeValue::N("-1".to_string()))
            .key("ps_suppkey", AttributeValue::N("-1".to_string()))
            .send()
            .await
            .context("Failed to delete partsupp marker record")?;

        println!("Deleted marker record from 'partsupp'");
        Ok(())
    }
}

#[async_trait::async_trait]
impl StreamingSource for DynamoDbStreamsSource {
    async fn prepare(&mut self) -> Result<()> {
        // For AWS DynamoDB, we just need to create the client
        // No container management needed
        println!(
            "Initializing AWS DynamoDB client for region {}",
            self.config.region
        );

        if let Some(ref endpoint) = self.config.endpoint_url {
            println!("Using custom endpoint: {endpoint}");
        }

        self.client = Some(Self::create_client(&self.config));

        println!("AWS DynamoDB client initialized successfully");
        Ok(())
    }

    async fn create_table(&self, dataset: DatasetType) -> Result<()> {
        let client = self.client()?;
        let table_name = dataset.table_name();

        // Delete table if it exists
        Self::delete_table_if_exists(client, table_name).await?;

        // Create the table
        match dataset {
            DatasetType::Lineitem => self.create_lineitem_table(client).await?,
            DatasetType::Orders => {
                self.create_simple_table(client, "orders", "o_orderkey")
                    .await?;
            }
            DatasetType::Customer => {
                self.create_simple_table(client, "customer", "c_custkey")
                    .await?;
            }
            DatasetType::Part => {
                self.create_simple_table(client, "part", "p_partkey")
                    .await?;
            }
            DatasetType::Supplier => {
                self.create_simple_table(client, "supplier", "s_suppkey")
                    .await?;
            }
            DatasetType::Partsupp => self.create_partsupp_table(client).await?,
            DatasetType::Nation => {
                self.create_simple_table(client, "nation", "n_nationkey")
                    .await?;
            }
            DatasetType::Region => {
                self.create_simple_table(client, "region", "r_regionkey")
                    .await?;
            }
            DatasetType::Hits => {
                self.create_simple_table(client, "hits", "WatchID").await?;
            }
        }

        // Wait for table to be ACTIVE
        Self::wait_for_table_active(client, table_name).await?;

        Ok(())
    }

    async fn insert(&self, table: &str, records: &[RecordBatch]) -> Result<()> {
        let client = self.client()?;
        Self::batch_write_items(client, table, records).await
    }

    async fn delete_marker(&self, dataset: DatasetType) -> Result<()> {
        let client = self.client()?;

        match dataset {
            DatasetType::Lineitem => self.delete_lineitem_marker(client).await?,
            DatasetType::Orders => {
                self.delete_simple_marker(client, "orders", "o_orderkey")
                    .await?;
            }
            DatasetType::Customer => {
                self.delete_simple_marker(client, "customer", "c_custkey")
                    .await?;
            }
            DatasetType::Part => {
                self.delete_simple_marker(client, "part", "p_partkey")
                    .await?;
            }
            DatasetType::Supplier => {
                self.delete_simple_marker(client, "supplier", "s_suppkey")
                    .await?;
            }
            DatasetType::Partsupp => self.delete_partsupp_marker(client).await?,
            DatasetType::Nation => {
                self.delete_simple_marker(client, "nation", "n_nationkey")
                    .await?;
            }
            DatasetType::Region => {
                self.delete_simple_marker(client, "region", "r_regionkey")
                    .await?;
            }
            DatasetType::Hits => {
                self.delete_simple_marker(client, "hits", "WatchID").await?;
            }
        }

        Ok(())
    }

    async fn update(&self, table: &str, records: &[RecordBatch]) -> Result<()> {
        // DynamoDB uses upsert semantics, so update is the same as insert
        self.insert(table, records).await
    }

    async fn delete(&self, table: &str, keys: &[RecordBatch]) -> Result<()> {
        let client = self.client()?;
        Self::batch_delete_items(client, table, keys).await
    }

    async fn cleanup(self: Box<Self>) -> Result<()> {
        // For AWS DynamoDB, we don't delete the tables on cleanup
        // as they may be expensive to recreate or contain other data.
        // Users should manage table lifecycle separately.
        println!("AWS DynamoDB cleanup complete (tables preserved)");
        Ok(())
    }
}
