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
//! It supports multiple authentication methods: IAM role, explicit keys, or environment.

use std::sync::Arc;

use arrow::array::{
    Array, Date32Array, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use aws_config::{BehaviorVersion, Region, SdkConfig, retry::RetryConfig};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType,
};
use test_framework::anyhow::{self, Context, Result};

use crate::commands::streaming::datasets::DatasetType;
use crate::commands::streaming::traits::StreamingSource;

/// AWS authentication method for `DynamoDB` access.
#[derive(Debug, Clone)]
pub enum AwsAuthMethod {
    /// Use IAM role authentication (from environment, metadata service, etc.)
    IamRole,
    /// Use explicit access key credentials
    Key {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
    /// Use environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
    Environment,
}

/// Configuration for AWS `DynamoDB` source.
#[derive(Debug, Clone)]
pub struct DynamoDbConfig {
    /// AWS region (e.g., "us-east-1")
    pub region: String,
    /// Authentication method
    pub auth: AwsAuthMethod,
    /// Optional custom endpoint URL (for `LocalStack`, testing, etc.)
    pub endpoint_url: Option<String>,
}

impl Default for DynamoDbConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            auth: AwsAuthMethod::IamRole,
            endpoint_url: None,
        }
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
    async fn create_client(config: &DynamoDbConfig) -> Result<Client> {
        let mut sdk_config_builder = SdkConfig::builder()
            .retry_config(RetryConfig::standard().with_max_attempts(5))
            .behavior_version(BehaviorVersion::latest())
            .region(Some(Region::new(config.region.clone())));

        // Configure endpoint URL if provided
        if let Some(ref endpoint_url) = config.endpoint_url {
            sdk_config_builder = sdk_config_builder.endpoint_url(endpoint_url.clone());
        }

        // Configure credentials based on auth method
        match &config.auth {
            AwsAuthMethod::IamRole | AwsAuthMethod::Environment => {
                // Use the default credential chain (includes environment, IAM roles, etc.)
                let default_config = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(config.region.clone()))
                    .load()
                    .await;

                if let Some(provider) = default_config.credentials_provider() {
                    sdk_config_builder = sdk_config_builder.credentials_provider(provider);
                }
            }
            AwsAuthMethod::Key {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                let credentials = Credentials::new(
                    access_key_id.clone(),
                    secret_access_key.clone(),
                    session_token.clone(),
                    None,
                    "testoperator-aws-dynamodb",
                );
                sdk_config_builder = sdk_config_builder
                    .credentials_provider(SharedCredentialsProvider::new(credentials));
            }
        }

        let sdk_config = sdk_config_builder.build();
        Ok(Client::new(&sdk_config))
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

        self.client = Some(Self::create_client(&self.config).await?);

        println!("AWS DynamoDB client initialized successfully");
        Ok(())
    }

    async fn create_table(&self, dataset: DatasetType) -> Result<()> {
        let client = self.client()?;

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

        Ok(())
    }

    #[expect(clippy::cast_precision_loss)]
    async fn insert(&self, table: &str, records: &[RecordBatch]) -> Result<()> {
        let client = self.client()?;

        let total_rows: usize = records.iter().map(RecordBatch::num_rows).sum();
        println!("Inserting {total_rows} records into DynamoDB table '{table}'");

        let mut inserted = 0;

        for batch in records {
            let schema = batch.schema();

            for row in 0..batch.num_rows() {
                let mut put_item = client.put_item().table_name(table);

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let array = batch.column(col_idx);
                    let attr_value = Self::array_to_attribute(array, row)?;
                    put_item = put_item.item(field.name(), attr_value);
                }

                put_item
                    .send()
                    .await
                    .with_context(|| format!("Failed to insert record {inserted}"))?;

                inserted += 1;

                if inserted % 1000 == 0 {
                    println!(
                        "Inserted {inserted}/{total_rows} records ({:.1}%)",
                        (f64::from(inserted) / total_rows as f64) * 100.0
                    );
                }
            }
        }

        println!("Successfully inserted {inserted} records into '{table}'");
        Ok(())
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

    #[expect(clippy::cast_precision_loss)]
    async fn delete(&self, table: &str, keys: &[RecordBatch]) -> Result<()> {
        let client = self.client()?;

        let total_rows: usize = keys.iter().map(RecordBatch::num_rows).sum();
        println!("Deleting {total_rows} records from DynamoDB table '{table}'");

        let mut deleted = 0;

        for batch in keys {
            let schema = batch.schema();

            for row in 0..batch.num_rows() {
                let mut delete_item = client.delete_item().table_name(table);

                // Add all columns as key attributes (assumes the batch contains only key columns)
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let array = batch.column(col_idx);
                    let attr_value = Self::array_to_attribute(array, row)?;
                    delete_item = delete_item.key(field.name(), attr_value);
                }

                delete_item
                    .send()
                    .await
                    .with_context(|| format!("Failed to delete record {deleted}"))?;

                deleted += 1;

                if deleted % 100 == 0 {
                    println!(
                        "Deleted {deleted}/{total_rows} records ({:.1}%)",
                        (f64::from(deleted) / total_rows as f64) * 100.0
                    );
                }
            }
        }

        println!("Successfully deleted {deleted} records from '{table}'");
        Ok(())
    }

    async fn cleanup(self: Box<Self>) -> Result<()> {
        // For AWS DynamoDB, we don't delete the tables on cleanup
        // as they may be expensive to recreate or contain other data.
        // Users should manage table lifecycle separately.
        println!("AWS DynamoDB cleanup complete (tables preserved)");
        Ok(())
    }
}
