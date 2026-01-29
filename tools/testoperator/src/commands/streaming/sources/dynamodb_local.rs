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

//! `DynamoDB` Streams source implementation.

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
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType,
};
use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, RemoveContainerOptions,
    StartContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::secret::{
    ContainerState, ContainerStateStatusEnum, Health, HealthConfig, HealthStatusEnum, HostConfig,
    PortBinding,
};
use futures::StreamExt;
use test_framework::anyhow::{self, Context, Result};

use spicepod::acceleration::SnapshotBehavior;
use spicepod::component::snapshot::Snapshots;
use spicepod::component::ComponentOrReference;
use spicepod::param::{ParamValue, Params};
use spicepod::spec::SpicepodDefinition;

use crate::commands::streaming::datasets::DatasetType;
use crate::commands::streaming::traits::{DynamoDBStreamingSource, SnapshotConfig, StreamingSource};

const DYNAMODB_DOCKER_IMAGE: &str = "amazon/dynamodb-local:latest";
const CONTAINER_NAME_PREFIX: &str = "testoperator-dynamodb";
const ACCESS_KEY: &str = "test";
const SECRET_KEY: &str = "test";

/// `DynamoDB` Streams source for streaming benchmarks.
pub struct DynamoDbStreamsLocalSource {
    docker: Option<Docker>,
    container_name: Option<String>,
    port: u16,
    client: Option<Client>,
    /// Optional table name prefix for isolated test runs.
    table_prefix: Option<String>,
}

impl DynamoDbStreamsLocalSource {
    /// Create a new `DynamoDB` Streams source.
    ///
    /// Configuration is read from environment variables:
    /// - `DYNAMODB_LOCAL_PORT`: Port for DynamoDB local (default: 8000)
    #[must_use]
    pub fn new() -> Self {
        let port = std::env::var("DYNAMODB_LOCAL_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8000);

        Self {
            docker: None,
            container_name: None,
            port,
            client: None,
            table_prefix: None,
        }
    }

    /// Get the `DynamoDB` client.
    fn client(&self) -> Result<&Client> {
        self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("DynamoDB client not initialized - call prepare() first")
        })
    }

    /// Get the actual table name, applying prefix if set.
    fn prefixed_table_name(&self, base_name: &str) -> String {
        match &self.table_prefix {
            Some(prefix) => format!("{prefix}_{base_name}"),
            None => base_name.to_string(),
        }
    }

    /// Create a `DynamoDB` client for the given port.
    fn create_client(port: u16) -> Client {
        let config = SdkConfig::builder()
            .endpoint_url(format!("http://localhost:{port}"))
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                ACCESS_KEY,
                SECRET_KEY,
                None,
                None,
                "testoperator",
            )))
            .retry_config(RetryConfig::standard().with_max_attempts(5))
            .behavior_version(BehaviorVersion::latest())
            .region(Some(Region::from_static("us-east-1")))
            .build();
        Client::new(&config)
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

    /// Create the lineitem table with `DynamoDB` Streams enabled and specified table name.
    async fn create_lineitem_table_named(&self, client: &Client, table_name: &str) -> Result<()> {
        client
            .create_table()
            .table_name(table_name)
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
            .with_context(|| format!("Failed to create {table_name} table"))?;

        println!("Created table '{table_name}' with DynamoDB Streams enabled");
        Ok(())
    }

    /// Delete the lineitem marker record from the specified table.
    async fn delete_lineitem_marker_named(&self, client: &Client, table_name: &str) -> Result<()> {
        client
            .delete_item()
            .table_name(table_name)
            .key("l_orderkey", AttributeValue::N("-1".to_string()))
            .key("l_linenumber", AttributeValue::N("-1".to_string()))
            .send()
            .await
            .with_context(|| format!("Failed to delete {table_name} marker record"))?;

        println!("Deleted marker record from '{table_name}'");
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

    /// Create the partsupp table with composite key and specified table name.
    async fn create_partsupp_table_named(&self, client: &Client, table_name: &str) -> Result<()> {
        client
            .create_table()
            .table_name(table_name)
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
            .with_context(|| format!("Failed to create {table_name} table"))?;

        println!("Created table '{table_name}' with DynamoDB Streams enabled");
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

    /// Delete the partsupp marker record from the specified table.
    async fn delete_partsupp_marker_named(&self, client: &Client, table_name: &str) -> Result<()> {
        client
            .delete_item()
            .table_name(table_name)
            .key("ps_partkey", AttributeValue::N("-1".to_string()))
            .key("ps_suppkey", AttributeValue::N("-1".to_string()))
            .send()
            .await
            .with_context(|| format!("Failed to delete {table_name} marker record"))?;

        println!("Deleted marker record from '{table_name}'");
        Ok(())
    }
}

impl Default for DynamoDbStreamsLocalSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StreamingSource for DynamoDbStreamsLocalSource {
    fn set_table_prefix(&mut self, prefix: String) {
        println!("Setting table prefix: {prefix}");
        self.table_prefix = Some(prefix);
    }

    fn get_table_name(&self, base_name: &str) -> String {
        self.prefixed_table_name(base_name)
    }

    async fn prepare(&mut self) -> Result<()> {
        let docker =
            Docker::connect_with_local_defaults().context("Failed to connect to Docker daemon")?;

        let name = format!("{CONTAINER_NAME_PREFIX}-{}", self.port);

        // Remove existing container if present
        if container_exists(&docker, &name).await? {
            remove_container(&docker, &name).await?;
        }

        // Pull image if needed
        pull_image_if_needed(&docker, DYNAMODB_DOCKER_IMAGE).await?;

        // Create port bindings
        let mut port_bindings = HashMap::new();
        port_bindings.insert(
            "8000/tcp".to_string(),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_string()),
                host_port: Some(self.port.to_string()),
            }]),
        );

        let host_config = Some(HostConfig {
            port_bindings: Some(port_bindings),
            ..Default::default()
        });

        let healthcheck = HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "curl -s http://localhost:8000 | grep -q 'MissingAuthenticationToken' || exit 1"
                    .to_string(),
            ]),
            interval: Some(2_000_000_000), // 2 seconds
            timeout: Some(10_000_000_000), // 10 seconds
            retries: Some(15),
            start_period: Some(10_000_000_000), // 10 seconds
            start_interval: None,
        };

        let exposed_ports: HashMap<&str, HashMap<(), ()>> =
            [("8000/tcp", HashMap::new())].into_iter().collect();

        let config = Config::<&str> {
            image: Some(DYNAMODB_DOCKER_IMAGE),
            host_config,
            healthcheck: Some(healthcheck),
            exposed_ports: Some(exposed_ports),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: name.as_str(),
            platform: None,
        };

        docker
            .create_container(Some(options), config)
            .await
            .context("Failed to create DynamoDB Local container")?;

        docker
            .start_container(&name, None::<StartContainerOptions<String>>)
            .await
            .context("Failed to start DynamoDB Local container")?;

        // Wait for container to be healthy
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(60);

        loop {
            let inspect = docker.inspect_container(&name, None).await?;

            if let Some(ContainerState {
                status: Some(ContainerStateStatusEnum::RUNNING),
                health:
                    Some(Health {
                        status: Some(HealthStatusEnum::HEALTHY),
                        ..
                    }),
                ..
            }) = inspect.state
            {
                println!("DynamoDB Local container is healthy on port {}", self.port);
                break;
            }

            if start_time.elapsed() > timeout {
                let _ = remove_container(&docker, &name).await;
                return Err(anyhow::anyhow!(
                    "DynamoDB Local container failed to become healthy within {timeout:?}"
                ));
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        self.docker = Some(docker);
        self.container_name = Some(name);
        self.client = Some(Self::create_client(self.port));

        Ok(())
    }

    async fn create_table(&self, dataset: DatasetType) -> Result<()> {
        let client = self.client()?;
        let base_name = dataset.table_name();
        let table_name = self.prefixed_table_name(base_name);

        match dataset {
            DatasetType::Lineitem => {
                self.create_lineitem_table_named(client, &table_name)
                    .await?;
            }
            DatasetType::Orders => {
                self.create_simple_table(client, &table_name, "o_orderkey")
                    .await?;
            }
            DatasetType::Customer => {
                self.create_simple_table(client, &table_name, "c_custkey")
                    .await?;
            }
            DatasetType::Part => {
                self.create_simple_table(client, &table_name, "p_partkey")
                    .await?;
            }
            DatasetType::Supplier => {
                self.create_simple_table(client, &table_name, "s_suppkey")
                    .await?;
            }
            DatasetType::Partsupp => {
                self.create_partsupp_table_named(client, &table_name)
                    .await?;
            }
            DatasetType::Nation => {
                self.create_simple_table(client, &table_name, "n_nationkey")
                    .await?;
            }
            DatasetType::Region => {
                self.create_simple_table(client, &table_name, "r_regionkey")
                    .await?;
            }
            DatasetType::Hits => {
                self.create_simple_table(client, &table_name, "WatchID")
                    .await?;
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
        let base_name = dataset.table_name();
        let table_name = self.prefixed_table_name(base_name);

        match dataset {
            DatasetType::Lineitem => {
                self.delete_lineitem_marker_named(client, &table_name)
                    .await?;
            }
            DatasetType::Orders => {
                self.delete_simple_marker(client, &table_name, "o_orderkey")
                    .await?;
            }
            DatasetType::Customer => {
                self.delete_simple_marker(client, &table_name, "c_custkey")
                    .await?;
            }
            DatasetType::Part => {
                self.delete_simple_marker(client, &table_name, "p_partkey")
                    .await?;
            }
            DatasetType::Supplier => {
                self.delete_simple_marker(client, &table_name, "s_suppkey")
                    .await?;
            }
            DatasetType::Partsupp => {
                self.delete_partsupp_marker_named(client, &table_name)
                    .await?;
            }
            DatasetType::Nation => {
                self.delete_simple_marker(client, &table_name, "n_nationkey")
                    .await?;
            }
            DatasetType::Region => {
                self.delete_simple_marker(client, &table_name, "r_regionkey")
                    .await?;
            }
            DatasetType::Hits => {
                self.delete_simple_marker(client, &table_name, "WatchID")
                    .await?;
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

    async fn cleanup(&self) -> Result<()> {
        if let (Some(docker), Some(name)) = (self.docker.as_ref(), self.container_name.as_ref()) {
            remove_container(docker, name).await?;
        }
        Ok(())
    }
}

impl DynamoDBStreamingSource for DynamoDbStreamsLocalSource {
    fn prepare_checkpoint_spicepod(
        &self,
        spicepod: SpicepodDefinition,
        run_id: &str,
        config_name: &str,
        snapshot_config: &SnapshotConfig,
    ) -> SpicepodDefinition {
        transform_spicepod(
            spicepod,
            run_id,
            config_name,
            snapshot_config,
            SnapshotBehavior::CreateOnly,
        )
    }

    fn prepare_benchmark_spicepod(
        &self,
        spicepod: SpicepodDefinition,
        run_id: &str,
        config_name: &str,
        snapshot_config: &SnapshotConfig,
    ) -> SpicepodDefinition {
        transform_spicepod(
            spicepod,
            run_id,
            config_name,
            snapshot_config,
            SnapshotBehavior::BootstrapOnly,
        )
    }
}

/// Transform a spicepod for DynamoDB streaming benchmarks.
///
/// This function:
/// 1. Renames all datasets with `{run_id}_` prefix
/// 2. Sets `acceleration.snapshots` to the specified behavior
/// 3. Configures runtime snapshots with unique location per config
fn transform_spicepod(
    mut spicepod: SpicepodDefinition,
    run_id: &str,
    config_name: &str,
    snapshot_config: &SnapshotConfig,
    snapshot_behavior: SnapshotBehavior,
) -> SpicepodDefinition {
    // 1. Rename datasets and set acceleration snapshot behavior
    for dataset in &mut spicepod.datasets {
        if let ComponentOrReference::Component(d) = dataset {
            // Prefix dataset name
            d.name = format!("{run_id}_{}", d.name);

            // Set acceleration snapshot behavior
            if let Some(ref mut accel) = d.acceleration {
                accel.snapshots = snapshot_behavior.clone();
            }
        }
    }

    // 2. Configure runtime snapshots
    let location = format!(
        "{}/{}/{}/",
        snapshot_config.location.trim_end_matches('/'),
        run_id,
        config_name
    );

    let mut params = Params::default();
    if let Some(ref key) = snapshot_config.access_key_id {
        params
            .data
            .insert("aws_access_key_id".to_string(), ParamValue::String(key.clone()));
    }
    if let Some(ref secret) = snapshot_config.secret_access_key {
        params.data.insert(
            "aws_secret_access_key".to_string(),
            ParamValue::String(secret.clone()),
        );
    }
    if let Some(ref region) = snapshot_config.region {
        params
            .data
            .insert("aws_region".to_string(), ParamValue::String(region.clone()));
    }

    spicepod.snapshots = Some(Snapshots {
        enabled: true,
        location: Some(location),
        params: if params.data.is_empty() {
            None
        } else {
            Some(params)
        },
        ..Default::default()
    });

    spicepod
}

async fn container_exists(docker: &Docker, name: &str) -> Result<bool> {
    let containers = docker
        .list_containers::<&str>(Some(ListContainersOptions {
            all: true,
            ..Default::default()
        }))
        .await?;

    for container in containers {
        if let Some(names) = container.names
            && names.iter().any(|n| n == name || n == &format!("/{name}"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn remove_container(docker: &Docker, name: &str) -> Result<()> {
    docker
        .remove_container(
            name,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await
        .context("Failed to remove container")?;
    Ok(())
}

async fn pull_image_if_needed(docker: &Docker, image: &str) -> Result<()> {
    let images = docker.list_images::<&str>(None).await?;
    for img in images {
        if img.repo_tags.iter().any(|t| t == image) {
            println!("Docker image {image} already pulled");
            return Ok(());
        }
    }

    println!("Pulling Docker image: {image}");
    let options = Some(CreateImageOptions::<&str> {
        from_image: image,
        ..Default::default()
    });

    let mut stream = docker.create_image(options, None, None);
    while let Some(event) = stream.next().await {
        if let Err(e) = event {
            return Err(anyhow::anyhow!("Failed to pull image: {e}"));
        }
    }

    println!("Successfully pulled Docker image: {image}");
    Ok(())
}
