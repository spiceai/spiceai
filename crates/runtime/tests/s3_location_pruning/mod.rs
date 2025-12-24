/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use app::AppBuilder;
use arrow::array::{Array, StringArray};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_datasource::metadata::MetadataColumn;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{ObjectMeta, ObjectStore, path::Path};
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};
use url::Url;

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    list_calls: AtomicUsize,
    list_delimiter_calls: AtomicUsize,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            list_calls: AtomicUsize::new(0),
            list_delimiter_calls: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.list_calls.store(0, Ordering::SeqCst);
        self.list_delimiter_calls.store(0, Ordering::SeqCst);
    }

    fn list_count(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }

    fn list_delimiter_count(&self) -> usize {
        self.list_delimiter_calls.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.list_delimiter_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn put(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<object_store::GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn get_s3_hive_dataset(name: &str, metadata_columns: Vec<MetadataColumn>) -> Dataset {
    let mut dataset = Dataset::new("s3://spiceai-public-datasets/hive_partitioned_data/", name);
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
            ("hive_partitioning_enabled".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    for column in metadata_columns {
        dataset.metadata.insert(
            column.name().to_string(),
            serde_json::Value::String("enabled".to_string()),
        );
    }
    dataset
}

#[tokio::test]
async fn s3_location_pruning_avoids_list() -> Result<(), anyhow::Error> {
    let store_url = Url::parse("s3://spiceai-public-datasets")?;
    let listing_url =
        ListingTableUrl::parse("s3://spiceai-public-datasets/hive_partitioned_data/")?;

    let runtime = Arc::new(
        Runtime::builder()
            .with_app_opt(Some(Arc::new(
                AppBuilder::new("s3_location_pruning")
                    .with_dataset(get_s3_hive_dataset(
                        "met_location_only",
                        vec![MetadataColumn::Location(None)],
                    ))
                    .build(),
            )))
            .build()
            .await,
    );

    let base_store = runtime
        .datafusion()
        .ctx
        .runtime_env()
        .object_store(&listing_url)
        .expect("base S3 object store");
    let counting_store = Arc::new(CountingObjectStore::new(base_store));
    runtime
        .datafusion()
        .ctx
        .runtime_env()
        .register_object_store(
            &store_url,
            Arc::clone(&counting_store) as Arc<dyn ObjectStore>,
        );

    Arc::clone(&runtime).load_components().await;

    counting_store.reset();

    let mut filtered = runtime
            .datafusion()
            .query_builder("SELECT * FROM met_location_only WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet'")
            .build()
            .run()
            .await?;

    let mut filtered_batches = Vec::new();
    while let Some(batch) = filtered.data.next().await.transpose()? {
        filtered_batches.push(batch);
    }

    let target_location =
        "s3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet";
    let mut rows = 0usize;
    for batch in &filtered_batches {
        let (idx, _) = batch
            .schema()
            .column_with_name("location")
            .expect("location column");
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("location string array");
        for i in 0..array.len() {
            let value = array.value(i);
            assert_eq!(value, target_location);
            rows += 1;
        }
    }
    assert!(
        rows > 0,
        "Expected filtered query to return at least one row"
    );

    assert_eq!(
        counting_store.list_count(),
        0,
        "location equality should avoid list()"
    );
    assert_eq!(
        counting_store.list_delimiter_count(),
        0,
        "location equality should avoid list_with_delimiter()"
    );

    counting_store.reset();

    let mut unfiltered = runtime
        .datafusion()
        .query_builder("SELECT * FROM met_location_only LIMIT 1")
        .build()
        .run()
        .await?;

    while unfiltered.data.next().await.transpose()?.is_some() {}

    assert!(
        counting_store.list_count() > 0 || counting_store.list_delimiter_count() > 0,
        "Queries without location predicates should list to discover files"
    );

    Ok(())
}

fn get_rustfs_hive_dataset_with_location(name: &str, endpoint: &str) -> Dataset {
    let mut dataset = Dataset::new("s3://data/hive_partitioned_data/", name);
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
            ("hive_partitioning_enabled".to_string(), "true".to_string()),
            ("s3_endpoint".to_string(), endpoint.to_string()),
            ("s3_region".to_string(), "us-west-2".to_string()),
            ("s3_key".to_string(), "rustfsadmin".to_string()),
            ("s3_secret".to_string(), "rustfsadmin".to_string()),
            ("s3_auth".to_string(), "key".to_string()),
            ("allow_http".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset.metadata.insert(
        "location".to_string(),
        serde_json::Value::String("enabled".to_string()),
    );
    dataset
}

/// Regression test for: Custom S3 endpoint configuration is preserved when querying
/// with location metadata predicates.
///
/// Previously, when a dataset was configured with a custom S3 endpoint (e.g., MinIO/rustfs)
/// and `metadata.location: enabled`, queries with location predicates would incorrectly
/// use the default AWS S3 endpoint instead of the configured custom endpoint.
///
/// This test uses a local rustfs container to verify the fix works end-to-end.
#[tokio::test]
async fn test_location_metadata_preserves_custom_s3_endpoint() -> Result<(), anyhow::Error> {
    use crate::docker::{ContainerRunnerBuilder, RunningContainer};
    use bollard::secret::HealthConfig;
    use object_store::aws::AmazonS3Builder;
    use std::time::Duration;
    use tracing::instrument;

    const ENDPOINT_PORT: u16 = 19123;
    const TEST_BUCKET: &str = "data";
    const TEST_FILE_PATH: &str = "hive_partitioned_data/year=2023/month=4/day=1/data_3.parquet";

    #[instrument]
    async fn start_rustfs_container() -> Result<RunningContainer<'static>, anyhow::Error> {
        let running_container = ContainerRunnerBuilder::new("spice_test_rustfs_location_pruning")
            .image("rustfs/rustfs:latest".to_string())
            .add_port_binding(9000, ENDPOINT_PORT)
            .command(["/data"])
            .healthcheck(HealthConfig {
                test: Some(vec![
                    "CMD-SHELL".to_string(),
                    "netstat -tulpn | grep 9000 || exit 1".to_string(),
                ]),
                interval: Some(500_000_000),  // 500ms
                timeout: Some(1_000_000_000), // 1s
                retries: Some(10),
                start_period: Some(2_000_000_000), // 2s
                start_interval: None,
            })
            .build()?
            .run(Some(Duration::from_secs(60)))
            .await?;

        // Give rustfs a moment to fully initialize
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(running_container)
    }

    async fn setup_test_data(endpoint: &str) -> Result<(), anyhow::Error> {
        use aws_sdk_s3::{
            config::{Credentials, Region},
            primitives::ByteStream,
        };

        // Create object store client for source (public S3)
        let source_store = AmazonS3Builder::new()
            .with_bucket_name("spiceai-public-datasets")
            .with_region("us-east-1")
            .with_skip_signature(true) // Anonymous access
            .build()?;

        // Create AWS SDK S3 client for destination (local rustfs) - needed for bucket creation
        let creds = Credentials::new("rustfsadmin", "rustfsadmin", None, None, "test");
        let config = aws_sdk_s3::Config::builder()
            .credentials_provider(creds)
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .behavior_version_latest()
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(config);

        // Create the bucket
        let bucket_result = s3_client.create_bucket().bucket(TEST_BUCKET).send().await;
        match bucket_result {
            Ok(_) => {}
            Err(e) => {
                let err_str = e.to_string();
                // Ignore "bucket already exists" errors
                if !err_str.contains("BucketAlreadyExists")
                    && !err_str.contains("BucketAlreadyOwnedByYou")
                {
                    return Err(anyhow::anyhow!("Failed to create bucket: {e}"));
                }
            }
        }

        // Download from public S3
        let source_path = object_store::path::Path::from(TEST_FILE_PATH);
        let data = source_store.get(&source_path).await?.bytes().await?;

        // Upload to local rustfs using AWS SDK
        s3_client
            .put_object()
            .bucket(TEST_BUCKET)
            .key(TEST_FILE_PATH)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await?;

        Ok(())
    }

    // Start the rustfs container
    let container = start_rustfs_container().await?;

    let endpoint = format!("http://127.0.0.1:{ENDPOINT_PORT}");

    // Setup test data using object_store crate
    if let Err(e) = setup_test_data(&endpoint).await {
        container.remove().await?;
        return Err(anyhow::anyhow!("Failed to setup test data: {e}"));
    }

    let runtime = Arc::new(
        Runtime::builder()
            .with_app_opt(Some(Arc::new(
                AppBuilder::new("s3_location_custom_endpoint")
                    .with_dataset(get_rustfs_hive_dataset_with_location(
                        "hive_local",
                        &endpoint,
                    ))
                    .build(),
            )))
            .build()
            .await,
    );

    // Load components with a timeout
    let cloned_rt = Arc::clone(&runtime);
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            container.remove().await?;
            return Err(anyhow::anyhow!("Timed out waiting for dataset to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    // Execute a query with a location filter - this is the scenario that was broken
    // Before the fix, this would try to use the default AWS S3 endpoint instead of
    // our custom rustfs endpoint, causing the query to fail
    let query =
        format!("SELECT * FROM hive_local WHERE location = 's3://{TEST_BUCKET}/{TEST_FILE_PATH}'");
    let query_result = runtime
        .datafusion()
        .query_builder(&query)
        .build()
        .run()
        .await;

    let result = match query_result {
        Ok(mut result) => {
            let mut row_count = 0usize;
            while let Some(batch) = result.data.next().await.transpose()? {
                row_count += batch.num_rows();
            }
            Ok(row_count)
        }
        Err(e) => Err(e),
    };

    // Clean up container
    container.remove().await?;

    // Now check the result
    let row_count = result.map_err(|e| {
        anyhow::anyhow!(
            "Query with location predicate failed. This likely means the custom S3 endpoint \
             was not used correctly: {e}"
        )
    })?;

    assert!(
        row_count > 0,
        "Expected query with location predicate to return rows from custom S3 endpoint"
    );
    println!("Query with location predicate returned {row_count} rows from custom S3 endpoint");

    Ok(())
}
