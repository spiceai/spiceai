/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Lists and aborts the multipart uploads left unfinished in an S3-compatible
//! state location, for the orphan sweep of each Cayenne dataset in it.
//!
//! `object_store` neither lists nor aborts an upload it did not start, so a
//! process that crashed mid-upload leaves parts that are never visible as an
//! object, are billed until aborted, and are kept for ever where the bucket has
//! no lifecycle rule (S3 Express One Zone directory buckets have none). The
//! sweep aborts each one once the store dates it older than its age floor.

use std::sync::Arc;

use async_trait::async_trait;
use cayenne::provider::orphan_sweep::{UnfinishedUpload, UnfinishedUploads};
use object_store::path::Path;

const STORE: &str = "S3";

/// An S3-compatible bucket's unfinished multipart uploads, through the AWS SDK.
#[derive(Clone)]
pub struct S3UnfinishedUploads {
    client: aws_sdk_s3::Client,
    bucket: Arc<str>,
}

impl std::fmt::Debug for S3UnfinishedUploads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3UnfinishedUploads")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl S3UnfinishedUploads {
    /// The uploads in `bucket`, reached through `client`. Prefixes given to
    /// [`UnfinishedUploads::list`] are object paths from the bucket's root.
    #[must_use]
    pub fn new(client: aws_sdk_s3::Client, bucket: &str) -> Self {
        Self {
            client,
            bucket: Arc::from(bucket),
        }
    }
}

fn generic(error: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE,
        source: Box::new(error),
    }
}

#[async_trait]
impl UnfinishedUploads for S3UnfinishedUploads {
    async fn list(&self, prefix: &Path) -> object_store::Result<Vec<UnfinishedUpload>> {
        // The prefix is a directory: `a/b` must not match `a/bc`.
        let prefix = if prefix.as_ref().is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let mut uploads = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        loop {
            let page = self
                .client
                .list_multipart_uploads()
                .bucket(self.bucket.as_ref())
                .prefix(&prefix)
                .set_key_marker(key_marker.take())
                .set_upload_id_marker(upload_id_marker.take())
                .send()
                .await
                .map_err(|error| generic(error.into_service_error()))?;
            for upload in page.uploads() {
                let (Some(key), Some(upload_id), Some(initiated)) =
                    (upload.key(), upload.upload_id(), upload.initiated())
                else {
                    continue;
                };
                let Some(initiated) =
                    chrono::DateTime::from_timestamp(initiated.secs(), initiated.subsec_nanos())
                else {
                    continue;
                };
                uploads.push(UnfinishedUpload {
                    location: Path::from(key),
                    upload_id: upload_id.to_string(),
                    initiated,
                });
            }
            if !page.is_truncated().unwrap_or(false) {
                return Ok(uploads);
            }
            key_marker = page.next_key_marker().map(str::to_string);
            upload_id_marker = page.next_upload_id_marker().map(str::to_string);
            if key_marker.is_none() && upload_id_marker.is_none() {
                return Ok(uploads);
            }
        }
    }

    async fn abort(&self, upload: &UnfinishedUpload) -> object_store::Result<()> {
        match self
            .client
            .abort_multipart_upload()
            .bucket(self.bucket.as_ref())
            .key(upload.location.as_ref())
            .upload_id(&upload.upload_id)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                let error = error.into_service_error();
                // Completed or aborted since it was listed: nothing left to abort.
                if error.is_no_such_upload() {
                    Ok(())
                } else {
                    Err(generic(error))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
    use cayenne::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
    use cayenne::{CayenneTableProviderBuilder, MetadataCatalog};
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt};

    use super::*;

    const VARIABLES: [&str; 4] = [
        "CAYENNE_TEST_RUSTFS_ENDPOINT",
        "CAYENNE_TEST_RUSTFS_BUCKET",
        "CAYENNE_TEST_RUSTFS_ACCESS_KEY",
        "CAYENNE_TEST_RUSTFS_SECRET_KEY",
    ];

    /// Unfinished uploads under `prefix`, as keys.
    async fn unfinished(uploads: &S3UnfinishedUploads, prefix: &Path) -> Vec<String> {
        uploads
            .list(prefix)
            .await
            .expect("list unfinished uploads")
            .into_iter()
            .map(|upload| upload.location.to_string())
            .collect()
    }

    /// An upload a crashed write left under a dataset in the location is kept
    /// while it is younger than the orphan sweep's floor, and aborted once it is
    /// older; the location's lister reaches the dataset through its catalog.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs an S3-compatible server: set CAYENNE_TEST_RUSTFS_ENDPOINT, _BUCKET, _ACCESS_KEY and _SECRET_KEY"]
    async fn the_orphan_sweep_aborts_an_unfinished_upload_once_it_is_older_than_the_floor() {
        let [endpoint, bucket, access_key, secret_key] =
            VARIABLES.map(|name| std::env::var(name).unwrap_or_else(|_| panic!("{name} is set")));
        let store: Arc<dyn ObjectStore> = Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_endpoint(&endpoint)
                .with_bucket_name(&bucket)
                .with_access_key_id(&access_key)
                .with_secret_access_key(&secret_key)
                .with_region("us-east-1")
                .with_allow_http(endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(false)
                .build()
                .expect("RustFS store"),
        );
        let client =
            aws_sdk_credential_bridge::object_store_builder::S3ObjectStoreBuilder::from_url(
                &url::Url::parse(&format!("s3://{bucket}")).expect("bucket url"),
                tokio::runtime::Handle::current(),
            )
            .expect("builder")
            .with_endpoint(&endpoint)
            .with_region("us-east-1")
            .with_credentials(&access_key, &secret_key)
            .build_sdk_client()
            .await;
        let uploads = Arc::new(S3UnfinishedUploads::new(client.clone(), &bucket));

        let root = format!(
            "cayenne-test/unfinished-uploads-{}",
            uuid::Uuid::now_v7().simple()
        );
        let catalog = ObjectNativeCatalog::open_or_create_at(
            Arc::clone(&store),
            Path::from(format!("{root}/cayenne/v1/catalog")),
            &format!("s3://{bucket}/{root}/cayenne/v1"),
        )
        .await
        .expect("open the catalog")
        .with_unfinished_uploads(Arc::clone(&uploads) as Arc<dyn UnfinishedUploads>);
        catalog.init().await.expect("init");
        let data_prefix = format!(
            "{}/{}/data",
            catalog.datasets_prefix(),
            new_dataset_folder("orders")
        );
        let data_url = format!("s3://{bucket}/{data_prefix}");
        let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
        let ctx = SessionContext::new();
        let table = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
                .with_object_store(ObjectStoreConfig {
                    url: url::Url::parse(&data_url).expect("data url"),
                    store: Arc::clone(&store),
                })
                .create(CreateTableOptions {
                    table_name: "orders".to_string(),
                    schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                    primary_key: vec![],
                    on_conflict: None,
                    base_path: data_url.clone(),
                    partition_column: None,
                    vortex_config: VortexConfig {
                        inline_max_rows: 0,
                        inline_max_bytes: 0,
                        ..VortexConfig::default()
                    },
                })
                .await
                .expect("create the table"),
        );
        ctx.register_table("orders", Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        ctx.sql("INSERT INTO orders SELECT value AS id FROM generate_series(1, 100)")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("insert");

        // A write that crashed mid-upload: one part sent, never completed.
        let snapshot = catalog
            .get_table("orders")
            .await
            .expect("table")
            .current_snapshot_id;
        let table_prefix = Path::from(format!("{data_prefix}/{}", table.table_id()));
        let key = format!(
            "{table_prefix}/{snapshot}/{}_00000.vortex",
            uuid::Uuid::now_v7().simple()
        );
        let started = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("start a multipart upload");
        let upload_id = started.upload_id().expect("an upload id").to_string();
        client
            .upload_part()
            .bucket(&bucket)
            .key(&key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![7_u8; 1024]))
            .send()
            .await
            .expect("upload a part");
        assert_eq!(unfinished(&uploads, &table_prefix).await, vec![key.clone()]);

        table.set_orphan_age_floor(Duration::from_secs(3600));
        let report = table.sweep_orphan_files_now().await.expect("sweep");
        assert_eq!(report.skipped, None, "the sweep ran: {report:?}");
        assert!(
            report.aborted_uploads.is_empty(),
            "an upload younger than the floor is kept: {report:?}"
        );
        assert_eq!(
            unfinished(&uploads, &table_prefix).await,
            vec![key.clone()],
            "the upload is still unfinished"
        );

        // The store dates objects to the second, so an upload started this
        // second may read as later than the sweep's clock object for a moment.
        table.set_orphan_age_floor(Duration::ZERO);
        let deadline = Instant::now() + Duration::from_secs(10);
        let aborted = loop {
            let report = table.sweep_orphan_files_now().await.expect("sweep");
            if !report.aborted_uploads.is_empty() {
                break report.aborted_uploads;
            }
            assert!(
                Instant::now() < deadline,
                "the upload is aborted within 10 s: {report:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        let relative = key
            .strip_prefix(&format!("{table_prefix}/"))
            .expect("key under the table");
        assert_eq!(aborted, vec![relative.to_string()]);
        assert_eq!(
            unfinished(&uploads, &table_prefix).await,
            Vec::<String>::new(),
            "ListMultipartUploads under the table's prefix is empty"
        );
        let rows = ctx
            .sql("SELECT count(*) FROM orders")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("scan");
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&rows)
                .expect("format")
                .to_string()
                .lines()
                .nth(3)
                .map(str::trim),
            Some("| 100      |"),
            "the committed rows are untouched"
        );

        drop((table, ctx, catalog));
        let leftover: Vec<Path> = store
            .list(Some(&Path::from(root.as_str())))
            .map_ok(|meta| meta.location)
            .try_collect()
            .await
            .expect("list the test's objects");
        for location in leftover {
            store.delete(&location).await.expect("delete a test object");
        }
        for upload in uploads
            .list(&Path::from(root.as_str()))
            .await
            .expect("list")
        {
            uploads.abort(&upload).await.expect("abort a test upload");
        }
    }
}
