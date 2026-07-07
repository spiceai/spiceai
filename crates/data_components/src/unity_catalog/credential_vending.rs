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

//! Unity Catalog credential vending.
//!
//! Unity Catalog can vend short-lived, downscoped storage credentials for a
//! table via `POST /api/2.1/unity-catalog/temporary-table-credentials`, so
//! users don't need to provide their own S3/Azure/GCS credentials.
//!
//! [`VendedTableCredentials`] caches one table's vended credentials and
//! re-vends them before they expire. The per-cloud
//! [`object_store::CredentialProvider`] implementations wrap that cache, and
//! because `object_store` consults its credential provider on every request,
//! refreshed credentials transparently apply mid-query and for the entire
//! lifetime of a table provider.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::aws::{AmazonS3Builder, AwsCredential, resolve_bucket_region};
use object_store::azure::{AzureCredential, MicrosoftAzureBuilder};
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
use object_store::{ClientOptions, CredentialProvider, ObjectStore};
use snafu::prelude::*;
use tokio::sync::RwLock;
use url::Url;

use super::{TableOperation, TemporaryTableCredentials, UnityCatalog};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to vend credentials from Unity Catalog: {source}"))]
    VendCredentials { source: super::Error },

    #[snafu(display(
        "Unity Catalog returned no table ID for table '{table}', so its credentials cannot be vended."
    ))]
    MissingTableId { table: String },

    #[snafu(display("The storage location '{url}' is not a valid URL: {source}"))]
    InvalidStorageUrl {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "The storage location '{url}' uses scheme '{scheme}', which is not supported for Unity Catalog credential vending. Supported schemes: s3, s3a, abfss, abfs, az, azure, wasbs, wasb, gs. Provide static storage credentials for this table instead."
    ))]
    UnsupportedScheme { url: String, scheme: String },

    #[snafu(display("The storage location '{url}' is missing a bucket or container name."))]
    MissingBucket { url: String },

    #[snafu(display(
        "Failed to determine the AWS region for bucket '{bucket}'. Set the connector's 'aws_region' parameter (or the AWS_REGION environment variable), and try again. Error: {source}"
    ))]
    ResolveBucketRegion {
        bucket: String,
        source: object_store::Error,
    },

    #[snafu(display("Failed to build the object store for '{url}': {source}"))]
    BuildObjectStore {
        url: String,
        source: object_store::Error,
    },

    #[snafu(display(
        "Unity Catalog vended no {expected} credentials for the storage location '{url}'. Verify the table's storage location matches the credentials returned by the catalog."
    ))]
    MissingVendedCredential { expected: &'static str, url: String },

    #[snafu(display("Failed to create the Delta table at '{url}': {source}"))]
    CreateDeltaTable {
        url: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Refresh credentials this long before they expire.
const REFRESH_MARGIN_MS: i64 = 5 * 60 * 1000;

/// Synthetic lifetime used when the vending response carries no expiration.
const DEFAULT_TTL_MS: i64 = 15 * 60 * 1000;

#[derive(Debug, Clone)]
struct CachedCredentials {
    creds: TemporaryTableCredentials,
    fetched_at_ms: i64,
}

impl CachedCredentials {
    /// The expiration to enforce, falling back to a synthetic TTL when the
    /// vending response carried none.
    fn effective_expiration_ms(&self) -> i64 {
        if self.creds.expiration_time > 0 {
            self.creds.expiration_time
        } else {
            self.fetched_at_ms + DEFAULT_TTL_MS
        }
    }

    /// Whether the cached credentials should be re-vended at `now_ms`.
    ///
    /// Credentials are refreshed [`REFRESH_MARGIN_MS`] before expiry. If the
    /// credentials were *vended* already inside that margin (a very short
    /// TTL), they are used until they actually expire instead of re-vending
    /// on every request.
    fn should_refresh(&self, now_ms: i64) -> bool {
        let expiration_ms = self.effective_expiration_ms();
        if now_ms >= expiration_ms {
            return true;
        }
        let in_margin = now_ms >= expiration_ms - REFRESH_MARGIN_MS;
        let vended_inside_margin = self.fetched_at_ms >= expiration_ms - REFRESH_MARGIN_MS;
        in_margin && !vended_inside_margin
    }
}

/// A refresh-aware cache of one table's vended credentials.
#[derive(Debug)]
pub struct VendedTableCredentials {
    client: Arc<UnityCatalog>,
    table_id: String,
    operation: TableOperation,
    cached: RwLock<Option<CachedCredentials>>,
}

impl VendedTableCredentials {
    #[must_use]
    pub fn new(client: Arc<UnityCatalog>, table_id: String, operation: TableOperation) -> Self {
        Self {
            client,
            table_id,
            operation,
            cached: RwLock::new(None),
        }
    }

    /// Returns valid vended credentials, re-vending from Unity Catalog if the
    /// cached ones are missing or close to expiry.
    pub async fn get(&self) -> Result<TemporaryTableCredentials> {
        let now_ms = now_millis();
        {
            let cached = self.cached.read().await;
            if let Some(cached) = cached.as_ref()
                && !cached.should_refresh(now_ms)
            {
                return Ok(cached.creds.clone());
            }
        }

        // The write lock single-flights concurrent refreshes: the first caller
        // re-vends while the rest block here, then hit the re-check below.
        let mut guard = self.cached.write().await;
        let now_ms = now_millis();
        if let Some(cached) = guard.as_ref()
            && !cached.should_refresh(now_ms)
        {
            return Ok(cached.creds.clone());
        }

        tracing::debug!(table_id = %self.table_id, "Vending temporary table credentials from Unity Catalog");
        let creds = self
            .client
            .temporary_table_credentials(&self.table_id, self.operation)
            .await
            .context(VendCredentialsSnafu)?;

        let cached = CachedCredentials {
            creds: creds.clone(),
            fetched_at_ms: now_ms,
        };
        if cached.effective_expiration_ms() - now_ms < REFRESH_MARGIN_MS {
            tracing::warn!(
                table_id = %self.table_id,
                expiration_time = creds.expiration_time,
                "Unity Catalog vended credentials with a very short lifetime; they will be re-vended once they expire"
            );
        }
        *guard = Some(cached);
        Ok(creds)
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn to_object_store_error(store: &'static str, error: Error) -> object_store::Error {
    object_store::Error::Generic {
        store,
        source: Box::new(error),
    }
}

/// Vends AWS credentials for an S3 object store.
#[derive(Debug)]
pub struct VendedAwsCredentialProvider {
    credentials: Arc<VendedTableCredentials>,
    url: String,
}

#[async_trait::async_trait]
impl CredentialProvider for VendedAwsCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let creds = self
            .credentials
            .get()
            .await
            .map_err(|e| to_object_store_error("S3", e))?;
        let aws = creds.aws_temp_credentials.ok_or_else(|| {
            to_object_store_error(
                "S3",
                Error::MissingVendedCredential {
                    expected: "AWS",
                    url: self.url.clone(),
                },
            )
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: aws.access_key_id,
            secret_key: aws.secret_access_key,
            token: aws.session_token,
        }))
    }
}

/// Vends an Azure user-delegation SAS token for an Azure object store.
#[derive(Debug)]
pub struct VendedAzureCredentialProvider {
    credentials: Arc<VendedTableCredentials>,
    url: String,
}

#[async_trait::async_trait]
impl CredentialProvider for VendedAzureCredentialProvider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
        let creds = self
            .credentials
            .get()
            .await
            .map_err(|e| to_object_store_error("MicrosoftAzure", e))?;
        let sas = creds.azure_user_delegation_sas.ok_or_else(|| {
            to_object_store_error(
                "MicrosoftAzure",
                Error::MissingVendedCredential {
                    expected: "Azure SAS",
                    url: self.url.clone(),
                },
            )
        })?;
        Ok(Arc::new(AzureCredential::SASToken(parse_sas_token(
            &sas.sas_token,
        ))))
    }
}

/// Vends a GCP OAuth token for a GCS object store.
#[derive(Debug)]
pub struct VendedGcpCredentialProvider {
    credentials: Arc<VendedTableCredentials>,
    url: String,
}

#[async_trait::async_trait]
impl CredentialProvider for VendedGcpCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        let creds = self
            .credentials
            .get()
            .await
            .map_err(|e| to_object_store_error("GCS", e))?;
        let token = creds.gcp_oauth_token.ok_or_else(|| {
            to_object_store_error(
                "GCS",
                Error::MissingVendedCredential {
                    expected: "GCP",
                    url: self.url.clone(),
                },
            )
        })?;
        Ok(Arc::new(GcpCredential {
            bearer: token.oauth_token,
        }))
    }
}

/// The AWS region to use for vended S3 access: the explicit `aws_region`
/// parameter wins, then the `AWS_REGION`/`AWS_DEFAULT_REGION` environment
/// variables. `None` means the region must be resolved from the bucket.
fn configured_aws_region(aws_region: Option<String>) -> Option<String> {
    aws_region
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
}

/// Parses an Azure SAS token (with or without a leading `?`) into the
/// key/value pairs expected by [`AzureCredential::SASToken`].
fn parse_sas_token(sas: &str) -> Vec<(String, String)> {
    url::form_urlencoded::parse(sas.trim_start_matches('?').as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

/// Builds an [`ObjectStore`] for `table_url` that authenticates with vended
/// Unity Catalog credentials, refreshing them before they expire.
///
/// The vending response carries no AWS region, so for S3 the region is taken
/// from `aws_region`, then the `AWS_REGION`/`AWS_DEFAULT_REGION` environment
/// variables, then resolved from the bucket itself.
pub async fn vended_object_store(
    table_url: &Url,
    credentials: Arc<VendedTableCredentials>,
    aws_region: Option<String>,
) -> Result<Arc<dyn ObjectStore>> {
    let url = table_url.to_string();
    match table_url.scheme() {
        "s3" | "s3a" => {
            let bucket = table_url
                .host_str()
                .context(MissingBucketSnafu { url: url.clone() })?
                .to_string();
            let region = match configured_aws_region(aws_region) {
                Some(region) => region,
                None => resolve_bucket_region(&bucket, &ClientOptions::new())
                    .await
                    .context(ResolveBucketRegionSnafu {
                        bucket: bucket.clone(),
                    })?,
            };
            let store = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region)
                .with_credentials(Arc::new(VendedAwsCredentialProvider {
                    credentials,
                    url: url.clone(),
                }))
                .build()
                .context(BuildObjectStoreSnafu { url })?;
            Ok(Arc::new(store))
        }
        "abfss" | "abfs" | "az" | "azure" | "wasbs" | "wasb" => {
            let store = MicrosoftAzureBuilder::new()
                .with_url(url.clone())
                .with_credentials(Arc::new(VendedAzureCredentialProvider {
                    credentials,
                    url: url.clone(),
                }))
                .build()
                .context(BuildObjectStoreSnafu { url })?;
            Ok(Arc::new(store))
        }
        "gs" => {
            let store = GoogleCloudStorageBuilder::new()
                .with_url(url.clone())
                .with_credentials(Arc::new(VendedGcpCredentialProvider {
                    credentials,
                    url: url.clone(),
                }))
                .build()
                .context(BuildObjectStoreSnafu { url })?;
            Ok(Arc::new(store))
        }
        scheme => {
            let scheme = scheme.to_string();
            UnsupportedSchemeSnafu { url, scheme }.fail()
        }
    }
}

#[cfg(feature = "delta_lake")]
mod delta {
    use std::collections::HashMap;

    use datafusion::config::TableParquetOptions;
    use datafusion::datasource::TableProvider;
    use datafusion::sql::TableReference;
    use secrecy::{ExposeSecret, SecretString};
    use snafu::prelude::*;
    use tokio::runtime::Handle;

    use super::*;
    use crate::delta_lake::DeltaTable;
    use crate::unity_catalog::UCTable;
    use crate::unity_catalog::provider::UCTableProviderFactory;

    /// Creates Delta Lake table providers whose object stores authenticate
    /// with vended Unity Catalog credentials.
    ///
    /// Tables that cannot be vended (no table ID, unsupported storage scheme,
    /// or the catalog denies vending) fall back to the static storage
    /// credentials in `params`, with a warning.
    pub struct VendedDeltaTableFactory {
        client: Arc<UnityCatalog>,
        params: HashMap<String, SecretString>,
        io_runtime: Handle,
        table_parquet_options: TableParquetOptions,
    }

    impl VendedDeltaTableFactory {
        #[must_use]
        pub fn new(
            client: Arc<UnityCatalog>,
            params: HashMap<String, SecretString>,
            io_runtime: Handle,
        ) -> Self {
            Self {
                client,
                params,
                io_runtime,
                table_parquet_options: TableParquetOptions::default(),
            }
        }

        #[must_use]
        pub fn with_table_parquet_options(mut self, opts: TableParquetOptions) -> Self {
            self.table_parquet_options = opts;
            self
        }

        async fn vended_table_provider(
            &self,
            table: &UCTable,
            delta_path: &str,
        ) -> Result<Arc<dyn TableProvider>> {
            let aws_region = self
                .params
                .get("aws_region")
                .map(|s| s.expose_secret().to_string());

            let delta = vended_delta_table(Arc::clone(&self.client), table, delta_path, aws_region)
                .await?
                .with_table_parquet_options(self.table_parquet_options.clone());

            Ok(Arc::new(delta))
        }
    }

    /// Builds a [`DeltaTable`] for a Unity Catalog table whose object store
    /// authenticates with vended, refresh-aware credentials.
    pub async fn vended_delta_table(
        client: Arc<UnityCatalog>,
        table: &UCTable,
        delta_path: &str,
        aws_region: Option<String>,
    ) -> Result<DeltaTable> {
        let table_id = table.table_id.as_deref().context(MissingTableIdSnafu {
            table: table.full_name(),
        })?;

        let table_url = Url::parse(delta_path).context(InvalidStorageUrlSnafu {
            url: delta_path.to_string(),
        })?;

        let credentials = Arc::new(VendedTableCredentials::new(
            client,
            table_id.to_string(),
            TableOperation::Read,
        ));

        // Vend eagerly so a denial surfaces as a clear, direct error (and
        // primes the cache) before any Delta log I/O is attempted.
        credentials.get().await?;

        let store = vended_object_store(&table_url, credentials, aws_region).await?;

        DeltaTable::from_object_store(delta_path.to_string(), store).map_err(|e| {
            Error::CreateDeltaTable {
                url: delta_path.to_string(),
                source: Box::new(e),
            }
        })
    }

    #[async_trait::async_trait]
    impl UCTableProviderFactory for VendedDeltaTableFactory {
        fn table_reference(&self, table: &UCTable) -> Option<TableReference> {
            let storage_location = table.storage_location.as_deref()?;
            // Don't append a trailing slash here — `DeltaTable::from` calls
            // `ensure_folder_location` which already adds one when needed.
            Some(TableReference::bare(storage_location.to_string()))
        }

        async fn table_provider(
            &self,
            table: &UCTable,
            table_reference: TableReference,
        ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
            let delta_path = table_reference.table().to_string();
            match self.vended_table_provider(table, &delta_path).await {
                Ok(provider) => Ok(provider),
                Err(err) => {
                    tracing::warn!(
                        table = %table.full_name(),
                        "Unable to use Unity Catalog credential vending for table; falling back to configured storage credentials: {err}"
                    );
                    let delta = DeltaTable::from(delta_path, self.params.clone(), &self.io_runtime)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                        .with_table_parquet_options(self.table_parquet_options.clone());
                    Ok(Arc::new(delta))
                }
            }
        }
    }
}

#[cfg(feature = "delta_lake")]
pub use delta::{VendedDeltaTableFactory, vended_delta_table};

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cached(expiration_time: i64, fetched_at_ms: i64) -> CachedCredentials {
        CachedCredentials {
            creds: TemporaryTableCredentials {
                aws_temp_credentials: None,
                azure_user_delegation_sas: None,
                gcp_oauth_token: None,
                r2_temp_credentials: None,
                expiration_time,
                url: None,
            },
            fetched_at_ms,
        }
    }

    const MINUTE_MS: i64 = 60 * 1000;

    #[test]
    fn test_should_refresh_fresh_credentials() {
        // Vended now, expires in 60 minutes: no refresh.
        let cached = make_cached(60 * MINUTE_MS, 0);
        assert!(!cached.should_refresh(MINUTE_MS));
    }

    #[test]
    fn test_should_refresh_inside_margin() {
        // Vended now, expires in 60 minutes: refresh once within 5 minutes of expiry.
        let cached = make_cached(60 * MINUTE_MS, 0);
        assert!(cached.should_refresh(56 * MINUTE_MS));
    }

    #[test]
    fn test_should_refresh_expired() {
        let cached = make_cached(60 * MINUTE_MS, 0);
        assert!(cached.should_refresh(61 * MINUTE_MS));
    }

    #[test]
    fn test_short_ttl_credentials_used_until_expiry() {
        // Vended already inside the refresh margin (2-minute TTL): keep using
        // them until they actually expire, instead of re-vending per request.
        let cached = make_cached(2 * MINUTE_MS, 0);
        assert!(!cached.should_refresh(MINUTE_MS));
        assert!(cached.should_refresh(2 * MINUTE_MS));
    }

    #[test]
    fn test_missing_expiration_uses_synthetic_ttl() {
        let cached = make_cached(0, 10 * MINUTE_MS);
        // Synthetic TTL is 15 minutes from fetch.
        assert!(!cached.should_refresh(15 * MINUTE_MS));
        assert!(cached.should_refresh(25 * MINUTE_MS));
    }

    mod object_store_dispatch {
        use super::*;
        use crate::unity_catalog::Endpoint;

        fn dummy_credentials() -> Arc<VendedTableCredentials> {
            let client = UnityCatalog::new(Endpoint("http://127.0.0.1:9".to_string()), None, None)
                .expect("client should build");
            Arc::new(VendedTableCredentials::new(
                Arc::new(client),
                "table-uuid".to_string(),
                TableOperation::Read,
            ))
        }

        #[tokio::test]
        async fn test_s3_scheme_builds_with_explicit_region() {
            let url = Url::parse("s3://my-bucket/path/to/table").expect("valid url");
            let store =
                vended_object_store(&url, dummy_credentials(), Some("us-west-2".to_string())).await;
            assert!(store.is_ok(), "expected S3 store, got: {:?}", store.err());
        }

        #[tokio::test]
        async fn test_s3_scheme_missing_bucket() {
            let url = Url::parse("s3:///no-bucket").expect("valid url");
            let err = vended_object_store(&url, dummy_credentials(), Some("us-west-2".to_string()))
                .await
                .expect_err("expected missing bucket error");
            assert!(matches!(err, Error::MissingBucket { .. }), "got: {err}");
        }

        #[tokio::test]
        async fn test_azure_scheme_builds() {
            let url = Url::parse("abfss://container@account.dfs.core.windows.net/path")
                .expect("valid url");
            let store = vended_object_store(&url, dummy_credentials(), None).await;
            assert!(
                store.is_ok(),
                "expected Azure store, got: {:?}",
                store.err()
            );
        }

        #[tokio::test]
        async fn test_gcs_scheme_builds() {
            let url = Url::parse("gs://bucket/path").expect("valid url");
            let store = vended_object_store(&url, dummy_credentials(), None).await;
            assert!(store.is_ok(), "expected GCS store, got: {:?}", store.err());
        }

        #[tokio::test]
        async fn test_unsupported_scheme() {
            let url = Url::parse("file:///tmp/table").expect("valid url");
            let err = vended_object_store(&url, dummy_credentials(), None)
                .await
                .expect_err("expected unsupported scheme error");
            assert!(
                matches!(&err, Error::UnsupportedScheme { scheme, .. } if scheme == "file"),
                "got: {err}"
            );
            // The error must list the schemes that are actually supported.
            for scheme in [
                "s3", "s3a", "abfss", "abfs", "az", "azure", "wasbs", "wasb", "gs",
            ] {
                assert!(
                    err.to_string().contains(scheme),
                    "error message missing scheme '{scheme}': {err}"
                );
            }
        }

        #[test]
        fn test_configured_aws_region_prefers_parameter() {
            // The explicit parameter takes precedence over any environment
            // configuration.
            assert_eq!(
                configured_aws_region(Some("eu-central-1".to_string())).as_deref(),
                Some("eu-central-1")
            );
        }
    }

    mod vending {
        use wiremock::matchers::{body_partial_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use super::*;
        use crate::unity_catalog::Endpoint;

        fn vending_response(expiration_time: i64) -> serde_json::Value {
            serde_json::json!({
                "aws_temp_credentials": {
                    "access_key_id": "AKIA123",
                    "secret_access_key": "SECRET",
                    "session_token": "TOKEN"
                },
                "expiration_time": expiration_time,
                "url": "s3://bucket/path"
            })
        }

        fn make_credentials(server: &MockServer) -> VendedTableCredentials {
            let client =
                UnityCatalog::new(Endpoint(server.uri()), None, None).expect("client should build");
            VendedTableCredentials::new(
                Arc::new(client),
                "table-uuid-1".to_string(),
                TableOperation::Read,
            )
        }

        #[tokio::test]
        async fn test_concurrent_gets_vend_once() {
            let server = MockServer::start().await;
            let expiration = now_millis() + 60 * MINUTE_MS;
            Mock::given(method("POST"))
                .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
                .and(body_partial_json(serde_json::json!({
                    "table_id": "table-uuid-1",
                    "operation": "READ"
                })))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(vending_response(expiration)),
                )
                .expect(1)
                .mount(&server)
                .await;

            let credentials = Arc::new(make_credentials(&server));

            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let credentials = Arc::clone(&credentials);
                    tokio::spawn(async move { credentials.get().await })
                })
                .collect();
            for handle in handles {
                let creds = handle
                    .await
                    .expect("task should not panic")
                    .expect("vending should succeed");
                assert_eq!(creds.expiration_time, expiration);
            }
        }

        #[tokio::test]
        async fn test_expired_credentials_revend() {
            let server = MockServer::start().await;
            // First response is already expired, forcing the second get() to re-vend.
            Mock::given(method("POST"))
                .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(vending_response(now_millis() - MINUTE_MS)),
                )
                .expect(2)
                .mount(&server)
                .await;

            let credentials = make_credentials(&server);
            credentials.get().await.expect("first vend should succeed");
            credentials.get().await.expect("second vend should succeed");
        }

        #[tokio::test]
        async fn test_denied_vend_surfaces_remediation_error() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
                .respond_with(
                    ResponseTemplate::new(403)
                        .set_body_string("PERMISSION_DENIED: missing EXTERNAL USE SCHEMA"),
                )
                .mount(&server)
                .await;

            let credentials = make_credentials(&server);
            let err = credentials
                .get()
                .await
                .expect_err("403 should surface an error");
            let message = err.to_string();
            assert!(
                message.contains("EXTERNAL USE SCHEMA"),
                "error should carry remediation guidance, got: {message}"
            );
        }
    }

    #[test]
    fn test_parse_sas_token() {
        let pairs = parse_sas_token("?sv=2024-01-01&sr=c&sig=abc%2Fdef%3D");
        assert_eq!(
            pairs,
            vec![
                ("sv".to_string(), "2024-01-01".to_string()),
                ("sr".to_string(), "c".to_string()),
                ("sig".to_string(), "abc/def=".to_string()),
            ]
        );

        // Without leading '?'
        let pairs = parse_sas_token("sv=2024&sig=xyz");
        assert_eq!(
            pairs,
            vec![
                ("sv".to_string(), "2024".to_string()),
                ("sig".to_string(), "xyz".to_string()),
            ]
        );
    }
}
