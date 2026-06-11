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

//! Dynamic URL table resolution for object store URLs (S3, ABFS, etc.)
//!
//! This module provides the ability to query directly from object store URLs in SQL:
//!
//! ## Single Files
//! ```sql
//! SELECT * FROM 's3://bucket/path/data.parquet' WHERE a = b
//! ```
//!
//! ## Directories / Prefixes
//! Query all files under a prefix (bucket or directory):
//! ```sql
//! -- All parquet files in a directory
//! SELECT * FROM 's3://bucket/data/'
//!
//! -- Entire bucket
//! SELECT * FROM 's3://my-bucket/'
//!
//! -- With glob patterns
//! SELECT * FROM 's3://bucket/data/*.parquet'
//! SELECT * FROM 's3://bucket/year=2024/month=*/data.parquet'
//! ```
//!
//! ## Supported URL Schemes
//! - S3: `s3://bucket/path/` or `s3://bucket/file.parquet`
//! - Azure Blob Storage: `abfs://container@account/path/` or `abfss://...`
//! - Google Cloud Storage: `gs://bucket/path/` or `gcs://bucket/path/`
//! - HTTP/HTTPS: `https://example.com/data.parquet` (for publicly accessible files)
//!
//! ## Authentication
//! Authentication is automatic via:
//! - AWS IAM roles / instance profiles for S3
//! - Azure Managed Identity for ABFS
//! - Google Cloud default credentials for GCS
//! - Environment variables (`AWS_ACCESS_KEY_ID`, `AZURE_STORAGE_ACCOUNT`, etc.)
//!
//! ## Partition Inference
//! Hive-style partitions are automatically inferred from paths:
//! ```sql
//! -- If data is at s3://bucket/data/year=2024/month=01/file.parquet
//! SELECT * FROM 's3://bucket/data/' WHERE year = '2024' AND month = '01'
//! ```

use std::any::Any;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion_catalog::UrlTableFactory;
use parking_lot::RwLock;
use url::Url;

/// Supported URL schemes for direct object store queries.
const SUPPORTED_SCHEMES: &[&str] = &["s3", "abfs", "abfss", "http", "https", "gs", "gcs"];

/// Azure URL schemes that require an account name.
const AZURE_SCHEMES: &[&str] = &["abfs", "abfss"];

/// Checks if a table name looks like a URL that can be resolved to an object store table.
///
/// Supports:
/// - Single files: `s3://bucket/file.parquet`
/// - Directories/prefixes: `s3://bucket/path/` (with or without trailing slash)
/// - Glob patterns: `s3://bucket/path/*.parquet`
fn is_url_like(name: &str) -> bool {
    // Parse scheme without allocating - find "://" and extract the prefix
    if let Some(pos) = name.find("://") {
        let scheme = &name[..pos];
        SUPPORTED_SCHEMES.contains(&scheme)
    } else {
        false
    }
}

/// Validates that an Azure URL contains the required account name.
///
/// Azure URLs must include the storage account in one of these formats:
/// - `abfss://container@account.dfs.core.windows.net/path/`
/// - `abfss://container/#account=myaccount` (via URL fragment)
///
/// Returns an error if the URL is missing the account name.
fn validate_azure_url(url_str: &str) -> DFResult<()> {
    let Ok(url) = Url::parse(url_str) else {
        return Ok(());
    };

    if !AZURE_SCHEMES.contains(&url.scheme()) {
        return Ok(());
    }

    // Check if account is provided via URL fragment parameter
    // Fragment format: #account=myaccount or #key1=value1&account=myaccount
    if url.fragment().is_some_and(|fragment| {
        fragment.split('&').any(|param| {
            param
                .split_once('=')
                .is_some_and(|(key, _)| key == "account")
        })
    }) {
        return Ok(());
    }

    // Check if account is in the host part of the URL
    // Format: abfss://container@account.dfs.core.windows.net/path/
    // In this format, the container is the "username" and the host contains the account
    if let Some(host) = url.host_str() {
        // If there's a username (container@host format), the host contains the account
        if !url.username().is_empty() && !host.is_empty() {
            return Ok(());
        }
    }

    Err(DataFusionError::Plan(format!(
        "Azure URL '{url_str}' is missing the storage account name. Valid formats: abfss://container@account.dfs.core.windows.net/path/ or abfss://container/#account=myaccount. Alternatively, set the AZURE_STORAGE_ACCOUNT environment variable."
    )))
}

/// Environment variable that, when set to a truthy value (`1`/`true`/`yes`/`on`),
/// disables the SSRF guard on `http`/`https` URL tables, allowing them to target
/// private, loopback, and link-local addresses. Off by default — intended only
/// for operators who knowingly query an internal HTTP object store.
const ALLOW_PRIVATE_HOSTS_ENV: &str = "SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS";

/// Whether the SSRF guard is disabled via [`ALLOW_PRIVATE_HOSTS_ENV`].
fn allow_private_hosts() -> bool {
    std::env::var(ALLOW_PRIVATE_HOSTS_ENV).is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Returns `true` if `ip` is in a range that must not be reachable through a
/// user-supplied `http`/`https` URL: loopback, private/RFC1918, link-local
/// (incl. the `169.254.169.254` cloud-metadata endpoint), unique-local,
/// CGNAT, unspecified, multicast, and other reserved ranges. Used to prevent
/// SSRF — both by the URL-table guard below and by the HTTPS connector's
/// redirect guard, so the two share one classifier and never diverge.
#[must_use]
pub fn is_internal_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_internal_ipv4(v4),
        // Unwrap IPv4-mapped/-compatible addresses (e.g. `::ffff:169.254.169.254`)
        // and re-check them as IPv4 so they can't bypass the v4 rules.
        IpAddr::V6(v6) => match v6.to_ipv4() {
            Some(v4) => is_internal_ipv4(v4),
            None => is_internal_ipv6(v6),
        },
    }
}

fn is_internal_ipv4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    // Only the four oldest, universally-stable classifiers are used as methods;
    // the rest are explicit prefix checks so behavior is obvious and portable.
    ip.is_loopback()            // 127.0.0.0/8
        || ip.is_private()      // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local()   // 169.254.0.0/16 (incl. 169.254.169.254 metadata)
        || ip.is_unspecified()  // 0.0.0.0
        || o[0] == 0                                 // 0.0.0.0/8 "this network"
        || (o[0] == 100 && (o[1] & 0xc0) == 64)      // 100.64.0.0/10 CGNAT
        || (o[0] == 198 && (o[1] & 0xfe) == 18)      // 198.18.0.0/15 benchmarking
        || (o[0] == 192 && o[1] == 0 && o[2] == 2)   // 192.0.2.0/24 TEST-NET-1
        || (o[0] == 198 && o[1] == 51 && o[2] == 100) // 198.51.100.0/24 TEST-NET-2
        || (o[0] == 203 && o[1] == 0 && o[2] == 113) // 203.0.113.0/24 TEST-NET-3
        || o[0] >= 224 // 224.0.0.0/4 multicast, 240.0.0.0/4 reserved, 255.255.255.255 broadcast
}

fn is_internal_ipv6(ip: Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    ip.is_loopback()                  // ::1
        || ip.is_unspecified()        // ::
        || (first & 0xfe00) == 0xfc00 // fc00::/7 unique local
        || (first & 0xffc0) == 0xfe80 // fe80::/10 link local
        || (first & 0xff00) == 0xff00 // ff00::/8 multicast
}

/// Returns `true` for hostnames that never legitimately point at a remote
/// endpoint and must be blocked without needing DNS resolution: `localhost`
/// (and `*.localhost`, RFC 6761) and the well-known cloud-metadata names.
/// Shared by the URL-table SSRF guard and the HTTPS connector's redirect guard
/// (the latter runs in a synchronous closure where DNS resolution isn't
/// available, so this literal-name check is its first line of defense).
#[must_use]
pub fn is_blocked_internal_hostname(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    lower == "localhost"
        || lower.ends_with(".localhost")
        || lower == "metadata.google.internal"
        || lower == "metadata.goog"
}

/// Builds the error returned when a URL table targets a blocked internal host.
fn ssrf_blocked_error(url_str: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "URL table '{url_str}' targets a private, loopback, or link-local address, which is not allowed for SSRF protection. If this runtime intentionally queries an internal HTTP endpoint, set {ALLOW_PRIVATE_HOSTS_ENV}=true."
    ))
}

/// Guards against SSRF for `http`/`https` URL tables by rejecting URLs whose
/// host is (or resolves to) an internal address. Object-store schemes (`s3`,
/// `gs`, `abfs`, ...) are not checked here because they reach a fixed cloud
/// provider endpoint rather than an arbitrary host.
///
/// Note: this resolves DNS and checks the result, then hands the URL to the
/// object-store HTTP client which resolves again — a determined attacker
/// controlling DNS could rebind between the two lookups. Blocking literal
/// internal IPs and resolved-internal hostnames still closes the common cases
/// (direct metadata-IP access, `localhost`, and names that resolve to RFC1918).
async fn validate_http_url_not_internal(url_str: &str) -> DFResult<()> {
    validate_http_url_with_policy(url_str, allow_private_hosts()).await
}

async fn validate_http_url_with_policy(url_str: &str, allow_private: bool) -> DFResult<()> {
    if allow_private {
        return Ok(());
    }

    let Ok(parsed) = Url::parse(url_str) else {
        // Not parseable here; the regular `ListingTableUrl::parse` will surface the error.
        return Ok(());
    };

    if !matches!(parsed.scheme(), "http" | "https") {
        return Ok(());
    }

    match parsed.host() {
        Some(url::Host::Ipv4(ip)) => {
            if is_internal_ip(IpAddr::V4(ip)) {
                return Err(ssrf_blocked_error(url_str));
            }
        }
        Some(url::Host::Ipv6(ip)) => {
            if is_internal_ip(IpAddr::V6(ip)) {
                return Err(ssrf_blocked_error(url_str));
            }
        }
        Some(url::Host::Domain(domain)) => {
            // RFC 6761 / cloud-metadata names that never legitimately point at a
            // remote object store.
            if is_blocked_internal_hostname(domain) {
                return Err(ssrf_blocked_error(url_str));
            }

            // Resolve the host and reject if ANY resolved address is internal.
            let lookup_host = domain.to_ascii_lowercase();
            let addrs = tokio::task::spawn_blocking(move || {
                (lookup_host.as_str(), 0u16)
                    .to_socket_addrs()
                    .map(|iter| iter.map(|s| s.ip()).collect::<Vec<IpAddr>>())
            })
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "DNS resolution task failed for URL table '{url_str}': {e}"
                ))
            })?
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to resolve host for URL table '{url_str}': {e}"
                ))
            })?;

            if addrs.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "Could not resolve host for URL table '{url_str}'"
                )));
            }
            if addrs.iter().any(|ip| is_internal_ip(*ip)) {
                return Err(ssrf_blocked_error(url_str));
            }
        }
        None => {
            return Err(DataFusionError::Plan(format!(
                "URL table '{url_str}' is missing a host"
            )));
        }
    }

    Ok(())
}

/// A factory that creates [`ListingTable`] providers from object store URLs.
///
/// This enables queries like:
/// ```sql
/// -- Single file
/// SELECT * FROM 's3://bucket/data.parquet'
///
/// -- All files in a directory/prefix
/// SELECT * FROM 's3://bucket/data/'
///
/// -- Glob pattern
/// SELECT * FROM 'abfs://container@account/path/*.parquet'
///
/// -- Partitioned data with filter pushdown
/// SELECT * FROM 's3://bucket/data/' WHERE year = '2024'
/// ```
///
/// The factory automatically:
/// - Infers file format from file extensions or content
/// - Discovers all files under a prefix/directory
/// - Infers Hive-style partitions from paths
/// - Infers schema from the files
#[derive(Debug)]
pub struct SpiceUrlTableFactory {
    /// Weak reference to the session state for schema inference.
    /// Uses `parking_lot::RwLock` because that's what `SessionContext` uses internally.
    state: RwLock<Option<Weak<RwLock<SessionState>>>>,
}

impl Default for SpiceUrlTableFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SpiceUrlTableFactory {
    /// Create a new `SpiceUrlTableFactory`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(None),
        }
    }

    /// Set the session state reference for schema inference.
    pub fn with_state(&self, state: Weak<RwLock<SessionState>>) {
        *self.state.write() = Some(state);
    }
}

#[async_trait]
impl UrlTableFactory for SpiceUrlTableFactory {
    async fn try_new(&self, url: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Only handle URLs that look like object store paths
        if !is_url_like(url) {
            return Ok(None);
        }

        // Validate Azure URLs have the required account name
        validate_azure_url(url)?;

        // Prevent SSRF: http/https URL tables must not target internal addresses.
        validate_http_url_not_internal(url).await?;

        // Parse the URL
        let table_url = ListingTableUrl::parse(url).map_err(|e| {
            DataFusionError::Plan(format!("Failed to parse URL table '{url}': {e}"))
        })?;

        // Get the session state for schema inference
        // We need to clone it because we can't hold RwLockReadGuard across await points
        let state: SessionState = {
            let weak_state = self
                .state
                .read()
                .as_ref()
                .and_then(Weak::upgrade)
                .ok_or_else(|| {
                    DataFusionError::Configuration(
                        "Session state not available for URL table creation".to_string(),
                    )
                })?;

            // parking_lot::RwLock::read() returns guard directly (not Result)
            weak_state.read().clone()
        }; // Lock is dropped here

        // Infer options and schema from the URL
        let config = ListingTableConfig::new(table_url)
            .infer_options(&state)
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to access URL table '{url}': {e}. Verify the path exists and credentials are configured."
                ))
            })?;

        // Infer partitions from the path (e.g., year=2023/month=01/)
        let config = config
            .infer_partitions_from_path(&state)
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to infer partitions for URL table '{url}': {e}"
                ))
            })?;

        // Infer schema from the files
        let config = config.infer_schema(&state).await.map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to infer schema for URL table '{url}': {e}. Verify the path contains valid data files."
            ))
        })?;

        // Create the listing table
        let table = ListingTable::try_new(config).map_err(|e| {
            DataFusionError::Plan(format!("Failed to create table for URL '{url}': {e}"))
        })?;

        tracing::debug!("Created ListingTable for URL: {url}");
        Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
    }
}

/// A schema provider wrapper that intercepts URL-like table names and creates
/// `ListingTable` providers dynamically.
#[derive(Debug)]
pub struct DynamicUrlSchemaProvider {
    /// The inner schema provider
    inner: Arc<dyn SchemaProvider>,
    /// Factory to create tables from URLs
    factory: Arc<SpiceUrlTableFactory>,
}

impl DynamicUrlSchemaProvider {
    /// Create a new `DynamicUrlSchemaProvider`.
    pub fn new(inner: Arc<dyn SchemaProvider>, factory: Arc<SpiceUrlTableFactory>) -> Self {
        Self { inner, factory }
    }
}

#[async_trait]
impl SchemaProvider for DynamicUrlSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // First check if the table exists in the inner provider
        if let Some(table) = self.inner.table(name).await? {
            return Ok(Some(table));
        }

        // If not found and name looks like a URL, try to create a table from it
        self.factory.try_new(name).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        // Check inner first, then check if it's a URL-like name
        if self.inner.table_exist(name) {
            return true;
        }
        // For URL-like names, we return true to allow the table() method to try creating it
        // This is a heuristic - actual existence will be determined when table() is called
        is_url_like(name)
    }
}

/// A catalog provider wrapper that wraps all schemas with `DynamicUrlSchemaProvider`.
#[derive(Debug)]
pub struct DynamicUrlCatalogProvider {
    /// The inner catalog provider
    inner: Arc<dyn CatalogProvider>,
    /// Factory to create tables from URLs
    factory: Arc<SpiceUrlTableFactory>,
}

impl DynamicUrlCatalogProvider {
    /// Create a new `DynamicUrlCatalogProvider`.
    pub fn new(inner: Arc<dyn CatalogProvider>, factory: Arc<SpiceUrlTableFactory>) -> Self {
        Self { inner, factory }
    }
}

impl CatalogProvider for DynamicUrlCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.inner.schema(name).map(|schema| {
            Arc::new(DynamicUrlSchemaProvider::new(
                schema,
                Arc::clone(&self.factory),
            )) as Arc<dyn SchemaProvider>
        })
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

/// A catalog list wrapper that wraps all catalogs with `DynamicUrlCatalogProvider`.
#[derive(Debug)]
pub struct DynamicUrlCatalogList {
    /// The inner catalog list
    inner: Arc<dyn CatalogProviderList>,
    /// Factory to create tables from URLs
    factory: Arc<SpiceUrlTableFactory>,
}

impl DynamicUrlCatalogList {
    /// Create a new `DynamicUrlCatalogList` that wraps the given catalog list.
    pub fn new(inner: Arc<dyn CatalogProviderList>, factory: Arc<SpiceUrlTableFactory>) -> Self {
        Self { inner, factory }
    }

    /// Get a reference to the URL table factory.
    #[must_use]
    pub fn factory(&self) -> &Arc<SpiceUrlTableFactory> {
        &self.factory
    }
}

impl CatalogProviderList for DynamicUrlCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.catalog(name).map(|catalog| {
            Arc::new(DynamicUrlCatalogProvider::new(
                catalog,
                Arc::clone(&self.factory),
            )) as Arc<dyn CatalogProvider>
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::catalog::{MemoryCatalogProvider, MemoryCatalogProviderList};

    #[test]
    fn test_is_url_like() {
        // S3 URLs
        assert!(is_url_like("s3://bucket/path/file.parquet"));
        assert!(is_url_like("s3://my-bucket/data/*.parquet"));
        assert!(is_url_like("s3://bucket/path/to/data.csv"));

        // Azure Blob Storage URLs
        assert!(is_url_like("abfs://container@account/path/file.parquet"));
        assert!(is_url_like(
            "abfss://container@account.dfs.core.windows.net/path/file.parquet"
        ));

        // HTTP/HTTPS URLs
        assert!(is_url_like("https://example.com/data.parquet"));
        assert!(is_url_like("http://localhost:8080/data.csv"));
        assert!(is_url_like(
            "https://raw.githubusercontent.com/repo/data.json"
        ));

        // Google Cloud Storage URLs
        assert!(is_url_like("gs://bucket/data.parquet"));
        assert!(is_url_like("gcs://bucket/path/to/data.parquet"));

        // Directory/prefix URLs (with trailing slash)
        assert!(is_url_like("s3://bucket/"));
        assert!(is_url_like("s3://bucket/data/"));
        assert!(is_url_like("s3://bucket/path/to/data/"));
        assert!(is_url_like("abfs://container@account/"));
        assert!(is_url_like("gs://bucket/prefix/"));

        // Directory/prefix URLs (without trailing slash - also valid)
        assert!(is_url_like("s3://bucket/data"));
        assert!(is_url_like("s3://my-bucket"));

        // Glob patterns
        assert!(is_url_like("s3://bucket/*.parquet"));
        assert!(is_url_like("s3://bucket/data/*.parquet"));
        assert!(is_url_like("s3://bucket/year=*/month=*/data.parquet"));
        assert!(is_url_like("gs://bucket/**/*.csv"));

        // Should not match regular table names
        assert!(!is_url_like("my_table"));
        assert!(!is_url_like("schema.table"));
        assert!(!is_url_like("catalog.schema.table"));
        assert!(!is_url_like("/local/path/file.parquet"));
        assert!(!is_url_like("file.parquet"));
        assert!(!is_url_like("relative/path/file.parquet"));
        assert!(!is_url_like(""));

        // Edge cases - should not match incomplete URLs
        assert!(!is_url_like("s3"));
        assert!(!is_url_like("s3:"));
        assert!(!is_url_like("s3:/"));
    }

    #[test]
    fn test_factory_creation() {
        let factory = SpiceUrlTableFactory::new();
        // Ensure factory can be created and is in expected state
        assert!(factory.state.read().is_none());
    }

    #[test]
    fn test_dynamic_url_catalog_list_wraps_catalogs() {
        let inner = Arc::new(MemoryCatalogProviderList::new());
        let factory = Arc::new(SpiceUrlTableFactory::new());
        let dynamic_list = DynamicUrlCatalogList::new(inner, Arc::clone(&factory));

        // Test that catalog_names works
        let names = dynamic_list.catalog_names();
        assert!(names.is_empty());

        // Register a catalog
        let catalog = Arc::new(MemoryCatalogProvider::new());
        dynamic_list.register_catalog("test_catalog".to_string(), catalog);

        // Verify it's registered
        let names = dynamic_list.catalog_names();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"test_catalog".to_string()));

        // Get the catalog back - it should be wrapped
        let retrieved = dynamic_list.catalog("test_catalog");
        assert!(retrieved.is_some());

        // Verify it's a DynamicUrlCatalogProvider by checking it can be downcast
        let retrieved = retrieved.expect("catalog should exist");
        assert!(retrieved.as_any().is::<DynamicUrlCatalogProvider>());
    }

    #[test]
    fn test_dynamic_url_catalog_provider_wraps_schemas() {
        use datafusion::catalog::MemorySchemaProvider;

        let inner_catalog = Arc::new(MemoryCatalogProvider::new());
        let schema = Arc::new(MemorySchemaProvider::new());
        inner_catalog
            .register_schema("test_schema", schema)
            .expect("register should succeed");

        let factory = Arc::new(SpiceUrlTableFactory::new());
        let dynamic_catalog = DynamicUrlCatalogProvider::new(inner_catalog, factory);

        // Test schema_names works
        let names = dynamic_catalog.schema_names();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"test_schema".to_string()));

        // Get schema - should be wrapped
        let retrieved = dynamic_catalog.schema("test_schema");
        assert!(retrieved.is_some());

        let retrieved = retrieved.expect("schema should exist");
        assert!(retrieved.as_any().is::<DynamicUrlSchemaProvider>());
    }

    #[tokio::test]
    async fn test_dynamic_url_schema_provider_table_exist() {
        use datafusion::catalog::MemorySchemaProvider;

        let inner_schema = Arc::new(MemorySchemaProvider::new());
        let factory = Arc::new(SpiceUrlTableFactory::new());
        let dynamic_schema = DynamicUrlSchemaProvider::new(inner_schema, factory);

        // Regular table names should not exist
        assert!(!dynamic_schema.table_exist("my_table"));
        assert!(!dynamic_schema.table_exist("other_table"));

        // Single file URLs should report as existing
        assert!(dynamic_schema.table_exist("s3://bucket/data.parquet"));
        assert!(dynamic_schema.table_exist("https://example.com/file.parquet"));
        assert!(dynamic_schema.table_exist("abfs://container@account/path/file.parquet"));

        // Directory/prefix URLs should also report as existing
        assert!(dynamic_schema.table_exist("s3://bucket/"));
        assert!(dynamic_schema.table_exist("s3://bucket/data/"));
        assert!(dynamic_schema.table_exist("gs://bucket/prefix/"));

        // Glob patterns should report as existing
        assert!(dynamic_schema.table_exist("s3://bucket/*.parquet"));
        assert!(dynamic_schema.table_exist("s3://bucket/data/**/*.csv"));
    }

    #[tokio::test]
    async fn test_factory_try_new_without_state_returns_error() {
        let factory = SpiceUrlTableFactory::new();

        // Without state set, try_new should return an error for valid URLs (including directories)
        let result = factory.try_new("s3://bucket/data.parquet").await;
        let _ = result.expect_err("should return error without state");

        let result = factory.try_new("s3://bucket/data/").await;
        let _ = result.expect_err("should return error without state");

        // Non-URL should return Ok(None)
        let result = factory.try_new("my_table").await;
        assert!(result.expect("should be Ok").is_none());
    }

    #[tokio::test]
    async fn test_factory_try_new_returns_none_for_non_urls() {
        let factory = SpiceUrlTableFactory::new();

        // Non-URL table names should return Ok(None) without needing state
        let result = factory.try_new("my_table").await;
        assert!(matches!(result, Ok(None)));

        let result = factory.try_new("schema.table").await;
        assert!(matches!(result, Ok(None)));

        let result = factory.try_new("/local/path/file.parquet").await;
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn test_validate_azure_url_valid_formats() {
        // Valid Azure URLs with account in host
        validate_azure_url("abfss://container@account.dfs.core.windows.net/path/file.parquet")
            .expect("should accept valid Azure URL with account in host");
        validate_azure_url("abfss://container@myaccount.dfs.core.windows.net/")
            .expect("should accept valid Azure URL with account in host");
        validate_azure_url("abfs://container@account.dfs.core.windows.net/")
            .expect("should accept valid Azure URL with account in host");
        validate_azure_url("abfss://container@storageaccount/path/")
            .expect("should accept valid Azure URL with account in host");

        // Valid Azure URLs with account in fragment
        validate_azure_url("abfss://container/#account=myaccount")
            .expect("should accept Azure URL with account in fragment");
        validate_azure_url("abfss://container/path/#account=myaccount")
            .expect("should accept Azure URL with account in fragment");
        validate_azure_url("abfss://container/#account=myaccount&other=value")
            .expect("should accept Azure URL with account in fragment");
        validate_azure_url("abfss://container/#other=value&account=myaccount")
            .expect("should accept Azure URL with account in fragment");

        // Non-Azure URLs should always pass (they have different validation)
        validate_azure_url("s3://bucket/path/").expect("should accept non-Azure URL");
        validate_azure_url("gs://bucket/path/").expect("should accept non-Azure URL");
        validate_azure_url("https://example.com/data.parquet")
            .expect("should accept non-Azure URL");
    }

    #[test]
    fn test_validate_azure_url_missing_account() {
        // Azure URLs without account should fail
        let result = validate_azure_url("abfss://container/path/file.parquet");
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            err.to_string().contains("missing the storage account name"),
            "Error should mention missing account: {err}"
        );

        let result = validate_azure_url("abfs://container/");
        assert!(result.is_err());

        let result = validate_azure_url("abfss://mycontainer/data/");
        assert!(result.is_err());

        // Empty fragment should still fail
        let result = validate_azure_url("abfss://container/#");
        assert!(result.is_err());

        // Fragment without account key should fail
        let result = validate_azure_url("abfss://container/#other=value");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_azure_url_error_message_contains_guidance() {
        let result = validate_azure_url("abfss://container/path/");
        let err = result.expect_err("should fail");
        let err_msg = err.to_string();

        // Error message should contain helpful guidance
        assert!(
            err_msg.contains("abfss://container@account.dfs.core.windows.net"),
            "Should show full URL format example: {err_msg}"
        );
        assert!(
            err_msg.contains("#account="),
            "Should show fragment format: {err_msg}"
        );
        assert!(
            err_msg.contains("AZURE_STORAGE_ACCOUNT"),
            "Should mention environment variable: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_factory_try_new_azure_missing_account_returns_error() {
        let factory = SpiceUrlTableFactory::new();

        // Azure URL without account should return an error (not Ok(None))
        let result = factory.try_new("abfss://container/path/file.parquet").await;
        assert!(
            result.is_err(),
            "Should return error for Azure URL missing account"
        );

        let err = result.expect_err("should be an error");
        assert!(
            err.to_string().contains("missing the storage account name"),
            "Error should mention missing account: {err}"
        );
    }

    #[test]
    fn test_is_internal_ip_blocks_dangerous_ranges() {
        use std::str::FromStr;

        // Must be blocked (SSRF targets).
        for ip in [
            "127.0.0.1",
            "127.1.2.3",
            "169.254.169.254", // AWS/GCP/Azure IMDS
            "10.0.0.5",
            "192.168.1.1",
            "172.16.0.1",
            "172.31.255.255",
            "100.64.0.1", // CGNAT
            "0.0.0.0",
            "198.18.0.1", // benchmarking
            "240.0.0.1",  // reserved
            "255.255.255.255",
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(is_internal_ip(addr), "{ip} should be treated as internal");
        }

        // IPv6 internal ranges.
        for ip in [
            "::1",                    // loopback
            "::",                     // unspecified
            "fe80::1",                // link local
            "fc00::1",                // unique local
            "fd12:3456::1",           // unique local
            "::ffff:169.254.169.254", // v4-mapped metadata
            "::ffff:127.0.0.1",       // v4-mapped loopback
            "ff02::1",                // multicast
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(is_internal_ip(addr), "{ip} should be treated as internal");
        }

        // Must be allowed (legitimate public addresses).
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",        // example.com
            "2606:4700:4700::1111", // Cloudflare DNS
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(!is_internal_ip(addr), "{ip} should be treated as public");
        }
    }

    #[test]
    fn test_is_blocked_internal_hostname() {
        // Well-known internal names blocked without DNS (case-insensitive).
        for host in [
            "localhost",
            "LOCALHOST",
            "service.localhost",
            "metadata.google.internal",
            "metadata.goog",
        ] {
            assert!(
                is_blocked_internal_hostname(host),
                "{host} should be blocked"
            );
        }

        // Legitimate public hostnames must pass (DNS resolution handles these).
        for host in [
            "example.com",
            "localhost.example.com", // not the `.localhost` TLD
            "metadata.google.com",   // not the metadata sentinel
        ] {
            assert!(
                !is_blocked_internal_hostname(host),
                "{host} should not be blocked by literal match"
            );
        }
    }

    #[tokio::test]
    async fn test_ssrf_guard_blocks_internal_http_urls() {
        // Direct internal IP literals and localhost are blocked (no DNS needed).
        for url in [
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            "http://127.0.0.1:8080/admin",
            "https://10.0.0.5/data.parquet",
            "http://[::1]:9000/data",
            "https://localhost/data.parquet",
            "https://service.localhost/data.parquet",
            "http://metadata.google.internal/computeMetadata/v1/",
        ] {
            let result = validate_http_url_with_policy(url, false).await;
            assert!(result.is_err(), "{url} should be blocked by the SSRF guard");
        }
    }

    #[tokio::test]
    async fn test_ssrf_guard_allows_public_and_non_http() {
        // Public IP literals (no DNS) pass.
        validate_http_url_with_policy("https://8.8.8.8/data.parquet", false)
            .await
            .expect("public IP should be allowed");

        // Non-http(s) schemes are out of scope for this guard.
        for url in [
            "s3://bucket/data.parquet",
            "gs://bucket/data/",
            "abfss://container@account.dfs.core.windows.net/path/",
        ] {
            validate_http_url_with_policy(url, false)
                .await
                .unwrap_or_else(|e| panic!("{url} should be allowed: {e}"));
        }
    }

    #[tokio::test]
    async fn test_ssrf_guard_can_be_disabled_by_policy() {
        // When the allow-private policy is on, internal hosts are permitted.
        validate_http_url_with_policy("http://127.0.0.1/data.parquet", true)
            .await
            .expect("internal host should be allowed when policy permits");
    }
}
