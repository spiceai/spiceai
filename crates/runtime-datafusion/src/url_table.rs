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

use std::net::{IpAddr, ToSocketAddrs};
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
use runtime_object_store::ssrf::URL_TABLE_SCHEME_PREFIX;
pub use runtime_object_store::ssrf::{
    ALLOW_PRIVATE_HOSTS_ENV, allow_private_hosts, is_blocked_internal_hostname, is_internal_ip,
};
use url::{Url, form_urlencoded};

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

/// Upper bound on how long the SSRF guard waits for a host's DNS resolution.
/// Caps the query-visible wait so a slow or hanging resolver can't stall the
/// query indefinitely. `spawn_blocking` work can't be cancelled, so the
/// underlying `getaddrinfo` thread keeps running until the OS resolver's own
/// timeout — but the caller stops waiting after this bound and returns a clear
/// error instead of pinning the request on a slow lookup.
const DNS_RESOLUTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Builds the error returned when a URL table targets a blocked internal host.
/// `url_str` is debug-formatted (`{:?}`) so user-controlled control characters
/// are escaped rather than echoed into the log/error line.
fn ssrf_blocked_error(url_str: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "URL table {url_str:?} targets a private, loopback, or link-local address, which is not allowed for SSRF protection. If this runtime intentionally queries an internal HTTP endpoint, set {ALLOW_PRIVATE_HOSTS_ENV}=true."
    ))
}

/// Fragment keys a URL table may set on an object-store URL (`s3`, `abfs`, `gs`, ...).
///
/// Every other key the object-store builders read — `endpoint`, `allow_http`, credentials,
/// proxies, identity endpoints — would let a query choose where requests go or which
/// credentials they carry, so those belong to a configured dataset.
const OBJECT_STORE_FRAGMENT_KEYS: &[&str] =
    &["account", "auth", "client_timeout", "region", "url_style"];

/// The object-store builders format `region` and `account` into the provider's endpoint URL,
/// so a value with URL syntax in it (`@`, `/`, `:`) could replace the host.
fn is_valid_fragment_value(key: &str, value: &str) -> bool {
    match key {
        "region" => {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        }
        "account" => !value.is_empty() && value.bytes().all(|b| b.is_ascii_alphanumeric()),
        _ => true,
    }
}

/// Rejects an object-store URL table whose fragment sets anything but
/// [`OBJECT_STORE_FRAGMENT_KEYS`], so a query reaches only the provider's own endpoint.
///
/// The fragment is decoded as the object-store registry decodes it, so a percent-encoded key
/// such as `%65ndpoint` is caught. This holds regardless of [`ALLOW_PRIVATE_HOSTS_ENV`].
fn validate_object_store_fragment(url_str: &str) -> DFResult<()> {
    let Ok(mut url) = Url::parse(url_str) else {
        return Ok(());
    };
    if matches!(url.scheme(), "http" | "https") {
        return Ok(());
    }
    let Some(fragment) = url.fragment().map(str::to_string) else {
        return Ok(());
    };
    url.set_fragment(None);
    let location = url.as_str();

    for (key, value) in form_urlencoded::parse(fragment.as_bytes()) {
        if !OBJECT_STORE_FRAGMENT_KEYS.contains(&key.as_ref()) {
            return Err(DataFusionError::Plan(format!(
                "URL table {location:?} sets `{}` in its URL fragment, which a query cannot set because it would choose the object store's endpoint or credentials. A URL table accepts only {}. To read this location, configure a dataset instead. See: https://spiceai.org/docs/components/data-connectors",
                key.escape_debug(),
                OBJECT_STORE_FRAGMENT_KEYS
                    .iter()
                    .map(|key| format!("`{key}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if !is_valid_fragment_value(&key, &value) {
            return Err(DataFusionError::Plan(format!(
                "URL table {location:?} sets an invalid `{key}` in its URL fragment. A `region` contains only letters, digits, and hyphens, and an `account` only letters and digits."
            )));
        }
    }
    Ok(())
}

/// Guards against SSRF for `http`/`https` URL tables by rejecting URLs whose
/// host is (or resolves to) an internal address. Object-store schemes (`s3`,
/// `gs`, `abfs`, ...) reach the provider's own endpoint, because
/// [`validate_object_store_fragment`] rejects every endpoint override.
///
/// This check gives a clear planning error up front. The URL table's object
/// store enforces the same classifier again on every address it resolves and
/// every redirect it follows, which also covers a host whose DNS answer changes
/// between the two lookups.
async fn validate_http_url_not_internal(url_str: &str) -> DFResult<()> {
    validate_http_url_with_policy(url_str, allow_private_hosts()).await
}

/// URL the object-store registry should build for a SQL URL table.
///
/// `http://` gains `allow_http=true` in the fragment. An existing fragment is
/// kept, and an explicit `allow_http` value is left as written. `https://` is
/// unchanged, so a redirect from it cannot switch the fetch to plaintext.
fn object_store_url_for_sql_table(url: &str) -> DFResult<Url> {
    let mut parsed = Url::parse(url).map_err(|err| {
        DataFusionError::Plan(format!("Failed to parse URL table '{url}': {err}"))
    })?;
    if parsed.scheme() == "http" {
        let fragment = parsed.fragment().unwrap_or("");
        // The registry parses the fragment with the form decoder, which
        // percent-decodes keys. `%61llow_http` is `allow_http` there, and a
        // later raw `allow_http=true` would override an explicit `false`.
        let already_set =
            url::form_urlencoded::parse(fragment.as_bytes()).any(|(key, _)| key == "allow_http");
        if !already_set {
            let mut next = fragment.to_string();
            if !next.is_empty() {
                next.push('&');
            }
            next.push_str("allow_http=true");
            parsed.set_fragment(Some(&next));
        }
    }
    Ok(parsed)
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
            // Bound the blocking lookup with a timeout so a malicious query that
            // supplies slow/hanging hosts can't make the caller wait unboundedly
            // on the blocking pool.
            let lookup_host = domain.to_ascii_lowercase();
            let resolve = tokio::task::spawn_blocking(move || {
                (lookup_host.as_str(), 0u16)
                    .to_socket_addrs()
                    .map(|iter| iter.map(|s| s.ip()).collect::<Vec<IpAddr>>())
            });
            let addrs = match tokio::time::timeout(DNS_RESOLUTION_TIMEOUT, resolve).await {
                Err(_) => {
                    return Err(DataFusionError::Plan(format!(
                        "Timed out resolving host for URL table {url_str:?} after {}s",
                        DNS_RESOLUTION_TIMEOUT.as_secs()
                    )));
                }
                Ok(join_result) => join_result
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "DNS resolution task failed for URL table {url_str:?}: {e}"
                        ))
                    })?
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "Failed to resolve host for URL table {url_str:?}: {e}"
                        ))
                    })?,
            };

            if addrs.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "Could not resolve host for URL table {url_str:?}"
                )));
            }
            if addrs.iter().any(|ip| is_internal_ip(*ip)) {
                return Err(ssrf_blocked_error(url_str));
            }
        }
        None => {
            return Err(DataFusionError::Plan(format!(
                "URL table {url_str:?} is missing a host"
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
    /// The session's object-store registry. An `http://` SQL URL registers a
    /// plaintext store here without replacing one a dataset already configured.
    registry: RwLock<Option<Arc<runtime_object_store::registry::SpiceObjectStoreRegistry>>>,
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
            registry: RwLock::new(None),
        }
    }

    /// Set the session state reference for schema inference.
    pub fn with_state(&self, state: Weak<RwLock<SessionState>>) {
        *self.state.write() = Some(state);
    }

    /// Use the same object-store registry the session queries with.
    pub fn with_registry(
        &self,
        registry: Arc<runtime_object_store::registry::SpiceObjectStoreRegistry>,
    ) {
        *self.registry.write() = Some(registry);
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

        // Prevent SSRF: object-store URL tables must not choose their endpoint or
        // credentials, and http/https URL tables must not target internal addresses.
        validate_object_store_fragment(url)?;
        validate_http_url_not_internal(url).await?;

        // An `http://` SQL URL is the operator asking for plaintext, the same
        // way an `http://` dataset does. Register that store after the address
        // check. An `https://` URL stays HTTPS-only, and a redirect to `http://`
        // is still refused.
        //
        // Schema inference looks the store up with the full listing URL,
        // fragment included. A fragment that differs from the one just
        // registered — `client_timeout` without `allow_http`, for example —
        // rebuilds an HTTPS-only store and replaces the selected one. The
        // listing URL is therefore fragment-free, and that bare lookup reuses
        // the store selected here.
        let store_url = object_store_url_for_sql_table(url)?;
        let plaintext = store_url.scheme() == "http";
        let listing_url = if plaintext {
            url.split_once('#')
                .map_or(url, |(without_fragment, _)| without_fragment)
        } else {
            url
        };

        // While the SSRF guard is on, http/https URL tables read through their own
        // guarded object store, never a dataset's store for the same host.
        let (store_url, listing_url) =
            if matches!(store_url.scheme(), "http" | "https") && !allow_private_hosts() {
                let guarded = Url::parse(&format!("{URL_TABLE_SCHEME_PREFIX}{store_url}"))
                    .map_err(|e| {
                        DataFusionError::Plan(format!("Failed to parse URL table '{url}': {e}"))
                    })?;
                (guarded, format!("{URL_TABLE_SCHEME_PREFIX}{listing_url}"))
            } else {
                (store_url, listing_url.to_string())
            };
        if plaintext {
            let registry = self.registry.read().clone();
            let Some(registry) = registry else {
                return Err(DataFusionError::Plan(format!(
                    "Failed to read URL table '{url}'. Plaintext HTTP is not set up for this runtime."
                )));
            };
            registry.ensure_plaintext_http_store(&store_url)?;
        }

        // Parse the URL
        let table_url = ListingTableUrl::parse(&listing_url).map_err(|e| {
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

    /// The catalog list this one adds URL tables to.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn CatalogProviderList> {
        &self.inner
    }
}

impl CatalogProviderList for DynamicUrlCatalogList {
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

    /// `SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS` is process-global. These tests
    /// set it, so they hold this lock across the awaits that read it. A tokio
    /// mutex is the lock that can stay held across `.await`.
    static HTTP_URL_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        assert!(retrieved.is::<DynamicUrlCatalogProvider>());
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
        assert!(retrieved.is::<DynamicUrlSchemaProvider>());
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

    const DATA_CONNECTORS_DOCS: &str = "https://spiceai.org/docs/components/data-connectors";

    /// The factory has no session state, so an error other than the missing state can only
    /// come from a check that runs before any object store is built or contacted.
    async fn try_new_without_state(url: &str) -> String {
        SpiceUrlTableFactory::new()
            .try_new(url)
            .await
            .expect_err("a factory without session state cannot build a table")
            .to_string()
    }

    #[tokio::test]
    async fn object_store_url_tables_reject_an_endpoint_override() {
        for (url, location) in [
            (
                "s3://bucket/data.parquet#endpoint=http://10.0.0.1:9000",
                "s3://bucket/data.parquet",
            ),
            (
                "s3://bucket/data/#region=us-east-1&endpoint=http://172.30.0.2:18077&allow_http=true",
                "s3://bucket/data/",
            ),
            (
                "abfss://container@account.dfs.core.windows.net/data/#endpoint=http://10.0.0.1",
                "abfss://container@account.dfs.core.windows.net/data/",
            ),
            (
                "abfss://container/data/#account=myaccount&endpoint=http://10.0.0.1",
                "abfss://container/data/",
            ),
            // Percent-encoded `endpoint`, which the object-store registry decodes.
            (
                "s3://bucket/data.parquet#%65ndpoint=http://10.0.0.1",
                "s3://bucket/data.parquet",
            ),
        ] {
            assert_eq!(
                try_new_without_state(url).await,
                format!(
                    "Error during planning: URL table {location:?} sets `endpoint` in its URL fragment, which a query cannot set because it would choose the object store's endpoint or credentials. A URL table accepts only `account`, `auth`, `client_timeout`, `region`, `url_style`. To read this location, configure a dataset instead. See: {DATA_CONNECTORS_DOCS}"
                ),
                "{url}"
            );
        }
    }

    #[tokio::test]
    async fn object_store_url_tables_reject_credential_and_transport_overrides() {
        for key in [
            "key",
            "secret",
            "session_token",
            "iam_role_source",
            "allow_http",
            "access_key",
            "sas_string",
            "bearer_token",
            "use_emulator",
            "msi_endpoint",
            "authority_host",
            "proxy_url",
            "service_account_key",
            "max_retries",
        ] {
            for scheme_url in ["s3://bucket/data.parquet", "gs://bucket/data.parquet"] {
                let error = try_new_without_state(&format!("{scheme_url}#{key}=x")).await;
                assert!(
                    error.contains(&format!("sets `{key}` in its URL fragment")),
                    "{scheme_url}#{key}: {error}"
                );
            }
        }
    }

    #[tokio::test]
    async fn object_store_url_tables_reject_a_region_or_account_with_url_syntax() {
        for (url, location, key) in [
            (
                "s3://bucket/data.parquet#region=x@10.0.0.1:9000/",
                "s3://bucket/data.parquet",
                "region",
            ),
            (
                "s3://bucket/data.parquet#region=",
                "s3://bucket/data.parquet",
                "region",
            ),
            (
                "abfss://container/data/#account=evil.example.com/",
                "abfss://container/data/",
                "account",
            ),
            (
                "abfss://container/data/#account=x@10.0.0.1",
                "abfss://container/data/",
                "account",
            ),
        ] {
            assert_eq!(
                try_new_without_state(url).await,
                format!(
                    "Error during planning: URL table {location:?} sets an invalid `{key}` in its URL fragment. A `region` contains only letters, digits, and hyphens, and an `account` only letters and digits."
                ),
                "{url}"
            );
        }
    }

    #[tokio::test]
    async fn object_store_url_tables_accept_the_allowed_fragment_keys() {
        for url in [
            "s3://bucket/data.parquet",
            "s3://bucket/data/#region=us-east-1&auth=public&url_style=path&client_timeout=30s",
            "abfss://container/data/#account=myaccount",
            "gs://bucket/data/#client_timeout=10s",
        ] {
            assert_eq!(
                try_new_without_state(url).await,
                "Invalid or Unsupported Configuration: Session state not available for URL table creation",
                "{url}"
            );
        }
    }

    /// Builds a session whose object store for `store_url` is an in-memory store holding a CSV
    /// at `path`.
    async fn session_with_csv(
        store_url: &str,
        path: &str,
    ) -> (
        datafusion::prelude::SessionContext,
        Arc<runtime_object_store::registry::SpiceObjectStoreRegistry>,
    ) {
        use datafusion::prelude::{SessionConfig, SessionContext};
        use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};
        use runtime_object_store::registry::{SpiceObjectStoreRegistry, runtime_env_with_registry};

        let store = Arc::new(InMemory::new());
        store
            .put(
                &object_store::path::Path::from(path),
                PutPayload::from_static(b"id,name\n1,a\n2,b\n"),
            )
            .await
            .expect("the CSV is written");
        let registry = Arc::new(SpiceObjectStoreRegistry::new(
            tokio::runtime::Handle::current(),
        ));
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new(),
            runtime_env_with_registry(Arc::clone(&registry)),
        );
        ctx.register_object_store(&Url::parse(store_url).expect("valid store URL"), store);
        (ctx, registry)
    }

    async fn row_count(
        ctx: &datafusion::prelude::SessionContext,
        table: Arc<dyn TableProvider>,
    ) -> usize {
        let batches = ctx
            .read_table(table)
            .expect("the table reads")
            .collect()
            .await
            .expect("the table scans");
        batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum()
    }

    #[tokio::test]
    async fn s3_url_table_with_allowed_fragment_builds() {
        let (ctx, _) = session_with_csv("s3://bucket", "data/items.csv").await;
        let factory = SpiceUrlTableFactory::new();
        factory.with_state(ctx.state_weak_ref());

        let table = factory
            .try_new("s3://bucket/data/items.csv#region=us-east-1&auth=public")
            .await
            .expect("the table builds")
            .expect("the URL is a URL table");

        assert_eq!(row_count(&ctx, table).await, 2);
    }

    /// An `http` URL table reads through the store registered under the URL table scheme, not
    /// the store a dataset for the same host would use.
    #[tokio::test]
    async fn http_url_table_reads_through_the_url_table_store() {
        let _env_lock = HTTP_URL_ENV.lock().await;
        assert!(
            !allow_private_hosts(),
            "this test needs {ALLOW_PRIVATE_HOSTS_ENV} unset"
        );
        let (ctx, registry) = session_with_csv(
            &format!("{URL_TABLE_SCHEME_PREFIX}http://8.8.8.8"),
            "items.csv",
        )
        .await;
        let factory = SpiceUrlTableFactory::new();
        factory.with_registry(registry);
        factory.with_state(ctx.state_weak_ref());

        let table = factory
            .try_new("http://8.8.8.8/items.csv")
            .await
            .expect("the table builds")
            .expect("the URL is a URL table");

        assert_eq!(row_count(&ctx, table).await, 2);
    }

    #[tokio::test]
    async fn test_ssrf_guard_can_be_disabled_by_policy() {
        // When the allow-private policy is on, internal hosts are permitted.
        validate_http_url_with_policy("http://127.0.0.1/data.parquet", true)
            .await
            .expect("internal host should be allowed when policy permits");
    }

    #[test]
    fn http_sql_url_opts_into_plaintext_and_https_does_not() {
        let http = object_store_url_for_sql_table("http://example.com/data.csv")
            .expect("an http URL parses");
        assert_eq!(http.fragment(), Some("allow_http=true"));

        let with_auth =
            object_store_url_for_sql_table("http://example.com/data.csv#basic_auth=tok")
                .expect("an http URL with a fragment parses");
        let fragment = with_auth.fragment().expect("the fragment is kept");
        assert!(
            fragment.split('&').any(|pair| pair == "basic_auth=tok"),
            "{fragment}"
        );
        assert!(
            fragment.split('&').any(|pair| pair == "allow_http=true"),
            "{fragment}"
        );

        let explicit =
            object_store_url_for_sql_table("http://example.com/data.csv#allow_http=false")
                .expect("an explicit allow_http parses");
        assert_eq!(explicit.fragment(), Some("allow_http=false"));

        let https = object_store_url_for_sql_table("https://example.com/data.csv")
            .expect("an https URL parses");
        assert_eq!(https.fragment(), None);

        // `%61` is `a`. The registry decodes the key, so this is an explicit
        // `allow_http=false` and must not gain a second `allow_http=true`.
        let encoded =
            object_store_url_for_sql_table("http://example.com/data.csv#%61llow_http=false")
                .expect("an encoded fragment parses");
        let fragment = encoded.fragment().expect("the encoded fragment is kept");
        let keys: Vec<String> = url::form_urlencoded::parse(fragment.as_bytes())
            .map(|(key, _)| key.into_owned())
            .filter(|key| key == "allow_http")
            .collect();
        assert_eq!(keys, ["allow_http".to_string()], "{fragment}");
        assert!(
            !fragment.split('&').any(|pair| pair == "allow_http=true"),
            "{fragment}"
        );
    }

    /// One CSV response for the SQL URL-table regression. `HEAD` reports the
    /// length; `GET` returns the body, including a single byte range.
    async fn serve_csv_once(
        mut socket: tokio::net::TcpStream,
        seen: Arc<parking_lot::Mutex<Vec<String>>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const CSV: &[u8] = b"id\n1\n";
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = match socket.read(&mut tmp).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|window| window == b"\r\n\r\n") || buf.len() > 8192 {
                break;
            }
        }
        let text = String::from_utf8_lossy(&buf);
        let mut lines = text.split("\r\n");
        let mut request = lines.next().unwrap_or("").split_whitespace();
        let method = request.next().unwrap_or("").to_string();
        let path = request.next().unwrap_or("").to_string();
        seen.lock().push(format!("{method} {path}"));

        let mut range: Option<(usize, usize)> = None;
        for line in lines {
            let Some(spec) = line.split_once(':') else {
                continue;
            };
            if !spec.0.eq_ignore_ascii_case("range") {
                continue;
            }
            let spec = spec.1.trim().strip_prefix("bytes=").unwrap_or("");
            let Some((start_s, end_s)) = spec.split_once('-') else {
                continue;
            };
            let Ok(start) = start_s.parse::<usize>() else {
                continue;
            };
            let end = end_s
                .parse::<usize>()
                .unwrap_or(CSV.len().saturating_sub(1))
                .min(CSV.len().saturating_sub(1));
            if start < CSV.len() && start <= end {
                range = Some((start, end));
            }
        }

        let response = if method == "HEAD" {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                CSV.len()
            )
            .into_bytes()
        } else if method == "GET" {
            let (status, body) = if let Some((start, end)) = range {
                let slice = &CSV[start..=end];
                (
                    format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Type: text/csv\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        CSV.len(),
                        slice.len()
                    ),
                    slice,
                )
            } else {
                (
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        CSV.len()
                    ),
                    CSV,
                )
            };
            let mut bytes = status.into_bytes();
            bytes.extend_from_slice(body);
            bytes
        } else {
            b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_vec()
        };
        let _ = socket.write_all(&response).await;
    }

    #[tokio::test]
    async fn http_sql_url_table_reaches_an_explicit_http_server() {
        use datafusion::prelude::{SessionConfig, SessionContext};
        use runtime_object_store::registry::{SpiceObjectStoreRegistry, runtime_env_with_registry};

        struct ClearAllowPrivateHosts;
        impl Drop for ClearAllowPrivateHosts {
            fn drop(&mut self) {
                // The variable is process-global; clear it even when the test fails.
                unsafe { std::env::remove_var(ALLOW_PRIVATE_HOSTS_ENV) };
            }
        }
        let _env_lock = HTTP_URL_ENV.lock().await;
        unsafe { std::env::remove_var(ALLOW_PRIVATE_HOSTS_ENV) };
        let _clear = ClearAllowPrivateHosts;

        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the csv server binds");
        let port = listener
            .local_addr()
            .expect("the csv server has a port")
            .port();
        let seen_server = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let seen_server = Arc::clone(&seen_server);
                tokio::spawn(serve_csv_once(socket, seen_server));
            }
        });

        let url = format!("http://127.0.0.1:{port}/data.csv");
        let registry = Arc::new(SpiceObjectStoreRegistry::new(
            tokio::runtime::Handle::current(),
        ));
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new(),
            runtime_env_with_registry(Arc::clone(&registry)),
        );
        let factory = SpiceUrlTableFactory::new();
        factory.with_registry(Arc::clone(&registry));
        factory.with_state(ctx.state_weak_ref());

        let blocked = factory
            .try_new(&url)
            .await
            .expect_err("an internal http URL is refused before any store is built");
        assert!(
            blocked
                .to_string()
                .contains("private, loopback, or link-local"),
            "{blocked}"
        );
        assert!(
            seen.lock().is_empty(),
            "the refused URL still contacted the server: {blocked}"
        );

        unsafe { std::env::set_var(ALLOW_PRIVATE_HOSTS_ENV, "true") };
        let table = factory
            .try_new(&url)
            .await
            .unwrap_or_else(|err| panic!("an explicit http URL loads: {err}"))
            .expect("the URL is a table");
        let requests = seen.lock().clone();
        assert!(
            requests.iter().any(|request| request.contains("/data.csv")),
            "the server saw {requests:?}"
        );
        assert!(
            table
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "id"),
            "schema: {:?}",
            table.schema()
        );
    }

    /// `client_timeout` is an object-store option. The listing lookup must not
    /// treat that fragment as a new store and drop `allow_http`.
    #[tokio::test]
    async fn http_sql_url_fragment_keeps_the_plaintext_store() {
        use datafusion::prelude::{SessionConfig, SessionContext};
        use runtime_object_store::registry::{SpiceObjectStoreRegistry, runtime_env_with_registry};

        struct ClearAllowPrivateHosts;
        impl Drop for ClearAllowPrivateHosts {
            fn drop(&mut self) {
                unsafe { std::env::remove_var(ALLOW_PRIVATE_HOSTS_ENV) };
            }
        }
        let _env_lock = HTTP_URL_ENV.lock().await;
        unsafe { std::env::remove_var(ALLOW_PRIVATE_HOSTS_ENV) };
        let _clear = ClearAllowPrivateHosts;

        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the csv server binds");
        let port = listener
            .local_addr()
            .expect("the csv server has a port")
            .port();
        let seen_server = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let seen_server = Arc::clone(&seen_server);
                tokio::spawn(serve_csv_once(socket, seen_server));
            }
        });

        let registry = Arc::new(SpiceObjectStoreRegistry::new(
            tokio::runtime::Handle::current(),
        ));
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new(),
            runtime_env_with_registry(Arc::clone(&registry)),
        );
        let factory = SpiceUrlTableFactory::new();
        factory.with_registry(Arc::clone(&registry));
        factory.with_state(ctx.state_weak_ref());
        unsafe { std::env::set_var(ALLOW_PRIVATE_HOSTS_ENV, "true") };

        let fragment_url = format!("http://127.0.0.1:{port}/fragment.csv#client_timeout=5s");
        let table = factory
            .try_new(&fragment_url)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "a fragment-bearing http URL loads: {err}; requests={:?}",
                    seen.lock()
                )
            })
            .expect("the URL is a table");
        let requests = seen.lock().clone();
        assert!(
            requests
                .iter()
                .any(|request| request.contains("/fragment.csv")),
            "{requests:?}"
        );
        assert!(
            table
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "id"),
            "schema: {:?}",
            table.schema()
        );

        seen.lock().clear();
        let again = factory
            .try_new(&format!("http://127.0.0.1:{port}/after.csv"))
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "a later bare http URL loads: {err}; requests={:?}",
                    seen.lock()
                )
            })
            .expect("the URL is a table");
        let again_requests = seen.lock().clone();
        assert!(
            again_requests
                .iter()
                .any(|request| request.contains("/after.csv")),
            "{again_requests:?}"
        );
        assert!(
            again
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "id"),
            "schema: {:?}",
            again.schema()
        );
    }

    /// A dataset store for this origin already carries `basic_auth`. The SQL
    /// URL must keep that store. Replacing it with only `allow_http=true`
    /// drops the credential, and the server answers 401.
    #[tokio::test]
    async fn http_sql_url_keeps_a_configured_store_credential() {
        use datafusion::execution::object_store::ObjectStoreRegistry;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use runtime_object_store::registry::{SpiceObjectStoreRegistry, runtime_env_with_registry};

        const EXPECTED_AUTH: &str = "Basic dXNlcjpwYXNz";

        struct ClearAllowPrivateHosts;
        impl Drop for ClearAllowPrivateHosts {
            fn drop(&mut self) {
                unsafe { std::env::remove_var(ALLOW_PRIVATE_HOSTS_ENV) };
            }
        }
        let _env_lock = HTTP_URL_ENV.lock().await;
        unsafe { std::env::remove_var(ALLOW_PRIVATE_HOSTS_ENV) };
        let _clear = ClearAllowPrivateHosts;
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the csv server binds");
        let port = listener
            .local_addr()
            .expect("the csv server has a port")
            .port();
        let seen_server = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let seen_server = Arc::clone(&seen_server);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    const CSV: &[u8] = b"id\n1\n";
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 1024];
                    loop {
                        let n = match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&tmp[..n]);
                        if buf.windows(4).any(|window| window == b"\r\n\r\n") || buf.len() > 8192 {
                            break;
                        }
                    }
                    let text = String::from_utf8_lossy(&buf);
                    let mut lines = text.split("\r\n");
                    let mut request = lines.next().unwrap_or("").split_whitespace();
                    let method = request.next().unwrap_or("").to_string();
                    let path = request.next().unwrap_or("").to_string();
                    let auth = lines
                        .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
                        .map_or_else(
                            || "missing".to_string(),
                            |line| {
                                line.split_once(':')
                                    .map_or("", |(_, value)| value.trim())
                                    .to_string()
                            },
                        );
                    seen_server.lock().push(format!("{method} {path} {auth}"));
                    if auth != EXPECTED_AUTH {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            )
                            .await;
                        return;
                    }
                    let mut range: Option<(usize, usize)> = None;
                    for line in lines {
                        let Some(spec) = line.split_once(':') else {
                            continue;
                        };
                        if !spec.0.eq_ignore_ascii_case("range") {
                            continue;
                        }
                        let spec = spec.1.trim().strip_prefix("bytes=").unwrap_or("");
                        let Some((start_s, end_s)) = spec.split_once('-') else {
                            continue;
                        };
                        let Ok(start) = start_s.parse::<usize>() else {
                            continue;
                        };
                        let end = end_s
                            .parse::<usize>()
                            .unwrap_or(CSV.len().saturating_sub(1))
                            .min(CSV.len().saturating_sub(1));
                        if start < CSV.len() && start <= end {
                            range = Some((start, end));
                        }
                    }
                    let response = if method == "HEAD" {
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            CSV.len()
                        )
                        .into_bytes()
                    } else if method == "GET" {
                        let (status, body) = if let Some((start, end)) = range {
                            let slice = &CSV[start..=end];
                            (
                                format!(
                                    "HTTP/1.1 206 Partial Content\r\nContent-Type: text/csv\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    CSV.len(),
                                    slice.len()
                                ),
                                slice,
                            )
                        } else {
                            (
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    CSV.len()
                                ),
                                CSV,
                            )
                        };
                        let mut bytes = status.into_bytes();
                        bytes.extend_from_slice(body);
                        bytes
                    } else {
                        b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_vec()
                    };
                    let _ = socket.write_all(&response).await;
                });
            }
        });

        let registry = Arc::new(SpiceObjectStoreRegistry::new(
            tokio::runtime::Handle::current(),
        ));
        let mut configured = Url::parse(&format!("http://127.0.0.1:{port}/data.csv"))
            .expect("the configured URL parses");
        let token = runtime_object_store::http_auth::register(
            &configured,
            "user",
            secrecy::SecretString::from("pass"),
        );
        configured.set_fragment(Some(&format!(
            "allow_http=true&{}={token}",
            runtime_object_store::http_auth::FRAGMENT_KEY
        )));
        registry
            .get_store(&configured)
            .expect("the dataset store builds");

        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new(),
            runtime_env_with_registry(Arc::clone(&registry)),
        );
        let factory = SpiceUrlTableFactory::new();
        factory.with_registry(Arc::clone(&registry));
        factory.with_state(ctx.state_weak_ref());
        unsafe { std::env::set_var(ALLOW_PRIVATE_HOSTS_ENV, "true") };

        let table = factory
            .try_new(&format!("http://127.0.0.1:{port}/data.csv"))
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "configured http URL loads: {err}; requests={:?}",
                    seen.lock()
                )
            })
            .expect("the URL is a table");
        let requests = seen.lock().clone();
        assert!(
            requests
                .iter()
                .all(|request| request.ends_with(EXPECTED_AUTH)),
            "{requests:?}"
        );
        assert!(
            table
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "id"),
            "schema: {:?}",
            table.schema()
        );

        // A different fragment on this origin must reuse the credentialed store.
        // Building `client_timeout` on its own would replace that store.
        seen.lock().clear();
        let timed = factory
            .try_new(&format!(
                "http://127.0.0.1:{port}/timed.csv#client_timeout=5s"
            ))
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "a fragment URL keeps the configured store: {err}; {:?}",
                    seen.lock()
                )
            })
            .expect("the URL is a table");
        let timed_requests = seen.lock().clone();
        assert!(
            timed_requests
                .iter()
                .any(|request| request.contains("/timed.csv"))
                && timed_requests
                    .iter()
                    .all(|request| request.ends_with(EXPECTED_AUTH)),
            "{timed_requests:?}"
        );
        assert!(
            timed
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "id"),
            "schema: {:?}",
            timed.schema()
        );

        // A later URL on this origin that sets `allow_http` to false must not
        // reuse the plaintext store, including when the key is percent-encoded.
        // The configured credential has to remain for a following plaintext URL.
        seen.lock().clear();
        for fragment in ["allow_http=false", "%61llow_http=false"] {
            let url = format!("http://127.0.0.1:{port}/must-not-fetch.csv#{fragment}");
            match factory.try_new(&url).await {
                Ok(_) => panic!("`{fragment}` was fetched: {:?}", seen.lock()),
                Err(err) => {
                    let text = err.to_string();
                    assert!(
                        text.contains("`allow_http` is false") && !text.contains("builder error"),
                        "{fragment}: {text}"
                    );
                }
            }
        }
        assert!(
            seen.lock()
                .iter()
                .all(|request| !request.contains("must-not-fetch")),
            "{:?}",
            seen.lock()
        );

        seen.lock().clear();
        let again = factory
            .try_new(&format!("http://127.0.0.1:{port}/again.csv"))
            .await
            .unwrap_or_else(|err| panic!("a later plaintext URL loads: {err}; {:?}", seen.lock()))
            .expect("the URL is a table");
        let again_requests = seen.lock().clone();
        assert!(
            again_requests
                .iter()
                .any(|request| request.contains("/again.csv"))
                && again_requests
                    .iter()
                    .all(|request| request.ends_with(EXPECTED_AUTH)),
            "{again_requests:?}"
        );
        assert!(
            again
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "id"),
            "schema: {:?}",
            again.schema()
        );
    }
}
