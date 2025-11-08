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

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Column, Constraint, Constraints, project_schema},
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use reqwest::{Client, header::CACHE_CONTROL};
use snafu::prelude::*;
use std::collections::HashMap;
use std::{
    any::Any,
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequest { source: reqwest::Error },

    #[snafu(display("Invalid URL: {source}"))]
    InvalidUrl { source: url::ParseError },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(err: Error) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

#[derive(Clone)]
struct CachedResponse {
    content: String,
    cached_at: SystemTime,
    max_age: Duration,
    stale_while_revalidate: Option<Duration>,
}

impl CachedResponse {
    fn is_fresh(&self) -> bool {
        self.cached_at
            .elapsed()
            .ok()
            .is_some_and(|elapsed| elapsed < self.max_age)
    }

    fn is_stale_but_revalidatable(&self) -> bool {
        if let Some(stale_duration) = self.stale_while_revalidate
            && let Ok(elapsed) = self.cached_at.elapsed()
        {
            return elapsed >= self.max_age && elapsed < self.max_age + stale_duration;
        }
        false
    }
}

/// A table provider that fetches data from HTTP endpoints based on path and query filters
#[derive(Clone)]
pub struct HttpTableProvider {
    base_url: Url,
    client: Client,
    file_format: String,
    schema: SchemaRef,
    constraints: Constraints,
    cache: Arc<RwLock<HashMap<String, CachedResponse>>>,
    acceleration_enabled: bool,
}

impl std::fmt::Debug for HttpTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTableProvider")
            .field("base_url", &self.base_url)
            .field("file_format", &self.file_format)
            .field("acceleration_enabled", &self.acceleration_enabled)
            .finish_non_exhaustive()
    }
}

impl HttpTableProvider {
    #[must_use]
    pub fn new(
        base_url: Url,
        client: Client,
        file_format: String,
        acceleration_enabled: bool,
    ) -> Self {
        Self {
            base_url,
            client,
            file_format,
            schema: Arc::new(Self::base_table_schema()),
            // Mark `path` and `query` as primary key components
            constraints: Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0, 1])]),
            cache: Arc::new(RwLock::new(HashMap::new())),
            acceleration_enabled,
        }
    }

    #[must_use]
    pub fn base_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("query", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, false),
        ])
    }

    /// Extract path and query from filters
    fn get_cache_key(path: &str, query: Option<&str>) -> String {
        format!("{}?{}", path, query.unwrap_or(""))
    }

    /// Validates the HTTP endpoint by attempting a request to a non-existent path.
    /// This helps detect issues like DNS errors, connection problems, or invalid URLs
    /// early in the initialization process.
    pub async fn validate_endpoint(&self) -> Result<()> {
        use rand::Rng;
        use rand::distr::Alphanumeric;

        // Generate a random path that should return 404
        let random_suffix: String = rand::rng()
            .sample_iter(Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        let test_path = format!("/__spice_health_check_{}", random_suffix);

        let mut test_url = self.base_url.clone();
        test_url.set_path(&test_path);

        tracing::debug!("Validating HTTP endpoint: {}", self.base_url);

        match self.client.get(test_url).send().await {
            Ok(response) => {
                let status = response.status();
                tracing::debug!(
                    "HTTP endpoint validation response: {} (status: {})",
                    self.base_url,
                    status
                );
                // Any response (including 404) means the endpoint is reachable
                Ok(())
            }
            Err(e) => {
                // Check the error type to provide more specific messages and just return the error
                Err(Error::HttpRequest { source: e })
            }
        }
    }

    fn parse_cache_control(cache_control_header: Option<&str>) -> (Duration, Option<Duration>) {
        let mut max_age = Duration::from_secs(0);
        let mut stale_while_revalidate = None;

        if let Some(header) = cache_control_header {
            for directive in header.split(',') {
                let directive = directive.trim();
                if let Some(value) = directive.strip_prefix("max-age=") {
                    if let Ok(seconds) = value.parse::<u64>() {
                        max_age = Duration::from_secs(seconds);
                    }
                } else if let Some(value) = directive.strip_prefix("stale-while-revalidate=")
                    && let Ok(seconds) = value.parse::<u64>()
                {
                    stale_while_revalidate = Some(Duration::from_secs(seconds));
                }
            }
        }

        (max_age, stale_while_revalidate)
    }

    async fn fetch_and_cache(&self, path: &str, query: Option<&str>) -> Result<String> {
        let mut url = self.base_url.clone();

        // Append the path to the base URL's path
        let base_path = self.base_url.path();
        let full_path = if base_path == "/" || base_path.is_empty() {
            path.to_string()
        } else if path.starts_with('/') {
            format!("{}{}", base_path.trim_end_matches('/'), path)
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), path)
        };
        url.set_path(&full_path);

        if let Some(q) = query {
            url.set_query(Some(q));
        }

        tracing::debug!("Fetching HTTP content from: {}", url);

        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .context(HttpRequestSnafu)?;

        if let Err(err) = response.error_for_status_ref() {
            return Err(Error::HttpRequest { source: err });
        }

        // Parse Cache-Control header
        let cache_control_header = response
            .headers()
            .get(CACHE_CONTROL)
            .and_then(|v| v.to_str().ok());

        let (max_age, stale_while_revalidate) = Self::parse_cache_control(cache_control_header);

        let content = response.text().await.context(HttpRequestSnafu)?;

        // Cache the response if max_age > 0
        if max_age.as_secs() > 0 {
            let cache_key = Self::get_cache_key(path, query);
            let cached_response = CachedResponse {
                content: content.clone(),
                cached_at: SystemTime::now(),
                max_age,
                stale_while_revalidate,
            };

            let mut cache = self.cache.write().await;
            cache.insert(cache_key, cached_response);
        }

        Ok(content)
    }

    async fn get_content(&self, path: &str, query: Option<&str>) -> Result<String> {
        let cache_key = Self::get_cache_key(path, query);

        // Try to get from cache
        let cached = {
            let cache = self.cache.read().await;
            cache.get(&cache_key).cloned()
        };

        if let Some(cached_response) = cached {
            if cached_response.is_fresh() {
                tracing::debug!("Returning fresh cached content for {}", cache_key);
                return Ok(cached_response.content);
            }

            if cached_response.is_stale_but_revalidatable() && self.acceleration_enabled {
                tracing::debug!(
                    "Returning stale content while revalidating for {}",
                    cache_key
                );

                // Trigger background refresh
                let provider = Self {
                    base_url: self.base_url.clone(),
                    client: self.client.clone(),
                    file_format: self.file_format.clone(),
                    schema: Arc::clone(&self.schema),
                    constraints: self.constraints.clone(),
                    cache: Arc::clone(&self.cache),
                    acceleration_enabled: self.acceleration_enabled,
                };
                let path = path.to_string();
                let query = query.map(String::from);

                tokio::spawn(async move {
                    tracing::debug!("Background revalidation for {}", cache_key);
                    if let Err(e) = provider.fetch_and_cache(&path, query.as_deref()).await {
                        tracing::warn!("Background revalidation failed: {}", e);
                    }
                });

                return Ok(cached_response.content);
            }
        }

        // Fetch fresh content
        self.fetch_and_cache(path, query).await
    }
}

#[async_trait]
impl TableProvider for HttpTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                // Check if filter references path or query columns
                let cols = f.column_refs();
                if cols.contains(&Column::from_qualified_name("path"))
                    || cols.contains(&Column::from_qualified_name("query"))
                {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        tracing::debug!(
            "HTTP scan called with {} filters: {:?}",
            filters.len(),
            filters
        );

        // Extract all (path, query) pairs from filters (supporting IN/OR)
        let partitions = Self::extract_partitions(filters, &self.base_url);

        tracing::debug!(
            "Extracted {} partitions: {:?}",
            partitions.len(),
            partitions
        );

        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(HttpExec::new(
            projected_schema,
            Arc::new(self.clone()),
            partitions,
        )))
    }
}

#[derive(Clone)]
pub struct HttpExec {
    projected_schema: SchemaRef,
    provider: Arc<HttpTableProvider>,
    partitions: Vec<(Option<String>, Option<String>)>,
    properties: PlanProperties,
}

impl HttpExec {
    #[must_use]
    pub fn new(
        projected_schema: SchemaRef,
        provider: Arc<HttpTableProvider>,
        partitions: Vec<(Option<String>, Option<String>)>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            projected_schema,
            provider,
            partitions,
            properties,
        }
    }

    async fn fetch_and_create_batches(
        &self,
        provider: &HttpTableProvider,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        let mut batches = Vec::with_capacity(self.partitions.len());
        for (path, query) in &self.partitions {
            // Use the filter path or empty string (base URL only)
            let path_val = path.as_deref().unwrap_or("");
            let query_val = query.as_deref();

            tracing::debug!(
                "HttpExec fetching partition: path={:?}, query={:?}",
                path_val,
                query_val
            );

            let content = provider
                .get_content(path_val, query_val)
                .await
                .map_err(DataFusionError::from)?;

            // The path and query values in the batch MUST match the filter values exactly
            // so that DataFusion's FilterExec will keep these rows
            let path_for_batch = path.as_deref().unwrap_or("");
            let query_for_batch = query.as_deref().unwrap_or("");

            tracing::debug!(
                "Creating batch with path={:?}, query={:?}, content_len={}",
                path_for_batch,
                query_for_batch,
                content.len()
            );

            let columns = self
                .projected_schema
                .fields()
                .iter()
                .map(|field| match field.name().as_str() {
                    "path" => Ok(Arc::new(StringArray::from(vec![path_for_batch])) as ArrayRef),
                    "query" => Ok(Arc::new(StringArray::from(vec![query_for_batch])) as ArrayRef),
                    "content" => {
                        Ok(Arc::new(StringArray::from(vec![content.as_str()])) as ArrayRef)
                    }
                    _ => Err(DataFusionError::Execution(format!(
                        "Unsupported field name: {}",
                        field.name()
                    ))),
                })
                .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
            let batch = RecordBatch::try_new(Arc::clone(&self.projected_schema), columns)
                .map_err(DataFusionError::from)?;
            batches.push(batch);
        }
        Ok(batches)
    }
}

impl std::fmt::Debug for HttpExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HttpExec")
    }
}

impl DisplayAs for HttpExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "HttpExec: base_url={}, format={}, partitions=[",
            self.provider.base_url, self.provider.file_format
        )?;

        for (i, (path, query)) in self.partitions.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "(path={:?}, query={:?})",
                path.as_deref().unwrap_or(""),
                query.as_deref().unwrap_or("")
            )?;
        }

        write!(f, "]")
    }
}

impl ExecutionPlan for HttpExec {
    fn name(&self) -> &'static str {
        "HttpExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        tracing::debug!(
            "HttpExec::execute called for partition {}, total partitions: {}",
            partition,
            self.partitions.len()
        );

        let exec = Arc::new(self.clone());
        let provider = Arc::clone(&self.provider);
        let schema = Arc::clone(&self.projected_schema);
        let stream = async_stream::try_stream! {
            tracing::debug!("Starting to fetch batches for {} partitions", exec.partitions.len());
            let batches = exec.fetch_and_create_batches(&provider).await?;
            tracing::debug!("Fetched {} batches", batches.len());
            for (i, batch) in batches.into_iter().enumerate() {
                tracing::debug!("Yielding batch {}: {} rows", i, batch.num_rows());
                yield batch;
            }
        };
        let stream_adapter = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream_adapter))
    }
}

impl HttpTableProvider {
    /// Extract all (path, query) pairs from filters, supporting = and IN-list
    fn extract_partitions(
        filters: &[Expr],
        _base_url: &url::Url,
    ) -> Vec<(Option<String>, Option<String>)> {
        // Extract path and query values from filters
        let mut paths: Vec<String> = vec![];
        let mut queries: Vec<Option<String>> = vec![];
        let mut has_path_filter = false;
        let mut has_query_filter = false;

        for filter in filters {
            if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter
                && *op == Operator::Eq
                && let Expr::Column(col) = left.as_ref()
                && let Expr::Literal(ScalarValue::Utf8(Some(value)), _) = right.as_ref()
            {
                match col.name.as_str() {
                    "path" => {
                        paths.push(value.clone());
                        has_path_filter = true;
                    }
                    "query" => {
                        queries.push(Some(value.clone()));
                        has_query_filter = true;
                    }
                    _ => {}
                }
            }
        }

        // If no path filter, use empty path (will use base URL's path as-is)
        if !has_path_filter {
            paths.push(String::new());
        }

        // If no query filter, use None (no query string)
        if !has_query_filter {
            queries.push(None);
        }

        // Cross product of paths and queries to create all partition combinations
        let mut partitions = vec![];
        for p in &paths {
            for q in &queries {
                partitions.push((if p.is_empty() { None } else { Some(p.clone()) }, q.clone()));
            }
        }

        partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use datafusion::scalar::ScalarValue;
    use url::Url;

    #[test]
    fn test_extract_partitions_with_path_and_query_filters() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");

        // Create filters: path = '/singlesearch/shows' AND query = 'q=South%20Park'
        let filters = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/singlesearch/shows".to_string())),
                    None,
                )),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("query"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("q=South%20Park".to_string())),
                    None,
                )),
            }),
        ];

        let partitions = HttpTableProvider::extract_partitions(&filters, &base_url);

        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0],
            (
                Some("/singlesearch/shows".to_string()),
                Some("q=South%20Park".to_string())
            )
        );
    }

    #[test]
    fn test_extract_partitions_with_only_path_filter() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");

        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/data".to_string())),
                None,
            )),
        })];

        let partitions = HttpTableProvider::extract_partitions(&filters, &base_url);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (Some("/api/data".to_string()), None));
    }

    #[test]
    fn test_extract_partitions_with_no_filters() {
        let base_url = Url::parse("https://api.example.com/default/path").expect("valid URL");

        let filters = vec![];

        let partitions = HttpTableProvider::extract_partitions(&filters, &base_url);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (None, None));
    }

    #[test]
    fn test_extract_partitions_multiple_paths() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");

        let filters = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/path1".to_string())),
                    None,
                )),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/path2".to_string())),
                    None,
                )),
            }),
        ];

        let partitions = HttpTableProvider::extract_partitions(&filters, &base_url);

        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(Some("/path1".to_string()), None)));
        assert!(partitions.contains(&(Some("/path2".to_string()), None)));
    }

    #[test]
    fn test_url_construction_with_base_path() {
        // Test that path from filter is appended to base URL path
        let base_url = Url::parse("https://api.example.com/v1").expect("valid URL");
        let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

        // Simulate what fetch_and_cache does
        let mut url = provider.base_url.clone();
        let filter_path = "/users";

        let base_path = provider.base_url.path();
        let full_path = if base_path == "/" || base_path.is_empty() {
            filter_path.to_string()
        } else if filter_path.starts_with('/') {
            format!("{}{}", base_path.trim_end_matches('/'), filter_path)
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filter_path)
        };
        url.set_path(&full_path);

        assert_eq!(url.path(), "/v1/users");
        assert_eq!(url.as_str(), "https://api.example.com/v1/users");
    }

    #[test]
    fn test_url_construction_without_base_path() {
        let base_url = Url::parse("https://api.example.com/").expect("valid URL");
        let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

        let mut url = provider.base_url.clone();
        let filter_path = "/singlesearch/shows";

        let base_path = provider.base_url.path();
        let full_path = if base_path == "/" || base_path.is_empty() {
            filter_path.to_string()
        } else if filter_path.starts_with('/') {
            format!("{}{}", base_path.trim_end_matches('/'), filter_path)
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filter_path)
        };
        url.set_path(&full_path);

        assert_eq!(url.path(), "/singlesearch/shows");
        assert_eq!(url.as_str(), "https://api.example.com/singlesearch/shows");
    }

    #[test]
    fn test_url_construction_with_query() {
        let base_url = Url::parse("https://api.example.com/").expect("valid URL");
        let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

        let mut url = provider.base_url.clone();
        let filter_path = "/singlesearch/shows";
        let filter_query = "q=South%20Park";

        let base_path = provider.base_url.path();
        let full_path = if base_path == "/" || base_path.is_empty() {
            filter_path.to_string()
        } else if filter_path.starts_with('/') {
            format!("{}{}", base_path.trim_end_matches('/'), filter_path)
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filter_path)
        };
        url.set_path(&full_path);
        url.set_query(Some(filter_query));

        assert_eq!(
            url.as_str(),
            "https://api.example.com/singlesearch/shows?q=South%20Park"
        );
    }

    #[test]
    fn test_cache_key_generation() {
        let key1 = HttpTableProvider::get_cache_key("/path", Some("query"));
        let key2 = HttpTableProvider::get_cache_key("/path", None);
        let key3 = HttpTableProvider::get_cache_key("/other", Some("query"));

        assert_eq!(key1, "/path?query");
        assert_eq!(key2, "/path?");
        assert_eq!(key3, "/other?query");
        assert_ne!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_base_table_schema() {
        let schema = HttpTableProvider::base_table_schema();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "path");
        assert_eq!(schema.field(1).name(), "query");
        assert_eq!(schema.field(2).name(), "content");
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert!(!schema.field(0).is_nullable()); // path is not nullable
        assert!(schema.field(1).is_nullable()); // query is nullable
        assert!(!schema.field(2).is_nullable()); // content is not nullable
    }
}
