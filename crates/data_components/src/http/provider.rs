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
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    physical_expr::EquivalenceProperties,
    scalar::ScalarValue,
};
use futures::Stream;
use reqwest::{Client, header::CACHE_CONTROL};
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc, time::{Duration, SystemTime}};
use tokio::sync::RwLock;
use std::collections::HashMap;
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
        self.cached_at.elapsed().ok().map_or(false, |elapsed| elapsed < self.max_age)
    }

    fn is_stale_but_revalidatable(&self) -> bool {
        if let Some(stale_duration) = self.stale_while_revalidate {
            if let Ok(elapsed) = self.cached_at.elapsed() {
                return elapsed >= self.max_age && elapsed < self.max_age + stale_duration;
            }
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
    pub fn new(base_url: Url, client: Client, file_format: String, acceleration_enabled: bool) -> Self {
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
    fn extract_path_and_query(filters: &[Expr]) -> (Option<String>, Option<String>) {
        let mut path = None;
        let mut query = None;

        for filter in filters {
            if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter {
                if *op == Operator::Eq {
                    if let Expr::Column(col) = left.as_ref() {
                        if let Expr::Literal(ScalarValue::Utf8(Some(value))) = right.as_ref() {
                            match col.name.as_str() {
                                "path" => path = Some(value.clone()),
                                "query" => query = Some(value.clone()),
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        (path, query)
    }

    fn get_cache_key(&self, path: &str, query: Option<&str>) -> String {
        format!("{}?{}", path, query.unwrap_or(""))
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
                } else if let Some(value) = directive.strip_prefix("stale-while-revalidate=") {
                    if let Ok(seconds) = value.parse::<u64>() {
                        stale_while_revalidate = Some(Duration::from_secs(seconds));
                    }
                }
            }
        }

        (max_age, stale_while_revalidate)
    }

    async fn fetch_and_cache(&self, path: &str, query: Option<&str>) -> Result<String> {
        let mut url = self.base_url.clone();
        url.set_path(path);
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

        if !response.status().is_success() {
            return Err(Error::HttpRequest {
                source: response.error_for_status().expect_err("expected error"),
            });
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
            let cache_key = self.get_cache_key(path, query);
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
        let cache_key = self.get_cache_key(path, query);

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
                tracing::debug!("Returning stale content while revalidating for {}", cache_key);

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
        let (path, query) = Self::extract_path_and_query(filters);

        let projected_schema = project_schema(&self.schema, projection)?;

        Ok(Arc::new(HttpExec::new(
            projected_schema,
            Arc::new(self.clone()),
            path,
            query,
        )))
    }
}

#[derive(Clone)]
pub struct HttpExec {
    projected_schema: SchemaRef,
    provider: Arc<HttpTableProvider>,
    path: Option<String>,
    query: Option<String>,
    properties: PlanProperties,
}

impl HttpExec {
    pub fn new(
        projected_schema: SchemaRef,
        provider: Arc<HttpTableProvider>,
        path: Option<String>,
        query: Option<String>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            projected_schema,
            provider,
            path,
            query,
            properties,
        }
    }

    async fn fetch_and_create_batch(&self, provider: &HttpTableProvider) -> DataFusionResult<RecordBatch> {
        let path = self.path.as_deref().unwrap_or("/");
        let query = self.query.as_deref();

        let content = provider
            .get_content(path, query)
            .await
            .map_err(DataFusionError::from)?;

        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "path" => Ok(Arc::new(StringArray::from(vec![path])) as ArrayRef),
                "query" => {
                    Ok(Arc::new(StringArray::from(vec![query.unwrap_or("")])) as ArrayRef)
                }
                "content" => Ok(Arc::new(StringArray::from(vec![content.as_str()])) as ArrayRef),
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported field name: {}",
                    field.name()
                ))),
            })
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;

        RecordBatch::try_new(Arc::clone(&self.projected_schema), columns)
            .map_err(DataFusionError::from)
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
            "HttpExec: base_url={}, format={}",
            self.provider.base_url, self.provider.file_format
        )
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let exec = Arc::new(self.clone());
        let provider = Arc::clone(&self.provider);
        let stream = futures::stream::once(async move {
            exec.fetch_and_create_batch(&provider).await
        });

        let stream_adapter = RecordBatchStreamAdapter::new(self.projected_schema.clone(), stream);

        Ok(Box::pin(stream_adapter))
    }
}
