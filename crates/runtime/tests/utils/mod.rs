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

use std::{
    fmt::Display,
    future::Future,
    sync::{Arc, LazyLock},
    time::Duration,
};

use opentelemetry::{InstrumentationScope, trace::TracerProvider as _};
use opentelemetry_sdk::{
    runtime::TokioCurrentThread,
    trace::{SdkTracerProvider, span_processor_with_async_runtime::BatchSpanProcessor},
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use runtime::{Runtime, task_history::otel_exporter::TaskHistoryExporter};
use serde::Deserialize;
use spicepod::component::runtime::TaskHistoryCapturedOutput;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::{EnvFilter, Layer, filter, fmt, layer::SubscriberExt};

use arrow::array::RecordBatch;
use chrono::Timelike;
use futures::StreamExt;
use runtime_request_context::{Protocol, RequestContext, UserAgent};

pub(crate) static TEST_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> = LazyLock::new(|| {
    Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str(&format!(
                "spiceci/{}",
                env!("CARGO_PKG_VERSION")
            )))
            .build(),
    )
});

pub(crate) async fn runtime_ready_check(rt: &Runtime) {
    runtime_ready_check_with_timeout(rt, Duration::from_secs(120)).await;
}

pub(crate) async fn runtime_ready_check_with_timeout(rt: &Runtime, duration: Duration) {
    assert!(wait_until_true(duration, || async { rt.status().is_ready() }).await);
}

#[expect(dead_code)]
pub(crate) async fn runtime_ready_check_with_timeout_err(
    rt: &Runtime,
    duration: Duration,
) -> Result<(), ()> {
    if wait_until_true(duration, || async { rt.status().is_ready() }).await {
        Ok(())
    } else {
        Err(())
    }
}

pub(crate) async fn wait_until_true<F, Fut>(max_wait: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait {
        if f().await {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

/// Returns the duration until the next occurrence of the nearest second.
/// Optionally, add an overhead to apply to wait for a bit longer after the nearest second is reached.
#[expect(dead_code)]
pub(crate) fn time_till_second(nearest_second: u32, wait: Option<u32>) -> Duration {
    assert!(
        nearest_second < 60,
        "nearest_second must be between 0 and 59"
    );
    let now_second = chrono::Utc::now().second();
    let modulus = now_second % nearest_second;
    let time_until_nearest = if modulus == 0 {
        0
    } else {
        nearest_second - modulus
    };

    Duration::from_secs(u64::from(time_until_nearest + wait.unwrap_or(0)))
}

#[expect(dead_code)]
pub(crate) async fn verify_env_secret_exists(secret_name: &str) -> Result<(), String> {
    let mut secrets = runtime::secrets::Secrets::new();
    // Will automatically load `env` as the default
    secrets
        .load_from(&[])
        .await
        .map_err(|err| err.to_string())?;

    secrets
        .get_secret(secret_name)
        .await
        .map_err(|err| err.to_string())?
        .ok_or_else(|| format!("Secret {secret_name} not found"))?;

    Ok(())
}

pub(crate) fn test_request_context() -> Arc<RequestContext> {
    Arc::clone(&TEST_REQUEST_CONTEXT)
}

#[expect(dead_code)]
pub(crate) async fn run_query(
    rt: &Arc<Runtime>,
    query: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut result = rt.datafusion().query_builder(query).build().run().await?;

    let mut results: Vec<RecordBatch> = vec![];
    while let Some(batch) = result.data.next().await {
        results.push(batch?);
    }

    Ok(results)
}

#[expect(dead_code)]
pub(crate) fn to_pretty_display(batches: &[RecordBatch]) -> Result<impl Display, anyhow::Error> {
    let pretty = arrow::util::pretty::pretty_format_batches(batches)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;

    Ok(pretty)
}

#[expect(dead_code)]
pub(crate) fn init_tracing_with_task_history(
    default_level: Option<&str>,
    rt: &Runtime,
) -> (DefaultGuard, SdkTracerProvider) {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new("runtime=debug,INFO"),
    };

    let fmt_layer = fmt::layer().with_ansi(true).with_filter(filter);

    let task_history_exporter = TaskHistoryExporter::new(
        rt.datafusion(),
        TaskHistoryCapturedOutput::Truncated,
        None, // min_sql_duration_ms
        spicepod::component::runtime::TaskHistoryCapturedPlan::None,
        None, // min_plan_duration_ms
        None, // scheduler_id - not in cluster mode for tests
    );

    // Tests hang if we don't use TokioCurrentThread here (similar to https://github.com/open-telemetry/opentelemetry-rust/issues/868)
    let processor = BatchSpanProcessor::builder(task_history_exporter, TokioCurrentThread).build();

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .build();

    let scope = InstrumentationScope::builder("task_history")
        .with_version(env!("CARGO_PKG_VERSION"))
        .build();
    let tracer = provider.tracer_with_scope(scope);

    let task_history_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(filter::filter_fn(|metadata| {
            metadata.target() == "task_history"
        }));

    let subscriber = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(task_history_layer);

    let guard = tracing::subscriber::set_default(subscriber);

    (guard, provider)
}

/// Response structure for xAI models list API
#[derive(Debug, Deserialize)]
struct XaiModelsResponse {
    data: Vec<XaiModel>,
}

#[derive(Debug, Deserialize)]
struct XaiModel {
    id: String,
}

/// Response structure for xAI API errors
#[derive(Debug, Deserialize)]
struct XaiErrorResponse {
    error: Option<XaiError>,
}

#[derive(Debug, Deserialize)]
struct XaiError {
    message: String,
    code: Option<String>,
}

/// Verify that a specific model is available from xAI.
/// This calls the xAI models API directly to check if the model exists.
/// Returns Ok(()) if the model is available, Err with a descriptive message otherwise.
#[expect(dead_code)]
pub(crate) async fn verify_xai_model_available(model_id: &str) -> Result<(), String> {
    let api_key = std::env::var("SPICE_XAI_API_KEY")
        .map_err(|_| "SPICE_XAI_API_KEY environment variable not set".to_string())?;

    let client = reqwest::Client::new();
    let url = format!("https://api.x.ai/v1/models/{model_id}");

    let response = client
        .get(&url)
        .header(AUTHORIZATION, format!("Bearer {api_key}"))
        .header(CONTENT_TYPE, "application/json")
        .send()
        .await
        .map_err(|e| format!("Failed to connect to xAI API: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("Failed to read xAI API response: {e}"))?;

    if status.is_success() {
        Ok(())
    } else {
        // Try to parse error response for better error message
        if let Ok(error_resp) = serde_json::from_str::<XaiErrorResponse>(&body)
            && let Some(error) = error_resp.error
        {
            return Err(format!(
                "xAI model '{model_id}' not available: {} (code: {})",
                error.message,
                error.code.unwrap_or_else(|| "unknown".to_string())
            ));
        }
        Err(format!(
            "xAI model '{model_id}' not available (HTTP {status}): {body}"
        ))
    }
}

/// List all available xAI models
#[expect(dead_code)]
pub(crate) async fn list_xai_models() -> Result<Vec<String>, String> {
    let api_key = std::env::var("SPICE_XAI_API_KEY")
        .map_err(|_| "SPICE_XAI_API_KEY environment variable not set".to_string())?;

    let client = reqwest::Client::new();

    let response = client
        .get("https://api.x.ai/v1/models")
        .header(AUTHORIZATION, format!("Bearer {api_key}"))
        .header(CONTENT_TYPE, "application/json")
        .send()
        .await
        .map_err(|e| format!("Failed to connect to xAI API: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("Failed to read xAI API response: {e}"))?;

    if status.is_success() {
        let models: XaiModelsResponse = serde_json::from_str(&body)
            .map_err(|e| format!("Failed to parse xAI models response: {e}. Body: {body}"))?;
        Ok(models.data.into_iter().map(|m| m.id).collect())
    } else {
        if let Ok(error_resp) = serde_json::from_str::<XaiErrorResponse>(&body)
            && let Some(error) = error_resp.error
        {
            return Err(format!(
                "Failed to list xAI models: {} (code: {})",
                error.message,
                error.code.unwrap_or_else(|| "unknown".to_string())
            ));
        }
        Err(format!("Failed to list xAI models (HTTP {status}): {body}"))
    }
}

/// Verify that a specific model is available from `OpenAI`.
#[expect(dead_code)]
pub(crate) async fn verify_openai_model_available(model_id: &str) -> Result<(), String> {
    let api_key = std::env::var("SPICE_OPENAI_API_KEY")
        .map_err(|_| "SPICE_OPENAI_API_KEY environment variable not set".to_string())?;

    let client = reqwest::Client::new();
    let url = format!("https://api.openai.com/v1/models/{model_id}");

    let response = client
        .get(&url)
        .header(AUTHORIZATION, format!("Bearer {api_key}"))
        .header(CONTENT_TYPE, "application/json")
        .send()
        .await
        .map_err(|e| format!("Failed to connect to OpenAI API: {e}"))?;

    let status = response.status();

    if status.is_success() {
        Ok(())
    } else {
        let body = response
            .text()
            .await
            .map_err(|e| format!("Failed to read OpenAI API response: {e}"))?;
        Err(format!(
            "OpenAI model '{model_id}' not available (HTTP {status}): {body}"
        ))
    }
}

/// Anthropic doesn't have a models list API, so we validate using a minimal messages request.
/// This sends a minimal request to check if the model is accessible.
#[expect(dead_code)]
pub(crate) async fn verify_anthropic_model_available(model_id: &str) -> Result<(), String> {
    let api_key = std::env::var("SPICE_ANTHROPIC_API_KEY")
        .map_err(|_| "SPICE_ANTHROPIC_API_KEY environment variable not set".to_string())?;

    let client = reqwest::Client::new();

    // Send a minimal request to check model availability
    let body = serde_json::json!({
        "model": model_id,
        "max_tokens": 1,
        "messages": [{"role": "user", "content": "hi"}]
    });

    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &api_key)
        .header("anthropic-version", "2023-06-01")
        .header(CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .map_err(|e| format!("Failed to connect to Anthropic API: {e}"))?;

    let status = response.status();

    if status.is_success() {
        Ok(())
    } else {
        let body = response
            .text()
            .await
            .map_err(|e| format!("Failed to read Anthropic API response: {e}"))?;

        // Check for specific error types
        if status.as_u16() == 404 {
            return Err(format!(
                "Anthropic model '{model_id}' not found. Verify the model identifier is correct."
            ));
        }

        Err(format!(
            "Anthropic model '{model_id}' not available (HTTP {status}): {body}"
        ))
    }
}

/// Response structure for Google Gemini models API
#[derive(Debug, Deserialize)]
struct GeminiModelResponse {
    name: String,
}

/// Verify that a specific model is available from Google Gemini.
/// This calls the Google Generative AI models API to check if the model exists.
#[expect(dead_code)]
pub(crate) async fn verify_google_model_available(model_id: &str) -> Result<(), String> {
    let api_key = std::env::var("SPICE_GOOGLE_API_KEY")
        .map_err(|_| "SPICE_GOOGLE_API_KEY environment variable not set".to_string())?;

    let client = reqwest::Client::new();
    // Google Gemini models API uses the format: models/{model_id}
    let url =
        format!("https://generativelanguage.googleapis.com/v1beta/models/{model_id}?key={api_key}");

    let response = client
        .get(&url)
        .header(CONTENT_TYPE, "application/json")
        .send()
        .await
        .map_err(|e| format!("Failed to connect to Google Gemini API: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("Failed to read Google Gemini API response: {e}"))?;

    if status.is_success() {
        // Verify the response contains model info
        if serde_json::from_str::<GeminiModelResponse>(&body).is_ok() {
            Ok(())
        } else {
            Err(format!(
                "Google Gemini model '{model_id}' response was unexpected: {body}"
            ))
        }
    } else {
        Err(format!(
            "Google Gemini model '{model_id}' not available (HTTP {status}): {body}"
        ))
    }
}

/// List available Google Gemini models
#[expect(dead_code)]
pub(crate) async fn list_google_models() -> Result<Vec<String>, String> {
    let api_key = std::env::var("SPICE_GOOGLE_API_KEY")
        .map_err(|_| "SPICE_GOOGLE_API_KEY environment variable not set".to_string())?;

    let client = reqwest::Client::new();
    let url = format!("https://generativelanguage.googleapis.com/v1beta/models?key={api_key}");

    let response = client
        .get(&url)
        .header(CONTENT_TYPE, "application/json")
        .send()
        .await
        .map_err(|e| format!("Failed to connect to Google Gemini API: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("Failed to read Google Gemini API response: {e}"))?;

    if status.is_success() {
        #[derive(Deserialize)]
        struct ModelsResponse {
            models: Vec<GeminiModelResponse>,
        }
        let models: ModelsResponse = serde_json::from_str(&body)
            .map_err(|e| format!("Failed to parse Google Gemini models response: {e}"))?;
        // Extract model names, stripping the "models/" prefix
        Ok(models
            .models
            .into_iter()
            .map(|m| {
                m.name
                    .strip_prefix("models/")
                    .unwrap_or(&m.name)
                    .to_string()
            })
            .collect())
    } else {
        Err(format!(
            "Failed to list Google Gemini models (HTTP {status}): {body}"
        ))
    }
}

/// Verify that a Bedrock model is accessible.
/// Since Bedrock uses AWS SDK authentication, we verify by checking if the model ID
/// matches known Bedrock model patterns.
/// For runtime verification, the actual health check happens when the model is loaded.
#[expect(dead_code)]
pub(crate) fn verify_bedrock_model_available(model_id: &str) -> Result<(), String> {
    // Bedrock model IDs follow specific patterns
    // Examples: amazon.titan-embed-text-v1, anthropic.claude-3-sonnet-20240229-v1:0
    let valid_prefixes = [
        "amazon.",
        "anthropic.",
        "cohere.",
        "meta.",
        "mistral.",
        "ai21.",
        "stability.",
    ];

    // Check if model ID matches any known provider prefix
    if valid_prefixes
        .iter()
        .any(|prefix| model_id.starts_with(prefix))
    {
        // For Bedrock, we rely on AWS SDK authentication at runtime
        // Here we just validate the format is correct
        Ok(())
    } else {
        Err(format!(
            "Bedrock model '{model_id}' does not match known model ID patterns. \
             Expected format: <provider>.<model-name>[-version] \
             (e.g., amazon.titan-embed-text-v1, anthropic.claude-3-sonnet-20240229-v1:0)"
        ))
    }
}

/// Verify models from multiple providers in parallel, failing fast if any model is unavailable.
/// Returns Ok(()) if all models are available, or an error listing all unavailable models.
#[expect(dead_code)]
pub(crate) async fn verify_models_available(
    models: &[(&str, &str)], // Vec of (provider, model_id) tuples
) -> Result<(), String> {
    use futures::future::join_all;

    let futures: Vec<_> = models
        .iter()
        .map(|(provider, model_id)| async move {
            let result = match *provider {
                "openai" => verify_openai_model_available(model_id).await,
                "anthropic" => verify_anthropic_model_available(model_id).await,
                "xai" => verify_xai_model_available(model_id).await,
                "google" | "gemini" => verify_google_model_available(model_id).await,
                "bedrock" => verify_bedrock_model_available(model_id),
                _ => Err(format!("Unknown provider: {provider}")),
            };
            (format!("{provider}:{model_id}"), result)
        })
        .collect();

    let results = join_all(futures).await;
    let errors: Vec<String> = results
        .into_iter()
        .filter_map(|(model, result)| result.err().map(|e| format!("{model}: {e}")))
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "The following models are not available:\n{}",
            errors.join("\n")
        ))
    }
}

/// Helper struct for building model verification lists
#[expect(dead_code)]
pub struct ModelVerificationBuilder {
    models: Vec<(String, String)>,
}

#[expect(dead_code)]
impl ModelVerificationBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { models: Vec::new() }
    }

    #[must_use]
    pub fn openai(mut self, model_id: &str) -> Self {
        self.models
            .push(("openai".to_string(), model_id.to_string()));
        self
    }

    #[must_use]
    pub fn anthropic(mut self, model_id: &str) -> Self {
        self.models
            .push(("anthropic".to_string(), model_id.to_string()));
        self
    }

    #[must_use]
    pub fn xai(mut self, model_id: &str) -> Self {
        self.models.push(("xai".to_string(), model_id.to_string()));
        self
    }

    #[must_use]
    pub fn google(mut self, model_id: &str) -> Self {
        self.models
            .push(("google".to_string(), model_id.to_string()));
        self
    }

    #[must_use]
    pub fn bedrock(mut self, model_id: &str) -> Self {
        self.models
            .push(("bedrock".to_string(), model_id.to_string()));
        self
    }

    /// Verify all added models are available
    pub async fn verify(self) -> Result<(), String> {
        let model_refs: Vec<(&str, &str)> = self
            .models
            .iter()
            .map(|(p, m)| (p.as_str(), m.as_str()))
            .collect();
        verify_models_available(&model_refs).await
    }
}

impl Default for ModelVerificationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Registers all external data connectors for integration tests.
///
/// This function must be called before creating a runtime in tests that use
/// connectors from the extracted crates (e.g., duckdb, postgres, mysql, etc.).
/// Without this, the runtime won't be able to find connectors like "duckdb".
///
/// This function is idempotent - calling it multiple times is safe and efficient
/// as the registration only happens once.
///
/// # Example
///
/// ```ignore
/// #[tokio::test]
/// async fn test_duckdb_connector() {
///     register_test_connectors().await;
///     let rt = Runtime::builder().build().await;
///     // Now duckdb connector is available
/// }
/// ```
pub(crate) async fn register_test_connectors() {
    // Simply register all connectors. This is idempotent since the registry
    // is a HashMap and duplicate inserts just overwrite with the same value.
    // We avoid using OnceCell to prevent any potential async synchronization issues.
    do_register_test_connectors().await;
}

async fn do_register_test_connectors() {
    use runtime::dataconnector::register_connector_factory;

    tracing::debug!("Starting connector registration for tests");

    // Register all connectors - dev-dependencies are always compiled regardless of features
    register_connector_factory(
        connector_clickhouse::CONNECTOR_NAME,
        connector_clickhouse::factory(),
    )
    .await;
    register_connector_factory(
        connector_databricks::CONNECTOR_NAME,
        connector_databricks::factory(),
    )
    .await;
    register_connector_factory(
        connector_delta_lake::CONNECTOR_NAME,
        connector_delta_lake::factory(),
    )
    .await;
    register_connector_factory(
        connector_dremio::CONNECTOR_NAME,
        connector_dremio::factory(),
    )
    .await;
    register_connector_factory(
        connector_duckdb::CONNECTOR_NAME,
        connector_duckdb::factory(),
    )
    .await;
    register_connector_factory(
        connector_flightsql::CONNECTOR_NAME,
        connector_flightsql::factory(),
    )
    .await;
    register_connector_factory(connector_ftp::CONNECTOR_NAME, connector_ftp::factory()).await;
    register_connector_factory(
        connector_graphql::CONNECTOR_NAME,
        connector_graphql::factory(),
    )
    .await;
    register_connector_factory(connector_imap::CONNECTOR_NAME, connector_imap::factory()).await;
    register_connector_factory(
        connector_mongodb::CONNECTOR_NAME,
        connector_mongodb::factory(),
    )
    .await;
    register_connector_factory(connector_mssql::CONNECTOR_NAME, connector_mssql::factory()).await;
    register_connector_factory(connector_mysql::CONNECTOR_NAME, connector_mysql::factory()).await;
    // Note: connector-odbc is not registered here because it requires the unixODBC system library
    // ODBC tests should use feature gates and run in environments with ODBC installed
    register_connector_factory(
        connector_oracle::CONNECTOR_NAME,
        connector_oracle::factory(),
    )
    .await;
    register_connector_factory(
        connector_postgres::CONNECTOR_NAME,
        connector_postgres::factory(),
    )
    .await;
    register_connector_factory(
        connector_scylladb::CONNECTOR_NAME,
        connector_scylladb::factory(),
    )
    .await;
    register_connector_factory(connector_sftp::CONNECTOR_NAME, connector_sftp::factory()).await;
    register_connector_factory(
        connector_sharepoint::CONNECTOR_NAME,
        connector_sharepoint::factory(),
    )
    .await;
    register_connector_factory(connector_smb::CONNECTOR_NAME, connector_smb::factory()).await;
    register_connector_factory(
        connector_snowflake::CONNECTOR_NAME,
        connector_snowflake::factory(),
    )
    .await;
    register_connector_factory(connector_spark::CONNECTOR_NAME, connector_spark::factory()).await;

    tracing::debug!("Completed connector registration for tests");
}
