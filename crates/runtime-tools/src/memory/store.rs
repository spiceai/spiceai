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

use arrow::array::RecordBatch;
use async_trait::async_trait;
use runtime_query_engine::query_engine::{QueryEngine, UpdateType};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tracing_futures::Instrument;

use crate::builtin::function_tool::current_principal_requires_read_only;
use crate::utils::parameters;
use app::App;
use tokio::sync::RwLock;
use tools::SpiceModelTool;

use super::{MemoryTableElement, memory_table_name, try_from};

const MAX_MEMORY_THOUGHTS_PER_REQUEST: usize = 128;
const MAX_MEMORY_THOUGHT_BYTES: usize = 4 * 1024;
const MAX_MEMORY_TOTAL_BYTES: usize = 64 * 1024;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct StoreMemoryParams {
    /// A list of details to persist
    thoughts: Vec<String>,
}

fn validate_store_memory_params(
    params: &StoreMemoryParams,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if params.thoughts.is_empty() {
        return Err("At least one thought must be provided".into());
    }

    if params.thoughts.len() > MAX_MEMORY_THOUGHTS_PER_REQUEST {
        return Err(format!(
            "Too many thoughts provided. Maximum allowed per request is {MAX_MEMORY_THOUGHTS_PER_REQUEST}"
        )
        .into());
    }

    let mut total_bytes = 0usize;
    for thought in &params.thoughts {
        let thought_bytes = thought.len();
        if thought_bytes > MAX_MEMORY_THOUGHT_BYTES {
            return Err(format!(
                "Thought exceeds maximum size of {MAX_MEMORY_THOUGHT_BYTES} bytes"
            )
            .into());
        }

        total_bytes += thought_bytes;
        if total_bytes > MAX_MEMORY_TOTAL_BYTES {
            return Err(format!(
                "Combined thoughts exceed maximum size of {MAX_MEMORY_TOTAL_BYTES} bytes"
            )
            .into());
        }
    }

    Ok(())
}

impl From<StoreMemoryParams> for Vec<MemoryTableElement> {
    fn from(val: StoreMemoryParams) -> Self {
        val.thoughts
            .iter()
            .map(|thought| MemoryTableElement {
                id: uuid::Uuid::now_v7(),
                value: thought.clone(),
                created_by: None,
                created_at: chrono::Utc::now().timestamp(),
            })
            .collect()
    }
}

pub struct StoreMemoryTool {
    name: String,
    description: String,
    df: Arc<dyn QueryEngine>,
    app: Arc<RwLock<Option<Arc<App>>>>,
}

impl StoreMemoryTool {
    #[must_use]
    pub fn new(
        df: Arc<dyn QueryEngine>,
        app: Arc<RwLock<Option<Arc<App>>>>,
        name: Option<&str>,
        description: Option<&str>,
    ) -> Self {
        Self {
            df,
            app,
            name: name.unwrap_or("store_memory").to_string(),
            description: description.unwrap_or("Persist short notes ('thoughts') from the current conversation so they can be retrieved in future sessions via `load_memory`. Call this when the user states a durable preference, fact, identity, or instruction worth remembering across conversations; do not record transient query inputs or tool results. Pass `thoughts` as a list of concise strings (each up to 4 KiB, at most 128 entries per call, 64 KiB total).").to_string(),
        }
    }
}

#[async_trait]
impl SpiceModelTool for StoreMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<StoreMemoryParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::store_memory", tool = self.name().to_string(), input = arg);
        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            // Auth gate before table lookup so RO rejection does not depend on app
            // wiring and is covered by unit tests without a live memory dataset.
            // Inside this future so `task_history` records a refusal as an error,
            // like every other failure below.
            if current_principal_requires_read_only().await {
                return Err("Failed to store memories: the API key on this request does not allow write access. Retry with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`). See https://spiceai.org/docs/api/auth".into());
            }
            let table_name = memory_table_name(&self.app).await?;
            let params: StoreMemoryParams = serde_json::from_str(arg).boxed()?;
            validate_store_memory_params(&params)?;

            let elements: Vec<MemoryTableElement> = params.into();
            let batch: RecordBatch = try_from(&elements).boxed()?;

            self.df
                .write_data(&table_name, batch.schema(), vec![batch], UpdateType::Append)
                .await
                .boxed()?;
            Ok(Value::Null)
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_MEMORY_THOUGHT_BYTES, MAX_MEMORY_THOUGHTS_PER_REQUEST, MAX_MEMORY_TOTAL_BYTES,
        StoreMemoryParams, StoreMemoryTool, validate_store_memory_params,
    };
    use app::AppBuilder;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::Schema;
    use async_trait::async_trait;
    use datafusion::common::TableReference;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;
    use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
    use runtime_query_engine::query_engine::{
        QueryEngine, QueryRequest, Result as QueryEngineResult, UpdateType,
    };
    use runtime_request_context::{Protocol, RequestContext as SpiceRequestContext};
    use spicepod::component::dataset::Dataset;
    use spicepod::component::runtime::ApiKey;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::RwLock;
    use tools::SpiceModelTool;

    #[test]
    fn test_store_memory_rejects_empty_thoughts() {
        let params = StoreMemoryParams { thoughts: vec![] };

        let err = validate_store_memory_params(&params).expect_err("must reject empty thoughts");
        assert_eq!(err.to_string(), "At least one thought must be provided");
    }

    #[test]
    fn test_store_memory_rejects_too_many_thoughts() {
        let params = StoreMemoryParams {
            thoughts: vec!["ok".to_string(); MAX_MEMORY_THOUGHTS_PER_REQUEST + 1],
        };

        let err = validate_store_memory_params(&params).expect_err("must reject too many thoughts");
        assert!(
            err.to_string()
                .contains(&MAX_MEMORY_THOUGHTS_PER_REQUEST.to_string())
        );
    }

    #[test]
    fn test_store_memory_rejects_oversized_thought() {
        let params = StoreMemoryParams {
            thoughts: vec!["a".repeat(MAX_MEMORY_THOUGHT_BYTES + 1)],
        };

        let err = validate_store_memory_params(&params)
            .expect_err("must reject oversized individual thought");
        assert!(
            err.to_string()
                .contains(&MAX_MEMORY_THOUGHT_BYTES.to_string())
        );
    }

    #[test]
    fn test_store_memory_rejects_oversized_total_payload() {
        let chunk = "a".repeat(MAX_MEMORY_THOUGHT_BYTES);
        let chunk_count = (MAX_MEMORY_TOTAL_BYTES / MAX_MEMORY_THOUGHT_BYTES) + 1;
        let params = StoreMemoryParams {
            thoughts: vec![chunk; chunk_count],
        };

        let err = validate_store_memory_params(&params)
            .expect_err("must reject oversized combined payload");
        assert!(
            err.to_string()
                .contains(&MAX_MEMORY_TOTAL_BYTES.to_string())
        );
    }

    /// Records `write_data` calls so principal gating can be asserted on the
    /// real [`StoreMemoryTool`] path (not a duplicated predicate).
    struct RecordingQueryEngine {
        session: Arc<SessionContext>,
        write_calls: AtomicU64,
        batches_written: Mutex<Vec<RecordBatch>>,
    }

    impl std::fmt::Debug for RecordingQueryEngine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RecordingQueryEngine")
                .field("write_calls", &self.write_calls.load(Ordering::SeqCst))
                .finish_non_exhaustive()
        }
    }

    impl RecordingQueryEngine {
        fn new() -> Self {
            Self {
                session: Arc::new(SessionContext::new()),
                write_calls: AtomicU64::new(0),
                batches_written: Mutex::new(Vec::new()),
            }
        }

        fn write_calls(&self) -> u64 {
            self.write_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl QueryEngine for RecordingQueryEngine {
        fn session_context(&self) -> &Arc<SessionContext> {
            &self.session
        }

        async fn get_table(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn get_table_sync(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn table_exists(&self, _table_ref: &TableReference) -> bool {
            true
        }

        async fn get_arrow_schema(&self, _table_ref: TableReference) -> QueryEngineResult<Schema> {
            Ok(Schema::empty())
        }

        fn get_user_table_names(&self) -> Vec<TableReference> {
            Vec::new()
        }

        fn get_public_table_names(&self) -> QueryEngineResult<Vec<String>> {
            Ok(Vec::new())
        }

        fn is_writable(&self, _table_ref: &TableReference) -> bool {
            true
        }

        fn is_path_catalog_writable(&self, _table_ref: &TableReference) -> bool {
            true
        }

        async fn execute_query(
            &self,
            _request: QueryRequest,
        ) -> QueryEngineResult<SendableRecordBatchStream> {
            unimplemented!("store_memory principal tests do not execute queries")
        }

        async fn execute_plan(
            &self,
            _plan: LogicalPlan,
        ) -> QueryEngineResult<SendableRecordBatchStream> {
            unimplemented!("store_memory principal tests do not execute plans")
        }

        async fn write_data(
            &self,
            _table_ref: &TableReference,
            _schema: Arc<Schema>,
            data: Vec<RecordBatch>,
            _update_type: UpdateType,
        ) -> QueryEngineResult<()> {
            self.write_calls.fetch_add(1, Ordering::SeqCst);
            self.batches_written
                .lock()
                .expect("batches lock")
                .extend(data);
            Ok(())
        }
    }

    fn spice_ctx_with_api_key(key: &str) -> Arc<SpiceRequestContext> {
        let ctx = Arc::new(SpiceRequestContext::builder(Protocol::Http).build());
        let principal: AuthPrincipalRef = Arc::new(ApiKey::parse_str(key));
        ctx.set_auth_principal(principal)
            .expect("set_auth_principal");
        ctx
    }

    fn store_memory_tool(engine: Arc<RecordingQueryEngine>) -> StoreMemoryTool {
        let app = Arc::new(
            AppBuilder::new("store_memory_auth_test")
                .with_dataset(Dataset::new("memory:memories", "memories"))
                .build(),
        );
        StoreMemoryTool::new(
            engine as Arc<dyn QueryEngine>,
            Arc::new(RwLock::new(Some(app))),
            None,
            None,
        )
    }

    #[tokio::test]
    async fn store_memory_rejects_read_only_principal() {
        let engine = Arc::new(RecordingQueryEngine::new());
        let tool = store_memory_tool(Arc::clone(&engine));
        let ctx = spice_ctx_with_api_key("topsecret123");
        let err = ctx
            .scope(async {
                tool.call(r#"{"thoughts":["remember this"]}"#)
                    .await
                    .expect_err("RO principal must reject store_memory")
            })
            .await;
        assert_eq!(
            err.to_string(),
            "Failed to store memories: the API key on this request does not allow write access. Retry with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`). See https://spiceai.org/docs/api/auth"
        );
        assert_eq!(
            engine.write_calls(),
            0,
            "RO principal must not reach write_data"
        );
    }

    #[tokio::test]
    async fn store_memory_allows_read_write_principal() {
        let engine = Arc::new(RecordingQueryEngine::new());
        let tool = store_memory_tool(Arc::clone(&engine));
        let ctx = spice_ctx_with_api_key("writer456:rw");
        let value = ctx
            .scope(async {
                tool.call(r#"{"thoughts":["remember this"]}"#)
                    .await
                    .expect("RW principal must allow store_memory")
            })
            .await;
        assert_eq!(value, serde_json::Value::Null);
        assert_eq!(
            engine.write_calls(),
            1,
            "RW principal must invoke write_data once"
        );
    }

    /// Records the span and message of every `task_history` ERROR event. The
    /// `runtime.task_history` exporter takes a row's `error_message` from the
    /// first ERROR event on its span, so a failure without one reads as success.
    #[derive(Clone, Default)]
    struct TaskHistoryErrors(Arc<Mutex<Vec<TaskHistoryError>>>);

    /// The name of the span an ERROR event was recorded on, and its message.
    type TaskHistoryError = (Option<String>, String);

    impl TaskHistoryErrors {
        fn recorded(&self) -> Vec<TaskHistoryError> {
            self.0.lock().expect("task_history errors lock").clone()
        }
    }

    impl<S> tracing_subscriber::Layer<S> for TaskHistoryErrors
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != "task_history"
                || *event.metadata().level() != tracing::Level::ERROR
            {
                return;
            }
            let mut message = String::new();
            event.record(&mut MessageVisitor(&mut message));
            let span = ctx.event_span(event).map(|span| span.name().to_string());
            self.0
                .lock()
                .expect("task_history errors lock")
                .push((span, message));
        }
    }

    struct MessageVisitor<'a>(&'a mut String);

    impl tracing::field::Visit for MessageVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                *self.0 = format!("{value:?}");
            }
        }
    }

    #[tokio::test]
    async fn store_memory_read_only_refusal_is_recorded_in_task_history() {
        use tracing_subscriber::layer::SubscriberExt;

        let errors = TaskHistoryErrors::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(errors.clone()));
        let engine = Arc::new(RecordingQueryEngine::new());
        let tool = store_memory_tool(Arc::clone(&engine));
        let err = spice_ctx_with_api_key("topsecret123")
            .scope(async {
                tool.call(r#"{"thoughts":["remember this"]}"#)
                    .await
                    .expect_err("RO principal must reject store_memory")
            })
            .await;
        assert_eq!(
            errors.recorded(),
            vec![(Some("tool_use::store_memory".to_string()), err.to_string())],
            "a refused write must be recorded as an error on its task_history span"
        );
    }
}
