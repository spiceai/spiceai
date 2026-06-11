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
#![expect(clippy::expect_used, reason = "integration-test helpers")]

//! Integration tests for AI UDF concurrent execution through datafusion SQL.
//!
//! These tests register the AI UDF in a real datafusion `SessionContext`, run SQL
//! queries against an in-memory table, and validate that:
//! 1. Concurrent execution actually happens (not sequential)
//! 2. Rate controller limits are enforced
//! 3. EXPLAIN ANALYZE output includes the AI UDF

#![cfg(feature = "models")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
};
use async_trait::async_trait;
use datafusion::prelude::*;
use governor::Quota;
use llms::chat::Chat;
use runtime_datafusion_udfs::ai::{Ai, ChatModelStore, RateControllerStore};
use runtime_rate_control::RateController;
use tokio::sync::RwLock;

/// Mock Chat implementation that tracks concurrent calls and simulates latency.
struct ConcurrencyTrackingChat {
    name: String,
    concurrent: Arc<AtomicUsize>,
    max_concurrent: Arc<AtomicUsize>,
    delay: Duration,
}

#[async_trait]
impl Chat for ConcurrencyTrackingChat {
    fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
        None
    }

    async fn chat_stream(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        // Track concurrent calls
        let current = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_concurrent.fetch_max(current, Ordering::SeqCst);

        // Simulate LLM latency
        tokio::time::sleep(self.delay).await;

        self.concurrent.fetch_sub(1, Ordering::SeqCst);

        let response_text = format!("Response from {}", self.name);
        Ok(llms::streaming_utils::create_mock_streaming_response(
            self.name.clone(),
            vec![response_text],
            None,
        ))
    }

    async fn chat_request(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        unreachable!("AI UDF uses chat_stream, not chat_request")
    }
}

/// Helper: create a `SessionContext` with the AI UDF registered using the given model
/// store, rate controllers, and an in-memory `prompts` table with `num_rows` rows.
fn setup_ctx(
    model_store: Arc<RwLock<ChatModelStore>>,
    rate_controllers: Arc<RwLock<RateControllerStore>>,
    num_rows: usize,
) -> SessionContext {
    let ctx = SessionContext::new();

    // Register UDF
    let udf = Ai::new(model_store, rate_controllers);
    ctx.register_udf(udf.into_async_udf().into_scalar_udf());

    // Create in-memory table
    let schema = Arc::new(Schema::new(vec![Field::new(
        "question",
        DataType::Utf8,
        false,
    )]));
    let questions: Vec<&str> = (0..num_rows)
        .map(|i| match i % 3 {
            0 => "What is 2+2?",
            1 => "What is the capital of France?",
            _ => "Tell me a joke",
        })
        .collect();
    let array = StringArray::from(questions);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("should create RecordBatch");
    ctx.register_batch("prompts", batch)
        .expect("should register table");

    ctx
}

/// Validates that the AI UDF executes concurrently through a real datafusion SQL query
/// and that the rate controller's concurrency limit is respected.
#[tokio::test]
async fn test_concurrent_execution_enforced_by_rate_controller() {
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    let model: Arc<dyn Chat> = Arc::new(ConcurrencyTrackingChat {
        name: "mock-llm".to_string(),
        concurrent: Arc::clone(&concurrent),
        max_concurrent: Arc::clone(&max_concurrent),
        delay: Duration::from_millis(200),
    });

    let mut model_store: ChatModelStore = HashMap::new();
    model_store.insert("mock-llm".to_string(), model);

    // Rate controller: max 3 concurrent, high RPM
    let rc = RateController::builder()
        .with_max_concurrent_requests(3)
        .add_quota(Quota::per_minute(
            std::num::NonZeroU32::new(10000).expect("non-zero"),
        ))
        .build();
    let mut rc_store: RateControllerStore = HashMap::new();
    rc_store.insert("mock-llm".to_string(), rc);

    let ctx = setup_ctx(
        Arc::new(RwLock::new(model_store)),
        Arc::new(RwLock::new(rc_store)),
        9, // 9 rows
    );

    let start = Instant::now();
    let df = ctx
        .sql("SELECT ai(question, 'mock-llm') as response FROM prompts")
        .await
        .expect("should parse SQL");
    let results: Vec<RecordBatch> = df.collect().await.expect("should execute query");
    let elapsed = start.elapsed();

    // Collect all responses
    let mut all_responses = Vec::new();
    for batch in &results {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("should be StringArray");
        for i in 0..col.len() {
            all_responses.push(col.value(i).to_string());
        }
    }

    // All 9 rows should produce responses
    assert_eq!(
        all_responses.len(),
        9,
        "Expected 9 responses, got {}",
        all_responses.len()
    );
    for (i, resp) in all_responses.iter().enumerate() {
        assert!(
            resp.contains("Response from mock-llm"),
            "Row {i} should contain model response, got: {resp}"
        );
    }

    let observed_max = max_concurrent.load(Ordering::SeqCst);

    // Concurrency should be bounded by rate controller (max 3)
    assert!(
        observed_max <= 3,
        "Rate controller should limit concurrency to 3, but observed max was {observed_max}"
    );

    // Concurrency should actually happen (not sequential)
    assert!(
        observed_max > 1,
        "Expected concurrent execution (max_concurrent > 1), but observed {observed_max}. \
         This suggests the AI UDF is not executing calls concurrently."
    );

    // Timing validation: 9 rows at 200ms each, max 3 concurrent = ~600ms minimum.
    // Sequential would be ~1800ms. Allow generous margin for CI flakiness.
    assert!(
        elapsed < Duration::from_millis(1500),
        "Expected concurrent execution to finish in <1500ms, took {}ms. \
         This suggests calls ran sequentially.",
        elapsed.as_millis()
    );
}

/// Validates that EXPLAIN ANALYZE output references the AI UDF and shows execution metrics.
#[tokio::test]
async fn test_explain_analyze_shows_ai_udf() {
    let model: Arc<dyn Chat> = Arc::new(ConcurrencyTrackingChat {
        name: "explain-model".to_string(),
        concurrent: Arc::new(AtomicUsize::new(0)),
        max_concurrent: Arc::new(AtomicUsize::new(0)),
        delay: Duration::from_millis(50),
    });

    let mut model_store: ChatModelStore = HashMap::new();
    model_store.insert("explain-model".to_string(), model);

    let ctx = setup_ctx(
        Arc::new(RwLock::new(model_store)),
        Arc::new(RwLock::new(HashMap::new())), // no rate controller
        3,                                     // 3 rows
    );

    let df = ctx
        .sql("EXPLAIN ANALYZE SELECT ai(question, 'explain-model') as response FROM prompts")
        .await
        .expect("should parse EXPLAIN ANALYZE");
    let results: Vec<RecordBatch> = df.collect().await.expect("should execute explain analyze");

    // Concatenate all plan text from the "plan" column (column index 1)
    let mut plan_text = String::new();
    for batch in &results {
        let plan_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("should be StringArray");
        for i in 0..plan_col.len() {
            plan_text.push_str(plan_col.value(i));
            plan_text.push('\n');
        }
    }

    // Plan should reference the ai() function
    assert!(
        plan_text.contains("ai("),
        "EXPLAIN ANALYZE plan should reference ai() UDF.\nPlan:\n{plan_text}"
    );

    // ANALYZE should show execution metrics (rows produced, elapsed time, etc.)
    assert!(
        plan_text.contains("metrics=") || plan_text.contains("rows="),
        "EXPLAIN ANALYZE should contain execution metrics.\nPlan:\n{plan_text}"
    );
}

/// Validates that without a rate controller, the UDF still executes correctly
/// and uses datafusion's `target_partitions` as the parallelism fallback.
#[tokio::test]
async fn test_no_rate_controller_still_executes() {
    let model: Arc<dyn Chat> = Arc::new(ConcurrencyTrackingChat {
        name: "no-rc-model".to_string(),
        concurrent: Arc::new(AtomicUsize::new(0)),
        max_concurrent: Arc::new(AtomicUsize::new(0)),
        delay: Duration::from_millis(50),
    });

    let mut model_store: ChatModelStore = HashMap::new();
    model_store.insert("no-rc-model".to_string(), model);

    // No rate controllers registered
    let ctx = setup_ctx(
        Arc::new(RwLock::new(model_store)),
        Arc::new(RwLock::new(HashMap::new())),
        5,
    );

    let df = ctx
        .sql("SELECT ai(question, 'no-rc-model') as response FROM prompts")
        .await
        .expect("should parse SQL");
    let results: Vec<RecordBatch> = df.collect().await.expect("should execute query");

    let mut count = 0;
    for batch in &results {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("should be StringArray");
        for i in 0..col.len() {
            assert!(col.value(i).contains("Response from no-rc-model"));
            count += 1;
        }
    }
    assert_eq!(count, 5, "All 5 rows should produce responses");
}
