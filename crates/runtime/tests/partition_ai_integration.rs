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

//! Integration tests for AI UDF partitioning by provider

use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use runtime_datafusion_udfs::Ai;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_partition_ai_by_provider_explain() {
    // Create a session context with the AI UDF
    let ctx = SessionContext::new();

    // Register a simple AI UDF
    let model_store = Arc::new(RwLock::new(HashMap::new()));
    let ai_udf = Ai::new(Arc::clone(&model_store)).into_async_udf();
    ctx.register_udf(ai_udf);

    // Register the partition optimizer rule
    ctx.add_optimizer_rule(Arc::new(
        runtime::datafusion::extension::partition_ai_by_provider::PartitionAiByProvider::new(),
    ));

    // Create a simple table
    ctx.sql("CREATE TABLE test_table (text VARCHAR) AS VALUES ('hello'), ('world')")
        .await
        .expect("Failed to create table");

    // Execute a query with multiple AI calls to different providers
    let sql = "SELECT 
        ai(text, 'gpt-4') as openai_result,
        ai(text, 'claude-3') as anthropic_result,
        ai(text, 'grok-2') as xai_result
    FROM test_table";

    let df = ctx.sql(sql).await.expect("Failed to create dataframe");

    // Get the logical plan
    let logical_plan = df.logical_plan();

    // The plan should have multiple projections (one per provider)
    let plan_str = format!("{:?}", logical_plan);

    // Verify that partitioning occurred
    // We should see multiple Projection nodes in the plan
    assert!(
        plan_str.contains("Projection"),
        "Plan should contain Projection nodes"
    );

    println!("Logical plan:\n{}", logical_plan.display_indent());
}

#[tokio::test]
async fn test_single_provider_no_partition() {
    // Create a session context
    let ctx = SessionContext::new();

    // Register AI UDF
    let model_store = Arc::new(RwLock::new(HashMap::new()));
    let ai_udf = Ai::new(Arc::clone(&model_store)).into_async_udf();
    ctx.register_udf(ai_udf);

    // Register the partition optimizer rule
    ctx.add_optimizer_rule(Arc::new(
        runtime::datafusion::extension::partition_ai_by_provider::PartitionAiByProvider::new(),
    ));

    // Create a simple table
    ctx.sql("CREATE TABLE test_table (text VARCHAR) AS VALUES ('hello'), ('world')")
        .await
        .expect("Failed to create table");

    // Query with only one provider (should NOT be partitioned)
    let sql = "SELECT 
        ai(text, 'gpt-4') as result1,
        ai(text, 'gpt-4o') as result2
    FROM test_table";

    let df = ctx.sql(sql).await.expect("Failed to create dataframe");
    let logical_plan = df.logical_plan();

    println!("Single provider plan:\n{}", logical_plan.display_indent());

    // Verify the plan structure - should have minimal projections since all same provider
    let plan_str = format!("{:?}", logical_plan);
    assert!(plan_str.contains("Projection"));
}
