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

use crate::{
    init_tracing,
    models::{
        create_api_bindings_config, get_anthropic_model, get_local_model, get_mega_science_dataset,
        get_xai_model, openai::get_openai_model,
    },
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
};
use app::AppBuilder;
use runtime::{Runtime, auth::EndpointAuth};
use std::sync::Arc;

#[tokio::test]
async fn test_ai_udf_basic() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;
            verify_env_secret_exists("SPICE_ANTHROPIC_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let app = AppBuilder::new("ai_udf_test")
                .with_model(get_openai_model("gpt-4o-mini", "gpt-4o-mini"))
                .with_model(get_anthropic_model(
                    "claude-3-5-haiku-latest",
                    "claude-haiku",
                ))
                .build();

            let api_config = create_api_bindings_config();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let rt_ref_copy = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 1: Basic ai() call with specific model (OpenAI)
            let query = "SELECT ai('hi', 'gpt-4o-mini')";
            tracing::info!("Testing: {}", query);
            let result = run_ai_query(&rt, query).await?;
            tracing::info!("✓ Test 1 result (gpt-4o-mini): {}", result);
            assert!(
                !result.is_empty(),
                "Basic ai('hi', 'gpt-4o-mini') should return a response"
            );

            // Test 2: ai() call with specific model (OpenAI)
            let query = "SELECT ai('hi', 'gpt-4o-mini')";
            tracing::info!("Testing: {}", query);
            let result = run_ai_query(&rt, query).await?;
            tracing::info!("✓ Test 2 result (gpt-4o-mini): {}", result);
            assert!(
                !result.is_empty(),
                "ai('hi', 'gpt-4o-mini') should return a response"
            );

            // Test 3: ai() call with column alias (Anthropic)
            let query = r#"SELECT ai('hi', 'claude-haiku') as "claude-haiku""#;
            tracing::info!("Testing: {}", query);
            let result = run_ai_query(&rt, query).await?;
            tracing::info!("✓ Test 3 result (claude-haiku): {}", result);
            assert!(
                !result.is_empty(),
                "ai('hi', 'claude-haiku') with alias should return a response"
            );

            // Test 4: LEFT() function on ai() result
            let query = "SELECT LEFT(ai('hi', 'gpt-4o-mini'), 10)";
            tracing::info!("Testing: {}", query);
            let result = run_ai_query(&rt, query).await?;
            tracing::info!(
                "✓ Test 4 result (LEFT 10 chars): '{}' (length: {})",
                result,
                result.len()
            );
            assert!(
                result.len() <= 10,
                "LEFT(ai(), 10) should return at most 10 characters"
            );

            // Test 5: Multiple ai() calls in single query (OpenAI + Anthropic)
            let query = "SELECT ai('hi', 'gpt-4o-mini'), ai('hi', 'claude-haiku')";
            tracing::info!("Testing: {}", query);
            let results = run_ai_query_multiple(&rt, query).await?;
            tracing::info!("✓ Test 5 results:");
            tracing::info!("  - gpt-4o-mini: {}", results[0]);
            tracing::info!("  - claude-haiku: {}", results[1]);
            assert_eq!(
                results.len(),
                2,
                "Query with two ai() calls should return two columns"
            );
            assert!(
                !results[0].is_empty(),
                "First ai() call (OpenAI) should return a response"
            );
            assert!(
                !results[1].is_empty(),
                "Second ai() call (Anthropic) should return a response"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_ai_udf_with_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;
            verify_env_secret_exists("SPICE_ANTHROPIC_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;
            verify_env_secret_exists("SPICE_XAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let app = AppBuilder::new("ai_udf_test")
                .with_dataset(get_mega_science_dataset(None, None, None))
                .with_model(get_openai_model("gpt-4o-mini", "gpt-4o-mini"))
                .with_model(get_xai_model("grok-4-fast-non-reasoning", "grok-4"))
                .with_model(get_anthropic_model("claude-3-5-haiku-latest", "claude-haiku"))
                .build();

            let api_config = create_api_bindings_config();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let rt_ref_copy = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 6: ai() with dataset - answer questions using three models (OpenAI, xAI, Anthropic)
            let query = r#"SELECT id, question, 
                ai(concat('Answer this question in 10 words or less: ', question), 'gpt-4o-mini') as "openai_answer",
                ai(concat('Answer this question in 10 words or less: ', question), 'grok-4') as "xai_answer",
                ai(concat('Answer this question in 10 words or less: ', question), 'claude-haiku') as "anthropic_answer"
            FROM megascience
            LIMIT 5"#;
            tracing::info!("Testing: AI answering questions from MegaScience dataset with 3 providers (OpenAI, xAI, Anthropic)");
            let results = run_ai_query_multiple(&rt, query).await?;
            tracing::info!("✓ Test 6 results (MegaScience Q&A):");
            tracing::info!("  - Question ID: {}", results[0]);
            tracing::info!("  - Question: {}", results[1]);
            tracing::info!("  - OpenAI answer: {}", results[2]);
            tracing::info!("  - xAI answer: {}", results[3]);
            tracing::info!("  - Anthropic answer: {}", results[4]);
            // Should have 5 columns: id, question, openai_answer, xai_answer, anthropic_answer
            assert_eq!(results.len(), 5, "Query should return 5 columns");
            // Check that AI responses are not empty
            assert!(!results[2].is_empty(), "OpenAI (gpt-4o-mini) answer should not be empty");
            assert!(!results[3].is_empty(), "xAI (grok-4) answer should not be empty");
            assert!(!results[4].is_empty(), "Anthropic (claude-haiku) answer should not be empty");

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_ai_udf_left_truncate() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;
            verify_env_secret_exists("SPICE_ANTHROPIC_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;
            verify_env_secret_exists("SPICE_XAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let app = AppBuilder::new("ai_udf_test")
                .with_model(get_openai_model("gpt-4o-mini", "gpt-4o-mini"))
                .with_model(get_xai_model("grok-4-fast-non-reasoning", "grok-4"))
                .with_model(get_anthropic_model("claude-3-5-haiku-latest", "claude-haiku"))
                .build();

            let api_config = create_api_bindings_config();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let rt_ref_copy = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 7: LEFT() with multiple ai() calls across all providers
            let query = r#"SELECT 
                left(ai('What datasets do you have access to?', 'gpt-4o-mini'), 25) as "openai",
                left(ai('What datasets do you have access to?', 'grok-4'), 25) as "xai",
                left(ai('What datasets do you have access to?', 'claude-haiku'), 25) as "anthropic""#;
            tracing::info!("Testing: LEFT truncation with 3 providers (OpenAI, xAI, Anthropic)");
            let results = run_ai_query_multiple(&rt, query).await?;
            tracing::info!("✓ Test 7 results (LEFT 25 chars):");
            tracing::info!("  - OpenAI: '{}' (length: {})", results[0], results[0].len());
            tracing::info!("  - xAI: '{}' (length: {})", results[1], results[1].len());
            tracing::info!("  - Anthropic: '{}' (length: {})", results[2], results[2].len());
            assert_eq!(results.len(), 3, "Query should return 3 columns");
            let provider_names = ["OpenAI (gpt-4o-mini)", "xAI (grok-4)", "Anthropic (claude-haiku)"];
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    !result.is_empty() && result.chars().count() <= 25,
                    "{} result should be non-empty and <= 25 chars, got: '{}'",
                    provider_names[idx],
                    result
                );
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_ai_udf_with_local_model() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            // Local model test - uses Phi-3.5 mini (has chat template support)
            let app = AppBuilder::new("ai_udf_local_test")
                .with_model(get_local_model(
                    "microsoft/Phi-3.5-mini-instruct",
                    "phi3",
                    "llama3",
                ))
                .build();

            let api_config = create_api_bindings_config();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let rt_ref_copy = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            // Local models take longer to load
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(180)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for local model to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 8: Basic query with local model
            let query = "SELECT ai('Say hello in one word', 'llama3')";
            tracing::info!("Testing: Local model (Phi-3.5-mini)");
            let result = run_ai_query(&rt, query).await?;
            tracing::info!("✓ Test 8 result (Phi-3.5-mini): {}", result);
            assert!(
                !result.is_empty(),
                "Local model (llama3) should return a response, got: '{result}'"
            );

            // Test 9: Verify local model works synchronously
            let query = "SELECT ai('hi', 'llama3'), ai('hello', 'llama3')";
            tracing::info!("Testing: Multiple calls to local model");
            let results = run_ai_query_multiple(&rt, query).await?;
            tracing::info!("✓ Test 9 results (multiple local model calls):");
            tracing::info!("  - First call ('hi'): {}", results[0]);
            tracing::info!("  - Second call ('hello'): {}", results[1]);
            assert_eq!(results.len(), 2, "Query should return 2 columns");
            assert!(
                !results[0].is_empty(),
                "First call to llama3 should return a response"
            );
            assert!(
                !results[1].is_empty(),
                "Second call to llama3 should return a response"
            );

            Ok(())
        })
        .await
}

/// Helper to run a query that returns a single string value
async fn run_ai_query(rt: &Arc<Runtime>, query: &str) -> Result<String, anyhow::Error> {
    use arrow::array::StringArray;
    use futures::TryStreamExt;

    let result = rt.datafusion().query_builder(query).build().run().await?;

    let batches: Vec<_> = result.data.try_collect().await?;

    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Err(anyhow::anyhow!("Query returned no results"));
    }

    let batch = &batches[0];
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("Expected StringArray"))?;

    Ok(col.value(0).to_string())
}

/// Helper to run a query that returns multiple columns
async fn run_ai_query_multiple(
    rt: &Arc<Runtime>,
    query: &str,
) -> Result<Vec<String>, anyhow::Error> {
    use arrow::array::{Array, Int64Array, StringArray};
    use futures::TryStreamExt;

    let result = rt.datafusion().query_builder(query).build().run().await?;

    let batches: Vec<_> = result.data.try_collect().await?;

    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Err(anyhow::anyhow!("Query returned no results"));
    }

    let batch = &batches[0];
    let mut results = Vec::new();

    for i in 0..batch.num_columns() {
        let col = batch.column(i);

        // Handle different column types
        if let Some(str_array) = col.as_any().downcast_ref::<StringArray>() {
            results.push(str_array.value(0).to_string());
        } else if let Some(int_array) = col.as_any().downcast_ref::<Int64Array>() {
            results.push(int_array.value(0).to_string());
        } else {
            // For other types, use Debug format
            results.push(format!("{col:?}"));
        }
    }

    Ok(results)
}
