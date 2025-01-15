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

#![allow(clippy::expect_used)]
use super::send_embeddings_request;
use crate::models::{sort_json_keys, sql_to_display, sql_to_single_json_value};
use crate::utils::runtime_ready_check_with_timeout;
use crate::{
    init_tracing, init_tracing_with_task_history,
    models::{
        create_api_bindings_config, get_params_with_secrets_value, get_taxi_trips_dataset,
        get_tpcds_dataset, normalize_chat_completion_response, normalize_embeddings_response,
        send_chat_completions_request,
    },
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
};
use app::AppBuilder;
use arrow::array::RecordBatch;
use async_openai::types::{
    ChatCompletionRequestSystemMessageArgs, ChatCompletionRequestUserMessageArgs,
    CreateChatCompletionRequestArgs, CreateEmbeddingResponse, EmbeddingInput,
};
use chrono::{DateTime, Utc};
use core::time;
use futures::TryStreamExt;
use jsonpath_rust::JsonPath;
use llms::chat::Chat;
use opentelemetry_sdk::trace::TracerProvider;
use runtime::{auth::EndpointAuth, model::try_to_chat_model, Runtime};
use serde_json::json;
use spicepod::component::{
    embeddings::{ColumnEmbeddingConfig, Embeddings},
    model::Model,
};
use std::str::FromStr;
use std::sync::Arc;

pub(crate) struct EmbeddingTestCase<'a> {
    pub input: EmbeddingInput,
    pub encoding_format: Option<&'static str>,
    pub user: Option<&'static str>,
    pub model_name: &'a str,
    pub dimensions: Option<u32>,
    pub test_id: &'static str,
}

/// Handle running a set of test cases against an Embeddings model.
/// This function sets up and runs the server.
pub(crate) async fn run_embedding_tests(
    models: Vec<Embeddings>,
    tests: Vec<EmbeddingTestCase<'_>>,
) -> Result<(), anyhow::Error> {
    let _ = start_runtime_with_embedding(models, None).await?;
    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);

    for EmbeddingTestCase {
        input,
        encoding_format,
        user,
        model_name,
        dimensions,
        test_id,
    } in tests.into_iter()
    {
        let response = send_embeddings_request(
            http_base_url.as_str(),
            model_name,
            input,
            encoding_format,
            user,
            dimensions,
        )
        .await?;

        insta::assert_snapshot!(
            format!("embeddings_{}", test_id),
            normalize_embeddings_response(response)
        );
    }
    Ok(())
}

/// Handle tests for beta embedding criteria. Does not run performance tests needed to satisfy beta criteria.
pub(crate) async fn run_beta_functionality_criteria_test(
    model: Embeddings,
    ready_timeout: time::Duration,
) -> Result<(), anyhow::Error> {
    let model_name = model.name.clone();
    let rt = start_runtime_with_embedding(vec![model], Some(ready_timeout)).await?;

    let tests = vec![
        EmbeddingTestCase {
            input: EmbeddingInput::String("The food was delicious and the waiter...".to_string()),
            model_name: model_name.as_str(),
            encoding_format: Some("float"),
            user: None,
            dimensions: None,
            test_id: "alpha_float",
        },
        EmbeddingTestCase {
            input: EmbeddingInput::String("The food was delicious and the waiter...".to_string()),
            model_name: model_name.as_str(),
            encoding_format: Some("base64"),
            user: None,
            dimensions: None,
            test_id: "alpha_base64",
        },
        EmbeddingTestCase {
            input: EmbeddingInput::StringArray(vec![
                "The food was delicious".to_string(),
                "and the waiter...".to_string(),
            ]),
            encoding_format: None,
            model_name: model_name.as_str(),
            user: None,
            dimensions: Some(256),
            test_id: "alpha_string_array",
        },
        EmbeddingTestCase {
            input: EmbeddingInput::IntegerArray(vec![83, 8251, 2488, 382, 2212, 0]),
            encoding_format: None,
            model_name: model_name.as_str(),
            user: None,
            dimensions: Some(256),
            test_id: "alpha_integer_array",
        },
        EmbeddingTestCase {
            input: EmbeddingInput::ArrayOfIntegerArray(vec![
                vec![17, 1343, 362, 796, 604],
                vec![83, 8251, 2488, 382, 2212, 0],
            ]),
            encoding_format: None,
            model_name: model_name.as_str(),
            user: None,
            dimensions: Some(256),
            test_id: "alpha_array_integer_array",
        },
    ];

    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);

    for EmbeddingTestCase {
        input,
        encoding_format,
        user,
        model_name,
        dimensions,
        test_id,
    } in tests.into_iter()
    {
        let response_raw = send_embeddings_request(
            http_base_url.as_str(),
            model_name,
            input,
            encoding_format,
            Some(test_id),
            dimensions,
        )
        .await?;

        let response: CreateEmbeddingResponse = serde_json::from_value(response_raw.clone())
            .expect(
                format!("Failed to parse response for test {test_id} for model {model_name}.")
                    .as_str(),
            );

        // Beta: Check for usage
        assert!(
            response.usage.prompt_tokens > 0,
            "Prompt tokens in usage should not be empty in response for test {} and model {}.",
            test_id,
            model_name
        );
        assert!(
            response.usage.total_tokens > 0,
            "Total tokens in usage should not be empty in response for test {} and model {}.",
            test_id,
            model_name
        );

        // Beta: Check for tracing
        let q = rt.datafusion()
                .query_builder(
                    format!("SELECT span_id FROM runtime.task_history where task='text_embed' && contains(input, '{test_id}');").as_str()
                )
                .build()
                .run()
                .await?
                .data.try_collect::<Vec<RecordBatch>>().await.expect("");
        assert!(
            q.first().is_some_and(|rb| rb.num_rows() > 0),
            "Embedding request did not create tracing in 'runtime.task_history' for test {} and model {}.", test_id, model_name
        );

        // Beta (TODO): Check for metrics

        // Check consistenct of response.
        insta::assert_snapshot!(test_id, normalize_embeddings_response(response_raw));
    }
    Ok(())
}

async fn start_runtime_with_embedding(
    models: Vec<Embeddings>,
    ready_timeout: Option<std::time::Duration>,
) -> Result<Arc<Runtime>, anyhow::Error> {
    let mut app_builder = AppBuilder::new("embedding_app");

    for m in models.into_iter() {
        app_builder = app_builder.with_embedding(m);
    }
    let app = app_builder.build();

    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let rt_ref_copy = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    match ready_timeout {
        Some(timeout) => runtime_ready_check_with_timeout(&rt, timeout).await,
        None => runtime_ready_check(&rt).await,
    }

    Ok(rt)
}
