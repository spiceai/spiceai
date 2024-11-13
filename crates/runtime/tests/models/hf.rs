/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use app::AppBuilder;
use async_openai::types::{
    ChatCompletionRequestSystemMessageArgs, ChatCompletionRequestUserMessageArgs,
    CreateChatCompletionRequestArgs, EmbeddingInput,
};
use llms::chat::create_hf_model;
use runtime::{
    auth::EndpointAuth,
    model::ToolUsingChat,
    tools::{options::SpiceToolsOptions, utils::get_tools},
    Runtime,
};
use spicepod::component::{
    embeddings::{ColumnEmbeddingConfig, Embeddings},
    model::Model,
};

use llms::chat::Chat;

use crate::{
    init_tracing,
    models::{
        create_api_bindings_config, get_taxi_trips_dataset, get_tpcds_dataset,
        json_is_single_row_with_value, normalize_chat_completion_response,
        normalize_embeddings_response, normalize_search_response, send_chat_completions_request,
        send_embeddings_request, send_nsql_request, send_search_request,
    },
    utils::runtime_ready_check,
};

const HF_TEST_MODEL: &str = "microsoft/Phi-3-mini-4k-instruct";
const HF_TEST_MODEL_TYPE: &str = "phi3";

#[tokio::test]
async fn huggingface_test_search() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    let mut ds_tpcds_item = get_tpcds_dataset("item");
    ds_tpcds_item.embeddings = vec![ColumnEmbeddingConfig {
        column: "i_item_desc".to_string(),
        model: "hf_minilm".to_string(),
        primary_keys: Some(vec!["i_item_sk".to_string()]),
        chunking: None,
    }];

    let app = AppBuilder::new("text-to-sql")
        .with_dataset(ds_tpcds_item)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .build();

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

    runtime_ready_check(&rt).await;

    tracing::info!("/v1/search: Ensure simple search request succeeds");
    let response = send_search_request(
        http_base_url.as_str(),
        "new patient",
        Some(2),
        Some(vec!["item".to_string()]),
        None,
        Some(vec!["i_color".to_string(), "i_item_id".to_string()]),
    )
    .await?;

    insta::assert_snapshot!(format!("search_1"), normalize_search_response(response));

    Ok(())
}

#[tokio::test]
async fn huggingface_test_nsql() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    let app = AppBuilder::new("text-to-sql")
        .with_dataset(get_taxi_trips_dataset())
        .with_model(get_huggingface_model(
            HF_TEST_MODEL,
            HF_TEST_MODEL_TYPE,
            "hf_model",
        ))
        .build();

    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let rt_ref_copy = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        // increased timeout to download and load huggingface model
        () = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    let response = send_nsql_request(
        http_base_url.as_str(),
        "how many records in taxi_trips dataset?",
        Some("hf_model"),
        Some(false),
        None,
    )
    .await?;

    assert!(
        json_is_single_row_with_value(&response, 10),
        "Expected a single record containing the value 10"
    );

    Ok(())
}

#[tokio::test]
async fn huggingface_test_embeddings() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    let app = AppBuilder::new("text-to-sql")
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .with_embedding(get_huggingface_embeddings("intfloat/e5-small-v2", "hf_e5"))
        .build();

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

    runtime_ready_check(&rt).await;

    let embeddins_test = vec![
        (
            "hf_minilm",
            EmbeddingInput::String("The food was delicious and the waiter...".to_string()),
            Some("float"),
            None,
        ),
        (
            "hf_minilm",
            EmbeddingInput::StringArray(vec![
                "The food was delicious".to_string(),
                "and the waiter...".to_string(),
            ]),
            None, // `base64` paramerter is not supported when using local model
            Some(256),
        ),
        (
            "hf_e5",
            EmbeddingInput::String("The food was delicious and the waiter...".to_string()),
            None,
            Some(384),
        ),
    ];

    let mut test_id = 0;

    for (model, input, encoding_format, dimensions) in embeddins_test {
        test_id += 1;
        let response = send_embeddings_request(
            http_base_url.as_str(),
            model,
            input,
            encoding_format,
            None, // `user` parameter is not supported when using local model
            dimensions,
        )
        .await?;

        insta::assert_snapshot!(
            format!("embeddings_{}", test_id),
            // Embeddingsare are not deterministic (values vary for the same input, model version, and parameters) so
            // we normalize the response before snapshotting
            normalize_embeddings_response(response)
        );
    }

    Ok(())
}

#[tokio::test]
async fn huggingface_test_chat_completion() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    let mut model_with_tools = get_huggingface_model(HF_TEST_MODEL, HF_TEST_MODEL_TYPE, "hf_model");
    model_with_tools
        .params
        .insert("tools".to_string(), "auto".into());

    let app = AppBuilder::new("text-to-sql")
        .with_dataset(get_taxi_trips_dataset())
        .with_model(model_with_tools)
        .build();

    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let rt_ref_copy = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        // increased timeout to download and load huggingface model
        () = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    let response = send_chat_completions_request(
        http_base_url.as_str(),
        vec![
            ("system".to_string(), "You are an assistant that responds to queries by providing only the requested data values without extra explanation.".to_string()),
            ("user".to_string(), "Provide the total number of records in the taxi_trips dataset. If known, return a single numeric value.".to_string()),
        ],
        "hf_model",
        false,
    ).await?;

    // Message content verification is disabled due to issue below: model does not use tools and can't provide the expected response.
    // https://github.com/spiceai/spiceai/issues/3426
    insta::assert_snapshot!(
        "chat_completion",
        normalize_chat_completion_response(response, true)
    );

    Ok(())
}

#[tokio::test]
async fn huggingface_test_chat_messages() -> Result<(), anyhow::Error> {
    let model = Arc::new(create_hf_model(
        HF_TEST_MODEL,
        &Some(HF_TEST_MODEL_TYPE.to_string()),
        None,
    )?);

    let app = AppBuilder::new("ai-app")
        .with_dataset(get_taxi_trips_dataset())
        .build();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    let tool_model = Box::new(ToolUsingChat::new(
        Arc::clone(&model),
        Arc::clone(&rt),
        get_tools(Arc::clone(&rt), &SpiceToolsOptions::Auto).await,
        Some(10),
    ));

    let req = CreateChatCompletionRequestArgs::default()
        .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
            .content("You are an assistant that responds to queries by providing only the requested data values without extra explanation.".to_string())
            .build()?
            .into(),ChatCompletionRequestUserMessageArgs::default()
            .content("Provide the total number of records in the taxi trips dataset. If known, return a single numeric value.".to_string())
            .build()?
            .into()])
        .build()?;

    let mut response = tool_model.chat_request(req).await?;

    // Message content verification is disabled due to issue below: model does not use tools and can't provide the expected response.
    // https://github.com/spiceai/spiceai/issues/3426
    response.choices.iter_mut().for_each(|c| {
        c.message.content = Some("__placeholder__".to_string());
    });

    insta::assert_snapshot!("chat_1_choices", format!("{:?}", response.choices));

    Ok(())
}

fn get_huggingface_model(
    model: impl Into<String>,
    model_type: impl Into<String>,
    name: impl Into<String>,
) -> Model {
    let mut model = Model::new(format!("huggingface:huggingface.co/{}", model.into()), name);
    model
        .params
        .insert("model_type".to_string(), model_type.into().into());

    model
}

fn get_huggingface_embeddings(model: impl Into<String>, name: impl Into<String>) -> Embeddings {
    Embeddings::new(format!("huggingface:huggingface.co/{}", model.into()), name)
}
