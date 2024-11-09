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
use async_openai::types::EmbeddingInput;
use runtime::{auth::EndpointAuth, Runtime};
use spicepod::component::{
    embeddings::{ColumnEmbeddingConfig, Embeddings},
    model::Model,
};

use crate::{
    init_tracing,
    models::{
        create_api_bindings_config, get_taxi_trips_dataset, get_tpcds_dataset,
        normalize_embeddings_response, normalize_search_response, send_embeddings_request,
        send_search_request,
    },
    utils::{runtime_ready_check, verify_env_secret_exists},
};

#[tokio::test]
async fn huggingface_search_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    verify_env_secret_exists("SPICE_HF_TOKEN")
        .await
        .map_err(anyhow::Error::msg)?;

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
async fn huggingface_model_download_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    verify_env_secret_exists("SPICE_HF_TOKEN")
        .await
        .map_err(anyhow::Error::msg)?;

    let app = AppBuilder::new("text-to-sql")
        .with_dataset(get_taxi_trips_dataset())
        .with_model(get_huggingface_model(
            "meta-llama/Llama-3.2-1B-Instruct",
            "llama",
            "hf_model",
        ))
        .build();

    let api_config = create_api_bindings_config();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let rt_ref_copy = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        // increased timeout to download and load huggingface model
        () = tokio::time::sleep(std::time::Duration::from_secs(180)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    Ok(())
}

#[tokio::test]
async fn huggingface_embeddings_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    verify_env_secret_exists("SPICE_HF_TOKEN")
        .await
        .map_err(anyhow::Error::msg)?;

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

fn get_huggingface_model(
    model: impl Into<String>,
    model_type: impl Into<String>,
    name: impl Into<String>,
) -> Model {
    let mut model = Model::new(format!("huggingface:huggingface.co/{}", model.into()), name);
    model
        .params
        .insert("hf_token".to_string(), "${ secrets:SPICE_HF_TOKEN }".into());
    model
        .params
        .insert("model_type".to_string(), model_type.into().into());

    model
}

fn get_huggingface_embeddings(model: impl Into<String>, name: impl Into<String>) -> Embeddings {
    let mut embeddings =
        Embeddings::new(format!("huggingface:huggingface.co/{}", model.into()), name);
    embeddings
        .params
        .insert("hf_token".to_string(), "${ secrets:SPICE_HF_TOKEN }".into());

    embeddings
}
