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
        json_is_single_row_with_value, normalize_embeddings_response, normalize_search_response,
        send_nsql_request, send_search_request,
    },
    utils::{runtime_ready_check, verify_env_secret_exists},
};

use super::send_embeddings_request;

#[tokio::test]
async fn openai_test_nsql() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    verify_env_secret_exists("SPICE_OPENAI_API_KEY")
        .await
        .map_err(anyhow::Error::msg)?;

    let app = AppBuilder::new("text-to-sql")
        .with_dataset(get_taxi_trips_dataset())
        .with_model(get_openai_model("gpt-4o-mini", "nql"))
        .with_model(get_openai_model("gpt-4o-mini", "nql-2"))
        .build();

    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let rt_ref_copy = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    // example responses for the test query: '[{"count(*)":10}]', '[{"record_count":10}]'

    tracing::info!("/v1/nsql: Ensure default request succeeds");
    let response = send_nsql_request(
        http_base_url.as_str(),
        "how many records in taxi_trips dataset?",
        None,
        Some(false),
        None,
    )
    .await?;

    assert!(
        json_is_single_row_with_value(&response, 10),
        "Expected a single record containing the value 10"
    );

    tracing::info!("/v1/nsql: Ensure model selection works");
    let response = send_nsql_request(
        http_base_url.as_str(),
        "how many records in taxi_trips dataset?",
        Some("nql-2"),
        Some(false),
        None,
    )
    .await?;

    assert!(
        json_is_single_row_with_value(&response, 10),
        "Expected a single record containing the value 10"
    );

    tracing::info!("/v1/nsql: Ensure error when invalid dataset name is provided");
    assert!(send_nsql_request(
        http_base_url.as_str(),
        "how many records in taxi_trips dataset?",
        Some("nql"),
        Some(false),
        Some(vec!["dataset_not_in_spice".to_string()]),
    )
    .await
    .is_err());

    tracing::info!("/v1/nsql: Ensure error when invalid model name is provided");
    assert!(send_nsql_request(
        http_base_url.as_str(),
        "how many records in taxi_trips dataset?",
        Some("model_not_in_spice"),
        Some(false),
        None,
    )
    .await
    .is_err());

    Ok(())
}

#[tokio::test]
async fn openai_test_search() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    verify_env_secret_exists("SPICE_OPENAI_API_KEY")
        .await
        .map_err(anyhow::Error::msg)?;

    let mut ds_tpcds_item = get_tpcds_dataset("item");
    ds_tpcds_item.embeddings = vec![ColumnEmbeddingConfig {
        column: "i_item_desc".to_string(),
        model: "openai_embeddings".to_string(),
        primary_keys: Some(vec!["i_item_sk".to_string()]),
        chunking: None,
    }];

    let app = AppBuilder::new("search_app")
        .with_dataset(ds_tpcds_item)
        // test default embeddings model
        .with_embedding(get_openai_embeddings(
            Option::<String>::None,
            "openai_embeddings",
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
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    tracing::info!("/v1/search: Ensure simple search request succeeds");
    let response = send_search_request(
        http_base_url.as_str(),
        "vehicles and journalists",
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
async fn openai_test_embeddings() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    verify_env_secret_exists("SPICE_OPENAI_API_KEY")
        .await
        .map_err(anyhow::Error::msg)?;

    let app = AppBuilder::new("search_app")
        .with_embedding(get_openai_embeddings(
            Some("text-embedding-3-small"),
            "openai_embeddings",
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
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    let embeddins_test = vec![
        (
            EmbeddingInput::String("The food was delicious and the waiter...".to_string()),
            Some("float"),
            None,
            None,
        ),
        (
            EmbeddingInput::StringArray(vec![
                "The food was delicious".to_string(),
                "and the waiter...".to_string(),
            ]),
            // Some("base64"), Error: 'When encoding_format is base64, use Embeddings::create_base64'
            None,
            Some("test_user_id"),
            Some(256),
        ),
    ];

    let mut test_id = 0;

    for (input, encoding_format, user, dimensions) in embeddins_test {
        test_id += 1;
        let response = send_embeddings_request(
            http_base_url.as_str(),
            "openai_embeddings",
            input,
            encoding_format,
            user,
            dimensions,
        )
        .await?;

        insta::assert_snapshot!(
            format!("embeddings_{}", test_id),
            // OpenAI's embeddings response is not deterministic (values vary for the same input, model version, and parameters) so
            // we normalize the response before snapshotting
            normalize_embeddings_response(response)
        );
    }

    Ok(())
}

fn get_openai_model(model: impl Into<String>, name: impl Into<String>) -> Model {
    let mut model = Model::new(format!("openai:{}", model.into()), name);
    model.params.insert(
        "openai_api_key".to_string(),
        "${ secrets:SPICE_OPENAI_API_KEY }".into(),
    );
    model
}

fn get_openai_embeddings(model: Option<impl Into<String>>, name: impl Into<String>) -> Embeddings {
    let mut embedding = match model {
        Some(model) => Embeddings::new(format!("openai:{}", model.into()), name),
        None => Embeddings::new("openai", name),
    };
    embedding.params.insert(
        "openai_api_key".to_string(),
        "${ secrets:SPICE_OPENAI_API_KEY }".into(),
    );
    embedding
}
