use anyhow::Error;
use app::AppBuilder;
use insta::assert_json_snapshot;
use reqwest::Client;
use runtime::{Runtime, auth::EndpointAuth, status::ComponentStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::{
    init_tracing,
    models::{create_api_bindings_config, openai::get_openai_model},
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct OpenAIModel {
    id: String,

    object: String,

    owned_by: String,

    datasets: Option<Vec<String>>,

    status: Option<ComponentStatus>,

    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Metadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(crate) struct Metadata {
    pub supports_responses_api: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct OpenAIModelResponse {
    object: String,
    data: Vec<OpenAIModel>,
}

#[tokio::test]
async fn test_models_http_endpoint() -> Result<(), Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let mut responses_model = get_openai_model("gpt-4o-mini", "responses_model");
            responses_model.params.insert(
                "responses_api".to_string(),
                Value::String("enabled".to_string()),
            );

            let mut chat_model = get_openai_model("gpt-4o-mini", "chat_model");
            chat_model.params.insert(
                "responses_api".to_string(),
                Value::String("disabled".to_string()),
            );

            let app = AppBuilder::new("model_endpoint")
                .with_model(responses_model)
                .with_model(chat_model)
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
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let url = format!("{http_base_url}/v1/models");
            let client = Client::new();
            let response = client.get(&url).send().await.map_err(anyhow::Error::from)?;

            assert!(
                response.status().is_success(),
                "Expected 200 OK, got {}",
                response.status()
            );

            let models_response: OpenAIModelResponse =
                response.json().await.map_err(anyhow::Error::from)?;

            assert_json_snapshot!("models_response_no_status_no_metadata", &models_response);

            let url = format!("{http_base_url}/v1/models?status=true");
            let response = client.get(&url).send().await.map_err(anyhow::Error::from)?;

            assert!(
                response.status().is_success(),
                "Expected 200 OK, got {}",
                response.status()
            );

            let models_response: OpenAIModelResponse =
                response.json().await.map_err(anyhow::Error::from)?;

            assert_json_snapshot!("models_response_with_status", &models_response);

            let url = format!("{http_base_url}/v1/models?metadata_fields=supports_responses_api");
            let response = client.get(&url).send().await.map_err(anyhow::Error::from)?;

            assert!(
                response.status().is_success(),
                "Expected 200 OK, got {}",
                response.status()
            );

            let models_response: OpenAIModelResponse =
                response.json().await.map_err(anyhow::Error::from)?;

            assert_json_snapshot!("models_response_with_metadata", &models_response);

            Ok(())
        })
        .await
}
