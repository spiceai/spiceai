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

mod mcp {
    use crate::models::create_api_bindings_config;
    use crate::models::{http_get, sort_json_keys};
    use crate::utils::init_tracing_with_task_history;
    use crate::utils::runtime_ready_check;
    use app::AppBuilder;
    use http::{
        HeaderMap, HeaderValue,
        header::{ACCEPT, CONTENT_TYPE},
    };
    use insta::{assert_json_snapshot, assert_snapshot};
    use runtime::Runtime;
    use runtime::auth::EndpointAuth;
    use serde_json::Value;
    use spicepod::component::tool::Tool;
    use std::sync::Arc;
    use test_framework::yaml;

    /// Test that spiced can run a stdio MCP server.
    #[tokio::test]
    async fn test_mcp_stdio() -> Result<(), anyhow::Error> {
        let tool_yaml = r"
name: mcp_fetch
from: mcp:docker
params:
  mcp_args: run -i --rm mcp/fetch
";
        let http_base_url = start_spiced_with_tools(vec![
            yaml::from_str(tool_yaml).expect("Tool spicepod component is not in expected format"),
        ])
        .await
        .expect("Failed to start spiced with tools");

        let tools_list = call_tool_list(http_base_url.as_str()).await?;

        let mcp_fetch = tools_list
            .into_iter()
            .find(|t| t.get("name") == Some(&Value::String("mcp_fetch/fetch".to_string())))
            .expect("'mcp_fetch' tool not found");

        assert_snapshot!("mcp_fetch_list", mcp_fetch);

        Ok(())
    }

    /// Test that spiced can connect to an SSE MCP server, as well as be an MCP server.
    #[tokio::test]
    async fn test_mcp_sse() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_tools(vec![])
            .await
            .expect("Failed to start spiced with tools");

        let tool_yaml = format!("name: mcp_from_spiced\nfrom: mcp:{http_server_url}/v1/mcp/sse");
        let http_client_url = start_spiced_with_tools(vec![
            yaml::from_str(tool_yaml.as_str())
                .expect("Tool spicepod component is not in expected format"),
        ])
        .await
        .expect("Failed to start spiced with tools");

        let tools_list = call_tool_list(http_client_url.as_str()).await?;
        assert_json_snapshot!("mcp_spiced_list", tools_list);

        Ok(())
    }

    /// Returns the runtime (with all components ready) and the base URL of the HTTP server.
    async fn start_spiced_with_tools(tools: Vec<Tool>) -> anyhow::Result<String> {
        let mut app_builder = AppBuilder::new("mcp-stdio");

        for tool in tools {
            app_builder = app_builder.with_tool(tool);
        }
        let app = app_builder.build();

        let api_config = create_api_bindings_config();
        let http_base_url = format!("http://{}", api_config.http_bind_address);

        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

        let _tracing = init_tracing_with_task_history(Some("integration=debug,info"), &rt);

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

        Ok(http_base_url)
    }

    async fn call_tool_list(base_url: &str) -> anyhow::Result<Vec<Value>> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let Ok(mut values) = http_get(format!("{base_url}/v1/tools").as_str(), headers).await
        else {
            return Err(anyhow::anyhow!("Failed to get tools list"));
        };

        sort_json_keys(&mut values);
        if let Value::Array(mut body) = values {
            body.sort_by_key(|v| {
                v.get("name")
                    .map(|n| n.as_str().unwrap_or_default().to_string())
            });
            Ok(body)
        } else {
            Err(anyhow::anyhow!("Failed to get tools list"))
        }
    }
}
