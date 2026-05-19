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
        header::{ACCEPT, CONTENT_TYPE, HOST},
    };
    use insta::{assert_json_snapshot, assert_snapshot};
    use runtime::Runtime;
    use runtime::auth::EndpointAuth;
    use serde_json::Value;
    use spicepod::component::runtime::{ApiKey, ApiKeyAuth, Auth, McpConfig};
    use spicepod::component::tool::Tool;
    use std::sync::Arc;
    use test_framework::yaml;

    /// A fixed test API key used when the runtime has auth enabled.
    const TEST_API_KEY: &str = "test-mcp-integration-key";

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

    /// Test that spiced can connect to a Streamable HTTP MCP server, as well as be an MCP server.
    #[tokio::test]
    async fn test_mcp_streamable_http() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_tools(vec![])
            .await
            .expect("Failed to start spiced with tools");

        let tool_yaml = format!("name: mcp_from_spiced\nfrom: mcp:{http_server_url}/v1/mcp");
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

    /// Test that spiced can connect to an auth-enabled Streamable HTTP MCP server using
    /// `params.mcp_auth_token`, which is mounted as `Authorization: Bearer <token>`.
    #[tokio::test]
    async fn test_mcp_streamable_http_with_auth_token() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_mcp_config(McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        })
        .await
        .expect("Failed to start auth-enabled spiced MCP server");

        let tool_yaml = format!(
            "name: mcp_from_spiced\nfrom: mcp:{http_server_url}/v1/mcp\nparams:\n  mcp_auth_token: {TEST_API_KEY}"
        );
        let http_client_url = start_spiced_with_tools(vec![
            yaml::from_str(tool_yaml.as_str())
                .expect("Tool spicepod component is not in expected format"),
        ])
        .await
        .expect("Failed to start spiced with MCP tool");

        let tools_list = call_tool_list(http_client_url.as_str()).await?;
        assert!(
            tools_list.iter().any(|tool| tool
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| name == "mcp_from_spiced/get_readiness")),
            "expected proxied MCP tools from auth-enabled Spice server: {tools_list:?}"
        );

        Ok(())
    }

    /// Test that spiced can connect to an auth-enabled Streamable HTTP MCP server using
    /// custom headers in the same format as the HTTP connector's `http_headers` param.
    #[tokio::test]
    async fn test_mcp_streamable_http_with_custom_headers() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_mcp_config(McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        })
        .await
        .expect("Failed to start auth-enabled spiced MCP server");

        let tool_yaml = format!(
            "name: mcp_from_spiced\nfrom: mcp:{http_server_url}/v1/mcp\nparams:\n  mcp_headers: 'X-API-Key: {TEST_API_KEY}'"
        );
        let http_client_url = start_spiced_with_tools(vec![
            yaml::from_str(tool_yaml.as_str())
                .expect("Tool spicepod component is not in expected format"),
        ])
        .await
        .expect("Failed to start spiced with MCP tool");

        let tools_list = call_tool_list(http_client_url.as_str()).await?;
        assert!(
            tools_list.iter().any(|tool| tool
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| name == "mcp_from_spiced/get_readiness")),
            "expected proxied MCP tools from auth-enabled Spice server: {tools_list:?}"
        );

        Ok(())
    }

    /// Test the MCP Streamable HTTP server endpoint directly via JSON-RPC,
    /// without going through the rmcp client. This verifies the wire format
    /// (`POST /v1/mcp` with `Accept: application/json, text/event-stream`)
    /// and the `initialize` handshake.
    #[tokio::test]
    async fn test_mcp_streamable_http_initialize() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_tools(vec![])
            .await
            .expect("Failed to start spiced with tools");

        let client = reqwest::Client::new();
        // Use a concrete known protocol version rather than `LATEST` so this test
        // documents a specific wire-format contract. rmcp exposes
        // `ProtocolVersion::KNOWN_VERSIONS`; using `V_2025_03_26` keeps the test
        // deterministic while still exercising the server's version negotiation.
        let protocol_version = rmcp::model::ProtocolVersion::V_2025_03_26
            .as_str()
            .to_string();
        let init_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": protocol_version,
                "capabilities": {},
                "clientInfo": {
                    "name": "spice-integration-test",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        });

        let resp = client
            .post(format!("{http_server_url}/v1/mcp"))
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .json(&init_body)
            .send()
            .await?;

        assert!(
            resp.status().is_success(),
            "initialize returned non-success status: {}",
            resp.status()
        );
        assert!(
            resp.headers().get("mcp-session-id").is_some(),
            "initialize response missing Mcp-Session-Id header"
        );

        // Response may be SSE-framed or plain JSON depending on server policy.
        let body = resp.text().await?;
        let json_str = body
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap_or(body.as_str());
        let v: Value = serde_json::from_str(json_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse initialize response '{body}': {e}"))?;
        assert_eq!(v.get("jsonrpc"), Some(&Value::String("2.0".to_string())));
        assert_eq!(v.get("id"), Some(&Value::Number(1.into())));
        let result = v
            .get("result")
            .expect("initialize response missing 'result'");
        assert_eq!(
            result.get("serverInfo").and_then(|s| s.get("name")),
            Some(&Value::String("Spice.ai Open Source".to_string()))
        );
        assert!(
            result
                .get("capabilities")
                .and_then(|c| c.get("tools"))
                .is_some(),
            "initialize result missing tools capability: {result}"
        );

        Ok(())
    }

    /// Test that an MCP request with a Host header matching `runtime.mcp.allowed_hosts` succeeds.
    #[tokio::test]
    async fn test_mcp_allowed_host_accepted() -> Result<(), anyhow::Error> {
        // Restrict allowed hosts to only "spice-test.local"; the actual bind address is
        // 127.0.0.1 so we override the Host header manually.
        let mcp_config = McpConfig {
            allowed_hosts: Some(vec!["spice-test.local".to_string()]),
        };
        let http_server_url = start_spiced_with_mcp_config(mcp_config).await?;

        let client = reqwest::Client::new();
        let protocol_version = rmcp::model::ProtocolVersion::V_2025_03_26
            .as_str()
            .to_string();
        let init_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": protocol_version,
                "capabilities": {},
                "clientInfo": {
                    "name": "spice-integration-test",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        });

        let resp = client
            .post(format!("{http_server_url}/v1/mcp"))
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .header(HOST, "spice-test.local")
            .header("X-API-Key", TEST_API_KEY)
            .json(&init_body)
            .send()
            .await?;

        assert!(
            resp.status().is_success(),
            "Request with allowed Host should succeed, got: {}",
            resp.status()
        );

        Ok(())
    }

    /// Test that an MCP request with a Host header NOT in `runtime.mcp.allowed_hosts` is rejected
    /// with 403 Forbidden.
    #[tokio::test]
    async fn test_mcp_disallowed_host_rejected() -> Result<(), anyhow::Error> {
        // Only allow "spice-test.local"; sending Host: evil.example.com should be rejected.
        let mcp_config = McpConfig {
            allowed_hosts: Some(vec!["spice-test.local".to_string()]),
        };
        let http_server_url = start_spiced_with_mcp_config(mcp_config).await?;

        let client = reqwest::Client::new();
        let init_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": rmcp::model::ProtocolVersion::V_2025_03_26.as_str(),
                "capabilities": {},
                "clientInfo": {
                    "name": "spice-integration-test",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        });

        let resp = client
            .post(format!("{http_server_url}/v1/mcp"))
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .header(HOST, "evil.example.com")
            .header("X-API-Key", TEST_API_KEY)
            .json(&init_body)
            .send()
            .await?;

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::FORBIDDEN,
            "Request with disallowed Host should be rejected with 403 Forbidden"
        );

        Ok(())
    }

    /// Test that setting `allowed_hosts: ["*"]` disables host checking entirely, allowing
    /// any `Host` header value — consistent with how `runtime.cors.allowed_origins: ["*"]` works.
    #[tokio::test]
    async fn test_mcp_wildcard_allowed_hosts_accepts_any() -> Result<(), anyhow::Error> {
        let mcp_config = McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        };
        let http_server_url = start_spiced_with_mcp_config(mcp_config).await?;

        let client = reqwest::Client::new();
        let protocol_version = rmcp::model::ProtocolVersion::V_2025_03_26
            .as_str()
            .to_string();
        let init_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": protocol_version,
                "capabilities": {},
                "clientInfo": {
                    "name": "spice-integration-test",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        });

        // Send with an arbitrary host that would normally be rejected
        let resp = client
            .post(format!("{http_server_url}/v1/mcp"))
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .header(HOST, "arbitrary.host.example.com")
            .header("X-API-Key", TEST_API_KEY)
            .json(&init_body)
            .send()
            .await?;

        assert!(
            resp.status().is_success(),
            "Wildcard allowed_hosts should accept any Host header, got: {}",
            resp.status()
        );

        Ok(())
    }

    /// Starts a spiced runtime with the given [`McpConfig`] and returns its HTTP base URL.
    ///
    /// Auth is enabled with [`TEST_API_KEY`] so that the MCP endpoint is reachable
    /// (the `require_auth_configured` guard requires auth to be set up).
    async fn start_spiced_with_mcp_config(mcp_config: McpConfig) -> anyhow::Result<String> {
        use spicepod::component::runtime::Runtime as SpicepodRuntime;

        let runtime_config = SpicepodRuntime {
            mcp: Some(mcp_config),
            auth: Some(Auth {
                api_key: Some(ApiKeyAuth {
                    enabled: true,
                    keys: vec![ApiKey::ReadWrite {
                        key: TEST_API_KEY.to_string(),
                    }],
                }),
            }),
            ..Default::default()
        };
        let app = AppBuilder::new("mcp-allowed-hosts-test")
            .with_runtime(runtime_config)
            .build();

        let api_config = create_api_bindings_config();
        let http_base_url = format!("http://{}", api_config.http_bind_address);

        let rt = Arc::new(Runtime::builder().with_app(app).build().await);
        let _tracing = init_tracing_with_task_history(Some("integration=debug,info"), &rt);

        let app_arc = rt
            .read_app()
            .await
            .ok_or_else(|| anyhow::anyhow!("App not loaded"))?;
        let endpoint_auth = EndpointAuth::new(rt.secrets(), &app_arc).await;

        let rt_ref_copy = Arc::clone(&rt);
        tokio::spawn(async move {
            Box::pin(rt_ref_copy.start_servers(api_config, None, endpoint_auth)).await
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
