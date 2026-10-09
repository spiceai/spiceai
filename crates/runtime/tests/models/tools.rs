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
    use app::{App, AppBuilder};
    use futures::TryStreamExt;
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

    /// A read-only API key, configured next to the read-write [`TEST_API_KEY`] by
    /// [`start_spiced_with_memory_dataset`].
    const TEST_READ_ONLY_API_KEY: &str = "test-mcp-read-only-key";

    /// The writable `memory:store` dataset that the write-access tests write to.
    const MEMORY_DATASET: &str = "memories";

    /// The `sql` tool's error for an INSERT by a read-only API key.
    const READ_ONLY_SQL_REJECTION: &str = "Query execution failed: External error: Failed to execute query: Error during planning: Insert Into operations are not allowed in read-only SQL context.";

    /// The `store_memory` tool's error for a read-only API key.
    const READ_ONLY_STORE_MEMORY_REJECTION: &str = "Failed to store memories: the API key on this request does not allow write access. Retry with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`). See https://spiceai.org/docs/api/auth";

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
    ///
    /// The upstream Spice MCP endpoint requires `runtime.auth`, so the client
    /// sends `mcp_auth_token`. This also exercises the dual-era client path
    /// (`server/discover` first, legacy `initialize` fallback).
    #[tokio::test]
    async fn test_mcp_streamable_http() -> Result<(), anyhow::Error> {
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
                .is_some_and(|name| name == "mcp_from_spiced__get_readiness")),
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
                .is_some_and(|name| name == "mcp_from_spiced__get_readiness")),
            "expected proxied MCP tools from auth-enabled Spice server: {tools_list:?}"
        );

        Ok(())
    }

    const MODERN_PROTOCOL_VERSION: &str = "2026-07-28";

    fn modern_request_meta() -> Value {
        serde_json::json!({
            "io.modelcontextprotocol/protocolVersion": MODERN_PROTOCOL_VERSION,
            "io.modelcontextprotocol/clientInfo": {
                "name": "spice-integration-test",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "io.modelcontextprotocol/clientCapabilities": {},
        })
    }

    fn parse_jsonrpc_body(body: &str) -> anyhow::Result<Value> {
        // rmcp may prefix the stream with an empty priming `data:` event
        // (`id` / `retry`). Skip empty payloads so initialize / tools/list
        // parse the JSON-RPC frame.
        let json_str = body
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .find(|payload| !payload.is_empty())
            .unwrap_or(body);
        serde_json::from_str(json_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON-RPC body '{body}': {e}"))
    }

    async fn post_mcp(
        client: &reqwest::Client,
        http_server_url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> anyhow::Result<reqwest::Response> {
        post_mcp_as(client, http_server_url, TEST_API_KEY, headers, body).await
    }

    /// [`post_mcp`] authenticated with `api_key` instead of [`TEST_API_KEY`].
    async fn post_mcp_as(
        client: &reqwest::Client,
        http_server_url: &str,
        api_key: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> anyhow::Result<reqwest::Response> {
        let mut req = client
            .post(format!("{http_server_url}/v1/mcp"))
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .header("X-API-Key", api_key);
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        Ok(req.json(body).send().await?)
    }

    /// Test the MCP Streamable HTTP server endpoint directly via JSON-RPC,
    /// without going through the rmcp client. This verifies the wire format
    /// (`POST /v1/mcp` with `Accept: application/json, text/event-stream`)
    /// and the full legacy `initialize` session (dual-era): mint
    /// `Mcp-Session-Id`, send `notifications/initialized`, then
    /// `tools/list` on that session.
    #[tokio::test]
    async fn test_mcp_streamable_http_initialize() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_mcp_config(McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        })
        .await
        .expect("Failed to start spiced MCP server");

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
            .header("X-API-Key", TEST_API_KEY)
            .json(&init_body)
            .send()
            .await?;

        assert!(
            resp.status().is_success(),
            "initialize returned non-success status: {}",
            resp.status()
        );
        let session_id = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
            .expect("initialize response missing Mcp-Session-Id header");

        let v = parse_jsonrpc_body(&resp.text().await?)?;
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

        let initialized_body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        });
        let initialized_resp = post_mcp(
            &client,
            &http_server_url,
            &[
                ("MCP-Protocol-Version", protocol_version.as_str()),
                ("mcp-session-id", session_id.as_str()),
            ],
            &initialized_body,
        )
        .await?;
        assert_eq!(
            initialized_resp.status(),
            reqwest::StatusCode::ACCEPTED,
            "notifications/initialized should be HTTP 202, got {}",
            initialized_resp.status()
        );

        let list_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {},
        });
        let list_resp = post_mcp(
            &client,
            &http_server_url,
            &[
                ("MCP-Protocol-Version", protocol_version.as_str()),
                ("mcp-session-id", session_id.as_str()),
            ],
            &list_body,
        )
        .await?;
        let list_status = list_resp.status();
        let list_text = list_resp.text().await?;
        assert!(
            list_status.is_success(),
            "legacy tools/list with Mcp-Session-Id failed: {list_status} body={list_text}"
        );
        let list_json = parse_jsonrpc_body(&list_text)?;
        assert!(
            list_json.get("error").is_none(),
            "session-bound tools/list returned an error: {list_json}"
        );
        let tools = list_json
            .pointer("/result/tools")
            .and_then(Value::as_array)
            .expect("session-bound tools/list missing result.tools");
        assert!(
            tools
                .iter()
                .any(|tool| tool.get("name").and_then(Value::as_str) == Some("get_readiness")),
            "session-bound tools/list should include get_readiness: {tools:?}"
        );

        Ok(())
    }

    /// Modern (`2026-07-28`) `server/discover` — no `initialize`, no session.
    #[tokio::test]
    async fn test_mcp_streamable_http_discover() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_mcp_config(McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        })
        .await
        .expect("Failed to start spiced MCP server");

        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {
                "_meta": modern_request_meta(),
            },
        });

        let resp = post_mcp(
            &client,
            &http_server_url,
            &[
                ("MCP-Protocol-Version", MODERN_PROTOCOL_VERSION),
                ("Mcp-Method", "server/discover"),
            ],
            &body,
        )
        .await?;

        let status = resp.status();
        let minted_session = resp.headers().get("mcp-session-id").cloned();
        let body = resp.text().await?;
        assert!(
            status.is_success(),
            "server/discover returned non-success status: {status} body={body}"
        );
        assert!(
            minted_session.is_none(),
            "2026-07-28 discover must not mint Mcp-Session-Id"
        );

        let v = parse_jsonrpc_body(&body)?;
        let result = v
            .get("result")
            .expect("server/discover response missing 'result'");
        let versions = result
            .get("supportedVersions")
            .and_then(Value::as_array)
            .expect("discover result missing supportedVersions");
        assert!(
            versions
                .iter()
                .any(|v| v.as_str() == Some(MODERN_PROTOCOL_VERSION)),
            "discover must list 2026-07-28: {result}"
        );
        assert!(
            result
                .get("capabilities")
                .and_then(|c| c.get("tools"))
                .is_some(),
            "discover result missing tools capability: {result}"
        );

        Ok(())
    }

    /// `tools/list` and `tools/call` succeed without a prior `initialize`.
    #[tokio::test]
    async fn test_mcp_streamable_http_tools_without_initialize() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_mcp_config(McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        })
        .await
        .expect("Failed to start spiced MCP server");

        let client = reqwest::Client::new();
        let list_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {
                "_meta": modern_request_meta(),
            },
        });

        let list_resp = post_mcp(
            &client,
            &http_server_url,
            &[
                ("MCP-Protocol-Version", MODERN_PROTOCOL_VERSION),
                ("Mcp-Method", "tools/list"),
            ],
            &list_body,
        )
        .await?;
        assert!(
            list_resp.status().is_success(),
            "tools/list without initialize failed: {}",
            list_resp.status()
        );
        let list_json = parse_jsonrpc_body(&list_resp.text().await?)?;
        let tools = list_json
            .pointer("/result/tools")
            .and_then(Value::as_array)
            .expect("tools/list missing result.tools");
        assert!(
            tools
                .iter()
                .any(|t| t.get("name").and_then(Value::as_str) == Some("get_readiness")),
            "tools/list should include get_readiness: {tools:?}"
        );

        let call_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "get_readiness",
                "arguments": {},
                "_meta": modern_request_meta(),
            },
        });
        let call_resp = post_mcp(
            &client,
            &http_server_url,
            &[
                ("MCP-Protocol-Version", MODERN_PROTOCOL_VERSION),
                ("Mcp-Method", "tools/call"),
                ("Mcp-Name", "get_readiness"),
            ],
            &call_body,
        )
        .await?;
        assert!(
            call_resp.status().is_success(),
            "tools/call without initialize failed: {}",
            call_resp.status()
        );
        let call_json = parse_jsonrpc_body(&call_resp.text().await?)?;
        assert!(
            call_json.get("result").is_some(),
            "tools/call missing result: {call_json}"
        );

        Ok(())
    }

    /// Header/body mismatches on modern Streamable HTTP must be rejected.
    #[tokio::test]
    async fn test_mcp_streamable_http_header_mismatch() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_mcp_config(McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        })
        .await
        .expect("Failed to start spiced MCP server");

        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {
                "_meta": modern_request_meta(),
            },
        });

        let resp = post_mcp(
            &client,
            &http_server_url,
            &[
                ("MCP-Protocol-Version", MODERN_PROTOCOL_VERSION),
                ("Mcp-Method", "tools/call"),
            ],
            &body,
        )
        .await?;

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "header/body method mismatch should be HTTP 400, got {}",
            resp.status()
        );
        let v = parse_jsonrpc_body(&resp.text().await?)?;
        let code = v.pointer("/error/code").and_then(Value::as_i64);
        assert_eq!(
            code,
            Some(-32020),
            "expected HeaderMismatch (-32020), got {v}"
        );

        Ok(())
    }

    /// An unknown protocol version must return `UnsupportedProtocolVersionError` (-32022)
    /// listing the versions Spice supports.
    #[tokio::test]
    async fn test_mcp_unsupported_protocol_version() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_mcp_config(McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        })
        .await
        .expect("Failed to start spiced MCP server");

        let client = reqwest::Client::new();
        let mut meta = modern_request_meta();
        meta["io.modelcontextprotocol/protocolVersion"] = Value::String("1900-01-01".to_string());
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": { "_meta": meta },
        });

        let resp = post_mcp(
            &client,
            &http_server_url,
            &[
                ("MCP-Protocol-Version", "1900-01-01"),
                ("Mcp-Method", "server/discover"),
            ],
            &body,
        )
        .await?;

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "unsupported protocol version should be HTTP 400, got {}",
            resp.status()
        );
        let v = parse_jsonrpc_body(&resp.text().await?)?;
        let error = v.get("error").expect("missing JSON-RPC error");
        assert_eq!(error.get("code").and_then(Value::as_i64), Some(-32022));
        let supported = error
            .pointer("/data/supported")
            .and_then(Value::as_array)
            .expect("UnsupportedProtocolVersionError must list supported versions");
        assert!(
            supported
                .iter()
                .any(|v| v.as_str() == Some(MODERN_PROTOCOL_VERSION)),
            "supported versions should include 2026-07-28: {error}"
        );

        Ok(())
    }

    /// `/v1/mcp` requires `runtime.auth`. A modern request without credentials is 401.
    #[tokio::test]
    async fn test_mcp_streamable_http_requires_auth() -> Result<(), anyhow::Error> {
        let http_server_url = start_spiced_with_mcp_config(McpConfig {
            allowed_hosts: Some(vec!["*".to_string()]),
        })
        .await
        .expect("Failed to start spiced MCP server");

        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {
                "_meta": modern_request_meta(),
            },
        });

        let resp = client
            .post(format!("{http_server_url}/v1/mcp"))
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .header("MCP-Protocol-Version", MODERN_PROTOCOL_VERSION)
            .header("Mcp-Method", "server/discover")
            .json(&body)
            .send()
            .await?;

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "modern request without credentials should be 401, got {}",
            resp.status()
        );

        Ok(())
    }

    /// `tools/call` runs as the API key on the request, as `/v1/sql` does: a
    /// read-only key is refused the `sql` INSERT and `store_memory` writes but can
    /// still read, and a read-write key's writes land.
    ///
    /// rmcp runs the tool on a worker task, outside the HTTP request's scope. If
    /// the authenticated principal does not reach that task, the call looks
    /// unauthenticated and a read-only key can write.
    #[tokio::test]
    async fn test_mcp_tools_call_enforces_api_key_write_access() -> Result<(), anyhow::Error> {
        let (http_server_url, rt) = start_spiced_with_memory_dataset().await?;
        let client = reqwest::Client::new();

        let denied = call_tool_modern(
            &client,
            &http_server_url,
            TEST_READ_ONLY_API_KEY,
            1,
            "sql",
            serde_json::json!({ "query": insert_memory_sql("sql insert by read-only key") }),
        )
        .await?;
        assert_read_only_rejection(&denied, READ_ONLY_SQL_REJECTION);

        let denied = call_tool_modern(
            &client,
            &http_server_url,
            TEST_READ_ONLY_API_KEY,
            2,
            "store_memory",
            serde_json::json!({ "thoughts": ["store_memory by read-only key"] }),
        )
        .await?;
        assert_read_only_rejection(&denied, READ_ONLY_STORE_MEMORY_REJECTION);

        assert_eq!(
            memory_values(&rt).await?,
            Vec::<String>::new(),
            "a read-only key's MCP writes must not land"
        );

        let inserted = call_tool_modern(
            &client,
            &http_server_url,
            TEST_API_KEY,
            3,
            "sql",
            serde_json::json!({ "query": insert_memory_sql("sql insert by read-write key") }),
        )
        .await?;
        assert_eq!(
            sql_tool_rows(&inserted)?,
            serde_json::json!([{ "count": 1 }])
        );

        let stored = call_tool_modern(
            &client,
            &http_server_url,
            TEST_API_KEY,
            4,
            "store_memory",
            serde_json::json!({ "thoughts": ["store_memory by read-write key"] }),
        )
        .await?;
        assert_eq!(tool_result_text(&stored)?, "null");

        assert_eq!(
            memory_values(&rt).await?,
            vec![
                "sql insert by read-write key".to_string(),
                "store_memory by read-write key".to_string(),
            ],
            "a read-write key's MCP writes must land"
        );

        let read = call_tool_modern(
            &client,
            &http_server_url,
            TEST_READ_ONLY_API_KEY,
            5,
            "sql",
            serde_json::json!({
                "query": format!("SELECT value FROM {MEMORY_DATASET} ORDER BY value")
            }),
        )
        .await?;
        assert_eq!(
            sql_tool_rows(&read)?,
            serde_json::json!([
                { "value": "sql insert by read-write key" },
                { "value": "store_memory by read-write key" },
            ]),
            "a read-only key must still read through MCP"
        );

        Ok(())
    }

    /// A legacy (`2025-03-26`) session keeps one rmcp worker across requests, so
    /// the API key that opened it must not carry over: every `tools/call` on the
    /// session runs as the key on that request.
    #[tokio::test]
    async fn test_mcp_legacy_session_enforces_each_request_api_key() -> Result<(), anyhow::Error> {
        let (http_server_url, rt) = start_spiced_with_memory_dataset().await?;
        let client = reqwest::Client::new();
        let session_id = initialize_legacy_session(&client, &http_server_url, TEST_API_KEY).await?;

        let inserted = call_tool_legacy(
            &client,
            &http_server_url,
            TEST_API_KEY,
            &session_id,
            2,
            "sql",
            serde_json::json!({ "query": insert_memory_sql("sql insert by read-write key") }),
        )
        .await?;
        assert_eq!(
            sql_tool_rows(&inserted)?,
            serde_json::json!([{ "count": 1 }])
        );

        let denied = call_tool_legacy(
            &client,
            &http_server_url,
            TEST_READ_ONLY_API_KEY,
            &session_id,
            3,
            "sql",
            serde_json::json!({ "query": insert_memory_sql("sql insert by read-only key") }),
        )
        .await?;
        assert_read_only_rejection(&denied, READ_ONLY_SQL_REJECTION);

        let denied = call_tool_legacy(
            &client,
            &http_server_url,
            TEST_READ_ONLY_API_KEY,
            &session_id,
            4,
            "store_memory",
            serde_json::json!({ "thoughts": ["store_memory by read-only key"] }),
        )
        .await?;
        assert_read_only_rejection(&denied, READ_ONLY_STORE_MEMORY_REJECTION);

        // The read-only calls must not leave the session read-only either.
        let stored = call_tool_legacy(
            &client,
            &http_server_url,
            TEST_API_KEY,
            &session_id,
            5,
            "store_memory",
            serde_json::json!({ "thoughts": ["store_memory by read-write key"] }),
        )
        .await?;
        assert_eq!(tool_result_text(&stored)?, "null");

        assert_eq!(
            memory_values(&rt).await?,
            vec![
                "sql insert by read-write key".to_string(),
                "store_memory by read-write key".to_string(),
            ],
            "only the read-write key's writes may land on a shared session"
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

        let (http_base_url, _rt) = start_spiced_app(app).await?;
        Ok(http_base_url)
    }

    /// Starts a spiced runtime whose `/v1/mcp` accepts the read-write
    /// [`TEST_API_KEY`] and the read-only [`TEST_READ_ONLY_API_KEY`], with a
    /// writable `memory:store` dataset named [`MEMORY_DATASET`] for the `sql` and
    /// `store_memory` tools to write to.
    ///
    /// Returns the HTTP base URL and the runtime, so a test can read the dataset
    /// without going through MCP.
    async fn start_spiced_with_memory_dataset() -> anyhow::Result<(String, Arc<Runtime>)> {
        use spicepod::component::access::AccessMode;
        use spicepod::component::dataset::Dataset;
        use spicepod::component::runtime::Runtime as SpicepodRuntime;

        let runtime_config = SpicepodRuntime {
            mcp: Some(McpConfig {
                allowed_hosts: Some(vec!["*".to_string()]),
            }),
            auth: Some(Auth {
                api_key: Some(ApiKeyAuth {
                    enabled: true,
                    keys: vec![
                        ApiKey::ReadWrite {
                            key: TEST_API_KEY.to_string(),
                        },
                        ApiKey::ReadOnly {
                            key: TEST_READ_ONLY_API_KEY.to_string(),
                        },
                    ],
                }),
            }),
            ..Default::default()
        };
        let mut memories = Dataset::new("memory:store", MEMORY_DATASET);
        memories.access = AccessMode::ReadWrite;
        let app = AppBuilder::new("mcp-write-access-test")
            .with_runtime(runtime_config)
            .with_dataset(memories)
            .build();

        start_spiced_app(app).await
    }

    /// Starts `app` with its servers, waits until its components are ready, and
    /// returns the HTTP base URL and the runtime.
    async fn start_spiced_app(app: App) -> anyhow::Result<(String, Arc<Runtime>)> {
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
            () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                return Err(anyhow::anyhow!("Timed out waiting for components to load"));
            }
            () = Arc::clone(&rt).load_components() => {}
        }

        runtime_ready_check(&rt).await;

        Ok((http_base_url, rt))
    }

    /// Sends a modern (`2026-07-28`, sessionless) `tools/call` as `api_key` and
    /// returns its JSON-RPC response.
    async fn call_tool_modern(
        client: &reqwest::Client,
        http_server_url: &str,
        api_key: &str,
        id: u64,
        tool: &str,
        arguments: Value,
    ) -> anyhow::Result<Value> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": tool,
                "arguments": arguments,
                "_meta": modern_request_meta(),
            },
        });
        let resp = post_mcp_as(
            client,
            http_server_url,
            api_key,
            &[
                ("MCP-Protocol-Version", MODERN_PROTOCOL_VERSION),
                ("Mcp-Method", "tools/call"),
                ("Mcp-Name", tool),
            ],
            &body,
        )
        .await?;
        jsonrpc_response(resp).await
    }

    /// The legacy protocol revision the session tests negotiate with `initialize`.
    const LEGACY_PROTOCOL_VERSION: &str = "2025-03-26";

    /// Opens a legacy (`initialize`) MCP session as `api_key` and returns its
    /// `Mcp-Session-Id`.
    async fn initialize_legacy_session(
        client: &reqwest::Client,
        http_server_url: &str,
        api_key: &str,
    ) -> anyhow::Result<String> {
        let init_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": LEGACY_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "spice-integration-test",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        });
        let resp = post_mcp_as(client, http_server_url, api_key, &[], &init_body).await?;
        let session_id = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "initialize response ({}) has no Mcp-Session-Id header",
                    resp.status()
                )
            })?;
        let init = jsonrpc_response(resp).await?;
        anyhow::ensure!(init.get("result").is_some(), "initialize failed: {init}");

        let initialized = post_mcp_as(
            client,
            http_server_url,
            api_key,
            &[
                ("MCP-Protocol-Version", LEGACY_PROTOCOL_VERSION),
                ("mcp-session-id", session_id.as_str()),
            ],
            &serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        )
        .await?;
        anyhow::ensure!(
            initialized.status() == reqwest::StatusCode::ACCEPTED,
            "notifications/initialized should be HTTP 202, got {}",
            initialized.status()
        );

        Ok(session_id)
    }

    /// Sends a legacy `tools/call` on `session_id` as `api_key` and returns its
    /// JSON-RPC response.
    async fn call_tool_legacy(
        client: &reqwest::Client,
        http_server_url: &str,
        api_key: &str,
        session_id: &str,
        id: u64,
        tool: &str,
        arguments: Value,
    ) -> anyhow::Result<Value> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": { "name": tool, "arguments": arguments },
        });
        let resp = post_mcp_as(
            client,
            http_server_url,
            api_key,
            &[
                ("MCP-Protocol-Version", LEGACY_PROTOCOL_VERSION),
                ("mcp-session-id", session_id),
            ],
            &body,
        )
        .await?;
        jsonrpc_response(resp).await
    }

    /// The JSON-RPC message of a successful HTTP response.
    async fn jsonrpc_response(resp: reqwest::Response) -> anyhow::Result<Value> {
        let status = resp.status();
        let body = resp.text().await?;
        anyhow::ensure!(
            status.is_success(),
            "MCP request failed: {status} body={body}"
        );
        parse_jsonrpc_body(&body)
    }

    /// Asserts that a `tools/call` was refused with `message`: a JSON-RPC
    /// internal error carrying the tool's error.
    fn assert_read_only_rejection(response: &Value, message: &str) {
        assert_eq!(
            response.pointer("/error/code").and_then(Value::as_i64),
            Some(-32603),
            "a read-only API key's write must be refused: {response}"
        );
        assert_eq!(
            response.pointer("/error/message").and_then(Value::as_str),
            Some(message),
            "unexpected refusal: {response}"
        );
    }

    /// The text content of a successful `tools/call`: the tool's result as JSON.
    fn tool_result_text(response: &Value) -> anyhow::Result<&str> {
        anyhow::ensure!(
            response.pointer("/result/isError").and_then(Value::as_bool) != Some(true),
            "tools/call failed: {response}"
        );
        response
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("tools/call returned no text content: {response}"))
    }

    /// The rows a successful `sql` tool call returned. Its result is a JSON
    /// string that holds the rows as a JSON array.
    fn sql_tool_rows(response: &Value) -> anyhow::Result<Value> {
        let rows: String = serde_json::from_str(tool_result_text(response)?)?;
        Ok(serde_json::from_str(&rows)?)
    }

    /// An INSERT of one row whose `id` and `value` are `value` into
    /// [`MEMORY_DATASET`].
    fn insert_memory_sql(value: &str) -> String {
        format!(
            "INSERT INTO {MEMORY_DATASET} (id, value, created_by, created_at) \
             VALUES ('{value}', '{value}', 'mcp-integration-test', to_timestamp_seconds(0))"
        )
    }

    /// The `value` column of [`MEMORY_DATASET`], sorted. Read in-process rather
    /// than over MCP, so the check does not share the path under test.
    async fn memory_values(rt: &Arc<Runtime>) -> anyhow::Result<Vec<String>> {
        let sql = format!("SELECT value FROM {MEMORY_DATASET} ORDER BY value");
        let batches = rt
            .datafusion()
            .query_builder(&sql)
            .build()
            .run()
            .await?
            .data
            .try_collect::<Vec<_>>()
            .await?;
        let mut values = Vec::new();
        for batch in &batches {
            let column = arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8)?;
            let column = column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| anyhow::anyhow!("'value' did not cast to a string column"))?;
            values.extend(
                column
                    .iter()
                    .map(|value| value.unwrap_or_default().to_string()),
            );
        }
        Ok(values)
    }

    /// Returns the runtime (with all components ready) and the base URL of the HTTP server.
    ///
    /// Auth is enabled with [`TEST_API_KEY`] so `/v1/tools` is reachable (the
    /// `require_auth_configured` guard requires auth to be set up).
    async fn start_spiced_with_tools(tools: Vec<Tool>) -> anyhow::Result<String> {
        use spicepod::component::runtime::Runtime as SpicepodRuntime;

        let mut app_builder = AppBuilder::new("mcp-stdio").with_runtime(SpicepodRuntime {
            auth: Some(Auth {
                api_key: Some(ApiKeyAuth {
                    enabled: true,
                    keys: vec![ApiKey::ReadWrite {
                        key: TEST_API_KEY.to_string(),
                    }],
                }),
            }),
            ..Default::default()
        });

        for tool in tools {
            app_builder = app_builder.with_tool(tool);
        }
        let app = app_builder.build();

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
            () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
        headers.insert("x-api-key", HeaderValue::from_static(TEST_API_KEY));
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
