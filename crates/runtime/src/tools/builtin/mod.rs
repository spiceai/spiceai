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
pub mod catalog;
pub mod get_readiness;

#[cfg(test)]
mod tests {
    //! Read/write gating for the built-in [`runtime_tools::builtin::sql::SqlTool`] tool.
    //!
    //! These tests live in `runtime` (not `runtime-tools`) because they exercise `SqlTool`
    //! against the full [`DataFusion`] backend.
    //!
    //! The tool writes only for a principal in the `write` or `read_write` group:
    //!
    //!   * `ReadWrite` principal → INSERT succeeds.
    //!   * `ReadOnly` principal  → INSERT rejected with "read-only SQL context".
    //!   * No principal (auth disabled) → INSERT rejected with "read-only SQL context",
    //!     including when a model calls the tool from a chat completion or a response.
    use crate::{
        datafusion::{DataFusion, builder::DataFusionBuilder},
        model::{ToolUsingChat, ToolUsingResponses},
        status::RuntimeStatus,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use async_openai::error::OpenAIError;
    use async_openai::types::chat::{
        ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    };
    use async_openai::types::responses::{CreateResponse, Response, ResponseStream};
    use async_trait::async_trait;
    use data_components::arrow::write::MemTable;
    use datafusion::common::TableReference;
    use llms::chat::Chat;
    use llms::chat::nsql::SqlGeneration;
    use llms::responses::Responses;
    use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
    use runtime_query_engine::query_engine::QueryEngine;
    use runtime_request_context::{Protocol, RequestContext};
    use runtime_tools::builtin::sql::SqlTool;
    use serde_json::{Value, json};
    use spicepod::component::runtime::ApiKey;
    use std::sync::Arc;
    use tokio::runtime::Handle;
    use tools::SpiceModelTool;

    const INSERT_SQL: &str = "INSERT INTO audit (id, name) VALUES (1, 'unit-test')";

    /// The `sql` tool's error for an INSERT it runs read-only.
    const READ_ONLY_REJECTION: &str = "Query execution failed: External error: Failed to execute query: Error during planning: Insert Into operations are not allowed in read-only SQL context.";

    /// The tool result a model receives when the `sql` tool rejects its INSERT.
    fn rejected_tool_result() -> String {
        format!("Failed to call the tool sql. An error occurred: {READ_ONLY_REJECTION}")
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn build_sql_tool() -> (Arc<DataFusion>, SqlTool) {
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(crate::dataaccelerator::AcceleratorEngineRegistry::new()),
                Handle::current(),
            )
            .build(),
        );
        df.set_self_ref();

        // Bare table name (not under `runtime.*`) so the writability
        // gate is the only thing being exercised — the validator treats
        // `runtime.*` as a Spice-internal dataset and rejects writes to
        // it regardless of principal role.
        let table_name = TableReference::bare("audit");
        let mem_table = Arc::new(
            MemTable::try_new(test_schema(), vec![]).expect("mem table should be created"),
        );
        df.ctx
            .register_table(table_name.clone(), mem_table)
            .expect("table registered");

        // Mark the table writable so a ReadWrite principal's INSERT
        // reaches the regular validator (which checks per-table
        // writability) instead of being rejected because no writable
        // tables exist.
        df.mark_dataset_writable(&table_name)
            .expect("dataset marked writable");

        let query_engine = Arc::clone(&df) as Arc<dyn QueryEngine>;
        let tool = SqlTool::new(query_engine, None, None, None);
        (df, tool)
    }

    fn context_with_principal(principal: Option<AuthPrincipalRef>) -> Arc<RequestContext> {
        let ctx = Arc::new(RequestContext::builder(Protocol::Http).build());
        if let Some(principal) = principal {
            ctx.set_auth_principal(principal)
                .expect("set_auth_principal");
        }
        ctx
    }

    async fn run_insert(
        tool: &SqlTool,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let args = json!({ "query": INSERT_SQL }).to_string();
        tool.call(args.as_str()).await
    }

    async fn assert_audit_is_empty(df: &DataFusion) {
        let rows = df
            .ctx
            .sql("SELECT count(*) AS rows FROM audit")
            .await
            .expect("plan the count of audit")
            .collect()
            .await
            .expect("count audit");
        datafusion::assert_batches_eq!(
            ["+------+", "| rows |", "+------+", "| 0    |", "+------+"],
            &rows
        );
    }

    /// Asserts the tool reported one inserted row and `audit` now holds
    /// exactly the row `run_insert` wrote.
    async fn assert_one_row_inserted(df: &DataFusion, result: &serde_json::Value) {
        assert_eq!(
            result,
            &serde_json::Value::String(r#"[{"count":1}]"#.to_string())
        );
        let rows = df
            .ctx
            .sql("SELECT id, name FROM audit")
            .await
            .expect("plan the read-back of audit")
            .collect()
            .await
            .expect("read audit back");
        datafusion::assert_batches_eq!(
            [
                "+----+-----------+",
                "| id | name      |",
                "+----+-----------+",
                "| 1  | unit-test |",
                "+----+-----------+",
            ],
            &rows
        );
    }

    #[tokio::test]
    async fn read_write_principal_can_insert() {
        let (df, tool) = build_sql_tool();
        let ctx = context_with_principal(Some(Arc::new(ApiKey::ReadWrite {
            key: "rw-key".into(),
        }) as AuthPrincipalRef));

        let result = ctx
            .scope(async { run_insert(&tool).await })
            .await
            .expect("INSERT under ReadWrite principal should succeed");
        assert_one_row_inserted(&df, &result).await;
    }

    #[tokio::test]
    async fn read_only_principal_is_blocked() {
        let (_df, tool) = build_sql_tool();
        let ctx = context_with_principal(Some(Arc::new(ApiKey::ReadOnly {
            key: "ro-key".into(),
        }) as AuthPrincipalRef));

        let result = ctx.scope(async { run_insert(&tool).await }).await;
        let err = result.expect_err("ReadOnly principal must be blocked from INSERT");
        let msg = err.to_string();
        assert!(
            msg.contains("read-only SQL context"),
            "expected strict read-only error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn no_principal_is_blocked() {
        let (df, tool) = build_sql_tool();
        let ctx = context_with_principal(None);

        let err = ctx
            .scope(async { run_insert(&tool).await })
            .await
            .expect_err("INSERT with no principal must be blocked");
        assert_eq!(err.to_string(), READ_ONLY_REJECTION);
        assert_audit_is_empty(&df).await;
    }

    /// A model that asks the `sql` tool to run [`INSERT_SQL`], then answers once it has a tool
    /// result. Keeps the tool results it receives.
    #[derive(Default)]
    struct InsertingModel {
        tool_results: parking_lot::Mutex<Vec<String>>,
    }

    impl InsertingModel {
        fn tool_results(&self) -> Vec<String> {
            self.tool_results.lock().clone()
        }
    }

    #[async_trait]
    impl Chat for InsertingModel {
        fn as_sql(&self) -> Option<&dyn SqlGeneration> {
            None
        }

        async fn chat_request(
            &self,
            req: CreateChatCompletionRequest,
        ) -> Result<CreateChatCompletionResponse, OpenAIError> {
            let messages = serde_json::to_value(&req.messages).expect("serializable messages");
            let results: Vec<String> = messages
                .as_array()
                .into_iter()
                .flatten()
                .filter(|message| message["role"] == "tool")
                .map(|message| {
                    // The tool loop sends the tool's JSON result as the message text.
                    let content = message["content"].as_str().unwrap_or_default();
                    serde_json::from_str::<Value>(content)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_string))
                        .unwrap_or_else(|| content.to_string())
                })
                .collect();
            let (message, finish_reason) = if results.is_empty() {
                let call = json!({
                    "id": "call_0", "type": "function",
                    "function": { "name": "sql", "arguments": json!({ "query": INSERT_SQL }).to_string() },
                });
                (
                    json!({ "role": "assistant", "tool_calls": [call] }),
                    "tool_calls",
                )
            } else {
                self.tool_results.lock().extend(results);
                (json!({ "role": "assistant", "content": "done" }), "stop")
            };
            Ok(serde_json::from_value(json!({
                "id": "chatcmpl-0", "object": "chat.completion", "created": 0, "model": "inserting",
                "choices": [{ "index": 0, "message": message, "finish_reason": finish_reason }],
            }))
            .expect("a valid chat completion"))
        }

        async fn chat_stream(
            &self,
            _req: CreateChatCompletionRequest,
        ) -> Result<ChatCompletionResponseStream, OpenAIError> {
            Err(OpenAIError::InvalidArgument(
                "InsertingModel answers only non-streamed requests".to_string(),
            ))
        }
    }

    #[async_trait]
    impl Responses for InsertingModel {
        async fn health(&self) -> llms::responses::Result<()> {
            Ok(())
        }

        async fn responses_stream(
            &self,
            _req: CreateResponse,
        ) -> Result<ResponseStream, OpenAIError> {
            Err(OpenAIError::InvalidArgument(
                "InsertingModel answers only non-streamed requests".to_string(),
            ))
        }

        async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
            let input = serde_json::to_value(&req.input).expect("serializable input");
            let results: Vec<String> = input
                .as_array()
                .into_iter()
                .flatten()
                .filter(|item| item["type"] == "function_call_output")
                .map(|item| item["output"].as_str().unwrap_or_default().to_string())
                .collect();
            let item = if results.is_empty() {
                json!({
                    "type": "function_call", "id": "fc_0", "call_id": "call_0", "name": "sql",
                    "arguments": json!({ "query": INSERT_SQL }).to_string(), "status": "completed",
                })
            } else {
                self.tool_results.lock().extend(results);
                json!({
                    "type": "message", "id": "msg_1", "role": "assistant", "status": "completed",
                    "content": [{ "type": "output_text", "text": "done", "annotations": [], "logprobs": null }],
                })
            };
            Ok(serde_json::from_value(json!({
                "id": "resp_0", "object": "response", "created_at": 0, "model": "inserting",
                "status": "completed", "output": [item],
            }))
            .expect("a valid response"))
        }
    }

    #[tokio::test]
    async fn chat_completion_with_no_principal_cannot_insert_through_the_sql_tool() {
        let (df, tool) = build_sql_tool();
        let provider = Arc::new(InsertingModel::default());
        let model = ToolUsingChat::new(
            Arc::clone(&provider) as Arc<dyn Chat>,
            vec![Arc::new(tool) as Arc<dyn SpiceModelTool>],
            None,
        );
        let request: CreateChatCompletionRequest = serde_json::from_value(json!({
            "model": "inserting",
            "messages": [{ "role": "user", "content": "insert a row" }],
        }))
        .expect("a valid chat completion request");

        context_with_principal(None)
            .scope(model.chat_request(request))
            .await
            .expect("the chat completion should finish");

        assert_eq!(provider.tool_results(), [rejected_tool_result()]);
        assert_audit_is_empty(&df).await;
    }

    #[tokio::test]
    async fn response_with_no_principal_cannot_insert_through_the_sql_tool() {
        let (df, tool) = build_sql_tool();
        let provider = Arc::new(InsertingModel::default());
        let model = ToolUsingResponses::new(
            Arc::clone(&provider) as Arc<dyn Responses>,
            vec![],
            vec![Arc::new(tool) as Arc<dyn SpiceModelTool>],
            None,
        );
        let request: CreateResponse = serde_json::from_value(json!({
            "model": "inserting",
            "input": "insert a row",
        }))
        .expect("a valid response request");

        context_with_principal(None)
            .scope(model.responses_request(request))
            .await
            .expect("the response should finish");

        assert_eq!(provider.tool_results(), [rejected_tool_result()]);
        assert_audit_is_empty(&df).await;
    }
}
