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
    //! The tool defers its read-only posture to the calling principal's
    //! `read_write` group (the same gate `/v1/sql` and `function_tool`
    //! use). These tests exercise the three cases that matter:
    //!
    //!   * `ReadWrite` principal → INSERT succeeds.
    //!   * `ReadOnly` principal  → INSERT rejected with "read-only SQL context".
    //!   * No principal (auth disabled) → INSERT succeeds.
    //!
    //! /v1/tools/* is gated by `require_auth_configured` upstream, so in
    //! practice anonymous callers never reach this code; the third case
    //! exists to document the behavior shared with the other tool
    //! surfaces that consult `current_principal_requires_read_only`.
    use crate::{
        datafusion::{DataFusion, builder::DataFusionBuilder},
        status::RuntimeStatus,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use data_components::arrow::write::MemTable;
    use datafusion::sql::TableReference;
    use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
    use runtime_request_context::{Protocol, RequestContext};
    use runtime_tools::builtin::sql::SqlTool;
    use search::pipeline::QueryEngine;
    use spicepod::component::runtime::ApiKey;
    use std::sync::Arc;
    use tokio::runtime::Handle;

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
        let args = serde_json::json!({
            "query": "INSERT INTO audit (id, name) VALUES (1, 'unit-test')"
        })
        .to_string();
        tool.call(args.as_str()).await
    }

    #[tokio::test]
    async fn read_write_principal_can_insert() {
        let (_df, tool) = build_sql_tool();
        let ctx = context_with_principal(Some(Arc::new(ApiKey::ReadWrite {
            key: "rw-key".into(),
        }) as AuthPrincipalRef));

        let result = ctx.scope(async { run_insert(&tool).await }).await;
        assert!(
            result.is_ok(),
            "INSERT under ReadWrite principal should succeed; got {result:?}"
        );
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
    async fn anonymous_caller_can_insert() {
        // Mirrors deployments that haven't configured `runtime.auth`: no
        // principal is set, the tool surface itself is gated upstream by
        // `require_auth_configured`, but the inner read-only check
        // resolves to `false` (writable) — same behavior as /v1/sql.
        let (_df, tool) = build_sql_tool();
        let ctx = context_with_principal(None);

        let result = ctx.scope(async { run_insert(&tool).await }).await;
        assert!(
            result.is_ok(),
            "INSERT with no principal should succeed; got {result:?}"
        );
    }
}
