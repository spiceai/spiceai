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

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use std::sync::Arc;

use app::AppBuilder;
use runtime::Runtime;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

#[tokio::test]
async fn test_prepared_statement_with_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("prepared_statements_basic").build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // PREPARE a simple statement with a parameter
            let prepare_sql = "PREPARE my_query AS SELECT 1 + 10 AS result";
            let prepare_result = rt
                .datafusion()
                .query_builder(prepare_sql)
                .build()
                .run()
                .await?;

            let prepare_batches: Vec<RecordBatch> = prepare_result
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await?;

            // PREPARE should complete successfully
            assert_eq!(prepare_batches.len(), 0, "PREPARE should return no rows");

            // Get explain plan for PREPARE statement
            let explain_sql = "EXPLAIN PREPARE my_query AS SELECT 1 + 10 AS result";
            let explain_result = rt
                .datafusion()
                .query_builder(explain_sql)
                .build()
                .run()
                .await?;

            let explain_batches: Vec<RecordBatch> = explain_result
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await?;

            let explain_plan = arrow::util::pretty::pretty_format_batches(&explain_batches)
                .expect("format explain");

            insta::with_settings!({
                description => "EXPLAIN PREPARE my_query AS SELECT 1 + 10 AS result",
                omit_expression => true
            }, {
                insta::assert_snapshot!("prepared_statement_prepare_explain", explain_plan);
            });

            // Note: EXECUTE is not yet fully supported in physical planning,
            // so we only test PREPARE and EXPLAIN here. Flight SQL tests
            // cover the full execution path with parameter binding.

            Ok(())
        })
        .await
}

// Additional tests to add once EXECUTE physical planning is fully supported:
// - test_prepared_statement_execute_with_parameters
// - test_prepared_statement_multiple_parameters
// - test_prepared_statement_question_mark_placeholders
//
// These features are currently tested via Flight SQL in:
// crates/runtime/tests/flight/prepared_statements.rs
