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
use crate::utils::{runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};
use app::AppBuilder;
use arrow_schema::{DataType, Schema};
use data_components::poly::PolyTableProvider;
use datafusion::common::{Constraints, TableReference, ToDFSchema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_expr::CreateExternalTable;
use datafusion_federation::{FederatedPlanner, FederatedTableProviderAdaptor};
use runtime::Runtime;
use runtime::accelerated_table::refresh_task::{accelerator_table_provider, max_timestamp_df};
use runtime::component::dataset::acceleration::Engine;
use runtime::datafusion::builder::AnalyzerRulesBuilder;
use runtime_datafusion::extension::bytes_processed::BytesProcessedPhysicalOptimizer;
use runtime_datafusion::{
    execution_plan::schema_cast::EnsureSchema, extension::ExtensionPlanQueryPlanner,
};
use runtime_datafusion_index::analyzer::{
    IndexTableScanExtensionPlanner, IndexTableScanOptimizerRule,
};
use runtime_object_store::registry::default_runtime_env;
use std::collections::HashMap;
use std::sync::Arc;
use telemetry::track_bytes_processed;
use tokio::runtime::Handle;

/// This test verifies:
///   *  `DataAccelerator::create_external_table` returns `PolyTableProvider`
///   *  `max_timestamp_df` returns `DataFrame` which can be properly federated
#[tokio::test]
async fn test_refresh_max_timestamp_df() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("test_refresh_max_timestamp_df").build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let registry = rt.datafusion().accelerator_engine_registry();
            let engine = registry
                .get_accelerator_engine(Engine::Sqlite)
                .await
                .expect("No engine");

            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time_in_string",
                DataType::Utf8,
                false,
            )]));

            let cmd = CreateExternalTable {
                schema: ToDFSchema::to_dfschema_ref(Arc::clone(&schema))?,
                name: TableReference::bare("test_table"),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options: HashMap::new(),
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };
            let accelerated_table = engine
                .create_external_table(cmd, None, vec![])
                .await
                .expect("Failed to create external table");

            accelerated_table
                .as_any()
                .downcast_ref::<PolyTableProvider>()
                .expect("Expected PolyTableProvider");

            let mut state = SessionStateBuilder::new()
                .with_runtime_env(default_runtime_env(Handle::current()))
                .with_default_features()
                .with_query_planner(Arc::new(
                    ExtensionPlanQueryPlanner::from_extension_planners(vec![
                        Arc::new(FederatedPlanner::new()),
                        Arc::new(IndexTableScanExtensionPlanner::new()),
                    ]),
                ))
                .with_analyzer_rules(AnalyzerRulesBuilder::default().build())
                .with_optimizer_rule(Arc::new(IndexTableScanOptimizerRule::new()))
                .with_physical_optimizer_rule(Arc::new(BytesProcessedPhysicalOptimizer::new(
                    Arc::new(Box::new(track_bytes_processed)),
                )))
                .build();

            if let Err(e) = datafusion_functions_json::register_all(&mut state) {
                tracing::error!("Unable to register JSON functions: {e}");
            }

            let ctx = SessionContext::new_with_state(state);

            let df = max_timestamp_df(&accelerated_table, ctx, "time_in_string")?;

            let explain_plan = arrow::util::pretty::pretty_format_batches(
                &df.clone().explain(false, false)?.collect().await?,
            )?;

            insta::assert_snapshot!(
                format!("refresh_max_timestamp_df_explain_plan"),
                explain_plan
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_accelerator_table_provider() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("test_accelerator_table_provider").build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let registry = rt.datafusion().accelerator_engine_registry();
            let engine = registry
                .get_accelerator_engine(Engine::Sqlite)
                .await
                .expect("No engine");

            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time_in_string",
                DataType::Utf8,
                false,
            )]));

            let cmd = CreateExternalTable {
                schema: ToDFSchema::to_dfschema_ref(Arc::clone(&schema))?,
                name: TableReference::bare("test_table"),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options: HashMap::new(),
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };
            let accelerated_table = engine
                .create_external_table(cmd, None, vec![])
                .await
                .expect("Failed to create external table");

            accelerated_table
                .as_any()
                .downcast_ref::<PolyTableProvider>()
                .expect("Expected PolyTableProvider");

            let table_provider = accelerator_table_provider(&accelerated_table);

            let federated_table_adaptor = table_provider
                .as_any()
                .downcast_ref::<FederatedTableProviderAdaptor>()
                .expect("Expected FederatedTableProviderAdaptor");

            federated_table_adaptor
                .table_provider
                .as_ref()
                .expect("Expected table provider")
                .as_any()
                .downcast_ref::<EnsureSchema>()
                .expect("Expected EnsureSchema");

            Ok(())
        })
        .await
}
