/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::{collections::HashMap, sync::Arc};

use app::AppBuilder;
use arrow::{
    array::{Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array},
    datatypes::DataType,
};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{
        Acceleration, MaintainAggregates, MaintainedAggregate, MaintainedAggregateExpr,
        MaintainedAggregateFunction, MaintainedAggregates, Mode, RefreshMode,
    },
    component::dataset::Dataset,
    param::Params,
};

use crate::utils::{runtime_ready_check, test_request_context};

async fn execute_rt_sql(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Query failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect results: {e}"))
}

async fn get_physical_plan(
    rt: &Arc<Runtime>,
    sql: &str,
) -> Result<Arc<dyn ExecutionPlan>, anyhow::Error> {
    let df = rt
        .datafusion()
        .ctx
        .sql(sql)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create dataframe: {e}"))?;

    df.create_physical_plan()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create physical plan: {e}"))
}

fn maintained_aggregate_view() -> MaintainedAggregate {
    MaintainedAggregate {
        group_by: vec!["region".to_string()],
        aggregates: vec![
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("amount".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Avg,
                column: Some("latency".to_string()),
            },
        ],
    }
}

fn cayenne_params(cayenne_dir: &std::path::Path, metadata_dir: &std::path::Path) -> Params {
    Params::from_string_map(HashMap::from([
        (
            "cayenne_file_path".to_string(),
            cayenne_dir.display().to_string(),
        ),
        (
            "cayenne_metadata_dir".to_string(),
            metadata_dir.display().to_string(),
        ),
    ]))
}

fn make_dataset(
    name: &str,
    csv_file: &std::path::Path,
    cayenne_dir: &std::path::Path,
    metadata_dir: &std::path::Path,
    maintained_aggregates: MaintainedAggregates,
) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", csv_file.display()), name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(cayenne_params(cayenne_dir, metadata_dir)),
        maintained_aggregates,
        ..Acceleration::default()
    });
    dataset
}

fn aggregate_rows(batches: &[RecordBatch]) -> Vec<(String, i64, i64, f64)> {
    let mut rows = Vec::new();

    for batch in batches {
        let schema = batch.schema();
        let region_idx = schema.index_of("region").expect("region column");
        let row_count_idx = schema.index_of("row_count").expect("row_count column");
        let total_amount_idx = schema
            .index_of("total_amount")
            .expect("total_amount column");
        let avg_latency_idx = schema.index_of("avg_latency").expect("avg_latency column");

        let regions = batch
            .column(region_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region column is Utf8");
        let total_amounts = batch
            .column(total_amount_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("total_amount column is Int64");
        let avg_latencies = batch
            .column(avg_latency_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("avg_latency column is Float64");

        for row in 0..batch.num_rows() {
            rows.push((
                regions.value(row).to_string(),
                int_value(batch, row_count_idx, row),
                total_amounts.value(row),
                avg_latencies.value(row),
            ));
        }
    }

    rows
}

fn int_value(batch: &RecordBatch, column_index: usize, row: usize) -> i64 {
    match batch.schema().field(column_index).data_type() {
        DataType::Int64 => batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column")
            .value(row),
        DataType::UInt64 => {
            let value = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("UInt64 column")
                .value(row);
            i64::try_from(value).expect("UInt64 value fits in i64")
        }
        data_type => panic!("expected Int64 or UInt64, got {data_type}"),
    }
}

async fn assert_plan_uses_maintained_aggregate(
    rt: &Arc<Runtime>,
    sql: &str,
    expected: bool,
) -> Result<(), anyhow::Error> {
    let plan = get_physical_plan(rt, sql).await?;
    let plan = displayable(plan.as_ref()).indent(true).to_string();
    assert_eq!(
        plan.contains("MaintainedAggregateExec"),
        expected,
        "unexpected maintained aggregate rewrite state for plan:\n{plan}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn maintained_aggregates_runtime_rewrite_and_disabled_mode() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            std::fs::create_dir_all(&data_dir)?;

            let csv_file = data_dir.join("sales.csv");
            std::fs::write(
                &csv_file,
                "region,amount,latency\n\
                 east,7,1.0\n\
                 east,8,3.0\n\
                 west,10,2.0\n\
                 west,15,4.0\n",
            )?;

            crate::configure_test_datafusion();

            let enabled_dataset = make_dataset(
                "sales_enabled",
                &csv_file,
                &temp_dir.path().join("cayenne_enabled"),
                &temp_dir.path().join("metadata_enabled"),
                MaintainedAggregates::from(vec![maintained_aggregate_view()]),
            );
            let disabled_dataset = make_dataset(
                "sales_disabled",
                &csv_file,
                &temp_dir.path().join("cayenne_disabled"),
                &temp_dir.path().join("metadata_disabled"),
                MaintainedAggregates::new(
                    MaintainAggregates::Disabled,
                    vec![maintained_aggregate_view()],
                ),
            );

            let app = AppBuilder::new("test_cayenne_maintained_aggregates")
                .with_dataset(enabled_dataset)
                .with_dataset(disabled_dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let enabled_query = "SELECT region, COUNT(*) AS row_count, SUM(amount) AS total_amount, AVG(latency) AS avg_latency FROM sales_enabled GROUP BY region ORDER BY region";
            let enabled_rows = aggregate_rows(&execute_rt_sql(&rt, enabled_query).await?);
            assert_eq!(
                enabled_rows,
                vec![
                    ("east".to_string(), 2, 15, 2.0),
                    ("west".to_string(), 2, 25, 3.0),
                ]
            );
            assert_plan_uses_maintained_aggregate(&rt, enabled_query, true).await?;

            let disabled_query = "SELECT region, COUNT(*) AS row_count, SUM(amount) AS total_amount, AVG(latency) AS avg_latency FROM sales_disabled GROUP BY region ORDER BY region";
            let disabled_rows = aggregate_rows(&execute_rt_sql(&rt, disabled_query).await?);
            assert_eq!(disabled_rows, enabled_rows);
            assert_plan_uses_maintained_aggregate(&rt, disabled_query, false).await?;

            Ok(())
        })
        .await
}
