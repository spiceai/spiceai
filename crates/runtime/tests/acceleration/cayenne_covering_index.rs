/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Real runtime wiring for the bounded Cayenne covering-index join path.

#![cfg(not(target_os = "windows"))]

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::ensure;
use app::AppBuilder;
use datafusion::{assert_batches_eq, assert_batches_sorted_eq, physical_plan::displayable};
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, IndexType, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};

use crate::utils::{runtime_ready_check, test_request_context};

fn cayenne_dataset(
    source: &std::path::Path,
    name: &str,
    index: &str,
    root: &std::path::Path,
) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", source.display()), name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        indexes: HashMap::from([(index.to_string(), IndexType::Enabled)]),
        params: Some(Params::from_string_map(HashMap::from([
            (
                "cayenne_file_path".to_string(),
                root.join("data").to_string_lossy().to_string(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                root.join("metadata").to_string_lossy().to_string(),
            ),
            ("cayenne_inline_max_rows".to_string(), "0".to_string()),
        ]))),
        ..Acceleration::default()
    });
    dataset
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_uses_cayenne_covering_index_join() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir()?;
            let a = temp.path().join("a.csv");
            let b = temp.path().join("b.csv");
            tokio::fs::write(
                &a,
                "some_value,foreign_id,label\nneedle,10,a1\nneedle,10,a2\nneedle,12,a3\nhay,11,a4\n",
            )
            .await?;
            tokio::fs::write(&b, "id,description\n10,ten-a\n10,ten-b\n11,eleven\n").await?;
            crate::configure_test_datafusion();

            let app = AppBuilder::new("cayenne_covering_index_runtime")
                .with_dataset(cayenne_dataset(&a, "a", "some_value", temp.path()))
                .with_dataset(cayenne_dataset(&b, "b", "id", temp.path()))
                .build();
            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("timed out loading Cayenne covering-index datasets"));
                }
                () = Arc::clone(&runtime).load_components() => {}
            }
            runtime_ready_check(&runtime).await;

            let sql = "SELECT a.label, b.description FROM a JOIN b ON a.foreign_id = b.id WHERE a.some_value = 'needle'";
            let mut plan_text = String::new();
            for _ in 0..50 {
                let dataframe = runtime.datafusion().ctx.sql(sql).await?;
                let plan = dataframe.create_physical_plan().await?;
                plan_text = displayable(plan.as_ref()).indent(true).to_string();
                if plan_text.contains("CayenneIndexJoinExec") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            ensure!(
                plan_text.contains("CayenneIndexScanExec")
                    && plan_text.contains("CayenneIndexJoinExec")
                    && !plan_text.contains("HashJoinExec"),
                "runtime did not select the Cayenne covering index path:\n{plan_text}"
            );

            let batches = runtime.datafusion().ctx.sql(sql).await?.collect().await?;
            assert_batches_sorted_eq!(
                [
                    "+-------+-------------+",
                    "| label | description |",
                    "+-------+-------------+",
                    "| a1    | ten-a       |",
                    "| a1    | ten-b       |",
                    "| a2    | ten-a       |",
                    "| a2    | ten-b       |",
                    "+-------+-------------+",
                ],
                &batches
            );
            Ok(())
        })
        .await
}

/// The three-way TPC-H dimension shape must carry the bounded output of the
/// first index join into the second one. This is the runtime form of the
/// customer/nation/region query used to validate covering-index planning.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_uses_covering_indexes_for_chained_joins() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir()?;
            let customer = temp.path().join("customer.csv");
            let nation = temp.path().join("nation.csv");
            let region = temp.path().join("region.csv");
            tokio::fs::write(
                &customer,
                "c_name,c_address,c_nationkey\nCustomer#000030003,needle street,7\nCustomer#000030004,other street,8\n",
            )
            .await?;
            tokio::fs::write(
                &nation,
                "n_nationkey,n_regionkey\n7,3\n8,4\n",
            )
            .await?;
            tokio::fs::write(&region, "r_regionkey,r_name\n3,AMERICA\n4,EUROPE\n").await?;
            crate::configure_test_datafusion();

            let app = AppBuilder::new("cayenne_covering_index_chained_runtime")
                .with_dataset(cayenne_dataset(
                    &customer,
                    "customer",
                    "c_name",
                    temp.path(),
                ))
                .with_dataset(cayenne_dataset(
                    &nation,
                    "nation",
                    "n_nationkey",
                    temp.path(),
                ))
                .with_dataset(cayenne_dataset(
                    &region,
                    "region",
                    "r_regionkey",
                    temp.path(),
                ))
                .build();
            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("timed out loading chained Cayenne covering-index datasets"));
                }
                () = Arc::clone(&runtime).load_components() => {}
            }
            runtime_ready_check(&runtime).await;

            let sql = "SELECT c_name, c_address, r_name FROM customer JOIN nation ON n_nationkey = c_nationkey JOIN region ON n_regionkey = r_regionkey WHERE c_name = 'Customer#000030003'";
            let mut plan_text = String::new();
            for _ in 0..50 {
                let dataframe = runtime.datafusion().ctx.sql(sql).await?;
                let plan = dataframe.create_physical_plan().await?;
                plan_text = displayable(plan.as_ref()).indent(true).to_string();
                if plan_text.matches("CayenneIndexJoinExec").count() == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            ensure!(
                plan_text.contains("CayenneIndexScanExec")
                    && plan_text.matches("CayenneIndexJoinExec").count() == 2
                    && !plan_text.contains("HashJoinExec")
                    && !plan_text.contains("DataSourceExec"),
                "runtime did not select covering indexes for the chained join:\n{plan_text}"
            );

            let batches = runtime.datafusion().ctx.sql(sql).await?.collect().await?;
            assert_batches_eq!(
                [
                    "+--------------------+---------------+---------+",
                    "| c_name             | c_address     | r_name  |",
                    "+--------------------+---------------+---------+",
                    "| Customer#000030003 | needle street | AMERICA |",
                    "+--------------------+---------------+---------+",
                ],
                &batches
            );
            Ok(())
        })
        .await
}
