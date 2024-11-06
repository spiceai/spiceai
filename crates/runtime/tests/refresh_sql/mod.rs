/*
Copyright 2024 The Spice.ai OSS Authors

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
use std::{sync::Arc, time::Duration};

use app::AppBuilder;
use runtime::{
    accelerated_table::{refresh::RefreshOverrides, AcceleratedTable},
    component::dataset::acceleration::RefreshMode,
    datafusion::query::Protocol,
    Runtime,
};
use spicepod::component::dataset::{acceleration::Acceleration, Dataset};

use crate::{
    init_tracing,
    utils::{runtime_ready_check, wait_until_true},
};

fn make_spiceai_dataset(path: &str, name: &str, refresh_sql: String) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai:{path}"), name.to_string());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        refresh_sql: Some(refresh_sql),
        ..Default::default()
    });
    ds
}

#[tokio::test]
async fn spiceai_integration_test_refresh_sql_pushdown() -> Result<(), String> {
    use runtime::accelerated_table::refresh_task::RefreshTask;

    let _tracing = init_tracing(None);
    let app = AppBuilder::new("refresh_sql_pushdown")
        .with_dataset(make_spiceai_dataset(
            "eth.traces",
            "traces",
            "SELECT * FROM traces WHERE block_number = 0 AND trace_id = 'foobar'".to_string(),
        ))
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    rt.load_components().await;

    let traces_table = rt
        .datafusion()
        .get_accelerated_table_provider("traces")
        .await
        .map_err(|e| e.to_string())?;

    let traces_accelerated_table = traces_table
        .as_any()
        .downcast_ref::<AcceleratedTable>()
        .ok_or("traces table is not an AcceleratedTable")?;

    let request = traces_accelerated_table
        .refresh_params()
        .read()
        .await
        .clone();

    let refresh_task = Arc::new(RefreshTask::new(
        rt.status(),
        "traces".into(),
        Arc::clone(&traces_accelerated_table.get_federated_table()),
        traces_table,
    ));

    // If the refresh SQL filters aren't being pushed down, this will timeout
    let data_update = tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        res = refresh_task.get_full_or_incremental_append_update(&request, None) => {
            res.map_err(|e| e.to_string())?
        }
    };

    let data_update = data_update
        .collect_data()
        .await
        .expect("should convert to DataUpdate");

    assert_eq!(data_update.data.len(), 1);
    assert_eq!(data_update.data[0].num_rows(), 0);

    Ok(())
}

#[tokio::test]
async fn spiceai_integration_test_refresh_sql_override_append() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    let app = AppBuilder::new("refresh_sql_override_append")
        .with_dataset(make_spiceai_dataset(
            "tpch.nation",
            "nation",
            "SELECT * FROM nation WHERE n_regionkey != 0".to_string(),
        ))
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            panic!("Timeout waiting for components to load");
        }
        () = rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    let query = rt
        .datafusion()
        .query_builder(
            "SELECT * FROM nation WHERE n_regionkey = 0",
            Protocol::Internal,
        )
        .build()
        .run()
        .await?;

    let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
    assert_eq!(
        results.len(),
        0,
        "Expected refresh SQL to filter out all rows for n_regionkey = 0"
    );

    rt.datafusion()
        .refresh_table(
            "nation",
            Some(RefreshOverrides {
                sql: Some("SELECT * FROM nation WHERE n_regionkey = 0".to_string()),
                mode: Some(RefreshMode::Append),
                max_jitter: None,
            }),
        )
        .await?;

    assert!(
        wait_until_true(Duration::from_secs(10), || async {
            let Ok(query) = rt
                .datafusion()
                .query_builder(
                    "SELECT * FROM nation WHERE n_regionkey = 0",
                    Protocol::Internal,
                )
                .build()
                .run()
                .await
            else {
                return false;
            };

            let results: Vec<RecordBatch> = match query.data.try_collect::<Vec<RecordBatch>>().await
            {
                Ok(results) => results,
                Err(_) => return false,
            };
            !results.is_empty()
        })
        .await
    );

    let query = rt
        .datafusion()
        .query_builder(
            "SELECT * FROM nation WHERE n_regionkey = 0 ORDER BY n_nationkey DESC",
            Protocol::Internal,
        )
        .build()
        .run()
        .await?;

    let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
    let results_str = arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
    insta::assert_snapshot!(results_str);

    Ok(())
}
