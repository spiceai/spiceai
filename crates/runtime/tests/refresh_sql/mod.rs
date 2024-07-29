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

use std::sync::Arc;

use app::AppBuilder;
use runtime::{accelerated_table::AcceleratedTable, Runtime};
use spicepod::component::dataset::{acceleration::Acceleration, Dataset};

use crate::init_tracing;

fn make_spiceai_dataset(path: &str, name: &str, refresh_sql: String) -> Dataset {
    let mut ds = Dataset::new(format!("spiceai:{path}"), name.to_string());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        refresh_sql: Some(refresh_sql),
        ..Default::default()
    });
    ds
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
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
        .ctx
        .table_provider("traces")
        .await
        .map_err(|e| e.to_string())?;

    let traces_accelerated_table = traces_table
        .as_any()
        .downcast_ref::<AcceleratedTable>()
        .ok_or("traces table is not an AcceleratedTable")?;

    let refresh_task = Arc::new(RefreshTask::new(
        "traces".into(),
        Arc::clone(&traces_accelerated_table.get_federated_table()),
        traces_accelerated_table.refresh_params(),
        traces_table,
    ));

    // If the refresh SQL filters aren't being pushed down, this will timeout
    let data_update = tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        res = refresh_task.get_full_or_incremental_append_update(None) => {
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
