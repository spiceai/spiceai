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

use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::TryStreamExt;
use runtime::{status, Runtime};
use spicepod::component::{
    dataset::{
        acceleration::{Acceleration, Mode, RefreshMode},
        Dataset,
    },
    params::Params,
};
use std::io::Write;
use std::sync::Arc;

use crate::{
    acceleration::get_params,
    get_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

#[expect(clippy::expect_used)]
fn get_dataset() -> Dataset {
    let path = std::env::current_dir()
        .expect("get current directory")
        .join("test_file_watcher.csv");
    Dataset::new(format!("file://{}", path.display()), "names")
}

const NAMES_CSV: &str = include_str!("data/names.csv");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_file_watcher() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _guard = super::ACCELERATION_MUTEX.lock().await;

    test_request_context()
        .scope(async {
            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            // Write the CSV to a file next to the test binary
            std::fs::write("./test_file_watcher.csv", NAMES_CSV).expect("write file");

            let mut dataset = get_dataset();
            let params = get_params(
                &Mode::File,
                Some("./test_file_watcher.db".to_string()),
                "sqlite",
            )
            .map(|params| {
                let mut map = params.as_string_map();
                map.insert("file_watcher".to_string(), "enabled".to_string());
                Params::from_string_map(map)
            });
            dataset.acceleration = Some(Acceleration {
                params,
                enabled: true,
                engine: Some("sqlite".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                refresh_sql: None,
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_file_watcher")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(
                Runtime::builder()
                    .with_app(app)
                    .with_datafusion(df)
                    .with_runtime_status(status)
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT * FROM names ORDER BY id")
                .build()
                .run()
                .await
                .expect("query is successful")
                .data
                .try_collect()
                .await
                .expect("collects results");

            let pretty = arrow::util::pretty::pretty_format_batches(&result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            // Append a new row to the CSV file
            let new_row = "11,Spaceman,29,LEO,100\n";
            std::fs::OpenOptions::new()
                .append(true)
                .open("./test_file_watcher.csv")
                .expect("open file")
                .write_all(new_row.as_bytes())
                .expect("append to file");

            // Wait for the file watcher to detect the change
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT * FROM names ORDER BY id")
                .build()
                .run()
                .await
                .expect("query is successful")
                .data
                .try_collect()
                .await
                .expect("collects results");

            let pretty = arrow::util::pretty::pretty_format_batches(&result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            drop(rt);

            // Remove the files
            std::fs::remove_file("./test_file_watcher.db").expect("remove file");
            let _ = std::fs::remove_file("./test_file_watcher.db-shm");
            let _ = std::fs::remove_file("./test_file_watcher.db-wal");
            std::fs::remove_file("./test_file_watcher.csv").expect("remove file");

            Ok(())
        })
        .await
}
