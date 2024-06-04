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

use std::{sync::Arc, time::Duration};

use app::AppBuilder;
use runtime::Runtime;
use tracing::instrument;

use crate::{init_tracing, wait_until_true};

use std::collections::HashMap;

use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    params::Params as DatasetParams,
};

// This method is only used in tests
#[allow(clippy::expect_used)]
fn make_databricks_odbc(path: &str, name: &str, acceleration: bool, engine: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("odbc:{path}"), name.to_string());
    let databricks_odbc_host =
        std::env::var("databricks_odbc_host").expect("databricks odbc host exists");
    let databricks_warehouse_id =
        std::env::var("databricks_warehouse_id").expect("databricks warehouse id exists");
    let databricks_access_token =
        std::env::var("databricks_access_token").expect("databricks access token exists");
    let params = HashMap::from([
        ("odbc_connection_string".to_string(), format!("Host={databricks_odbc_host};Port=443;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/{databricks_warehouse_id};Driver={{Simba Spark ODBC Driver}};UID=token;PWD={databricks_access_token};ThriftTransport=2").to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset.acceleration = Some(Acceleration {
        enabled: acceleration,
        mode: spicepod::component::dataset::acceleration::Mode::Memory,
        engine: Some(engine.to_string()),
        refresh_mode: spicepod::component::dataset::acceleration::RefreshMode::Full,
        refresh_check_interval: None,
        refresh_sql: Some(format!("SELECT * FROM {name} LIMIT 10")),
        refresh_data_window: None,
        params: None,
        engine_secret: None,
        retention_period: None,
        retention_check_interval: None,
        retention_check_enabled: false,
        on_zero_results: spicepod::component::dataset::acceleration::ZeroResultsAction::ReturnEmpty,
    });
    dataset
}

// Run these tests with
// `databricks_odbc_host=copy-paste-here databricks_warehouse_id=copy-paste-here databricks_access_token=copy-paste-here cargo test --package runtime 'databricks_odbc' --features=odbc,duckdb,sqlite`
//
// Running this test in local requires ODBC setup in local, check https://github.com/spiceai/spiceai/pull/1204 to see the details

#[tokio::test]
async fn databricks_odbc() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let app = AppBuilder::new("databricks_odbc")
        .with_dataset(make_databricks_odbc(
            "samples.tpch.lineitem",
            "line",
            false,
            "arrow",
        ))
        .build();

    let rt = Runtime::new(Some(app), Arc::new(vec![])).await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        () = rt.load_datasets_and_views() => {}
    }

    let result = rt
        .df
        .ctx
        .sql("SELECT * FROM line LIMIT 10")
        .await
        .expect("SQL is used")
        .collect()
        .await
        .expect("Query return result");

    let mut num_rows = 0;

    for i in result {
        num_rows += i.num_rows();
    }

    assert_eq!(10, num_rows);

    Ok(())
}

#[tokio::test]
#[instrument]
async fn databricks_odbc_with_acceleration() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    for engine in [
        "arrow",
        #[cfg(feature = "duckdb")]
        "duckdb",
        #[cfg(feature = "sqlite")]
        "sqlite",
    ] {
        let app = AppBuilder::new("databricks_odbc")
            .with_dataset(make_databricks_odbc(
                "samples.tpch.lineitem",
                "line",
                true,
                engine,
            ))
            .build();

        let rt = Runtime::new(Some(app), Arc::new(vec![])).await;

        // Set a timeout for the test
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err("Timed out waiting for datasets to load".to_string());
            }
            () = rt.load_datasets_and_views() => {}
        }

        assert!(
            wait_until_true(Duration::from_secs(10), || async {
                let result = rt
                    .df
                    .ctx
                    .sql("SELECT * FROM line LIMIT 10")
                    .await
                    .expect("SQL is used")
                    .collect()
                    .await
                    .expect("Query return result");

                let mut num_rows = 0;

                for i in result {
                    num_rows += i.num_rows();
                }

                10 == num_rows
            })
            .await,
            "Expected 10 rows returned for engine {engine}"
        );
    }

    Ok(())
}
