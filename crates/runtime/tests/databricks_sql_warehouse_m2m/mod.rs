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

//! Databricks SQL warehouse tests that authenticate with a service principal
//! (machine-to-machine OAuth) against the workspace behind `NEW_DATABRICKS_HOST`.

use std::{sync::Arc, time::Duration};

use app::AppBuilder;
use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::RecordBatchStream;
use futures::TryStreamExt;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};

/// Unity Catalog `STREAMING_TABLE` the test reads.
const STREAMING_TABLE: &str = "spiceai_sandbox.integration.streaming_dest";

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("databricks:{path}"), name.to_string());
    dataset.params = Some(get_params());
    dataset
}

#[expect(clippy::expect_used)]
fn get_params() -> Params {
    for var in [
        "NEW_DATABRICKS_HOST",
        "NEW_DATABRICKS_SP_CLIENT_ID",
        "NEW_DATABRICKS_SP_CLIENT_SECRET",
        "NEW_DATABRICKS_SQL_WAREHOUSE_ID",
    ] {
        std::env::var(var).unwrap_or_else(|_| panic!("{var} is not set"));
    }

    Params::from_string_map(
        [
            ("databricks_endpoint", "${ env:NEW_DATABRICKS_HOST }"),
            (
                "databricks_client_id",
                "${ env:NEW_DATABRICKS_SP_CLIENT_ID }",
            ),
            (
                "databricks_client_secret",
                "${ env:NEW_DATABRICKS_SP_CLIENT_SECRET }",
            ),
            (
                "databricks_sql_warehouse_id",
                "${ env:NEW_DATABRICKS_SQL_WAREHOUSE_ID }",
            ),
            ("client_timeout", "120s"),
            ("mode", "sql_warehouse"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect(),
    )
}

/// Runs `sql` and returns the result schema alongside the batches. The schema
/// comes from the stream, so it is available even when no rows come back.
async fn collect(rt: &Runtime, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), anyhow::Error> {
    let stream = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .data;
    let schema = stream.schema();
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok((schema, batches))
}

/// A dataset pointed at a Unity Catalog `STREAMING_TABLE` must become ready
/// and serve queries through the SQL warehouse like any managed table. The
/// connector used to reject the table type before contacting the warehouse,
/// so the dataset never left the initializing state.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_sql_warehouse_m2m_streaming_table_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_sql_warehouse_m2m_streaming_table_test")
                .with_dataset(make_dataset(STREAMING_TABLE, "streaming_table"))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => {
                    return Err(anyhow::anyhow!(
                        "Timed out waiting for streaming table '{STREAMING_TABLE}' to load"
                    ));
                }
                () = cloned_rt.load_components() => {}
            }

            // Readiness is the regression under test: the Unity Catalog type
            // check runs before any warehouse call, so a rejected type never
            // gets this far.
            runtime_ready_check(&rt).await;

            // The count exercises the full scan path on the warehouse.
            let (_, count) = collect(&rt, "SELECT COUNT(*) AS n FROM streaming_table").await?;
            let count_column = count
                .first()
                .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| anyhow::anyhow!("COUNT(*) returned no Int64 column"))?;
            assert_eq!(count_column.len(), 1, "COUNT(*) should return one row");
            assert!(!count_column.is_null(0), "COUNT(*) should not be NULL");

            // A projected read confirms the schema probe produced usable
            // columns; the schema is checked independently of the row count so
            // an empty or not-yet-refreshed table cannot pass vacuously.
            let (schema, sample) = collect(&rt, "SELECT * FROM streaming_table LIMIT 5").await?;
            assert!(
                !schema.fields().is_empty(),
                "streaming table should expose at least one column"
            );
            let sample_rows: usize = sample.iter().map(RecordBatch::num_rows).sum();
            assert!(
                sample_rows <= 5,
                "expected at most 5 rows, got {sample_rows}"
            );

            Ok(())
        })
        .await
}
