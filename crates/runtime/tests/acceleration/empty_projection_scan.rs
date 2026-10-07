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

//! A `DuckDB` or `SQLite` acceleration that a statement only filters must still
//! contribute its matching rows.
//!
//! When the select list is a scalar subquery over an Arrow acceleration, as in
//! TPC-DS q9, the outer table is read for nothing but its filter, so it is
//! federated on its own as a scan with an empty projection. The unparser renders that as `SELECT 1 FROM … WHERE …`, because
//! neither engine has an empty select list, and the engine answers with that one
//! placeholder column. `datafusion-federation` has to ask the executor for the
//! placeholder and reduce it to the row count (spiceai/datafusion-federation#91):
//! told to expect no column instead, `DuckDB` fails with "Unexpected number of
//! columns. Expected: 0, Found: 1" and the `SQLite` row decoder panics.

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::Context;
use app::AppBuilder;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode},
    component::dataset::Dataset,
    param::Params,
};

use crate::{
    acceleration::{get_params, load_runtime_datasets},
    configure_test_datafusion,
    utils::{pushed_down_sql, run_query, test_request_context, to_pretty_display},
};

/// One row per `flags` row with `id > 1` (three of four), each counting the two
/// `items` rows.
const EXPECTED: &str = "\
+---+
| n |
+---+
| 2 |
| 2 |
| 2 |
+---+";

fn dataset(dir: &Path, table: &str, engine: &str, mode: &Mode) -> Dataset {
    let mut dataset = Dataset::new(
        format!("file://{}", dir.join(format!("{table}.csv")).display()),
        table,
    );
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: mode.clone(),
        params: get_params(
            mode,
            Some(dir.join(format!("{table}.{engine}")).display().to_string()),
            engine,
        ),
        ..Acceleration::default()
    });
    dataset
}

async fn check(engine: &str, mode: &Mode) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("items.csv"), "label\na\nb\n")?;
    std::fs::write(dir.path().join("flags.csv"), "id\n1\n2\n3\n4\n")?;
    let app = AppBuilder::new("empty_projection_scan")
        .with_dataset(dataset(dir.path(), "items", "arrow", &Mode::Memory))
        .with_dataset(dataset(dir.path(), "flags", engine, mode))
        .build();
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let result: anyhow::Result<()> = async {
        load_runtime_datasets(&rt, Duration::from_mins(1)).await?;

        let sql = "SELECT (SELECT count(*) FROM items) AS n FROM flags WHERE id > 1";
        let pushed = pushed_down_sql(&run_query(&rt, &format!("EXPLAIN {sql}")).await?)?;
        let answer = run_query(&rt, sql)
            .await
            .with_context(|| format!("{engine} {mode:?}: the query failed; pushed down: {pushed}"))?;
        anyhow::ensure!(
            to_pretty_display(&answer)?.to_string() == EXPECTED,
            "{engine} {mode:?}: expected\n{EXPECTED}\ngot\n{}",
            to_pretty_display(&answer)?
        );

        // The path under test: the engine is sent a scan with an empty
        // projection. A plan that reads a column of `flags`, which a cross join
        // with the same tables does, would pass the check above without
        // exercising it.
        anyhow::ensure!(
            pushed.contains("SELECT 1 FROM") && pushed.contains("flags"),
            "{engine} {mode:?}: expected `flags` to be scanned as `SELECT 1 FROM …`; pushed down: {pushed}"
        );
        Ok(())
    }
    .await;
    rt.shutdown().await;
    result
}

async fn check_engine(engine: &str) -> anyhow::Result<()> {
    test_request_context()
        .scope(async {
            let mut failures = Vec::new();
            for mode in [Mode::Memory, Mode::File] {
                if let Err(error) = check(engine, &mode).await {
                    failures.push(format!("{error:#}"));
                }
            }
            anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));
            Ok(())
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn duckdb_filter_only_scan_keeps_its_rows() -> anyhow::Result<()> {
    check_engine("duckdb").await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_filter_only_scan_keeps_its_rows() -> anyhow::Result<()> {
    check_engine("sqlite").await
}
