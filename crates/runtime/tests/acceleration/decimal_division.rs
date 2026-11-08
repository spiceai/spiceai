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

//! Decimal `/` on a `DuckDB` acceleration must match local `DataFusion`
//! evaluation, not the `DOUBLE` that `DuckDB` answers for `//`.
//!
//! A `DECIMAL(38, 10)` whose magnitude is past `2^53` is the witness: the
//! integer digits of `amt/3` change when the divide is pushed down as `//`
//! and the double is cast back to a decimal. Integer `/` must still be
//! integer division (`n/2` of 7 is 3) and still federate as `//`.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use app::AppBuilder;
use arrow::array::{Array, Decimal128Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};

use crate::{
    acceleration::{get_params, load_runtime_datasets},
    configure_test_datafusion, init_tracing,
    utils::{
        pushed_down_sql, register_test_connectors, run_query, test_request_context,
        to_pretty_display,
    },
};

const LOAD_TIMEOUT: Duration = Duration::from_mins(1);

/// The three rows the defect was measured on: `id` 1, 5 and 8 from the
/// tricky-type fixture. `amt` is `DECIMAL(38, 10)`; id 5 and 8 sit past
/// `2^53`. `n` is 7 so integer `/` can be checked on the same table.
fn write_tricky_parquet(path: &std::path::Path) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amt", DataType::Decimal128(38, 10), true),
        Field::new("price", DataType::Decimal128(10, 2), true),
        Field::new("n", DataType::Int64, false),
    ]));
    let amt = Decimal128Array::from(vec![
        Some(i128::from_str("10000000001").expect("amt id=1")),
        Some(i128::from_str("999999999999999999999999999999").expect("amt id=5")),
        Some(i128::from_str("123456789012345678901234567890").expect("amt id=8")),
    ])
    .with_precision_and_scale(38, 10)
    .context("amt precision")?;
    let price = Decimal128Array::from(vec![Some(10_i128), Some(1_999_i128), Some(1_i128)])
        .with_precision_and_scale(10, 2)
        .context("price precision")?;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 5, 8])),
            Arc::new(amt),
            Arc::new(price),
            Arc::new(Int64Array::from(vec![7_i64, 7, 7])),
        ],
    )
    .context("tricky batch")?;
    let file = std::fs::File::create(path).context("create parquet")?;
    let mut writer = ArrowWriter::try_new(file, schema, None).context("parquet writer")?;
    writer.write(&batch).context("write parquet")?;
    writer.close().context("close parquet")?;
    Ok(())
}

fn parquet_dataset(
    path: &std::path::Path,
    name: &str,
    engine: Option<&str>,
    mode: &Mode,
) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", path.display()), name);
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "parquet".to_string())].into(),
    ));
    dataset.acceleration = engine.map(|engine| Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: mode.clone(),
        refresh_mode: Some(RefreshMode::Full),
        params: get_params(
            mode,
            Some(
                path.parent()
                    .unwrap_or(path)
                    .join(format!("{name}.{engine}"))
                    .display()
                    .to_string(),
            ),
            engine,
        ),
        ..Acceleration::default()
    });
    dataset
}

const DECIMAL_SQL: &str =
    "SELECT id, amt/3 AS a3, price/7 AS p7 FROM {table} WHERE id IN (1, 5, 8) ORDER BY id";
const INTEGER_SQL: &str = "SELECT n/2 AS half FROM {table} WHERE id = 1";

/// Digits `DuckDB` returns when `amt/3` for id=5 is evaluated as a `DOUBLE`
/// and cast back to a decimal. The exact answer's integer digits are
/// `33333333333333333333`.
const DOUBLE_GARBAGE_ID5: &str = "33333333333333333440";
const EXACT_ID5: &str = "33333333333333333333";
const EXACT_ID8: &str = "4115226300411522630";
const TRUNCATING_PRICE: &str = "0.014285";
const ROUNDED_PRICE: &str = "0.014286";

fn first_i64(batches: &[RecordBatch]) -> anyhow::Result<i64> {
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .context("integer / returned no rows")?;
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("integer / did not return Int64")?;
    anyhow::ensure!(
        values.len() == 1,
        "integer / returned {} rows",
        values.len()
    );
    Ok(values.value(0))
}

async fn check_duckdb(mode: &Mode) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let parquet = dir.path().join("tricky.parquet");
    write_tricky_parquet(&parquet)?;

    let app = AppBuilder::new("decimal_division")
        .with_dataset(parquet_dataset(&parquet, "t_fed", None, mode))
        .with_dataset(parquet_dataset(&parquet, "t_duckdb", Some("duckdb"), mode))
        .build();
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let result: anyhow::Result<()> = async {
        load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

        let decimal_sql_fed = DECIMAL_SQL.replace("{table}", "t_fed");
        let decimal_sql_duck = DECIMAL_SQL.replace("{table}", "t_duckdb");
        let fed = run_query(&rt, &decimal_sql_fed).await?;
        let duck = run_query(&rt, &decimal_sql_duck).await?;
        let fed_pretty = to_pretty_display(&fed)?.to_string();
        let duck_pretty = to_pretty_display(&duck)?.to_string();
        anyhow::ensure!(
            duck_pretty == fed_pretty,
            "DuckDB {mode:?}: decimal / must match the unaccelerated table\nfed:\n{fed_pretty}\nduckdb:\n{duck_pretty}"
        );
        anyhow::ensure!(
            duck_pretty.contains(EXACT_ID5) && duck_pretty.contains(EXACT_ID8),
            "DuckDB {mode:?}: expected exact digits of amt/3 past 2^53, got:\n{duck_pretty}"
        );
        anyhow::ensure!(
            !duck_pretty.contains(DOUBLE_GARBAGE_ID5),
            "DuckDB {mode:?}: amt/3 came back through f64:\n{duck_pretty}"
        );
        anyhow::ensure!(
            duck_pretty.contains(TRUNCATING_PRICE) && !duck_pretty.contains(ROUNDED_PRICE),
            "DuckDB {mode:?}: price/7 must truncate the way DataFusion does, got:\n{duck_pretty}"
        );

        let decimal_plan = run_query(&rt, &format!("EXPLAIN {decimal_sql_duck}")).await?;
        let decimal_pushed = pushed_down_sql(&decimal_plan)?;
        anyhow::ensure!(
            !decimal_pushed.contains("//"),
            "DuckDB {mode:?}: decimal / must not be pushed as `//`; pushed down: {decimal_pushed}"
        );
        anyhow::ensure!(
            decimal_pushed.contains("amt"),
            "DuckDB {mode:?}: the scan must still federate the decimal column; pushed down: {decimal_pushed}"
        );

        let integer_sql = INTEGER_SQL.replace("{table}", "t_duckdb");
        let integer = run_query(&rt, &integer_sql).await?;
        let half = first_i64(&integer)?;
        anyhow::ensure!(
            half == 3,
            "DuckDB {mode:?}: integer / must stay integer division (7/2 = 3), got {half}"
        );
        let integer_plan = run_query(&rt, &format!("EXPLAIN {integer_sql}")).await?;
        let integer_pushed = pushed_down_sql(&integer_plan)?;
        anyhow::ensure!(
            integer_pushed.contains("//"),
            "DuckDB {mode:?}: integer / must still federate as `//`; pushed down: {integer_pushed}"
        );
        Ok(())
    }
    .await;
    rt.shutdown().await;
    result
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn duckdb_decimal_division_matches_federated() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let mut failures = Vec::new();
            for mode in [Mode::Memory, Mode::File] {
                if let Err(error) = check_duckdb(&mode).await {
                    failures.push(format!("{error:#}"));
                }
            }
            anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));
            Ok(())
        })
        .await
}

/// `SQLite` stores a wide decimal through a double (#14662), so `amt/3` on
/// id 5 cannot be required to match the federated table. The guard still
/// keeps decimal `/` out of the pushed-down SQL, `price/7` (a value that
/// round-trips) must truncate, and integer `/` stays `7/2 = 3`.
#[cfg(feature = "sqlite")]
async fn check_sqlite() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let parquet = dir.path().join("tricky.parquet");
    write_tricky_parquet(&parquet)?;

    let app = AppBuilder::new("decimal_division_sqlite")
        .with_dataset(parquet_dataset(&parquet, "t_fed", None, &Mode::Memory))
        .with_dataset(parquet_dataset(
            &parquet,
            "t_sqlite",
            Some("sqlite"),
            &Mode::Memory,
        ))
        .build();
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let result: anyhow::Result<()> = async {
        load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

        let price_sql =
            "SELECT id, price/7 AS p7 FROM {table} WHERE id = 1 ORDER BY id";
        let fed = run_query(&rt, &price_sql.replace("{table}", "t_fed")).await?;
        let sqlite = run_query(&rt, &price_sql.replace("{table}", "t_sqlite")).await?;
        let fed_pretty = to_pretty_display(&fed)?.to_string();
        let sqlite_pretty = to_pretty_display(&sqlite)?.to_string();
        anyhow::ensure!(
            sqlite_pretty == fed_pretty,
            "SQLite: price/7 must match the unaccelerated table\nfed:\n{fed_pretty}\nsqlite:\n{sqlite_pretty}"
        );
        anyhow::ensure!(
            sqlite_pretty.contains(TRUNCATING_PRICE) && !sqlite_pretty.contains(ROUNDED_PRICE),
            "SQLite: price/7 must truncate, got:\n{sqlite_pretty}"
        );

        let decimal_plan = run_query(
            &rt,
            &format!(
                "EXPLAIN {}",
                DECIMAL_SQL.replace("{table}", "t_sqlite")
            ),
        )
        .await?;
        let decimal_pushed = pushed_down_sql(&decimal_plan)?;
        anyhow::ensure!(
            !decimal_pushed.contains("amt") || !decimal_pushed.contains('/'),
            "SQLite: decimal / must stay local; pushed down: {decimal_pushed}"
        );

        let integer_sql = INTEGER_SQL.replace("{table}", "t_sqlite");
        let integer = run_query(&rt, &integer_sql).await?;
        let half = first_i64(&integer)?;
        anyhow::ensure!(
            half == 3,
            "SQLite: integer / must stay integer division (7/2 = 3), got {half}"
        );
        Ok(())
    }
    .await;
    rt.shutdown().await;
    result
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_decimal_division_stays_local() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context().scope(check_sqlite()).await
}
