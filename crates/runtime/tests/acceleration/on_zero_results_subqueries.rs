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

#![expect(clippy::expect_used, reason = "test schema and runtime invariants")]

//! Subqueries over `on_zero_results: use_source` must be planned before the
//! fallback scan evaluates its predicates (regression test for #14010).

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::Context;
use app::AppBuilder;
use arrow::array::{Array, Int64Array, RecordBatch};
use runtime::{Runtime, accelerated::AcceleratedTable};
use spicepod::{
    acceleration::{Acceleration, Mode, ZeroResultsAction},
    component::dataset::Dataset,
    param::Params,
};

use crate::{
    acceleration::load_runtime_datasets,
    configure_test_datafusion,
    utils::{register_test_connectors, run_query, test_request_context},
};

const QUERIES: &[(&str, &[i64])] = &[
    (
        "SELECT id FROM items WHERE EXISTS (SELECT 1 FROM details WHERE item_id = id)",
        &[1, 2],
    ),
    (
        "SELECT id FROM items WHERE NOT EXISTS (SELECT 1 FROM details WHERE item_id = id)",
        &[3, 4],
    ),
    (
        "SELECT id FROM items WHERE id IN (SELECT item_id FROM details)",
        &[1, 2],
    ),
    (
        "SELECT id FROM items WHERE id NOT IN (SELECT item_id FROM details)",
        &[],
    ),
    (
        "SELECT id FROM items WHERE id IN (SELECT item_id FROM details GROUP BY item_id HAVING count(*) > 1)",
        &[1],
    ),
    (
        "SELECT id FROM items WHERE v > (SELECT avg(val) FROM details WHERE item_id = id)",
        &[2],
    ),
    (
        "SELECT id FROM items WHERE v = (SELECT min(val) FROM details WHERE item_id = id)",
        &[1],
    ),
    (
        "SELECT id FROM items WHERE v > (SELECT avg(val) FROM details)",
        &[2, 4],
    ),
    (
        "SELECT id FROM items WHERE id NOT IN (SELECT item_id FROM details WHERE val > 100)",
        &[1, 2, 3, 4],
    ),
];

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("the query returns BIGINT ids");
            assert_eq!(column.null_count(), 0);
            column.values().to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn dataset(
    dir: &Path,
    table: &str,
    engine: &str,
    mode: &Mode,
    empty: bool,
    action: &ZeroResultsAction,
) -> Dataset {
    let mut dataset = Dataset::new(
        format!("file://{}", dir.join(format!("{table}.csv")).display()),
        table,
    );
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    let params = if engine == "cayenne" {
        Some(Params::from_string_map(
            [(
                "cayenne_file_path".to_string(),
                dir.join(format!("{table}-cayenne")).display().to_string(),
            )]
            .into(),
        ))
    } else {
        crate::acceleration::get_params(
            mode,
            Some(dir.join(format!("{table}.{engine}")).display().to_string()),
            engine,
        )
    };
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: mode.clone(),
        on_zero_results: action.clone(),
        refresh_sql: empty.then(|| format!("SELECT * FROM {table} LIMIT 0")),
        params,
        ..Acceleration::default()
    });
    dataset
}

async fn check_subqueries(
    engine: &str,
    mode: &Mode,
    empty: bool,
    action: &ZeroResultsAction,
    mixed_federation: bool,
) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("items.csv"), "id,v\n1,10\n2,20\n3,\n4,40\n")?;
    std::fs::write(
        dir.path().join("details.csv"),
        "item_id,val\n1,10\n1,20\n2,5\n,7\n",
    )?;
    let app = AppBuilder::new("on_zero_results_subqueries")
        .with_dataset(dataset(dir.path(), "items", engine, mode, empty, action))
        .with_dataset(dataset(
            dir.path(),
            "details",
            engine,
            mode,
            empty,
            if mixed_federation {
                &ZeroResultsAction::ReturnEmpty
            } else {
                action
            },
        ))
        .build();
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let result: anyhow::Result<()> = async {
        load_runtime_datasets(&rt, Duration::from_mins(1)).await?;

        // Inspect the storage beneath the fallback wrapper so source reads cannot
        // make an empty or uninitialized acceleration look populated.
        for table in ["items", "details"] {
            let provider = rt
                .datafusion()
                .get_accelerated_table_provider(table)
                .await?;
            let accelerated = spice_table::find_layer::<AcceleratedTable>(
                provider.as_ref(),
                spice_table::LayerWalk::Read,
            )
            .expect("the runtime registered an accelerated table");
            assert!(accelerated.refresher().initial_load_completed());
            let batches = rt
                .datafusion()
                .ctx
                .read_table(accelerated.get_accelerator())?
                .collect()
                .await?;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                if empty { 0 } else { 4 }
            );
        }

        let mut failures = Vec::new();
        for &(query, expected) in QUERIES {
            match run_query(&rt, query).await {
                Ok(batches) => {
                    let expected = if empty && *action == ZeroResultsAction::ReturnEmpty {
                        &[][..]
                    } else {
                        expected
                    };
                    if ids(&batches) != expected {
                        failures.push(format!(
                            "{query}: expected {expected:?}, got {:?}",
                            ids(&batches)
                        ));
                    }
                }
                Err(error) => failures.push(format!("{query}: {error}")),
            }
        }
        anyhow::ensure!(
            failures.is_empty(),
            "{engine} {mode:?}, empty={empty}, {action:?}, mixed_federation={mixed_federation}:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    rt.shutdown().await;
    result.context(
        "subqueries must execute through the runtime acceleration and source fallback path",
    )
}

async fn check_engine(engine: &str, modes: &[Mode]) -> anyhow::Result<()> {
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let mut failures = Vec::new();
            for mode in modes {
                for empty in [false, true] {
                    for action in [ZeroResultsAction::UseSource, ZeroResultsAction::ReturnEmpty] {
                        if let Err(error) =
                            check_subqueries(engine, mode, empty, &action, false).await
                        {
                            failures.push(format!("{error:#}"));
                        }
                    }
                }
            }
            // An enabled SQL-federated table elsewhere in the statement can
            // invoke federation even when the fallback table has no provider.
            if let Err(error) = check_subqueries(
                engine,
                &modes[0],
                false,
                &ZeroResultsAction::UseSource,
                true,
            )
            .await
            {
                failures.push(format!("{error:#}"));
            }
            anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));
            Ok(())
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn duckdb_on_zero_results_subqueries() -> anyhow::Result<()> {
    check_engine("duckdb", &[Mode::Memory, Mode::File]).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_on_zero_results_subqueries() -> anyhow::Result<()> {
    check_engine("sqlite", &[Mode::Memory, Mode::File]).await
}

#[cfg(not(windows))]
#[tokio::test]
async fn cayenne_on_zero_results_subqueries() -> anyhow::Result<()> {
    check_engine("cayenne", &[Mode::File]).await
}
