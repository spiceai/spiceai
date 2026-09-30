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

//! Cayenne resolves a primary key repeated in the incoming data of one refresh
//! per the documented `on_conflict` semantics, in which each record batch is one
//! upsert statement (regression tests for #14578):
//!
//! | `on_conflict`            | repeat within a batch          | repeat across batches |
//! |--------------------------|--------------------------------|-----------------------|
//! | `drop`                   | first copy kept                | first copy kept       |
//! | `upsert`                 | error                          | last copy wins        |
//! | `upsert_dedup`           | identical collapse, else error | last copy wins        |
//! | `upsert_dedup_by_row_id` | last copy wins                 | last copy wins        |
#![expect(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{AsArray, RecordBatch};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use spicepod::partitioning::PartitionedBy;

use crate::configure_test_datafusion;
use crate::utils::{runtime_ready_check_with_timeout_err, test_request_context};

const POLICIES: [(&str, OnConflictBehavior); 4] = [
    ("drop", OnConflictBehavior::Drop),
    ("upsert", OnConflictBehavior::Upsert),
    ("upsert_dedup", OnConflictBehavior::UpsertDedup),
    (
        "upsert_dedup_by_row_id",
        OnConflictBehavior::UpsertDedupByRowId,
    ),
];

struct Case {
    mode: Mode,
    refresh: RefreshMode,
    partitioned: bool,
}

impl Case {
    fn label(&self) -> String {
        format!(
            "{:?}/{:?}{}",
            self.mode,
            self.refresh,
            if self.partitioned { "/partitioned" } else { "" }
        )
    }
}

/// Load `csv` into a Cayenne dataset keyed by `id` (and `region` when
/// partitioned), returning the runtime once it is ready, or `None` if the load
/// failed.
async fn load(
    csv: &str,
    case: &Case,
    behavior: OnConflictBehavior,
    label: &str,
) -> (Option<Runtime>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let file = dir.path().join("rows.csv");
    std::fs::write(&file, csv).expect("csv");
    let mut params = HashMap::new();
    if case.mode == Mode::File {
        params.insert(
            "cayenne_file_path".to_string(),
            dir.path().join("data").display().to_string(),
        );
        params.insert(
            "cayenne_metadata_dir".to_string(),
            dir.path().join("meta").display().to_string(),
        );
    }
    let key = if case.partitioned {
        "(id, region)"
    } else {
        "id"
    };
    let mut dataset = Dataset::new(format!("file://{}", file.display()), "t");
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: case.mode.clone(),
        refresh_mode: Some(case.refresh.clone()),
        params: Some(Params::from_string_map(params)),
        primary_key: Some(key.to_string()),
        on_conflict: HashMap::from([(key.to_string(), behavior)]),
        partition_by: if case.partitioned {
            vec![PartitionedBy {
                name: "region".to_string(),
                expression: "region".to_string(),
            }]
        } else {
            Vec::new()
        },
        ..Acceleration::default()
    });
    configure_test_datafusion();
    let app = AppBuilder::new(format!("cayenne_on_conflict_{label}"))
        .with_dataset(dataset)
        .build();
    let rt = Runtime::builder().with_app(app).build().await;
    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(120)) => panic!("{label}: load timed out"),
        () = Arc::new(rt.clone()).load_components() => {}
    }
    let ready = runtime_ready_check_with_timeout_err(&rt, Duration::from_secs(15))
        .await
        .is_ok();
    (ready.then_some(rt), dir)
}

async fn rows(rt: &Runtime, sql: &str) -> Vec<RecordBatch> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .expect("query")
        .data
        .try_collect()
        .await
        .expect("collect")
}

async fn value_of(rt: &Runtime, id: i64) -> Vec<String> {
    rows(rt, &format!("SELECT v FROM t WHERE id = {id}"))
        .await
        .iter()
        .flat_map(|batch| {
            let values = batch.column(0).as_string::<i32>();
            (0..batch.num_rows())
                .map(|row| values.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn count(rt: &Runtime) -> i64 {
    rows(rt, "SELECT COUNT(*) FROM t").await[0]
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(0)
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for mode in [Mode::Memory, Mode::File] {
        for refresh in [RefreshMode::Full, RefreshMode::Append] {
            cases.push(Case {
                mode: mode.clone(),
                refresh,
                partitioned: false,
            });
        }
    }
    cases.push(Case {
        mode: Mode::File,
        refresh: RefreshMode::Full,
        partitioned: true,
    });
    cases
}

/// 8,192 distinct keys fill the first record batch; key 0 repeats with a
/// different value in the second.
fn repeated_across_batches() -> String {
    let mut csv = String::from("id,region,v\n");
    for id in 0..8_192 {
        csv.push_str(&format!("{id},us,first\n"));
    }
    csv.push_str("0,us,last\n");
    csv
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_repeated_across_batches_resolves_per_on_conflict() {
    test_request_context()
        .scope(async {
            let mut failures = Vec::new();
            for case in cases() {
                for (name, behavior) in POLICIES {
                    let label = format!("{}/{name}", case.label());
                    let (rt, _dir) =
                        load(&repeated_across_batches(), &case, behavior, &label).await;
                    let Some(rt) = rt else {
                        failures.push(format!("{label}: did not load"));
                        continue;
                    };
                    let expected = if behavior == OnConflictBehavior::Drop {
                        "first"
                    } else {
                        "last"
                    };
                    let (values, count) = (value_of(&rt, 0).await, count(&rt).await);
                    let ok = values == [expected] && count == 8_192;
                    eprintln!(
                        "{label}: key 0 = {values:?}, COUNT(*) = {count}: {}",
                        if ok { "ok" } else { "WRONG" }
                    );
                    if !ok {
                        failures.push(format!("{label}: key 0 = {values:?}, COUNT(*) = {count}"));
                    }
                }
            }
            assert!(failures.is_empty(), "{failures:#?}");
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_repeated_within_a_batch_resolves_per_on_conflict() {
    test_request_context()
        .scope(async {
            let identical = "id,region,v\n1,us,a\n2,us,b\n1,us,a\n";
            let differing = "id,region,v\n1,us,a\n2,us,b\n1,us,c\n";
            // (csv, policy, expected value of key 1, or None when the load must fail)
            let expectations: [(&str, OnConflictBehavior, Option<&str>); 8] = [
                (identical, OnConflictBehavior::Drop, Some("a")),
                (differing, OnConflictBehavior::Drop, Some("a")),
                (identical, OnConflictBehavior::Upsert, None),
                (differing, OnConflictBehavior::Upsert, None),
                (identical, OnConflictBehavior::UpsertDedup, Some("a")),
                (differing, OnConflictBehavior::UpsertDedup, None),
                (identical, OnConflictBehavior::UpsertDedupByRowId, Some("a")),
                (differing, OnConflictBehavior::UpsertDedupByRowId, Some("c")),
            ];
            let mut failures = Vec::new();
            for case in cases() {
                for (index, (csv, behavior, expected)) in expectations.iter().enumerate() {
                    let label = format!("{}/{behavior:?}/{index}", case.label());
                    let (rt, _dir) = load(csv, &case, *behavior, &label).await;
                    let observed = match &rt {
                        None => None,
                        Some(rt) => {
                            let values = value_of(rt, 1).await;
                            let count = count(rt).await;
                            if count != 2 || values.len() != 1 {
                                failures.push(format!(
                                    "{label}: key 1 = {values:?}, COUNT(*) = {count}"
                                ));
                            }
                            values.into_iter().next()
                        }
                    };
                    let ok = observed.as_deref() == *expected;
                    eprintln!(
                        "{label}: expected {expected:?}, observed {observed:?}: {}",
                        if ok { "ok" } else { "WRONG" }
                    );
                    if !ok {
                        failures.push(format!(
                            "{label}: expected {expected:?}, observed {observed:?}"
                        ));
                    }
                }
            }
            assert!(failures.is_empty(), "{failures:#?}");
        })
        .await;
}
