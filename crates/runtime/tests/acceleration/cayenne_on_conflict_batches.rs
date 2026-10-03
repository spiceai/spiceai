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
//! per `on_conflict`, each record batch being one upsert statement (regression
//! tests for #14578):
//!
//! | `on_conflict`            | repeat within a batch          | repeat across batches |
//! |--------------------------|--------------------------------|-----------------------|
//! | `drop`                   | first copy kept                | first copy kept       |
//! | `upsert`                 | last copy wins                 | last copy wins        |
//! | `upsert_dedup`           | identical collapse, else error | last copy wins        |
//! | `upsert_dedup_by_row_id` | last copy wins                 | last copy wins        |
#![expect(clippy::expect_used)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{AsArray, RecordBatch};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::access::AccessMode;
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
    load_with_access(csv, case, behavior, label, AccessMode::Read).await
}

async fn load_with_access(
    csv: &str,
    case: &Case,
    behavior: OnConflictBehavior,
    label: &str,
    access: AccessMode,
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
    dataset.access = access;
    // An append refresh of a partitioned table needs a time column to load.
    if case.partitioned && case.refresh == RefreshMode::Append {
        dataset.time_column = Some("ts".to_string());
    }
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
        () = tokio::time::sleep(Duration::from_mins(2)) => panic!("{label}: load timed out"),
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
    for refresh in [RefreshMode::Full, RefreshMode::Append] {
        cases.push(Case {
            mode: Mode::File,
            refresh,
            partitioned: true,
        });
    }
    cases
}

/// 8,192 distinct keys fill the first record batch; key 0 repeats with a
/// different value in the second.
fn repeated_across_batches() -> String {
    let mut csv = String::from("id,region,ts,v\n");
    for id in 0..8_192 {
        writeln!(csv, "{id},us,2026-01-01T00:00:00,first").expect("write to a String");
    }
    csv.push_str("0,us,2026-01-01T00:00:00,last\n");
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
            let identical = "id,region,ts,v\n1,us,2026-01-01T00:00:00,a\n2,us,2026-01-01T00:00:00,b\n1,us,2026-01-01T00:00:00,a\n";
            let differing = "id,region,ts,v\n1,us,2026-01-01T00:00:00,a\n2,us,2026-01-01T00:00:00,b\n1,us,2026-01-01T00:00:00,c\n";
            // (csv, policy, expected value of key 1, or None when the load must fail)
            let expectations: [(&str, OnConflictBehavior, Option<&str>); 8] = [
                (identical, OnConflictBehavior::Drop, Some("a")),
                (differing, OnConflictBehavior::Drop, Some("a")),
                (identical, OnConflictBehavior::Upsert, Some("a")),
                (differing, OnConflictBehavior::Upsert, Some("c")),
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

/// A user's `UPDATE` keeps statement semantics: moving rows from several batches
/// onto one key fails it, rather than resolving the repeat per `on_conflict` as
/// a refresh does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_update_repeating_a_key_across_batches_still_fails() {
    test_request_context()
        .scope(async {
            let distinct: String = std::iter::once("id,region,ts,v\n".to_string())
                .chain((0..8_193).map(|id| format!("{id},us,2026-01-01T00:00:00,first\n")))
                .collect();
            for mode in [Mode::Memory, Mode::File] {
                let case = Case {
                    mode,
                    refresh: RefreshMode::Full,
                    partitioned: false,
                };
                let label = format!("{}/update", case.label());
                let (rt, _dir) = load_with_access(
                    &distinct,
                    &case,
                    OnConflictBehavior::Upsert,
                    &label,
                    AccessMode::ReadWrite,
                )
                .await;
                let rt = rt.expect("the distinct keys load");
                let result = rt
                    .datafusion()
                    .query_builder("UPDATE t SET id = 0")
                    .build()
                    .run()
                    .await;
                let outcome = match result {
                    Err(error) => Err(error.to_string()),
                    Ok(query) => query
                        .data
                        .try_collect::<Vec<_>>()
                        .await
                        .map_err(|error| error.to_string()),
                };
                let Err(error) = outcome else {
                    panic!(
                        "{label}: an UPDATE moving every row onto key 0 must fail; table now holds {} rows",
                        count(&rt).await
                    );
                };
                assert!(
                    error.contains("duplicate primary key across batches"),
                    "{label}: {error}"
                );
            }
        })
        .await;
}

/// A parent with a `localpod` child refreshes through the child-syncing sink, and
/// still resolves a key its data repeats across batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_localpod_parents_refresh_resolves_repeated_keys() {
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir().expect("temp dir");
            let file = dir.path().join("rows.csv");
            let distinct: String = std::iter::once("id,region,v\n".to_string())
                .chain((0..8_192).map(|id| format!("{id},us,first\n")))
                .collect();
            std::fs::write(&file, &distinct).expect("csv");
            let params = HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    dir.path().join("data").display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    dir.path().join("meta").display().to_string(),
                ),
            ]);
            let mut parent = Dataset::new(format!("file://{}", file.display()), "t");
            parent.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(params)),
                primary_key: Some("id".to_string()),
                on_conflict: HashMap::from([("id".to_string(), OnConflictBehavior::Upsert)]),
                ..Acceleration::default()
            });
            let mut child = Dataset::new("localpod:t", "t_child");
            child.acceleration = Some(Acceleration {
                enabled: true,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });
            configure_test_datafusion();
            let app = AppBuilder::new("cayenne_on_conflict_localpod")
                .with_dataset(parent)
                .with_dataset(child)
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => panic!("load timed out"),
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check_with_timeout_err(&rt, Duration::from_secs(30))
                .await
                .expect("ready");
            assert_eq!(count(&rt).await, 8_192, "initial load");

            std::fs::write(&file, format!("{distinct}0,us,last\n")).expect("csv");
            crate::acceleration::trigger_refresh(&rt, "t")
                .await
                .expect("refresh");
            let deadline = std::time::Instant::now() + Duration::from_mins(1);
            loop {
                let values = value_of(&rt, 0).await;
                if values.contains(&"last".to_string()) {
                    let count = count(&rt).await;
                    assert_eq!(
                        (values, count),
                        (vec!["last".to_string()], 8_192),
                        "the parent's refresh must keep one copy of key 0"
                    );
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the refresh did not land; key 0 = {values:?}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await;
}
