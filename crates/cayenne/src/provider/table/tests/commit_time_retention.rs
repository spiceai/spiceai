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

//! A transaction's `retention_sql` is evaluated in ONE context created at its
//! commit, after the commit's lock waits and optimistic check, over the staged
//! candidates (kept unfiltered while staging) and the resident rows alike.
//! Staging evaluates nothing, so no staging-time verdict is applied to a
//! publication that happens later.
//!
//! Predicate `aged(value)`, switched by the test and recording every value it
//! evaluates: OFF matches nothing, ON matches a negative value, NULLS answers
//! NULL for every row, BROKEN fails on a negative value. Shapes: a keyed table
//! that folds hidden rows out of its staged files (key deletes), a keyed table
//! that hides them by position, a table without a primary key.

use super::*;
use crate::metadata::DeletionMode;
use crate::provider::transaction::CayenneTransaction;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::prelude::col;
use std::sync::atomic::AtomicU8;

const OFF: u8 = 0;
const ON: u8 = 1;
const NULLS: u8 = 2;
const BROKEN: u8 = 3;
/// Matches a negative value, fails on 13, keeps anything else.
const MIXED: u8 = 4;
/// NULL for every row, but fails on 13.
const NULL_MIXED: u8 = 5;

type Seen = Arc<ParkingMutex<Vec<i64>>>;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn aged(mode: Arc<AtomicU8>, seen: Seen) -> ScalarUDF {
    create_udf(
        "aged",
        vec![DataType::Int64],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |args: &[ColumnarValue]| {
            let array = args[0].clone().into_array(1)?;
            let values = array.as_any().downcast_ref::<Int64Array>().expect("int64");
            let mode = mode.load(Ordering::SeqCst);
            seen.lock().extend(values.iter().flatten());
            let mut out = Vec::with_capacity(values.len());
            for v in values.iter() {
                let archived = v.is_some_and(|v| v < 0);
                if (mode == MIXED || mode == NULL_MIXED) && v == Some(13) {
                    return Err(datafusion_common::DataFusionError::Execution(
                        "aged() cannot evaluate 13".to_string(),
                    ));
                }
                if archived && mode == BROKEN {
                    return Err(datafusion_common::DataFusionError::Execution(
                        "aged() cannot evaluate this row".to_string(),
                    ));
                }
                out.push(match mode {
                    NULLS | NULL_MIXED => None,
                    _ => Some(archived && (mode == ON || mode == MIXED)),
                });
            }
            Ok(ColumnarValue::Array(Arc::new(
                arrow::array::BooleanArray::from(out),
            )))
        }),
    )
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Shape {
    KeyedFold,
    KeyedPosition,
    /// A primary key without `on_conflict` (no key resolver), key deletes.
    PkNoConflictKey,
    /// A primary key without `on_conflict`, position deletes.
    PkNoConflictPosition,
    Unkeyed,
}

impl Shape {
    fn upserts(self) -> bool {
        matches!(self, Self::KeyedFold | Self::KeyedPosition)
    }
}

const SHAPES: [Shape; 5] = [
    Shape::KeyedFold,
    Shape::KeyedPosition,
    Shape::PkNoConflictKey,
    Shape::PkNoConflictPosition,
    Shape::Unkeyed,
];

struct Fixture {
    provider: CayenneTableProvider,
    catalog: Arc<dyn MetadataCatalog>,
    name: String,
    mode: Arc<AtomicU8>,
    seen: Seen,
    retention: Vec<Expr>,
    _pass_hold: tokio::sync::OwnedRwLockWriteGuard<()>,
    _dir: TempDir,
}

impl Fixture {
    fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    fn take_seen(&self) -> Vec<i64> {
        let mut seen = std::mem::take(&mut *self.seen.lock());
        seen.sort_unstable();
        seen
    }

    async fn reopen(&self) -> CayenneTableProvider {
        let runtime_env = SessionContext::new().runtime_env();
        CayenneTableProviderBuilder::new(Arc::clone(&self.catalog), runtime_env)
            .with_retention_filters(self.retention.clone())
            .open(&self.name)
            .await
            .expect("reopen")
    }
}

/// A table with `retention_sql`; retention OFF, seeded with `(1, 1)`, the
/// background retention pass held for the whole test so it neither evaluates
/// the predicate nor deletes behind the assertions.
async fn table(name: &str, shape: Shape) -> Fixture {
    table_with(name, shape, None).await
}

/// [`table`] with `compaction_trigger_protected_snapshots` set.
async fn table_with(name: &str, shape: Shape, trigger: Option<usize>) -> Fixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", dir.path().to_str().expect("path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let catalog = Arc::new(
        crate::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db")).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog init");
    let mode = Arc::new(AtomicU8::new(OFF));
    let seen: Seen = Arc::new(ParkingMutex::new(Vec::new()));
    let retention = vec![aged(Arc::clone(&mode), Arc::clone(&seen)).call(vec![col("value")])];
    let keyed = shape != Shape::Unkeyed;
    let runtime_env = SessionContext::new().runtime_env();
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .with_retention_filters(retention.clone())
        .create(CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(),
            primary_key: if keyed {
                vec!["id".to_string()]
            } else {
                vec![]
            },
            on_conflict: shape.upserts().then(|| {
                OnConflict::Upsert(
                    datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                        "id".to_string(),
                    ]),
                )
            }),
            base_path: format!("{}/data", dir.path().to_str().expect("path")),
            partition_column: None,
            vortex_config: VortexConfig {
                compaction_trigger_protected_snapshots: trigger.unwrap_or_else(|| {
                    VortexConfig::default().compaction_trigger_protected_snapshots
                }),
                deletion_mode: if matches!(shape, Shape::KeyedFold | Shape::PkNoConflictKey) {
                    DeletionMode::Key
                } else {
                    DeletionMode::Position
                },
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("table created");
    let pass_hold = Arc::clone(&provider.test_retention_pass_gate)
        .write_owned()
        .await;
    insert_batch(&provider, id_value_batch(schema(), &[1], &[1])).await;
    let f = Fixture {
        provider,
        catalog,
        name: name.to_string(),
        mode,
        seen,
        retention,
        _pass_hold: pass_hold,
        _dir: dir,
    };
    f.take_seen();
    f
}

async fn rows(provider: &CayenneTableProvider) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan plan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan");
    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let values = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v");
        out.extend((0..b.num_rows()).map(|r| (ids.value(r), values.value(r))));
    }
    out.sort_unstable();
    out
}

/// Stage `rows` in a new transaction (not committed).
async fn stage(f: &Fixture, rows: &[(i64, i64)]) -> CayenneTransaction {
    let p = &f.provider;
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let values: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    let txn = CayenneTransaction::new();
    let token = p.transaction_write_token().await;
    txn.register(
        p.table_id().to_string(),
        token,
        p.clone_for_write_operations(),
    );
    let staged = p
        .begin_staged_upsert_occ(
            token,
            single_batch_stream(id_value_batch(schema(), &ids, &values)),
            1,
        )
        .await
        .expect("staged");
    txn.set_staged(p.table_id(), staged);
    txn
}

/// Rows that age between staging and commit are retired by the commit, and
/// rows that matched while staging but no longer match at commit are kept:
/// the commit's context decides, and staging evaluated nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_commit_context_decides_rows_that_change_between_stage_and_commit() {
    for shape in SHAPES {
        let f = table(&format!("ctr_aging_{shape:?}").to_lowercase(), shape).await;
        f.set(OFF);
        let txn = stage(&f, &[(7, -7), (8, 8)]).await;
        f.set(ON);
        assert!(
            f.take_seen().is_empty(),
            "{shape:?}: staging evaluated nothing"
        );
        let committed = txn.commit().await.expect("commit");
        assert_eq!(
            rows(&f.provider).await,
            vec![(1, 1), (8, 8)],
            "{shape:?}: aged at commit"
        );
        // The accepted insert count includes the row retention removed, as
        // upstream (9aa41988c) reported for every shape: its transactions did
        // not evaluate `retention_sql` at all.
        assert_eq!(committed.row_count, 2, "{shape:?}: accepted rows");

        let txn = stage(&f, &[(9, -9)]).await;
        f.set(OFF);
        txn.commit().await.expect("commit");
        assert_eq!(
            rows(&f.provider).await,
            vec![(1, 1), (8, 8), (9, -9)],
            "{shape:?}: no staging verdict survives to the commit"
        );
        assert_eq!(
            rows(&f.reopen().await).await,
            vec![(1, 1), (8, 8), (9, -9)],
            "{shape:?}: reopened"
        );
    }
}

/// At commit each surviving staged candidate and each resident row is
/// evaluated exactly once; a copy the statement's own repeated key superseded
/// is never evaluated (keyed shapes), and staging evaluated nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_candidate_is_evaluated_exactly_once_after_dedup() {
    for shape in SHAPES {
        let f = table(&format!("ctr_once_{shape:?}").to_lowercase(), shape).await;
        f.set(ON);
        // A repeated key only where the table resolves it (an upsert) or has
        // no key at all.
        let staged: &[(i64, i64)] = if shape == Shape::Unkeyed || shape.upserts() {
            &[(5, -50), (5, 5), (6, -6)]
        } else {
            &[(5, 5), (6, -6)]
        };
        let txn = stage(&f, staged).await;
        assert!(
            f.take_seen().is_empty(),
            "{shape:?}: staging evaluated nothing"
        );
        txn.commit().await.expect("commit");
        let expected_seen = if shape == Shape::Unkeyed {
            vec![-50, -6, 1, 5]
        } else {
            vec![-6, 1, 5]
        };
        assert_eq!(
            f.take_seen(),
            expected_seen,
            "{shape:?}: evaluated once each"
        );
        let expected = if shape == Shape::Unkeyed {
            vec![(1, 1), (5, 5)]
        } else {
            vec![(1, 1), (5, 5)]
        };
        assert_eq!(rows(&f.provider).await, expected, "{shape:?}: rows");
    }
}

/// NULL keeps the row (not a match). An evaluation error at commit fails the
/// transaction: nothing is published, live or durable, and a retry commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn null_keeps_and_an_error_at_commit_rolls_back() {
    for shape in SHAPES {
        let f = table(&format!("ctr_err_{shape:?}").to_lowercase(), shape).await;
        f.set(NULLS);
        stage(&f, &[(7, -7)]).await.commit().await.expect("commit");
        assert_eq!(
            rows(&f.provider).await,
            vec![(1, 1), (7, -7)],
            "{shape:?}: NULL keeps"
        );

        f.set(OFF);
        let txn = stage(&f, &[(8, -8)]).await;
        f.set(BROKEN);
        let error = txn
            .commit()
            .await
            .expect_err("an evaluation error fails the commit");
        assert!(
            error.to_string().contains("cannot evaluate"),
            "{shape:?}: {error}"
        );
        f.set(OFF);
        assert_eq!(
            rows(&f.provider).await,
            vec![(1, 1), (7, -7)],
            "{shape:?}: nothing live"
        );
        assert_eq!(
            rows(&f.reopen().await).await,
            vec![(1, 1), (7, -7)],
            "{shape:?}: nothing durable"
        );
        stage(&f, &[(8, -8)]).await.commit().await.expect("retry");
        assert_eq!(
            rows(&f.provider).await,
            vec![(1, 1), (7, -7), (8, -8)],
            "{shape:?}: retried"
        );
    }
}

/// A transaction that loses its optimistic check, or is aborted, evaluates
/// nothing and publishes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_or_aborted_transaction_evaluates_nothing() {
    for shape in [Shape::KeyedFold, Shape::KeyedPosition] {
        let f = table(&format!("ctr_occ_{shape:?}").to_lowercase(), shape).await;
        f.set(ON);
        let txn = stage(&f, &[(7, -7)]).await;
        insert_batch(&f.provider, id_value_batch(schema(), &[7], &[7])).await;
        f.take_seen();
        let error = txn.commit().await.expect_err("conflict");
        assert!(
            matches!(error, Error::WriteConflict { .. }),
            "{shape:?}: {error}"
        );
        assert!(
            f.take_seen().is_empty(),
            "{shape:?}: a conflict evaluates nothing"
        );
        assert_eq!(
            rows(&f.provider).await,
            vec![(1, 1), (7, 7)],
            "{shape:?}: conflict"
        );
    }
    for shape in SHAPES {
        let f = table(&format!("ctr_abort_{shape:?}").to_lowercase(), shape).await;
        f.set(ON);
        let txn = stage(&f, &[(7, -7), (8, 8)]).await;
        txn.abort().await;
        assert!(
            f.take_seen().is_empty(),
            "{shape:?}: an abort evaluates nothing"
        );
        assert_eq!(rows(&f.provider).await, vec![(1, 1)], "{shape:?}: aborted");
    }
}

/// How a test writes its replacement: a staged transaction or a plain durable
/// append (`INSERT` on the upsert table).
#[derive(Clone, Copy, Debug)]
enum Route {
    Transaction,
    Append,
}

/// A replacement supersedes its resident version whatever the commit's
/// predicate decides about the replacement: the superseded `(5, 13)` is never
/// evaluated (evaluating it fails), and it never comes back. A matching
/// replacement removes the key; a NULL verdict keeps the replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_supersedes_its_resident_version_whatever_retention_decides() {
    for shape in [Shape::KeyedFold, Shape::KeyedPosition] {
        for route in [Route::Transaction, Route::Append] {
            for (mode, expected) in [(MIXED, vec![(1, 1)]), (NULL_MIXED, vec![(1, 1), (5, -50)])] {
                let label = format!("{shape:?}/{route:?}/mode {mode}");
                let f = table(&format!("ctr_supersede_{shape:?}_{route:?}_{mode}"), shape).await;
                f.set(OFF);
                insert_batch(&f.provider, id_value_batch(schema(), &[5], &[13])).await;
                f.set(mode);
                f.take_seen();
                match route {
                    Route::Transaction => {
                        stage(&f, &[(5, -50)])
                            .await
                            .commit()
                            .await
                            .unwrap_or_else(|e| panic!("{label}: commit: {e}"));
                    }
                    Route::Append => {
                        insert_batch(&f.provider, id_value_batch(schema(), &[5], &[-50])).await;
                    }
                }
                assert!(
                    !f.take_seen().contains(&13),
                    "{label}: the superseded version is never evaluated"
                );
                f.set(OFF);
                assert_eq!(rows(&f.provider).await, expected, "{label}: live");
                assert_eq!(rows(&f.reopen().await).await, expected, "{label}: reopened");
            }
        }
    }
}

fn composite_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("part", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn composite_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        composite_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

async fn composite_rows(provider: &CayenneTableProvider) -> Vec<(i64, i64, i64)> {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan plan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan");
    let mut out = Vec::new();
    for b in &batches {
        let col = |i: usize| {
            b.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .clone()
        };
        let (a, p, v) = (col(0), col(1), col(2));
        out.extend((0..b.num_rows()).map(|r| (a.value(r), p.value(r), v.value(r))));
    }
    out.sort_unstable();
    out
}

/// The same supersession over a composite primary key `(id, part)`: `(5, 2)`
/// is replaced, `(5, 1)` (same `id`, other `part`) is an unrelated resident
/// row the predicate keeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_composite_key_replacement_supersedes_its_resident_version() {
    for shape in [Shape::KeyedFold, Shape::KeyedPosition] {
        for route in [Route::Transaction, Route::Append] {
            for (mode, replaced) in [(MIXED, None), (NULL_MIXED, Some((5, 2, -50)))] {
                let label = format!("{shape:?}/{route:?}/mode {mode}");
                let dir = tempfile::tempdir().expect("temp dir");
                let metadata_dir = format!("{}/metadata", dir.path().to_str().expect("path"));
                std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
                let catalog = Arc::new(
                    crate::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db"))
                        .expect("catalog"),
                ) as Arc<dyn MetadataCatalog>;
                catalog.init().await.expect("catalog init");
                let mode_cell = Arc::new(AtomicU8::new(OFF));
                let seen: Seen = Arc::new(ParkingMutex::new(Vec::new()));
                let retention =
                    vec![aged(Arc::clone(&mode_cell), Arc::clone(&seen)).call(vec![col("value")])];
                let name = format!("ctr_composite_{shape:?}_{route:?}_{mode}");
                let p = CayenneTableProviderBuilder::new(
                    Arc::clone(&catalog),
                    SessionContext::new().runtime_env(),
                )
                .with_retention_filters(retention.clone())
                .create(CreateTableOptions {
                    table_name: name.clone(),
                    schema: composite_schema(),
                    primary_key: vec!["id".to_string(), "part".to_string()],
                    on_conflict: Some(OnConflict::Upsert(
                        datafusion_table_providers::util::column_reference::ColumnReference::new(
                            vec!["id".to_string(), "part".to_string()],
                        ),
                    )),
                    base_path: format!("{}/data", dir.path().to_str().expect("path")),
                    partition_column: None,
                    vortex_config: VortexConfig {
                        deletion_mode: if shape == Shape::KeyedFold {
                            DeletionMode::Key
                        } else {
                            DeletionMode::Position
                        },
                        inline_max_rows: 0,
                        inline_max_bytes: 0,
                        inline_max_buffer_bytes: 0,
                        compaction_background_interval_ms: 3_600_000,
                        ..VortexConfig::default()
                    },
                })
                .await
                .expect("table created");
                let _pass_hold = Arc::clone(&p.test_retention_pass_gate).write_owned().await;
                insert_batch(&p, composite_batch(&[(5, 1, 7), (5, 2, 13)])).await;
                mode_cell.store(mode, Ordering::SeqCst);
                seen.lock().clear();
                match route {
                    Route::Transaction => {
                        let txn = CayenneTransaction::new();
                        let token = p.transaction_write_token().await;
                        txn.register(
                            p.table_id().to_string(),
                            token,
                            p.clone_for_write_operations(),
                        );
                        let staged = p
                            .begin_staged_upsert_occ(
                                token,
                                single_batch_stream(composite_batch(&[(5, 2, -50)])),
                                1,
                            )
                            .await
                            .expect("staged");
                        txn.set_staged(p.table_id(), staged);
                        txn.commit()
                            .await
                            .unwrap_or_else(|e| panic!("{label}: commit: {e}"));
                    }
                    Route::Append => insert_batch(&p, composite_batch(&[(5, 2, -50)])).await,
                }
                assert!(
                    !seen.lock().contains(&13),
                    "{label}: the superseded (5, 2, 13) is never evaluated"
                );
                mode_cell.store(OFF, Ordering::SeqCst);
                let mut expected = vec![(5, 1, 7)];
                expected.extend(replaced);
                assert_eq!(composite_rows(&p).await, expected, "{label}: live");
                let reopened = CayenneTableProviderBuilder::new(
                    Arc::clone(&catalog),
                    SessionContext::new().runtime_env(),
                )
                .with_retention_filters(retention)
                .open(&name)
                .await
                .expect("reopen");
                assert_eq!(
                    composite_rows(&reopened).await,
                    expected,
                    "{label}: reopened"
                );
            }
        }
    }
}

/// The publication's one `now()` is taken after the commit's wait for the
/// mem-tier checkpoint lock (its last lock wait). Deterministic: a seam pauses
/// the commit at that wait while the test holds the lock; the instant the
/// test releases it is recorded first, and every `now()` the commit evaluates
/// must be at or after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_commit_now_is_taken_after_the_checkpoint_lock_wait() {
    use crate::provider::append_commit::test_seams;
    use arrow::array::TimestampNanosecondArray;
    use datafusion::prelude::now;
    for shape in [
        Shape::KeyedFold,
        Shape::KeyedPosition,
        Shape::PkNoConflictKey,
    ] {
        let seen: Arc<ParkingMutex<Vec<i64>>> = Arc::default();
        let s = Arc::clone(&seen);
        let udf = create_udf(
            "seen_now",
            vec![DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("+00:00".into()),
            )],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new(move |args: &[ColumnarValue]| {
                let rows = match &args[0] {
                    ColumnarValue::Array(a) => a.len(),
                    ColumnarValue::Scalar(_) => 1,
                };
                let array = args[0].clone().into_array(rows)?;
                let ts = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("timestamp");
                s.lock().extend(ts.iter().flatten());
                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::BooleanArray::from(vec![Some(false); rows]),
                )))
            }),
        );
        let dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = format!("{}/metadata", dir.path().to_str().expect("path"));
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            crate::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db"))
                .expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog init");
        let p = CayenneTableProviderBuilder::new(
            Arc::clone(&catalog),
            SessionContext::new().runtime_env(),
        )
        .with_retention_filters(vec![udf.call(vec![now()])])
        .create(CreateTableOptions {
            table_name: format!("ctr_now_after_wait_{shape:?}"),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: shape.upserts().then(|| {
                OnConflict::Upsert(
                    datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                        "id".to_string(),
                    ]),
                )
            }),
            base_path: format!("{}/data", dir.path().to_str().expect("path")),
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode: if matches!(shape, Shape::KeyedFold | Shape::PkNoConflictKey) {
                    DeletionMode::Key
                } else {
                    DeletionMode::Position
                },
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("table created");
        let _pass_hold = Arc::clone(&p.test_retention_pass_gate).write_owned().await;
        insert_batch(&p, id_value_batch(schema(), &[1], &[1])).await;

        let txn = CayenneTransaction::new();
        let token = p.transaction_write_token().await;
        txn.register(
            p.table_id().to_string(),
            token,
            p.clone_for_write_operations(),
        );
        let staged = p
            .begin_staged_upsert_occ(
                token,
                single_batch_stream(id_value_batch(schema(), &[2], &[2])),
                1,
            )
            .await
            .expect("staged");
        txn.set_staged(p.table_id(), staged);
        seen.lock().clear();

        let hold = p.lock_mem_tier_checkpoint_for_durable_write().await;
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel::<()>();
        test_seams::pause_at_checkpoint_wait(
            p.table_id(),
            Box::new(move || {
                Box::pin(async move {
                    reached_tx.send(()).expect("test waits");
                })
            }),
        );
        let commit = tokio::spawn(async move { txn.commit().await.map(|_| ()) });
        reached_rx
            .await
            .unwrap_or_else(|_| panic!("{shape:?}: the commit reached the checkpoint lock wait"));
        let released_at = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos(),
        )
        .expect("nanos");
        drop(hold);
        commit.await.expect("join").expect("commit");
        let seen = std::mem::take(&mut *seen.lock());
        assert!(
            !seen.is_empty(),
            "{shape:?}: the commit evaluated retention"
        );
        assert!(
            seen.iter().all(|&t| t >= released_at),
            "{shape:?}: now() {seen:?} predates the end of the lock wait {released_at}"
        );
    }
}

/// Stage `rows` and commit; the error text of whichever step refused.
async fn stage_and_commit(f: &Fixture, rows: &[(i64, i64)]) -> std::result::Result<(), String> {
    let p = &f.provider;
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let values: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    let txn = CayenneTransaction::new();
    let token = p.transaction_write_token().await;
    txn.register(
        p.table_id().to_string(),
        token,
        p.clone_for_write_operations(),
    );
    let staged = p
        .begin_staged_upsert_occ(
            token,
            single_batch_stream(id_value_batch(schema(), &ids, &values)),
            1,
        )
        .await
        .map_err(|e| format!("staging: {e}"))?;
    txn.set_staged(p.table_id(), staged);
    txn.commit()
        .await
        .map(|_| ())
        .map_err(|e| format!("commit: {e}"))
}

/// OBSERVED semantics, pinned (run-99, 2026-11-11 09:22 PST): a transaction
/// on a primary key without `on_conflict` does NOT refuse a repeated key.
/// The premise that it fails validation (true on the non-transactional append
/// route) is false here; whether the routes should agree is an open decision,
/// and the 9aa41988c baseline for this route is not yet measured. What holds:
/// - within the statement the FIRST copy is kept and published, the later copy
///   is hidden and never evaluated; the kept copy is the published row, so
///   `retention_sql` evaluates it (an error on it fails the commit and
///   publishes nothing);
/// - over a resident row the staged copy is hidden, never evaluated, and the
///   resident row is not hidden;
/// - the table accepts the next valid commit (nothing held).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_no_conflict_key_transaction_keeps_the_first_copy_and_never_evaluates_hidden_ones() {
    for shape in [Shape::PkNoConflictKey, Shape::PkNoConflictPosition] {
        // Within the statement, retention OFF: the first copy is published.
        let f = table(
            &format!("ctr_nc_within_off_{shape:?}").to_lowercase(),
            shape,
        )
        .await;
        stage_and_commit(&f, &[(5, 13), (5, 7)])
            .await
            .unwrap_or_else(|e| panic!("{shape:?}: accepted: {e}"));
        assert_eq!(
            rows(&f.provider).await,
            vec![(1, 1), (5, 13)],
            "{shape:?}: first copy"
        );
        assert_eq!(
            rows(&f.reopen().await).await,
            vec![(1, 1), (5, 13)],
            "{shape:?}: reopened"
        );

        // Within the statement, MIXED: only the kept copy is evaluated; its
        // error fails the commit and nothing is published.
        let f = table(
            &format!("ctr_nc_within_mixed_{shape:?}").to_lowercase(),
            shape,
        )
        .await;
        f.set(MIXED);
        let refused = stage_and_commit(&f, &[(5, 13), (5, 7)])
            .await
            .expect_err("the kept copy fails retention");
        assert!(
            refused.contains("aged() cannot evaluate 13"),
            "{shape:?}: {refused}"
        );
        assert_eq!(
            f.take_seen(),
            vec![13],
            "{shape:?}: the hidden copy is never evaluated"
        );
        assert_eq!(
            rows(&f.provider).await,
            vec![(1, 1)],
            "{shape:?}: nothing published"
        );
        assert_eq!(
            rows(&f.reopen().await).await,
            vec![(1, 1)],
            "{shape:?}: reopened"
        );

        // Over a resident row: the staged copy is hidden and never evaluated,
        // the resident row stays.
        for (case, staged) in [("resident", (1, 13)), ("resident_matching", (1, -1))] {
            let f = table(&format!("ctr_nc_{case}_{shape:?}").to_lowercase(), shape).await;
            f.set(MIXED);
            stage_and_commit(&f, &[staged])
                .await
                .unwrap_or_else(|e| panic!("{shape:?}/{case}: accepted: {e}"));
            assert_eq!(
                f.take_seen(),
                Vec::<i64>::new(),
                "{shape:?}/{case}: nothing evaluated"
            );
            assert_eq!(
                rows(&f.provider).await,
                vec![(1, 1)],
                "{shape:?}/{case}: resident kept"
            );
            assert_eq!(
                rows(&f.reopen().await).await,
                vec![(1, 1)],
                "{shape:?}/{case}: reopened"
            );
            f.set(OFF);
            stage_and_commit(&f, &[(6, 6)])
                .await
                .unwrap_or_else(|e| panic!("{shape:?}/{case}: next commit: {e}"));
            assert_eq!(
                rows(&f.provider).await,
                vec![(1, 1), (6, 6)],
                "{shape:?}/{case}: next commit"
            );
        }
    }
}
