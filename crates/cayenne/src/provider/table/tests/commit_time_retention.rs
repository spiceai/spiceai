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
                if archived && mode == BROKEN {
                    return Err(datafusion_common::DataFusionError::Execution(
                        "aged() cannot evaluate this row".to_string(),
                    ));
                }
                out.push(match mode {
                    NULLS => None,
                    _ => Some(archived && mode == ON),
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
    Unkeyed,
}

const SHAPES: [Shape; 3] = [Shape::KeyedFold, Shape::KeyedPosition, Shape::Unkeyed];

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
            on_conflict: keyed.then(|| {
                OnConflict::Upsert(
                    datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                        "id".to_string(),
                    ]),
                )
            }),
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
        // Counts as when staging decided: a folding table and a table without a
        // primary key report the rows kept; a table hiding by position reports
        // the rows written, the hidden one counted superseded.
        let reported = if shape == Shape::KeyedPosition { 2 } else { 1 };
        assert_eq!(committed.row_count, reported, "{shape:?}: reported rows");

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
        let txn = stage(&f, &[(5, -50), (5, 5), (6, -6)]).await;
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
