/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Append and full refresh against a `PostgreSQL` source.
//!
//! [`test_refresh_append_for_engine`] and [`test_refresh_full_for_engine`] compare the rows
//! Spice returns with the same SQL executed by `PostgreSQL`: the table projection, one
//! aggregation, and one self-join. `created_at` is compared as UTC epoch milliseconds.
//!
//! [`upsert_history_matches_postgres_source`] drives append refresh with `on_conflict: upsert`
//! through inserts and primary-key updates, including NULL transitions, and compares the
//! rows after each refresh with the same SQL executed by `PostgreSQL`.
//!
//! [`append_time_format_matches_postgres_source`] drives append refresh with each
//! `time_format` (`timestamp`, `unix_seconds`, `iso8601`) through an insert of two rows that
//! share one `created_at` and a refresh that finds nothing new, and compares the rows after
//! each refresh with the same SQL executed by `PostgreSQL`.

use crate::acceleration::refresh::common::{
    execute_ps_sql, execute_rt_sql, get_acceleration_config_append, get_acceleration_config_full,
    get_dataset, initialize_postgres_labeled, initialize_postgres_time_format,
    initialize_postgres_with_value_column, refresh_table, start_test_runtime,
    start_test_runtime_with_dataset,
};
use crate::postgres::common;
use crate::{init_tracing, utils::test_request_context};
use arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection;
use runtime::Runtime;
use spicepod::acceleration::Acceleration;
use spicepod::component::dataset::TimeFormat;
use spicepod::param::Params;
use std::sync::Arc;
use tokio_postgres::types::Type;

/// Same text on both sides. No `ORDER BY`: the comparison is a multiset, so collation and
/// NULL ordering cannot decide it. None of these statements divide integers.
const PROJECTION_SQL: &str = "SELECT * FROM test_table";
const AGG_SQL: &str = "SELECT COUNT(*) AS row_count, SUM(n) AS sum_n, MIN(n) AS min_n, MAX(n) AS max_n FROM test_table";
const JOIN_SQL: &str = "SELECT a.id AS id, a.n AS left_n, b.label AS right_label FROM test_table AS a INNER JOIN test_table AS b ON a.id = b.id";

/// The upsert history's statements, over `initialize_postgres_with_value_column`'s table.
/// The same rules hold: no `ORDER BY`, no integer division. `GROUP BY value` puts the NULL
/// values in one group on both engines.
const UPSERT_PROJECTION_SQL: &str = "SELECT id, created_at, value FROM test_table";
const UPSERT_AGG_SQL: &str = "SELECT COUNT(*) AS row_count, COUNT(DISTINCT id) AS distinct_ids, COUNT(value) AS non_null_values, MIN(id) AS min_id, MAX(id) AS max_id FROM test_table";
const UPSERT_GROUP_SQL: &str = "SELECT value, COUNT(*) AS row_count FROM test_table GROUP BY value";

/// The `time_format` histories' statements, over `initialize_postgres_time_format`'s table.
/// The same rules hold. `MIN`/`MAX` of an ISO8601 `created_at` compare fixed-width strings
/// that differ only in digits, so no collation can order them differently.
const TIME_FORMAT_PROJECTION_SQL: &str = "SELECT id, created_at, n, label FROM test_table";
const TIME_FORMAT_AGG_SQL: &str = "SELECT COUNT(*) AS row_count, COUNT(DISTINCT id) AS distinct_ids, COUNT(label) AS non_null_labels, SUM(n) AS sum_n, MIN(created_at) AS min_created_at, MAX(created_at) AS max_created_at FROM test_table";
const TIME_FORMAT_GROUP_SQL: &str =
    "SELECT label, COUNT(*) AS row_count FROM test_table GROUP BY label";

#[derive(Clone, Copy)]
pub(crate) enum RefreshCase {
    Append,
    Full,
}

impl RefreshCase {
    fn label(self) -> &'static str {
        match self {
            RefreshCase::Append => "append",
            RefreshCase::Full => "full",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Int,
    Text,
}

/// `created_at` is stored here as UTC epoch milliseconds, not as a timestamp display.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Int(i64),
    Text(String),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Answer {
    columns: Vec<String>,
    kinds: Vec<Kind>,
    rows: Vec<Vec<Cell>>,
}

/// Get acceleration parameters for postgres engine
fn get_postgres_acceleration_params(port: usize) -> Params {
    let mut params = common::get_pg_params(port);
    // Override pg_db to use the acceleration database instead of the default
    params.insert(
        "pg_db".to_string(),
        secrecy::SecretString::from("acceleration".to_string()),
    );

    Params::from_string_map(
        params
            .into_iter()
            .map(|(k, v)| (k, secrecy::ExposeSecret::expose_secret(&v).to_string()))
            .collect(),
    )
}

async fn test_refresh_append_for_engine(engine: &str) -> Result<(), anyhow::Error> {
    test_refresh_matches_source(engine, RefreshCase::Append).await
}

async fn test_refresh_full_for_engine(engine: &str) -> Result<(), anyhow::Error> {
    test_refresh_matches_source(engine, RefreshCase::Full).await
}

async fn test_refresh_matches_source(engine: &str, mode: RefreshCase) -> Result<(), anyhow::Error> {
    refresh_matches_postgres_source(engine, mode, |port| {
        let acceleration_params =
            (engine == "postgres").then(|| get_postgres_acceleration_params(port));
        acceleration_for(engine, mode, acceleration_params)
    })
    .await
}

async fn test_upsert_history_for_engine(engine: &str) -> Result<(), anyhow::Error> {
    upsert_history_matches_postgres_source(engine, |port| {
        let acceleration_params =
            (engine == "postgres").then(|| get_postgres_acceleration_params(port));
        get_acceleration_config_append(engine, acceleration_params)
    })
    .await
}

async fn test_time_format_for_engine(
    engine: &str,
    format: TimeFormatCase,
) -> Result<(), anyhow::Error> {
    append_time_format_matches_postgres_source(engine, format, |_port| {
        get_acceleration_config_append(engine, None)
    })
    .await
}

/// Append or full refresh of `engine` against the labeled `PostgreSQL` source.
///
/// `configure` builds the acceleration after the container port is known. The
/// comparison is the projection, one aggregation, and one self-join, each as a
/// schema and a row multiset. `created_at` is UTC epoch milliseconds. None of
/// the statements order rows or divide integers.
pub(crate) async fn refresh_matches_postgres_source(
    engine: &str,
    mode: RefreshCase,
    configure: impl FnOnce(usize) -> Acceleration,
) -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async move {
            let running_container = common::start_postgres_docker_container().await?;
            let port = usize::from(running_container.host_port(5432)?);
            let (db_conn, instants) = initialize_postgres_labeled(port).await?;

            let acceleration_config = configure(port);
            let rt = start_test_runtime(port, acceleration_config).await?;
            let mode_name = mode.label();

            compare_stage(
                &Stage {
                    engine,
                    mode: mode_name,
                    name: "initial",
                },
                &db_conn,
                &rt,
                &[(10, "alpha")],
                &[instants.alpha.timestamp_millis()],
                (1, 10, 10, 10),
            )
            .await?;

            insert_labeled(&db_conn, instants.beta, 20, "beta").await?;
            refresh_table(Arc::clone(&rt), "test_table").await?;

            let snapshot = compare_stage(
                &Stage {
                    engine,
                    mode: mode_name,
                    name: "refreshed",
                },
                &db_conn,
                &rt,
                &[(10, "alpha"), (20, "beta")],
                &[
                    instants.alpha.timestamp_millis(),
                    instants.beta.timestamp_millis(),
                ],
                (2, 30, 10, 20),
            )
            .await?;

            // The source moves again and Spice is not refreshed. A scan of the acceleration
            // still returns `snapshot`. A scan of the federated source returns the probe row.
            insert_labeled(&db_conn, instants.probe, -1, "not-refreshed").await?;
            assert_acceleration_held(
                engine,
                mode_name,
                PROJECTION_SQL,
                &db_conn,
                &rt,
                &snapshot,
                |answer| column_has_int(answer, "n", -1),
            )
            .await?;

            running_container.cleanup().await;
            Ok(())
        })
        .await
}

/// Append refresh with `on_conflict: upsert` on `id`, against the `PostgreSQL` source.
///
/// The source history is the initial row, then two inserts (one with a NULL `value`), then
/// three updates that reuse primary keys: text replaced by text, text replaced by NULL, and
/// NULL replaced by text. Each change moves `created_at` past every earlier one, so the append
/// refresh fetches it. After the load and after each refresh, the projection, one
/// aggregation, and one grouped count must match `PostgreSQL` as a schema and a row multiset.
/// A probe row inserted after the last refresh must stay out of the acceleration.
pub(crate) async fn upsert_history_matches_postgres_source(
    engine: &str,
    configure: impl FnOnce(usize) -> Acceleration,
) -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async move {
            let running_container = common::start_postgres_docker_container().await?;
            let port = usize::from(running_container.host_port(5432)?);
            let db_conn = initialize_postgres_with_value_column(port).await?;

            let rt = start_test_runtime(port, configure(port)).await?;
            let stage = |name| Stage {
                engine,
                mode: "append-upsert",
                name,
            };

            compare_upsert_stage(
                &stage("initial"),
                &db_conn,
                &rt,
                &[(1, Some("initial_value"))],
            )
            .await?;

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at, value) VALUES \
                 (date_trunc('milliseconds', now()) + interval '1 second', 'second_value'), \
                 (date_trunc('milliseconds', now()) + interval '2 seconds', NULL)",
            )
            .await?;
            refresh_table(Arc::clone(&rt), "test_table").await?;
            compare_upsert_stage(
                &stage("inserted"),
                &db_conn,
                &rt,
                &[
                    (1, Some("initial_value")),
                    (2, Some("second_value")),
                    (3, None),
                ],
            )
            .await?;

            execute_ps_sql(
                &db_conn,
                "UPDATE test_table SET \
                 created_at = date_trunc('milliseconds', now()) + interval '3 seconds' + id * interval '1 second', \
                 value = CASE id WHEN 1 THEN 'updated_value' WHEN 2 THEN NULL ELSE 'third_value' END",
            )
            .await?;
            refresh_table(Arc::clone(&rt), "test_table").await?;
            let snapshot = compare_upsert_stage(
                &stage("upserted"),
                &db_conn,
                &rt,
                &[
                    (1, Some("updated_value")),
                    (2, None),
                    (3, Some("third_value")),
                ],
            )
            .await?;

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at, value) VALUES \
                 (date_trunc('milliseconds', now()) + interval '10 seconds', 'not-refreshed')",
            )
            .await?;
            assert_acceleration_held(
                engine,
                "append-upsert",
                UPSERT_PROJECTION_SQL,
                &db_conn,
                &rt,
                &snapshot,
                |answer| {
                    Ok(int_text_pairs(answer, "id", "value")?
                        .contains(&(4, Some("not-refreshed".to_string()))))
                },
            )
            .await?;

            running_container.cleanup().await;
            Ok(())
        })
        .await
}

async fn compare_upsert_stage(
    stage: &Stage<'_>,
    db_conn: &PostgresConnection,
    rt: &Arc<Runtime>,
    expected_pairs: &[(i64, Option<&str>)],
) -> Result<Answer, anyhow::Error> {
    compare_history_stage(
        stage,
        db_conn,
        rt,
        [UPSERT_PROJECTION_SQL, UPSERT_AGG_SQL, UPSERT_GROUP_SQL],
        |postgres_projection| {
            assert_inserted_pairs(stage, postgres_projection, ("id", "value"), expected_pairs)
        },
    )
    .await
}

/// Compares the projection, aggregation and group statements in `sqls`, in that order, after
/// `pin_oracle` checks `PostgreSQL`'s projection holds what the history wrote. Returns Spice's
/// projection.
async fn compare_history_stage(
    stage: &Stage<'_>,
    db_conn: &PostgresConnection,
    rt: &Arc<Runtime>,
    [projection_sql, agg_sql, group_sql]: [&str; 3],
    pin_oracle: impl FnOnce(&Answer) -> Result<(), anyhow::Error>,
) -> Result<Answer, anyhow::Error> {
    let postgres_projection = answer_from_postgres(db_conn, projection_sql).await?;
    let spice_projection = answer_from_spice(rt, projection_sql).await?;
    pin_oracle(&postgres_projection)?;
    assert_same_multiset(
        stage.engine,
        stage.mode,
        stage.name,
        "projection",
        projection_sql,
        &spice_projection,
        &postgres_projection,
    );

    for (query_name, sql) in [("aggregation", agg_sql), ("group", group_sql)] {
        let postgres = answer_from_postgres(db_conn, sql).await?;
        let spice = answer_from_spice(rt, sql).await?;
        assert_same_multiset(
            stage.engine,
            stage.mode,
            stage.name,
            query_name,
            sql,
            &spice,
            &postgres,
        );
    }

    Ok(spice_projection)
}

/// A `(created_at, n, label)` row a time-format history wrote to `PostgreSQL`.
type WrittenRow<'a> = (chrono::DateTime<chrono::Utc>, i64, Option<&'a str>);

/// How an append history's `created_at` is stored in `PostgreSQL` and declared to Spice.
#[derive(Clone, Copy)]
pub(crate) enum TimeFormatCase {
    /// `TIMESTAMP(3) WITH TIME ZONE`, `time_format: timestamp`, compared as UTC epoch
    /// milliseconds. Keeps the append config's `primary_key` and `on_conflict: upsert`.
    Timestamp,
    /// `BIGINT` seconds since the epoch, `time_format: unix_seconds`. No primary key.
    UnixSeconds,
    /// `TEXT` such as `2026-11-01T01:00:00.000000`, `time_format: iso8601`, compared as the
    /// string. No primary key.
    Iso8601,
}

impl TimeFormatCase {
    fn label(self) -> &'static str {
        match self {
            TimeFormatCase::Timestamp => "append-timestamp",
            TimeFormatCase::UnixSeconds => "append-unix-seconds",
            TimeFormatCase::Iso8601 => "append-iso8601",
        }
    }

    fn column_type(self) -> &'static str {
        match self {
            TimeFormatCase::Timestamp => "TIMESTAMP(3) WITH TIME ZONE",
            TimeFormatCase::UnixSeconds => "BIGINT",
            TimeFormatCase::Iso8601 => "TEXT",
        }
    }

    /// Fits `acceleration` to the column type: a `refresh_sql` window in the column's own
    /// encoding, and no primary key for the integer and string formats.
    fn configure(self, acceleration: &mut Acceleration) {
        let refresh_sql = match self {
            TimeFormatCase::Timestamp => return,
            TimeFormatCase::UnixSeconds => {
                "select * from test_table where created_at > extract(epoch from now() - INTERVAL '10 years')::bigint"
            }
            TimeFormatCase::Iso8601 => {
                "select * from test_table where created_at > to_char(now() - INTERVAL '10 years', 'YYYY-MM-DD\"T\"HH24:MI:SS.US')"
            }
        };
        acceleration.refresh_sql = Some(refresh_sql.to_string());
        acceleration.primary_key = None;
        acceleration.on_conflict = std::collections::HashMap::new();
        acceleration.indexes = std::collections::HashMap::new();
    }

    fn time_format(self) -> TimeFormat {
        match self {
            TimeFormatCase::Timestamp => TimeFormat::Timestamp,
            TimeFormatCase::UnixSeconds => TimeFormat::UnixSeconds,
            TimeFormatCase::Iso8601 => TimeFormat::ISO8601,
        }
    }

    fn iso8601(at: chrono::DateTime<chrono::Utc>) -> String {
        at.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
    }

    /// The cell `answer_from_postgres` reads back for `at`.
    fn cell(self, at: chrono::DateTime<chrono::Utc>) -> Cell {
        match self {
            TimeFormatCase::Timestamp => Cell::Int(at.timestamp_millis()),
            TimeFormatCase::UnixSeconds => Cell::Int(at.timestamp()),
            TimeFormatCase::Iso8601 => Cell::Text(Self::iso8601(at)),
        }
    }

    async fn insert(
        self,
        db_conn: &PostgresConnection,
        at: chrono::DateTime<chrono::Utc>,
        n: i32,
        label: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let created_at: Box<dyn tokio_postgres::types::ToSql + Sync> = match self {
            TimeFormatCase::Timestamp => Box::new(at),
            TimeFormatCase::UnixSeconds => Box::new(at.timestamp()),
            TimeFormatCase::Iso8601 => Box::new(Self::iso8601(at)),
        };
        let inserted = db_conn
            .conn
            .execute(
                "INSERT INTO test_table (created_at, n, label) VALUES ($1, $2, $3)",
                &[created_at.as_ref(), &n, &label],
            )
            .await
            .map_err(|e| anyhow::anyhow!("Error inserting into test_table: {e}"))?;
        anyhow::ensure!(
            inserted == 1,
            "expected to insert 1 row into test_table, inserted {inserted}"
        );
        Ok(())
    }
}

/// Append refresh with `format`'s `time_column` against the `PostgreSQL` source.
///
/// The history is one row, then two rows that share a later `created_at` (one with a NULL
/// `label`), then a refresh with nothing new. After the load and after each refresh, the
/// projection, one aggregation over every column including `created_at`, and one grouped
/// count must match `PostgreSQL` as a schema and a row multiset. A row the source gained
/// twice, lost, or held with another value fails. A probe row inserted after the last
/// refresh must stay out of the acceleration.
///
/// `configure` builds the engine's acceleration after the container port is known.
/// `format` then sets `refresh_sql` and drops the primary key where it requires.
pub(crate) async fn append_time_format_matches_postgres_source(
    engine: &str,
    format: TimeFormatCase,
    configure: impl FnOnce(usize) -> Acceleration,
) -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async move {
            let running_container = common::start_postgres_docker_container().await?;
            let port = usize::from(running_container.host_port(5432)?);
            let db_conn = initialize_postgres_time_format(port, format.column_type()).await?;

            // Whole hours inside the `refresh_sql` window, so every encoding is exact.
            let first = (chrono::Utc::now().date_naive() - chrono::Days::new(2))
                .and_time(chrono::NaiveTime::MIN)
                .and_utc();
            let later = first + chrono::TimeDelta::hours(1);
            let probe = first + chrono::TimeDelta::hours(2);

            format.insert(&db_conn, first, 10, Some("alpha")).await?;

            let mut acceleration = configure(port);
            format.configure(&mut acceleration);
            let mut dataset = get_dataset(port);
            dataset.time_format = Some(format.time_format());
            let rt = start_test_runtime_with_dataset(port, acceleration, dataset).await?;
            let stage = |name| Stage {
                engine,
                mode: format.label(),
                name,
            };

            compare_time_format_stage(
                &stage("initial"),
                format,
                &db_conn,
                &rt,
                &[(first, 10, Some("alpha"))],
            )
            .await?;

            format.insert(&db_conn, later, 20, Some("beta")).await?;
            format.insert(&db_conn, later, 30, None).await?;
            refresh_table(Arc::clone(&rt), "test_table").await?;
            let appended = [
                (first, 10, Some("alpha")),
                (later, 20, Some("beta")),
                (later, 30, None),
            ];
            compare_time_format_stage(&stage("appended"), format, &db_conn, &rt, &appended).await?;

            refresh_table(Arc::clone(&rt), "test_table").await?;
            let snapshot = compare_time_format_stage(
                &stage("refreshed-unchanged"),
                format,
                &db_conn,
                &rt,
                &appended,
            )
            .await?;

            format
                .insert(&db_conn, probe, -1, Some("not-refreshed"))
                .await?;
            assert_acceleration_held(
                engine,
                format.label(),
                TIME_FORMAT_PROJECTION_SQL,
                &db_conn,
                &rt,
                &snapshot,
                |answer| column_has_int(answer, "n", -1),
            )
            .await?;

            running_container.cleanup().await;
            Ok(())
        })
        .await
}

async fn compare_time_format_stage(
    stage: &Stage<'_>,
    format: TimeFormatCase,
    db_conn: &PostgresConnection,
    rt: &Arc<Runtime>,
    written: &[WrittenRow<'_>],
) -> Result<Answer, anyhow::Error> {
    compare_history_stage(
        stage,
        db_conn,
        rt,
        [
            TIME_FORMAT_PROJECTION_SQL,
            TIME_FORMAT_AGG_SQL,
            TIME_FORMAT_GROUP_SQL,
        ],
        |postgres_projection| {
            assert_written_time_format_rows(stage, format, postgres_projection, written)
        },
    )
    .await
}

/// Pins the oracle: `PostgreSQL` holds exactly the `(created_at, n, label)` rows written.
fn assert_written_time_format_rows(
    stage: &Stage<'_>,
    format: TimeFormatCase,
    postgres: &Answer,
    written: &[WrittenRow<'_>],
) -> Result<(), anyhow::Error> {
    let indexes = [
        column_index(postgres, "created_at")?,
        column_index(postgres, "n")?,
        column_index(postgres, "label")?,
    ];
    let mut held: Vec<Vec<Cell>> = postgres
        .rows
        .iter()
        .map(|row| indexes.iter().map(|&idx| row[idx].clone()).collect())
        .collect();
    held.sort();
    let mut expected: Vec<Vec<Cell>> = written
        .iter()
        .map(|(at, n, label)| {
            vec![
                format.cell(*at),
                Cell::Int(*n),
                label.map_or(Cell::Null, |label| Cell::Text(label.to_string())),
            ]
        })
        .collect();
    expected.sort();
    anyhow::ensure!(
        held == expected,
        "{} {} {}: PostgreSQL does not hold the written (created_at, n, label) rows\n  postgres: {held:?}\n  written: {expected:?}",
        stage.engine,
        stage.mode,
        stage.name
    );
    Ok(())
}

fn acceleration_for(
    engine: &str,
    mode: RefreshCase,
    acceleration_params: Option<Params>,
) -> Acceleration {
    match mode {
        RefreshCase::Append => get_acceleration_config_append(engine, acceleration_params),
        RefreshCase::Full => get_acceleration_config_full(engine, acceleration_params),
    }
}

async fn insert_labeled(
    db_conn: &PostgresConnection,
    created_at: chrono::DateTime<chrono::Utc>,
    n: i32,
    label: &str,
) -> Result<(), anyhow::Error> {
    let inserted = db_conn
        .conn
        .execute(
            "INSERT INTO test_table (created_at, n, label) VALUES ($1, $2, $3)",
            &[&created_at, &n, &label],
        )
        .await
        .map_err(|e| anyhow::anyhow!("Error inserting into test_table: {e}"))?;
    anyhow::ensure!(
        inserted == 1,
        "expected to insert 1 row into test_table, inserted {inserted}"
    );
    Ok(())
}

struct Stage<'a> {
    engine: &'a str,
    mode: &'a str,
    name: &'a str,
}

async fn compare_stage(
    stage: &Stage<'_>,
    db_conn: &PostgresConnection,
    rt: &Arc<Runtime>,
    expected_pairs: &[(i64, &str)],
    expected_epochs_ms: &[i64],
    agg: (i64, i64, i64, i64),
) -> Result<Answer, anyhow::Error> {
    let postgres_projection = answer_from_postgres(db_conn, PROJECTION_SQL).await?;
    let spice_projection = answer_from_spice(rt, PROJECTION_SQL).await?;
    let expected_pairs: Vec<(i64, Option<&str>)> = expected_pairs
        .iter()
        .map(|(n, label)| (*n, Some(*label)))
        .collect();
    assert_inserted_pairs(stage, &postgres_projection, ("n", "label"), &expected_pairs)?;
    assert_epochs(
        stage.engine,
        stage.mode,
        stage.name,
        &postgres_projection,
        expected_epochs_ms,
    )?;
    assert_same_multiset(
        stage.engine,
        stage.mode,
        stage.name,
        "projection",
        PROJECTION_SQL,
        &spice_projection,
        &postgres_projection,
    );

    let postgres_agg = answer_from_postgres(db_conn, AGG_SQL).await?;
    let spice_agg = answer_from_spice(rt, AGG_SQL).await?;
    assert_agg_literals(stage.engine, stage.mode, stage.name, &postgres_agg, agg)?;
    assert_same_multiset(
        stage.engine,
        stage.mode,
        stage.name,
        "aggregation",
        AGG_SQL,
        &spice_agg,
        &postgres_agg,
    );

    let postgres_join = answer_from_postgres(db_conn, JOIN_SQL).await?;
    let spice_join = answer_from_spice(rt, JOIN_SQL).await?;
    assert_join_echoes_source(
        stage.engine,
        stage.mode,
        stage.name,
        &postgres_join,
        &postgres_projection,
    )?;
    assert_same_multiset(
        stage.engine,
        stage.mode,
        stage.name,
        "join",
        JOIN_SQL,
        &spice_join,
        &postgres_join,
    );

    Ok(spice_projection)
}

/// `has_probe` recognizes the probe row inserted into `PostgreSQL` after the last refresh.
/// `sql` must be the statement that produced `snapshot`.
async fn assert_acceleration_held(
    engine: &str,
    mode: &str,
    sql: &str,
    db_conn: &PostgresConnection,
    rt: &Arc<Runtime>,
    snapshot: &Answer,
    has_probe: impl Fn(&Answer) -> Result<bool, anyhow::Error>,
) -> Result<(), anyhow::Error> {
    let postgres = answer_from_postgres(db_conn, sql).await?;
    let spice = answer_from_spice(rt, sql).await?;
    anyhow::ensure!(
        has_probe(&postgres)?,
        "{engine} {mode}: the probe row did not land in PostgreSQL\n  postgres: {postgres:?}"
    );
    anyhow::ensure!(
        !has_probe(snapshot)?,
        "{engine} {mode}: the pre-probe snapshot already contained the probe row\n  snapshot: {snapshot:?}"
    );
    assert!(
        !spice.rows.is_empty(),
        "{engine} {mode}: Spice returned no rows after the probe insert\n  spice: {spice:?}"
    );
    assert_eq!(
        sorted_rows(&spice),
        sorted_rows(snapshot),
        "{engine} {mode}: Spice returned the row inserted into PostgreSQL after refresh, so this query did not read the acceleration\n  spice: {spice:?}\n  snapshot: {snapshot:?}\n  postgres: {postgres:?}"
    );
    eprintln!(
        "compared engine={engine} mode={mode} stage=post-refresh-probe rows={} held acceleration snapshot",
        spice.rows.len()
    );
    Ok(())
}

fn assert_same_multiset(
    engine: &str,
    mode: &str,
    stage: &str,
    query_name: &str,
    sql: &str,
    spice: &Answer,
    postgres: &Answer,
) {
    // A `timestamp` `created_at` is read as UTC epoch milliseconds; Unix seconds and
    // ISO8601 strings are compared as stored.
    let note = if query_name == "projection"
        && !matches!(mode, "append-unix-seconds" | "append-iso8601")
    {
        " (created_at cells are UTC epoch milliseconds)"
    } else {
        ""
    };
    assert!(
        !spice.rows.is_empty(),
        "{engine} {mode} {stage} {query_name}: Spice returned no rows{note}\n  sql: {sql}\n  spice: {spice:?}\n  postgres: {postgres:?}"
    );
    assert!(
        !postgres.rows.is_empty(),
        "{engine} {mode} {stage} {query_name}: PostgreSQL returned no rows{note}\n  sql: {sql}\n  spice: {spice:?}\n  postgres: {postgres:?}"
    );
    assert_eq!(
        (&spice.columns, &spice.kinds),
        (&postgres.columns, &postgres.kinds),
        "{engine} {mode} {stage} {query_name}: schema differs{note}\n  sql: {sql}\n  spice: {spice:?}\n  postgres: {postgres:?}"
    );
    assert_eq!(
        sorted_rows(spice),
        sorted_rows(postgres),
        "{engine} {mode} {stage} {query_name}: rows differ{note}\n  sql: {sql}\n  spice: {:?}\n  postgres: {:?}",
        sorted_rows(spice),
        sorted_rows(postgres),
    );
    eprintln!(
        "compared engine={engine} mode={mode} stage={stage} query={query_name} rows={} columns={:?} spice={:?} postgres={:?}",
        spice.rows.len(),
        spice.columns,
        sorted_rows(spice),
        sorted_rows(postgres),
    );
}

fn assert_inserted_pairs(
    stage: &Stage<'_>,
    postgres: &Answer,
    (int_column, text_column): (&str, &str),
    expected_pairs: &[(i64, Option<&str>)],
) -> Result<(), anyhow::Error> {
    let pairs = int_text_pairs(postgres, int_column, text_column)?;
    let mut expected: Vec<(i64, Option<String>)> = expected_pairs
        .iter()
        .map(|(n, text)| (*n, text.map(str::to_string)))
        .collect();
    expected.sort();
    anyhow::ensure!(
        pairs == expected,
        "{} {} {}: PostgreSQL does not hold the written ({int_column}, {text_column}) rows\n  postgres: {pairs:?}\n  written: {expected:?}",
        stage.engine,
        stage.mode,
        stage.name
    );
    Ok(())
}

fn assert_epochs(
    engine: &str,
    mode: &str,
    stage: &str,
    postgres: &Answer,
    expected_epochs_ms: &[i64],
) -> Result<(), anyhow::Error> {
    let mut epochs = column_ints(postgres, "created_at")?;
    let mut expected = expected_epochs_ms.to_vec();
    epochs.sort_unstable();
    expected.sort_unstable();
    anyhow::ensure!(
        epochs == expected,
        "{engine} {mode} {stage}: PostgreSQL stored a different created_at instant than the one inserted\n  postgres_epoch_ms: {epochs:?}\n  inserted_epoch_ms: {expected:?}"
    );
    Ok(())
}

fn assert_agg_literals(
    engine: &str,
    mode: &str,
    stage: &str,
    postgres: &Answer,
    expected: (i64, i64, i64, i64),
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        postgres.rows.len() == 1,
        "{engine} {mode} {stage}: PostgreSQL aggregation returned {} rows: {postgres:?}",
        postgres.rows.len()
    );
    let row = &postgres.rows[0];
    let got = (
        int_at(postgres, row, "row_count")?,
        int_at(postgres, row, "sum_n")?,
        int_at(postgres, row, "min_n")?,
        int_at(postgres, row, "max_n")?,
    );
    anyhow::ensure!(
        got == expected,
        "{engine} {mode} {stage}: PostgreSQL aggregation {got:?} != inserted {expected:?}\n  {postgres:?}"
    );
    Ok(())
}

fn assert_join_echoes_source(
    engine: &str,
    mode: &str,
    stage: &str,
    join_answer: &Answer,
    projection: &Answer,
) -> Result<(), anyhow::Error> {
    let joined = int_text_pairs(join_answer, "left_n", "right_label")?;
    let projected = int_text_pairs(projection, "n", "label")?;
    anyhow::ensure!(
        joined == projected,
        "{engine} {mode} {stage}: PostgreSQL self-join does not echo the source rows\n  join: {joined:?}\n  source: {projected:?}"
    );
    Ok(())
}

fn sorted_rows(answer: &Answer) -> Vec<Vec<Cell>> {
    let mut rows = answer.rows.clone();
    rows.sort();
    rows
}

fn column_index(answer: &Answer, name: &str) -> Result<usize, anyhow::Error> {
    answer
        .columns
        .iter()
        .position(|column| column == name)
        .ok_or_else(|| anyhow::anyhow!("column '{name}' not in {:?}", answer.columns))
}

fn column_has_int(answer: &Answer, name: &str, value: i64) -> Result<bool, anyhow::Error> {
    let idx = column_index(answer, name)?;
    Ok(answer
        .rows
        .iter()
        .any(|row| row.get(idx) == Some(&Cell::Int(value))))
}

fn column_ints(answer: &Answer, name: &str) -> Result<Vec<i64>, anyhow::Error> {
    let idx = column_index(answer, name)?;
    answer
        .rows
        .iter()
        .map(|row| match row.get(idx) {
            Some(Cell::Int(value)) => Ok(*value),
            Some(other) => anyhow::bail!("column '{name}' is {other:?}, not an integer"),
            None => anyhow::bail!("column '{name}' missing from row {row:?}"),
        })
        .collect()
}

fn int_at(answer: &Answer, row: &[Cell], name: &str) -> Result<i64, anyhow::Error> {
    let idx = column_index(answer, name)?;
    match row.get(idx) {
        Some(Cell::Int(value)) => Ok(*value),
        Some(other) => anyhow::bail!("column '{name}' is {other:?}, not an integer"),
        None => anyhow::bail!("column '{name}' missing from row {row:?}"),
    }
}

/// `(int, text)` pairs sorted by value. A NULL text cell is `None`.
fn int_text_pairs(
    answer: &Answer,
    int_column: &str,
    text_column: &str,
) -> Result<Vec<(i64, Option<String>)>, anyhow::Error> {
    let int_idx = column_index(answer, int_column)?;
    let text_idx = column_index(answer, text_column)?;
    let mut pairs = Vec::with_capacity(answer.rows.len());
    for row in &answer.rows {
        let Cell::Int(n) = row[int_idx] else {
            anyhow::bail!("column '{int_column}' is not an integer: {row:?}");
        };
        let text = match &row[text_idx] {
            Cell::Text(text) => Some(text.clone()),
            Cell::Null => None,
            Cell::Int(_) => anyhow::bail!("column '{text_column}' is not text: {row:?}"),
        };
        pairs.push((n, text));
    }
    pairs.sort();
    Ok(pairs)
}

async fn answer_from_spice(rt: &Arc<Runtime>, sql: &str) -> Result<Answer, anyhow::Error> {
    let batches = execute_rt_sql(Arc::clone(rt), sql).await?;
    answer_from_batches(&batches)
}

fn answer_from_batches(batches: &[RecordBatch]) -> Result<Answer, anyhow::Error> {
    let Some(first) = batches.first() else {
        return Ok(Answer {
            columns: Vec::new(),
            kinds: Vec::new(),
            rows: Vec::new(),
        });
    };
    let schema = first.schema();
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let kinds = schema
        .fields()
        .iter()
        .map(|field| kind_for(field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows == 0 {
        return Ok(Answer {
            columns,
            kinds,
            rows: Vec::new(),
        });
    }
    let batch = arrow::compute::concat_batches(&schema, batches)
        .map_err(|e| anyhow::anyhow!("concat Spice batches: {e}"))?;
    let mut column_cells = Vec::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        column_cells.push(cells_for_column(field.name(), batch.column(idx))?);
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let row = column_cells
            .iter()
            .map(|column| column[row_idx].clone())
            .collect();
        rows.push(row);
    }
    Ok(Answer {
        columns,
        kinds,
        rows,
    })
}

async fn answer_from_postgres(
    db_conn: &PostgresConnection,
    sql: &str,
) -> Result<Answer, anyhow::Error> {
    let statement = db_conn
        .conn
        .prepare(sql)
        .await
        .map_err(|e| anyhow::anyhow!("prepare PostgreSQL sql: {e}\n  sql: {sql}"))?;
    let rows = db_conn
        .conn
        .query(&statement, &[])
        .await
        .map_err(|e| anyhow::anyhow!("query PostgreSQL: {e}\n  sql: {sql}"))?;
    let columns: Vec<String> = statement
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect();
    let kinds = statement
        .columns()
        .iter()
        .map(|column| kind_for_pg(column.type_()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut table_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut cells = Vec::with_capacity(columns.len());
        for idx in 0..columns.len() {
            cells.push(pg_cell(row, idx)?);
        }
        table_rows.push(cells);
    }
    Ok(Answer {
        columns,
        kinds,
        rows: table_rows,
    })
}

fn kind_for(data_type: &DataType) -> Result<Kind, anyhow::Error> {
    let base = match data_type {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    };
    match base {
        DataType::Timestamp(_, _)
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(Kind::Int),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(Kind::Text),
        other => anyhow::bail!("unsupported Spice type {other}"),
    }
}

fn kind_for_pg(ty: &Type) -> Result<Kind, anyhow::Error> {
    if *ty == Type::INT2 || *ty == Type::INT4 || *ty == Type::INT8 || *ty == Type::TIMESTAMPTZ {
        Ok(Kind::Int)
    } else if *ty == Type::TEXT || *ty == Type::VARCHAR || *ty == Type::BPCHAR {
        Ok(Kind::Text)
    } else {
        anyhow::bail!("unsupported PostgreSQL type {}", ty.name())
    }
}

fn pg_cell(row: &tokio_postgres::Row, idx: usize) -> Result<Cell, anyhow::Error> {
    let ty = row.columns()[idx].type_().clone();
    if ty == Type::INT2 {
        let value: Option<i16> = row.try_get(idx)?;
        return Ok(value.map_or(Cell::Null, |value| Cell::Int(i64::from(value))));
    }
    if ty == Type::INT4 {
        let value: Option<i32> = row.try_get(idx)?;
        return Ok(value.map_or(Cell::Null, |value| Cell::Int(i64::from(value))));
    }
    if ty == Type::INT8 {
        let value: Option<i64> = row.try_get(idx)?;
        return Ok(value.map_or(Cell::Null, Cell::Int));
    }
    if ty == Type::TEXT || ty == Type::VARCHAR || ty == Type::BPCHAR {
        let value: Option<String> = row.try_get(idx)?;
        return Ok(value.map_or(Cell::Null, Cell::Text));
    }
    if ty == Type::TIMESTAMPTZ {
        let value: Option<chrono::DateTime<chrono::Utc>> = row.try_get(idx)?;
        return match value {
            None => Ok(Cell::Null),
            Some(ts) => timestamptz_cell(ts),
        };
    }
    anyhow::bail!("unsupported PostgreSQL type {} at column {idx}", ty.name())
}

fn timestamptz_cell(ts: chrono::DateTime<chrono::Utc>) -> Result<Cell, anyhow::Error> {
    let subsec_nanos = ts.timestamp_subsec_nanos();
    anyhow::ensure!(
        subsec_nanos.is_multiple_of(1_000_000),
        "PostgreSQL timestamptz {ts} is not millisecond-aligned (subsecond nanos {subsec_nanos})"
    );
    Ok(Cell::Int(ts.timestamp_millis()))
}

fn cells_for_column(name: &str, array: &ArrayRef) -> Result<Vec<Cell>, anyhow::Error> {
    let materialized = unpack_dictionary(array)?;
    let data_type = materialized.data_type().clone();
    if let DataType::Timestamp(unit, _) = data_type {
        return timestamp_cells(name, materialized.as_ref(), unit);
    }
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => int_cells(name, &materialized),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            text_cells(name, &materialized)
        }
        other => anyhow::bail!("column '{name}' has unsupported type {other}"),
    }
}

fn unpack_dictionary(array: &ArrayRef) -> Result<ArrayRef, anyhow::Error> {
    if let DataType::Dictionary(_, value) = array.data_type() {
        return arrow::compute::cast(array.as_ref(), value.as_ref())
            .map_err(|e| anyhow::anyhow!("unpack dictionary column {}: {e}", array.data_type()));
    }
    Ok(Arc::clone(array))
}

fn timestamp_cells(
    name: &str,
    array: &dyn Array,
    unit: TimeUnit,
) -> Result<Vec<Cell>, anyhow::Error> {
    timestamp_i64s(name, array)?
        .into_iter()
        .map(|value| match value {
            None => Ok(Cell::Null),
            Some(ticks) => Ok(Cell::Int(epoch_millis(ticks, unit)?)),
        })
        .collect()
}

fn timestamp_i64s(name: &str, array: &dyn Array) -> Result<Vec<Option<i64>>, anyhow::Error> {
    if let Some(values) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(optional_i64(values));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(optional_i64(values));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(optional_i64(values));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(optional_i64(values));
    }
    anyhow::bail!(
        "column '{name}' is {}, not a timestamp array",
        array.data_type()
    )
}

fn optional_i64<T>(values: &arrow::array::PrimitiveArray<T>) -> Vec<Option<i64>>
where
    T: arrow::datatypes::ArrowPrimitiveType<Native = i64>,
{
    (0..values.len())
        .map(|idx| {
            if values.is_null(idx) {
                None
            } else {
                Some(values.value(idx))
            }
        })
        .collect()
}

fn epoch_millis(ticks: i64, unit: TimeUnit) -> Result<i64, anyhow::Error> {
    match unit {
        TimeUnit::Second => ticks
            .checked_mul(1_000)
            .ok_or_else(|| anyhow::anyhow!("timestamp {ticks} seconds overflows milliseconds")),
        TimeUnit::Millisecond => Ok(ticks),
        TimeUnit::Microsecond => exact_millis(ticks, 1_000, unit),
        TimeUnit::Nanosecond => exact_millis(ticks, 1_000_000, unit),
    }
}

fn exact_millis(ticks: i64, divisor: i64, unit: TimeUnit) -> Result<i64, anyhow::Error> {
    let remainder = ticks
        .checked_rem(divisor)
        .ok_or_else(|| anyhow::anyhow!("timestamp {ticks} {unit:?} remainder overflow"))?;
    anyhow::ensure!(
        remainder == 0,
        "timestamp {ticks} {unit:?} is not an exact number of milliseconds"
    );
    ticks
        .checked_div(divisor)
        .ok_or_else(|| anyhow::anyhow!("timestamp {ticks} {unit:?} division overflow"))
}

fn int_cells(name: &str, array: &ArrayRef) -> Result<Vec<Cell>, anyhow::Error> {
    let casted = arrow::compute::cast(array.as_ref(), &DataType::Int64).map_err(|e| {
        anyhow::anyhow!(
            "column '{name}' type {} is not an integer: {e}",
            array.data_type()
        )
    })?;
    let Some(values) = casted.as_any().downcast_ref::<Int64Array>() else {
        anyhow::bail!("column '{name}' Int64 cast did not produce an Int64Array");
    };
    Ok((0..values.len())
        .map(|idx| {
            if values.is_null(idx) {
                Cell::Null
            } else {
                Cell::Int(values.value(idx))
            }
        })
        .collect())
}

fn text_cells(name: &str, array: &ArrayRef) -> Result<Vec<Cell>, anyhow::Error> {
    let casted = arrow::compute::cast(array.as_ref(), &DataType::Utf8).map_err(|e| {
        anyhow::anyhow!(
            "column '{name}' type {} is not text: {e}",
            array.data_type()
        )
    })?;
    let Some(values) = casted.as_any().downcast_ref::<StringArray>() else {
        anyhow::bail!("column '{name}' Utf8 cast did not produce a StringArray");
    };
    Ok((0..values.len())
        .map(|idx| {
            if values.is_null(idx) {
                Cell::Null
            } else {
                Cell::Text(values.value(idx).to_string())
            }
        })
        .collect())
}

// ============================================================================
// Test variants for each accelerator engine
// ============================================================================

// Arrow (always available)
#[tokio::test]
async fn test_acceleration_refresh_arrow_append_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("arrow").await
}

#[tokio::test]
async fn test_acceleration_refresh_arrow_append_timestamp() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_time_format_for_engine("arrow", TimeFormatCase::Timestamp).await
}

#[tokio::test]
async fn test_acceleration_refresh_arrow_append_unix_seconds() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_time_format_for_engine("arrow", TimeFormatCase::UnixSeconds).await
}

#[tokio::test]
async fn test_acceleration_refresh_arrow_append_iso8601() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_time_format_for_engine("arrow", TimeFormatCase::Iso8601).await
}

#[tokio::test]
async fn test_acceleration_refresh_arrow_append_upsert_history() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_upsert_history_for_engine("arrow").await
}

#[tokio::test]
async fn test_acceleration_refresh_arrow_full_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("arrow").await
}

// Cayenne memory-mode refreshes
#[tokio::test]
async fn test_acceleration_refresh_cayenne_memory_append_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("cayenne").await
}

#[tokio::test]
async fn test_acceleration_refresh_cayenne_memory_append_upsert_history()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_upsert_history_for_engine("cayenne").await
}

#[tokio::test]
async fn test_acceleration_refresh_cayenne_memory_full_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("cayenne").await
}

#[tokio::test]
async fn test_acceleration_refresh_cayenne_memory_append_timestamp() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_time_format_for_engine("cayenne", TimeFormatCase::Timestamp).await
}

#[tokio::test]
async fn test_acceleration_refresh_cayenne_memory_append_unix_seconds() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_time_format_for_engine("cayenne", TimeFormatCase::UnixSeconds).await
}

#[tokio::test]
async fn test_acceleration_refresh_cayenne_memory_append_iso8601() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_time_format_for_engine("cayenne", TimeFormatCase::Iso8601).await
}

// Postgres (feature-gated)
#[cfg(feature = "postgres-accel")]
#[tokio::test]
async fn test_acceleration_refresh_postgres_append_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("postgres").await
}

#[cfg(feature = "postgres-accel")]
#[tokio::test]
async fn test_acceleration_refresh_postgres_append_upsert_history() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_upsert_history_for_engine("postgres").await
}

#[cfg(feature = "postgres-accel")]
#[tokio::test]
async fn test_acceleration_refresh_postgres_full_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("postgres").await
}

// Cayenne
// Note: Cayenne requires mode: file in addition to refresh_mode: append/full.
// These tests need custom helpers that set both mode and refresh_mode.
#[tokio::test]
#[ignore = "Cayenne requires mode: file which is not set by the generic test helpers"]
async fn test_acceleration_refresh_cayenne_append_variant() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("cayenne")
        .await
        .expect("Test should pass when mode: file is properly configured");
}

#[tokio::test]
#[ignore = "Cayenne requires mode: file which is not set by the generic test helpers"]
async fn test_acceleration_refresh_cayenne_full_variant() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("cayenne")
        .await
        .expect("Test should pass when mode: file is properly configured");
}
