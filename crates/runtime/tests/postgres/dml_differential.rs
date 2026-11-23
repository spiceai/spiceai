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

//! Differential test of `INSERT`, `UPDATE`, and `DELETE` through Spice against
//! `PostgreSQL` running the same statements itself.
//!
//! Three `PostgreSQL` tables start from the same rows. The oracle table takes
//! each statement straight from `PostgreSQL`, which shares no code with Spice.
//! The two subject tables take it through Spice: one as a federated dataset
//! with `access: read_write`, and one as a Cayenne acceleration with
//! `write_mode: write_through`, which Spice also writes to `PostgreSQL` and then
//! reloads with a refresh. After every statement:
//!
//! - the count Spice returns must equal the rows `PostgreSQL` reports changing
//!   in the oracle table;
//! - each subject table, read straight from `PostgreSQL`, must hold exactly the
//!   oracle table's rows, every column in `PostgreSQL`'s own text form, so a
//!   predicate Spice translated wrongly, a value it converted wrongly, or a row
//!   it touched that it should not have shows up as a differing row;
//! - each subject dataset, read through Spice, must return the oracle table's
//!   rows, so the federated read and the reloaded Cayenne acceleration agree
//!   with what was written.
//!
//! The history holds what a translation gets wrong: NULLs under `NOT IN`, `IS
//! DISTINCT FROM`, and three-valued `NOT`, `LIKE` with escaped wildcards,
//! fractional-second timestamp bounds, `bigint` boundaries, `numeric`
//! arithmetic, `SET` expressions that read the row, and inserts of NULLs and
//! boundary values. The tables live in a database whose sessions default to a
//! time zone west of UTC, so a literal rendered as a `timestamptz` changes the
//! rows a statement writes.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::TableReference;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::{
    acceleration::{Acceleration, RefreshMode, WriteMode},
    component::{access::AccessMode, dataset::Dataset},
    param::Params,
};
use tokio_postgres::SimpleQueryMessage;

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context},
};

/// The database the tables live in. Its sessions default to [`TIME_ZONE`].
const DATABASE: &str = "dml_differential";

/// A zone west of UTC with no daylight saving time. `PostgreSQL` reads a
/// `timestamptz` in the session's time zone wherever it meets a `timestamp` or
/// becomes a `date`, so a statement that renders a literal as a `timestamptz`
/// writes different rows here than at UTC, where CI's servers run. Fixing the
/// zone makes such a statement fail on every server, not only on one whose
/// default zone happens to be west of UTC.
const TIME_ZONE: &str = "Pacific/Honolulu";

/// The `PostgreSQL` table the statements run on directly.
const ORACLE: &str = "dml_oracle";

/// The columns of the fixture, as `CREATE TABLE` declares them.
const COLUMNS: &str = "id integer PRIMARY KEY, grp integer, qty bigint, name text, \
     price numeric(10, 2), flag boolean, d date, ts timestamp";

/// The rows every table starts from.
const SEED_ROWS: &str = r"
   (1, 1, 10, 'apple', 1.50, true, '2024-01-01', '2024-01-01 00:00:00'),
   (2, 1, -5, 'Apple', 2.25, false, '2024-02-29', '2024-02-29 12:30:00.5'),
   (3, 2, NULL, 'banana', NULL, NULL, NULL, NULL),
   (4, NULL, 0, NULL, 0.00, true, '1999-12-31', '1999-12-31 23:59:59.999999'),
   (5, 2, 9223372036854775807, 'a_b', 99999999.99, false, '2024-03-01', '2024-03-01 08:00:00'),
   (6, 3, -9223372036854775808, 'a%b', -0.01, true, '2023-12-31', '2023-12-31 23:59:59'),
   (7, 3, 7, '', 3.33, NULL, '2024-02-01', '2024-02-01 00:00:00.000001'),
   (8, NULL, NULL, ' padded ', -3.33, false, NULL, '2024-06-15 18:45:00'),
   (9, 2, 42, 'ümlaut', 10.00, true, '2024-02-15', NULL),
   (10, 4, 3, 'O''Brien', 0.10, NULL, '2000-01-01', '2000-01-01 00:00:00'),
   (11, 4, 3, 'aB', 1.50, true, '2024-01-31', '2024-01-31 23:59:59.5'),
   (12, 1, 100, 'apple', 2.25, false, '2024-02-29', '2024-02-29 12:30:00'),
   (13, 5, -1, 'zeta', NULL, true, '1970-01-01', '1970-01-01 00:00:00'),
   (14, 5, 2147483648, 'Zeta', 7.77, false, '2038-01-19', '2038-01-19 03:14:08'),
   (15, NULL, 15, 'back\slash', 15.15, NULL, '2024-12-31', '2024-12-31 23:59:59'),
   (16, 6, -2147483649, 'a\%b', -99999999.99, false, '2024-07-04', '2024-07-04 12:00:00')";

/// One statement of the history, with `{t}` standing for the table.
struct Step {
    sql: &'static str,
    /// Whether changing no row is the right answer. Every other step must
    /// change rows in the oracle table, so a statement that matched nothing on
    /// every side cannot pass for agreement.
    none: bool,
    /// Whether Spice refuses the statement today, because its `PostgreSQL`
    /// writer cannot render one of the expressions in it. A refusal is an
    /// error, not a wrong answer: the table must be left as it was, and the
    /// statement then runs on the table directly so the next step starts from
    /// the oracle's rows. A step marked here that Spice starts to run fails
    /// too, so the mark comes off and the step is compared in full.
    refused: bool,
}

const fn changes(sql: &'static str) -> Step {
    Step {
        sql,
        none: false,
        refused: false,
    }
}

const fn no_change(sql: &'static str) -> Step {
    Step {
        sql,
        none: true,
        refused: false,
    }
}

impl Step {
    const fn refused(self) -> Self {
        Self {
            refused: true,
            ..self
        }
    }
}

/// The statements, applied in order to every table. Each one is valid, and
/// means the same, in `PostgreSQL` and in Spice's SQL dialect.
const HISTORY: &[Step] = &[
    // Predicates whose NULL semantics a translation can lose.
    no_change("DELETE FROM {t} WHERE grp NOT IN (1, NULL)").refused(),
    no_change("UPDATE {t} SET qty = 0 WHERE NOT (grp = 1 OR grp IS NULL) AND grp IS NULL")
        .refused(),
    changes("DELETE FROM {t} WHERE grp IS NULL AND qty IS NULL").refused(),
    changes("UPDATE {t} SET grp = COALESCE(grp, 0) * 10 WHERE grp IS DISTINCT FROM 2").refused(),
    changes(
        "UPDATE {t} SET flag = NOT flag WHERE flag IS NOT NULL AND grp IS NOT DISTINCT FROM 10",
    )
    .refused(),
    changes("UPDATE {t} SET name = 'was-null' WHERE name IS NULL").refused(),
    // Integer boundaries and arithmetic that reads the row.
    changes("UPDATE {t} SET qty = qty + 1 WHERE grp IN (10, 30) AND qty BETWEEN -100 AND 100"),
    changes("UPDATE {t} SET qty = -qty WHERE qty = 9223372036854775807").refused(),
    changes("UPDATE {t} SET qty = qty % 7 WHERE qty > 2147483647 OR qty < -2147483648"),
    // Text: pattern matching with escaped wildcards, functions in SET.
    changes(r"UPDATE {t} SET name = upper(name) WHERE name LIKE 'a\_b'"),
    changes(r"DELETE FROM {t} WHERE name LIKE 'a\%b'"),
    changes(r"UPDATE {t} SET grp = 99 WHERE name LIKE '%\\%'"),
    changes("UPDATE {t} SET name = name || '!' WHERE length(name) > 5"),
    changes("UPDATE {t} SET name = rtrim(name, '!') WHERE name LIKE '%!' AND grp = 2"),
    changes("UPDATE {t} SET name = 'O''Neil' WHERE name LIKE '%''%'"),
    // numeric(10, 2).
    changes("UPDATE {t} SET price = NULL WHERE price < 0").refused(),
    changes("UPDATE {t} SET price = price * 2 WHERE price > 1.5 AND price < 1000").refused(),
    changes("DELETE FROM {t} WHERE price = 99999999.99"),
    // Dates and fractional-second timestamps.
    changes("UPDATE {t} SET d = DATE '2025-01-01' WHERE ts < TIMESTAMP '2000-01-01 00:00:00'"),
    changes("DELETE FROM {t} WHERE ts >= TIMESTAMP '2024-02-29 12:30:00.5'"),
    changes(
        "UPDATE {t} SET qty = 1 WHERE ts > TIMESTAMP '1999-12-31 23:59:59.999998' AND ts < TIMESTAMP '2000-01-01 00:00:00'",
    ),
    // Inserts: NULLs, boundaries, a quote, an empty string, an explicit column list.
    changes("INSERT INTO {t} VALUES (17, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"),
    changes(
        "INSERT INTO {t} (id, name, price, qty) VALUES \
         (18, 'x''y', 12.34, -9223372036854775808), (19, '', -0.01, 9223372036854775807)",
    ),
    changes("UPDATE {t} SET grp = 7, flag = false WHERE id >= 17 AND grp IS NULL").refused(),
    // Statements that change every row, and none.
    changes("UPDATE {t} SET qty = COALESCE(qty, -1)").refused(),
    no_change("DELETE FROM {t} WHERE id > 1000"),
    changes("DELETE FROM {t} WHERE flag"),
];

/// A row as its cells, each rendered, or `None` for NULL.
type Row = Vec<Option<String>>;

/// One side that takes the history through Spice.
struct Subject {
    name: &'static str,
    /// The `PostgreSQL` table the dataset writes to, and the dataset's name.
    table: &'static str,
    /// Whether the dataset is a Cayenne acceleration that Spice reloads with a
    /// refresh after each statement.
    accelerated: bool,
}

const SUBJECTS: [Subject; 2] = [
    Subject {
        name: "federated",
        table: "dml_federated",
        accelerated: false,
    },
    Subject {
        name: "cayenne write_through",
        table: "dml_cayenne",
        accelerated: true,
    },
];

fn dataset(port: usize, subject: &Subject) -> Dataset {
    let mut ds = Dataset::new(
        format!("postgres:{}", subject.table),
        subject.table.to_string(),
    );
    ds.access = AccessMode::ReadWrite;
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .chain([("pg_db".to_string(), DATABASE.to_string())])
            .collect::<HashMap<String, String>>(),
    ));
    if subject.accelerated {
        ds.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            refresh_mode: Some(RefreshMode::Full),
            write_mode: WriteMode::WriteThrough,
            ..Acceleration::default()
        });
    }
    ds
}

/// A connection to [`DATABASE`].
async fn connect(port: u16) -> Result<tokio_postgres::Client, anyhow::Error> {
    let (client, connection) = tokio_postgres::Config::new()
        .host("localhost")
        .port(port)
        .user("postgres")
        .password(common::PG_PASSWORD)
        .dbname(DATABASE)
        .connect(tokio_postgres::NoTls)
        .await?;
    tokio::spawn(async move {
        let _: Result<(), tokio_postgres::Error> = connection.await;
    });
    Ok(client)
}

/// Every row of `table`, read straight from `PostgreSQL` in its own text form.
fn source_rows_sql(table: &str) -> String {
    format!("SELECT * FROM {table} ORDER BY id")
}

/// The columns a Spice read and a `PostgreSQL` read render alike: integers and
/// text as they are, `numeric` and `date` cast to text, `boolean` as an
/// integer. The `timestamp` column, which the two render differently, is
/// compared on the `PostgreSQL` side by [`source_rows_sql`].
fn shared_rows_sql(table: &str) -> String {
    format!(
        "SELECT id, grp, qty, name, CAST(price AS VARCHAR), \
         CASE WHEN flag THEN 1 WHEN NOT flag THEN 0 END, CAST(d AS VARCHAR) \
         FROM {table} ORDER BY id"
    )
}

/// `sql` sent to `PostgreSQL` over the simple query protocol, which returns
/// every cell in `PostgreSQL`'s own text form.
async fn postgres_rows(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<Row>, String> {
    let messages = client.simple_query(sql).await.map_err(|e| e.to_string())?;
    Ok(messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect())
}

/// `sql` run by `PostgreSQL`, with the number of rows it reports changing.
async fn postgres_changed(client: &tokio_postgres::Client, sql: &str) -> Result<u64, String> {
    let messages = client.simple_query(sql).await.map_err(|e| e.to_string())?;
    messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::CommandComplete(changed) => Some(changed),
            _ => None,
        })
        .ok_or_else(|| format!("PostgreSQL reported no row count for `{sql}`"))
}

fn render(batches: &[RecordBatch]) -> Result<Vec<Row>, String> {
    let options = FormatOptions::default();
    let mut rendered = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        for row in 0..batch.num_rows() {
            rendered.push(
                batch
                    .columns()
                    .iter()
                    .zip(&formatters)
                    .map(|(column, formatter)| {
                        (!column.is_null(row)).then(|| formatter.value(row).to_string())
                    })
                    .collect(),
            );
        }
    }
    Ok(rendered)
}

async fn spice_rows(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<Row>, String> {
    let batches = run_query(rt, sql).await.map_err(|e| e.to_string())?;
    render(&batches)
}

/// The count a Spice DML statement returns: one row, one `count` cell.
async fn spice_changed(rt: &Arc<Runtime>, sql: &str) -> Result<u64, String> {
    let rows = spice_rows(rt, sql).await?;
    match rows.as_slice() {
        [row] => match row.as_slice() {
            [Some(count)] => count
                .parse()
                .map_err(|e| format!("count `{count}` is not a number: {e}")),
            _ => Err(format!("expected one count cell, got {row:?}")),
        },
        _ => Err(format!("expected one count row, got {rows:?}")),
    }
}

async fn refresh(rt: &Runtime, name: &str) -> Result<(), anyhow::Error> {
    rt.datafusion()
        .refresh_table(&TableReference::from(name), None)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{name} returned no refresh notifier"))?
        .wait()
        .await;
    Ok(())
}

/// Checks every subject against the oracle table, pushing each disagreement
/// onto `failures` under `label`.
async fn compare_tables(
    rt: &Arc<Runtime>,
    client: &tokio_postgres::Client,
    label: &str,
    failures: &mut Vec<String>,
) -> Result<(), anyhow::Error> {
    let oracle_source = postgres_rows(client, &source_rows_sql(ORACLE))
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let oracle_shared = postgres_rows(client, &shared_rows_sql(ORACLE))
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    for subject in &SUBJECTS {
        let name = subject.name;
        let source = postgres_rows(client, &source_rows_sql(subject.table))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        if source != oracle_source {
            failures.push(format!(
                "{name} after {label}\n    the PostgreSQL table differs\n    spice wrote: {source:?}\n    postgres:    {oracle_source:?}"
            ));
        }
        match spice_rows(rt, &shared_rows_sql(subject.table)).await {
            Ok(read) if read == oracle_shared => {}
            Ok(read) => failures.push(format!(
                "{name} after {label}\n    the Spice read differs\n    spice read: {read:?}\n    postgres:   {oracle_shared:?}"
            )),
            Err(e) => failures.push(format!("{name} after {label}\n    read failed: {e}")),
        }
    }
    Ok(())
}

#[tokio::test]
async fn dml_through_spice_matches_postgres() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (port, container) = common::test_database().await?;
            let admin = common::connect(u16::try_from(port)?).await?;
            admin
                .batch_execute(&format!("DROP DATABASE IF EXISTS {DATABASE} WITH (FORCE)"))
                .await?;
            admin.batch_execute(&format!("CREATE DATABASE {DATABASE}")).await?;
            admin
                .batch_execute(&format!("ALTER DATABASE {DATABASE} SET timezone TO '{TIME_ZONE}'"))
                .await?;
            let client = connect(u16::try_from(port)?).await?;
            let zone = postgres_rows(&client, "SHOW timezone")
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            assert_eq!(zone, vec![vec![Some(TIME_ZONE.to_string())]]);
            for table in std::iter::once(ORACLE).chain(SUBJECTS.iter().map(|s| s.table)) {
                client
                    .batch_execute(&format!(
                        "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({COLUMNS}); \
                         INSERT INTO {table} VALUES {SEED_ROWS}"
                    ))
                    .await?;
            }

            register_test_connectors().await;
            let mut app = AppBuilder::new("postgres_dml_differential");
            for subject in &SUBJECTS {
                app = app.with_dataset(dataset(port, subject));
            }
            let app = app.build();
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => {
                    anyhow::bail!("Timed out waiting for the datasets to load");
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // Every side must start from the whole fixture before any statement
            // counts.
            let mut failures = Vec::new();
            compare_tables(&rt, &client, "loading", &mut failures).await?;
            anyhow::ensure!(
                failures.is_empty(),
                "the sides disagree before any statement:\n{}",
                failures.join("\n")
            );
            let seeded = postgres_rows(&client, &format!("SELECT count(*) FROM {ORACLE}"))
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            assert_eq!(seeded, vec![vec![Some("16".to_string())]]);

            let mut changed_rows = 0_u64;
            let mut refused = Vec::new();
            for (i, step) in HISTORY.iter().enumerate() {
                let label = format!("step {}: {}", i + 1, step.sql);
                let oracle_sql = step.sql.replace("{t}", ORACLE);
                let expected = postgres_changed(&client, &oracle_sql)
                    .await
                    .map_err(|e| anyhow::anyhow!("PostgreSQL rejected `{oracle_sql}`: {e}"))?;
                if (expected == 0) != step.none {
                    failures.push(format!(
                        "{label}\n    PostgreSQL changed {expected} rows, but the step expects {}",
                        if step.none { "none" } else { "some" }
                    ));
                }
                changed_rows += expected;
                for subject in &SUBJECTS {
                    let sql = step.sql.replace("{t}", subject.table);
                    let before = postgres_rows(&client, &source_rows_sql(subject.table))
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;
                    match spice_changed(&rt, &sql).await {
                        Ok(_) if step.refused => failures.push(format!(
                            "{} after {label}\n    Spice now runs this statement: remove its `.refused()` mark so it is compared",
                            subject.name
                        )),
                        Ok(count) if count == expected => {}
                        Ok(count) => failures.push(format!(
                            "{} after {label}\n    Spice reports {count} rows changed, PostgreSQL {expected}",
                            subject.name
                        )),
                        Err(e) => {
                            // A refusal must leave the table as it was. The statement then
                            // runs on the table directly, so the next step starts from the
                            // oracle's rows and is judged on its own.
                            let after = postgres_rows(&client, &source_rows_sql(subject.table))
                                .await
                                .map_err(|e| anyhow::anyhow!(e))?;
                            if after != before {
                                failures.push(format!(
                                    "{} after {label}\n    Spice refused the statement but changed the table: {e}\n    before: {before:?}\n    after:  {after:?}",
                                    subject.name
                                ));
                            }
                            if !step.refused {
                                failures.push(format!(
                                    "{} after {label}\n    failed: {e}",
                                    subject.name
                                ));
                            }
                            refused.push(format!("{} step {}: {e}", subject.name, i + 1));
                            postgres_changed(&client, &sql)
                                .await
                                .map_err(|e| anyhow::anyhow!("PostgreSQL rejected `{sql}`: {e}"))?;
                        }
                    }
                    if subject.accelerated {
                        refresh(&rt, subject.table).await?;
                    }
                }
                compare_tables(&rt, &client, &label, &mut failures).await?;
            }
            eprintln!(
                "postgres dml differential: {} statements, {} rows changed in the oracle table, {} subjects, {} failures",
                HISTORY.len(),
                changed_rows,
                SUBJECTS.len(),
                failures.len()
            );
            eprintln!("refused:\n{}", refused.join("\n"));

            rt.shutdown().await;
            if let Some(container) = container {
                container.cleanup().await;
            }
            anyhow::ensure!(
                failures.is_empty(),
                "{} disagreements with PostgreSQL:\n{}",
                failures.len(),
                failures.join("\n")
            );
            Ok(())
        })
        .await
}
