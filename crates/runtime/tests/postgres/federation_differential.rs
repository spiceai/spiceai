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

//! Differential test of `PostgreSQL` federation against `PostgreSQL` itself.
//!
//! Each query runs over the same committed rows: sent straight to
//! `PostgreSQL` (the oracle, which shares no code with Spice), against the
//! federated datasets, whose plans Spice rewrites and sends to `PostgreSQL`,
//! and against accelerations of the same tables, loaded with
//! `refresh_mode: full` after the connector converts the rows: Cayenne
//! `mode: memory`, Cayenne `mode: file` with its defaults, which keeps a load
//! this small as rows inlined in its metastore, Cayenne `mode: file` with
//! inlining turned off, which writes and reads Vortex files, Cayenne
//! `mode: file` partitioned by `grp`, and, with the `sqlite` feature, `SQLite`
//! `mode: memory` and `mode: file`. Spice sends each query over a `SQLite` acceleration to `SQLite`
//! as SQL in `SQLite`'s dialect, so those sides check that translation, and
//! must read nothing but `SQLite`. The Cayenne sides read the rows
//! back through Cayenne's own scan, with filters and projections pushed into
//! it, so a value Cayenne stores, encodes, or prunes wrongly shows up as a row
//! that differs from `PostgreSQL`'s. The fixture holds the values
//! a translation gets wrong: NULLs in every column, `bigint` and `numeric`
//! boundaries, `LIKE` wildcards as literal characters, non-ASCII and padded
//! text, fractional-second timestamps, and duplicate join keys.
//!
//! The queries state what the two dialects would otherwise disagree on: NULL
//! placement in every `ORDER BY`, and no text ordering or text `MIN`/`MAX`,
//! which `PostgreSQL` decides by the database collation and `DataFusion` by
//! byte order. Each answer is rendered as text cell by cell — the `PostgreSQL`
//! side through the simple query protocol — so the projected values stay to
//! integers, text, and `numeric` and `date` cast to `VARCHAR`, whose text forms
//! the two engines share.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use app::AppBuilder;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
    partitioning::PartitionedBy,
};
use tokio_postgres::SimpleQueryMessage;

use crate::{
    acceleration::has_vortex_file,
    configure_test_datafusion,
    federation_differential::{
        Case, PlanCheck, Row, agreement, check_cayenne_sources, check_federated, check_sources,
        empty, operators, ordered, physical_plan, rows, spice_rows,
    },
    init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

/// The rows of the fixture table, so a load that dropped some fails up front.
const ITEM_ROWS: usize = 16;

const SEED: &[&str] = &[
    "DROP TABLE IF EXISTS diff_items",
    "DROP TABLE IF EXISTS diff_regions",
    "CREATE TABLE diff_items (\
       id integer PRIMARY KEY, grp integer, qty bigint, name text, \
       price numeric(10, 2), flag boolean, d date, ts timestamp)",
    "CREATE TABLE diff_regions (grp integer, region text)",
    r"INSERT INTO diff_items VALUES
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
       (16, 6, -2147483649, 'a\%b', -99999999.99, false, '2024-07-04', '2024-07-04 12:00:00')",
    "INSERT INTO diff_regions VALUES \
       (1, 'north'), (2, 'south'), (2, 'south-east'), (3, 'west'), \
       (NULL, 'nowhere'), (7, 'unused')",
];

/// The scans that send SQL to `PostgreSQL`, or to a `SQLite` acceleration.
const REMOTE_SCANS: [&str; 2] = ["VirtualExecutionPlan", "SqlExec"];

fn cases() -> Vec<Case> {
    vec![
        // Comparison and NULL semantics.
        rows("SELECT id FROM {items} WHERE grp = 2"),
        rows("SELECT id FROM {items} WHERE grp <> 2"),
        rows("SELECT id FROM {items} WHERE grp IS NULL"),
        rows("SELECT id FROM {items} WHERE grp IS NOT NULL"),
        rows("SELECT id FROM {items} WHERE grp IN (1, 3)"),
        rows("SELECT id FROM {items} WHERE grp NOT IN (1, 3)"),
        empty("SELECT id FROM {items} WHERE grp NOT IN (1, NULL)"),
        rows("SELECT id FROM {items} WHERE grp BETWEEN 2 AND 4"),
        rows("SELECT id FROM {items} WHERE grp IS DISTINCT FROM 2"),
        rows("SELECT id FROM {items} WHERE grp IS NOT DISTINCT FROM NULL"),
        rows("SELECT id FROM {items} WHERE NOT (grp = 1 OR grp IS NULL)"),
        rows("SELECT id FROM {items} WHERE (grp = 1 OR qty > 10) AND name IS NOT NULL"),
        rows("SELECT id FROM {items} WHERE COALESCE(grp, -1) = -1"),
        rows("SELECT id FROM {items} WHERE CASE WHEN grp > 2 THEN 'hi' ELSE 'lo' END = 'lo'"),
        // Integer boundaries and arithmetic.
        rows("SELECT id FROM {items} WHERE qty > 0"),
        rows("SELECT id FROM {items} WHERE qty >= 9223372036854775807"),
        rows("SELECT id FROM {items} WHERE qty < -9223372036854775807"),
        rows("SELECT id FROM {items} WHERE qty = 2147483648"),
        rows("SELECT id FROM {items} WHERE qty < -2147483648"),
        rows("SELECT id FROM {items} WHERE qty % 2 = 1"),
        rows("SELECT id FROM {items} WHERE qty % 2 = -1"),
        rows("SELECT id FROM {items} WHERE grp / 2 = 1"),
        rows("SELECT id, qty + 1 FROM {items} WHERE qty BETWEEN -100 AND 100"),
        rows("SELECT id, abs(qty) FROM {items} WHERE qty > -100 AND qty < 100"),
        // Text: equality, pattern matching, functions. No ordering of text.
        rows("SELECT id FROM {items} WHERE name = 'apple'"),
        rows("SELECT id FROM {items} WHERE name = ''"),
        rows("SELECT id FROM {items} WHERE name <> 'apple'"),
        rows("SELECT id FROM {items} WHERE name LIKE 'a%'"),
        rows("SELECT id FROM {items} WHERE name LIKE 'a_b'"),
        rows(r"SELECT id FROM {items} WHERE name LIKE 'a\_b'"),
        rows(r"SELECT id FROM {items} WHERE name LIKE 'a\%b'"),
        rows(r"SELECT id FROM {items} WHERE name LIKE '%\\%'"),
        rows("SELECT id FROM {items} WHERE name NOT LIKE '%a%'"),
        rows("SELECT id FROM {items} WHERE name ILIKE 'APPLE'"),
        rows("SELECT id FROM {items} WHERE name LIKE '%''%'"),
        rows("SELECT id FROM {items} WHERE upper(name) = 'ZETA'"),
        rows("SELECT id FROM {items} WHERE lower(name) = 'apple'"),
        rows("SELECT id FROM {items} WHERE trim(name) = 'padded'"),
        rows("SELECT id FROM {items} WHERE length(name) > 5"),
        rows("SELECT id, length(name) FROM {items}"),
        rows("SELECT id, name FROM {items}"),
        rows("SELECT id, substr(name, 2, 3) FROM {items}"),
        rows("SELECT id, strpos(name, 'a') FROM {items}"),
        rows("SELECT id, name || '-' || CAST(grp AS VARCHAR) FROM {items}"),
        // No `concat`: Spice's skips no NULL argument where PostgreSQL's does, by
        // a choice issue #13875 tracks, so PostgreSQL cannot judge it. The `||`
        // case above concatenates under semantics the two share.
        // numeric(10, 2).
        rows("SELECT id FROM {items} WHERE price > 2.00"),
        rows("SELECT id FROM {items} WHERE price = 1.5"),
        rows("SELECT id FROM {items} WHERE price < 0"),
        rows("SELECT id FROM {items} WHERE price >= 99999999.99"),
        rows("SELECT id, CAST(price AS VARCHAR) FROM {items}"),
        rows("SELECT sum(price) FROM {items}"),
        rows("SELECT grp, CAST(sum(price) AS VARCHAR) FROM {items} GROUP BY grp"),
        // Booleans, as integers on the way out.
        rows("SELECT id FROM {items} WHERE flag"),
        rows("SELECT id FROM {items} WHERE NOT flag"),
        rows("SELECT id FROM {items} WHERE flag IS NOT TRUE"),
        rows("SELECT id FROM {items} WHERE flag IS DISTINCT FROM true"),
        rows("SELECT id FROM {items} WHERE flag IS UNKNOWN"),
        rows("SELECT id, CASE WHEN flag THEN 1 WHEN NOT flag THEN 0 END FROM {items}"),
        // Dates and timestamps.
        rows("SELECT id FROM {items} WHERE d >= DATE '2024-01-01'"),
        rows("SELECT id FROM {items} WHERE d BETWEEN DATE '2024-02-01' AND DATE '2024-02-29'"),
        rows("SELECT id FROM {items} WHERE d < DATE '2000-01-01'"),
        rows("SELECT id, CAST(d AS VARCHAR) FROM {items}"),
        rows("SELECT id FROM {items} WHERE ts > TIMESTAMP '2024-02-29 12:30:00'"),
        rows("SELECT id FROM {items} WHERE ts >= TIMESTAMP '2024-02-29 12:30:00.5'"),
        rows("SELECT id FROM {items} WHERE ts < TIMESTAMP '2000-01-01 00:00:00'"),
        rows("SELECT id FROM {items} WHERE ts > TIMESTAMP '1999-12-31 23:59:59.999998'"),
        rows("SELECT id FROM {items} WHERE ts IS NULL"),
        // Aggregates.
        rows(
            "SELECT count(*), count(grp), count(DISTINCT grp), sum(grp), min(grp), max(grp), \
             count(name), count(DISTINCT name) FROM {items}",
        ),
        rows(
            "SELECT grp, count(*), sum(qty) FROM {items} \
             WHERE qty BETWEEN -100 AND 100 GROUP BY grp",
        ),
        rows("SELECT grp, count(*) FROM {items} GROUP BY grp HAVING count(*) > 1"),
        rows("SELECT DISTINCT grp FROM {items}"),
        rows("SELECT count(*) FROM {items} WHERE name IS NULL"),
        rows("SELECT min(qty), max(qty), min(d), max(d) FROM {items}"),
        rows("SELECT count(*) FROM {items} WHERE grp > 100"),
        empty("SELECT grp, count(*) FROM {items} WHERE grp > 100 GROUP BY grp"),
        // Sorts and limits, with NULL placement stated and a unique tie-breaker.
        ordered("SELECT id FROM {items} ORDER BY qty DESC NULLS LAST, id"),
        ordered("SELECT id FROM {items} ORDER BY qty ASC NULLS FIRST, id"),
        ordered("SELECT id FROM {items} ORDER BY grp ASC NULLS FIRST, id LIMIT 5"),
        ordered("SELECT id FROM {items} ORDER BY grp DESC NULLS LAST, id DESC LIMIT 5"),
        ordered("SELECT id FROM {items} ORDER BY price DESC NULLS FIRST, id LIMIT 4"),
        ordered("SELECT id FROM {items} ORDER BY ts ASC NULLS LAST, id LIMIT 6"),
        ordered("SELECT id FROM {items} ORDER BY id LIMIT 3 OFFSET 2"),
        ordered("SELECT id, name FROM {items} WHERE name IS NOT NULL ORDER BY id DESC LIMIT 4"),
        // Subqueries, joins, unions, windows.
        rows("SELECT id FROM {items} WHERE grp IN (SELECT grp FROM {regions})"),
        rows(
            "SELECT id FROM {items} WHERE grp NOT IN (SELECT grp FROM {regions} WHERE grp IS NOT NULL)",
        ),
        rows(
            "SELECT id FROM {items} i WHERE EXISTS \
             (SELECT 1 FROM {regions} r WHERE r.grp = i.grp)",
        ),
        rows("SELECT i.id, r.region FROM {items} i JOIN {regions} r ON i.grp = r.grp"),
        rows("SELECT i.id, r.region FROM {items} i LEFT JOIN {regions} r ON i.grp = r.grp"),
        rows("SELECT i.id, r.region FROM {items} i RIGHT JOIN {regions} r ON i.grp = r.grp"),
        rows("SELECT i.id, r.region FROM {items} i FULL JOIN {regions} r ON i.grp = r.grp"),
        rows(
            "SELECT r.region, count(i.id) FROM {regions} r \
             LEFT JOIN {items} i ON i.grp = r.grp GROUP BY r.region",
        ),
        rows("SELECT grp FROM {items} UNION SELECT grp FROM {regions}"),
        rows("SELECT grp FROM {items} UNION ALL SELECT grp FROM {regions}"),
        rows("SELECT grp FROM {items} INTERSECT SELECT grp FROM {regions}"),
        rows("SELECT grp FROM {items} EXCEPT SELECT grp FROM {regions}"),
        rows("SELECT id, row_number() OVER (PARTITION BY grp ORDER BY id) FROM {items}"),
        rows(
            "SELECT id, sum(qty) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM {items} WHERE qty BETWEEN -100 AND 100",
        ),
        rows("SELECT id, count(*) OVER (PARTITION BY grp) FROM {items}"),
    ]
}

/// How a dataset reads `PostgreSQL`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// No acceleration: Spice sends each query to `PostgreSQL`.
    Federated,
    /// A Cayenne acceleration held in memory, loaded once with
    /// `refresh_mode: full`.
    CayenneMemory,
    /// A Cayenne acceleration with a metastore on disk, loaded once with
    /// `refresh_mode: full`, with its default inlining: a load under
    /// `inline_max_rows` is kept as rows in the metastore, not a Vortex file.
    CayenneFile,
    /// [`Kind::CayenneFile`] with `inline_max_rows: 0`, so the load is written
    /// to Vortex files and every scan reads them.
    CayenneVortex,
    /// [`Kind::CayenneFile`] with `partition_by: grp`: one Cayenne table per
    /// `grp` value, NULL included. Cayenne refuses `partition_by` in memory mode.
    CayennePartitioned,
    /// A `SQLite` acceleration held in memory, loaded once with
    /// `refresh_mode: full`. Spice sends each query over it to `SQLite` as SQL.
    #[cfg(feature = "sqlite")]
    SqliteMemory,
    /// [`Kind::SqliteMemory`] in a database file on disk.
    #[cfg(feature = "sqlite")]
    SqliteFile,
}

/// Where a `mode: file` acceleration of dataset `name` keeps its data.
fn data_dir(dir: &Path, name: &str) -> PathBuf {
    dir.join(name).join("data")
}

/// `dir` holds a `mode: file` acceleration's data and metastore, one
/// directory per dataset `name`.
fn dataset(port: usize, table: &str, name: &str, kind: Kind, dir: &Path) -> Dataset {
    let mut ds = Dataset::new(format!("postgres:{table}"), name.to_string());
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    let full = Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    };
    ds.acceleration = match kind {
        Kind::Federated => None,
        Kind::CayenneMemory => Some(Acceleration {
            engine: Some("cayenne".to_string()),
            mode: Mode::Memory,
            ..full
        }),
        Kind::CayenneFile | Kind::CayenneVortex | Kind::CayennePartitioned => {
            let mut params = HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    data_dir(dir, name).display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    dir.join(name).join("metadata").display().to_string(),
                ),
            ]);
            if kind == Kind::CayenneVortex {
                params.insert("cayenne_inline_max_rows".to_string(), "0".to_string());
            }
            let partition_by = if kind == Kind::CayennePartitioned {
                vec![PartitionedBy {
                    name: "grp".to_string(),
                    expression: "grp".to_string(),
                }]
            } else {
                Vec::new()
            };
            Some(Acceleration {
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                params: Some(Params::from_string_map(params)),
                partition_by,
                ..full
            })
        }
        #[cfg(feature = "sqlite")]
        Kind::SqliteMemory => Some(Acceleration {
            engine: Some("sqlite".to_string()),
            mode: Mode::Memory,
            ..full
        }),
        #[cfg(feature = "sqlite")]
        Kind::SqliteFile => Some(Acceleration {
            engine: Some("sqlite".to_string()),
            mode: Mode::File,
            params: Some(Params::from_string_map(HashMap::from([(
                "sqlite_file".to_string(),
                dir.join(format!("{name}.db")).display().to_string(),
            )]))),
            ..full
        }),
    };
    ds
}

/// The oracle: `sql` sent to `PostgreSQL` over the simple query protocol, which
/// returns every cell in `PostgreSQL`'s own text form.
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

/// One Spice side of the comparison: the datasets it reads, and how they read
/// `PostgreSQL`, which decides what its plan must show.
struct Subject {
    name: &'static str,
    items: &'static str,
    regions: &'static str,
    kind: Kind,
}

const SUBJECTS: &[Subject] = &[
    Subject {
        name: "federated",
        items: "fed_items",
        regions: "fed_regions",
        kind: Kind::Federated,
    },
    Subject {
        name: "cayenne memory",
        items: "cmem_items",
        regions: "cmem_regions",
        kind: Kind::CayenneMemory,
    },
    Subject {
        name: "cayenne file",
        items: "cfile_items",
        regions: "cfile_regions",
        kind: Kind::CayenneFile,
    },
    Subject {
        name: "cayenne vortex",
        items: "cvortex_items",
        regions: "cvortex_regions",
        kind: Kind::CayenneVortex,
    },
    Subject {
        name: "cayenne partitioned",
        items: "cpart_items",
        regions: "cpart_regions",
        kind: Kind::CayennePartitioned,
    },
    #[cfg(feature = "sqlite")]
    Subject {
        name: "sqlite memory",
        items: "smem_items",
        regions: "smem_regions",
        kind: Kind::SqliteMemory,
    },
    #[cfg(feature = "sqlite")]
    Subject {
        name: "sqlite file",
        items: "sfile_items",
        regions: "sfile_regions",
        kind: Kind::SqliteFile,
    },
];

/// Checks that every source of the physical plan in `plan` is SQL sent to the
/// `SQLite` acceleration, so a `SQLite` side answers from `SQLite`.
#[cfg(feature = "sqlite")]
fn check_sqlite_sources(plan: &[Row]) -> Result<(), String> {
    check_sources(
        plan,
        |leaf, _| REMOTE_SCANS.contains(&leaf.name) && leaf.line.contains("name=sqlite"),
        "the SQLite acceleration",
    )
}

async fn compare_case(
    rt: &Arc<Runtime>,
    client: &tokio_postgres::Client,
    case: &Case,
    failures: &mut Vec<String>,
) -> Result<bool, anyhow::Error> {
    let oracle_sql = case.oracle_sql("diff_items", "diff_regions");
    let oracle = postgres_rows(client, &oracle_sql)
        .await
        .map_err(|e| anyhow::anyhow!("PostgreSQL rejected `{oracle_sql}`: {e}"))?;
    if oracle.is_empty() != case.empty {
        failures.push(format!(
            "{oracle_sql}\n    PostgreSQL returned {} rows, but the case expects {}",
            oracle.len(),
            if case.empty { "none" } else { "some" }
        ));
    }

    let mut all_remote = true;
    for subject in SUBJECTS {
        let name = subject.name;
        let sql = Case::bind(case.sql, subject.items, subject.regions);
        match spice_rows(rt, &sql).await {
            Ok(spice) => {
                if let Err(reason) = agreement(&spice, &oracle, case.ordered) {
                    failures.push(format!(
                        "{name}: {sql}\n    {reason}\n    spice:      {spice:?}\n    postgres:   {oracle:?}"
                    ));
                }
            }
            Err(e) => failures.push(format!("{name}: {sql}\n    failed: {e}")),
        }
        // The federated side must send the whole query to PostgreSQL, or it is
        // DataFusion over a wider scan and compares nothing the accelerated side
        // does not. A Cayenne side must read only Cayenne, or its agreement says
        // nothing about what Cayenne stored.
        let check: Option<PlanCheck> = match subject.kind {
            Kind::Federated => {
                Some(&|plan| check_federated(plan, &REMOTE_SCANS, "PostgreSQL", case.kept_local))
            }
            Kind::CayenneMemory
            | Kind::CayenneFile
            | Kind::CayenneVortex
            | Kind::CayennePartitioned => Some(&check_cayenne_sources),
            #[cfg(feature = "sqlite")]
            Kind::SqliteMemory | Kind::SqliteFile => Some(&check_sqlite_sources),
        };
        if let Some(check) = check {
            let plan = spice_rows(rt, &format!("EXPLAIN {sql}"))
                .await
                .map_err(|e| anyhow::anyhow!("EXPLAIN {sql}: {e}"))?;
            if let Err(reason) = check(&plan) {
                failures.push(format!("{name}: {sql}\n    {reason}"));
                all_remote &= subject.kind != Kind::Federated;
            }
        }
    }
    Ok(all_remote)
}

#[tokio::test]
async fn federated_and_accelerated_postgres_answers_match_postgres() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (port, container) = common::test_database().await?;
            let dir = tempfile::tempdir()?;
            let client = common::connect(u16::try_from(port)?).await?;
            for statement in SEED {
                client.batch_execute(statement).await?;
            }

            register_test_connectors().await;
            let mut app = AppBuilder::new("postgres_federation_differential");
            for subject in SUBJECTS {
                app = app
                    .with_dataset(dataset(port, "diff_items", subject.items, subject.kind, dir.path()))
                    .with_dataset(dataset(port, "diff_regions", subject.regions, subject.kind, dir.path()));
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

            // Each side must hold the whole fixture before any answer counts.
            let expected = vec![vec![Some(ITEM_ROWS.to_string())]];
            assert_eq!(
                postgres_rows(&client, "SELECT count(*) FROM diff_items")
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?,
                expected
            );
            for subject in SUBJECTS {
                assert_eq!(
                    spice_rows(&rt, &format!("SELECT count(*) FROM {}", subject.items))
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?,
                    expected,
                    "{} must hold every fixture row",
                    subject.items
                );
            }
            // The Vortex side must serve its rows from Vortex files on disk,
            // not from rows inlined in its metastore.
            for subject in SUBJECTS.iter().filter(|s| s.kind == Kind::CayenneVortex) {
                for table in [subject.items, subject.regions] {
                    let data = data_dir(dir.path(), table);
                    anyhow::ensure!(
                        has_vortex_file(&data),
                        "{table} turns inlining off but wrote no Vortex file under {}",
                        data.display()
                    );
                }
            }

            // The partitioned Cayenne side must scan one table per `grp` value,
            // so the partitioned layout is exercised.
            for subject in SUBJECTS.iter().filter(|s| s.kind == Kind::CayennePartitioned) {
                let plan = spice_rows(&rt, &format!("EXPLAIN SELECT id FROM {}", subject.items))
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                let physical = physical_plan(&plan).map_err(|e| anyhow::anyhow!(e))?;
                anyhow::ensure!(
                    operators(&physical)
                        .iter()
                        .any(|op| op.name == "PartitionedUnionExec"),
                    "{} sets `partition_by` but its scan reads no partitions:\n{physical}",
                    subject.items
                );
            }

            let cases = cases();
            let mut failures = Vec::new();
            let mut all_remote = 0_usize;
            for case in &cases {
                if compare_case(&rt, &client, case, &mut failures).await? {
                    all_remote += 1;
                }
            }
            eprintln!(
                "postgres federation differential: {} cases, {} federated plans sent the whole query to PostgreSQL, {} failures",
                cases.len(),
                all_remote,
                failures.len()
            );

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
