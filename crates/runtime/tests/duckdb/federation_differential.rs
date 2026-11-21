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

//! Differential test of the `DuckDB` data connector against `DuckDB` itself.
//!
//! Each query runs over the same committed rows: sent straight to a
//! standalone `DuckDB` database (the oracle, which shares no code with Spice),
//! against the federated `duckdb:` datasets, whose plans Spice rewrites into
//! `DuckDB`'s dialect and sends to `DuckDB`, and against accelerations of the
//! same tables loaded once with `refresh_mode: full`: Arrow, Cayenne
//! `mode: memory`, and Cayenne `mode: file` with inlining turned off, which
//! writes and reads Vortex files. The oracle reads a byte-for-byte copy of
//! the database file Spice opens, so the two never share a `DuckDB` instance.
//!
//! The fixture holds the values a translation gets wrong: NULLs in every
//! column, `BIGINT` and `DECIMAL` boundaries, `LIKE` wildcards stored as
//! literal characters, a backslash and a quote, non-ASCII and padded text,
//! fractional-second timestamps, non-finite and signed-zero `DOUBLE`s, and
//! duplicate and NULL join keys.
//!
//! Where `DuckDB` and Spice define an operator differently, the case carries
//! the `DuckDB` spelling of Spice's semantics for the oracle: `//` for
//! integer division, which `DuckDB`'s `/` does in floating point, and an
//! explicit `ESCAPE '\'` on a `LIKE` pattern with a backslash, which Spice
//! reads as an escape and `DuckDB` without `ESCAPE` does not. The federated
//! side must make the same translation for its answer to match. Unlike
//! `PostgreSQL`, `DuckDB` compares text by its bytes, as Spice does, so text
//! ordering and text `MIN`/`MAX` are in the corpus. Every `ORDER BY` states
//! its NULL placement and ends in a unique key. Both sides return Arrow, and
//! each cell is compared in Arrow's text form.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use app::AppBuilder;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};

use crate::{
    acceleration::has_vortex_file,
    configure_test_datafusion,
    federation_differential::{
        Case, PlanCheck, Row, agreement, check_cayenne_sources, check_federated, empty, ordered,
        render, rewritten, rows, spice_rows,
    },
    init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

/// The rows of the fixture table, so a load that dropped some fails up front.
const ITEM_ROWS: usize = 16;

const SEED: &str = r"
CREATE TABLE diff_items (
  id INTEGER PRIMARY KEY, grp INTEGER, qty BIGINT, name VARCHAR,
  price DECIMAL(10, 2), flag BOOLEAN, d DATE, ts TIMESTAMP, score DOUBLE);
CREATE TABLE diff_regions (grp INTEGER, region VARCHAR);
INSERT INTO diff_items VALUES
  (1, 1, 10, 'apple', 1.50, true, '2024-01-01', '2024-01-01 00:00:00', 0.5),
  (2, 1, -5, 'Apple', 2.25, false, '2024-02-29', '2024-02-29 12:30:00.5', -0.0),
  (3, 2, NULL, 'banana', NULL, NULL, NULL, NULL, NULL),
  (4, NULL, 0, NULL, 0.00, true, '1999-12-31', '1999-12-31 23:59:59.999999', 0.0),
  (5, 2, 9223372036854775807, 'a_b', 99999999.99, false, '2024-03-01', '2024-03-01 08:00:00', 'inf'),
  (6, 3, -9223372036854775808, 'a%b', -0.01, true, '2023-12-31', '2023-12-31 23:59:59', '-inf'),
  (7, 3, 7, '', 3.33, NULL, '2024-02-01', '2024-02-01 00:00:00.000001', 'nan'),
  (8, NULL, NULL, ' padded ', -3.33, false, NULL, '2024-06-15 18:45:00', 1e308),
  (9, 2, 42, 'ümlaut', 10.00, true, '2024-02-15', NULL, -2.5),
  (10, 4, 3, 'O''Brien', 0.10, NULL, '2000-01-01', '2000-01-01 00:00:00', 3.0),
  (11, 4, 3, 'aB', 1.50, true, '2024-01-31', '2024-01-31 23:59:59.5', 3.0),
  (12, 1, 100, 'apple', 2.25, false, '2024-02-29', '2024-02-29 12:30:00', -1e-300),
  (13, 5, -1, 'zeta', NULL, true, '1970-01-01', '1970-01-01 00:00:00', 100.25),
  (14, 5, 2147483648, 'Zeta', 7.77, false, '2038-01-19', '2038-01-19 03:14:08', NULL),
  (15, NULL, 15, 'back\slash', 15.15, NULL, '2024-12-31', '2024-12-31 23:59:59', 7.0),
  (16, 6, -2147483649, 'a\%b', -99999999.99, false, '2024-07-04', '2024-07-04 12:00:00', -7.0);
INSERT INTO diff_regions VALUES
  (1, 'north'), (2, 'south'), (2, 'south-east'), (3, 'west'),
  (NULL, 'nowhere'), (7, 'unused');
";

/// A case whose `LIKE` pattern holds a backslash, which `DuckDB` spells
/// `duckdb`, and which Spice must evaluate itself: `DuckDB` without `ESCAPE`
/// reads the backslash literally, and Spice reads it as an escape.
const fn backslash_like(sql: &'static str, duckdb: &'static str) -> Case {
    Case {
        kept_local: Some(" LIKE "),
        ..rewritten(sql, duckdb)
    }
}

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
        rewritten(
            "SELECT id FROM {items} WHERE grp / 2 = 1",
            "SELECT id FROM {items} WHERE grp // 2 = 1",
        ),
        rewritten(
            "SELECT id, qty / 4 FROM {items} WHERE qty BETWEEN -100 AND 100",
            "SELECT id, qty // 4 FROM {items} WHERE qty BETWEEN -100 AND 100",
        ),
        rows("SELECT id, qty + 1 FROM {items} WHERE qty BETWEEN -100 AND 100"),
        rows("SELECT id, abs(qty) FROM {items} WHERE qty > -100 AND qty < 100"),
        // Text: equality, ordering, pattern matching, functions.
        rows("SELECT id FROM {items} WHERE name = 'apple'"),
        rows("SELECT id FROM {items} WHERE name = ''"),
        rows("SELECT id FROM {items} WHERE name <> 'apple'"),
        rows("SELECT id FROM {items} WHERE name > 'a'"),
        rows("SELECT id FROM {items} WHERE name < 'B'"),
        rows("SELECT id FROM {items} WHERE name LIKE 'a%'"),
        rows("SELECT id FROM {items} WHERE name LIKE 'a_b'"),
        backslash_like(
            r"SELECT id FROM {items} WHERE name LIKE 'a\_b'",
            r"SELECT id FROM {items} WHERE name LIKE 'a\_b' ESCAPE '\'",
        ),
        backslash_like(
            r"SELECT id FROM {items} WHERE name LIKE 'a\%b'",
            r"SELECT id FROM {items} WHERE name LIKE 'a\%b' ESCAPE '\'",
        ),
        backslash_like(
            r"SELECT id FROM {items} WHERE name LIKE '%\\%'",
            r"SELECT id FROM {items} WHERE name LIKE '%\\%' ESCAPE '\'",
        ),
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
        rows("SELECT min(name), max(name) FROM {items}"),
        rows("SELECT grp, min(name), max(name) FROM {items} GROUP BY grp"),
        rows("SELECT name, count(*) FROM {items} GROUP BY name"),
        // DECIMAL(10, 2).
        rows("SELECT id FROM {items} WHERE price > 2.00"),
        rows("SELECT id FROM {items} WHERE price = 1.5"),
        rows("SELECT id FROM {items} WHERE price < 0"),
        rows("SELECT id FROM {items} WHERE price >= 99999999.99"),
        rows("SELECT id, CAST(price AS VARCHAR) FROM {items}"),
        rows("SELECT CAST(sum(price) AS VARCHAR) FROM {items}"),
        rows("SELECT grp, CAST(sum(price) AS VARCHAR) FROM {items} GROUP BY grp"),
        // DOUBLE, with infinities, a NaN, and a negative zero.
        rows("SELECT id FROM {items} WHERE score > 0"),
        rows("SELECT id FROM {items} WHERE score < 0"),
        rows("SELECT id FROM {items} WHERE score = 0"),
        rows("SELECT id FROM {items} WHERE score = 3.0"),
        rows("SELECT id FROM {items} WHERE score BETWEEN -10 AND 10"),
        rows("SELECT id FROM {items} WHERE isnan(score)"),
        rows("SELECT id FROM {items} WHERE score IS NULL"),
        rows("SELECT id, score FROM {items}"),
        rows("SELECT id, score * 2 FROM {items} WHERE score BETWEEN -10 AND 10"),
        // Booleans, as integers on the way out.
        rows("SELECT id FROM {items} WHERE flag"),
        rows("SELECT id FROM {items} WHERE NOT flag"),
        rows("SELECT id FROM {items} WHERE flag IS NOT TRUE"),
        rows("SELECT id FROM {items} WHERE flag IS DISTINCT FROM true"),
        rows("SELECT id, CASE WHEN flag THEN 1 WHEN NOT flag THEN 0 END FROM {items}"),
        // Dates and timestamps.
        rows("SELECT id FROM {items} WHERE d >= DATE '2024-01-01'"),
        rows("SELECT id FROM {items} WHERE d BETWEEN DATE '2024-02-01' AND DATE '2024-02-29'"),
        rows("SELECT id FROM {items} WHERE d < DATE '2000-01-01'"),
        rows("SELECT id, CAST(d AS VARCHAR) FROM {items}"),
        rows("SELECT id, d, ts FROM {items}"),
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
        rows("SELECT min(qty), max(qty), min(d), max(d), min(ts), max(ts) FROM {items}"),
        rows("SELECT count(*) FROM {items} WHERE grp > 100"),
        empty("SELECT grp, count(*) FROM {items} WHERE grp > 100 GROUP BY grp"),
        // Sorts and limits, with NULL placement stated and a unique tie-breaker.
        ordered("SELECT id FROM {items} ORDER BY qty DESC NULLS LAST, id"),
        ordered("SELECT id FROM {items} ORDER BY qty ASC NULLS FIRST, id"),
        ordered("SELECT id FROM {items} ORDER BY grp ASC NULLS FIRST, id LIMIT 5"),
        ordered("SELECT id FROM {items} ORDER BY grp DESC NULLS LAST, id DESC LIMIT 5"),
        ordered("SELECT id FROM {items} ORDER BY price DESC NULLS FIRST, id LIMIT 4"),
        ordered("SELECT id FROM {items} ORDER BY ts ASC NULLS LAST, id LIMIT 6"),
        ordered("SELECT id FROM {items} ORDER BY name ASC NULLS FIRST, id"),
        ordered("SELECT id FROM {items} ORDER BY name DESC NULLS LAST, id LIMIT 7"),
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

/// The scans that send SQL to `DuckDB`.
const REMOTE_SCANS: [&str; 3] = ["VirtualExecutionPlan", "SqlExec", "DuckSqlExec"];

/// How a dataset reads `DuckDB`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// No acceleration: Spice sends each query to `DuckDB`.
    Federated,
    /// An Arrow acceleration loaded once with `refresh_mode: full`.
    Arrow,
    /// A Cayenne acceleration held in memory, loaded once with
    /// `refresh_mode: full`.
    CayenneMemory,
    /// A Cayenne acceleration with a metastore on disk and
    /// `inline_max_rows: 0`, loaded once with `refresh_mode: full`, so the
    /// load is written to Vortex files and every scan reads them.
    CayenneVortex,
}

/// One Spice side of the comparison: the datasets it reads, and how they read
/// `DuckDB`, which decides what its plan must show.
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
        name: "arrow",
        items: "arrow_items",
        regions: "arrow_regions",
        kind: Kind::Arrow,
    },
    Subject {
        name: "cayenne memory",
        items: "cmem_items",
        regions: "cmem_regions",
        kind: Kind::CayenneMemory,
    },
    Subject {
        name: "cayenne vortex",
        items: "cvortex_items",
        regions: "cvortex_regions",
        kind: Kind::CayenneVortex,
    },
];

/// Where a Cayenne `mode: file` acceleration of dataset `name` keeps its data.
fn data_dir(dir: &Path, name: &str) -> PathBuf {
    dir.join(name).join("data")
}

/// `db` is the `DuckDB` file Spice opens; `dir` holds a `mode: file`
/// acceleration's data and metastore, one directory per dataset `name`.
fn dataset(db: &Path, table: &str, name: &str, kind: Kind, dir: &Path) -> Dataset {
    let mut ds = Dataset::new(format!("duckdb:{table}"), name.to_string());
    // `duckdb_open`, the connector's parameter, not the accelerator's `duckdb_file`.
    ds.params = Some(Params::from_string_map(HashMap::from([(
        "duckdb_open".to_string(),
        db.display().to_string(),
    )])));
    let full = Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    };
    ds.acceleration = match kind {
        Kind::Federated => None,
        Kind::Arrow => Some(Acceleration {
            engine: Some("arrow".to_string()),
            ..full
        }),
        Kind::CayenneMemory => Some(Acceleration {
            engine: Some("cayenne".to_string()),
            mode: Mode::Memory,
            ..full
        }),
        Kind::CayenneVortex => Some(Acceleration {
            engine: Some("cayenne".to_string()),
            mode: Mode::File,
            params: Some(Params::from_string_map(HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    data_dir(dir, name).display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    dir.join(name).join("metadata").display().to_string(),
                ),
                ("cayenne_inline_max_rows".to_string(), "0".to_string()),
            ]))),
            ..full
        }),
    };
    ds
}

/// The oracle: `sql` sent to the standalone `DuckDB` database.
fn duckdb_rows(oracle: &duckdb_oracle::Connection, sql: &str) -> Result<Vec<Row>, String> {
    let batches = oracle.query(sql).map_err(|e| e.to_string())?;
    render(&batches)
}

async fn compare_case(
    rt: &Arc<Runtime>,
    oracle: &duckdb_oracle::Connection,
    case: &Case,
    failures: &mut Vec<String>,
) -> Result<bool, anyhow::Error> {
    let oracle_sql = case.oracle_sql("diff_items", "diff_regions");
    let expected = duckdb_rows(oracle, &oracle_sql)
        .map_err(|e| anyhow::anyhow!("DuckDB rejected `{oracle_sql}`: {e}"))?;
    if expected.is_empty() != case.empty {
        failures.push(format!(
            "{oracle_sql}\n    DuckDB returned {} rows, but the case expects {}",
            expected.len(),
            if case.empty { "none" } else { "some" }
        ));
    }

    let mut all_remote = case.kept_local.is_none();
    for subject in SUBJECTS {
        let name = subject.name;
        let sql = Case::bind(case.sql, subject.items, subject.regions);
        match spice_rows(rt, &sql).await {
            Ok(spice) => {
                if let Err(reason) = agreement(&spice, &expected, case.ordered) {
                    failures.push(format!(
                        "{name}: {sql}\n    {reason}\n    spice:  {spice:?}\n    duckdb: {expected:?}"
                    ));
                }
            }
            Err(e) => failures.push(format!("{name}: {sql}\n    failed: {e}")),
        }
        // The federated side must send the whole query to DuckDB, but for an
        // expression the case keeps local, or it is DataFusion over a wider scan
        // and checks no translation. A Cayenne side must read only Cayenne, or
        // its agreement says nothing about what Cayenne stored.
        let check: Option<PlanCheck> = match subject.kind {
            Kind::Federated => {
                Some(&|plan| check_federated(plan, &REMOTE_SCANS, "DuckDB", case.kept_local))
            }
            Kind::CayenneMemory | Kind::CayenneVortex => Some(&check_cayenne_sources),
            Kind::Arrow => None,
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
async fn federated_and_accelerated_duckdb_answers_match_duckdb() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let spice_db = dir.path().join("spice.duckdb");
            let oracle_db = dir.path().join("oracle.duckdb");
            {
                let seed = duckdb_oracle::Connection::open(&spice_db)?;
                seed.execute_batch(SEED)?;
                seed.execute_batch("CHECKPOINT")?;
            }
            // The oracle reads its own copy, so it and Spice never open one file
            // from two DuckDB instances in this process.
            std::fs::copy(&spice_db, &oracle_db)?;
            let oracle = duckdb_oracle::Connection::open(&oracle_db)?;

            register_test_connectors().await;
            let mut app = AppBuilder::new("duckdb_federation_differential");
            for subject in SUBJECTS {
                app = app
                    .with_dataset(dataset(&spice_db, "diff_items", subject.items, subject.kind, dir.path()))
                    .with_dataset(dataset(&spice_db, "diff_regions", subject.regions, subject.kind, dir.path()));
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
                duckdb_rows(&oracle, "SELECT count(*) FROM diff_items")
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

            let cases = cases();
            let mut failures = Vec::new();
            let mut all_remote = 0_usize;
            for case in &cases {
                if compare_case(&rt, &oracle, case, &mut failures).await? {
                    all_remote += 1;
                }
            }
            eprintln!(
                "duckdb federation differential: {} cases, {} federated plans sent the whole query to DuckDB, {} failures",
                cases.len(),
                all_remote,
                failures.len()
            );

            rt.shutdown().await;
            anyhow::ensure!(
                failures.is_empty(),
                "{} disagreements with DuckDB:\n{}",
                failures.len(),
                failures.join("\n")
            );
            Ok(())
        })
        .await
}
