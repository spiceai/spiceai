// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQLLancer-style query corpus for three-engine result parity.
//!
//! [SQLLancer](https://github.com/sqlancer/sqlancer) fuzzes DBMSs with randomly
//! generated SQL and differential oracles. This module vendors a **deterministic,
//! curated** corpus of SQLLancer-shaped statements (filters, joins, aggregates,
//! subqueries, ORDER BY) over a fixed two-table schema so Cayenne, DuckDB, and
//! chDB can be compared on identical logical SQL without running the Java fuzzer
//! at test time.
//!
//! Schema (loaded identically into every engine):
//! - `sqllancer_t0(c0 BIGINT, c1 BIGINT, c2 VARCHAR, c3 DOUBLE)`
//! - `sqllancer_t1(c0 BIGINT, c1 BIGINT, c2 VARCHAR)`
//!
//! Set `SQLLANCER_EXTRA_SQL` to a path of additional newline-separated SQL
//! statements (same schema) to extend the gate without code changes.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use test_framework::queries::Query;

/// Tables the SQLLancer corpus expects.
pub const SQLLANCER_TABLES: &[&str] = &["sqllancer_t0", "sqllancer_t1"];

#[must_use]
pub fn t0_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int64, true),
        Field::new("c1", DataType::Int64, true),
        Field::new("c2", DataType::Utf8, true),
        Field::new("c3", DataType::Float64, true),
    ]))
}

#[must_use]
pub fn t1_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int64, true),
        Field::new("c1", DataType::Int64, true),
        Field::new("c2", DataType::Utf8, true),
    ]))
}

/// Deterministic seed data for `sqllancer_t0` / `sqllancer_t1`.
#[must_use]
pub fn make_t0_batch(rows: usize) -> RecordBatch {
    let schema = t0_schema();
    let mut c0 = Vec::with_capacity(rows);
    let mut c1 = Vec::with_capacity(rows);
    let mut c2 = Vec::with_capacity(rows);
    let mut c3 = Vec::with_capacity(rows);
    for i in 0..rows {
        let i64 = i as i64;
        c0.push(Some(i64));
        c1.push(Some(i64 % 17));
        c2.push(Some(format!("v{}", i % 11)));
        c3.push(Some((i as f64) * 0.5 - 10.0));
    }
    // Sprinkle NULLs the way SQLLancer often does.
    if rows > 5 {
        c0[3] = None;
        c2[4] = None;
        c3[5] = None;
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(c0)),
            Arc::new(Int64Array::from(c1)),
            Arc::new(StringArray::from(c2)),
            Arc::new(Float64Array::from(c3)),
        ],
    )
    .expect("sqllancer t0")
}

#[must_use]
pub fn make_t1_batch(rows: usize) -> RecordBatch {
    let schema = t1_schema();
    let mut c0 = Vec::with_capacity(rows);
    let mut c1 = Vec::with_capacity(rows);
    let mut c2 = Vec::with_capacity(rows);
    for i in 0..rows {
        let i64 = i as i64;
        c0.push(Some(i64 % 50));
        c1.push(Some(i64 % 7));
        c2.push(Some(format!("w{}", i % 5)));
    }
    if rows > 2 {
        c1[1] = None;
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(c0)),
            Arc::new(Int64Array::from(c1)),
            Arc::new(StringArray::from(c2)),
        ],
    )
    .expect("sqllancer t1")
}

/// Built-in SQLLancer-shaped query inventory.
#[must_use]
pub fn sqllancer_queries() -> Vec<Query> {
    let mut q = vec![
        // --- scans / projections ---
        q("sl_scan_t0", "SELECT c0, c1, c2, c3 FROM sqllancer_t0"),
        q("sl_scan_t1", "SELECT c0, c1, c2 FROM sqllancer_t1"),
        q(
            "sl_project_expr",
            "SELECT c0 + c1 AS s, c3 * 2.0 AS d FROM sqllancer_t0",
        ),
        // --- filters ---
        q(
            "sl_filter_eq",
            "SELECT c0, c1 FROM sqllancer_t0 WHERE c1 = 3",
        ),
        q(
            "sl_filter_range",
            "SELECT c0 FROM sqllancer_t0 WHERE c0 BETWEEN 10 AND 40",
        ),
        // Keep a non-null companion column so CSV null-type inference never
        // collapses the batch to Arrow `Null` when only empty fields remain.
        q(
            "sl_filter_null",
            "SELECT c0, c1, c2 FROM sqllancer_t0 WHERE c2 IS NULL",
        ),
        q(
            "sl_filter_not_null",
            "SELECT c0 FROM sqllancer_t0 WHERE c0 IS NOT NULL",
        ),
        q(
            "sl_filter_or",
            "SELECT c0, c1 FROM sqllancer_t0 WHERE c1 = 1 OR c1 = 2",
        ),
        q(
            "sl_filter_and",
            "SELECT c0 FROM sqllancer_t0 WHERE c1 > 5 AND c3 < 100.0",
        ),
        q(
            "sl_filter_like",
            "SELECT c0, c2 FROM sqllancer_t0 WHERE c2 LIKE 'v1%'",
        ),
        // --- aggregates ---
        q("sl_count_star", "SELECT COUNT(*) FROM sqllancer_t0"),
        q("sl_count_col", "SELECT COUNT(c0) FROM sqllancer_t0"),
        q("sl_sum", "SELECT SUM(c0), SUM(c1) FROM sqllancer_t0"),
        q(
            "sl_avg",
            "SELECT AVG(c3) FROM sqllancer_t0 WHERE c3 IS NOT NULL",
        ),
        q(
            "sl_minmax",
            "SELECT MIN(c0), MAX(c0), MIN(c1), MAX(c1) FROM sqllancer_t0",
        ),
        q(
            "sl_groupby",
            "SELECT c1, COUNT(*), SUM(c0) FROM sqllancer_t0 GROUP BY c1",
        ),
        q(
            "sl_groupby_having",
            "SELECT c1, COUNT(*) AS n FROM sqllancer_t0 GROUP BY c1 HAVING COUNT(*) > 2",
        ),
        q(
            "sl_groupby_multi",
            "SELECT c1, c2, COUNT(*) FROM sqllancer_t0 WHERE c2 IS NOT NULL GROUP BY c1, c2",
        ),
        // --- joins ---
        q(
            "sl_inner_join",
            "SELECT a.c0, b.c1, a.c2 FROM sqllancer_t0 a \
             INNER JOIN sqllancer_t1 b ON a.c0 = b.c0",
        ),
        q(
            "sl_left_join",
            "SELECT a.c0, b.c2 FROM sqllancer_t0 a \
             LEFT JOIN sqllancer_t1 b ON a.c1 = b.c1",
        ),
        q(
            "sl_join_filter",
            "SELECT a.c0, b.c0 FROM sqllancer_t0 a \
             JOIN sqllancer_t1 b ON a.c0 = b.c0 WHERE a.c1 < 10",
        ),
        q(
            "sl_join_agg",
            "SELECT b.c2, COUNT(*), SUM(a.c0) FROM sqllancer_t0 a \
             JOIN sqllancer_t1 b ON a.c0 = b.c0 GROUP BY b.c2",
        ),
        // --- subqueries ---
        q(
            "sl_subquery_scalar",
            "SELECT c0 FROM sqllancer_t0 WHERE c1 > (SELECT MIN(c1) FROM sqllancer_t1)",
        ),
        q(
            "sl_subquery_in",
            "SELECT c0, c2 FROM sqllancer_t0 WHERE c1 IN (SELECT c1 FROM sqllancer_t1 WHERE c1 IS NOT NULL)",
        ),
        q(
            "sl_subquery_exists",
            "SELECT c0 FROM sqllancer_t0 a WHERE EXISTS \
             (SELECT 1 FROM sqllancer_t1 b WHERE b.c0 = a.c0)",
        ),
        // --- ORDER BY (unique keys for deterministic order) ---
        q(
            "sl_order_limit",
            "SELECT c0, c1, c2 FROM sqllancer_t0 WHERE c0 IS NOT NULL ORDER BY c0 LIMIT 20",
        ),
        q(
            "sl_order_desc",
            "SELECT c0, c3 FROM sqllancer_t0 WHERE c0 IS NOT NULL ORDER BY c0 DESC LIMIT 15",
        ),
        q(
            "sl_distinct",
            "SELECT DISTINCT c1 FROM sqllancer_t0 WHERE c1 IS NOT NULL ORDER BY c1",
        ),
        // --- arithmetic / CASE ---
        q(
            "sl_case",
            "SELECT c0, CASE WHEN c1 < 5 THEN 'lo' WHEN c1 < 12 THEN 'mid' ELSE 'hi' END AS b \
             FROM sqllancer_t0 WHERE c0 IS NOT NULL ORDER BY c0 LIMIT 30",
        ),
        q(
            "sl_coalesce",
            "SELECT c0, COALESCE(c2, 'missing') AS c2x FROM sqllancer_t0 ORDER BY c0 NULLS LAST LIMIT 25",
        ),
    ];

    if let Some(extra) = load_extra_queries() {
        q.extend(extra);
    }
    q
}

fn q(name: &str, sql: &str) -> Query {
    Query::new(name.into(), sql.into(), false)
}

fn load_extra_queries() -> Option<Vec<Query>> {
    let path = std::env::var_os("SQLLANCER_EXTRA_SQL")?;
    let path = std::path::PathBuf::from(path);
    let text = std::fs::read_to_string(&path).ok()?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") || line.starts_with('#') {
            continue;
        }
        // Strip trailing semicolon for DataFusion/DuckDB consistency.
        let sql = line.trim_end_matches(';').to_string();
        out.push(Query::new(
            format!("sl_extra_{i}").into(),
            sql.into(),
            false,
        ));
    }
    Some(out)
}

/// ClickHouse-compatible rewrites for the SQLLancer corpus (NULL ordering, etc.).
#[must_use]
pub fn sqllancer_sql_for_chdb(sql: &str) -> String {
    // DataFusion `NULLS LAST` is not valid in ClickHouse — drop it.
    sql.replace(" NULLS LAST", "").replace(" nulls last", "")
}
