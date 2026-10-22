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

//! TPC-H data from the `duckdb` CLI's `dbgen`, loaded into sources: the same
//! rows in every source, so one can be the oracle for another.

use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::{generate::Dialect, source::Source};

#[derive(Clone, Copy)]
enum Kind {
    Int,
    Decimal,
    Text,
    Date,
}

const TABLES: &[(&str, &[(&str, Kind)])] = &[
    (
        "region",
        &[
            ("r_regionkey", Kind::Int),
            ("r_name", Kind::Text),
            ("r_comment", Kind::Text),
        ],
    ),
    (
        "nation",
        &[
            ("n_nationkey", Kind::Int),
            ("n_name", Kind::Text),
            ("n_regionkey", Kind::Int),
            ("n_comment", Kind::Text),
        ],
    ),
    (
        "supplier",
        &[
            ("s_suppkey", Kind::Int),
            ("s_name", Kind::Text),
            ("s_address", Kind::Text),
            ("s_nationkey", Kind::Int),
            ("s_phone", Kind::Text),
            ("s_acctbal", Kind::Decimal),
            ("s_comment", Kind::Text),
        ],
    ),
    (
        "customer",
        &[
            ("c_custkey", Kind::Int),
            ("c_name", Kind::Text),
            ("c_address", Kind::Text),
            ("c_nationkey", Kind::Int),
            ("c_phone", Kind::Text),
            ("c_acctbal", Kind::Decimal),
            ("c_mktsegment", Kind::Text),
            ("c_comment", Kind::Text),
        ],
    ),
    (
        "part",
        &[
            ("p_partkey", Kind::Int),
            ("p_name", Kind::Text),
            ("p_mfgr", Kind::Text),
            ("p_brand", Kind::Text),
            ("p_type", Kind::Text),
            ("p_size", Kind::Int),
            ("p_container", Kind::Text),
            ("p_retailprice", Kind::Decimal),
            ("p_comment", Kind::Text),
        ],
    ),
    (
        "partsupp",
        &[
            ("ps_partkey", Kind::Int),
            ("ps_suppkey", Kind::Int),
            ("ps_availqty", Kind::Int),
            ("ps_supplycost", Kind::Decimal),
            ("ps_comment", Kind::Text),
        ],
    ),
    (
        "orders",
        &[
            ("o_orderkey", Kind::Int),
            ("o_custkey", Kind::Int),
            ("o_orderstatus", Kind::Text),
            ("o_totalprice", Kind::Decimal),
            ("o_orderdate", Kind::Date),
            ("o_orderpriority", Kind::Text),
            ("o_clerk", Kind::Text),
            ("o_shippriority", Kind::Int),
            ("o_comment", Kind::Text),
        ],
    ),
    (
        "lineitem",
        &[
            ("l_orderkey", Kind::Int),
            ("l_partkey", Kind::Int),
            ("l_suppkey", Kind::Int),
            ("l_linenumber", Kind::Int),
            ("l_quantity", Kind::Decimal),
            ("l_extendedprice", Kind::Decimal),
            ("l_discount", Kind::Decimal),
            ("l_tax", Kind::Decimal),
            ("l_returnflag", Kind::Text),
            ("l_linestatus", Kind::Text),
            ("l_shipdate", Kind::Date),
            ("l_commitdate", Kind::Date),
            ("l_receiptdate", Kind::Date),
            ("l_shipinstruct", Kind::Text),
            ("l_shipmode", Kind::Text),
            ("l_comment", Kind::Text),
        ],
    ),
];

fn sql_type(kind: Kind, dialect: Dialect) -> &'static str {
    match (kind, dialect) {
        (Kind::Int, Dialect::ClickHouse) => "Int64",
        (Kind::Int, _) => "BIGINT",
        (Kind::Decimal, Dialect::ClickHouse) => "Decimal(15, 2)",
        (Kind::Decimal, _) => "DECIMAL(15, 2)",
        (Kind::Text, Dialect::ClickHouse) => "String",
        (Kind::Text, _) => "VARCHAR(200)",
        (Kind::Date, Dialect::ClickHouse) => "Date32",
        (Kind::Date, _) => "DATE",
    }
}

fn create_table(table: &str, columns: &[(&str, Kind)], dialect: Dialect) -> String {
    let defs: Vec<String> = columns
        .iter()
        .map(|(name, kind)| format!("{name} {}", sql_type(*kind, dialect)))
        .collect();
    let key = columns[0].0;
    if dialect == Dialect::ClickHouse {
        format!(
            "CREATE TABLE {table} ({}) ENGINE = MergeTree ORDER BY ({key})",
            defs.join(", ")
        )
    } else {
        format!("CREATE TABLE {table} ({})", defs.join(", "))
    }
}

/// Generates TPC-H at `scale` with the `duckdb` CLI into `dir` (once per
/// scale), as one `|`-separated file per table: TPC-H has no NULLs and no `|`
/// in its text.
async fn generate(dir: &Path, scale: f64) -> Result<PathBuf> {
    // Instances run side by side share the cache: one generates, the rest wait.
    static GENERATING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = GENERATING.lock().await;
    let out = dir.join(format!("tpch-sf{scale}"));
    if out.join("lineitem.tbl").exists() {
        return Ok(out);
    }
    std::fs::create_dir_all(&out)?;
    let mut script = format!("INSTALL tpch; LOAD tpch; CALL dbgen(sf = {scale});\n");
    for (table, _) in TABLES {
        let _ = writeln!(
            script,
            "COPY {table} TO '{}' (FORMAT csv, DELIMITER '|', HEADER false, QUOTE '');",
            out.join(format!("{table}.tbl")).display()
        );
    }
    let status = tokio::process::Command::new("duckdb")
        .args(["-bail", "-c", &script])
        .status()
        .await
        .context("Failed to run `duckdb`; is it installed and on PATH?")?;
    if !status.success() {
        bail!("duckdb dbgen failed: {status}");
    }
    Ok(out)
}

/// Creates the TPC-H tables in `source`'s namespace and loads them.
pub async fn load(source: &dyn Source, namespace: &str, work: &Path, scale: f64) -> Result<u64> {
    let dir = generate(work, scale).await?;
    let mut total = 0;
    for (table, columns) in TABLES {
        source
            .exec(namespace, &create_table(table, columns, source.dialect()))
            .await
            .with_context(|| format!("Failed to create TPC-H table {table}"))?;
        let text = tokio::fs::read_to_string(dir.join(format!("{table}.tbl"))).await?;
        let names: Vec<String> = columns.iter().map(|(n, _)| (*n).to_string()).collect();
        let mut rows = text.lines().filter(|l| !l.is_empty()).map(|l| {
            l.split('|')
                .map(|c| Some(c.to_string()))
                .collect::<Vec<_>>()
        });
        total += source
            .load(namespace, table, &names, &mut rows)
            .await
            .with_context(|| format!("Failed to load TPC-H table {table}"))?;
    }
    Ok(total)
}
