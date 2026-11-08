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

//! A differential test of how a `refresh_mode: changes` `MySQL` dataset judges
//! a source `ALTER TABLE ... MODIFY` that changes a numeric column's type,
//! against what `MySQL` itself stores.
//!
//! The change stream follows such an `ALTER` only when
//! [`source_retype_keeps_every_value`] says the new type holds every value of
//! the old one, because `MySQL` converts the stored values in place and sends
//! no row change for any of them. For each ordered pair of numeric types, this
//! test puts the old type's boundary values in a table, runs the `ALTER` on a
//! live server, and reads the values back. Whenever the classification says
//! the change keeps every value, `MySQL` must have kept every value, or the
//! acceleration would silently hold values the source no longer has.

use std::collections::BTreeSet;

use anyhow::anyhow;
use data_components::mysql_replication::source_retype_keeps_every_value;
use mysql_async::prelude::Queryable;

use crate::mysql::common;
use crate::{init_tracing, utils::test_request_context};

/// The numeric types compared, each with the values that bound it: its
/// extremes, and the values a narrower or binary type cannot hold exactly.
const TYPES: &[(&str, &[&str])] = &[
    ("tinyint", &["-128", "127", "0"]),
    ("tinyint unsigned", &["0", "255"]),
    ("smallint", &["-32768", "32767"]),
    ("mediumint", &["-8388608", "8388607"]),
    ("mediumint unsigned", &["0", "16777215"]),
    ("int", &["-2147483648", "2147483647", "16777217"]),
    ("int unsigned", &["0", "4294967295"]),
    (
        "bigint",
        &[
            "-9223372036854775808",
            "9223372036854775807",
            "9007199254740993",
        ],
    ),
    ("bigint unsigned", &["0", "18446744073709551615"]),
    ("decimal(3,0)", &["-999", "999"]),
    ("decimal(10,0)", &["-9999999999", "9999999999"]),
    ("decimal(15,0)", &["-999999999999999", "999999999999999"]),
    (
        "decimal(18,0)",
        &["-999999999999999999", "999999999999999999"],
    ),
    (
        "decimal(20,0)",
        &["-99999999999999999999", "99999999999999999999"],
    ),
    ("decimal(10,1)", &["-999999999.9", "999999999.9", "0.1"]),
    (
        "decimal(12,2)",
        &["-9999999999.99", "9999999999.99", "0.01"],
    ),
    ("float", &["-3.4e38", "3.4e38", "0.1", "16777216"]),
    ("float(7,4)", &["-999.9999", "999.9999", "1.2346"]),
    ("double(10,2)", &["-99999999.99", "99999999.99", "1.25"]),
    (
        "double",
        &[
            "-1.7976931348623157e308",
            "1.7976931348623157e308",
            "0.1",
            "1.5",
            "9007199254740992",
        ],
    ),
];

/// Changes the classification refuses although `MySQL` read back every
/// boundary value unchanged. Converting a decimal fraction to `DOUBLE` stores
/// the nearest binary value (`0.1` becomes `0.1000000000000000055…`), and
/// converting a `DOUBLE` to `DECIMAL` stores its shortest decimal form. `MySQL`
/// reads both forms back through that same shortest decimal, so the comparison
/// cannot see the change either conversion made.
const REFUSED_THOUGH_READ_BACK_UNCHANGED: &[&str] = &[
    "decimal(10,1) -> double",
    "decimal(12,2) -> double",
    "double(10,2) -> decimal(12,2)",
];

/// What `ALTER TABLE ... MODIFY` did to a column's boundary values.
enum Outcome {
    /// Every value reads back unchanged.
    Kept,
    /// This many values read back different.
    Changed(u64),
    /// `MySQL` rejected the `ALTER` (strict mode refuses an out-of-range
    /// value), so the column kept its type and no stream sees a change.
    Rejected(String),
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kept => f.write_str("every value read back unchanged"),
            Self::Changed(count) => write!(f, "{count} values read back different"),
            Self::Rejected(error) => write!(f, "MySQL rejected the ALTER: {error}"),
        }
    }
}

async fn alter_outcome(
    conn: &mut mysql_async::Conn,
    table: &str,
    old: &str,
    new: &str,
    values: &[&str],
) -> Result<Outcome, anyhow::Error> {
    conn.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, k {old}, ref {old})"
    ))
    .await
    .map_err(|e| anyhow!("create {table} ({old}): {e}"))?;
    let rows = values
        .iter()
        .enumerate()
        .map(|(id, value)| format!("({id}, {value}, {value})"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.query_drop(format!("INSERT INTO {table} VALUES {rows}"))
        .await
        .map_err(|e| anyhow!("seed {table} ({old}): {e}"))?;
    if let Err(e) = conn
        .query_drop(format!("ALTER TABLE {table} MODIFY k {new}"))
        .await
    {
        return Ok(Outcome::Rejected(e.to_string()));
    }
    // Cast to `DOUBLE` before comparing: `<=>` on two columns with fixed
    // decimals (`FLOAT(M,D)`) compares within a tolerance, and every value
    // here converts to a `DOUBLE` exactly or rounds the same way on both
    // sides. The decimal cast tells apart integers that round to one `DOUBLE`.
    let changed: Option<u64> = conn
        .query_first(format!(
            "SELECT COUNT(*) FROM {table} WHERE NOT ((CAST(k AS DOUBLE) <=> CAST(ref AS DOUBLE)) \
             AND (CAST(k AS DECIMAL(65,30)) <=> CAST(ref AS DECIMAL(65,30))))"
        ))
        .await
        .map_err(|e| anyhow!("read back {table} ({old} -> {new}): {e}"))?;
    Ok(match changed.unwrap_or_default() {
        0 => Outcome::Kept,
        n => Outcome::Changed(n),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_retype_classification_matches_what_mysql_stores() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let pool = common::get_mysql_conn(port)?;
            let mut conn = pool.get_conn().await?;

            let mut followed_though_changed = Vec::new();
            let mut refused_though_kept = BTreeSet::new();
            let mut pairs = 0;
            for (i, (old, values)) in TYPES.iter().enumerate() {
                for (j, (new, _)) in TYPES.iter().enumerate() {
                    if i == j {
                        continue;
                    }
                    pairs += 1;
                    let pair = format!("{old} -> {new}");
                    let keeps = source_retype_keeps_every_value(old, new)
                        .ok_or_else(|| anyhow!("{pair} is not classified as numeric"))?;
                    let outcome =
                        alter_outcome(&mut conn, &format!("r{i}_{j}"), old, new, values).await?;
                    // MySQL rounds and clamps a value it converts into a
                    // `FLOAT(M,D)` or `DOUBLE(M,D)`, so the classification
                    // refuses every bounded target but its own type.
                    let bounded_target = new.contains('(') && !new.starts_with("decimal");
                    if bounded_target {
                        assert!(!keeps, "{pair} must be refused");
                    }
                    match (keeps, &outcome) {
                        (true, Outcome::Kept)
                        | (false, Outcome::Changed(_) | Outcome::Rejected(_)) => {}
                        (true, _) => followed_though_changed.push(format!("{pair}: {outcome}")),
                        (false, Outcome::Kept) if bounded_target => {}
                        (false, Outcome::Kept) => {
                            refused_though_kept.insert(pair);
                        }
                    }
                }
            }

            assert_eq!(pairs, TYPES.len() * (TYPES.len() - 1));
            assert!(
                followed_though_changed.is_empty(),
                "the stream would follow these `ALTER`s although MySQL changed or rejected a \
                 stored value: {followed_though_changed:#?}"
            );
            assert_eq!(
                refused_though_kept,
                REFUSED_THOUGH_READ_BACK_UNCHANGED
                    .iter()
                    .map(|pair| (*pair).to_string())
                    .collect::<BTreeSet<_>>(),
                "the stream refuses these `ALTER`s although MySQL read back every boundary value \
                 unchanged"
            );
            Ok(())
        })
        .await
}
