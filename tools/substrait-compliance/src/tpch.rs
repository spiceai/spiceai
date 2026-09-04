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

//! Register IBM TPC-H SF 0.01 CSVs as the uppercase named tables the
//! Substrait plans reference (`LINEITEM`, `ORDERS`, …).

use std::path::Path;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use snafu::ResultExt;

use crate::error::{self, Result};

struct TableSpec {
    file_stem: &'static str,
    sql_name: &'static str,
    columns: &'static [(&'static str, &'static str)],
}

/// Column casts match the `baseSchema` embedded in the IBM TPC-H plans.
const TABLES: &[TableSpec] = &[
    TableSpec {
        file_stem: "region",
        sql_name: "REGION",
        columns: &[
            ("R_REGIONKEY", "INT"),
            ("R_NAME", "VARCHAR"),
            ("R_COMMENT", "VARCHAR"),
        ],
    },
    TableSpec {
        file_stem: "nation",
        sql_name: "NATION",
        columns: &[
            ("N_NATIONKEY", "INT"),
            ("N_NAME", "VARCHAR"),
            ("N_REGIONKEY", "INT"),
            ("N_COMMENT", "VARCHAR"),
        ],
    },
    TableSpec {
        file_stem: "part",
        sql_name: "PART",
        columns: &[
            ("P_PARTKEY", "INT"),
            ("P_NAME", "VARCHAR"),
            ("P_MFGR", "VARCHAR"),
            ("P_BRAND", "VARCHAR"),
            ("P_TYPE", "VARCHAR"),
            ("P_SIZE", "INT"),
            ("P_CONTAINER", "VARCHAR"),
            ("P_RETAILPRICE", "DECIMAL(15, 2)"),
            ("P_COMMENT", "VARCHAR"),
        ],
    },
    TableSpec {
        file_stem: "supplier",
        sql_name: "SUPPLIER",
        columns: &[
            ("S_SUPPKEY", "INT"),
            ("S_NAME", "VARCHAR"),
            ("S_ADDRESS", "VARCHAR"),
            ("S_NATIONKEY", "INT"),
            ("S_PHONE", "VARCHAR"),
            ("S_ACCTBAL", "DECIMAL(15, 2)"),
            ("S_COMMENT", "VARCHAR"),
        ],
    },
    TableSpec {
        file_stem: "partsupp",
        sql_name: "PARTSUPP",
        columns: &[
            ("PS_PARTKEY", "INT"),
            ("PS_SUPPKEY", "INT"),
            ("PS_AVAILQTY", "INT"),
            ("PS_SUPPLYCOST", "DECIMAL(15, 2)"),
            ("PS_COMMENT", "VARCHAR"),
        ],
    },
    TableSpec {
        file_stem: "customer",
        sql_name: "CUSTOMER",
        columns: &[
            ("C_CUSTKEY", "INT"),
            ("C_NAME", "VARCHAR"),
            ("C_ADDRESS", "VARCHAR"),
            ("C_NATIONKEY", "INT"),
            ("C_PHONE", "VARCHAR"),
            ("C_ACCTBAL", "DECIMAL(15, 2)"),
            ("C_MKTSEGMENT", "VARCHAR"),
            ("C_COMMENT", "VARCHAR"),
        ],
    },
    TableSpec {
        file_stem: "orders",
        sql_name: "ORDERS",
        columns: &[
            ("O_ORDERKEY", "INT"),
            ("O_CUSTKEY", "INT"),
            ("O_ORDERSTATUS", "VARCHAR"),
            ("O_TOTALPRICE", "DECIMAL(15, 2)"),
            ("O_ORDERDATE", "DATE"),
            ("O_ORDERPRIORITY", "VARCHAR"),
            ("O_CLERK", "VARCHAR"),
            ("O_SHIPPRIORITY", "INT"),
            ("O_COMMENT", "VARCHAR"),
        ],
    },
    TableSpec {
        file_stem: "lineitem",
        sql_name: "LINEITEM",
        columns: &[
            ("L_ORDERKEY", "INT"),
            ("L_PARTKEY", "INT"),
            ("L_SUPPKEY", "INT"),
            ("L_LINENUMBER", "INT"),
            ("L_QUANTITY", "DECIMAL(15, 2)"),
            ("L_EXTENDEDPRICE", "DECIMAL(15, 2)"),
            ("L_DISCOUNT", "DECIMAL(15, 2)"),
            ("L_TAX", "DECIMAL(15, 2)"),
            ("L_RETURNFLAG", "VARCHAR"),
            ("L_LINESTATUS", "VARCHAR"),
            ("L_SHIPDATE", "DATE"),
            ("L_COMMITDATE", "DATE"),
            ("L_RECEIPTDATE", "DATE"),
            ("L_SHIPINSTRUCT", "VARCHAR"),
            ("L_SHIPMODE", "VARCHAR"),
            ("L_COMMENT", "VARCHAR"),
        ],
    },
];

pub async fn register_tpch_tables(ctx: &SessionContext, data_dir: &Path) -> Result<()> {
    for spec in TABLES {
        let path = data_dir.join(format!("{}.csv", spec.file_stem));
        if !path.exists() {
            return error::MissingSuitePathSnafu {
                path: path.display().to_string(),
            }
            .fail();
        }

        let raw_name = format!("{}_raw", spec.file_stem);
        let fields: Vec<Field> = (1..=spec.columns.len())
            .map(|i| Field::new(format!("column_{i}"), DataType::Utf8, true))
            .collect();
        let schema = Schema::new(fields);
        let options = CsvReadOptions::new()
            .has_header(false)
            .delimiter(b'|')
            .schema(&schema);

        ctx.register_csv(&raw_name, path.to_string_lossy().as_ref(), options)
            .await
            .context(error::RegisterTableSnafu {
                table: spec.sql_name,
            })?;

        let select = spec
            .columns
            .iter()
            .enumerate()
            .map(|(idx, (name, ty))| format!(r#"CAST(column_{} AS {ty}) AS "{name}""#, idx + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"CREATE VIEW "{}" AS SELECT {select} FROM {raw_name}"#,
            spec.sql_name
        );
        ctx.sql(&sql)
            .await
            .context(error::RegisterTableSnafu {
                table: spec.sql_name,
            })?
            .collect()
            .await
            .context(error::RegisterTableSnafu {
                table: spec.sql_name,
            })?;

        // Plans name tables in UPPERCASE; some consumers look up the
        // lowercase file stem. Register a second view so either works.
        let alias_sql = format!(
            r#"CREATE VIEW "{}" AS SELECT * FROM "{}""#,
            spec.file_stem, spec.sql_name
        );
        ctx.sql(&alias_sql)
            .await
            .context(error::RegisterTableSnafu {
                table: spec.file_stem,
            })?
            .collect()
            .await
            .context(error::RegisterTableSnafu {
                table: spec.file_stem,
            })?;
    }
    Ok(())
}
