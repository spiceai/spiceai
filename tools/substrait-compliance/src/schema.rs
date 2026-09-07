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

//! TPC-H Arrow schemas matching the Isthmus-produced Substrait plans in
//! `IBM/substrait-compliance` v0.1.1 (`test-suites/tpch/plans/q01.json` uses
//! `i32` keys, `decimal(15, 2)` money columns, `date`, and `fixedChar`/`varchar`).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

const DECIMAL_MONEY: DataType = DataType::Decimal128(15, 2);

/// Canonical on-disk file stem (lowercase) for a TPC-H table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TpchTable {
    pub file_stem: &'static str,
    pub plan_name: &'static str,
}

pub const TPCH_TABLES: &[TpchTable] = &[
    TpchTable {
        file_stem: "region",
        plan_name: "REGION",
    },
    TpchTable {
        file_stem: "nation",
        plan_name: "NATION",
    },
    TpchTable {
        file_stem: "part",
        plan_name: "PART",
    },
    TpchTable {
        file_stem: "supplier",
        plan_name: "SUPPLIER",
    },
    TpchTable {
        file_stem: "partsupp",
        plan_name: "PARTSUPP",
    },
    TpchTable {
        file_stem: "customer",
        plan_name: "CUSTOMER",
    },
    TpchTable {
        file_stem: "orders",
        plan_name: "ORDERS",
    },
    TpchTable {
        file_stem: "lineitem",
        plan_name: "LINEITEM",
    },
];

#[must_use]
pub fn schema_for(file_stem: &str) -> Option<SchemaRef> {
    Some(match file_stem.to_ascii_lowercase().as_str() {
        "region" => region(),
        "nation" => nation(),
        "part" => part(),
        "supplier" => supplier(),
        "partsupp" => partsupp(),
        "customer" => customer(),
        "orders" => orders(),
        "lineitem" => lineitem(),
        _ => return None,
    })
}

fn fields(cols: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        cols.iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn region() -> SchemaRef {
    fields(&[
        ("R_REGIONKEY", DataType::Int32),
        ("R_NAME", DataType::Utf8),
        ("R_COMMENT", DataType::Utf8),
    ])
}

fn nation() -> SchemaRef {
    fields(&[
        ("N_NATIONKEY", DataType::Int32),
        ("N_NAME", DataType::Utf8),
        ("N_REGIONKEY", DataType::Int32),
        ("N_COMMENT", DataType::Utf8),
    ])
}

fn part() -> SchemaRef {
    fields(&[
        ("P_PARTKEY", DataType::Int32),
        ("P_NAME", DataType::Utf8),
        ("P_MFGR", DataType::Utf8),
        ("P_BRAND", DataType::Utf8),
        ("P_TYPE", DataType::Utf8),
        ("P_SIZE", DataType::Int32),
        ("P_CONTAINER", DataType::Utf8),
        ("P_RETAILPRICE", DECIMAL_MONEY),
        ("P_COMMENT", DataType::Utf8),
    ])
}

fn supplier() -> SchemaRef {
    fields(&[
        ("S_SUPPKEY", DataType::Int32),
        ("S_NAME", DataType::Utf8),
        ("S_ADDRESS", DataType::Utf8),
        ("S_NATIONKEY", DataType::Int32),
        ("S_PHONE", DataType::Utf8),
        ("S_ACCTBAL", DECIMAL_MONEY),
        ("S_COMMENT", DataType::Utf8),
    ])
}

fn partsupp() -> SchemaRef {
    fields(&[
        ("PS_PARTKEY", DataType::Int32),
        ("PS_SUPPKEY", DataType::Int32),
        ("PS_AVAILQTY", DataType::Int32),
        ("PS_SUPPLYCOST", DECIMAL_MONEY),
        ("PS_COMMENT", DataType::Utf8),
    ])
}

fn customer() -> SchemaRef {
    fields(&[
        ("C_CUSTKEY", DataType::Int32),
        ("C_NAME", DataType::Utf8),
        ("C_ADDRESS", DataType::Utf8),
        ("C_NATIONKEY", DataType::Int32),
        ("C_PHONE", DataType::Utf8),
        ("C_ACCTBAL", DECIMAL_MONEY),
        ("C_MKTSEGMENT", DataType::Utf8),
        ("C_COMMENT", DataType::Utf8),
    ])
}

fn orders() -> SchemaRef {
    fields(&[
        ("O_ORDERKEY", DataType::Int32),
        ("O_CUSTKEY", DataType::Int32),
        ("O_ORDERSTATUS", DataType::Utf8),
        ("O_TOTALPRICE", DECIMAL_MONEY),
        ("O_ORDERDATE", DataType::Date32),
        ("O_ORDERPRIORITY", DataType::Utf8),
        ("O_CLERK", DataType::Utf8),
        ("O_SHIPPRIORITY", DataType::Int32),
        ("O_COMMENT", DataType::Utf8),
    ])
}

fn lineitem() -> SchemaRef {
    fields(&[
        ("L_ORDERKEY", DataType::Int32),
        ("L_PARTKEY", DataType::Int32),
        ("L_SUPPKEY", DataType::Int32),
        ("L_LINENUMBER", DataType::Int32),
        ("L_QUANTITY", DECIMAL_MONEY),
        ("L_EXTENDEDPRICE", DECIMAL_MONEY),
        ("L_DISCOUNT", DECIMAL_MONEY),
        ("L_TAX", DECIMAL_MONEY),
        ("L_RETURNFLAG", DataType::Utf8),
        ("L_LINESTATUS", DataType::Utf8),
        ("L_SHIPDATE", DataType::Date32),
        ("L_COMMITDATE", DataType::Date32),
        ("L_RECEIPTDATE", DataType::Date32),
        ("L_SHIPINSTRUCT", DataType::Utf8),
        ("L_SHIPMODE", DataType::Utf8),
        ("L_COMMENT", DataType::Utf8),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_catalogued_table_has_a_schema() {
        for table in TPCH_TABLES {
            let schema = schema_for(table.file_stem).expect("schema for catalogued table");
            assert!(
                !schema.fields().is_empty(),
                "{} schema must not be empty",
                table.file_stem
            );
        }
    }

    #[test]
    fn unknown_table_has_no_schema() {
        assert!(schema_for("not_a_tpch_table").is_none());
    }
}
