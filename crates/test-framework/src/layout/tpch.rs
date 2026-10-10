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

//! TPC-H layout keys. The primary keys are the specification's (unique in
//! `dbgen` output at every scale factor), the indexes follow
//! `test/tpc-bench/tpch_index.sql` plus the columns the queries filter on, and
//! each sort order differs from the order `dbgen` writes the rows in.

use spicepod::component::dataset::TimeFormat;

use super::TableKeys;

pub(super) static TABLES: &[TableKeys] = &[
    TableKeys {
        table: "lineitem",
        primary_key: &["l_orderkey", "l_linenumber"],
        indexes: &[
            &["l_orderkey"],
            &["l_partkey"],
            &["l_suppkey"],
            &["l_shipdate"],
            &["l_partkey", "l_suppkey"],
        ],
        sort: &["l_shipdate", "l_orderkey"],
        cluster: &["l_shipdate", "l_partkey"],
        time_column: Some(("l_shipdate", TimeFormat::Date)),
        partition_by: Some("date_part('year', l_shipdate)"),
    },
    TableKeys {
        table: "orders",
        primary_key: &["o_orderkey"],
        indexes: &[
            &["o_custkey"],
            &["o_orderdate"],
            &["o_custkey", "o_orderdate"],
        ],
        sort: &["o_orderdate", "o_orderkey"],
        cluster: &["o_orderdate", "o_custkey"],
        time_column: Some(("o_orderdate", TimeFormat::Date)),
        partition_by: Some("date_part('year', o_orderdate)"),
    },
    TableKeys {
        table: "customer",
        primary_key: &["c_custkey"],
        indexes: &[&["c_nationkey"], &["c_mktsegment"]],
        sort: &["c_nationkey", "c_custkey"],
        cluster: &["c_nationkey", "c_acctbal"],
        time_column: None,
        partition_by: Some("bucket(4, c_custkey)"),
    },
    TableKeys {
        table: "part",
        primary_key: &["p_partkey"],
        indexes: &[&["p_brand", "p_container"], &["p_type"], &["p_size"]],
        sort: &["p_brand", "p_partkey"],
        cluster: &["p_brand", "p_size"],
        time_column: None,
        partition_by: Some("bucket(4, p_partkey)"),
    },
    TableKeys {
        table: "partsupp",
        primary_key: &["ps_partkey", "ps_suppkey"],
        indexes: &[&["ps_suppkey"], &["ps_partkey"]],
        sort: &["ps_suppkey", "ps_partkey"],
        cluster: &["ps_suppkey", "ps_partkey"],
        time_column: None,
        partition_by: Some("bucket(4, ps_partkey)"),
    },
    TableKeys {
        table: "supplier",
        primary_key: &["s_suppkey"],
        indexes: &[&["s_nationkey"]],
        sort: &["s_nationkey", "s_suppkey"],
        cluster: &["s_nationkey"],
        time_column: None,
        partition_by: Some("bucket(4, s_suppkey)"),
    },
    TableKeys {
        table: "nation",
        primary_key: &["n_nationkey"],
        indexes: &[&["n_regionkey"], &["n_name"]],
        sort: &["n_name"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
    TableKeys {
        table: "region",
        primary_key: &["r_regionkey"],
        indexes: &[&["r_name"]],
        sort: &["r_name"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
];
