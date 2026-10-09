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

//! CH-benCHmark layout keys, for the schema `tools/chbench-driver` creates on
//! `PostgreSQL` and `MySQL`. The primary keys are that DDL's; `history` declares
//! none, so it has none here. The indexes follow the driver's own secondary
//! indexes and the queries' join and filter columns, and the clustering columns
//! are those the adaptive CH-benCH Spicepods cluster on.
//!
//! The OLTP workload sets `o_carrier_id` and `ol_delivery_d`, NULL until an
//! order is delivered, so a table sorted or timed on them sees updates move
//! rows between sort positions — under CDC, the paths a layout most changes.
//! The seed writes one constant timestamp, which the workload's own writes then
//! follow.
//!
//! Partitions take no column an update changes: a keyed partitioned
//! acceleration resolves each key within its partition, so an update that moved
//! a row to another partition would leave its old version behind (#14596).
//! Every keyed table partitions on its own primary key, and on the district or
//! item column rather than the warehouse: SF 1 has one warehouse, which would
//! put every row in one partition and leave the cross-partition write untested.

use spicepod::component::dataset::TimeFormat;

use super::TableKeys;

pub(super) static TABLES: &[TableKeys] = &[
    TableKeys {
        table: "warehouse",
        primary_key: &["w_id"],
        indexes: &[&["w_state"]],
        sort: &["w_state", "w_id"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
    TableKeys {
        table: "district",
        primary_key: &["d_w_id", "d_id"],
        indexes: &[&["d_w_id"]],
        sort: &["d_next_o_id", "d_id"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
    TableKeys {
        table: "customer",
        primary_key: &["c_w_id", "c_d_id", "c_id"],
        indexes: &[&["c_w_id", "c_d_id", "c_last", "c_first"], &["c_state"]],
        sort: &["c_state", "c_id"],
        cluster: &["c_w_id", "c_state"],
        time_column: Some(("c_since", TimeFormat::Timestamp)),
        partition_by: Some("bucket(4, c_d_id)"),
    },
    TableKeys {
        table: "history",
        primary_key: &[],
        indexes: &[&["h_w_id"], &["h_c_w_id"]],
        sort: &["h_date"],
        cluster: &["h_w_id", "h_d_id"],
        time_column: Some(("h_date", TimeFormat::Timestamp)),
        partition_by: Some("bucket(4, h_d_id)"),
    },
    TableKeys {
        table: "new_order",
        primary_key: &["no_w_id", "no_d_id", "no_o_id"],
        indexes: &[&["no_w_id", "no_d_id"]],
        sort: &["no_o_id"],
        cluster: &["no_w_id", "no_d_id"],
        time_column: None,
        partition_by: Some("bucket(4, no_d_id)"),
    },
    TableKeys {
        table: "oorder",
        primary_key: &["o_w_id", "o_d_id", "o_id"],
        indexes: &[&["o_w_id", "o_d_id", "o_c_id", "o_id"], &["o_entry_d"]],
        sort: &["o_entry_d", "o_id"],
        cluster: &["o_w_id", "o_carrier_id"],
        time_column: Some(("o_entry_d", TimeFormat::Timestamp)),
        partition_by: Some("bucket(4, o_d_id)"),
    },
    TableKeys {
        table: "order_line",
        primary_key: &["ol_w_id", "ol_d_id", "ol_o_id", "ol_number"],
        indexes: &[
            &["ol_i_id"],
            &["ol_supply_w_id", "ol_i_id"],
            &["ol_delivery_d"],
        ],
        sort: &["ol_delivery_d", "ol_o_id"],
        cluster: &["ol_w_id", "ol_i_id"],
        time_column: Some(("ol_delivery_d", TimeFormat::Timestamp)),
        partition_by: Some("bucket(4, ol_d_id)"),
    },
    TableKeys {
        table: "stock",
        primary_key: &["s_w_id", "s_i_id"],
        indexes: &[&["s_i_id"], &["s_quantity"]],
        sort: &["s_quantity", "s_i_id"],
        cluster: &["s_w_id", "s_quantity"],
        time_column: None,
        partition_by: Some("bucket(4, s_i_id)"),
    },
    TableKeys {
        table: "item",
        primary_key: &["i_id"],
        indexes: &[&["i_im_id"], &["i_price"]],
        sort: &["i_price", "i_id"],
        cluster: &["i_im_id"],
        time_column: None,
        partition_by: Some("bucket(4, i_id)"),
    },
    TableKeys {
        table: "nation",
        primary_key: &["n_nationkey"],
        indexes: &[&["n_regionkey"]],
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
    TableKeys {
        table: "supplier",
        primary_key: &["su_suppkey"],
        indexes: &[&["su_nationkey"]],
        sort: &["su_nationkey", "su_suppkey"],
        cluster: &["su_nationkey"],
        time_column: None,
        partition_by: None,
    },
];
