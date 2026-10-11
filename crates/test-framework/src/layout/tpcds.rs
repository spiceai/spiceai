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

//! TPC-DS layout keys. The primary keys are the specification's, which hold in
//! `dsdgen` output: unique, with no NULL key column. The indexes follow
//! `test/tpc-bench/tpcds_index.sql` and the fact tables' join keys. The fact
//! tables sort, cluster and partition on their date keys, which are NULL on a
//! share of rows (130K of 2.9M `store_sales` rows at scale factor 1), so every
//! one of those layouts also exercises its engine's NULL handling.
//!
//! The time columns are the `*_rec_start_date` dates of the slowly-changing
//! dimensions and `d_date`: TPC-DS fact tables carry date surrogate keys, not
//! dates. `i_rec_start_date` is NULL on 46 of 18K items.

use spicepod::component::dataset::TimeFormat;

use super::TableKeys;

pub(super) static TABLES: &[TableKeys] = &[
    // ---- fact tables ----
    TableKeys {
        table: "store_sales",
        primary_key: &["ss_item_sk", "ss_ticket_number"],
        indexes: &[
            &["ss_sold_date_sk"],
            &["ss_customer_sk"],
            &["ss_item_sk"],
            &["ss_store_sk"],
            &["ss_sold_date_sk", "ss_item_sk", "ss_customer_sk"],
            &["ss_customer_sk", "ss_sold_date_sk"],
        ],
        sort: &["ss_sold_date_sk", "ss_item_sk"],
        cluster: &["ss_sold_date_sk", "ss_store_sk"],
        time_column: None,
        partition_by: Some("bucket(5, ss_sold_date_sk)"),
    },
    TableKeys {
        table: "store_returns",
        primary_key: &["sr_item_sk", "sr_ticket_number"],
        indexes: &[
            &["sr_returned_date_sk", "sr_store_sk"],
            &["sr_customer_sk"],
            &["sr_item_sk"],
        ],
        sort: &["sr_returned_date_sk", "sr_item_sk"],
        cluster: &["sr_returned_date_sk", "sr_store_sk"],
        time_column: None,
        partition_by: Some("bucket(5, sr_returned_date_sk)"),
    },
    TableKeys {
        table: "catalog_sales",
        primary_key: &["cs_item_sk", "cs_order_number"],
        indexes: &[
            &["cs_sold_date_sk", "cs_item_sk"],
            &["cs_item_sk"],
            &["cs_ship_customer_sk"],
            &["cs_bill_customer_sk", "cs_sold_date_sk"],
            &["cs_sold_date_sk", "cs_catalog_page_sk"],
        ],
        sort: &["cs_sold_date_sk", "cs_item_sk"],
        cluster: &["cs_sold_date_sk", "cs_item_sk"],
        time_column: None,
        partition_by: Some("bucket(5, cs_sold_date_sk)"),
    },
    TableKeys {
        table: "catalog_returns",
        primary_key: &["cr_item_sk", "cr_order_number"],
        indexes: &[
            &["cr_returned_date_sk", "cr_catalog_page_sk"],
            &["cr_item_sk"],
        ],
        sort: &["cr_returned_date_sk", "cr_item_sk"],
        cluster: &["cr_returned_date_sk", "cr_item_sk"],
        time_column: None,
        partition_by: Some("bucket(5, cr_returned_date_sk)"),
    },
    TableKeys {
        table: "web_sales",
        primary_key: &["ws_item_sk", "ws_order_number"],
        indexes: &[
            &["ws_bill_customer_sk"],
            &["ws_sold_date_sk"],
            &["ws_bill_customer_sk", "ws_sold_date_sk"],
            &["ws_sold_date_sk", "ws_web_site_sk"],
        ],
        sort: &["ws_sold_date_sk", "ws_item_sk"],
        cluster: &["ws_sold_date_sk", "ws_item_sk"],
        time_column: None,
        partition_by: Some("bucket(5, ws_sold_date_sk)"),
    },
    TableKeys {
        table: "web_returns",
        primary_key: &["wr_item_sk", "wr_order_number"],
        indexes: &[&["wr_returned_date_sk"], &["wr_item_sk"]],
        sort: &["wr_returned_date_sk", "wr_item_sk"],
        cluster: &["wr_returned_date_sk", "wr_item_sk"],
        time_column: None,
        partition_by: Some("bucket(5, wr_returned_date_sk)"),
    },
    TableKeys {
        table: "inventory",
        primary_key: &["inv_date_sk", "inv_item_sk", "inv_warehouse_sk"],
        indexes: &[&["inv_item_sk"], &["inv_warehouse_sk"]],
        sort: &["inv_item_sk", "inv_date_sk"],
        cluster: &["inv_date_sk", "inv_item_sk"],
        time_column: None,
        partition_by: Some("bucket(5, inv_date_sk)"),
    },
    // ---- dimensions with a date ----
    TableKeys {
        table: "date_dim",
        primary_key: &["d_date_sk"],
        indexes: &[&["d_date"], &["d_year"], &["d_year", "d_moy"]],
        sort: &["d_dow", "d_date_sk"],
        cluster: &["d_year", "d_moy"],
        time_column: Some(("d_date", TimeFormat::Date)),
        partition_by: Some("date_part('year', d_date)"),
    },
    TableKeys {
        table: "item",
        primary_key: &["i_item_sk"],
        indexes: &[
            &["i_category_id", "i_brand_id"],
            &["i_item_sk", "i_category_id"],
            &["i_manufact_id"],
        ],
        sort: &["i_category", "i_item_sk"],
        cluster: &["i_category_id", "i_brand_id"],
        time_column: Some(("i_rec_start_date", TimeFormat::Date)),
        partition_by: Some("bucket(4, i_item_sk)"),
    },
    TableKeys {
        table: "store",
        primary_key: &["s_store_sk"],
        indexes: &[&["s_store_sk", "s_store_name"], &["s_state"]],
        sort: &["s_state", "s_store_sk"],
        cluster: &[],
        time_column: Some(("s_rec_start_date", TimeFormat::Date)),
        partition_by: None,
    },
    TableKeys {
        table: "call_center",
        primary_key: &["cc_call_center_sk"],
        indexes: &[&["cc_call_center_sk", "cc_country"]],
        sort: &["cc_name"],
        cluster: &[],
        time_column: Some(("cc_rec_start_date", TimeFormat::Date)),
        partition_by: None,
    },
    TableKeys {
        table: "web_page",
        primary_key: &["wp_web_page_sk"],
        indexes: &[&["wp_char_count"]],
        sort: &["wp_char_count", "wp_web_page_sk"],
        cluster: &[],
        time_column: Some(("wp_rec_start_date", TimeFormat::Date)),
        partition_by: None,
    },
    TableKeys {
        table: "web_site",
        primary_key: &["web_site_sk"],
        indexes: &[&["web_name"]],
        sort: &["web_name", "web_site_sk"],
        cluster: &[],
        time_column: Some(("web_rec_start_date", TimeFormat::Date)),
        partition_by: None,
    },
    // ---- dimensions without a date ----
    TableKeys {
        table: "time_dim",
        primary_key: &["t_time_sk"],
        indexes: &[&["t_time_id"], &["t_hour"]],
        sort: &["t_minute", "t_time_sk"],
        cluster: &["t_hour", "t_minute"],
        time_column: None,
        partition_by: Some("bucket(4, t_time_sk)"),
    },
    TableKeys {
        table: "customer",
        primary_key: &["c_customer_sk"],
        indexes: &[
            &["c_last_name", "c_first_name"],
            &["c_current_addr_sk"],
            &["c_current_cdemo_sk"],
        ],
        sort: &["c_birth_country", "c_customer_sk"],
        cluster: &["c_birth_year", "c_birth_month"],
        time_column: None,
        partition_by: Some("bucket(4, c_customer_sk)"),
    },
    TableKeys {
        table: "customer_address",
        primary_key: &["ca_address_sk"],
        indexes: &[&["ca_county"], &["ca_state"], &["ca_zip"]],
        sort: &["ca_state", "ca_address_sk"],
        cluster: &["ca_state", "ca_county"],
        time_column: None,
        partition_by: Some("bucket(4, ca_address_sk)"),
    },
    TableKeys {
        table: "customer_demographics",
        primary_key: &["cd_demo_sk"],
        indexes: &[&["cd_gender", "cd_marital_status", "cd_education_status"]],
        sort: &["cd_education_status", "cd_demo_sk"],
        cluster: &["cd_gender", "cd_marital_status"],
        time_column: None,
        partition_by: Some("bucket(4, cd_demo_sk)"),
    },
    TableKeys {
        table: "household_demographics",
        primary_key: &["hd_demo_sk"],
        indexes: &[&["hd_income_band_sk"], &["hd_buy_potential"]],
        sort: &["hd_buy_potential", "hd_demo_sk"],
        cluster: &["hd_dep_count", "hd_vehicle_count"],
        time_column: None,
        partition_by: Some("bucket(4, hd_demo_sk)"),
    },
    TableKeys {
        table: "catalog_page",
        primary_key: &["cp_catalog_page_sk"],
        indexes: &[&["cp_catalog_page_id"]],
        sort: &["cp_department", "cp_catalog_page_sk"],
        cluster: &["cp_catalog_number", "cp_catalog_page_number"],
        time_column: None,
        partition_by: Some("bucket(4, cp_catalog_page_sk)"),
    },
    TableKeys {
        table: "income_band",
        primary_key: &["ib_income_band_sk"],
        indexes: &[&["ib_lower_bound", "ib_upper_bound"]],
        sort: &["ib_upper_bound"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
    TableKeys {
        table: "promotion",
        primary_key: &["p_promo_sk"],
        indexes: &[&["p_channel_email", "p_channel_event"]],
        sort: &["p_promo_id"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
    TableKeys {
        table: "reason",
        primary_key: &["r_reason_sk"],
        indexes: &[&["r_reason_desc"]],
        sort: &["r_reason_desc"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
    TableKeys {
        table: "ship_mode",
        primary_key: &["sm_ship_mode_sk"],
        indexes: &[&["sm_type"]],
        sort: &["sm_carrier"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
    TableKeys {
        table: "warehouse",
        primary_key: &["w_warehouse_sk"],
        indexes: &[&["w_state"]],
        sort: &["w_warehouse_name"],
        cluster: &[],
        time_column: None,
        partition_by: None,
    },
];
