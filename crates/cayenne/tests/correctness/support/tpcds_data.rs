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

//! TPC-DS fixtures, generated in-process by `tpcdsgen` in its C `dsdgen`
//! compatibility mode.
//!
//! The DuckDB lane builds its TPC-DS fixture with DuckDB's own `dsdgen`
//! extension. The SQLite and chDB lanes cannot: chDB aborts a process that also
//! drives DuckDB, and `INSTALL tpcds` needs a network. `tpcdsgen` is the pure-Rust
//! port of `dsdgen` from the `tpchgen` project. Its rows are not those of DuckDB's
//! `dsdgen`: at SF1 the two agree on row counts, keys and dimension tables such as
//! `item`, and differ in fact-table measures (`store_sales` `sum(ss_quantity)` is
//! 138,963,631 against 138,943,711). A query can therefore select rows from one
//! fixture and none from the other, which is why the inventory reviews an empty
//! answer per fixture.
//!
//! [`SCHEMAS`] gives each table the column types DuckDB's export of `dsdgen`
//! data carries, so the fixture matches the DuckDB lane's column for column.
//! Values arrive as `dsdgen`'s text fields, an empty field being NULL, and are
//! parsed into those types; one that does not parse fails the load rather than
//! loading as NULL.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, Date32Builder, Decimal128Builder, Int32Builder, Int64Builder,
    RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::parquet::arrow::ArrowWriter;
use tpcdsgen::config::{CompatMode, SessionBuilder, Table};
use tpcdsgen::row::{
    CallCenterRowGenerator, CatalogPageRowGenerator, CatalogSalesRowGenerator,
    CustomerAddressRowGenerator, CustomerDemographicsRowGenerator, CustomerRowGenerator,
    DateDimRowGenerator, GeneratedRow, HouseholdDemographicsRowGenerator, IncomeBandRowGenerator,
    InventoryRowGenerator, ItemRowGenerator, PromotionRowGenerator, ReasonRowGenerator,
    RowGenerator, ShipModeRowGenerator, StoreRowGenerator, StoreSalesRowGenerator, TableRow,
    TimeDimRowGenerator, WarehouseRowGenerator, WebPageRowGenerator, WebSalesRowGenerator,
    WebSiteRowGenerator,
};

/// A column's type, as DuckDB exports `dsdgen` data.
#[derive(Clone, Copy, Debug)]
enum Ty {
    I64,
    I32,
    Dec(u8, i8),
    Date,
    Str,
}

use Ty::{Date, Dec, I32, I64, Str};

/// Every TPC-DS table the suite queries, in the column order `dsdgen` writes.
#[rustfmt::skip]
const SCHEMAS: &[(&str, &[(&str, Ty)])] = &[
    (
        "call_center",
        &[
            ("cc_call_center_sk", I64),
            ("cc_call_center_id", Str),
            ("cc_rec_start_date", Date),
            ("cc_rec_end_date", Date),
            ("cc_closed_date_sk", I64),
            ("cc_open_date_sk", I64),
            ("cc_name", Str),
            ("cc_class", Str),
            ("cc_employees", I64),
            ("cc_sq_ft", I64),
            ("cc_hours", Str),
            ("cc_manager", Str),
            ("cc_mkt_id", I64),
            ("cc_mkt_class", Str),
            ("cc_mkt_desc", Str),
            ("cc_market_manager", Str),
            ("cc_division", I64),
            ("cc_division_name", Str),
            ("cc_company", I64),
            ("cc_company_name", Str),
            ("cc_street_number", Str),
            ("cc_street_name", Str),
            ("cc_street_type", Str),
            ("cc_suite_number", Str),
            ("cc_city", Str),
            ("cc_county", Str),
            ("cc_state", Str),
            ("cc_zip", Str),
            ("cc_country", Str),
            ("cc_gmt_offset", Dec(5, 2)),
            ("cc_tax_percentage", Dec(5, 2)),
        ],
    ),
    (
        "catalog_page",
        &[
            ("cp_catalog_page_sk", I64),
            ("cp_catalog_page_id", Str),
            ("cp_start_date_sk", I64),
            ("cp_end_date_sk", I64),
            ("cp_department", Str),
            ("cp_catalog_number", I64),
            ("cp_catalog_page_number", I64),
            ("cp_description", Str),
            ("cp_type", Str),
        ],
    ),
    (
        "catalog_returns",
        &[
            ("cr_returned_date_sk", I64),
            ("cr_returned_time_sk", I64),
            ("cr_item_sk", I64),
            ("cr_refunded_customer_sk", I64),
            ("cr_refunded_cdemo_sk", I64),
            ("cr_refunded_hdemo_sk", I64),
            ("cr_refunded_addr_sk", I64),
            ("cr_returning_customer_sk", I64),
            ("cr_returning_cdemo_sk", I64),
            ("cr_returning_hdemo_sk", I64),
            ("cr_returning_addr_sk", I64),
            ("cr_call_center_sk", I64),
            ("cr_catalog_page_sk", I64),
            ("cr_ship_mode_sk", I64),
            ("cr_warehouse_sk", I64),
            ("cr_reason_sk", I64),
            ("cr_order_number", I64),
            ("cr_return_quantity", I64),
            ("cr_return_amount", Dec(7, 2)),
            ("cr_return_tax", Dec(7, 2)),
            ("cr_return_amt_inc_tax", Dec(7, 2)),
            ("cr_fee", Dec(7, 2)),
            ("cr_return_ship_cost", Dec(7, 2)),
            ("cr_refunded_cash", Dec(7, 2)),
            ("cr_reversed_charge", Dec(7, 2)),
            ("cr_store_credit", Dec(7, 2)),
            ("cr_net_loss", Dec(7, 2)),
        ],
    ),
    (
        "catalog_sales",
        &[
            ("cs_sold_date_sk", I64),
            ("cs_sold_time_sk", I64),
            ("cs_ship_date_sk", I64),
            ("cs_bill_customer_sk", I64),
            ("cs_bill_cdemo_sk", I64),
            ("cs_bill_hdemo_sk", I64),
            ("cs_bill_addr_sk", I64),
            ("cs_ship_customer_sk", I64),
            ("cs_ship_cdemo_sk", I64),
            ("cs_ship_hdemo_sk", I64),
            ("cs_ship_addr_sk", I64),
            ("cs_call_center_sk", I64),
            ("cs_catalog_page_sk", I64),
            ("cs_ship_mode_sk", I64),
            ("cs_warehouse_sk", I64),
            ("cs_item_sk", I64),
            ("cs_promo_sk", I64),
            ("cs_order_number", I64),
            ("cs_quantity", I64),
            ("cs_wholesale_cost", Dec(7, 2)),
            ("cs_list_price", Dec(7, 2)),
            ("cs_sales_price", Dec(7, 2)),
            ("cs_ext_discount_amt", Dec(7, 2)),
            ("cs_ext_sales_price", Dec(7, 2)),
            ("cs_ext_wholesale_cost", Dec(7, 2)),
            ("cs_ext_list_price", Dec(7, 2)),
            ("cs_ext_tax", Dec(7, 2)),
            ("cs_coupon_amt", Dec(7, 2)),
            ("cs_ext_ship_cost", Dec(7, 2)),
            ("cs_net_paid", Dec(7, 2)),
            ("cs_net_paid_inc_tax", Dec(7, 2)),
            ("cs_net_paid_inc_ship", Dec(7, 2)),
            ("cs_net_paid_inc_ship_tax", Dec(7, 2)),
            ("cs_net_profit", Dec(7, 2)),
        ],
    ),
    (
        "customer",
        &[
            ("c_customer_sk", I64),
            ("c_customer_id", Str),
            ("c_current_cdemo_sk", I64),
            ("c_current_hdemo_sk", I64),
            ("c_current_addr_sk", I64),
            ("c_first_shipto_date_sk", I64),
            ("c_first_sales_date_sk", I64),
            ("c_salutation", Str),
            ("c_first_name", Str),
            ("c_last_name", Str),
            ("c_preferred_cust_flag", Str),
            ("c_birth_day", I64),
            ("c_birth_month", I64),
            ("c_birth_year", I64),
            ("c_birth_country", Str),
            ("c_login", Str),
            ("c_email_address", Str),
            ("c_last_review_date_sk", I32),
        ],
    ),
    (
        "customer_address",
        &[
            ("ca_address_sk", I64),
            ("ca_address_id", Str),
            ("ca_street_number", Str),
            ("ca_street_name", Str),
            ("ca_street_type", Str),
            ("ca_suite_number", Str),
            ("ca_city", Str),
            ("ca_county", Str),
            ("ca_state", Str),
            ("ca_zip", Str),
            ("ca_country", Str),
            ("ca_gmt_offset", Dec(5, 2)),
            ("ca_location_type", Str),
        ],
    ),
    (
        "customer_demographics",
        &[
            ("cd_demo_sk", I64),
            ("cd_gender", Str),
            ("cd_marital_status", Str),
            ("cd_education_status", Str),
            ("cd_purchase_estimate", I64),
            ("cd_credit_rating", Str),
            ("cd_dep_count", I64),
            ("cd_dep_employed_count", I64),
            ("cd_dep_college_count", I32),
        ],
    ),
    (
        "date_dim",
        &[
            ("d_date_sk", I64),
            ("d_date_id", Str),
            ("d_date", Date),
            ("d_month_seq", I64),
            ("d_week_seq", I64),
            ("d_quarter_seq", I64),
            ("d_year", I64),
            ("d_dow", I64),
            ("d_moy", I64),
            ("d_dom", I64),
            ("d_qoy", I64),
            ("d_fy_year", I64),
            ("d_fy_quarter_seq", I64),
            ("d_fy_week_seq", I64),
            ("d_day_name", Str),
            ("d_quarter_name", Str),
            ("d_holiday", Str),
            ("d_weekend", Str),
            ("d_following_holiday", Str),
            ("d_first_dom", I64),
            ("d_last_dom", I64),
            ("d_same_day_ly", I64),
            ("d_same_day_lq", I64),
            ("d_current_day", Str),
            ("d_current_week", Str),
            ("d_current_month", Str),
            ("d_current_quarter", Str),
            ("d_current_year", Str),
        ],
    ),
    (
        "household_demographics",
        &[
            ("hd_demo_sk", I64),
            ("hd_income_band_sk", I64),
            ("hd_buy_potential", Str),
            ("hd_dep_count", I64),
            ("hd_vehicle_count", I32),
        ],
    ),
    (
        "income_band",
        &[
            ("ib_income_band_sk", I64),
            ("ib_lower_bound", I64),
            ("ib_upper_bound", I32),
        ],
    ),
    (
        "inventory",
        &[
            ("inv_date_sk", I64),
            ("inv_item_sk", I64),
            ("inv_warehouse_sk", I64),
            ("inv_quantity_on_hand", I32),
        ],
    ),
    (
        "item",
        &[
            ("i_item_sk", I64),
            ("i_item_id", Str),
            ("i_rec_start_date", Date),
            ("i_rec_end_date", Date),
            ("i_item_desc", Str),
            ("i_current_price", Dec(7, 2)),
            ("i_wholesale_cost", Dec(7, 2)),
            ("i_brand_id", I64),
            ("i_brand", Str),
            ("i_class_id", I64),
            ("i_class", Str),
            ("i_category_id", I64),
            ("i_category", Str),
            ("i_manufact_id", I64),
            ("i_manufact", Str),
            ("i_size", Str),
            ("i_formulation", Str),
            ("i_color", Str),
            ("i_units", Str),
            ("i_container", Str),
            ("i_manager_id", I64),
            ("i_product_name", Str),
        ],
    ),
    (
        "promotion",
        &[
            ("p_promo_sk", I64),
            ("p_promo_id", Str),
            ("p_start_date_sk", I64),
            ("p_end_date_sk", I64),
            ("p_item_sk", I64),
            ("p_cost", Dec(15, 2)),
            ("p_response_target", I64),
            ("p_promo_name", Str),
            ("p_channel_dmail", Str),
            ("p_channel_email", Str),
            ("p_channel_catalog", Str),
            ("p_channel_tv", Str),
            ("p_channel_radio", Str),
            ("p_channel_press", Str),
            ("p_channel_event", Str),
            ("p_channel_demo", Str),
            ("p_channel_details", Str),
            ("p_purpose", Str),
            ("p_discount_active", Str),
        ],
    ),
    (
        "reason",
        &[
            ("r_reason_sk", I64),
            ("r_reason_id", Str),
            ("r_reason_desc", Str),
        ],
    ),
    (
        "ship_mode",
        &[
            ("sm_ship_mode_sk", I64),
            ("sm_ship_mode_id", Str),
            ("sm_type", Str),
            ("sm_code", Str),
            ("sm_carrier", Str),
            ("sm_contract", Str),
        ],
    ),
    (
        "store",
        &[
            ("s_store_sk", I64),
            ("s_store_id", Str),
            ("s_rec_start_date", Date),
            ("s_rec_end_date", Date),
            ("s_closed_date_sk", I64),
            ("s_store_name", Str),
            ("s_number_employees", I64),
            ("s_floor_space", I64),
            ("s_hours", Str),
            ("s_manager", Str),
            ("s_market_id", I64),
            ("s_geography_class", Str),
            ("s_market_desc", Str),
            ("s_market_manager", Str),
            ("s_division_id", I64),
            ("s_division_name", Str),
            ("s_company_id", I64),
            ("s_company_name", Str),
            ("s_street_number", Str),
            ("s_street_name", Str),
            ("s_street_type", Str),
            ("s_suite_number", Str),
            ("s_city", Str),
            ("s_county", Str),
            ("s_state", Str),
            ("s_zip", Str),
            ("s_country", Str),
            ("s_gmt_offset", Dec(5, 2)),
            ("s_tax_percentage", Dec(5, 2)),
        ],
    ),
    (
        "store_returns",
        &[
            ("sr_returned_date_sk", I64),
            ("sr_return_time_sk", I64),
            ("sr_item_sk", I64),
            ("sr_customer_sk", I64),
            ("sr_cdemo_sk", I64),
            ("sr_hdemo_sk", I64),
            ("sr_addr_sk", I64),
            ("sr_store_sk", I64),
            ("sr_reason_sk", I64),
            ("sr_ticket_number", I64),
            ("sr_return_quantity", I64),
            ("sr_return_amt", Dec(7, 2)),
            ("sr_return_tax", Dec(7, 2)),
            ("sr_return_amt_inc_tax", Dec(7, 2)),
            ("sr_fee", Dec(7, 2)),
            ("sr_return_ship_cost", Dec(7, 2)),
            ("sr_refunded_cash", Dec(7, 2)),
            ("sr_reversed_charge", Dec(7, 2)),
            ("sr_store_credit", Dec(7, 2)),
            ("sr_net_loss", Dec(7, 2)),
        ],
    ),
    (
        "store_sales",
        &[
            ("ss_sold_date_sk", I64),
            ("ss_sold_time_sk", I64),
            ("ss_item_sk", I64),
            ("ss_customer_sk", I64),
            ("ss_cdemo_sk", I64),
            ("ss_hdemo_sk", I64),
            ("ss_addr_sk", I64),
            ("ss_store_sk", I64),
            ("ss_promo_sk", I64),
            ("ss_ticket_number", I64),
            ("ss_quantity", I64),
            ("ss_wholesale_cost", Dec(7, 2)),
            ("ss_list_price", Dec(7, 2)),
            ("ss_sales_price", Dec(7, 2)),
            ("ss_ext_discount_amt", Dec(7, 2)),
            ("ss_ext_sales_price", Dec(7, 2)),
            ("ss_ext_wholesale_cost", Dec(7, 2)),
            ("ss_ext_list_price", Dec(7, 2)),
            ("ss_ext_tax", Dec(7, 2)),
            ("ss_coupon_amt", Dec(7, 2)),
            ("ss_net_paid", Dec(7, 2)),
            ("ss_net_paid_inc_tax", Dec(7, 2)),
            ("ss_net_profit", Dec(7, 2)),
        ],
    ),
    (
        "time_dim",
        &[
            ("t_time_sk", I64),
            ("t_time_id", Str),
            ("t_time", I64),
            ("t_hour", I64),
            ("t_minute", I64),
            ("t_second", I64),
            ("t_am_pm", Str),
            ("t_shift", Str),
            ("t_sub_shift", Str),
            ("t_meal_time", Str),
        ],
    ),
    (
        "warehouse",
        &[
            ("w_warehouse_sk", I64),
            ("w_warehouse_id", Str),
            ("w_warehouse_name", Str),
            ("w_warehouse_sq_ft", I64),
            ("w_street_number", Str),
            ("w_street_name", Str),
            ("w_street_type", Str),
            ("w_suite_number", Str),
            ("w_city", Str),
            ("w_county", Str),
            ("w_state", Str),
            ("w_zip", Str),
            ("w_country", Str),
            ("w_gmt_offset", Dec(5, 2)),
        ],
    ),
    (
        "web_page",
        &[
            ("wp_web_page_sk", I64),
            ("wp_web_page_id", Str),
            ("wp_rec_start_date", Date),
            ("wp_rec_end_date", Date),
            ("wp_creation_date_sk", I64),
            ("wp_access_date_sk", I64),
            ("wp_autogen_flag", Str),
            ("wp_customer_sk", I64),
            ("wp_url", Str),
            ("wp_type", Str),
            ("wp_char_count", I64),
            ("wp_link_count", I64),
            ("wp_image_count", I64),
            ("wp_max_ad_count", I32),
        ],
    ),
    (
        "web_returns",
        &[
            ("wr_returned_date_sk", I64),
            ("wr_returned_time_sk", I64),
            ("wr_item_sk", I64),
            ("wr_refunded_customer_sk", I64),
            ("wr_refunded_cdemo_sk", I64),
            ("wr_refunded_hdemo_sk", I64),
            ("wr_refunded_addr_sk", I64),
            ("wr_returning_customer_sk", I64),
            ("wr_returning_cdemo_sk", I64),
            ("wr_returning_hdemo_sk", I64),
            ("wr_returning_addr_sk", I64),
            ("wr_web_page_sk", I64),
            ("wr_reason_sk", I64),
            ("wr_order_number", I64),
            ("wr_return_quantity", I64),
            ("wr_return_amt", Dec(7, 2)),
            ("wr_return_tax", Dec(7, 2)),
            ("wr_return_amt_inc_tax", Dec(7, 2)),
            ("wr_fee", Dec(7, 2)),
            ("wr_return_ship_cost", Dec(7, 2)),
            ("wr_refunded_cash", Dec(7, 2)),
            ("wr_reversed_charge", Dec(7, 2)),
            ("wr_account_credit", Dec(7, 2)),
            ("wr_net_loss", Dec(7, 2)),
        ],
    ),
    (
        "web_sales",
        &[
            ("ws_sold_date_sk", I64),
            ("ws_sold_time_sk", I64),
            ("ws_ship_date_sk", I64),
            ("ws_item_sk", I64),
            ("ws_bill_customer_sk", I64),
            ("ws_bill_cdemo_sk", I64),
            ("ws_bill_hdemo_sk", I64),
            ("ws_bill_addr_sk", I64),
            ("ws_ship_customer_sk", I64),
            ("ws_ship_cdemo_sk", I64),
            ("ws_ship_hdemo_sk", I64),
            ("ws_ship_addr_sk", I64),
            ("ws_web_page_sk", I64),
            ("ws_web_site_sk", I64),
            ("ws_ship_mode_sk", I64),
            ("ws_warehouse_sk", I64),
            ("ws_promo_sk", I64),
            ("ws_order_number", I64),
            ("ws_quantity", I64),
            ("ws_wholesale_cost", Dec(7, 2)),
            ("ws_list_price", Dec(7, 2)),
            ("ws_sales_price", Dec(7, 2)),
            ("ws_ext_discount_amt", Dec(7, 2)),
            ("ws_ext_sales_price", Dec(7, 2)),
            ("ws_ext_wholesale_cost", Dec(7, 2)),
            ("ws_ext_list_price", Dec(7, 2)),
            ("ws_ext_tax", Dec(7, 2)),
            ("ws_coupon_amt", Dec(7, 2)),
            ("ws_ext_ship_cost", Dec(7, 2)),
            ("ws_net_paid", Dec(7, 2)),
            ("ws_net_paid_inc_tax", Dec(7, 2)),
            ("ws_net_paid_inc_ship", Dec(7, 2)),
            ("ws_net_paid_inc_ship_tax", Dec(7, 2)),
            ("ws_net_profit", Dec(7, 2)),
        ],
    ),
    (
        "web_site",
        &[
            ("web_site_sk", I64),
            ("web_site_id", Str),
            ("web_rec_start_date", Date),
            ("web_rec_end_date", Date),
            ("web_name", Str),
            ("web_open_date_sk", I64),
            ("web_close_date_sk", I64),
            ("web_class", Str),
            ("web_manager", Str),
            ("web_mkt_id", I64),
            ("web_mkt_class", Str),
            ("web_mkt_desc", Str),
            ("web_market_manager", Str),
            ("web_company_id", I64),
            ("web_company_name", Str),
            ("web_street_number", Str),
            ("web_street_name", Str),
            ("web_street_type", Str),
            ("web_suite_number", Str),
            ("web_city", Str),
            ("web_county", Str),
            ("web_state", Str),
            ("web_zip", Str),
            ("web_country", Str),
            ("web_gmt_offset", Dec(5, 2)),
            ("web_tax_percentage", Dec(5, 2)),
        ],
    ),
];

/// The TPC-DS tables the fixture holds.
pub const TPCDS_TABLES: &[&str] = &[
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

/// The Arrow schema of every table, as the fixture writes it.
#[must_use]
pub fn table_schemas() -> Vec<Schema> {
    SCHEMAS
        .iter()
        .map(|(_, columns)| {
            Schema::new(
                columns
                    .iter()
                    .map(|(column, ty)| Field::new(*column, arrow_type(*ty), true))
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

/// Rows per written batch.
const BATCH_ROWS: usize = 65_536;

/// Generate the TPC-DS fixture into `out_dir` unless this generator already
/// finished writing one there — the same reuse rule as the TPC-H and SSB
/// fixtures.
pub fn ensure_tpcds_fixture(out_dir: &Path, scale_factor: f64) {
    let revision = super::generator_revision(include_str!("tpcds_data.rs"));
    if super::fixture_is_current(out_dir, &revision) {
        return;
    }
    let _ = std::fs::remove_dir_all(out_dir);
    std::fs::create_dir_all(out_dir).expect("tpcds out dir");
    write_tpcds_parquet(out_dir, scale_factor);
    super::mark_fixture_complete(out_dir, &revision);
}

fn write_tpcds_parquet(out_dir: &Path, scale_factor: f64) {
    let session = SessionBuilder::new()
        .with_scale_factor(scale_factor)
        .with_compat_mode(CompatMode::C)
        .build()
        .expect("tpcdsgen session");
    // One pass per generator. A sales generator also yields that channel's
    // returns, as `dsdgen` does, so the returns tables come out of it.
    let passes: Vec<(Box<dyn RowGenerator>, Table, &[&str])> = vec![
        (
            Box::new(CallCenterRowGenerator::new()),
            Table::CallCenter,
            &["call_center"],
        ),
        (
            Box::new(CatalogPageRowGenerator::new()),
            Table::CatalogPage,
            &["catalog_page"],
        ),
        (
            Box::new(CatalogSalesRowGenerator::new()),
            Table::CatalogSales,
            &["catalog_sales", "catalog_returns"],
        ),
        (
            Box::new(CustomerRowGenerator::new()),
            Table::Customer,
            &["customer"],
        ),
        (
            Box::new(CustomerAddressRowGenerator::new()),
            Table::CustomerAddress,
            &["customer_address"],
        ),
        (
            Box::new(CustomerDemographicsRowGenerator::new()),
            Table::CustomerDemographics,
            &["customer_demographics"],
        ),
        (
            Box::new(DateDimRowGenerator::new()),
            Table::DateDim,
            &["date_dim"],
        ),
        (
            Box::new(HouseholdDemographicsRowGenerator::new()),
            Table::HouseholdDemographics,
            &["household_demographics"],
        ),
        (
            Box::new(IncomeBandRowGenerator::new()),
            Table::IncomeBand,
            &["income_band"],
        ),
        (
            Box::new(InventoryRowGenerator::new()),
            Table::Inventory,
            &["inventory"],
        ),
        (Box::new(ItemRowGenerator::new()), Table::Item, &["item"]),
        (
            Box::new(PromotionRowGenerator::new()),
            Table::Promotion,
            &["promotion"],
        ),
        (
            Box::new(ReasonRowGenerator::new()),
            Table::Reason,
            &["reason"],
        ),
        (
            Box::new(ShipModeRowGenerator::new()),
            Table::ShipMode,
            &["ship_mode"],
        ),
        (Box::new(StoreRowGenerator::new()), Table::Store, &["store"]),
        (
            Box::new(StoreSalesRowGenerator::new()),
            Table::StoreSales,
            &["store_sales", "store_returns"],
        ),
        (
            Box::new(TimeDimRowGenerator::new()),
            Table::TimeDim,
            &["time_dim"],
        ),
        (
            Box::new(WarehouseRowGenerator::new()),
            Table::Warehouse,
            &["warehouse"],
        ),
        (
            Box::new(WebPageRowGenerator::new()),
            Table::WebPage,
            &["web_page"],
        ),
        (
            Box::new(WebSalesRowGenerator::new()),
            Table::WebSales,
            &["web_sales", "web_returns"],
        ),
        (
            Box::new(WebSiteRowGenerator::new()),
            Table::WebSite,
            &["web_site"],
        ),
    ];
    for (mut generator, table, outputs) in passes {
        let mut writers: Vec<TableWriter> = outputs
            .iter()
            .map(|name| TableWriter::create(out_dir, name))
            .collect();
        let row_count = session.get_scaling().get_row_count(table);
        let mut row_number = 1;
        while row_number <= row_count {
            let result = generator
                .generate_row_and_child_rows(row_number, &session, None, None)
                .unwrap_or_else(|e| panic!("tpcdsgen {table:?} row {row_number}: {e}"));
            for row in result.get_rows() {
                let name = table_of(row);
                let writer = writers
                    .iter_mut()
                    .find(|writer| writer.name == name)
                    .unwrap_or_else(|| panic!("tpcdsgen {table:?} yielded a {name} row"));
                writer.push(&row.get_values());
            }
            if result.should_end_row() {
                generator.consume_remaining_seeds_for_row();
                row_number += 1;
            }
        }
        for writer in writers {
            writer.finish();
        }
    }
}

/// The table a generated row belongs to.
fn table_of(row: &GeneratedRow) -> &'static str {
    match row {
        GeneratedRow::CallCenter(_) => "call_center",
        GeneratedRow::CatalogPage(_) => "catalog_page",
        GeneratedRow::CatalogReturns(_) => "catalog_returns",
        GeneratedRow::CatalogSales(_) => "catalog_sales",
        GeneratedRow::Customer(_) => "customer",
        GeneratedRow::CustomerAddress(_) => "customer_address",
        GeneratedRow::CustomerDemographics(_) => "customer_demographics",
        GeneratedRow::DateDim(_) => "date_dim",
        GeneratedRow::DbgenVersion(_) => "dbgen_version",
        GeneratedRow::HouseholdDemographics(_) => "household_demographics",
        GeneratedRow::IncomeBand(_) => "income_band",
        GeneratedRow::Inventory(_) => "inventory",
        GeneratedRow::Item(_) => "item",
        GeneratedRow::Promotion(_) => "promotion",
        GeneratedRow::Reason(_) => "reason",
        GeneratedRow::ShipMode(_) => "ship_mode",
        GeneratedRow::Store(_) => "store",
        GeneratedRow::StoreReturns(_) => "store_returns",
        GeneratedRow::StoreSales(_) => "store_sales",
        GeneratedRow::TimeDim(_) => "time_dim",
        GeneratedRow::Warehouse(_) => "warehouse",
        GeneratedRow::WebPage(_) => "web_page",
        GeneratedRow::WebReturns(_) => "web_returns",
        GeneratedRow::WebSales(_) => "web_sales",
        GeneratedRow::WebSite(_) => "web_site",
    }
}

/// Streams one table's text rows into a parquet file, typed per [`SCHEMAS`].
struct TableWriter {
    name: &'static str,
    columns: &'static [(&'static str, Ty)],
    schema: SchemaRef,
    builders: Vec<Box<dyn ArrayBuilder>>,
    rows: usize,
    writer: ArrowWriter<File>,
}

impl TableWriter {
    fn create(out_dir: &Path, name: &'static str) -> Self {
        let (_, columns) = SCHEMAS
            .iter()
            .find(|(table, _)| *table == name)
            .unwrap_or_else(|| panic!("no TPC-DS schema for {name}"));
        let schema: SchemaRef = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(column, ty)| Field::new(*column, arrow_type(*ty), true))
                .collect::<Vec<_>>(),
        ));
        let path = out_dir.join(format!("{name}.parquet"));
        let file = File::create(&path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("parquet writer");
        Self {
            name,
            columns,
            schema,
            builders: columns.iter().map(|(_, ty)| builder(*ty)).collect(),
            rows: 0,
            writer,
        }
    }

    fn push(&mut self, values: &[String]) {
        assert_eq!(
            values.len(),
            self.columns.len(),
            "tpcdsgen wrote {} fields for a {}-column {} row: {values:?}",
            values.len(),
            self.columns.len(),
            self.name
        );
        for ((column, ty), (builder, value)) in self
            .columns
            .iter()
            .zip(self.builders.iter_mut().zip(values))
        {
            append(builder.as_mut(), *ty, value)
                .unwrap_or_else(|e| panic!("TPC-DS {}.{column} value {value:?}: {e}", self.name));
        }
        self.rows += 1;
        if self.rows == BATCH_ROWS {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.rows == 0 {
            return;
        }
        let columns: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(arrow::array::ArrayBuilder::finish)
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns).expect("tpcds batch");
        self.writer.write(&batch).expect("write tpcds batch");
        self.rows = 0;
    }

    fn finish(mut self) {
        self.flush();
        self.writer.close().expect("close tpcds parquet");
    }
}

fn arrow_type(ty: Ty) -> DataType {
    match ty {
        I64 => DataType::Int64,
        I32 => DataType::Int32,
        Dec(precision, scale) => DataType::Decimal128(precision, scale),
        Date => DataType::Date32,
        Str => DataType::Utf8,
    }
}

fn builder(ty: Ty) -> Box<dyn ArrayBuilder> {
    match ty {
        I64 => Box::new(Int64Builder::new()),
        I32 => Box::new(Int32Builder::new()),
        Dec(precision, scale) => Box::new(
            Decimal128Builder::new()
                .with_precision_and_scale(precision, scale)
                .expect("decimal type"),
        ),
        Date => Box::new(Date32Builder::new()),
        Str => Box::new(StringBuilder::new()),
    }
}

/// Append one text field; `dsdgen` writes NULL as an empty field.
fn append(builder: &mut dyn ArrayBuilder, ty: Ty, value: &str) -> Result<(), String> {
    let any = builder.as_any_mut();
    macro_rules! typed {
        ($b:ty) => {
            any.downcast_mut::<$b>().ok_or("builder type")?
        };
    }
    let null = value.is_empty();
    match ty {
        I64 => {
            let b = typed!(Int64Builder);
            if null {
                b.append_null();
            } else {
                b.append_value(value.parse().map_err(|e| format!("{e}"))?);
            }
        }
        I32 => {
            let b = typed!(Int32Builder);
            if null {
                b.append_null();
            } else {
                b.append_value(value.parse().map_err(|e| format!("{e}"))?);
            }
        }
        Dec(_, scale) => {
            let b = typed!(Decimal128Builder);
            if null {
                b.append_null();
            } else {
                b.append_value(parse_decimal(value, scale)?);
            }
        }
        Date => {
            let b = typed!(Date32Builder);
            if null {
                b.append_null();
            } else {
                let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                    .map_err(|e| format!("{e}"))?;
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("epoch")?;
                let days = i32::try_from((date - epoch).num_days()).map_err(|e| format!("{e}"))?;
                b.append_value(days);
            }
        }
        Str => {
            let b = typed!(StringBuilder);
            if null {
                b.append_null();
            } else {
                b.append_value(value);
            }
        }
    }
    Ok(())
}

/// `123.45` as the unscaled `12345` of a scale-2 decimal. More fractional
/// digits than the column's scale would need rounding, so they fail instead.
fn parse_decimal(value: &str, scale: i8) -> Result<i128, String> {
    let scale = usize::try_from(scale).map_err(|e| format!("{e}"))?;
    let (sign, digits) = match value.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, value),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if fraction.len() > scale {
        return Err(format!("more than {scale} fractional digits"));
    }
    let unscaled: i128 = format!("{whole}{fraction:0<scale$}")
        .parse()
        .map_err(|e| format!("{e}"))?;
    Ok(sign * unscaled)
}
