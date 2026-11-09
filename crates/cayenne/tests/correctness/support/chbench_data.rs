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

//! CH-benCHmark (TPC-C + TPC-H hybrid) fixtures, generated in-process.
//!
//! CH-benCHmark analytical queries reference TPC-C tables (`order_line`,
//! `oorder`, `customer`, `stock`, `item`, …) plus TPC-H `nation` / `supplier`.
//! There is no standard TPC-C generator to borrow, so this builds a
//! deterministic synthetic warehouse per scale unit — the full item catalog,
//! scaled-down customers and orders — as Arrow batches, with no engine in the
//! path, and every lane loads both of its sides from the parquet it writes.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Tables required by the CH-benCHmark query set in this repo.
pub const CHBENCH_TABLES: &[&str] = &[
    "warehouse",
    "district",
    "customer",
    "history",
    "new_order",
    "oorder",
    "order_line",
    "stock",
    "item",
    "nation",
    "supplier",
    "region",
];

/// Generate the CH-benCHmark fixture into `out_dir` unless this generator
/// already finished writing one there — the reuse rule of the TPC-H, TPC-DS and
/// SSB fixtures.
pub fn ensure_chbench_fixture(out_dir: &Path, warehouses: i64) {
    let revision = super::generator_revision(include_str!("chbench_data.rs"));
    if super::fixture_is_current(out_dir, &revision) {
        return;
    }
    let _ = std::fs::remove_dir_all(out_dir);
    std::fs::create_dir_all(out_dir).expect("chbench out dir");
    for (name, batch) in chbench_batches(warehouses) {
        super::write_parquet(&batch, &out_dir.join(format!("{name}.parquet")));
    }
    super::mark_fixture_complete(out_dir, &revision);
}

/// Every CH-benCHmark table as an Arrow batch.
///
/// Built in-process rather than by an engine so that every lane — including
/// chDB's, whose process cannot drive DuckDB — compares Cayenne on the same
/// rows. `warehouses` is the TPC-C scale; SF1 is one warehouse.
#[must_use]
pub fn chbench_batches(warehouses: i64) -> Vec<(&'static str, RecordBatch)> {
    let w = warehouses.max(1);
    let warehouse_ids: Vec<i64> = (1..=w).collect();
    // (w_id, d_id) for every district.
    let districts: Vec<(i64, i64)> = warehouse_ids
        .iter()
        .flat_map(|&w_id| (1..=DISTRICTS).map(move |d_id| (w_id, d_id)))
        .collect();
    // (w_id, d_id, c_id) for every customer.
    let customers: Vec<(i64, i64, i64)> = districts
        .iter()
        .flat_map(|&(w_id, d_id)| (1..=CUSTOMERS_PER_DISTRICT).map(move |c| (w_id, d_id, c)))
        .collect();
    // (w_id, d_id, o_id) for every order.
    let orders: Vec<(i64, i64, i64)> = districts
        .iter()
        .flat_map(|&(w_id, d_id)| (1..=ORDERS_PER_DISTRICT).map(move |o| (w_id, d_id, o)))
        .collect();

    vec![
        ("region", region_batch()),
        ("nation", nation_batch()),
        ("supplier", supplier_batch()),
        ("warehouse", warehouse_batch(&warehouse_ids)),
        ("district", district_batch(&districts)),
        ("customer", customer_batch(&customers)),
        ("item", item_batch()),
        ("stock", stock_batch(&warehouse_ids)),
        ("oorder", oorder_batch(&orders)),
        ("new_order", new_order_batch(&orders)),
        ("order_line", order_line_batch(&orders)),
        ("history", history_batch(&customers)),
    ]
}

const DISTRICTS: i64 = 10;
const CUSTOMERS_PER_DISTRICT: i64 = 300;
/// Orders go to the first 250 customers of a district; the rest have none, which
/// is what CH-benCHmark Q22's `NOT EXISTS` over `oorder` looks for.
const CUSTOMERS_WITH_ORDERS: i64 = 250;
const ITEMS: i64 = 10_000;
const ORDERS_PER_DISTRICT: i64 = 300;
const LINES_PER_ORDER: i64 = 5;
const SUPPLIERS: i64 = 10_000;

/// An order below 90% of a district's orders has been delivered and carries a
/// carrier; the rest are the new orders.
fn carrier_of(o_id: i64) -> Option<i64> {
    (o_id * 10 < ORDERS_PER_DISTRICT * 9).then_some(1 + o_id % 10)
}

/// An order's entry time: a day in 2007 through 2012, spread so the queries'
/// date bounds (Q20's `2010-05-23`) and per-year groupings split the orders.
fn entry_micros(o_id: i64) -> i64 {
    const JAN_2_2007: i64 = 1_167_696_000_000_000;
    JAN_2_2007 + (o_id * 37 % 2000) * DAY_MICROS
}

/// The 25 TPC-H nations with their TPC-H regions, which the queries filter on
/// by name (`'CHINA'`, `'JAPAN'`, `'INDIA'`; regions `'EUROPE'`, `'ASIA'`).
const NATIONS: [(&str, i64); 25] = [
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
];

const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

/// A customer's state. The queries read the nation of a customer as
/// `ascii(substr(c_state, 1, 1)) - 65`, so the first letter walks all 25.
fn customer_state(c_id: i64) -> String {
    let letter = char::from(b'A' + u8::try_from(c_id % 25).expect("state letter"));
    format!("{letter}{letter}")
}

const JAN_1_2007_MICROS: i64 = 1_167_609_600_000_000;
const DAY_MICROS: i64 = 86_400_000_000;

fn col_i64(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}

fn col_i32(name: &str) -> Field {
    Field::new(name, DataType::Int32, true)
}

fn col_str(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn col_dec(name: &str, precision: u8, scale: i8) -> Field {
    Field::new(name, DataType::Decimal128(precision, scale), true)
}

fn col_ts(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true)
}

fn i64s(values: impl IntoIterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(values))
}

fn i32s(value: i32, rows: usize) -> ArrayRef {
    Arc::new(Int32Array::from(vec![value; rows]))
}

fn strs(values: impl IntoIterator<Item = String>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn same_str(value: &str, rows: usize) -> ArrayRef {
    Arc::new(StringArray::from(vec![value; rows]))
}

/// A decimal column of unscaled values.
fn decs(values: impl IntoIterator<Item = i128>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from_iter_values(values)
            .with_precision_and_scale(precision, scale)
            .expect("chbench decimal"),
    )
}

fn timestamps(values: impl IntoIterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from_iter(values))
}

fn table(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("chbench batch")
}

fn region_batch() -> RecordBatch {
    table(
        vec![
            col_i64("r_regionkey"),
            col_str("r_name"),
            col_str("r_comment"),
        ],
        vec![
            i64s(0..5),
            strs(REGIONS.iter().map(ToString::to_string)),
            same_str("comment", 5),
        ],
    )
}

fn nation_batch() -> RecordBatch {
    table(
        vec![
            col_i64("n_nationkey"),
            col_str("n_name"),
            col_i64("n_regionkey"),
            col_str("n_comment"),
        ],
        vec![
            i64s(0..25),
            strs(NATIONS.iter().map(|(name, _)| (*name).to_string())),
            i64s(NATIONS.iter().map(|(_, region)| *region)),
            same_str("comment", 25),
        ],
    )
}

fn supplier_batch() -> RecordBatch {
    let keys = 0..SUPPLIERS;
    let rows = keys.clone().count();
    table(
        vec![
            col_i64("su_suppkey"),
            col_str("su_name"),
            col_str("su_address"),
            col_i64("su_nationkey"),
            col_str("su_phone"),
            col_dec("su_acctbal", 22, 2),
            col_str("su_comment"),
        ],
        vec![
            i64s(keys.clone()),
            strs(keys.clone().map(|i| format!("Supplier#{i}"))),
            same_str("addr", rows),
            i64s(keys.clone().map(|i| i % 25)),
            same_str("12-345-678-9011", rows),
            // `(i % 100) * 0.01`
            decs(keys.clone().map(|i| i128::from(i % 100)), 22, 2),
            strs(keys.map(|i| {
                (if i % 17 == 0 {
                    "bad supplier note"
                } else {
                    "ok"
                })
                .to_string()
            })),
        ],
    )
}

fn warehouse_batch(warehouse_ids: &[i64]) -> RecordBatch {
    let rows = warehouse_ids.len();
    table(
        vec![
            col_i64("w_id"),
            col_str("w_name"),
            col_str("w_street_1"),
            col_str("w_street_2"),
            col_str("w_city"),
            col_str("w_state"),
            col_str("w_zip"),
            col_dec("w_tax", 2, 1),
            col_dec("w_ytd", 7, 1),
        ],
        vec![
            i64s(warehouse_ids.iter().copied()),
            strs(warehouse_ids.iter().map(|w| format!("W{w}"))),
            same_str("st1", rows),
            same_str("st2", rows),
            same_str("city", rows),
            same_str("ST", rows),
            same_str("12345", rows),
            decs(std::iter::repeat_n(1, rows), 2, 1),
            decs(std::iter::repeat_n(3_000_000, rows), 7, 1),
        ],
    )
}

fn district_batch(districts: &[(i64, i64)]) -> RecordBatch {
    let rows = districts.len();
    table(
        vec![
            col_i64("d_w_id"),
            col_i64("d_id"),
            col_str("d_name"),
            col_str("d_street_1"),
            col_str("d_street_2"),
            col_str("d_city"),
            col_str("d_state"),
            col_str("d_zip"),
            col_dec("d_tax", 2, 1),
            col_dec("d_ytd", 6, 1),
            col_i32("d_next_o_id"),
        ],
        vec![
            i64s(districts.iter().map(|d| d.0)),
            i64s(districts.iter().map(|d| d.1)),
            strs(districts.iter().map(|d| format!("D{}", d.1))),
            same_str("st1", rows),
            same_str("st2", rows),
            same_str("city", rows),
            same_str("ST", rows),
            same_str("12345", rows),
            decs(std::iter::repeat_n(1, rows), 2, 1),
            decs(std::iter::repeat_n(300_000, rows), 6, 1),
            i32s(
                i32::try_from(ORDERS_PER_DISTRICT + 1).expect("d_next_o_id"),
                rows,
            ),
        ],
    )
}

fn customer_batch(customers: &[(i64, i64, i64)]) -> RecordBatch {
    let rows = customers.len();
    table(
        vec![
            col_i64("c_w_id"),
            col_i64("c_d_id"),
            col_i64("c_id"),
            col_str("c_last"),
            col_str("c_middle"),
            col_str("c_first"),
            col_str("c_street_1"),
            col_str("c_street_2"),
            col_str("c_city"),
            col_str("c_state"),
            col_str("c_zip"),
            col_str("c_phone"),
            col_ts("c_since"),
            col_str("c_credit"),
            col_dec("c_credit_lim", 6, 1),
            col_dec("c_discount", 2, 1),
            col_dec("c_balance", 3, 1),
            col_dec("c_ytd_payment", 3, 1),
            col_i32("c_payment_cnt"),
            col_i32("c_delivery_cnt"),
            col_str("c_data"),
        ],
        vec![
            i64s(customers.iter().map(|c| c.0)),
            i64s(customers.iter().map(|c| c.1)),
            i64s(customers.iter().map(|c| c.2)),
            strs(customers.iter().map(|c| format!("last{}", c.2 % 1000))),
            same_str("OE", rows),
            strs(customers.iter().map(|c| format!("first{}", c.2))),
            same_str("st1", rows),
            same_str("st2", rows),
            strs(customers.iter().map(|c| format!("city{}", c.2 % 50))),
            strs(customers.iter().map(|c| customer_state(c.2))),
            same_str("12345", rows),
            // Q22 keeps the phones starting 1 through 7.
            strs(
                customers
                    .iter()
                    .map(|c| format!("{}2-345-678-9012", 1 + c.2 % 9)),
            ),
            // `TIMESTAMP '2007-01-01' + (c % 1000) hours`
            timestamps(
                customers
                    .iter()
                    .map(|c| Some(JAN_1_2007_MICROS + (c.2 % 1000) * 3_600_000_000)),
            ),
            same_str("GC", rows),
            decs(std::iter::repeat_n(500_000, rows), 6, 1),
            decs(std::iter::repeat_n(1, rows), 2, 1),
            // Balances from 0.0 to 49.0, so Q22's `c_balance > avg(c_balance)`
            // splits them.
            decs(customers.iter().map(|c| i128::from(c.2 % 50) * 10), 3, 1),
            decs(std::iter::repeat_n(100, rows), 3, 1),
            i32s(1, rows),
            i32s(0, rows),
            same_str("data", rows),
        ],
    )
}

fn item_batch() -> RecordBatch {
    let ids = 1..=ITEMS;
    let rows = ids.clone().count();
    table(
        vec![
            col_i64("i_id"),
            col_i32("i_im_id"),
            col_str("i_name"),
            col_dec("i_price", 22, 1),
            col_str("i_data"),
        ],
        vec![
            i64s(ids.clone()),
            i32s(1, rows),
            strs(ids.clone().map(|i| format!("item{i}"))),
            // `(i % 100) * 0.5 + 1.0`, in tenths.
            decs(ids.clone().map(|i| i128::from(i % 100) * 5 + 10), 22, 1),
            // Every pattern a query matches `i_data` against: `PR%` (Q14),
            // `zz%` (Q16), `co%` (Q20), `%BB` (Q9), and `%a`, `%b`, `%c`.
            strs(ids.map(|i| {
                if i % 10 == 0 {
                    "PRoriginal".to_string()
                } else if i % 7 == 0 {
                    "zz".to_string()
                } else if i % 11 == 0 {
                    "coBB".to_string()
                } else {
                    let letter = char::from(b'a' + u8::try_from(i % 26).expect("letter"));
                    format!("data{letter}")
                }
            })),
        ],
    )
}

fn stock_batch(warehouse_ids: &[i64]) -> RecordBatch {
    let stock: Vec<(i64, i64)> = warehouse_ids
        .iter()
        .flat_map(|&w| (1..=ITEMS).map(move |i| (w, i)))
        .collect();
    let rows = stock.len();
    let mut fields = vec![col_i64("s_w_id"), col_i64("s_i_id"), col_i32("s_quantity")];
    let mut columns = vec![
        i64s(stock.iter().map(|s| s.0)),
        i64s(stock.iter().map(|s| s.1)),
        Arc::new(Int32Array::from_iter_values(
            stock
                .iter()
                .map(|s| 10 + i32::try_from(s.1 % 91).expect("s_quantity")),
        )) as ArrayRef,
    ];
    for district in 1..=10 {
        fields.push(col_str(&format!("s_dist_{district:02}")));
        columns.push(same_str(&format!("dist{district:02}"), rows));
    }
    fields.extend([
        col_dec("s_ytd", 2, 1),
        col_i64("s_order_cnt"),
        col_i32("s_remote_cnt"),
        col_str("s_data"),
    ]);
    columns.extend([
        decs(std::iter::repeat_n(0, rows), 2, 1),
        // One item in a hundred sells far above the rest, so Q11's `HAVING
        // sum(s_order_cnt) > 0.005 * total` keeps some.
        i64s(
            stock
                .iter()
                .map(|s| if s.1 % 100 == 43 { 10_000 } else { s.1 % 50 }),
        ),
        i32s(0, rows),
        same_str("stockdata", rows),
    ]);
    table(fields, columns)
}

fn oorder_batch(orders: &[(i64, i64, i64)]) -> RecordBatch {
    let rows = orders.len();
    table(
        vec![
            col_i64("o_w_id"),
            col_i64("o_d_id"),
            col_i64("o_id"),
            col_i64("o_c_id"),
            col_ts("o_entry_d"),
            col_i64("o_carrier_id"),
            col_i32("o_ol_cnt"),
            col_i32("o_all_local"),
        ],
        vec![
            i64s(orders.iter().map(|o| o.0)),
            i64s(orders.iter().map(|o| o.1)),
            i64s(orders.iter().map(|o| o.2)),
            // Mixed with the district so a customer's nation does not follow the
            // order's lines' supplier nations: Q5 and Q7 join the two.
            i64s(
                orders
                    .iter()
                    .map(|o| 1 + (o.2 * 163 + o.1 * 59) % CUSTOMERS_WITH_ORDERS),
            ),
            timestamps(orders.iter().map(|o| Some(entry_micros(o.2)))),
            Arc::new(
                orders
                    .iter()
                    .map(|o| carrier_of(o.2))
                    .collect::<Int64Array>(),
            ),
            i32s(i32::try_from(LINES_PER_ORDER).expect("o_ol_cnt"), rows),
            i32s(1, rows),
        ],
    )
}

fn new_order_batch(orders: &[(i64, i64, i64)]) -> RecordBatch {
    let undelivered: Vec<&(i64, i64, i64)> = orders
        .iter()
        .filter(|o| carrier_of(o.2).is_none())
        .collect();
    table(
        vec![col_i64("no_w_id"), col_i64("no_d_id"), col_i64("no_o_id")],
        vec![
            i64s(undelivered.iter().map(|o| o.0)),
            i64s(undelivered.iter().map(|o| o.1)),
            i64s(undelivered.iter().map(|o| o.2)),
        ],
    )
}

fn order_line_batch(orders: &[(i64, i64, i64)]) -> RecordBatch {
    let lines: Vec<(&(i64, i64, i64), i64)> = orders
        .iter()
        .flat_map(|o| (1..=LINES_PER_ORDER).map(move |line| (o, line)))
        .collect();
    let rows = lines.len();
    table(
        vec![
            col_i64("ol_w_id"),
            col_i64("ol_d_id"),
            col_i64("ol_o_id"),
            col_i64("ol_number"),
            col_i64("ol_i_id"),
            col_i64("ol_supply_w_id"),
            col_ts("ol_delivery_d"),
            col_i32("ol_quantity"),
            col_dec("ol_amount", 21, 1),
            col_str("ol_dist_info"),
        ],
        vec![
            i64s(lines.iter().map(|(o, _)| o.0)),
            i64s(lines.iter().map(|(o, _)| o.1)),
            i64s(lines.iter().map(|(o, _)| o.2)),
            i64s(lines.iter().map(|(_, line)| *line)),
            // Spread across the catalog — by district too, since order numbers
            // repeat in every district — so an item's lines come from different
            // orders (Q17 compares a line with its item's average) and no filter
            // lines up with a line number (Q21 reads each order's last line).
            i64s(
                lines
                    .iter()
                    .map(|(o, line)| 1 + (o.2 * 131 + line * 997 + o.1 * 7919) % ITEMS),
            ),
            i64s(lines.iter().map(|(o, _)| o.0)),
            // A delivered order's lines arrive on five different days, and which
            // line is last varies by order, so Q21's `NOT EXISTS` a later line keeps
            // one line per order and those lines reach every supplier nation.
            timestamps(lines.iter().map(|(o, line)| {
                carrier_of(o.2).map(|_| entry_micros(o.2) + (1 + (o.2 + line) % 5) * DAY_MICROS)
            })),
            Arc::new(Int32Array::from_iter_values(lines.iter().map(
                |(o, line)| 1 + i32::try_from((o.2 * line + o.1) % 10).expect("ol_quantity"),
            ))) as ArrayRef,
            // `(1 + (o_id % 100)) * 1.0`, in tenths.
            decs(
                lines.iter().map(|(o, _)| i128::from(1 + o.2 % 100) * 10),
                21,
                1,
            ),
            same_str("dist", rows),
        ],
    )
}

fn history_batch(customers: &[(i64, i64, i64)]) -> RecordBatch {
    let rows = customers.len().min(5000);
    let customers = &customers[..rows];
    table(
        vec![
            col_i64("h_id"),
            col_i64("h_c_id"),
            col_i64("h_c_d_id"),
            col_i64("h_c_w_id"),
            col_i64("h_d_id"),
            col_i64("h_w_id"),
            col_ts("h_date"),
            col_dec("h_amount", 3, 1),
            col_str("h_data"),
        ],
        vec![
            i64s(1..=i64::try_from(rows).expect("history rows")),
            i64s(customers.iter().map(|c| c.2)),
            i64s(customers.iter().map(|c| c.1)),
            i64s(customers.iter().map(|c| c.0)),
            i64s(customers.iter().map(|c| c.1)),
            i64s(customers.iter().map(|c| c.0)),
            timestamps(std::iter::repeat_n(Some(JAN_1_2007_MICROS), rows)),
            decs(std::iter::repeat_n(100, rows), 3, 1),
            same_str("hist", rows),
        ],
    )
}

/// Rewrite CH-benCH SQL for DataFusion: `mod(a, b)` → `(a % b)`.
#[must_use]
pub fn chbench_sql_for_datafusion(sql: &str) -> String {
    // Simple token rewrite: mod(x, y) appears with nested arithmetic in CH-benCH.
    // Use a conservative approach: replace "mod(" with temporary and parse pairs.
    let mut out = String::with_capacity(sql.len() + 16);
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 4 <= bytes.len()
            && bytes[i].eq_ignore_ascii_case(&b'm')
            && bytes[i + 1].eq_ignore_ascii_case(&b'o')
            && bytes[i + 2].eq_ignore_ascii_case(&b'd')
            && bytes[i + 3] == b'('
        {
            // Find matching close paren for mod( ... )
            let mut depth = 1usize;
            let mut j = i + 4;
            let start_args = j;
            while j < bytes.len() && depth > 0 {
                match bytes[j] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
                j += 1;
            }
            let args = &sql[start_args..j - 1];
            // Split on top-level comma.
            let mut comma = None;
            let mut d = 0i32;
            for (k, ch) in args.char_indices() {
                match ch {
                    '(' => d += 1,
                    ')' => d -= 1,
                    ',' if d == 0 => {
                        comma = Some(k);
                        break;
                    }
                    _ => {}
                }
            }
            if let Some(c) = comma {
                let left = args[..c].trim();
                let right = args[c + 1..].trim();
                out.push('(');
                out.push_str(left);
                out.push_str(" % ");
                out.push_str(right);
                out.push(')');
            } else {
                out.push_str(&sql[i..j]);
            }
            i = j;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}
