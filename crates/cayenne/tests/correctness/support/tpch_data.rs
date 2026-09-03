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

//! TPC-H fixtures, generated in-process by `tpchgen`.
//!
//! The lane used to build these by asking DuckDB to `INSTALL tpch; CALL dbgen(…)`,
//! which fetches an extension over the network. That made fixture generation — and
//! so the whole comparison — depend on a reachable extension repository, which a
//! correctness gate should not. `tpchgen` is the pure-Rust generator the
//! `DataFusion` benchmarks use; it has no dependencies of its own, so it runs
//! offline and deterministically.
//!
//! The schemas below reproduce what `dbgen` emitted, column for column, so the
//! swap changes where the rows come from and nothing about what the engines are
//! asked to compare. Two conversions are load-bearing and easy to get wrong:
//!
//! - `l_quantity` is an `i64` count in `tpchgen` and `DECIMAL(15,2)` out of
//!   `dbgen`, so it is scaled rather than widened.
//! - `TPCHDecimal` already carries hundredths in an `i64`, so it maps to
//!   `Decimal128(15, 2)` by value, with no floating point in between.
//!
//! `tpchgen` is not linked into the runtime; it is a dev-dependency of this test
//! tree only.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

/// TPC-H tables, in the order the suite registers them.
pub const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// `dbgen` emits every money column as `DECIMAL(15, 2)`.
const MONEY_PRECISION: u8 = 15;
const MONEY_SCALE: i8 = 2;

fn money(values: Vec<i128>) -> ArrayRef {
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(MONEY_PRECISION, MONEY_SCALE)
        .expect("tpch decimal precision/scale");
    Arc::new(array)
}

fn strings(values: Vec<String>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("tpch batch")
}

fn money_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Decimal128(MONEY_PRECISION, MONEY_SCALE),
        false,
    )
}

fn utf8(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn i64_field(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}

fn i32_field(name: &str) -> Field {
    Field::new(name, DataType::Int32, false)
}

fn date(name: &str) -> Field {
    Field::new(name, DataType::Date32, false)
}

/// Generate every TPC-H table at `scale_factor` and write it as parquet under
/// `out_dir`, matching the layout the lane already loads from.
pub fn write_tpch_parquet(out_dir: &Path, scale_factor: f64) {
    std::fs::create_dir_all(out_dir).expect("tpch out dir");
    for (name, batch) in tpch_batches(scale_factor) {
        super::write_parquet(&batch, &out_dir.join(format!("{name}.parquet")));
    }
}

/// Every TPC-H table as an Arrow batch. Deterministic for a given scale factor.
#[must_use]
pub fn tpch_batches(scale_factor: f64) -> Vec<(&'static str, RecordBatch)> {
    vec![
        ("nation", nation_batch()),
        ("region", region_batch()),
        ("part", part_batch(scale_factor)),
        ("supplier", supplier_batch(scale_factor)),
        ("partsupp", partsupp_batch(scale_factor)),
        ("customer", customer_batch(scale_factor)),
        ("orders", orders_batch(scale_factor)),
        ("lineitem", lineitem_batch(scale_factor)),
    ]
}

fn nation_batch() -> RecordBatch {
    let (mut key, mut name, mut regionkey, mut comment) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for n in NationGenerator::default() {
        key.push(i32::try_from(n.n_nationkey).expect("n_nationkey fits i32"));
        name.push(n.n_name.to_string());
        regionkey.push(i32::try_from(n.n_regionkey).expect("n_regionkey fits i32"));
        comment.push(n.n_comment.to_string());
    }
    batch(
        vec![
            i32_field("n_nationkey"),
            utf8("n_name"),
            i32_field("n_regionkey"),
            utf8("n_comment"),
        ],
        vec![
            Arc::new(Int32Array::from(key)),
            strings(name),
            Arc::new(Int32Array::from(regionkey)),
            strings(comment),
        ],
    )
}

fn region_batch() -> RecordBatch {
    let (mut key, mut name, mut comment) = (Vec::new(), Vec::new(), Vec::new());
    for r in RegionGenerator::default() {
        key.push(i32::try_from(r.r_regionkey).expect("r_regionkey fits i32"));
        name.push(r.r_name.to_string());
        comment.push(r.r_comment.to_string());
    }
    batch(
        vec![i32_field("r_regionkey"), utf8("r_name"), utf8("r_comment")],
        vec![
            Arc::new(Int32Array::from(key)),
            strings(name),
            strings(comment),
        ],
    )
}

fn part_batch(sf: f64) -> RecordBatch {
    let (mut key, mut name, mut mfgr, mut brand, mut ty, mut size, mut container) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let (mut retail, mut comment) = (Vec::new(), Vec::new());
    for p in PartGenerator::new(sf, 1, 1) {
        key.push(p.p_partkey);
        name.push(p.p_name.to_string());
        mfgr.push(p.p_mfgr.to_string());
        brand.push(p.p_brand.to_string());
        ty.push(p.p_type.to_string());
        size.push(p.p_size);
        container.push(p.p_container.to_string());
        retail.push(i128::from(p.p_retailprice.into_inner()));
        comment.push(p.p_comment.to_string());
    }
    batch(
        vec![
            i64_field("p_partkey"),
            utf8("p_name"),
            utf8("p_mfgr"),
            utf8("p_brand"),
            utf8("p_type"),
            i32_field("p_size"),
            utf8("p_container"),
            money_field("p_retailprice"),
            utf8("p_comment"),
        ],
        vec![
            Arc::new(Int64Array::from(key)),
            strings(name),
            strings(mfgr),
            strings(brand),
            strings(ty),
            Arc::new(Int32Array::from(size)),
            strings(container),
            money(retail),
            strings(comment),
        ],
    )
}

fn supplier_batch(sf: f64) -> RecordBatch {
    let (mut key, mut name, mut address, mut nationkey) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut phone, mut acctbal, mut comment) = (Vec::new(), Vec::new(), Vec::new());
    for s in SupplierGenerator::new(sf, 1, 1) {
        key.push(s.s_suppkey);
        name.push(s.s_name.to_string());
        address.push(s.s_address.to_string());
        nationkey.push(i32::try_from(s.s_nationkey).expect("s_nationkey fits i32"));
        phone.push(s.s_phone.to_string());
        acctbal.push(i128::from(s.s_acctbal.into_inner()));
        comment.push(s.s_comment.clone());
    }
    batch(
        vec![
            i64_field("s_suppkey"),
            utf8("s_name"),
            utf8("s_address"),
            i32_field("s_nationkey"),
            utf8("s_phone"),
            money_field("s_acctbal"),
            utf8("s_comment"),
        ],
        vec![
            Arc::new(Int64Array::from(key)),
            strings(name),
            strings(address),
            Arc::new(Int32Array::from(nationkey)),
            strings(phone),
            money(acctbal),
            strings(comment),
        ],
    )
}

fn partsupp_batch(sf: f64) -> RecordBatch {
    let (mut partkey, mut suppkey, mut availqty, mut cost, mut comment) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for ps in PartSuppGenerator::new(sf, 1, 1) {
        partkey.push(ps.ps_partkey);
        suppkey.push(ps.ps_suppkey);
        availqty.push(i64::from(ps.ps_availqty));
        cost.push(i128::from(ps.ps_supplycost.into_inner()));
        comment.push(ps.ps_comment.to_string());
    }
    batch(
        vec![
            i64_field("ps_partkey"),
            i64_field("ps_suppkey"),
            i64_field("ps_availqty"),
            money_field("ps_supplycost"),
            utf8("ps_comment"),
        ],
        vec![
            Arc::new(Int64Array::from(partkey)),
            Arc::new(Int64Array::from(suppkey)),
            Arc::new(Int64Array::from(availqty)),
            money(cost),
            strings(comment),
        ],
    )
}

fn customer_batch(sf: f64) -> RecordBatch {
    let (mut key, mut name, mut address, mut nationkey) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut phone, mut acctbal, mut segment, mut comment) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in CustomerGenerator::new(sf, 1, 1) {
        key.push(c.c_custkey);
        name.push(c.c_name.to_string());
        address.push(c.c_address.to_string());
        nationkey.push(i32::try_from(c.c_nationkey).expect("c_nationkey fits i32"));
        phone.push(c.c_phone.to_string());
        acctbal.push(i128::from(c.c_acctbal.into_inner()));
        segment.push(c.c_mktsegment.to_string());
        comment.push(c.c_comment.to_string());
    }
    batch(
        vec![
            i64_field("c_custkey"),
            utf8("c_name"),
            utf8("c_address"),
            i32_field("c_nationkey"),
            utf8("c_phone"),
            money_field("c_acctbal"),
            utf8("c_mktsegment"),
            utf8("c_comment"),
        ],
        vec![
            Arc::new(Int64Array::from(key)),
            strings(name),
            strings(address),
            Arc::new(Int32Array::from(nationkey)),
            strings(phone),
            money(acctbal),
            strings(segment),
            strings(comment),
        ],
    )
}

fn orders_batch(sf: f64) -> RecordBatch {
    let (mut key, mut custkey, mut status, mut total, mut orderdate) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut priority, mut clerk, mut shippriority, mut comment) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for o in OrderGenerator::new(sf, 1, 1) {
        key.push(o.o_orderkey);
        custkey.push(o.o_custkey);
        status.push(o.o_orderstatus.to_string());
        total.push(i128::from(o.o_totalprice.into_inner()));
        orderdate.push(o.o_orderdate.to_unix_epoch());
        priority.push(o.o_orderpriority.to_string());
        clerk.push(o.o_clerk.to_string());
        shippriority.push(o.o_shippriority);
        comment.push(o.o_comment.to_string());
    }
    batch(
        vec![
            i64_field("o_orderkey"),
            i64_field("o_custkey"),
            utf8("o_orderstatus"),
            money_field("o_totalprice"),
            date("o_orderdate"),
            utf8("o_orderpriority"),
            utf8("o_clerk"),
            i32_field("o_shippriority"),
            utf8("o_comment"),
        ],
        vec![
            Arc::new(Int64Array::from(key)),
            Arc::new(Int64Array::from(custkey)),
            strings(status),
            money(total),
            Arc::new(Date32Array::from(orderdate)),
            strings(priority),
            strings(clerk),
            Arc::new(Int32Array::from(shippriority)),
            strings(comment),
        ],
    )
}

fn lineitem_batch(sf: f64) -> RecordBatch {
    let (mut orderkey, mut partkey, mut suppkey, mut linenumber) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut quantity, mut extended, mut discount, mut tax) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut returnflag, mut linestatus) = (Vec::new(), Vec::new());
    let (mut shipdate, mut commitdate, mut receiptdate) = (Vec::new(), Vec::new(), Vec::new());
    let (mut shipinstruct, mut shipmode, mut comment) = (Vec::new(), Vec::new(), Vec::new());

    for l in LineItemGenerator::new(sf, 1, 1) {
        orderkey.push(l.l_orderkey);
        partkey.push(l.l_partkey);
        suppkey.push(l.l_suppkey);
        linenumber.push(i64::from(l.l_linenumber));
        // A plain count here, but `DECIMAL(15,2)` in the schema the queries were
        // written against — scale it rather than widening it, or every
        // `sum(l_quantity)` comes back a hundred times too small.
        quantity.push(i128::from(l.l_quantity) * 100);
        extended.push(i128::from(l.l_extendedprice.into_inner()));
        discount.push(i128::from(l.l_discount.into_inner()));
        tax.push(i128::from(l.l_tax.into_inner()));
        returnflag.push(l.l_returnflag.to_string());
        linestatus.push(l.l_linestatus.to_string());
        shipdate.push(l.l_shipdate.to_unix_epoch());
        commitdate.push(l.l_commitdate.to_unix_epoch());
        receiptdate.push(l.l_receiptdate.to_unix_epoch());
        shipinstruct.push(l.l_shipinstruct.to_string());
        shipmode.push(l.l_shipmode.to_string());
        comment.push(l.l_comment.to_string());
    }

    batch(
        vec![
            i64_field("l_orderkey"),
            i64_field("l_partkey"),
            i64_field("l_suppkey"),
            i64_field("l_linenumber"),
            money_field("l_quantity"),
            money_field("l_extendedprice"),
            money_field("l_discount"),
            money_field("l_tax"),
            utf8("l_returnflag"),
            utf8("l_linestatus"),
            date("l_shipdate"),
            date("l_commitdate"),
            date("l_receiptdate"),
            utf8("l_shipinstruct"),
            utf8("l_shipmode"),
            utf8("l_comment"),
        ],
        vec![
            Arc::new(Int64Array::from(orderkey)),
            Arc::new(Int64Array::from(partkey)),
            Arc::new(Int64Array::from(suppkey)),
            Arc::new(Int64Array::from(linenumber)),
            money(quantity),
            money(extended),
            money(discount),
            money(tax),
            strings(returnflag),
            strings(linestatus),
            Arc::new(Date32Array::from(shipdate)),
            Arc::new(Date32Array::from(commitdate)),
            Arc::new(Date32Array::from(receiptdate)),
            strings(shipinstruct),
            strings(shipmode),
            strings(comment),
        ],
    )
}
