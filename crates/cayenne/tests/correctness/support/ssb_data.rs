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

//! Star Schema Benchmark (SSB) schema, deterministic data, and the classic 13 queries.
//!
//! Data is generated in pure Arrow (no DuckDB) so both the DuckDB and SQLite
//! correctness gates share identical parquet. Scale is reduced relative to
//! classic SF1 (~6M lineorders) but preserves join cardinality shapes; override
//! with `CAYENNE_PARITY_SSB_SCALE` (integer multiplier, default 1).

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use test_framework::queries::Query;

/// SSB base tables (fact + four dimensions).
pub const SSB_TABLES: &[&str] = &["lineorder", "customer", "supplier", "part", "date"];

/// Write all SSB tables as `{table}.parquet` under `out_dir`.
pub fn write_ssb_parquet(out_dir: &Path, scale: i64) {
    std::fs::create_dir_all(out_dir).expect("ssb out dir");
    let scale = scale.max(1);
    for (name, batch) in ssb_batches(scale) {
        let path = out_dir.join(format!("{name}.parquet"));
        super::write_parquet(&batch, &path);
    }
}

/// Classic SSB queries Q1.1 … Q4.3 (O'Neil et al.), portable SQL for
/// DataFusion / DuckDB / SQLite (comma joins + standard aggregates).
#[must_use]
pub fn ssb_queries() -> Vec<Query> {
    vec![
        Query::new(
            "ssb_q1_1".into(),
            "SELECT SUM(lo_extendedprice * lo_discount) AS revenue \
             FROM lineorder, date \
             WHERE lo_orderdate = d_datekey \
               AND d_year = 1993 \
               AND lo_discount BETWEEN 1 AND 3 \
               AND lo_quantity < 25"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q1_2".into(),
            "SELECT SUM(lo_extendedprice * lo_discount) AS revenue \
             FROM lineorder, date \
             WHERE lo_orderdate = d_datekey \
               AND d_yearmonthnum = 199401 \
               AND lo_discount BETWEEN 4 AND 6 \
               AND lo_quantity BETWEEN 26 AND 35"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q1_3".into(),
            "SELECT SUM(lo_extendedprice * lo_discount) AS revenue \
             FROM lineorder, date \
             WHERE lo_orderdate = d_datekey \
               AND d_weeknuminyear = 6 \
               AND d_year = 1994 \
               AND lo_discount BETWEEN 5 AND 7 \
               AND lo_quantity BETWEEN 26 AND 35"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q2_1".into(),
            "SELECT SUM(lo_revenue) AS revenue, d_year, p_brand1 \
             FROM lineorder, date, part, supplier \
             WHERE lo_orderdate = d_datekey \
               AND lo_partkey = p_partkey \
               AND lo_suppkey = s_suppkey \
               AND p_category = 'MFGR#12' \
               AND s_region = 'AMERICA' \
             GROUP BY d_year, p_brand1 \
             ORDER BY d_year, p_brand1"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q2_2".into(),
            "SELECT SUM(lo_revenue) AS revenue, d_year, p_brand1 \
             FROM lineorder, date, part, supplier \
             WHERE lo_orderdate = d_datekey \
               AND lo_partkey = p_partkey \
               AND lo_suppkey = s_suppkey \
               AND p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228' \
               AND s_region = 'ASIA' \
             GROUP BY d_year, p_brand1 \
             ORDER BY d_year, p_brand1"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q2_3".into(),
            "SELECT SUM(lo_revenue) AS revenue, d_year, p_brand1 \
             FROM lineorder, date, part, supplier \
             WHERE lo_orderdate = d_datekey \
               AND lo_partkey = p_partkey \
               AND lo_suppkey = s_suppkey \
               AND p_brand1 = 'MFGR#2221' \
               AND s_region = 'EUROPE' \
             GROUP BY d_year, p_brand1 \
             ORDER BY d_year, p_brand1"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q3_1".into(),
            "SELECT c_nation, s_nation, d_year, SUM(lo_revenue) AS revenue \
             FROM customer, lineorder, supplier, date \
             WHERE lo_custkey = c_custkey \
               AND lo_suppkey = s_suppkey \
               AND lo_orderdate = d_datekey \
               AND c_region = 'ASIA' \
               AND s_region = 'ASIA' \
               AND d_year >= 1992 AND d_year <= 1997 \
             GROUP BY c_nation, s_nation, d_year \
             ORDER BY d_year ASC, revenue DESC"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q3_2".into(),
            "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue \
             FROM customer, lineorder, supplier, date \
             WHERE lo_custkey = c_custkey \
               AND lo_suppkey = s_suppkey \
               AND lo_orderdate = d_datekey \
               AND c_nation = 'UNITED KI1' \
               AND s_nation = 'UNITED KI1' \
               AND d_year >= 1992 AND d_year <= 1997 \
             GROUP BY c_city, s_city, d_year \
             ORDER BY d_year ASC, revenue DESC"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q3_3".into(),
            "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue \
             FROM customer, lineorder, supplier, date \
             WHERE lo_custkey = c_custkey \
               AND lo_suppkey = s_suppkey \
               AND lo_orderdate = d_datekey \
               AND (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5') \
               AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5') \
               AND d_year >= 1992 AND d_year <= 1997 \
             GROUP BY c_city, s_city, d_year \
             ORDER BY d_year ASC, revenue DESC"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q3_4".into(),
            "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue \
             FROM customer, lineorder, supplier, date \
             WHERE lo_custkey = c_custkey \
               AND lo_suppkey = s_suppkey \
               AND lo_orderdate = d_datekey \
               AND (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5') \
               AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5') \
               AND d_yearmonth = 'Dec1997' \
             GROUP BY c_city, s_city, d_year \
             ORDER BY d_year ASC, revenue DESC"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q4_1".into(),
            "SELECT d_year, c_nation, SUM(lo_revenue - lo_supplycost) AS profit \
             FROM date, customer, supplier, part, lineorder \
             WHERE lo_custkey = c_custkey \
               AND lo_suppkey = s_suppkey \
               AND lo_partkey = p_partkey \
               AND lo_orderdate = d_datekey \
               AND c_region = 'AMERICA' \
               AND s_region = 'AMERICA' \
               AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') \
             GROUP BY d_year, c_nation \
             ORDER BY d_year, c_nation"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q4_2".into(),
            "SELECT d_year, s_nation, p_category, SUM(lo_revenue - lo_supplycost) AS profit \
             FROM date, customer, supplier, part, lineorder \
             WHERE lo_custkey = c_custkey \
               AND lo_suppkey = s_suppkey \
               AND lo_partkey = p_partkey \
               AND lo_orderdate = d_datekey \
               AND c_region = 'AMERICA' \
               AND s_region = 'AMERICA' \
               AND (d_year = 1997 OR d_year = 1998) \
               AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') \
             GROUP BY d_year, s_nation, p_category \
             ORDER BY d_year, s_nation, p_category"
                .into(),
            false,
        ),
        Query::new(
            "ssb_q4_3".into(),
            "SELECT d_year, s_city, p_brand1, SUM(lo_revenue - lo_supplycost) AS profit \
             FROM date, customer, supplier, part, lineorder \
             WHERE lo_custkey = c_custkey \
               AND lo_suppkey = s_suppkey \
               AND lo_partkey = p_partkey \
               AND lo_orderdate = d_datekey \
               AND c_region = 'AMERICA' \
               AND s_nation = 'UNITED ST' \
               AND (d_year = 1997 OR d_year = 1998) \
               AND p_category = 'MFGR#14' \
             GROUP BY d_year, s_city, p_brand1 \
             ORDER BY d_year, s_city, p_brand1"
                .into(),
            false,
        ),
    ]
}

/// Deterministic SSB-scale batches. `scale=1` is the correctness default.
#[must_use]
pub fn ssb_batches(scale: i64) -> Vec<(&'static str, RecordBatch)> {
    let scale = scale.max(1) as usize;
    let n_customers = 1_000 * scale;
    let n_suppliers = 100 * scale;
    let n_parts = 1_000 * scale;
    // ~7 years of dates keeps year filters meaningful without SF1 bulk.
    let n_dates = 2_557; // 1992-01-01 .. 1998-12-31
    let n_lineorders = 10_000 * scale;

    vec![
        ("date", make_date_batch(n_dates)),
        ("customer", make_customer_batch(n_customers)),
        ("supplier", make_supplier_batch(n_suppliers)),
        ("part", make_part_batch(n_parts)),
        (
            "lineorder",
            make_lineorder_batch(n_lineorders, n_customers, n_suppliers, n_parts, n_dates),
        ),
    ]
}

const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
const NATIONS_PER_REGION: [[&str; 5]; 5] = [
    ["ALGERIA", "ETHIOPIA", "KENYA", "MOROCCO", "MOZAMBIQ"],
    ["ARGENTINA", "BRAZIL", "CANADA", "PERU", "UNITED ST"],
    ["INDIA", "INDONESIA", "JAPAN", "CHINA", "VIETNAM"],
    ["FRANCE", "GERMANY", "ROMANIA", "RUSSIA", "UNITED KI"],
    ["EGYPT", "IRAN", "IRAQ", "JORDAN", "SAUDI ARA"],
];

fn make_date_batch(n: usize) -> RecordBatch {
    // 1992-01-01 as YYYYMMDD = 19920101
    let schema = Arc::new(Schema::new(vec![
        Field::new("d_datekey", DataType::Int32, false),
        Field::new("d_date", DataType::Utf8, false),
        Field::new("d_dayofweek", DataType::Utf8, false),
        Field::new("d_month", DataType::Utf8, false),
        Field::new("d_year", DataType::Int32, false),
        Field::new("d_yearmonthnum", DataType::Int32, false),
        Field::new("d_yearmonth", DataType::Utf8, false),
        Field::new("d_daynuminweek", DataType::Int32, false),
        Field::new("d_daynuminmonth", DataType::Int32, false),
        Field::new("d_daynuminyear", DataType::Int32, false),
        Field::new("d_monthnuminyear", DataType::Int32, false),
        Field::new("d_weeknuminyear", DataType::Int32, false),
        Field::new("d_sellingseason", DataType::Utf8, false),
        Field::new("d_lastdayinweekfl", DataType::Int32, false),
        Field::new("d_lastdayinmonthfl", DataType::Int32, false),
        Field::new("d_holidayfl", DataType::Int32, false),
        Field::new("d_weekdayfl", DataType::Int32, false),
    ]));

    let months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let month_abbr = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let dow_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ];

    let mut datekey = Vec::with_capacity(n);
    let mut date_s = Vec::with_capacity(n);
    let mut dayofweek = Vec::with_capacity(n);
    let mut month = Vec::with_capacity(n);
    let mut year = Vec::with_capacity(n);
    let mut yearmonthnum = Vec::with_capacity(n);
    let mut yearmonth = Vec::with_capacity(n);
    let mut daynuminweek = Vec::with_capacity(n);
    let mut daynuminmonth = Vec::with_capacity(n);
    let mut daynuminyear = Vec::with_capacity(n);
    let mut monthnuminyear = Vec::with_capacity(n);
    let mut weeknuminyear = Vec::with_capacity(n);
    let mut sellingseason = Vec::with_capacity(n);
    let mut lastdayinweekfl = Vec::with_capacity(n);
    let mut lastdayinmonthfl = Vec::with_capacity(n);
    let mut holidayfl = Vec::with_capacity(n);
    let mut weekdayfl = Vec::with_capacity(n);

    let mut y = 1992i32;
    let mut m = 0usize; // 0-based
    let mut d = 1i32;
    let mut doy = 1i32;
    // 1992-01-01 was a Wednesday → index 2
    let mut dow = 2i32;

    for _ in 0..n {
        let dim =
            days_in_month[m] + i32::from(m == 1 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
        let key = y * 10_000 + (m as i32 + 1) * 100 + d;
        datekey.push(key);
        date_s.push(format!("{y:04}-{:02}-{:02}", m + 1, d));
        dayofweek.push(dow_names[dow as usize].to_string());
        month.push(months[m].to_string());
        year.push(y);
        yearmonthnum.push(y * 100 + m as i32 + 1);
        yearmonth.push(format!("{}{y}", month_abbr[m]));
        daynuminweek.push(dow + 1);
        daynuminmonth.push(d);
        daynuminyear.push(doy);
        monthnuminyear.push(m as i32 + 1);
        weeknuminyear.push(((doy - 1) / 7) + 1);
        sellingseason.push(
            if m < 3 {
                "Winter"
            } else if m < 6 {
                "Spring"
            } else if m < 9 {
                "Summer"
            } else {
                "Fall"
            }
            .to_string(),
        );
        lastdayinweekfl.push(i32::from(dow == 6));
        lastdayinmonthfl.push(i32::from(d == dim));
        holidayfl.push(0);
        weekdayfl.push(i32::from(dow < 5));

        // advance
        d += 1;
        doy += 1;
        dow = (dow + 1) % 7;
        if d > dim {
            d = 1;
            m += 1;
            if m == 12 {
                m = 0;
                y += 1;
                doy = 1;
            }
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(datekey)),
            Arc::new(StringArray::from(date_s)),
            Arc::new(StringArray::from(dayofweek)),
            Arc::new(StringArray::from(month)),
            Arc::new(Int32Array::from(year)),
            Arc::new(Int32Array::from(yearmonthnum)),
            Arc::new(StringArray::from(yearmonth)),
            Arc::new(Int32Array::from(daynuminweek)),
            Arc::new(Int32Array::from(daynuminmonth)),
            Arc::new(Int32Array::from(daynuminyear)),
            Arc::new(Int32Array::from(monthnuminyear)),
            Arc::new(Int32Array::from(weeknuminyear)),
            Arc::new(StringArray::from(sellingseason)),
            Arc::new(Int32Array::from(lastdayinweekfl)),
            Arc::new(Int32Array::from(lastdayinmonthfl)),
            Arc::new(Int32Array::from(holidayfl)),
            Arc::new(Int32Array::from(weekdayfl)),
        ],
    )
    .expect("date batch")
}

fn nation_for(key: usize) -> (String, String, String) {
    let region_i = key % REGIONS.len();
    let nation_i = (key / REGIONS.len()) % 5;
    let region = REGIONS[region_i].to_string();
    let base_nation = NATIONS_PER_REGION[region_i][nation_i];
    // Classic SSB city is nation + digit (e.g. UNITED KI1). Nations that are
    // truncated to 9 chars (UNITED KI / UNITED ST) keep that form; only city
    // appends the digit so filters like c_nation='UNITED KI1' and
    // c_city='UNITED KI1' both hit for UK, while US stays c_nation='UNITED ST'.
    let city = format!("{base_nation}{}", (key % 10) + 1);
    let nation = if base_nation == "UNITED KI" {
        // Q3.2 filters c_nation = 'UNITED KI1' (nation with digit in classic SSB).
        format!("UNITED KI{}", (key % 2) + 1)
    } else {
        base_nation.to_string()
    };
    (city, nation, region)
}

fn make_customer_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_city", DataType::Utf8, false),
        Field::new("c_nation", DataType::Utf8, false),
        Field::new("c_region", DataType::Utf8, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
    ]));
    let segs = [
        "AUTOMOBILE",
        "BUILDING",
        "FURNITURE",
        "HOUSEHOLD",
        "MACHINERY",
    ];
    let mut keys = Vec::with_capacity(n);
    let mut names = Vec::with_capacity(n);
    let mut addrs = Vec::with_capacity(n);
    let mut cities = Vec::with_capacity(n);
    let mut nations = Vec::with_capacity(n);
    let mut regions = Vec::with_capacity(n);
    let mut phones = Vec::with_capacity(n);
    let mut mkt = Vec::with_capacity(n);
    for i in 1..=n {
        let (city, nation, region) = nation_for(i);
        keys.push(i as i32);
        names.push(format!("Customer#{i:09}"));
        addrs.push(format!("addr {i}"));
        cities.push(city);
        nations.push(nation);
        regions.push(region);
        phones.push(format!(
            "10-{:03}-{:03}-{:04}",
            i % 1000,
            i % 1000,
            i % 10000
        ));
        mkt.push(segs[i % segs.len()].to_string());
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(addrs)),
            Arc::new(StringArray::from(cities)),
            Arc::new(StringArray::from(nations)),
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(phones)),
            Arc::new(StringArray::from(mkt)),
        ],
    )
    .expect("customer batch")
}

fn make_supplier_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int32, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_city", DataType::Utf8, false),
        Field::new("s_nation", DataType::Utf8, false),
        Field::new("s_region", DataType::Utf8, false),
        Field::new("s_phone", DataType::Utf8, false),
    ]));
    let mut keys = Vec::with_capacity(n);
    let mut names = Vec::with_capacity(n);
    let mut addrs = Vec::with_capacity(n);
    let mut cities = Vec::with_capacity(n);
    let mut nations = Vec::with_capacity(n);
    let mut regions = Vec::with_capacity(n);
    let mut phones = Vec::with_capacity(n);
    for i in 1..=n {
        let (city, nation, region) = nation_for(i * 3);
        keys.push(i as i32);
        names.push(format!("Supplier#{i:09}"));
        addrs.push(format!("saddr {i}"));
        cities.push(city);
        nations.push(nation);
        regions.push(region);
        phones.push(format!(
            "20-{:03}-{:03}-{:04}",
            i % 1000,
            i % 1000,
            i % 10000
        ));
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(addrs)),
            Arc::new(StringArray::from(cities)),
            Arc::new(StringArray::from(nations)),
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(phones)),
        ],
    )
    .expect("supplier batch")
}

fn make_part_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int32, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_category", DataType::Utf8, false),
        Field::new("p_brand1", DataType::Utf8, false),
        Field::new("p_color", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
    ]));
    let colors = ["almond", "antique", "aquamarine", "azure", "beige"];
    let types = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
    let containers = ["SM CASE", "SM BOX", "SM PACK", "SM PKG", "MED BAG"];
    let mut keys = Vec::with_capacity(n);
    let mut names = Vec::with_capacity(n);
    let mut mfgrs = Vec::with_capacity(n);
    let mut cats = Vec::with_capacity(n);
    let mut brands = Vec::with_capacity(n);
    let mut color = Vec::with_capacity(n);
    let mut ptype = Vec::with_capacity(n);
    let mut size = Vec::with_capacity(n);
    let mut container = Vec::with_capacity(n);
    for i in 1..=n {
        // Classic SSB naming: p_mfgr=MFGR#1..5, p_category=MFGR#XY (X=mfgr, Y=1..5
        // independent of mfgr), p_brand1=MFGR#XYZZ so filters like MFGR#12 /
        // MFGR#2221..MFGR#2228 / MFGR#14 hit.
        let mfgr_n = ((i - 1) % 5) + 1;
        let cat_digit = (((i - 1) / 5) % 5) + 1;
        let brand_suffix = (((i - 1) / 25) % 40) + 1; // 01..40
        keys.push(i as i32);
        names.push(format!("part {i}"));
        mfgrs.push(format!("MFGR#{mfgr_n}"));
        cats.push(format!("MFGR#{mfgr_n}{cat_digit}"));
        brands.push(format!("MFGR#{mfgr_n}{cat_digit}{brand_suffix:02}"));
        color.push(colors[i % colors.len()].to_string());
        ptype.push(types[i % types.len()].to_string());
        size.push(((i % 50) + 1) as i32);
        container.push(containers[i % containers.len()].to_string());
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(mfgrs)),
            Arc::new(StringArray::from(cats)),
            Arc::new(StringArray::from(brands)),
            Arc::new(StringArray::from(color)),
            Arc::new(StringArray::from(ptype)),
            Arc::new(Int32Array::from(size)),
            Arc::new(StringArray::from(container)),
        ],
    )
    .expect("part batch")
}

fn make_lineorder_batch(
    n: usize,
    n_customers: usize,
    n_suppliers: usize,
    n_parts: usize,
    n_dates: usize,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lo_orderkey", DataType::Int32, false),
        Field::new("lo_linenumber", DataType::Int32, false),
        Field::new("lo_custkey", DataType::Int32, false),
        Field::new("lo_partkey", DataType::Int32, false),
        Field::new("lo_suppkey", DataType::Int32, false),
        Field::new("lo_orderdate", DataType::Int32, false),
        Field::new("lo_orderpriority", DataType::Utf8, false),
        Field::new("lo_shippriority", DataType::Utf8, false),
        Field::new("lo_quantity", DataType::Int32, false),
        Field::new("lo_extendedprice", DataType::Int64, false),
        Field::new("lo_ordtotalprice", DataType::Int64, false),
        Field::new("lo_discount", DataType::Int32, false),
        Field::new("lo_revenue", DataType::Int64, false),
        Field::new("lo_supplycost", DataType::Int64, false),
        Field::new("lo_tax", DataType::Int32, false),
        Field::new("lo_commitdate", DataType::Int32, false),
        Field::new("lo_shipmode", DataType::Utf8, false),
    ]));

    // Precompute date keys for 1992-01-01 .. covering n_dates days.
    let mut date_keys = Vec::with_capacity(n_dates);
    {
        let mut y = 1992i32;
        let mut m = 1i32;
        let mut d = 1i32;
        let dim = |y: i32, m: i32| -> i32 {
            match m {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                2 if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) => 29,
                2 => 28,
                _ => 30,
            }
        };
        for _ in 0..n_dates {
            date_keys.push(y * 10_000 + m * 100 + d);
            d += 1;
            if d > dim(y, m) {
                d = 1;
                m += 1;
                if m > 12 {
                    m = 1;
                    y += 1;
                }
            }
        }
    }

    let prios = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
    let ships = ["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"];

    let mut orderkey = Vec::with_capacity(n);
    let mut linenumber = Vec::with_capacity(n);
    let mut custkey = Vec::with_capacity(n);
    let mut partkey = Vec::with_capacity(n);
    let mut suppkey = Vec::with_capacity(n);
    let mut orderdate = Vec::with_capacity(n);
    let mut orderpriority = Vec::with_capacity(n);
    let mut shippriority = Vec::with_capacity(n);
    let mut quantity = Vec::with_capacity(n);
    let mut extendedprice = Vec::with_capacity(n);
    let mut ordtotalprice = Vec::with_capacity(n);
    let mut discount = Vec::with_capacity(n);
    let mut revenue = Vec::with_capacity(n);
    let mut supplycost = Vec::with_capacity(n);
    let mut tax = Vec::with_capacity(n);
    let mut commitdate = Vec::with_capacity(n);
    let mut shipmode = Vec::with_capacity(n);

    // Dimension FKs aligned to classic SSB filters (see `nation_for` / part naming).
    // customer key k → nation_for(k); supplier key s → nation_for(s*3).
    // part 6=MFGR#12; 507=MFGR#2221; 16=MFGR#14; 1=MFGR#1; 2=MFGR#2.
    // supp 2,7: AMERICA (s*3 %5 ==1); 4,9: ASIA; 1,6: EUROPE; 16: UNITED KI;
    // supp 7: UNITED ST (AMERICA).
    let seed_specs: &[(i32, i32, i32, usize)] = &[
        // (custkey, partkey, suppkey, date_idx)
        (1, 6, 2, 400), // Q2.1: MFGR#12 × AMERICA ~1993
        (6, 6, 7, 401),
        (11, 6, 12, 402),
        (2, 507, 4, 500), // Q2.2: MFGR#2221 × ASIA
        (7, 507, 9, 501),
        (12, 507, 14, 502),
        (3, 507, 1, 800), // Q2.3: MFGR#2221 × EUROPE
        (8, 507, 6, 801),
        (23, 6, 16, 1_000), // Q3.2/3.3: UNITED KI × UNITED KI
        (48, 6, 16, 1_001),
        (23, 6, 16, 2_165), // Q3.4: UK × UK × Dec1997
        (48, 6, 16, 2_166),
        (2, 6, 4, 900), // Q3.1: ASIA × ASIA
        (7, 6, 9, 901),
        (1, 1, 2, 1_900), // Q4.1/4.2: AMERICA × AMERICA × MFGR#1 ~1997
        (6, 2, 7, 1_901), // MFGR#2
        (1, 1, 2, 2_200), // ~1998
        (6, 2, 7, 2_201),
        (1, 16, 7, 1_900), // Q4.3: AMERICA × UNITED ST × MFGR#14 ~1997
        (6, 16, 7, 2_200), // ~1998
    ];

    for i in 0..n {
        let ok = (i / 4 + 1) as i32;
        let ln = (i % 4 + 1) as i32;
        // Repeat seed patterns so filter paths stay dense under modular fill.
        let (ck, pk, sk, di) = if i < seed_specs.len() * 50 {
            let (c, p, s, d) = seed_specs[i % seed_specs.len()];
            (
                c.min(n_customers as i32).max(1),
                p.min(n_parts as i32).max(1),
                s.min(n_suppliers as i32).max(1),
                d.min(n_dates - 1),
            )
        } else {
            (
                ((i * 17) % n_customers + 1) as i32,
                ((i * 31) % n_parts + 1) as i32,
                ((i * 13) % n_suppliers + 1) as i32,
                (i * 7) % n_dates,
            )
        };
        let od = date_keys[di];
        let qty = ((i % 50) + 1) as i32;
        let price = 100i64 + ((i as i64 * 37) % 900);
        let ext = price * i64::from(qty);
        let disc = (i % 11) as i32; // 0..10
        let rev = ext * (100 - i64::from(disc)) / 100;
        let cost = ext * 80 / 100;
        let t = (i % 9) as i32;
        let cd = date_keys[(di + 30).min(n_dates - 1)];

        orderkey.push(ok);
        linenumber.push(ln);
        custkey.push(ck);
        partkey.push(pk);
        suppkey.push(sk);
        orderdate.push(od);
        orderpriority.push(prios[i % prios.len()].to_string());
        shippriority.push("0".to_string());
        quantity.push(qty);
        extendedprice.push(ext);
        ordtotalprice.push(ext * 4);
        discount.push(disc);
        revenue.push(rev);
        supplycost.push(cost);
        tax.push(t);
        commitdate.push(cd);
        shipmode.push(ships[i % ships.len()].to_string());
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(orderkey)),
            Arc::new(Int32Array::from(linenumber)),
            Arc::new(Int32Array::from(custkey)),
            Arc::new(Int32Array::from(partkey)),
            Arc::new(Int32Array::from(suppkey)),
            Arc::new(Int32Array::from(orderdate)),
            Arc::new(StringArray::from(orderpriority)),
            Arc::new(StringArray::from(shippriority)),
            Arc::new(Int32Array::from(quantity)),
            Arc::new(Int64Array::from(extendedprice)),
            Arc::new(Int64Array::from(ordtotalprice)),
            Arc::new(Int32Array::from(discount)),
            Arc::new(Int64Array::from(revenue)),
            Arc::new(Int64Array::from(supplycost)),
            Arc::new(Int32Array::from(tax)),
            Arc::new(Int32Array::from(commitdate)),
            Arc::new(StringArray::from(shipmode)),
        ],
    )
    .expect("lineorder batch")
}
