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

use std::collections::{BTreeSet, HashMap};

use spicepod::{
    acceleration::{Acceleration, IndexType, Mode},
    component::dataset::{Dataset, TimeFormat},
};

use super::{Layout, LayoutFeature, apply_layout, benchmark_tables};
use crate::queries::QuerySet;

fn accelerated(name: &str, engine: &str, mode: Mode) -> Dataset {
    let mut dataset = Dataset::new(format!("file:data/{name}.parquet"), name.to_string());
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode,
        ..Acceleration::default()
    });
    dataset
}

fn params(dataset: &Dataset) -> HashMap<String, String> {
    dataset
        .acceleration
        .as_ref()
        .and_then(|acceleration| acceleration.params.as_ref())
        .map(spicepod::param::Params::as_string_map)
        .unwrap_or_default()
}

fn layout(s: &str) -> Layout {
    s.parse().expect("valid layout")
}

fn tpch() -> &'static [super::TableKeys] {
    benchmark_tables(&QuerySet::Tpch).expect("TPC-H has layout keys")
}

#[test]
fn parses_feature_names_and_refuses_unknown_or_repeated_ones() {
    assert_eq!(
        layout("primary_key, indexes,sort")
            .features()
            .collect::<Vec<_>>(),
        vec![
            LayoutFeature::PrimaryKey,
            LayoutFeature::Indexes,
            LayoutFeature::Sort
        ]
    );
    assert_eq!(layout("sort,primary_key").to_string(), "primary_key,sort");
    assert_eq!(
        "primary_key,pk"
            .parse::<Layout>()
            .expect_err("unknown feature")
            .to_string(),
        "unknown layout feature 'pk'; expected one of: primary_key, indexes, sort, cluster, time_column, partition"
    );
    assert_eq!(
        "sort,sort"
            .parse::<Layout>()
            .expect_err("repeated feature")
            .to_string(),
        "layout 'sort,sort' names 'sort' more than once"
    );
}

/// The settings a layout writes are the Spicepod a user would write for the
/// same table, so each is pinned exactly.
#[test]
fn configures_every_feature_on_a_file_backed_cayenne_table() {
    let mut datasets = vec![accelerated("lineitem", "cayenne", Mode::File)];
    let applied = apply_layout(
        &mut datasets,
        tpch(),
        &layout("primary_key,indexes,sort,time_column,partition"),
    )
    .expect("every feature applies to a file-backed Cayenne table");

    let lineitem = &datasets[0];
    let acceleration = lineitem.acceleration.as_ref().expect("still accelerated");
    assert_eq!(
        acceleration.primary_key.as_deref(),
        Some("(l_orderkey, l_linenumber)")
    );
    assert_eq!(
        acceleration
            .indexes
            .iter()
            .map(|(columns, index)| (columns.as_str(), index.to_string()))
            .collect::<BTreeSet<_>>(),
        [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_shipdate",
            "(l_partkey, l_suppkey)"
        ]
        .into_iter()
        .map(|columns| (columns, IndexType::Enabled.to_string()))
        .collect::<BTreeSet<_>>()
    );
    assert_eq!(
        params(lineitem),
        HashMap::from([(
            "cayenne_sort_columns".to_string(),
            "l_shipdate, l_orderkey".to_string()
        )])
    );
    assert_eq!(lineitem.time_column.as_deref(), Some("l_shipdate"));
    assert_eq!(lineitem.time_format, Some(TimeFormat::Date));
    assert_eq!(
        acceleration
            .partition_by
            .iter()
            .map(|partition| partition.expression.as_str())
            .collect::<Vec<_>>(),
        vec!["date_part('year', l_shipdate)"]
    );
    assert_eq!(
        applied.iter().map(ToString::to_string).collect::<Vec<_>>(),
        vec![
            "lineitem: time_column=l_shipdate (Date); primary_key=(l_orderkey, l_linenumber); indexes=[l_orderkey, l_partkey, l_suppkey, l_shipdate, (l_partkey, l_suppkey)]; cayenne_sort_columns=l_shipdate, l_orderkey; partition_by=date_part('year', l_shipdate)"
        ]
    );
}

#[test]
fn spells_the_sort_order_the_way_each_engine_reads_it() {
    for (engine, param, value) in [
        ("cayenne", "cayenne_sort_columns", "o_orderdate, o_orderkey"),
        (
            "duckdb",
            "on_refresh_sort_columns",
            "o_orderdate ASC, o_orderkey ASC",
        ),
        (
            "arrow",
            "arrow_sort_columns",
            "o_orderdate ASC, o_orderkey ASC",
        ),
    ] {
        let mut datasets = vec![accelerated("orders", engine, Mode::Memory)];
        apply_layout(&mut datasets, tpch(), &layout("sort")).expect("engine sorts");
        assert_eq!(
            params(&datasets[0]),
            HashMap::from([(param.to_string(), value.to_string())]),
            "{engine}"
        );
    }
}

/// The `__test_reference.*` oracle clones are unaccelerated. A layout that
/// reached them would configure the oracle with the layout under test.
#[test]
fn leaves_unaccelerated_datasets_untouched() {
    let reference = Dataset::new(
        "file:data/orders.parquet".to_string(),
        "__test_reference.orders".to_string(),
    );
    let mut datasets = vec![
        accelerated("orders", "duckdb", Mode::File),
        reference.clone(),
    ];
    apply_layout(&mut datasets, tpch(), &layout("primary_key,time_column"))
        .expect("layout applies");
    assert_eq!(datasets[1], reference);
    assert_eq!(datasets[0].time_column.as_deref(), Some("o_orderdate"));
}

#[test]
fn refuses_a_layout_it_cannot_honour() {
    let refusal = |datasets: &mut Vec<Dataset>, layout_str: &str| {
        apply_layout(datasets, tpch(), &layout(layout_str))
            .expect_err("layout refused")
            .to_string()
    };

    assert_eq!(
        refusal(
            &mut vec![accelerated("orders", "sqlite", Mode::File)],
            "sort"
        ),
        "dataset 'orders' uses the Sqlite engine in File mode, which cannot take the layout feature 'sort'"
    );
    assert_eq!(
        refusal(
            &mut vec![accelerated("orders", "cayenne", Mode::Memory)],
            "partition"
        ),
        "dataset 'orders' uses the Cayenne engine in Memory mode, which cannot take the layout feature 'partition'"
    );
    assert_eq!(
        refusal(
            &mut vec![accelerated("orders", "duckdb", Mode::File)],
            "cluster"
        ),
        "dataset 'orders' uses the DuckDb engine in File mode, which cannot take the layout feature 'cluster'"
    );
    assert_eq!(
        refusal(
            &mut vec![accelerated("orders", "cayenne", Mode::File)],
            "sort,cluster"
        ),
        "Cayenne does not combine `cayenne_sort_columns` with `cayenne_cluster_by`; use one of `sort` and `cluster` per layout"
    );
    assert_eq!(
        refusal(
            &mut vec![accelerated("lineorder", "duckdb", Mode::File)],
            "indexes"
        ),
        "dataset 'lineorder' is accelerated but has no layout keys; add its table to the benchmark's layout catalog"
    );

    let mut keyed = accelerated("orders", "duckdb", Mode::File);
    keyed
        .acceleration
        .as_mut()
        .expect("accelerated")
        .primary_key = Some("o_orderkey".to_string());
    assert_eq!(
        refusal(&mut vec![keyed], "primary_key"),
        "dataset 'orders' already sets `primary_key`; give the layout a Spicepod that does not"
    );

    // `region` has no time column and no partition expression, so a layout of
    // only those would configure nothing and test nothing.
    assert_eq!(
        refusal(
            &mut vec![accelerated("region", "cayenne", Mode::File)],
            "time_column,partition"
        ),
        "layout 'time_column,partition' configured time_column, partition on no accelerated dataset, so the run would not test it"
    );
}

/// Every table of a benchmark must have one entry under its own name, or
/// `apply_layout` would refuse a Spicepod that reads it, and a key unless its
/// schema declares none — of all the benchmarks, only CH-benCH `history`.
#[test]
fn every_benchmark_table_has_one_entry_and_a_key() {
    for (query_set, expected_tables) in [
        (QuerySet::Tpch, 8_usize),
        (QuerySet::Tpcds, 24),
        (QuerySet::Clickbench, 1),
        (QuerySet::ChBench, 12),
    ] {
        let tables = benchmark_tables(&query_set).expect("benchmark has layout keys");
        let names: BTreeSet<&str> = tables.iter().map(|keys| keys.table).collect();
        assert_eq!(
            names.len(),
            expected_tables,
            "{query_set:?} tables: {names:?}"
        );
        assert_eq!(
            names.len(),
            tables.len(),
            "{query_set:?} lists a table twice"
        );
        let keyless: Vec<&str> = tables
            .iter()
            .filter(|keys| keys.primary_key.is_empty())
            .map(|keys| keys.table)
            .collect();
        let expected_keyless: &[&str] = if query_set == QuerySet::ChBench {
            &["history"]
        } else {
            &[]
        };
        assert_eq!(
            keyless, expected_keyless,
            "{query_set:?} tables without a key"
        );
    }
}

/// The columns a partition expression reads. Covers the forms the catalogs
/// use; any other form fails the test rather than going unchecked.
fn partition_columns(expression: &str) -> BTreeSet<String> {
    use datafusion::sql::sqlparser::{
        ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments},
        dialect::GenericDialect,
        parser::Parser,
    };

    fn visit(expr: &Expr, columns: &mut BTreeSet<String>) {
        match expr {
            Expr::Identifier(ident) => {
                columns.insert(ident.value.clone());
            }
            Expr::Value(_) => {}
            Expr::Nested(inner) => visit(inner, columns),
            Expr::Function(function) => {
                let FunctionArguments::List(list) = &function.args else {
                    panic!("unexpected arguments in partition expression: {expr}");
                };
                for arg in &list.args {
                    let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) = arg else {
                        panic!("unexpected argument in partition expression: {expr}");
                    };
                    visit(arg, columns);
                }
            }
            other => panic!("unexpected partition expression form: {other}"),
        }
    }

    let expr = Parser::new(&GenericDialect {})
        .try_with_sql(expression)
        .and_then(|mut parser| parser.parse_expr())
        .expect("partition expression parses");
    let mut columns = BTreeSet::new();
    visit(&expr, &mut columns);
    columns
}

/// The HTAP workload updates CH-benCH tables, and a keyed partitioned
/// acceleration resolves a key within its partition, so an expression over a
/// column an update changes would leave the old version of a moved row behind
/// (#14596) and fail the HTAP gates on that alone.
#[test]
fn chbench_partitions_each_keyed_table_on_its_primary_key() {
    let tables = benchmark_tables(&QuerySet::ChBench).expect("CH-benCH layout keys");
    let partitioned: Vec<(&str, BTreeSet<String>)> = tables
        .iter()
        .filter(|keys| !keys.primary_key.is_empty())
        .filter_map(|keys| {
            keys.partition_by
                .map(|expression| (keys.table, partition_columns(expression)))
        })
        .collect();
    assert_eq!(
        partitioned
            .iter()
            .map(|(table, _)| *table)
            .collect::<Vec<_>>(),
        [
            "customer",
            "new_order",
            "oorder",
            "order_line",
            "stock",
            "item"
        ],
        "keyed CH-benCH tables with a partition expression"
    );
    for (table, columns) in partitioned {
        let keys = tables
            .iter()
            .find(|keys| keys.table == table)
            .expect("table keys");
        let outside_key: Vec<&String> = columns
            .iter()
            .filter(|column| !keys.primary_key.contains(&column.as_str()))
            .collect();
        assert_eq!(
            outside_key,
            Vec::<&String>::new(),
            "{table} partitions on columns outside its primary key {:?}",
            keys.primary_key
        );
    }
}
