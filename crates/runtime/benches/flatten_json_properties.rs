#![allow(clippy::expect_used)]

//! Benchmarks for `flatten_json_properties`.
//!
//! Exercises the walker in isolation (no `DataFusion` plumbing) so regressions
//! attributable to the walker itself surface without noise from query planning
//! or Arrow I/O. `bench_catalog_simulation` approximates the typical
//! materialization shape — 1k schemas × 50 fields per schema.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use runtime::datafusion::udtf::json_properties::{FlattenOptions, flatten_with_options};

fn synthetic_schema(num_fields: usize) -> String {
    // One flat object with `num_fields` primitive properties. Representative of
    // a wide data-product schema where most fields are leaves.
    let mut props = String::from("{");
    for i in 0..num_fields {
        if i > 0 {
            props.push(',');
        }
        props.push_str(&format!(
            r#""field_{i}":{{"type":"string","description":"Field {i}","format":"text"}}"#
        ));
    }
    props.push('}');
    format!(r#"{{"properties":{props}}}"#)
}

fn nested_schema(depth: usize) -> String {
    // Deeply nested single-chain schema. Exercises the recursion path.
    let mut inner = String::from(r#"{"type":"string"}"#);
    for _ in 0..depth {
        inner = format!(r#"{{"type":"object","properties":{{"n":{inner}}}}}"#);
    }
    format!(r#"{{"properties":{{"root":{inner}}}}}"#)
}

fn bench_flat_schemas(c: &mut Criterion) {
    let opts = FlattenOptions {
        include_internal: true,
        ..FlattenOptions::default()
    };
    let mut group = c.benchmark_group("flatten_json_properties/flat");
    for fields in [16usize, 128, 512] {
        let doc = synthetic_schema(fields);
        group.throughput(Throughput::Elements(fields as u64));
        group.bench_with_input(BenchmarkId::new("fields", fields), &doc, |b, doc| {
            b.iter(|| {
                let rows = flatten_with_options(black_box(doc), &opts);
                black_box(rows);
            });
        });
    }
    group.finish();
}

fn bench_nested_schemas(c: &mut Criterion) {
    let opts = FlattenOptions {
        include_internal: true,
        max_depth: 32,
        ..FlattenOptions::default()
    };
    let mut group = c.benchmark_group("flatten_json_properties/nested");
    for depth in [4usize, 8, 16] {
        let doc = nested_schema(depth);
        group.throughput(Throughput::Elements(depth as u64));
        group.bench_with_input(BenchmarkId::new("depth", depth), &doc, |b, doc| {
            b.iter(|| {
                let rows = flatten_with_options(black_box(doc), &opts);
                black_box(rows);
            });
        });
    }
    group.finish();
}

fn bench_catalog_simulation(c: &mut Criterion) {
    let opts = FlattenOptions::default();
    let doc = synthetic_schema(50);
    c.bench_function("flatten_json_properties/catalog_1k_schemas", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let rows = flatten_with_options(black_box(&doc), &opts);
                black_box(rows);
            }
        });
    });
}

criterion_group!(
    benches,
    bench_flat_schemas,
    bench_nested_schemas,
    bench_catalog_simulation
);
criterion_main!(benches);
