// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Micro-benchmark for the per-file schema-reconciliation lookup that
//! `convert::schema::calculate_physical_schema` performs on every file open.
//!
//! For each of a file's fields it resolves the matching field in the reference
//! logical schema. Doing that with Arrow's `Schema::field_with_name` — a linear
//! scan, since Arrow keeps no name index — is O(fields^2) per file. Building a
//! `name -> field` index once makes it O(fields). This bench measures exactly
//! that operation over a wide schema; `linear_field_with_name` is the old
//! behavior, `indexed_lookup` is the new one.

use std::collections::HashMap;
use std::hint::black_box;

use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn wide_schema(num_fields: usize) -> Schema {
    let fields: Vec<Field> = (0..num_fields)
        .map(|i| Field::new(format!("c{i}"), DataType::Int64, true))
        .collect();
    Schema::new(fields)
}

/// Old behavior: resolve every field via a linear `field_with_name` scan — O(n^2).
fn linear_field_with_name(schema: &Schema, names: &[String]) -> usize {
    let mut matched = 0usize;
    for name in names {
        if schema.field_with_name(name).is_ok() {
            matched += 1;
        }
    }
    matched
}

/// New behavior: index the reference schema once, then resolve each field in
/// O(1) — O(n) overall.
fn indexed_lookup(schema: &Schema, names: &[String]) -> usize {
    let index: HashMap<&str, &Field> = {
        let fields = schema.fields();
        let mut m = HashMap::with_capacity(fields.len());
        for field in fields {
            m.entry(field.name().as_str()).or_insert(field.as_ref());
        }
        m
    };
    let mut matched = 0usize;
    for name in names {
        if index.get(name.as_str()).is_some() {
            matched += 1;
        }
    }
    matched
}

fn bench_schema_reconciliation(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_field_reconciliation");
    for num_fields in [64usize, 256, 1024] {
        let schema = wide_schema(num_fields);
        // The file lists the same fields; reconciliation looks up every one.
        let names: Vec<String> = (0..num_fields).map(|i| format!("c{i}")).collect();

        group.bench_with_input(
            BenchmarkId::new("linear_field_with_name", num_fields),
            &num_fields,
            |b, _| b.iter(|| black_box(linear_field_with_name(&schema, &names))),
        );
        group.bench_with_input(
            BenchmarkId::new("indexed_lookup", num_fields),
            &num_fields,
            |b, _| b.iter(|| black_box(indexed_lookup(&schema, &names))),
        );
    }
    group.finish();
}

criterion_group!(benches, bench_schema_reconciliation);
criterion_main!(benches);
