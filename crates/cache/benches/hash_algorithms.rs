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

#![allow(clippy::expect_used)] // Benchmarks can panic

use cache::{get_hash_builder, key::CacheKey};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use spicepod::component::caching::HashingAlgorithm;
use std::hash::BuildHasher;
use std::hint::black_box;

/// Benchmark hashing of small SQL queries (typical cache key)
fn bench_hash_small_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_small_query");
    let query = "SELECT id, name FROM users WHERE id = 123";

    let algorithms = [
        ("siphash", HashingAlgorithm::Siphash),
        ("ahash", HashingAlgorithm::Ahash),
        ("blake3", HashingAlgorithm::Blake3),
        ("xxh3", HashingAlgorithm::XXH3),
        ("xxh32", HashingAlgorithm::XXH32),
        ("xxh64", HashingAlgorithm::XXH64),
        ("xxh128", HashingAlgorithm::XXH128),
    ];

    for (name, algo) in algorithms {
        let hash_builder = get_hash_builder(algo).expect("Failed to get hash builder");
        group.bench_with_input(BenchmarkId::new(name, query.len()), &query, |b, q| {
            b.iter(|| {
                let key = CacheKey::Query(black_box(q), None);
                let hash = key.as_raw_key(hash_builder.build_hasher());
                black_box(hash.as_u64());
            });
        });
    }

    group.finish();
}

/// Benchmark hashing of medium SQL queries
fn bench_hash_medium_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_medium_query");
    let query = "SELECT u.id, u.name, u.email, o.order_id, o.total, o.created_at \
                 FROM users u JOIN orders o ON u.id = o.user_id \
                 WHERE u.created_at > '2024-01-01' AND o.status = 'completed' \
                 ORDER BY o.created_at DESC LIMIT 100";

    let algorithms = [
        ("siphash", HashingAlgorithm::Siphash),
        ("ahash", HashingAlgorithm::Ahash),
        ("blake3", HashingAlgorithm::Blake3),
        ("xxh3", HashingAlgorithm::XXH3),
        ("xxh32", HashingAlgorithm::XXH32),
        ("xxh64", HashingAlgorithm::XXH64),
        ("xxh128", HashingAlgorithm::XXH128),
    ];

    for (name, algo) in algorithms {
        let hash_builder = get_hash_builder(algo).expect("Failed to get hash builder");
        group.bench_with_input(BenchmarkId::new(name, query.len()), &query, |b, q| {
            b.iter(|| {
                let key = CacheKey::Query(black_box(q), None);
                let hash = key.as_raw_key(hash_builder.build_hasher());
                black_box(hash.as_u64());
            });
        });
    }

    group.finish();
}

/// Benchmark hashing of large SQL queries (complex analytical queries)
fn bench_hash_large_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_large_query");
    let query = "WITH monthly_sales AS (
        SELECT 
            DATE_TRUNC('month', order_date) as month,
            product_id,
            SUM(quantity * unit_price) as total_sales,
            COUNT(DISTINCT customer_id) as unique_customers,
            AVG(quantity) as avg_quantity
        FROM orders
        WHERE order_date >= '2023-01-01' 
          AND order_date < '2024-01-01'
          AND status IN ('completed', 'shipped')
        GROUP BY DATE_TRUNC('month', order_date), product_id
    ),
    product_rankings AS (
        SELECT 
            month,
            product_id,
            total_sales,
            unique_customers,
            avg_quantity,
            ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_sales DESC) as sales_rank,
            LAG(total_sales) OVER (PARTITION BY product_id ORDER BY month) as prev_month_sales
        FROM monthly_sales
    )
    SELECT 
        pr.month,
        p.product_name,
        p.category,
        pr.total_sales,
        pr.unique_customers,
        pr.avg_quantity,
        pr.sales_rank,
        CASE 
            WHEN pr.prev_month_sales IS NULL THEN NULL
            ELSE ((pr.total_sales - pr.prev_month_sales) / pr.prev_month_sales) * 100
        END as growth_percentage
    FROM product_rankings pr
    JOIN products p ON pr.product_id = p.id
    WHERE pr.sales_rank <= 10
    ORDER BY pr.month DESC, pr.sales_rank ASC";

    let algorithms = [
        ("siphash", HashingAlgorithm::Siphash),
        ("ahash", HashingAlgorithm::Ahash),
        ("blake3", HashingAlgorithm::Blake3),
        ("xxh3", HashingAlgorithm::XXH3),
        ("xxh32", HashingAlgorithm::XXH32),
        ("xxh64", HashingAlgorithm::XXH64),
        ("xxh128", HashingAlgorithm::XXH128),
    ];

    for (name, algo) in algorithms {
        let hash_builder = get_hash_builder(algo).expect("Failed to get hash builder");
        group.bench_with_input(BenchmarkId::new(name, query.len()), &query, |b, q| {
            b.iter(|| {
                let key = CacheKey::Query(black_box(q), None);
                let hash = key.as_raw_key(hash_builder.build_hasher());
                black_box(hash.as_u64());
            });
        });
    }

    group.finish();
}

/// Benchmark throughput - how many queries can be hashed per second
fn bench_hash_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_throughput");

    // Generate a diverse set of queries
    let queries: Vec<String> = (0..1000)
        .map(|i| {
            format!(
                "SELECT * FROM table_{} WHERE id = {} AND status = 'active'",
                i % 10,
                i
            )
        })
        .collect();

    let algorithms = [
        ("siphash", HashingAlgorithm::Siphash),
        ("ahash", HashingAlgorithm::Ahash),
        ("blake3", HashingAlgorithm::Blake3),
        ("xxh3", HashingAlgorithm::XXH3),
        ("xxh32", HashingAlgorithm::XXH32),
        ("xxh64", HashingAlgorithm::XXH64),
        ("xxh128", HashingAlgorithm::XXH128),
    ];

    for (name, algo) in algorithms {
        let hash_builder = get_hash_builder(algo).expect("Failed to get hash builder");
        group.bench_with_input(BenchmarkId::new(name, queries.len()), &queries, |b, qs| {
            b.iter(|| {
                for query in qs {
                    let key = CacheKey::Query(black_box(query.as_str()), None);
                    let hash = key.as_raw_key(hash_builder.build_hasher());
                    black_box(hash.as_u64());
                }
            });
        });
    }

    group.finish();
}

/// Benchmark collision resistance - hash distribution quality
/// This tests how well the hash spreads across the space
fn bench_hash_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_distribution");

    // Generate queries with minimal differences to test avalanche effect
    let queries: Vec<String> = (0..100)
        .map(|i| format!("SELECT * FROM users WHERE id = {i}"))
        .collect();

    let algorithms = [
        ("siphash", HashingAlgorithm::Siphash),
        ("ahash", HashingAlgorithm::Ahash),
        ("blake3", HashingAlgorithm::Blake3),
        ("xxh3", HashingAlgorithm::XXH3),
        ("xxh32", HashingAlgorithm::XXH32),
        ("xxh64", HashingAlgorithm::XXH64),
        ("xxh128", HashingAlgorithm::XXH128),
    ];

    for (name, algo) in algorithms {
        let hash_builder = get_hash_builder(algo).expect("Failed to get hash builder");
        group.bench_with_input(BenchmarkId::new(name, queries.len()), &queries, |b, qs| {
            b.iter(|| {
                let mut hashes = Vec::with_capacity(qs.len());
                for query in qs {
                    let key = CacheKey::Query(black_box(query.as_str()), None);
                    let hash = key.as_raw_key(hash_builder.build_hasher());
                    hashes.push(hash.as_u64());
                }
                black_box(hashes);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_hash_small_query,
    bench_hash_medium_query,
    bench_hash_large_query,
    bench_hash_throughput,
    bench_hash_distribution
);
criterion_main!(benches);
