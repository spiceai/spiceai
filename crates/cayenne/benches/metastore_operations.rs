#![allow(clippy::expect_used)]

use cayenne::metastore::sqlite::SqliteMetastore;
#[cfg(feature = "turso")]
use cayenne::metastore::turso::TursoMetastore;
use cayenne::metastore::{
    ExecuteParams, MetastoreBackend, MetastoreValue, QueryParams, QueryRowParams,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL
);
";

fn get_sqlite_metastore() -> (SqliteMetastore, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let connection_string = format!("sqlite://{}", db_path.display());
    let metastore = SqliteMetastore::new(&connection_string);
    (metastore, temp_dir)
}

#[cfg(feature = "turso")]
fn get_turso_metastore() -> (TursoMetastore, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let connection_string = format!("libsql://{}", db_path.display());
    let metastore = TursoMetastore::new(&connection_string);
    (metastore, temp_dir)
}

/// Benchmark schema initialization
fn bench_init_schema(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("init_schema");

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (metastore, _temp_dir) = get_sqlite_metastore();
                metastore.init_schema().await.expect("Failed to init");
                black_box(());
            });
        });
    });

    #[cfg(feature = "turso")]
    group.bench_function("turso", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (metastore, _temp_dir) = get_turso_metastore();
                metastore.init_schema().await.expect("Failed to init");
                black_box(());
            });
        });
    });

    group.finish();
}

/// Benchmark single row insertion
fn bench_insert_single(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("insert_single");

    let sql = "INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, ?)";

    group.bench_function("sqlite", |b| {
        let setup = rt.block_on(async {
            let (metastore, temp_dir) = get_sqlite_metastore();
            metastore
                .execute_batch(SCHEMA_SQL)
                .await
                .expect("Failed to init");
            (metastore, temp_dir)
        });

        b.iter(|| {
            rt.block_on(async {
                let params = vec![
                    MetastoreValue::Integer(1),
                    MetastoreValue::Text("test_name".to_string()),
                    MetastoreValue::Integer(100),
                    MetastoreValue::Bool(true),
                ];
                setup
                    .0
                    .execute(ExecuteParams { sql, params })
                    .await
                    .expect("Failed to insert");
                black_box(());
                // Cleanup for next iteration
                setup
                    .0
                    .execute(ExecuteParams {
                        sql: "DELETE FROM test_table WHERE id = 1",
                        params: vec![],
                    })
                    .await
                    .expect("Failed to cleanup");
            });
        });
    });

    #[cfg(feature = "turso")]
    group.bench_function("turso", |b| {
        let setup = rt.block_on(async {
            let (metastore, temp_dir) = get_turso_metastore();
            metastore
                .execute_batch(SCHEMA_SQL)
                .await
                .expect("Failed to init");
            (metastore, temp_dir)
        });

        b.iter(|| {
            rt.block_on(async {
                let params = vec![
                    MetastoreValue::Integer(1),
                    MetastoreValue::Text("test_name".to_string()),
                    MetastoreValue::Integer(100),
                    MetastoreValue::Bool(true),
                ];
                setup
                    .0
                    .execute(ExecuteParams { sql, params })
                    .await
                    .expect("Failed to insert");
                black_box(());
                // Cleanup for next iteration
                setup
                    .0
                    .execute(ExecuteParams {
                        sql: "DELETE FROM test_table WHERE id = 1",
                        params: vec![],
                    })
                    .await
                    .expect("Failed to cleanup");
            });
        });
    });

    group.finish();
}

/// Benchmark batch insertions
fn bench_insert_batch(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("insert_batch");

    let sizes = vec![10, 100];

    for size in sizes {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            let setup = rt.block_on(async {
                let (metastore, temp_dir) = get_sqlite_metastore();
                metastore.execute_batch(SCHEMA_SQL).await.expect("Failed to init");
                (metastore, temp_dir)
            });

            b.iter(|| {
                rt.block_on(async {
                    for i in 0..size {
                        let params = vec![
                            MetastoreValue::Integer(i),
                            MetastoreValue::Text(format!("name_{i}")),
                            MetastoreValue::Integer(i * 10),
                            MetastoreValue::Bool(i % 2 == 0),
                        ];
                        setup
                            .0
                            .execute(ExecuteParams {
                                sql: "INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, ?)",
                                params
                            })
                            .await
                            .expect("Failed to insert");
                        black_box(());
                    }
                    // Cleanup
                    setup.0.execute(ExecuteParams {
                        sql: "DELETE FROM test_table",
                        params: vec![]
                    }).await.expect("Failed to cleanup");
                });
            });
        });

        #[cfg(feature = "turso")]
        group.bench_with_input(BenchmarkId::new("turso", size), &size, |b, &size| {
            let setup = rt.block_on(async {
                let (metastore, temp_dir) = get_turso_metastore();
                metastore.execute_batch(SCHEMA_SQL).await.expect("Failed to init");
                (metastore, temp_dir)
            });

            b.iter(|| {
                rt.block_on(async {
                    for i in 0..size {
                        let params = vec![
                            MetastoreValue::Integer(i),
                            MetastoreValue::Text(format!("name_{i}")),
                            MetastoreValue::Integer(i * 10),
                            MetastoreValue::Bool(i % 2 == 0),
                        ];
                        setup
                            .0
                            .execute(ExecuteParams {
                                sql: "INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, ?)",
                                params
                            })
                            .await
                            .expect("Failed to insert");
                        black_box(());
                    }
                    // Cleanup
                    setup.0.execute(ExecuteParams {
                        sql: "DELETE FROM test_table",
                        params: vec![]
                    }).await.expect("Failed to cleanup");
                });
            });
        });
    }

    group.finish();
}

/// Benchmark single row query
fn bench_query_single(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("query_single");

    group.bench_function("sqlite", |b| {
        let setup = rt.block_on(async {
            let (metastore, temp_dir) = get_sqlite_metastore();
            metastore
                .execute_batch(SCHEMA_SQL)
                .await
                .expect("Failed to init");
            // Insert test data
            for i in 0..100 {
                let params = vec![
                    MetastoreValue::Integer(i),
                    MetastoreValue::Text(format!("name_{i}")),
                    MetastoreValue::Integer(i * 10),
                    MetastoreValue::Bool(i % 2 == 0),
                ];
                metastore.execute(ExecuteParams {
                    sql: "INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, ?)",
                    params
                }).await.expect("Failed to insert");
            }
            (metastore, temp_dir)
        });

        b.iter(|| {
            rt.block_on(async {
                let result = setup
                    .0
                    .query_row(
                        QueryRowParams {
                            sql: "SELECT id, name, value, is_active FROM test_table WHERE id = ?",
                            params: vec![MetastoreValue::Integer(50)],
                        },
                        |row| {
                            let id = row.get_i64(0)?;
                            let name = row.get_string(1)?;
                            let value = row.get_i64(2)?;
                            let is_active = row.get_bool(3)?;
                            Ok((id, name, value, is_active))
                        },
                    )
                    .await
                    .expect("Failed to query");
                black_box(result);
            });
        });
    });

    #[cfg(feature = "turso")]
    group.bench_function("turso", |b| {
        let setup = rt.block_on(async {
            let (metastore, temp_dir) = get_turso_metastore();
            metastore
                .execute_batch(SCHEMA_SQL)
                .await
                .expect("Failed to init");
            // Insert test data
            for i in 0..100 {
                let params = vec![
                    MetastoreValue::Integer(i),
                    MetastoreValue::Text(format!("name_{i}")),
                    MetastoreValue::Integer(i * 10),
                    MetastoreValue::Bool(i % 2 == 0),
                ];
                metastore.execute(ExecuteParams {
                    sql: "INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, ?)",
                    params
                }).await.expect("Failed to insert");
            }
            (metastore, temp_dir)
        });

        b.iter(|| {
            rt.block_on(async {
                let result = setup
                    .0
                    .query_row(
                        QueryRowParams {
                            sql: "SELECT id, name, value, is_active FROM test_table WHERE id = ?",
                            params: vec![MetastoreValue::Integer(50)],
                        },
                        |row| {
                            let id = row.get_i64(0)?;
                            let name = row.get_string(1)?;
                            let value = row.get_i64(2)?;
                            let is_active = row.get_bool(3)?;
                            Ok((id, name, value, is_active))
                        },
                    )
                    .await
                    .expect("Failed to query");
                black_box(result);
            });
        });
    });

    group.finish();
}

/// Benchmark multiple row query
fn bench_query_batch(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("query_batch");

    let sizes = vec![10, 100];

    for size in sizes {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            let setup = rt.block_on(async {
                let (metastore, temp_dir) = get_sqlite_metastore();
                metastore.execute_batch(SCHEMA_SQL).await.expect("Failed to init");
                // Insert test data
                for i in 0..size {
                    let params = vec![
                        MetastoreValue::Integer(i),
                        MetastoreValue::Text(format!("name_{i}")),
                        MetastoreValue::Integer(i * 10),
                        MetastoreValue::Bool(true), // All active
                    ];
                    metastore.execute(ExecuteParams {
                        sql: "INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, ?)",
                        params
                    }).await.expect("Failed to insert");
                }
                (metastore, temp_dir)
            });

            b.iter(|| {
                rt.block_on(async {
                    let results = setup.0.query(
                        QueryParams {
                            sql: "SELECT id, name, value, is_active FROM test_table WHERE is_active = ?",
                            params: vec![MetastoreValue::Bool(true)]
                        },
                        |row| {
                            let id = row.get_i64(0)?;
                            let name = row.get_string(1)?;
                            let value = row.get_i64(2)?;
                            let is_active = row.get_bool(3)?;
                            Ok((id, name, value, is_active))
                        }
                    ).await.expect("Failed to query");
                    black_box(results);
                });
            });
        });

        #[cfg(feature = "turso")]
        group.bench_with_input(BenchmarkId::new("turso", size), &size, |b, &size| {
            let setup = rt.block_on(async {
                let (metastore, temp_dir) = get_turso_metastore();
                metastore.execute_batch(SCHEMA_SQL).await.expect("Failed to init");
                // Insert test data
                for i in 0..size {
                    let params = vec![
                        MetastoreValue::Integer(i),
                        MetastoreValue::Text(format!("name_{i}")),
                        MetastoreValue::Integer(i * 10),
                        MetastoreValue::Bool(true), // All active
                    ];
                    metastore.execute(ExecuteParams {
                        sql: "INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, ?)",
                        params
                    }).await.expect("Failed to insert");
                }
                (metastore, temp_dir)
            });

            b.iter(|| {
                rt.block_on(async {
                    let results = setup.0.query(
                        QueryParams {
                            sql: "SELECT id, name, value, is_active FROM test_table WHERE is_active = ?",
                            params: vec![MetastoreValue::Bool(true)]
                        },
                        |row| {
                            let id = row.get_i64(0)?;
                            let name = row.get_string(1)?;
                            let value = row.get_i64(2)?;
                            let is_active = row.get_bool(3)?;
                            Ok((id, name, value, is_active))
                        }
                    ).await.expect("Failed to query");
                    black_box(results);
                });
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_init_schema,
    bench_insert_single,
    bench_insert_batch,
    bench_query_single,
    bench_query_batch
);
criterion_main!(benches);
