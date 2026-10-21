#![allow(clippy::expect_used)]

use cayenne::metastore::sqlite::SqliteMetastore;
#[cfg(feature = "turso")]
use cayenne::metastore::turso::TursoMetastore;
use cayenne::metastore::{
    ExecuteParams, MetastoreBackend, MetastoreValue, QueryParams, QueryRowParams,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
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

    let sizes = vec![10, 100, 1000, 10000];

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

    let sizes = vec![10, 100, 1000, 10000];

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

/// Benchmark inlined data operations (add + read + clear).
///
/// Simulates the small-batch streaming ingestion pattern where each write
/// stores Arrow IPC blobs directly in the metastore.
fn bench_inlined_data(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("inlined_data");

    // Schema for the Cayenne catalog tables (needed for inlined_data foreign key)
    let cayenne_init_sql = r"
        CREATE TABLE IF NOT EXISTS cayenne_table (
            table_id TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            schema_json TEXT NOT NULL,
            primary_key_json TEXT,
            on_conflict_json TEXT,
            current_snapshot_id TEXT NOT NULL DEFAULT '',
            partition_column TEXT,
            vortex_config_json TEXT,
            current_sequence_number BIGINT NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS cayenne_inlined_data (
            inlined_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            partition_key TEXT,
            data_ipc BLOB NOT NULL,
            record_count BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        );
        INSERT OR IGNORE INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json)
            VALUES ('bench-table-id', 'bench_table', '/tmp/bench', 0, '{}');
    ";

    // Create a small Arrow IPC blob (~100 rows)
    let ipc_blob: Vec<u8> = {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let ids: Vec<i64> = (0..100).collect();
        let vals: Vec<i64> = (0..100).map(|i| i * 10).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .expect("batch");
        let mut buf = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("writer");
            writer.write(&batch).expect("write");
            writer.finish().expect("finish");
        }
        buf
    };

    let batch_sizes = vec![10, 100];
    for batch_count in batch_sizes {
        group.bench_with_input(
            BenchmarkId::new("sqlite_add_inlined", batch_count),
            &batch_count,
            |b, &count| {
                let setup = rt.block_on(async {
                    let (metastore, temp_dir) = get_sqlite_metastore();
                    metastore
                        .execute_batch(cayenne_init_sql)
                        .await
                        .expect("Failed to init");
                    (metastore, temp_dir)
                });

                let blob = ipc_blob.clone();
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..count {
                            let id = format!("bench-inline-{i}");
                            setup
                                .0
                                .execute(ExecuteParams {
                                    sql: "INSERT INTO cayenne_inlined_data (inlined_id, table_id, data_ipc, record_count, sequence_number) VALUES (?1, ?2, ?3, ?4, ?5)",
                                    params: vec![
                                        MetastoreValue::Text(id),
                                        MetastoreValue::Text("bench-table-id".to_string()),
                                        MetastoreValue::Blob(blob.clone()),
                                        MetastoreValue::Integer(100),
                                        MetastoreValue::Integer(i64::from(i)),
                                    ],
                                })
                                .await
                                .expect("Failed to insert inlined data");
                        }
                        // Cleanup
                        setup
                            .0
                            .execute(ExecuteParams {
                                sql: "DELETE FROM cayenne_inlined_data",
                                params: vec![],
                            })
                            .await
                            .expect("Failed to cleanup");
                        black_box(());
                    });
                });
            },
        );
    }

    group.finish();
}

/// Benchmark table statistics upsert operations (single BLOB per table).
fn bench_table_statistics_upsert(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("table_statistics_upsert");

    let cayenne_init_sql = r"
        CREATE TABLE IF NOT EXISTS cayenne_table (
            table_id TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            schema_json TEXT NOT NULL,
            primary_key_json TEXT,
            on_conflict_json TEXT,
            current_snapshot_id TEXT NOT NULL DEFAULT '',
            partition_column TEXT,
            vortex_config_json TEXT,
            current_sequence_number BIGINT NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS cayenne_table_statistics (
            table_id TEXT NOT NULL PRIMARY KEY,
            statistics_blob BLOB NOT NULL,
            num_rows BIGINT NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        );
        INSERT OR IGNORE INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json)
            VALUES ('bench-table-id', 'bench_table', '/tmp/bench', 0, '{}');
    ";

    let blob_sizes = vec![256, 1024, 4096];
    for blob_size in blob_sizes {
        group.bench_with_input(
            BenchmarkId::new("sqlite_upsert", blob_size),
            &blob_size,
            |b, &size| {
                let setup = rt.block_on(async {
                    let (metastore, temp_dir) = get_sqlite_metastore();
                    metastore
                        .execute_batch(cayenne_init_sql)
                        .await
                        .expect("Failed to init");
                    (metastore, temp_dir)
                });

                let blob = vec![0u8; size];
                b.iter(|| {
                    rt.block_on(async {
                        setup
                            .0
                            .execute(ExecuteParams {
                                sql: "INSERT OR REPLACE INTO cayenne_table_statistics (table_id, statistics_blob, num_rows) VALUES (?1, ?2, ?3)",
                                params: vec![
                                    MetastoreValue::Text("bench-table-id".to_string()),
                                    MetastoreValue::Blob(blob.clone()),
                                    MetastoreValue::Integer(10000),
                                ],
                            })
                            .await
                            .expect("Failed to upsert stats");
                        black_box(());
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_init_schema,
    bench_insert_single,
    bench_insert_batch,
    bench_query_single,
    bench_query_batch,
    bench_inlined_data,
    bench_table_statistics_upsert
);
criterion_main!(benches);
