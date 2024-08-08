/*
Copyright 2024 The Spice.ai OSS Authors

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

use super::*;
use std::sync::Arc;

use arrow::{
    array::{TimestampMillisecondArray, *},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use async_graphql::Data;
use chrono::NaiveTime;
use datafusion::{
    execution::context::SessionContext, physical_plan::insert, sql::sqlparser::ast::Insert,
};
use datafusion_table_providers::sql::{
    arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder},
    db_connection_pool::postgrespool::PostgresConnectionPool,
};
use datafusion_table_providers::{
    postgres::DynPostgresConnectionPool, sql::sql_provider_datafusion::SqlTable,
};
use futures::TryFutureExt;
use tracing_subscriber::fmt::format;
use types::{Float32Type, LargeBinaryType};

use crate::init_tracing;

mod common;

#[tokio::test]
async fn test_postgres_types() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let port = common::get_random_port();
    let running_container = common::start_postgres_docker_container(port).await?;

    let ctx = SessionContext::new();
    let pool = common::get_postgres_connection_pool(port).await?;
    let db_conn = pool
        .connect_direct()
        .await
        .expect("connection can be established");
    db_conn
        .conn
        .execute(
            "
CREATE TABLE test (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);",
            &[],
        )
        .await
        .expect("table is created");
    db_conn
        .conn
        .execute(
            "INSERT INTO test (id, created_at) VALUES ('5ea5a3ac-07a0-4d4d-b201-faff68d8356c', '2023-05-02 10:30:00-04:00');",
            &[],
        )
        .await.expect("inserted data");
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, "test", None)
        .await
        .expect("table can be created");
    ctx.register_table("test_datafusion", Arc::new(table))
        .expect("Table should be registered");
    let sql = "SELECT id, created_at FROM test_datafusion";
    let df = ctx
        .sql(sql)
        .await
        .expect("DataFrame can be created from query");
    let record_batch = df.collect().await.expect("RecordBatch can be collected");
    assert_eq!(record_batch.len(), 1);
    let record_batch = record_batch
        .first()
        .expect("At least 1 record batch is returned");
    assert_eq!(record_batch.num_rows(), 1);

    assert_eq!(
        DataType::Utf8,
        *record_batch.schema().fields()[0].data_type()
    );
    assert_eq!(
        DataType::Timestamp(TimeUnit::Millisecond, None),
        *record_batch.schema().fields()[1].data_type()
    );

    assert_eq!(
        "5ea5a3ac-07a0-4d4d-b201-faff68d8356c",
        record_batch.columns()[0]
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("array can be cast")
            .value(0)
    );
    assert_eq!(
        1_683_037_800_000,
        record_batch.columns()[1]
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("array can be cast")
            .value(0)
    );

    running_container.remove().await?;

    Ok(())
}

#[tokio::test]
async fn test_postgres_chunking_performance() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let port = common::get_random_port();
    let running_container = common::start_postgres_docker_container(port).await?;

    let ctx = SessionContext::new();
    let pool = common::get_postgres_connection_pool(port).await?;
    let db_conn = pool
        .connect_direct()
        .await
        .expect("connection can be established");
    db_conn
        .conn
        .execute(
            "
CREATE TABLE test (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);",
            &[],
        )
        .await
        .expect("table is created");

    let mut values: Vec<String> = Vec::new();
    for i in 0..250_000 {
        values.push(format!("('{i}')"));
    }

    let values = values.join(",");
    db_conn
        .conn
        .execute(&format!("INSERT INTO test (id) VALUES {values};"), &[])
        .await
        .expect("inserted data");

    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, "test", None)
        .await
        .expect("table can be created");
    ctx.register_table("test_datafusion", Arc::new(table))
        .expect("Table should be registered");
    let sql = "SELECT id, created_at FROM test_datafusion";
    let start = std::time::Instant::now();
    let df = ctx
        .sql(sql)
        .await
        .expect("DataFrame can be created from query");
    let record_batch = df.collect().await.expect("RecordBatch can be collected");
    let end = std::time::Instant::now();
    let duration = end - start;
    let duration_ms = duration.as_millis();
    let num_rows = record_batch
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum::<usize>();
    assert_eq!(num_rows, 250_000);

    assert!(
        duration_ms < 1000,
        "Duration {duration_ms}ms was higher than 1000ms",
    );

    running_container.remove().await?;

    Ok(())
}

async fn arrow_postgres_round_trip(
    port: usize,
    arrow_record: RecordBatch,
    table_name: &str,
) -> Result<(), String> {
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

    let pool = common::get_postgres_connection_pool(port)
        .await
        .map_err(|e| format!("Failed to create postgres connection pool: {e}"))?;

    let db_conn = pool
        .connect_direct()
        .await
        .expect("connection can be established");

    // Create postgres table from arrow records and insert arrow records
    let schema = Arc::clone(&arrow_record.schema());
    let create_table_stmts = CreateTableBuilder::new(schema, table_name).build_postgres();
    let insert_table_stmt = InsertBuilder::new(table_name, vec![arrow_record.clone()])
        .build_postgres(None)
        .map_err(|e| format!("Unable to construct postgres insert statement: {e}"))?;

    // Test arrow -> Postgres row coverage
    for create_table_stmt in create_table_stmts {
        let _ = db_conn
            .conn
            .execute(&create_table_stmt, &[])
            .await
            .map_err(|e| format!("Postgres table cannot be created: {e}"));
    }
    let _ = db_conn
        .conn
        .execute(&insert_table_stmt, &[])
        .await
        .map_err(|e| format!("Postgres table cannot be created: {e}"));

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, table_name, None)
        .await
        .expect("table can be created");
    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");
    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame can't be created from query");

    let record_batch = df.collect().await.expect("RecordBatch can't be collected");

    // Print original arrow record batch and record batch converted from postgres row in terminal
    // Check if the values are the same
    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Postgres returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());

    Ok(())
}

#[tokio::test]
async fn test_arrow_postgres_types_conversion() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let port = common::get_random_port();
    let running_container = common::start_postgres_docker_container(port)
        .await
        .map_err(|e| format!("Failed to create postgres container: {e}"))?;

    tracing::debug!("Container started");

    let binary_record_batch = get_arrow_binary_record_batch();
    match arrow_postgres_round_trip(port, binary_record_batch, "binary_types").await {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    };

    let int_record_batch = get_arrow_int_recordbatch();
    match arrow_postgres_round_trip(port, int_record_batch, "int_types").await {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    };

    let float_record_batch = get_arrow_float_record_batch();
    match arrow_postgres_round_trip(port, float_record_batch, "float_types").await {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    };

    let utf8_record_batch = get_arrow_utf8_record_batch();
    match arrow_postgres_round_trip(port, utf8_record_batch, "utf8_types").await {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    };

    // // Pending on datafusion-table-providers merge
    // let time_record_batch = get_arrow_time_record_batch();
    // match arrow_postgres_round_trip(port, time_record_batch, "time_types").await {
    //     Ok(_) => (),
    //     Err(e) => panic!("{}", e),
    // };

    let timestamp_record_batch = get_arrow_timestamp_record_batch();
    match arrow_postgres_round_trip(port, timestamp_record_batch, "timestamp_types").await {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    };

    // // Pending on datafusion-table-providers merge
    // let date_record_batch = get_arrow_date_record_batch();
    // match arrow_postgres_round_trip(port, date_record_batch, "date_types").await {
    //     Ok(_) => (),
    //     Err(e) => panic!("{}", e),
    // };

    let struct_record_batch = get_arrow_struct_record_batch();
    match arrow_postgres_round_trip(port, struct_record_batch, "struct_types").await {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    };

    // // Pending on datafusion-table-providers fix
    // let decimal_record_batch = get_arrow_decimal_record_batch();
    // match arrow_postgres_round_trip(port, decimal_record_batch, "decimal_types").await {
    //     Ok(_) => (),
    //     Err(e) => panic!("{}", e),
    // };

    Ok(())
}
