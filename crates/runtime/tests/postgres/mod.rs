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

use std::sync::Arc;

use arrow::{
    array::TimestampMillisecondArray,
    datatypes::{DataType, TimeUnit},
};
use datafusion::execution::context::SessionContext;
use datafusion_table_providers::{
    postgres::DynPostgresConnectionPool, sql::sql_provider_datafusion::SqlTable,
};

use crate::init_tracing;

pub mod common;

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
        DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
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
