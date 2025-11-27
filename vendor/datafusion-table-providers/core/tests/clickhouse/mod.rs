use clickhouse::Client;
use common::{get_clickhouse_params, start_clickhouse_docker_container};
use datafusion::{prelude::SessionContext, sql::TableReference};
use datafusion_table_providers::{
    clickhouse::ClickHouseTableFactory,
    sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool,
};

mod common;

use serde::{Deserialize, Serialize};

#[derive(clickhouse::Row, Serialize, Deserialize, Debug, PartialEq)]
struct Row {
    id: i64,
    name: String,
    age: i32,
    is_active: bool,
    score: f64,
    created_at: i64,
    tags: Vec<String>,
    tag_groups: Vec<Vec<String>>,
    attributes: (f32, f32),
}

fn create_sample_rows() -> Vec<Row> {
    vec![
        Row {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
            is_active: true,
            score: 91.5,
            created_at: 1689000000000,
            tags: vec!["fast".to_string(), "smart".to_string()],
            tag_groups: vec![
                vec!["group1".to_string(), "groupA".to_string()],
                vec!["group2".to_string()],
            ],
            attributes: (5.5, 130.0),
        },
        Row {
            id: 2,
            name: "Bob".to_string(),
            age: 45,
            is_active: false,
            score: 85.2,
            created_at: 1689000360000,
            tags: vec!["strong".to_string()],
            tag_groups: vec![vec!["group3".to_string()]],
            attributes: (6.1, 180.0),
        },
    ]
}

async fn create_table(client: Client, table_name: &str) {
    let sql: String = format!(
        "
    CREATE TABLE IF NOT EXISTS {table_name} (
    id Int64,
    name String,
    age Int32,
    is_active Bool,
    score Float64,
    created_at DateTime64(3),
    tags Array(String),
    tag_groups Array(Array(String)),
    attributes Tuple(Float32, Float32),
    ) ENGINE = MergeTree() ORDER BY id;
    "
    );
    client.query(&sql).execute().await.unwrap();
}

async fn insert_rows(
    client: &clickhouse::Client,
    table: &str,
    rows: Vec<Row>,
) -> clickhouse::error::Result<()> {
    let mut insert = client.insert(table)?;
    for row in rows {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

/// inserts data into clickhouse using official client and reads it back for now
#[tokio::test]
async fn clickhouse_insert_and_read() {
    start_clickhouse_docker_container().await.unwrap();

    let table_name = "test_table";
    let pool = ClickHouseConnectionPool::new(get_clickhouse_params())
        .await
        .unwrap();

    create_table(pool.client(), table_name).await;
    insert_rows(&pool.client, table_name, create_sample_rows())
        .await
        .unwrap();

    let factory = ClickHouseTableFactory::new(pool);
    let ctx = SessionContext::new();

    let table_provider = factory
        .table_provider(TableReference::bare(table_name), None)
        .await
        .unwrap();

    ctx.register_table(table_name, table_provider)
        .expect("Table should be registered");

    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");

    assert_eq!(record_batch[0].num_rows(), 2);
    assert_eq!(record_batch[0].num_columns(), 9);
}
