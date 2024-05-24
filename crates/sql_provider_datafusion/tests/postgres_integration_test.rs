use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::TimestampMillisecondArray,
    datatypes::{DataType, TimeUnit},
};
use datafusion::execution::context::SessionContext;
use db_connection_pool::{
    dbconnection::postgresconn::PostgresConnection, postgrespool::PostgresConnectionPool,
    DbConnectionPool,
};
use pgtemp::PgTempDB;
use sql_provider_datafusion::SqlTable;

// Run this test requires local installation of postgres
// See Requirements in https://github.com/boustrophedon/pgtemp
#[ignore]
#[tokio::test]
async fn test_postgres_types() {
    let db = PgTempDB::async_new().await;
    let ctx = SessionContext::new();
    let params = Arc::new(HashMap::from([
        ("pg_host".to_string(), "localhost".into()),
        ("pg_port".to_string(), format!("{}", db.db_port())),
        ("pg_user".to_string(), db.db_user().into()),
        ("pg_pass".to_string(), db.db_pass().into()),
        ("pg_db".to_string(), db.db_name().into()),
        ("pg_sslmode".to_string(), "disable".into()),
    ]));
    let pool: Arc<dyn DbConnectionPool<_, _> + Send + Sync> = Arc::new(
        PostgresConnectionPool::new(params, None)
            .await
            .expect("Postgres connection pool should be created"),
    );
    let conn = pool.connect().await.expect("Connection should be created");
    let db_conn = conn
        .as_any()
        .downcast_ref::<PostgresConnection>()
        .expect("Unable to downcast to DuckDbConnection");
    db_conn
        .conn
        .execute(
            "
CREATE TABLE test (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);",
            &[],
        )
        .await
        .expect("table created");
    db_conn
        .conn
        .execute(
            "INSERT INTO test (id, created_at) VALUES (gen_random_uuid(), '2023-05-02 10:30:00-04:00');",
            &[],
        )
        .await
        .expect("row inserted");
    let table = SqlTable::new("postgres", &pool, "test", None)
        .await
        .expect("SqlTable should be created");
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
        DataType::FixedSizeBinary(16),
        *record_batch.schema().fields()[0].data_type()
    );
    assert_eq!(
        DataType::Timestamp(TimeUnit::Millisecond, None),
        *record_batch.schema().fields()[1].data_type()
    );

    assert_eq!(
        1_683_037_800_000,
        record_batch.columns()[1]
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("array can be cast")
            .value(0)
    );
}
