use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use duckdb::DuckdbConnectionManager;
use duckdb_datafusion::DuckDBTable;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    let conn = DuckdbConnectionManager::memory()?;

    let pool = r2d2::Pool::new(conn)?;

    let db_conn = pool.get()?;
    db_conn.execute_batch(
        "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
    )?;

    let duckdb_table = DuckDBTable::new(pool, "test")?;

    ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;

    let sql = "SELECT * FROM test_datafusion";
    let df = ctx.sql(sql).await?;

    df.show().await?;

    Ok(())
}
