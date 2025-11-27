use std::sync::Arc;

use datafusion::{prelude::SessionContext, sql::TableReference};
use datafusion_table_providers::{
    duckdb::DuckDBTableFactory, sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};

/// This example demonstrates how to register a TableProvider into DataFusion that
/// uses a DuckDB function as its source.
#[tokio::main]
async fn main() {
    let duckdb_pool = Arc::new(
        DuckDbConnectionPool::new_memory().expect("unable to create DuckDB connection pool"),
    );

    let duckdb_table_factory = DuckDBTableFactory::new(duckdb_pool);

    // Use any DuckDB function as the the source of the table
    let duckdb_read_csv_function = "read_csv_auto('https://docs.google.com/spreadsheets/d/1Oo9M9ZI_esARoXfCPx7aJHKdSSdOjNoKF0NmT3naFGc/export?format=csv')";
    let amazing_projects = duckdb_table_factory
        .table_provider(TableReference::bare(duckdb_read_csv_function))
        .await
        .expect("to create table provider");

    let ctx = SessionContext::new();

    ctx.register_table("amazing_projects", amazing_projects)
        .expect("to register table");

    let df = ctx
        .sql("SELECT * FROM amazing_projects")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
