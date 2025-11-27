use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, duckdb::DuckDBTableFactory,
    sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use duckdb::AccessMode;

/// This example demonstrates how to:
/// 1. Create a DuckDB connection pool
/// 2. Create and use DuckDBTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access DuckDB table data
#[tokio::main]
async fn main() {
    // Create DuckDB connection pool
    // Opening in ReadOnly mode allows multiple reader processes to access
    // the database at the same time
    let duckdb_pool = Arc::new(
        DuckDbConnectionPool::new_file("core/examples/duckdb_example.db", &AccessMode::ReadOnly)
            .expect("unable to create DuckDB connection pool"),
    );

    // Create DuckDB table provider factory
    // Used to generate TableProvider instances that can read DuckDB table data
    let table_factory = DuckDBTableFactory::new(duckdb_pool.clone());

    // Create database catalog provider
    // This allows us to access tables through catalog structure (catalog.schema.table)
    let catalog = DatabaseCatalogProvider::try_new(duckdb_pool).await.unwrap();

    // Create DataFusion session context
    let ctx = SessionContext::new();
    // Register DuckDB catalog, making it accessible via the "duckdb" name
    ctx.register_catalog("duckdb", Arc::new(catalog));

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the DuckDB "companies" table as "companies_v2"
    ctx.register_table(
        "companies_v2",
        table_factory
            .table_provider(TableReference::bare("companies"))
            .await
            .expect("failed to register table provider"),
    )
    .expect("failed to register table");

    // Query Example 1: Query the renamed table through default catalog
    let df = ctx
        .sql("SELECT * FROM datafusion.public.companies_v2")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");

    // Query Example 2: Query the original table through DuckDB catalog
    let df = ctx
        .sql("SELECT * FROM duckdb.main.companies")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");

    // Query Example 3: Query the projects table in DuckDB
    let df = ctx
        .sql("SELECT * FROM duckdb.main.projects")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
