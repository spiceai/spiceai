use std::{sync::Arc, time::Duration};

use datafusion::{prelude::SessionContext, sql::TableReference};
use datafusion_table_providers::{
    common::DatabaseCatalogProvider,
    sql::db_connection_pool::{sqlitepool::SqliteConnectionPoolFactory, Mode},
    sqlite::SqliteTableFactory,
};

/// This example demonstrates how to:
/// 1. Create a SQLite connection pool
/// 2. Create and use SqliteTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access SQLite table data
#[tokio::main]
async fn main() {
    // Create SQLite connection pool
    // - arg1: SQLite database file path
    // - arg2: Database mode (file mode)
    // - arg3: Connection timeout duration
    let sqlite_pool = Arc::new(
        SqliteConnectionPoolFactory::new(
            "core/examples/sqlite_example.db",
            Mode::File,
            Duration::from_millis(5000),
        )
        .build()
        .await
        .expect("failed to create sqlite connection pool"),
    );

    // Create SQLite table provider factory
    // Used to generate TableProvider instances that can read SQLite table data
    let table_factory = SqliteTableFactory::new(sqlite_pool.clone());

    // Create database catalog provider
    // This allows us to access tables through catalog structure (catalog.schema.table)
    let catalog_provider = DatabaseCatalogProvider::try_new(sqlite_pool).await.unwrap();

    // Create DataFusion session context
    let ctx = SessionContext::new();
    // Register SQLite catalog, making it accessible via the "sqlite" name
    ctx.register_catalog("sqlite", Arc::new(catalog_provider));

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the SQLite "companies" table as "companies_v2"
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

    // Query Example 2: Query the original table through SQLite catalog
    let df = ctx
        .sql("SELECT * FROM sqlite.main.companies")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");

    // Query Example 3: Query the projects table in SQLite
    let df = ctx
        .sql("SELECT * FROM sqlite.main.projects")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
