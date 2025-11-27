use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, postgres::PostgresTableFactory,
    sql::db_connection_pool::postgrespool::PostgresConnectionPool, util::secrets::to_secret_map,
};
use std::collections::HashMap;
use std::sync::Arc;

/// This example demonstrates how to:
/// 1. Create a PostgreSQL connection pool
/// 2. Create and use PostgresTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access PostgreSQL table data
///
/// Prerequisites:
/// Start a PostgreSQL server using Docker:
/// ```bash
/// docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5432:5432 -d postgres:16-alpine
/// # Wait for the Postgres server to start
/// sleep 30
///
/// # Create a table and insert sample data
/// docker exec -i postgres psql -U postgres test_db <<EOF
/// CREATE TABLE companies (
///    id SERIAL PRIMARY KEY,
///    name VARCHAR(100)
/// );
///
/// INSERT INTO companies (name) VALUES ('Example Corp');
/// EOF
/// ```
#[tokio::main]
async fn main() {
    // Create PostgreSQL connection parameters
    let postgres_params = to_secret_map(HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("user".to_string(), "postgres".to_string()),
        ("db".to_string(), "postgres_db".to_string()),
        ("pass".to_string(), "password".to_string()),
        ("port".to_string(), "5432".to_string()),
        ("sslmode".to_string(), "disable".to_string()),
    ]));

    // Create PostgreSQL connection pool
    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(postgres_params)
            .await
            .expect("unable to create PostgreSQL connection pool"),
    );

    // Create PostgreSQL table provider factory
    // Used to generate TableProvider instances that can read PostgreSQL table data
    let table_factory = PostgresTableFactory::new(postgres_pool.clone());

    // Create database catalog provider
    // This allows us to access tables through catalog structure (catalog.schema.table)
    let catalog = DatabaseCatalogProvider::try_new(postgres_pool)
        .await
        .unwrap();

    // Create DataFusion session context
    let ctx = SessionContext::new();
    // Register PostgreSQL catalog, making it accessible via the "postgres" name
    ctx.register_catalog("postgres", Arc::new(catalog));

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the PostgreSQL "companies" table as "companies_v2"
    ctx.register_table(
        "companies_v2",
        table_factory
            .table_provider(TableReference::bare("companies"))
            .await
            .expect("failed to register table provider"),
    )
    .expect("failed to register table");

    let companies_view = table_factory
        .table_provider(TableReference::bare("companies_view"))
        .await
        .expect("to create table provider for view");

    let companies_materialized_view = table_factory
        .table_provider(TableReference::bare("companies_materialized_view"))
        .await
        .expect("to create table provider for materialized view");

    ctx.register_table("companies_view", companies_view)
        .expect("to register view");
    ctx.register_table("companies_materialized_view", companies_materialized_view)
        .expect("to register materialized view");

    // Query Example 1: Query the renamed table through default catalog
    let df = ctx
        .sql("SELECT * FROM datafusion.public.companies_v2")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");

    // Query Example 2: Query the original table through PostgreSQL catalog
    let df = ctx
        .sql("SELECT * FROM postgres.public.companies")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");

    let df = ctx
        .sql("SELECT * FROM companies_view")
        .await
        .expect("select from view failed");

    df.show().await.expect("show failed");

    let df = ctx
        .sql("SELECT * FROM companies_materialized_view")
        .await
        .expect("select from materialized view failed");

    df.show().await.expect("show failed");
}
