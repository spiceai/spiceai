use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, mysql::MySQLTableFactory,
    sql::db_connection_pool::mysqlpool::MySQLConnectionPool, util::secrets::to_secret_map,
};

/// This example demonstrates how to:
/// 1. Create a MySQL connection pool
/// 2. Create and use MySQLTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access MySQL table data
///
/// Prerequisites:
/// Start a MySQL server using Docker:
/// ```bash
/// docker run --name mysql -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=mysql_db -p 3306:3306 -d mysql:9.0
/// # Wait for the MySQL server to start
/// sleep 30
///
/// # Create a table and insert sample data
/// docker exec -i mysql mysql -uroot -ppassword mysql_db <<EOF
/// CREATE TABLE companies (
///    id INT PRIMARY KEY,
///    name VARCHAR(100)
/// );
///
/// INSERT INTO companies (id, name) VALUES (1, 'Acme Corporation');
/// EOF
/// ```
#[tokio::main]
async fn main() {
    // Create MySQL connection parameters
    // Including connection string and SSL mode settings
    let mysql_params = to_secret_map(HashMap::from([
        (
            "connection_string".to_string(),
            "mysql://root:password@localhost:3306/mysql_db".to_string(),
        ),
        ("sslmode".to_string(), "disabled".to_string()),
    ]));

    // Create MySQL connection pool
    let mysql_pool = Arc::new(
        MySQLConnectionPool::new(mysql_params)
            .await
            .expect("unable to create MySQL connection pool"),
    );

    // Create MySQL table provider factory
    // Used to generate TableProvider instances that can read MySQL table data
    let table_factory = MySQLTableFactory::new(mysql_pool.clone());

    // Create database catalog provider
    // This allows us to access tables through catalog structure (catalog.schema.table)
    let catalog = DatabaseCatalogProvider::try_new(mysql_pool).await.unwrap();

    // Create DataFusion session context
    let ctx = SessionContext::new();
    // Register MySQL catalog, making it accessible via the "mysql" name
    ctx.register_catalog("mysql", Arc::new(catalog));

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the MySQL "companies" table as "companies_v2"
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

    // Query Example 2: Query the original table through MySQL catalog
    let df = ctx
        .sql("SELECT * FROM mysql.mysql_db.companies")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
