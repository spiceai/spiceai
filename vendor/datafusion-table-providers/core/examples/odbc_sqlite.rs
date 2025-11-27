use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    odbc::ODBCTableFactory, sql::db_connection_pool::odbcpool::ODBCPool,
    util::secrets::to_secret_map,
};

/// This example demonstrates how to:
/// 1. Create a SQLite ODBC connection pool
/// 2. Create and use ODBCTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access SQLite ODBC table data
#[tokio::main]
async fn main() {
    // Create SQLite ODBC connection pool
    let params = to_secret_map(HashMap::from([(
        "connection_string".to_owned(),
        "driver=SQLite3;database=core/examples/sqlite_example.db;".to_owned(),
    )]));
    let odbc_pool =
        Arc::new(ODBCPool::new(params).expect("unable to create SQLite ODBC connection pool"));

    // Create SQLite ODBC table provider factory
    // Used to generate TableProvider instances that can read SQLite ODBC table data
    let table_factory = ODBCTableFactory::new(odbc_pool.clone());

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the SQLite ODBC "companies" table as "companies_v2"
    ctx.register_table(
        "companies_v2",
        table_factory
            .table_provider(TableReference::bare("companies"), None)
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
}
