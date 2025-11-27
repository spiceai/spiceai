use std::sync::Arc;

use datafusion::{
    catalog::TableProviderFactory,
    execution::{runtime_env::RuntimeEnv, session_state::SessionStateBuilder},
    prelude::SessionContext,
};
use datafusion_table_providers::duckdb::DuckDBTableProviderFactory;
use duckdb::AccessMode;

/// This example demonstrates how to register the DuckDBTableProviderFactory into DataFusion so that
/// DuckDB-backed tables can be created at runtime.
#[tokio::main]
async fn main() {
    let duckdb = Arc::new(DuckDBTableProviderFactory::new(AccessMode::ReadWrite));

    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .with_table_factories(
            vec![(
                "DUCKDB".to_string(),
                duckdb as Arc<dyn TableProviderFactory>,
            )]
            .into_iter()
            .collect(),
        )
        .build();

    let ctx = SessionContext::new_with_state(state);

    // TODO: We could rework the DuckDB table factory to also respect LOCATION
    ctx.sql(
        "CREATE EXTERNAL TABLE person (id INT, name STRING) 
         STORED AS duckdb 
         LOCATION 'not_used' 
         OPTIONS ('duckdb.mode' 'file', 'duckdb.open' 'examples/duckdb_external_table_person.db');",
    )
    .await
    .expect("create table failed");

    // Inserting works!
    ctx.sql("INSERT INTO person VALUES (1, 'Alice')")
        .await
        .expect("insert plan failed")
        .collect()
        .await
        .expect("insert failed");

    let df = ctx
        .sql("SELECT * FROM person LIMIT 10")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
