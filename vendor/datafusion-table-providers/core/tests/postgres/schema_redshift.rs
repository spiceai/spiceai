use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::Constraints;
use datafusion::common::ToDFSchema;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use std::collections::HashMap;
use std::sync::Arc;

use crate::postgres::common;
use crate::postgres::PostgresTableProviderFactory;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::util::secrets::to_secret_map;

const COMPLEX_TABLE_SQL: &str = include_str!("scripts/complex_table_redshift.sql");

fn get_schema() -> SchemaRef {
    let fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("large_id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int16, true),
        Field::new("height", DataType::Float64, true),
        Field::new("is_active", DataType::Boolean, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("data", DataType::Binary, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ];

    Arc::new(Schema::new(fields))
}

#[tokio::test]
async fn test_postgres_redshift_schema_inference() {
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(
        "docker-pgredshift:latest",
        port,
        Some("ghcr.io/hearthsim"),
    )
    .await
    .expect("Postgres container to start");

    let factory = PostgresTableProviderFactory::new();
    let ctx = SessionContext::new();
    let table_name = "test_table";
    let schema = get_schema();

    let cmd = CreateExternalTable {
        schema: schema.to_dfschema_ref().expect("to df schema"),
        name: table_name.into(),
        location: "".to_string(),
        file_type: "".to_string(),
        table_partition_cols: vec![],
        if_not_exists: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: common::get_pg_params(port),
        constraints: Constraints::new_unverified(vec![]),
        column_defaults: HashMap::new(),
        temporary: false,
    };
    let _ = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(to_secret_map(common::get_pg_params(port)))
            .await
            .expect("unable to create Postgres connection pool"),
    );
    let table_factory = PostgresTableFactory::new(postgres_pool);
    let table_provider = table_factory
        .table_provider(TableReference::bare(table_name))
        .await
        .expect("to create table provider");

    assert_eq!(table_provider.schema(), get_schema());

    // Tear down
    container
        .remove()
        .await
        .expect("to stop postgres container");
}

#[tokio::test]
async fn test_postgres_redshift_schema_inference_complex_types() {
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(
        "docker-pgredshift:latest",
        port,
        Some("ghcr.io/hearthsim"),
    )
    .await
    .expect("Postgres container to start");

    let table_name = "example_table";

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(to_secret_map(common::get_pg_params(port)))
            .await
            .expect("unable to create Postgres connection pool"),
    );

    let pg_conn = postgres_pool
        .connect_direct()
        .await
        .expect("to connect to postgres");
    for cmd in COMPLEX_TABLE_SQL.split(";") {
        pg_conn
            .conn
            .execute(cmd, &[])
            .await
            .expect("to create table");
    }

    let table_factory = PostgresTableFactory::new(postgres_pool);
    let table_provider = table_factory
        .table_provider(TableReference::bare(table_name))
        .await
        .expect("to create table provider");

    let pretty_schema = format!("{:#?}", table_provider.schema());
    insta::assert_snapshot!(pretty_schema);

    // Tear down
    container
        .remove()
        .await
        .expect("to stop postgres container");
}

#[tokio::test]
async fn test_postgres_redshift_view_schema_inference() {
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(
        "docker-pgredshift:latest",
        port,
        Some("ghcr.io/hearthsim"),
    )
    .await
    .expect("Postgres container to start");

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(to_secret_map(common::get_pg_params(port)))
            .await
            .expect("unable to create Postgres connection pool"),
    );
    let pg_conn = postgres_pool
        .connect_direct()
        .await
        .expect("to connect to postgres");

    for cmd in COMPLEX_TABLE_SQL.split(";") {
        if cmd.trim().is_empty() {
            continue;
        }
        pg_conn
            .conn
            .execute(cmd, &[])
            .await
            .expect("executing SQL from complex_table_redshift.sql");
    }

    let table_factory = PostgresTableFactory::new(postgres_pool.clone());
    let table_provider = table_factory
        .table_provider(TableReference::bare("example_view"))
        .await
        .expect("to create table provider for view");

    let pretty_schema = format!("{:#?}", table_provider.schema());
    insta::assert_snapshot!(pretty_schema);

    // Tear down
    container
        .remove()
        .await
        .expect("to stop postgres container");
}

#[tokio::test]
async fn test_postgres_redshift_materialized_view_schema_inference() {
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(
        "docker-pgredshift:latest",
        port,
        Some("ghcr.io/hearthsim"),
    )
    .await
    .expect("Postgres container to start");

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(to_secret_map(common::get_pg_params(port)))
            .await
            .expect("unable to create Postgres connection pool"),
    );
    let pg_conn = postgres_pool
        .connect_direct()
        .await
        .expect("to connect to postgres");

    for cmd in COMPLEX_TABLE_SQL.split(";") {
        if cmd.trim().is_empty() {
            continue;
        }
        pg_conn
            .conn
            .execute(cmd, &[])
            .await
            .expect("executing SQL from complex_table_redshift.sql");
    }

    let table_factory = PostgresTableFactory::new(postgres_pool);
    let table_provider = table_factory
        .table_provider(TableReference::bare("example_materialized_view"))
        .await
        .expect("to create table provider for materialized view");

    let pretty_schema = format!("{:#?}", table_provider.schema());
    insta::assert_snapshot!(pretty_schema);

    // Tear down
    container
        .remove()
        .await
        .expect("to stop postgres container");
}
