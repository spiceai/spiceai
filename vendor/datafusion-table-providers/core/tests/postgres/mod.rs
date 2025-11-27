use crate::{arrow_record_batch_gen::*, docker::RunningContainer};
use arrow::{
    array::{Decimal128Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::collect;
use datafusion::{catalog::TableProviderFactory, logical_expr::dml::InsertOp};
use datafusion::{
    common::{Constraints, ToDFSchema},
    datasource::memory::MemorySourceConfig,
};
#[cfg(feature = "postgres-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;

use datafusion_table_providers::{
    postgres::{DynPostgresConnectionPool, PostgresTableProviderFactory},
    sql::sql_provider_datafusion::SqlTable,
    UnsupportedTypeAction,
};
use rstest::{fixture, rstest};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

mod common;
mod schema;

async fn arrow_postgres_round_trip(
    port: usize,
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    let factory = PostgresTableProviderFactory::new();
    let ctx = SessionContext::new();
    let cmd = CreateExternalTable {
        schema: Arc::new(arrow_record.schema().to_dfschema().expect("to df schema")),
        name: table_name.into(),
        location: "".to_string(),
        file_type: "".to_string(),
        table_partition_cols: vec![],
        if_not_exists: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: common::get_pg_params(port),
        constraints: Constraints::default(),
        column_defaults: HashMap::new(),
        temporary: false,
    };
    let table_provider = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let ctx = SessionContext::new();
    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![arrow_record.clone()]],
        arrow_record.schema(),
        None,
    )
    .expect("memory exec created");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), mem_exec, InsertOp::Append)
        .await
        .expect("insert plan created");

    let _ = collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert done");
    ctx.register_table(table_name, table_provider)
        .expect("Table should be registered");
    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");

    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Postgres returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    #[cfg(feature = "postgres-federation")]
    let casted_result =
        try_cast_to(record_batch[0].clone(), source_schema).expect("Failed to cast record batch");

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
    #[cfg(feature = "postgres-federation")]
    assert_eq!(arrow_record, casted_result);
}

struct ContainerManager {
    port: usize,
    claimed: bool,
    running_container: Option<RunningContainer>,
}

impl Drop for ContainerManager {
    fn drop(&mut self) {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(stop_container(self.running_container.take(), self.port));
    }
}

async fn stop_container(running_container: Option<RunningContainer>, port: usize) {
    println!("Stopping Postgres container on port {}", port);
    if let Some(running_container) = running_container {
        if let Err(e) = running_container.stop().await {
            tracing::error!("Error stopping container: {}", e);
        };
    }
}

#[fixture]
#[once]
fn container_manager() -> Mutex<ContainerManager> {
    Mutex::new(ContainerManager {
        port: crate::get_random_port(),
        claimed: false,
        running_container: None,
    })
}

async fn start_container(manager: &mut MutexGuard<'_, ContainerManager>) {
    let running_container =
        common::start_postgres_docker_container("postgres:latest", manager.port, None)
            .await
            .expect("Postgres container to start");

    manager.running_container = Some(running_container);

    tracing::debug!("Container started");
}

#[rstest]
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[case::date(get_arrow_date_record_batch(), "date")]
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[case::bytea_array(get_arrow_bytea_array_record_batch(), "bytea_array")]
#[test_log::test(tokio::test)]
async fn test_arrow_postgres_roundtrip(
    container_manager: &Mutex<ContainerManager>,
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&mut container_manager).await;
    }

    arrow_postgres_round_trip(
        container_manager.port,
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_arrow_postgres_one_way(container_manager: &Mutex<ContainerManager>) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&mut container_manager).await;
    }

    test_postgres_enum_type(container_manager.port).await;
    test_postgres_numeric_type(container_manager.port).await;
    test_postgres_jsonb_type(container_manager.port).await;
    test_postgres_nullability_constraints(container_manager.port).await;
}

async fn test_postgres_enum_type(port: usize) {
    let extra_stmt = Some("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');");
    let create_table_stmt = "
    CREATE TABLE person_mood (
    mood_status mood NOT NULL
    );";

    let insert_table_stmt = "
    INSERT INTO person_mood (mood_status) VALUES ('happy'), ('sad'), ('neutral');
    ";

    // Create expected record with nullable=false to match NOT NULL constraint
    let mut builder = arrow::array::StringDictionaryBuilder::<arrow::datatypes::Int8Type>::new();
    builder.append_value("happy");
    builder.append_value("sad");
    builder.append_value("neutral");
    let array: arrow::array::DictionaryArray<arrow::datatypes::Int8Type> = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "mood_status",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        false, // NOT NULL
    )]));

    let expected_record = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])
        .expect("Failed to create arrow dictionary array record batch");

    arrow_postgres_one_way(
        port,
        "person_mood",
        create_table_stmt,
        insert_table_stmt,
        extra_stmt,
        expected_record,
        UnsupportedTypeAction::default(),
    )
    .await;
}

async fn test_postgres_numeric_type(port: usize) {
    let extra_stmt = None;
    let create_table_stmt = "
    CREATE TABLE numeric_values (
    first_column NUMERIC,  -- No precision specified
    second_column NUMERIC  -- No precision specified
);";

    let insert_table_stmt = "
    INSERT INTO numeric_values (first_column, second_column) VALUES
(1.0917217805754313, 0.00000000000000000000),
(0.97824560830666753739, 1220.9175000000000000),
(1.0917217805754313, 52.9533333333333333);
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("first_column", DataType::Decimal128(38, 20), true),
        Field::new("second_column", DataType::Decimal128(38, 20), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                Decimal128Array::from(vec![
                    109172178057543130000i128,
                    97824560830666753739i128,
                    109172178057543130000i128,
                ])
                .with_precision_and_scale(38, 20)
                .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![
                    0i128,
                    122091750000000000000000i128,
                    5295333333333333330000i128,
                ])
                .with_precision_and_scale(38, 20)
                .unwrap(),
            ),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_postgres_one_way(
        port,
        "numeric_values",
        create_table_stmt,
        insert_table_stmt,
        extra_stmt,
        expected_record,
        UnsupportedTypeAction::default(),
    )
    .await;
}

async fn test_postgres_jsonb_type(port: usize) {
    let create_table_stmt = "
    CREATE TABLE jsonb_values (
        data JSONB
    );";

    let insert_table_stmt = r#"
    INSERT INTO jsonb_values (data) VALUES 
    ('{"name": "John", "age": 30}'),
    ('{"name": "Jane", "age": 25}'),
    ('[1, 2, 3]'),
    ('null'),
    ('{"nested": {"key": "value"}}');
    "#;

    let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)]));

    // Parse and re-serialize the JSON to ensure consistent ordering
    let expected_values = vec![
        serde_json::from_str::<Value>(r#"{"name":"John","age":30}"#)
            .unwrap()
            .to_string(),
        serde_json::from_str::<Value>(r#"{"name":"Jane","age":25}"#)
            .unwrap()
            .to_string(),
        serde_json::from_str::<Value>("[1,2,3]")
            .unwrap()
            .to_string(),
        serde_json::from_str::<Value>("null").unwrap().to_string(),
        serde_json::from_str::<Value>(r#"{"nested":{"key":"value"}}"#)
            .unwrap()
            .to_string(),
    ];

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(arrow::array::StringArray::from(expected_values))],
    )
    .expect("Failed to create arrow record batch");

    arrow_postgres_one_way(
        port,
        "jsonb_values",
        create_table_stmt,
        insert_table_stmt,
        None,
        expected_record,
        UnsupportedTypeAction::String,
    )
    .await;
}

async fn test_postgres_nullability_constraints(port: usize) {
    let create_table_stmt = "
        CREATE TABLE nullability_table (
            id INTEGER NOT NULL,
            name VARCHAR(50) NOT NULL,
            age INTEGER,
            email VARCHAR(100),
            score NUMERIC(5, 2) NOT NULL
        );
    ";
    let insert_table_stmt = "
        INSERT INTO nullability_table (id, name, age, email, score)
        VALUES
        (1, 'Alice', 30, 'alice@example.com', 95.50),
        (2, 'Bob', NULL, NULL, 87.25),
        (3, 'Charlie', 25, 'charlie@example.com', 92.00);
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("score", DataType::Decimal128(5, 2), false),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec![
                "Alice", "Bob", "Charlie",
            ])),
            Arc::new(arrow::array::Int32Array::from(vec![
                Some(30),
                None,
                Some(25),
            ])),
            Arc::new(arrow::array::StringArray::from(vec![
                Some("alice@example.com"),
                None,
                Some("charlie@example.com"),
            ])),
            Arc::new(
                Decimal128Array::from(vec![i128::from(9550), i128::from(8725), i128::from(9200)])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
        ],
    )
    .expect("Failed to create expected arrow record batch");

    arrow_postgres_one_way(
        port,
        "nullability_table",
        create_table_stmt,
        insert_table_stmt,
        None,
        expected_record,
        UnsupportedTypeAction::default(),
    )
    .await;
}

async fn arrow_postgres_one_way(
    port: usize,
    table_name: &str,
    create_table_stmt: &str,
    insert_table_stmt: &str,
    extra_stmt: Option<&str>,
    expected_record: RecordBatch,
    unsupported_type_action: UnsupportedTypeAction,
) {
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

    let pool = common::get_postgres_connection_pool(port)
        .await
        .expect("Postgres connection pool should be created")
        .with_unsupported_type_action(unsupported_type_action);

    let db_conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    if let Some(extra_stmt) = extra_stmt {
        let _ = db_conn
            .conn
            .execute(extra_stmt, &[])
            .await
            .expect("Statement should be created");
    }

    let _ = db_conn
        .conn
        .execute(create_table_stmt, &[])
        .await
        .expect("Postgres table should be created");

    let _ = db_conn
        .conn
        .execute(insert_table_stmt, &[])
        .await
        .expect("Postgres table data should be inserted");

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, table_name, None)
        .await
        .expect("Table should be created");
    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");
    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");

    assert_eq!(record_batch[0], expected_record);
}
