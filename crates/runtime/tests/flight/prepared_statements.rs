use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::sql::client::{FlightSqlServiceClient, PreparedStatement};
use futures::TryStreamExt as _;
use runtime_auth::{FlightBasicAuth, api_key::ApiKeyAuth};
use spicepod::component::runtime::ApiKey;
use tonic::transport::Channel;

use crate::{
    flight::{create_flight_client, start_spice_test_app, test_record_batch, write_record_batches},
    init_tracing,
    utils::test_request_context,
};

#[tokio::test]
async fn test_basic_binding() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;

            let mut client = FlightSqlServiceClient::new(channel);
            let param_batch = create_param_batch(
                vec![("$1", arrow::datatypes::DataType::Int64, false)],
                vec![Arc::new(Int64Array::from(vec![41])) as Arc<dyn arrow::array::Array>],
            )?;

            let results = execute_parameterized_query(
                &mut client,
                "SELECT $1 + 1 AS the_answer",
                param_batch,
            )
            .await?;

            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!("basic_binding_table_content", results_str);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_question_mark_placeholder() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;

            let mut client = FlightSqlServiceClient::new(channel);
            let param_batch = create_param_batch(
                vec![("$1", arrow::datatypes::DataType::Int64, false)],
                vec![Arc::new(Int64Array::from(vec![41])) as Arc<dyn arrow::array::Array>],
            )?;

            let results =
                execute_parameterized_query(&mut client, "SELECT ? + 1 AS the_answer", param_batch)
                    .await?;

            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!("question_mark_placeholder_table_content", results_str);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_multiple_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
                as Arc<dyn FlightBasicAuth + Send + Sync>;
            let (channel, _df) = start_spice_test_app(Some(auth), None, None).await?;

            let test_record_batch = test_record_batch()?;

            let batches = vec![test_record_batch.clone(), test_record_batch];

            let mut client = create_flight_client(channel.clone(), Some("valid"))?;
            write_record_batches(&mut client, batches).await?;

            let mut client = FlightSqlServiceClient::new(channel);
            client.handshake("", "valid").await?;
            let param_batch = create_param_batch(
                vec![
                    ("$1", arrow::datatypes::DataType::Int32, false),
                    ("$2", arrow::datatypes::DataType::Utf8, false),
                ],
                vec![
                    Arc::new(Int32Array::from(vec![1])) as Arc<dyn arrow::array::Array>,
                    Arc::new(StringArray::from(vec!["a"])) as Arc<dyn arrow::array::Array>,
                ],
            )?;

            let results = execute_parameterized_query(
                &mut client,
                "SELECT a, b FROM my_table WHERE a = $1 AND b = $2",
                param_batch,
            )
            .await?;

            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!("multiple_parameters_table_content", results_str);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_more_than_ten_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;

            let mut client = FlightSqlServiceClient::new(channel);

            // Create 15 parameters to test the ordering fix for >10 parameters
            // This tests the specific bug where $10, $11, $12 would come before $2, $3, etc.
            let param_fields = vec![
                ("$1", arrow::datatypes::DataType::Int64, false),
                ("$2", arrow::datatypes::DataType::Int64, false),
                ("$3", arrow::datatypes::DataType::Int64, false),
                ("$4", arrow::datatypes::DataType::Int64, false),
                ("$5", arrow::datatypes::DataType::Int64, false),
                ("$6", arrow::datatypes::DataType::Int64, false),
                ("$7", arrow::datatypes::DataType::Int64, false),
                ("$8", arrow::datatypes::DataType::Int64, false),
                ("$9", arrow::datatypes::DataType::Int64, false),
                ("$10", arrow::datatypes::DataType::Int64, false),
                ("$11", arrow::datatypes::DataType::Int64, false),
                ("$12", arrow::datatypes::DataType::Int64, false),
                ("$13", arrow::datatypes::DataType::Int64, false),
                ("$14", arrow::datatypes::DataType::Int64, false),
                ("$15", arrow::datatypes::DataType::Int64, false),
            ];

            let param_arrays = vec![
                Arc::new(Int64Array::from(vec![1])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![2])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![3])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![4])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![5])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![6])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![7])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![8])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![9])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![10])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![11])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![12])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![13])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![14])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![15])) as Arc<dyn arrow::array::Array>,
            ];

            let param_batch = create_param_batch(param_fields, param_arrays)?;

            // Query that uses parameters intentionally out of order to test the sorting fix
            // The bug would cause parameters to be bound in lexicographic order: $1, $10, $11, $12, $13, $14, $15, $2, $3, ...
            // With the fix, they should be bound in numeric order: $1, $2, $3, ..., $15
            let query = "SELECT $1, $10, $11, $12, $2, $3, $4, $5, $6, $7, $8, $9, $13, $14, $15";

            let parameterized_statement = get_prepared_statement(&mut client, query).await?;

            let parameter_schema = parameterized_statement.parameter_schema()?;
            assert_eq!(parameter_schema.fields().len(), 15);
            assert_eq!(parameter_schema.field(0).name(), "$1");
            assert_eq!(parameter_schema.field(1).name(), "$2");
            assert_eq!(parameter_schema.field(2).name(), "$3");
            assert_eq!(parameter_schema.field(3).name(), "$4");
            assert_eq!(parameter_schema.field(4).name(), "$5");
            assert_eq!(parameter_schema.field(5).name(), "$6");
            assert_eq!(parameter_schema.field(6).name(), "$7");
            assert_eq!(parameter_schema.field(7).name(), "$8");
            assert_eq!(parameter_schema.field(8).name(), "$9");
            assert_eq!(parameter_schema.field(9).name(), "$10");
            assert_eq!(parameter_schema.field(10).name(), "$11");
            assert_eq!(parameter_schema.field(11).name(), "$12");
            assert_eq!(parameter_schema.field(12).name(), "$13");
            assert_eq!(parameter_schema.field(13).name(), "$14");
            assert_eq!(parameter_schema.field(14).name(), "$15");

            let results =
                execute_prepared_statement(&mut client, parameterized_statement, param_batch)
                    .await?;

            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!("more_than_ten_parameters_table_content", results_str);

            // Verify the results are in the expected order
            if let Some(batch) = results.first() {
                // The result should be a single row with 15 columns in the order they appear in the SELECT
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.num_columns(), 15);

                // Verify the values are correct - they should match the parameter values
                // Column order in result: $1, $10, $11, $12, $2, $3, $4, $5, $6, $7, $8, $9, $13, $14, $15
                let expected_values = [1, 10, 11, 12, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15];

                for (i, expected) in expected_values.iter().enumerate() {
                    let column = batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("column should be Int64Array");
                    let actual = column.value(0);
                    assert_eq!(actual, *expected, "Column {i} should have value {expected}");
                }
            } else {
                panic!("Expected at least one result batch");
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_more_than_ten_question_mark_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;

            let mut client = FlightSqlServiceClient::new(channel);

            // Create 15 parameters to test the ordering fix for >10 parameters using ? placeholders
            // This tests that the parameter ordering fix works for both $n and ? style placeholders
            let param_fields = vec![
                ("$1", arrow::datatypes::DataType::Int64, false),
                ("$2", arrow::datatypes::DataType::Int64, false),
                ("$3", arrow::datatypes::DataType::Int64, false),
                ("$4", arrow::datatypes::DataType::Int64, false),
                ("$5", arrow::datatypes::DataType::Int64, false),
                ("$6", arrow::datatypes::DataType::Int64, false),
                ("$7", arrow::datatypes::DataType::Int64, false),
                ("$8", arrow::datatypes::DataType::Int64, false),
                ("$9", arrow::datatypes::DataType::Int64, false),
                ("$10", arrow::datatypes::DataType::Int64, false),
                ("$11", arrow::datatypes::DataType::Int64, false),
                ("$12", arrow::datatypes::DataType::Int64, false),
                ("$13", arrow::datatypes::DataType::Int64, false),
                ("$14", arrow::datatypes::DataType::Int64, false),
                ("$15", arrow::datatypes::DataType::Int64, false),
            ];

            let param_arrays = vec![
                Arc::new(Int64Array::from(vec![1])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![2])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![3])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![4])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![5])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![6])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![7])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![8])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![9])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![10])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![11])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![12])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![13])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![14])) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(vec![15])) as Arc<dyn arrow::array::Array>,
            ];

            let param_batch = create_param_batch(param_fields, param_arrays)?;

            // Query using ? placeholders - these get converted to $1, $2, ... internally
            // The ? placeholders should be converted in sequential order: ?, ?, ?, ... -> $1, $2, $3, ...
            // This tests that the parameter ordering fix works after the ? -> $n conversion
            let query = "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";

            let parameterized_statement = get_prepared_statement(&mut client, query).await?;

            let parameter_schema = parameterized_statement.parameter_schema()?;
            assert_eq!(parameter_schema.fields().len(), 15);
            assert_eq!(parameter_schema.field(0).name(), "$1");
            assert_eq!(parameter_schema.field(1).name(), "$2");
            assert_eq!(parameter_schema.field(2).name(), "$3");
            assert_eq!(parameter_schema.field(3).name(), "$4");
            assert_eq!(parameter_schema.field(4).name(), "$5");
            assert_eq!(parameter_schema.field(5).name(), "$6");
            assert_eq!(parameter_schema.field(6).name(), "$7");
            assert_eq!(parameter_schema.field(7).name(), "$8");
            assert_eq!(parameter_schema.field(8).name(), "$9");
            assert_eq!(parameter_schema.field(9).name(), "$10");
            assert_eq!(parameter_schema.field(10).name(), "$11");
            assert_eq!(parameter_schema.field(11).name(), "$12");
            assert_eq!(parameter_schema.field(12).name(), "$13");
            assert_eq!(parameter_schema.field(13).name(), "$14");
            assert_eq!(parameter_schema.field(14).name(), "$15");

            let results =
                execute_prepared_statement(&mut client, parameterized_statement, param_batch)
                    .await?;

            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!(
                "more_than_ten_question_mark_parameters_table_content",
                results_str
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_parameters_in_case_expressions() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;

            let mut client = FlightSqlServiceClient::new(channel);

            // Test CASE WHEN ? THEN 'foo' (searched CASE with parameter in condition)
            let query1 = "SELECT CASE WHEN ? THEN 'foo' ELSE 'bar' END AS result";
            let prepared_stmt1 = get_prepared_statement(&mut client, query1).await?;

            let parameter_schema1 = prepared_stmt1.parameter_schema()?;
            assert_eq!(parameter_schema1.fields().len(), 1);
            assert_eq!(parameter_schema1.field(0).name(), "$1");
            assert_eq!(
                parameter_schema1.field(0).data_type(),
                &arrow::datatypes::DataType::Boolean
            );

            let param_batch1 = create_param_batch(
                vec![("$1", arrow::datatypes::DataType::Boolean, false)],
                vec![Arc::new(BooleanArray::from(vec![true])) as Arc<dyn arrow::array::Array>],
            )?;

            let results1 =
                execute_prepared_statement(&mut client, prepared_stmt1, param_batch1).await?;

            let results1_str =
                arrow::util::pretty::pretty_format_batches(&results1).expect("pretty batches");
            insta::assert_snapshot!("case_when_parameter_table_content", results1_str);

            // Test CASE 'foo' WHEN ? THEN 'bar' (simple CASE with parameter in WHEN)
            let query2 = "SELECT CASE 'foo' WHEN ? THEN 'bar' ELSE 'baz' END AS result";
            let prepared_stmt2 = get_prepared_statement(&mut client, query2).await?;

            let parameter_schema2 = prepared_stmt2.parameter_schema()?;
            assert_eq!(parameter_schema2.fields().len(), 1);
            assert_eq!(parameter_schema2.field(0).name(), "$1");
            assert_eq!(
                parameter_schema2.field(0).data_type(),
                &arrow::datatypes::DataType::Utf8
            );

            let param_batch2 = create_param_batch(
                vec![("$1", arrow::datatypes::DataType::Utf8, false)],
                vec![Arc::new(StringArray::from(vec!["foo"])) as Arc<dyn arrow::array::Array>],
            )?;

            let results2 =
                execute_prepared_statement(&mut client, prepared_stmt2, param_batch2).await?;

            let results2_str =
                arrow::util::pretty::pretty_format_batches(&results2).expect("pretty batches");
            insta::assert_snapshot!("case_simple_parameter_table_content", results2_str);

            // Test with false condition to ensure ELSE clause works
            let query3 = "SELECT CASE WHEN ? THEN 'foo' ELSE 'bar' END AS result";
            let prepared_stmt3 = get_prepared_statement(&mut client, query3).await?;

            let parameter_schema3 = prepared_stmt3.parameter_schema()?;
            assert_eq!(parameter_schema3.fields().len(), 1);
            assert_eq!(parameter_schema3.field(0).name(), "$1");
            assert_eq!(
                parameter_schema3.field(0).data_type(),
                &arrow::datatypes::DataType::Boolean
            );

            let param_batch3 = create_param_batch(
                vec![("$1", arrow::datatypes::DataType::Boolean, false)],
                vec![Arc::new(BooleanArray::from(vec![false])) as Arc<dyn arrow::array::Array>],
            )?;

            let results3 =
                execute_prepared_statement(&mut client, prepared_stmt3, param_batch3).await?;

            let results3_str =
                arrow::util::pretty::pretty_format_batches(&results3).expect("pretty batches");
            insta::assert_snapshot!("case_when_parameter_false_table_content", results3_str);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_binder_error_specificity() -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightSqlServiceClient::new(channel);

            // Write a statement with two parameter bindings
            let stmt = get_prepared_statement(&mut client, "select ?, ?").await?;

            // ...but only provide one parameter
            let param_batch = create_param_batch(
                vec![("$1", arrow::datatypes::DataType::Int32, false)],
                vec![Arc::new(Int32Array::from(vec![1])) as Arc<dyn arrow::array::Array>],
            )?;

            let result = execute_prepared_statement(&mut client, stmt, param_batch).await;

            assert!(
                result.is_err(),
                "There are two parameter placeholders, but only one binding"
            );

            // Check that the error specifically uses the InvalidArgument code vs generic "Internal"
            let result_err_message = result.expect_err("Must be error").to_string();
            assert!(result_err_message.contains("code: InvalidArgument"));
            assert!(result_err_message.contains("No value found for placeholder with id $2"));

            Ok(())
        })
        .await
}

async fn get_prepared_statement(
    client: &mut FlightSqlServiceClient<Channel>,
    query: &str,
) -> Result<PreparedStatement<Channel>, anyhow::Error> {
    Ok(client.prepare(query.to_string(), None).await?)
}

async fn execute_prepared_statement(
    client: &mut FlightSqlServiceClient<Channel>,
    mut prepared_stmt: PreparedStatement<Channel>,
    parameters: RecordBatch,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    prepared_stmt.set_parameters(parameters)?;
    let flight_info = prepared_stmt.execute().await?;

    let ticket = flight_info
        .endpoint
        .first()
        .ok_or_else(|| anyhow::anyhow!("No endpoint in FlightInfo"))?
        .ticket
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No ticket in endpoint"))?;

    let stream = client.do_get(ticket.clone()).await?;
    let results: Vec<RecordBatch> = stream.try_collect().await?;

    Ok(results)
}

async fn execute_parameterized_query(
    client: &mut FlightSqlServiceClient<Channel>,
    query: &str,
    parameters: RecordBatch,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let prepared_stmt = get_prepared_statement(client, query).await?;

    execute_prepared_statement(client, prepared_stmt, parameters).await
}

fn create_param_batch(
    fields: Vec<(&str, arrow::datatypes::DataType, bool)>,
    arrays: Vec<ArrayRef>,
) -> Result<RecordBatch, anyhow::Error> {
    let schema = arrow::datatypes::Schema::new(
        fields
            .into_iter()
            .map(|(name, datatype, nullable)| {
                arrow::datatypes::Field::new(name, datatype, nullable)
            })
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(Arc::new(schema), arrays).map_err(Into::into)
}
