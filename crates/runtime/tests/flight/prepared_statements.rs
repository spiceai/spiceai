#[expect(clippy::similar_names)]
mod flight_prepared_statements {

    use std::sync::Arc;

    use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_flight::sql::client::{FlightSqlServiceClient, PreparedStatement};
    use futures::TryStreamExt as _;
    use runtime_auth::{FlightBasicAuth, api_key::ApiKeyAuth};
    use spicepod::component::runtime::ApiKey;
    use tonic::transport::Channel;

    use crate::{
        flight::{
            create_flight_client, start_spice_test_app, test_record_batch, write_record_batches,
        },
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

                let results = execute_parameterized_query(
                    &mut client,
                    "SELECT ? + 1 AS the_answer",
                    param_batch,
                )
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
                let query =
                    "SELECT $1, $10, $11, $12, $2, $3, $4, $5, $6, $7, $8, $9, $13, $14, $15";

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

    /// Test SQL PREPARE/EXECUTE/DEALLOCATE statements through Arrow Flight SQL.
    ///
    /// `DataFusion` supports PREPARE/EXECUTE/DEALLOCATE SQL statements (see <https://datafusion.apache.org/user-guide/sql/prepared_statements.html>).
    /// These statements modify session state by storing/retrieving prepared statements in the `SessionContext`.
    ///
    /// This test validates that Flight SQL session tracking works correctly, allowing prepared statements
    /// created with SQL PREPARE in one request to be available in subsequent EXECUTE requests.
    #[tokio::test]
    async fn test_sql_prepare_execute_statements() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));

        test_request_context()
            .scope(async {
                let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
                    as Arc<dyn FlightBasicAuth + Send + Sync>;
                let (channel, _df) = start_spice_test_app(Some(auth), None, None).await?;

                // Create and populate test table
                let test_record_batch = test_record_batch()?;
                let batches = vec![test_record_batch.clone(), test_record_batch];

                let mut write_client = create_flight_client(channel.clone(), Some("valid"))?;
                write_record_batches(&mut write_client, batches).await?;

                // Create a client and perform authentication handshake.
                // After handshake, the client should maintain authentication for subsequent requests.
                let mut client = FlightSqlServiceClient::new(channel);
                client.handshake("", "valid").await?;

                // Test 1: PREPARE a SQL statement with parameters
                // SQL PREPARE creates a prepared statement but doesn't return data
                let prepare_sql =
                    "PREPARE my_query AS SELECT a, b FROM my_table WHERE a = $1 AND b = $2";
                let mut prepare_stmt = client.prepare(prepare_sql.to_string(), None).await?;
                let prepare_info = prepare_stmt.execute().await?;

                // PREPARE doesn't return data, but we need to fetch to trigger the server-side execution
                if let Some(endpoint) = prepare_info.endpoint.first()
                    && let Some(ticket) = &endpoint.ticket
                {
                    let prepare_stream = client.do_get(ticket.clone()).await?;
                    let _prepare_results: Vec<RecordBatch> = prepare_stream.try_collect().await?;
                }

                // Test 2: EXECUTE the prepared statement with parameters
                let execute_sql = "EXECUTE my_query(1, 'a')";
                let mut execute_stmt = client.prepare(execute_sql.to_string(), None).await?;
                let execute_flight_info = execute_stmt.execute().await?;

                let ticket = execute_flight_info
                    .endpoint
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("No endpoint in FlightInfo"))?
                    .ticket
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("No ticket in endpoint"))?;

                let stream = client.do_get(ticket.clone()).await?;
                let results: Vec<RecordBatch> = stream.try_collect().await?;

                // Verify we got the expected rows (a=1, b='a')
                // Since we wrote 2 duplicate batches, we expect 2 matching rows
                let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(total_rows, 2, "should return two rows total");

                // Check first row
                let first_batch = &results[0];
                assert!(first_batch.num_rows() > 0, "first batch should have rows");

                let a_col = first_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("column 'a' should be Int32Array");
                assert_eq!(a_col.value(0), 1, "column 'a' should be 1");

                let b_col = first_batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("column 'b' should be StringArray");
                assert_eq!(b_col.value(0), "a", "column 'b' should be 'a'");

                let results_str =
                    arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
                insta::assert_snapshot!("sql_prepare_execute_first_execution", results_str);

                // Test 3: EXECUTE the same prepared statement with different parameters
                let execute2_sql = "EXECUTE my_query(2, 'b')";
                let mut execute2_stmt = client.prepare(execute2_sql.to_string(), None).await?;
                let execute2_flight_info = execute2_stmt.execute().await?;

                let ticket2 = execute2_flight_info
                    .endpoint
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("No endpoint in FlightInfo"))?
                    .ticket
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("No ticket in endpoint"))?;

                let stream2 = client.do_get(ticket2.clone()).await?;
                let results2: Vec<RecordBatch> = stream2.try_collect().await?;

                // Verify we got the expected rows (a=2, b='b')
                // Since we wrote 2 duplicate batches, we expect 2 matching rows
                let total_rows2: usize = results2.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(total_rows2, 2, "should return two rows total");

                // Check first row
                let first_batch2 = &results2[0];
                assert!(first_batch2.num_rows() > 0, "first batch should have rows");

                let a2_col = first_batch2
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("column 'a' should be Int32Array");
                assert_eq!(a2_col.value(0), 2, "column 'a' should be 2");

                let b2_col = first_batch2
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("column 'b' should be StringArray");
                assert_eq!(b2_col.value(0), "b", "column 'b' should be 'b'");

                let results2_str =
                    arrow::util::pretty::pretty_format_batches(&results2).expect("pretty batches");
                insta::assert_snapshot!("sql_prepare_execute_second_execution", results2_str);

                // Test 4: DEALLOCATE the prepared statement
                let deallocate_sql = "DEALLOCATE my_query";
                let mut deallocate_stmt = client.prepare(deallocate_sql.to_string(), None).await?;
                let _deallocate_flight_info = deallocate_stmt.execute().await?;

                // DEALLOCATE completes successfully (returns empty result)
                // No need to check further - if it didn't work, execute() would have failed

                Ok(())
            })
            .await
    }

    /// Test null parameter handling in prepared statements.
    ///
    /// Verifies that null values can be correctly bound and used in prepared statements.
    #[tokio::test]
    async fn test_null_parameter_handling() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));

        test_request_context()
            .scope(async {
                let (channel, _df) = start_spice_test_app(None, None, None).await?;

                let mut client = FlightSqlServiceClient::new(channel);

                // Test with a nullable parameter using COALESCE to handle nulls
                let query = "SELECT COALESCE($1, 'default_value') AS result";

                let param_batch = {
                    let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                        "$1",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    )]);
                    // Create a null string value
                    let null_array = arrow::array::StringArray::from(vec![None as Option<&str>]);
                    RecordBatch::try_new(
                        Arc::new(schema),
                        vec![Arc::new(null_array) as Arc<dyn arrow::array::Array>],
                    )?
                };

                let results = execute_parameterized_query(&mut client, query, param_batch).await?;

                let results_str =
                    arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
                insta::assert_snapshot!("null_parameter_coalesce", results_str);

                // Verify the result is 'default_value' since we passed null
                if let Some(batch) = results.first() {
                    assert_eq!(batch.num_rows(), 1);
                    // DataFusion may return Utf8 or LargeUtf8 depending on the query
                    let col = batch.column(0);
                    let value = if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        arr.value(0).to_string()
                    } else if let Some(arr) = col
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                    {
                        arr.value(0).to_string()
                    } else {
                        panic!(
                            "Expected Utf8 or LargeUtf8 array, got {:?}",
                            col.data_type()
                        );
                    };
                    assert_eq!(value, "default_value");
                }

                Ok(())
            })
            .await
    }

    /// Test prepared statement with no parameters.
    ///
    /// Verifies that prepared statements work correctly when no parameters are bound.
    #[tokio::test]
    async fn test_no_parameters() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));

        test_request_context()
            .scope(async {
                let (channel, _df) = start_spice_test_app(None, None, None).await?;

                let mut client = FlightSqlServiceClient::new(channel);

                // Query with no parameters
                let query = "SELECT 1 + 1 AS result";

                let prepared_stmt = get_prepared_statement(&mut client, query).await?;

                // Verify parameter schema is empty
                let param_schema = prepared_stmt.parameter_schema()?;
                assert_eq!(
                    param_schema.fields().len(),
                    0,
                    "Query without parameters should have empty parameter schema"
                );

                // Execute with empty parameters
                let empty_batch = {
                    let schema = arrow::datatypes::Schema::empty();
                    RecordBatch::new_empty(Arc::new(schema))
                };

                let results =
                    execute_prepared_statement(&mut client, prepared_stmt, empty_batch).await?;

                let results_str =
                    arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
                insta::assert_snapshot!("no_parameters_query", results_str);

                // Verify the result is 2
                if let Some(batch) = results.first() {
                    assert_eq!(batch.num_rows(), 1);
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("column should be Int64Array");
                    assert_eq!(col.value(0), 2);
                }

                Ok(())
            })
            .await
    }

    /// Test prepared statement re-execution with different parameters.
    ///
    /// Verifies that the same prepared statement can be executed multiple times
    /// with different parameter values.
    #[tokio::test]
    async fn test_prepared_statement_reexecution() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));

        test_request_context()
            .scope(async {
                let (channel, _df) = start_spice_test_app(None, None, None).await?;

                let mut client = FlightSqlServiceClient::new(channel);

                let query = "SELECT $1 * $2 AS product";

                // First execution: 3 * 7 = 21
                let param_batch1 = create_param_batch(
                    vec![
                        ("$1", arrow::datatypes::DataType::Int64, false),
                        ("$2", arrow::datatypes::DataType::Int64, false),
                    ],
                    vec![
                        Arc::new(Int64Array::from(vec![3])) as Arc<dyn arrow::array::Array>,
                        Arc::new(Int64Array::from(vec![7])) as Arc<dyn arrow::array::Array>,
                    ],
                )?;

                let results1 =
                    execute_parameterized_query(&mut client, query, param_batch1).await?;

                if let Some(batch) = results1.first() {
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("column should be Int64Array");
                    assert_eq!(col.value(0), 21, "3 * 7 should equal 21");
                }

                // Second execution with different values: 5 * 11 = 55
                let param_batch2 = create_param_batch(
                    vec![
                        ("$1", arrow::datatypes::DataType::Int64, false),
                        ("$2", arrow::datatypes::DataType::Int64, false),
                    ],
                    vec![
                        Arc::new(Int64Array::from(vec![5])) as Arc<dyn arrow::array::Array>,
                        Arc::new(Int64Array::from(vec![11])) as Arc<dyn arrow::array::Array>,
                    ],
                )?;

                let results2 =
                    execute_parameterized_query(&mut client, query, param_batch2).await?;

                if let Some(batch) = results2.first() {
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("column should be Int64Array");
                    assert_eq!(col.value(0), 55, "5 * 11 should equal 55");
                }

                // Third execution: -2 * 6 = -12
                let param_batch3 = create_param_batch(
                    vec![
                        ("$1", arrow::datatypes::DataType::Int64, false),
                        ("$2", arrow::datatypes::DataType::Int64, false),
                    ],
                    vec![
                        Arc::new(Int64Array::from(vec![-2])) as Arc<dyn arrow::array::Array>,
                        Arc::new(Int64Array::from(vec![6])) as Arc<dyn arrow::array::Array>,
                    ],
                )?;

                let results3 =
                    execute_parameterized_query(&mut client, query, param_batch3).await?;

                if let Some(batch) = results3.first() {
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("column should be Int64Array");
                    assert_eq!(col.value(0), -12, "-2 * 6 should equal -12");
                }

                Ok(())
            })
            .await
    }

    /// Test session isolation for SQL PREPARE/EXECUTE statements.
    ///
    /// Verifies that prepared statements are scoped to individual sessions and
    /// are not visible across sessions.
    ///
    /// Each handshake creates a unique session with its own `SessionContext`. The session ID
    /// is returned in the Authorization Bearer token, ensuring subsequent requests from
    /// the same client use the same session.
    #[tokio::test]
    async fn test_session_isolation() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));

        test_request_context()
            .scope(async {
                let auth = Arc::new(ApiKeyAuth::new(vec![
                    ApiKey::parse_str("user1:rw"),
                    ApiKey::parse_str("user2:rw"),
                ])) as Arc<dyn FlightBasicAuth + Send + Sync>;
                let (channel, _df) = start_spice_test_app(Some(auth), None, None).await?;

                // Session 1: Create a prepared statement
                let mut client1 = FlightSqlServiceClient::new(channel.clone());
                client1.handshake("", "user1").await?;

                let prepare_sql = "PREPARE session1_query AS SELECT 'from_session_1' AS source";
                let mut prepare_stmt1 = client1.prepare(prepare_sql.to_string(), None).await?;
                let prepare_info1 = prepare_stmt1.execute().await?;

                // Consume the prepare result
                if let Some(endpoint) = prepare_info1.endpoint.first()
                    && let Some(ticket) = &endpoint.ticket
                {
                    let prepare_stream = client1.do_get(ticket.clone()).await?;
                    let _: Vec<RecordBatch> = prepare_stream.try_collect().await?;
                }

                // Session 1 should be able to execute its own prepared statement
                let execute1_sql = "EXECUTE session1_query";
                let mut execute1_stmt = client1.prepare(execute1_sql.to_string(), None).await?;
                let execute1_info = execute1_stmt.execute().await?;

                let ticket1 = execute1_info
                    .endpoint
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("No endpoint"))?
                    .ticket
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("No ticket"))?;

                let stream1 = client1.do_get(ticket1.clone()).await?;
                let results1: Vec<RecordBatch> = stream1.try_collect().await?;

                // Verify session 1 gets correct result
                let results1_str =
                    arrow::util::pretty::pretty_format_batches(&results1).expect("pretty batches");
                insta::assert_snapshot!("session_isolation_session1_result", results1_str);

                // Session 2: Should NOT be able to access session 1's prepared statement
                let mut client2 = FlightSqlServiceClient::new(channel);
                client2.handshake("", "user2").await?;

                // Attempting to execute session1_query from session2 should fail
                // Note: prepare() and execute() succeed because they just create/return metadata.
                // The actual execution happens during do_get(), which is when we verify
                // the prepared statement exists in the session.
                let execute2_sql = "EXECUTE session1_query";
                let mut execute2_stmt = client2.prepare(execute2_sql.to_string(), None).await?;
                let execute2_info = execute2_stmt.execute().await?;

                // Get the ticket and try to fetch results - this is when the actual execution happens
                let ticket2 = execute2_info
                    .endpoint
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("No endpoint"))?
                    .ticket
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("No ticket"))?;

                // This should fail because session2 doesn't have the prepared statement
                let stream2_result = client2.do_get(ticket2.clone()).await;
                assert!(
                    stream2_result.is_err(),
                    "Session 2 should not be able to execute session 1's prepared statement"
                );

                let err_msg = stream2_result
                    .expect_err("Expected error")
                    .to_string()
                    .to_lowercase();
                assert!(
                    err_msg.contains("session1_query") || err_msg.contains("not found"),
                    "Error should mention the statement name or 'not found': {err_msg}",
                );

                Ok(())
            })
            .await
    }

    /// Test error handling when executing non-existent prepared statement.
    ///
    /// Verifies that attempting to execute a non-existent prepared statement returns
    /// a proper error.
    #[tokio::test]
    async fn test_execute_nonexistent_statement() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));

        test_request_context()
            .scope(async {
                let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
                    as Arc<dyn FlightBasicAuth + Send + Sync>;
                let (channel, _df) = start_spice_test_app(Some(auth), None, None).await?;

                let mut client = FlightSqlServiceClient::new(channel);
                client.handshake("", "valid").await?;

                // Try to execute a statement that was never prepared
                // Note: prepare() and execute() succeed because they just create/return metadata.
                // The actual execution happens during do_get(), which is when we verify
                // the prepared statement exists in the session.
                let execute_sql = "EXECUTE nonexistent_statement";
                let mut execute_stmt = client.prepare(execute_sql.to_string(), None).await?;
                let execute_info = execute_stmt.execute().await?;

                // Get the ticket and try to fetch results - this is when the actual execution happens
                let ticket = execute_info
                    .endpoint
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("No endpoint"))?
                    .ticket
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("No ticket"))?;

                // This should fail because the prepared statement doesn't exist
                let stream_result = client.do_get(ticket.clone()).await;
                assert!(
                    stream_result.is_err(),
                    "Executing non-existent prepared statement should fail"
                );

                let err_msg = stream_result
                    .expect_err("Expected error")
                    .to_string()
                    .to_lowercase();
                assert!(
                    err_msg.contains("nonexistent_statement") || err_msg.contains("not found"),
                    "Error should mention the statement name: {err_msg}",
                );

                Ok(())
            })
            .await
    }

    /// Test DEALLOCATE on non-existent statement.
    ///
    /// Verifies proper error handling when attempting to deallocate a statement that doesn't exist.
    #[tokio::test]
    async fn test_deallocate_nonexistent_statement() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));

        test_request_context()
            .scope(async {
                let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
                    as Arc<dyn FlightBasicAuth + Send + Sync>;
                let (channel, _df) = start_spice_test_app(Some(auth), None, None).await?;

                let mut client = FlightSqlServiceClient::new(channel);
                client.handshake("", "valid").await?;

                // Try to deallocate a statement that was never prepared
                // Note: prepare() and execute() succeed because they just create/return metadata.
                // The actual execution happens during do_get(), which is when we verify
                // the prepared statement exists in the session.
                let deallocate_sql = "DEALLOCATE nonexistent_statement";
                let mut deallocate_stmt = client.prepare(deallocate_sql.to_string(), None).await?;
                let deallocate_info = deallocate_stmt.execute().await?;

                // Get the ticket and try to fetch results - this is when the actual execution happens
                let ticket = deallocate_info
                    .endpoint
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("No endpoint"))?
                    .ticket
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("No ticket"))?;

                // DEALLOCATE on non-existent statement should fail
                let stream_result = client.do_get(ticket.clone()).await;
                assert!(
                    stream_result.is_err(),
                    "Deallocating non-existent prepared statement should fail"
                );

                let err_msg = stream_result
                    .expect_err("Expected error")
                    .to_string()
                    .to_lowercase();
                assert!(
                    err_msg.contains("nonexistent_statement") || err_msg.contains("not found"),
                    "Error should mention the statement name: {err_msg}",
                );

                Ok(())
            })
            .await
    }

    /// Test mixed parameter types.
    ///
    /// Verifies that prepared statements can handle a mix of different parameter types
    /// (int, string, boolean, float) in the same query.
    #[tokio::test]
    async fn test_mixed_parameter_types() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));

        test_request_context()
            .scope(async {
                use arrow::array::Float64Array;

                let (channel, _df) = start_spice_test_app(None, None, None).await?;

                let mut client = FlightSqlServiceClient::new(channel);

                // Query with multiple parameter types
                let query = "SELECT CAST($1 AS INTEGER) AS int_val, $2 AS str_val, $3 AS bool_val, CAST($4 AS DOUBLE) AS float_val";

                let param_batch = create_param_batch(
                    vec![
                        ("$1", arrow::datatypes::DataType::Int64, false),
                        ("$2", arrow::datatypes::DataType::Utf8, false),
                        ("$3", arrow::datatypes::DataType::Boolean, false),
                        ("$4", arrow::datatypes::DataType::Float64, false),
                    ],
                    vec![
                        Arc::new(Int64Array::from(vec![42])) as Arc<dyn arrow::array::Array>,
                        Arc::new(StringArray::from(vec!["hello"])) as Arc<dyn arrow::array::Array>,
                        Arc::new(BooleanArray::from(vec![true])) as Arc<dyn arrow::array::Array>,
                        Arc::new(Float64Array::from(vec![std::f64::consts::PI]))
                            as Arc<dyn arrow::array::Array>,
                    ],
                )?;

                let results = execute_parameterized_query(&mut client, query, param_batch).await?;

                let results_str =
                    arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
                insta::assert_snapshot!("mixed_parameter_types", results_str);

                // Verify the results
                if let Some(batch) = results.first() {
                    assert_eq!(batch.num_columns(), 4);
                    assert_eq!(batch.num_rows(), 1);

                    // Verify string column value (may be Utf8 or LargeUtf8)
                    let str_col = batch.column(1);
                    let str_value = if let Some(arr) = str_col.as_any().downcast_ref::<StringArray>()
                    {
                        arr.value(0).to_string()
                    } else if let Some(arr) = str_col
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                    {
                        arr.value(0).to_string()
                    } else {
                        panic!(
                            "str column should be Utf8 or LargeUtf8, got {:?}",
                            str_col.data_type()
                        );
                    };
                    assert_eq!(str_value, "hello");

                    let bool_col = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("bool column should be BooleanArray");
                    assert!(bool_col.value(0));
                }

                Ok(())
            })
            .await
    }
}
