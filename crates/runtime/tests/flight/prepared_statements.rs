use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::sql::client::FlightSqlServiceClient;
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

            let results =
                execute_prepared_statement(&mut client, "SELECT $1 + 1 AS the_answer", param_batch)
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
                execute_prepared_statement(&mut client, "SELECT ? + 1 AS the_answer", param_batch)
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

            let results = execute_prepared_statement(
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

async fn execute_prepared_statement(
    client: &mut FlightSqlServiceClient<Channel>,
    query: &str,
    parameters: RecordBatch,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut prepared_stmt = client.prepare(query.to_string(), None).await?;

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
