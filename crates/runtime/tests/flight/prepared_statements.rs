use std::sync::Arc;

use arrow::array::{Array as _, ArrayRef, Int64Array, RecordBatch};
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt as _;
use tonic::transport::Channel;

use crate::{flight::start_spice_test_app, init_tracing, utils::test_request_context};

#[tokio::test]
async fn test_basic_binding() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None).await?;

            let mut client = FlightSqlServiceClient::new(channel);
            let param_batch = create_param_batch(
                vec![("$1", arrow::datatypes::DataType::Int64, false)],
                vec![Arc::new(Int64Array::from(vec![41])) as Arc<dyn arrow::array::Array>],
            )?;

            let results =
                execute_prepared_statement(&mut client, "SELECT $1 + 1 AS the_answer", param_batch)
                    .await?;

            assert_eq!(results.len(), 1);
            let record = results.first().expect("1 record batch only");
            let (i, _) = record
                .schema()
                .column_with_name("the_answer")
                .expect("the_answer column");
            let column = record
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("the_answer is Int64Array");
            assert_eq!(column.len(), 1);
            assert_eq!(column.value(0), 42);
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
