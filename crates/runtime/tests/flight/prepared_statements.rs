use arrow::array::{Int64Array, RecordBatch};
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt as _;

use crate::{flight::start_spice_test_app, init_tracing, utils::test_request_context};

#[tokio::test]
async fn test_basic_binding() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (_, df) = start_spice_test_app(None, None).await?;

            let result = df
                .query_builder("SELECT $1 + 1 AS the_answer")
                .parameters(vec![ScalarValue::Int64(Some(41))].into())
                .build()
                .run()
                .await?;

            let mut results: Vec<RecordBatch> =
                result.data.try_collect::<Vec<RecordBatch>>().await?;

            assert_eq!(results.len(), 1);

            let record = results.pop().expect("1 record batch only");
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
