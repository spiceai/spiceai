/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use crate::{
    flight::{
        RepeatingStream, create_flight_client, large_test_record_batch, start_spice_test_app,
        test_record_batch, write_record_batches,
    },
    init_tracing,
    utils::test_request_context,
};
use arrow::array::RecordBatch;
use arrow_flight::{
    FlightDescriptor, PutResult, encode::FlightDataEncoderBuilder, error::FlightError,
};

use futures::stream::{self, TryStreamExt};
use governor::Quota;
use runtime::flight::RateLimits;
use runtime_auth::{FlightBasicAuth, api_key::ApiKeyAuth};
use spicepod::component::runtime::ApiKey;
use tokio::time::{sleep, timeout};
use tokio_stream::StreamExt;

#[tokio::test]
async fn test_flight_do_put_basic() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
                as Arc<dyn FlightBasicAuth + Send + Sync>;

            let (channel, df) = start_spice_test_app(Some(auth), None, None).await?;

            let mut client = create_flight_client(channel, Some("valid"))?;

            let test_record_batch = test_record_batch()?;

            let response = write_record_batches(
                &mut client,
                // simulate two record batches / two FlightData messages
                vec![test_record_batch.clone(), test_record_batch].into_iter(),
            )
            .await?;

            let response_str = format!("{response:?}");
            insta::assert_snapshot!("do_put_basic_reponse", response_str);

            let query = df
                .query_builder("SELECT * from my_table")
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!("do_put_basic_table_content", results_str);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_do_put_stream_error() -> Result<(), Box<dyn std::error::Error>> {
    let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
        as Arc<dyn FlightBasicAuth + Send + Sync>;

    let (channel, df) = start_spice_test_app(Some(auth), None, None).await?;

    let mut client = create_flight_client(channel, Some("valid"))?;

    let test_record_batch = test_record_batch()?;

    let repeating_stream = RepeatingStream {
        batch: test_record_batch.clone(),
    };

    // simulate a sending the same record batch every 250ms
    let delayed_stream = repeating_stream.then(|batch| async move {
        sleep(Duration::from_millis(250)).await;
        batch
    });

    let flight_descriptor = FlightDescriptor::new_path(vec!["my_table".to_string()]);
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(delayed_stream);

    // simulate unexpected stream termination after 3 seconds
    let result = timeout(Duration::from_secs(3), async {
        let result: Result<Vec<PutResult>, FlightError> = client
            .do_put(flight_data_stream)
            .await
            .expect("to get result stream")
            .try_collect()
            .await;
        result
    })
    .await;

    assert!(
        result.is_err(),
        "Expected an error but got a successful result"
    );

    // Verify that no data was written to the table
    let query = df
        .query_builder("SELECT * from my_table")
        .build()
        .run()
        .await?;

    let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
    let results_str = arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
    insta::assert_snapshot!("stream_error_table_content", results_str);

    Ok(())
}

#[tokio::test]
async fn test_flight_do_put_no_auth() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope_retry(3, || async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;

            let mut client = create_flight_client(channel, None)?;

            let response =
                write_record_batches(&mut client, vec![test_record_batch()?].into_iter()).await;

            assert!(
                response.is_err(),
                "Expected an error but got a successful result"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_flight_do_put_ro_key() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid")]))
                as Arc<dyn FlightBasicAuth + Send + Sync>;

            let (channel, _df) = start_spice_test_app(Some(auth), None, None).await?;

            let mut client = create_flight_client(channel, Some("valid"))?;

            let response =
                write_record_batches(&mut client, vec![test_record_batch()?].into_iter()).await;

            assert!(
                response.is_err(),
                "Expected an error but got a successful result"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_flight_do_put_rate_limit() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
                as Arc<dyn FlightBasicAuth + Send + Sync>;

            let write_quota = Quota::with_period(Duration::from_secs(10))
                .expect("to create quota")
                .allow_burst(NonZeroU32::new(5).expect("should convert 5 to NonZeroU32"));

            let (channel, df) = start_spice_test_app(
                Some(auth),
                Some(RateLimits::new().with_flight_write_limit(write_quota)),
                None,
            )
            .await?;

            let mut client = create_flight_client(channel, Some("valid"))?;

            let test_record_batch = test_record_batch()?;

            // simulate 5 requests to reach rate limit
            for _ in 1..=5 {
                let _ =
                    write_record_batches(&mut client, vec![test_record_batch.clone()].into_iter())
                        .await?;
            }

            // rate limit error is expected next
            assert!(
                write_record_batches(&mut client, vec![test_record_batch.clone()].into_iter())
                    .await
                    .is_err(),
                "Expected an error but got a successful result"
            );

            // wait for the rate limit reset and perform another request attempt
            sleep(Duration::from_secs(10)).await;

            let _ = write_record_batches(&mut client, vec![test_record_batch.clone()].into_iter())
                .await?;

            let query = df
                .query_builder("SELECT * from my_table")
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!("rate_limit_table_content", results_str);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_flight_do_put_max_rows_allowed() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
                as Arc<dyn FlightBasicAuth + Send + Sync>;

            let (channel, df) = start_spice_test_app(Some(auth), None, None).await?;

            let mut client = create_flight_client(channel, Some("valid"))?;

            assert!(
                // Simulate a normal batch, followed by a batch that exceeds the allowed number of rows, and then another normal batch.
                write_record_batches(
                    &mut client,
                    vec![
                        test_record_batch()?,
                        large_test_record_batch()?,
                        test_record_batch()?
                    ]
                    .into_iter()
                )
                .await
                .is_err(),
                "Expected an error but got a successful result"
            );

            let query = df
                .query_builder("SELECT * from my_table")
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!("max_rows_allowed_table_content", results_str);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_do_put_read_timeout() -> Result<(), Box<dyn std::error::Error>> {
    let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid:rw")]))
        as Arc<dyn FlightBasicAuth + Send + Sync>;

    let (channel, df) = start_spice_test_app(Some(auth), None, None).await?;

    let mut client = create_flight_client(channel, Some("valid"))?;

    let record_batch_1 = test_record_batch()?;
    let record_batch_2 = record_batch_1.clone();
    let record_batch_3 = record_batch_1.clone();

    let first_batch =
        stream::once(async { Ok(record_batch_1) as Result<RecordBatch, FlightError> });
    // batch with 40s delay
    let second_batch = stream::once(async {
        sleep(Duration::from_secs(40)).await;
        Ok(record_batch_2) as Result<RecordBatch, FlightError>
    });
    let third_batch =
        stream::once(async { Ok(record_batch_3) as Result<RecordBatch, FlightError> });

    let flight_descriptor = FlightDescriptor::new_path(vec!["my_table".to_string()]);
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(first_batch.chain(second_batch).chain(third_batch));

    let result: Result<Vec<PutResult>, FlightError> = client
        .do_put(flight_data_stream)
        .await
        .expect("to get result stream")
        .try_collect()
        .await;

    assert!(
        result.is_err(),
        "Expected an error but got a successful result"
    );

    // Verify that no data was written to the table
    let query = df
        .query_builder("SELECT * from my_table")
        .build()
        .run()
        .await?;

    let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
    let results_str = arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
    insta::assert_snapshot!("read_timeout_table_content", results_str);

    Ok(())
}
