/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Ends a refresh whose source stopped sending mid-stream.
//!
//! A connection the network stops carrying (a partition, a dropped route, a
//! peer that vanished without a FIN) gives a reading client no error: a client
//! that is only reading sends nothing, so TCP never notices, and without a
//! keepalive or a read timeout the read waits for ever. The MySQL connector sets
//! neither. A full refresh caught that way never finishes and never fails, and
//! it blocks every refresh queued behind it, while the runtime stays Ready.
//!
//! Once the source has sent its first batch, a refresh that waits longer than
//! [`SOURCE_STALL_TIMEOUT`] for the next one fails with a retriable error, so it
//! is logged, counted in `dataset_acceleration_refresh_errors`, and retried on
//! a new connection. The wait before
//! the first batch is not bounded: a source may take a long time to plan and
//! start a query.

use std::time::Duration;

use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_table_providers::util::retriable_error::check_and_mark_retriable_error;
use futures::{StreamExt, stream};
use snafu::Snafu;

/// How long a refresh waits for the next batch once its source has started
/// sending. A source that streams leaves gaps of seconds between batches; a
/// wait this long is treated as a lost connection.
pub(crate) const SOURCE_STALL_TIMEOUT: Duration = Duration::from_mins(10);

/// The error a stalled refresh fails with.
#[derive(Debug, Snafu)]
#[snafu(display(
    "The source of dataset '{dataset}' stopped sending data mid-refresh: no data for {}s after it started sending, so the connection is treated as lost and the refresh is retried",
    SOURCE_STALL_TIMEOUT.as_secs()
))]
struct SourceStalled {
    dataset: String,
}

/// `source`, failing with a retriable [`SourceStalled`] error when it goes
/// [`SOURCE_STALL_TIMEOUT`] without a batch after its first one.
pub(crate) fn with_stall_timeout(
    source: SendableRecordBatchStream,
    dataset: String,
) -> SendableRecordBatchStream {
    let schema = source.schema();
    let guarded = stream::unfold(Some((source, false, dataset)), |state| async move {
        let (mut source, started, dataset) = state?;
        let next = if started {
            let Ok(next) = tokio::time::timeout(SOURCE_STALL_TIMEOUT, source.next()).await else {
                let error = DataFusionError::External(Box::new(SourceStalled { dataset }));
                // Ends the stream, dropping the stalled source.
                return Some((Err(check_and_mark_retriable_error(error)), None));
            };
            next
        } else {
            source.next().await
        };
        next.map(|item| (item, Some((source, true, dataset))))
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, guarded))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_table_providers::util::retriable_error::is_retriable_error;

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .expect("batch")
    }

    /// A source that sends `batches`, then goes silent without ending: what a
    /// reading client sees on a connection the network stopped carrying.
    fn silent_after(batches: usize) -> SendableRecordBatchStream {
        let schema = batch().schema();
        let sent = stream::iter((0..batches).map(|_| Ok(batch())));
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            sent.chain(stream::pending()),
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_silent_after_its_first_batch_fails_retriably() {
        let timeout = SOURCE_STALL_TIMEOUT;
        let mut guarded = with_stall_timeout(silent_after(2), "big".to_string());
        assert!(guarded.next().await.expect("first").is_ok());
        assert!(guarded.next().await.expect("second").is_ok());
        let silent_from = tokio::time::Instant::now();
        let error = tokio::time::timeout(timeout * 2, guarded.next())
            .await
            .expect("a stalled source must not be waited on for ever")
            .expect("a stalled source yields an error")
            .expect_err("not a batch");
        assert_eq!(silent_from.elapsed(), timeout, "fails at the timeout");
        assert!(
            is_retriable_error(&error),
            "retried, not permanent: {error}"
        );
        assert!(
            error.to_string().contains(
                "The source of dataset 'big' stopped sending data mid-refresh: no data for 600s after it started sending, so the connection is treated as lost and the refresh is retried"
            ),
            "{error}"
        );
        assert!(
            guarded.next().await.is_none(),
            "the stream ends after the error"
        );
    }

    /// The timeout bounds each gap between batches, not the whole refresh.
    #[tokio::test(start_paused = true)]
    async fn a_source_slower_than_the_timeout_in_total_is_passed_through() {
        let slow = stream::unfold(0, |sent| async move {
            if sent == 3 {
                return None;
            }
            tokio::time::sleep(SOURCE_STALL_TIMEOUT - Duration::from_secs(1)).await;
            Some((Ok(batch()), sent + 1))
        });
        let source: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(batch().schema(), slow));
        let started = tokio::time::Instant::now();
        let rows: Vec<_> = with_stall_timeout(source, "big".to_string())
            .collect()
            .await;
        assert_eq!(
            started.elapsed(),
            3 * (SOURCE_STALL_TIMEOUT - Duration::from_secs(1))
        );
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(Result::is_ok));
    }

    #[tokio::test(start_paused = true)]
    async fn the_wait_for_the_first_batch_is_not_bounded() {
        let mut guarded = with_stall_timeout(silent_after(0), "big".to_string());
        let first = tokio::time::timeout(SOURCE_STALL_TIMEOUT * 6, guarded.next()).await;
        assert!(
            first.is_err(),
            "no error before the source has sent anything"
        );
    }
}
