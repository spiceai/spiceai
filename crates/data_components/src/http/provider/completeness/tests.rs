/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
};
use datafusion::{error::DataFusionError, physical_plan::stream::RecordBatchStreamAdapter};
use futures::{StreamExt, stream};

use super::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![7]))])
        .expect("HTTP response row")
}

fn tracked(
    completion: &HttpFetchCompletion,
    partition: usize,
    limited: bool,
    batches: Vec<Result<RecordBatch>>,
) -> (SendableRecordBatchStream, Progress) {
    let guard = completion.start(partition, limited);
    let progress = guard.progress();
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        stream::iter(batches),
    ));
    (track(stream, Some(guard)), progress)
}

#[tokio::test]
async fn only_eof_in_every_partition_completes() {
    let completion = HttpFetchCompletion::new(2);
    let (mut first, _) = tracked(&completion, 0, false, vec![Ok(batch())]);
    assert!(!completion.is_complete());
    assert_eq!(
        first
            .next()
            .await
            .expect("row")
            .expect("response")
            .num_rows(),
        1
    );
    assert!(!completion.is_complete(), "last batch is not EOF");
    assert!(first.next().await.is_none());
    assert!(
        !completion.is_complete(),
        "another partition has not started"
    );
    let (mut second, _) = tracked(&completion, 1, false, vec![Ok(batch())]);
    assert!(!completion.is_complete());
    assert!(second.next().await.expect("row").is_ok());
    assert!(!completion.is_complete());
    assert!(second.next().await.is_none());
    assert!(completion.is_complete());
    drop((first, second));
    assert!(
        completion.is_complete(),
        "dropping exhausted streams preserves completion"
    );
}

#[tokio::test]
async fn empty_response_requires_eof() {
    let completion = HttpFetchCompletion::new(1);
    let (mut response, _) = tracked(&completion, 0, false, vec![]);
    assert!(!completion.is_complete());
    assert!(response.next().await.is_none());
    assert!(completion.is_complete());
    assert!(!HttpFetchCompletion::new(0).is_complete());
}

#[tokio::test]
async fn limited_response_cannot_complete() {
    let completion = HttpFetchCompletion::new(1);
    let (mut response, _) = tracked(&completion, 0, true, vec![Ok(batch())]);
    while let Some(row) = response.next().await {
        row.expect("response");
    }
    assert!(!completion.is_complete());
}

#[tokio::test]
async fn source_truncation_cannot_complete() {
    let completion = HttpFetchCompletion::new(1);
    let (mut response, progress) = tracked(&completion, 0, false, vec![Ok(batch())]);
    response.next().await.expect("row").expect("response");
    progress.truncated();
    assert!(response.next().await.is_none());
    assert!(!completion.is_complete());
}

#[tokio::test]
async fn failed_stream_cannot_complete_after_eof() {
    let completion = HttpFetchCompletion::new(1);
    let (mut response, _) = tracked(
        &completion,
        0,
        false,
        vec![Err(DataFusionError::Execution("HTTP failure".into()))],
    );
    assert!(response.next().await.expect("error item").is_err());
    assert!(response.next().await.is_none());
    assert!(!completion.is_complete());
}

#[tokio::test]
async fn source_failure_signal_cannot_complete() {
    let completion = HttpFetchCompletion::new(1);
    let (mut response, progress) = tracked(&completion, 0, false, vec![]);
    progress.failed();
    assert!(response.next().await.is_none());
    assert!(!completion.is_complete());
}

#[tokio::test]
async fn cancellation_before_or_after_a_batch_cannot_complete() {
    for consume_batch in [false, true] {
        let completion = HttpFetchCompletion::new(1);
        let (mut response, _) = tracked(&completion, 0, false, vec![Ok(batch())]);
        if consume_batch {
            response.next().await.expect("row").expect("response");
        }
        drop(response);
        assert!(!completion.is_complete());
    }
}

#[tokio::test]
async fn sequential_and_concurrent_reuse_invalidate_completion() {
    for finish_first in [false, true] {
        let completion = HttpFetchCompletion::new(1);
        let (mut first, _) = tracked(&completion, 0, false, vec![]);
        if finish_first {
            assert!(first.next().await.is_none());
            assert!(completion.is_complete());
        }
        let (mut second, progress) = tracked(&completion, 0, false, vec![]);
        assert!(!completion.is_complete());
        assert!(first.next().await.is_none());
        assert!(second.next().await.is_none());
        progress.failed();
        drop((first, second));
        assert!(!completion.is_complete());
    }
}
