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
    second
        .next()
        .await
        .expect("row")
        .expect("second execution yields its row");
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
async fn single_request_proof_requires_eof_without_a_followed_page() {
    for followed in [false, true] {
        let completion = HttpFetchCompletion::new(1);
        let (mut response, progress) = tracked(&completion, 0, false, vec![Ok(batch())]);
        assert!(!completion.is_complete_single_request());
        response.next().await.expect("row").expect("response");
        if followed {
            progress.followed_page();
        }
        assert!(!completion.is_complete_single_request());
        assert!(response.next().await.is_none());
        assert!(completion.is_complete());
        assert_eq!(completion.is_complete_single_request(), !followed);
    }
    let completion = HttpFetchCompletion::new(2);
    for partition in 0..2 {
        let (mut response, _) = tracked(&completion, partition, false, vec![]);
        assert!(response.next().await.is_none());
    }
    assert!(completion.is_complete());
    assert!(!completion.is_complete_single_request());
}

#[tokio::test]
async fn actual_http_pagination_tracks_followed_pages_not_configuration() {
    use super::super::{HttpExec, HttpTableProvider, PaginationConfig};
    use datafusion::{
        datasource::TableProvider, execution::context::SessionContext, physical_plan::ExecutionPlan,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for pages in [1, 2] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await.expect("HTTP listener");
            let url = format!("http://{}", listener.local_addr().expect("address"));
            let server = tokio::spawn(async move {
                for page in 0..pages {
                    let (mut socket, _) = listener.accept().await.expect("request");
                    let mut request = Vec::new();
                    loop {
                        let mut buffer = [0; 1024];
                        let size = socket.read(&mut buffer).await.expect("request bytes");
                        assert!(size > 0);
                        request.extend_from_slice(&buffer[..size]);
                        assert!(request.len() < 8192);
                        if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8(request).expect("HTTP headers");
                    assert!(request.starts_with(if page == 0 {
                        "GET /items HTTP/1.1\r\n"
                    } else {
                        "GET /items?page=2 HTTP/1.1\r\n"
                    }));
                    let link = if page + 1 < pages {
                        "Link: </items?page=2>; rel=\"next\"\r\n"
                    } else {
                        ""
                    };
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Type: text/plain\r\n{link}Connection: close\r\n\r\nx").as_bytes())
                        .await.expect("HTTP response");
                }
            });
            let provider = HttpTableProvider::new(url.parse().expect("URL"), reqwest::Client::default(), "text".into(), true)
                .with_allowed_paths(["/items"]).expect("allowed path")
                .with_max_retries(0)
                .with_pagination(PaginationConfig::default()).expect("automatic pagination");
            let exec = HttpExec::new(provider.schema(), Arc::new(provider), vec![(Some("/items".into()), None, None, None)], None);
            let (exec, completion) = exec.for_cache_fetch();
            let mut response = exec.execute(0, SessionContext::new().task_ctx()).expect("stream");
            let mut rows = 0;
            while let Some(batch) = response.next().await {
                assert!(!completion.is_complete_single_request(), "EOF is required");
                rows += batch.expect("response batch").num_rows();
            }
            server.await.expect("origin stopped");
            assert_eq!(rows, pages);
            assert!(completion.is_complete());
            assert_eq!(completion.is_complete_single_request(), pages == 1);
        }
    }).await.expect("bounded HTTP execution");
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
    response
        .next()
        .await
        .expect("error item")
        .expect_err("failed fetch yields an error");
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
