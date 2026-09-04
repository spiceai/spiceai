use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::{Stream, StreamExt};
use runtime_request_context::RequestContext;
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::Span;
use tracing_futures::Instrument;

#[derive(Debug)]
pub enum ManagedRuntimeError<E> {
    Future(E),
    DriverTaskEnded,
}

pub struct ManagedRecordBatchStream<M> {
    metadata: M,
    stream: SendableRecordBatchStream,
}

impl<M> ManagedRecordBatchStream<M> {
    fn new(metadata: M, stream: SendableRecordBatchStream) -> Self {
        Self { metadata, stream }
    }

    #[must_use]
    pub fn into_parts(self) -> (M, SendableRecordBatchStream) {
        (self.metadata, self.stream)
    }
}

/// Executes a future that produces a [`SendableRecordBatchStream`] on the provided Tokio runtime.
///
/// The future and the resulting stream are both driven by the supplied runtime handle. The resulting
/// stream can be consumed from the caller's runtime without blocking the managed runtime.
///
/// # Errors
///
/// Returns [`ManagedRuntimeError::JoinError`] if the spawned task panics, or
/// [`ManagedRuntimeError::ExecutionError`] if the future itself returns an error.
pub async fn run_record_batch_stream_on_runtime<Fut, M, E>(
    runtime_handle: Handle,
    request_context: Arc<RequestContext>,
    span: Span,
    future: Fut,
) -> Result<ManagedRecordBatchStream<M>, ManagedRuntimeError<E>>
where
    Fut: Future<Output = Result<(M, SendableRecordBatchStream), E>> + Send + 'static,
    M: Send + 'static,
    E: Send + 'static,
{
    let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(2);
    let (meta_tx, meta_rx) = oneshot::channel::<Result<(M, SchemaRef), E>>();

    let driver_request_context = Arc::clone(&request_context);
    let driver_span = span.clone();

    let driver_task = async move {
        // Scope the planning/execution future under the originating request
        // context so task-local reads (`RequestContext::current()`) resolve to
        // the request's context on this managed runtime task. The streaming
        // loop below is already scoped; without scoping the future too, code
        // that reads the task-local context during query planning/execution
        // (identity UDFs like `current_user_id()`/`current_org_id()`, task
        // history attribution, per-principal cache namespacing) falls back to
        // the empty/anonymous context.
        match Arc::clone(&driver_request_context)
            .scope(future.instrument(driver_span.clone()))
            .await
        {
            Ok((metadata, mut stream)) => {
                let schema = stream.schema();

                if meta_tx.send(Ok((metadata, schema))).is_err() {
                    return;
                }

                let stream_span = driver_span.clone();
                while let Some(batch) = Arc::clone(&driver_request_context)
                    .scope(stream.next().instrument(stream_span.clone()))
                    .await
                {
                    if batch_tx.send(batch).await.is_err() {
                        break;
                    }
                }
            }
            Err(err) => {
                let _ = meta_tx.send(Err(err));
            }
        }
    };

    let driver_handle = runtime_handle.spawn(driver_task.instrument(span.clone()));

    let (metadata, schema) = match meta_rx.await {
        Ok(Ok((metadata, schema))) => (metadata, schema),
        Ok(Err(err)) => return Err(ManagedRuntimeError::Future(err)),
        Err(_) => return Err(ManagedRuntimeError::DriverTaskEnded),
    };

    let driver_stream = RuntimeDriverStream::new(batch_rx, driver_handle);
    let adapter = RecordBatchStreamAdapter::new(schema, Box::pin(driver_stream));
    let stream: SendableRecordBatchStream = Box::pin(adapter);

    Ok(ManagedRecordBatchStream::new(metadata, stream))
}

/// Response stream for the offloaded query driver. Wraps the receiver of batches
/// produced on the managed runtime and owns the driver task's [`JoinHandle`] so
/// that buffered batches are drained first, then a panic — or an unexpected
/// cancellation (e.g. runtime shutdown) — of the driver task surfaces as a stream
/// error instead of a silent end-of-stream, which the caller cannot tell apart
/// from a query that legitimately matched no rows.
struct RuntimeDriverStream {
    receiver: ReceiverStream<Result<RecordBatch, DataFusionError>>,
    driver_handle: Option<JoinHandle<()>>,
}

impl RuntimeDriverStream {
    fn new(
        receiver: tokio::sync::mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
        driver_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            receiver: ReceiverStream::new(receiver),
            driver_handle: Some(driver_handle),
        }
    }
}

impl Stream for RuntimeDriverStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Drain already-produced batches first, so a driver failure surfaces only
        // after the caller has received everything the driver actually sent.
        if let Some(batch) = std::task::ready!(Pin::new(&mut this.receiver).poll_next(cx)) {
            return Poll::Ready(Some(batch));
        }

        // The channel is closed, so the driver task has ended. Its sender is dropped
        // as the task's future is dropped, which during a panic unwind happens before
        // the runtime publishes the task's outcome — so the handle can still be
        // pending here. Ending the stream at this point would turn a panicking query
        // into an empty success that no client can tell apart from "no rows matched",
        // so `ready!` yields `Pending` until the outcome is known.
        let Some(handle) = this.driver_handle.as_mut() else {
            return Poll::Ready(None);
        };
        let result = std::task::ready!(Future::poll(Pin::new(handle), cx));
        this.driver_handle = None;
        match result {
            Ok(()) => Poll::Ready(None),
            Err(err) if err.is_panic() => Poll::Ready(Some(Err(DataFusionError::Execution(
                format!("Query driver task panicked: {err}"),
            )))),
            Err(err) => Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                "Query driver task ended before completing: {err}"
            ))))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.receiver.size_hint()
    }
}

impl Drop for RuntimeDriverStream {
    fn drop(&mut self) {
        if let Some(handle) = self.driver_handle.take()
            && !handle.is_finished()
        {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use runtime_request_context::Protocol;
    use tokio::runtime::Builder;

    fn test_request_context() -> Arc<RequestContext> {
        Arc::new(RequestContext::builder(Protocol::Internal).build())
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime")
    }

    #[tokio::test]
    async fn run_record_batch_stream_on_runtime_streams_batches() {
        let runtime = test_runtime();
        let handle = runtime.handle().clone();
        let request_context = test_request_context();

        let managed = run_record_batch_stream_on_runtime(
            handle,
            Arc::clone(&request_context),
            Span::current(),
            async move {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )]));

                let columns: Vec<ArrayRef> =
                    vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef];
                let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
                    .expect("create record batch");

                let batches = vec![
                    Ok::<_, DataFusionError>(batch.clone()),
                    Ok::<_, DataFusionError>(batch),
                ];

                let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&schema),
                    futures::stream::iter(batches).boxed(),
                ));

                Ok::<_, DataFusionError>((42_u8, stream))
            },
        )
        .await
        .expect("managed stream");

        let (metadata, stream) = managed.into_parts();
        assert_eq!(metadata, 42_u8);

        let results: Vec<_> = stream.collect().await;
        assert_eq!(results.len(), 2);
        let first_batch = results
            .first()
            .expect("first batch result")
            .as_ref()
            .expect("batch ok");
        assert_eq!(first_batch.num_rows(), 3);
        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn run_record_batch_stream_on_runtime_propagates_future_errors() {
        let runtime = test_runtime();
        let handle = runtime.handle().clone();
        let request_context = test_request_context();

        let result = run_record_batch_stream_on_runtime(
            handle,
            Arc::clone(&request_context),
            Span::current(),
            async move { Err::<(u8, SendableRecordBatchStream), &'static str>("boom") },
        )
        .await;

        match result {
            Err(ManagedRuntimeError::Future(message)) => assert_eq!(message, "boom"),
            Ok(_) => panic!("expected managed runtime error"),
            Err(ManagedRuntimeError::DriverTaskEnded) => {
                panic!("expected future error, got driver termination")
            }
        }
        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn run_record_batch_stream_on_runtime_handles_driver_task_end() {
        let runtime = test_runtime();
        let handle = runtime.handle().clone();
        let request_context = test_request_context();

        let result = run_record_batch_stream_on_runtime::<_, u8, &'static str>(
            handle,
            request_context,
            Span::current(),
            async move {
                panic!("driver task panic");
            },
        )
        .await;

        match result {
            Err(ManagedRuntimeError::DriverTaskEnded) => (),
            Ok(_) => panic!("expected driver termination error"),
            Err(ManagedRuntimeError::Future(_)) => {
                panic!("expected driver termination error, got future error")
            }
        }
        runtime.shutdown_background();
    }

    /// A driver that panics *after* the stream has started must surface an error.
    ///
    /// The sender is dropped as the panicking task's future unwinds, which closes
    /// the batch channel before the runtime publishes the task's outcome. Sleeping
    /// between the two makes that window wide enough to hit on every run instead of
    /// roughly a third of them: the stream is woken by the channel close while the
    /// join handle is still pending, which is exactly the ordering that used to end
    /// the stream as an empty success.
    ///
    /// Regression test for #13876.
    #[tokio::test]
    async fn driver_panic_after_stream_start_is_an_error_not_an_empty_success() {
        let runtime = test_runtime();
        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(2);

        let driver_handle = runtime.spawn(async move {
            drop(batch_tx);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            panic!("driver task panic after stream start");
        });

        let results: Vec<_> = RuntimeDriverStream::new(batch_rx, driver_handle)
            .collect()
            .await;

        let [Err(err)] = results.as_slice() else {
            panic!("expected exactly one error item, got {results:?}");
        };
        assert!(
            err.to_string().contains("Query driver task panicked"),
            "unexpected error: {err}"
        );
        runtime.shutdown_background();
    }

    /// Batches the driver did produce are delivered before its panic surfaces, so
    /// the failure never silently truncates a partial result into a short success.
    #[tokio::test]
    async fn driver_panic_after_a_batch_yields_the_batch_then_the_error() {
        let runtime = test_runtime();
        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(2);

        let driver_handle = runtime.spawn(async move {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
            let batch = RecordBatch::try_new(schema, columns).expect("create record batch");
            batch_tx.send(Ok(batch)).await.expect("send batch");
            drop(batch_tx);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            panic!("driver task panic after one batch");
        });

        let results: Vec<_> = RuntimeDriverStream::new(batch_rx, driver_handle)
            .collect()
            .await;

        let [Ok(batch), Err(err)] = results.as_slice() else {
            panic!("expected one batch then one error, got {results:?}");
        };
        assert_eq!(batch.num_rows(), 3);
        assert!(
            err.to_string().contains("Query driver task panicked"),
            "unexpected error: {err}"
        );
        runtime.shutdown_background();
    }

    /// A driver cancelled out from under the stream (e.g. runtime shutdown) is the
    /// same silent-truncation hazard as a panic and must also surface as an error.
    #[tokio::test]
    async fn driver_cancelled_after_stream_start_is_an_error() {
        let runtime = test_runtime();
        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(2);

        let driver_handle = runtime.spawn(async move {
            drop(batch_tx);
            std::future::pending::<()>().await;
        });
        driver_handle.abort();

        let results: Vec<_> = RuntimeDriverStream::new(batch_rx, driver_handle)
            .collect()
            .await;

        let [Err(err)] = results.as_slice() else {
            panic!("expected exactly one error item, got {results:?}");
        };
        assert!(
            err.to_string()
                .contains("Query driver task ended before completing"),
            "unexpected error: {err}"
        );
        runtime.shutdown_background();
    }

    /// The preserved direction: a driver that finishes cleanly still ends the stream
    /// as a success once its batches are drained.
    #[tokio::test]
    async fn driver_completing_cleanly_still_ends_the_stream() {
        let runtime = test_runtime();
        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(2);

        let driver_handle = runtime.spawn(async move {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![7]))];
            let batch = RecordBatch::try_new(schema, columns).expect("create record batch");
            batch_tx.send(Ok(batch)).await.expect("send batch");
        });

        let results: Vec<_> = RuntimeDriverStream::new(batch_rx, driver_handle)
            .collect()
            .await;

        let [Ok(batch)] = results.as_slice() else {
            panic!("expected exactly one batch and no error, got {results:?}");
        };
        assert_eq!(batch.num_rows(), 1);
        runtime.shutdown_background();
    }
}
