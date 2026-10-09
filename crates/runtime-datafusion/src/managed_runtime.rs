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

/// When the managed runtime starts pulling batches from the stream the future produced.
#[derive(Debug, Clone, Copy)]
pub enum StreamStart {
    /// As soon as the future has produced the stream, so the first batches are ready by the
    /// time the caller asks for them.
    Immediately,
    /// Only once the caller first polls the returned stream. For a consumer that has to finish
    /// preparing before any batch is produced: a refresh's source stream computes the dataset's
    /// indexes as it is read, so it must not run ahead of the sink opening the index write
    /// window (#14619).
    OnFirstPoll,
}

/// Executes a future that produces a [`SendableRecordBatchStream`] on the provided Tokio runtime.
///
/// The future and the resulting stream are both driven by the supplied runtime handle. The resulting
/// stream can be consumed from the caller's runtime without blocking the managed runtime. `start`
/// decides whether the stream is pulled ahead of the caller's first poll.
///
/// # Errors
///
/// Returns [`ManagedRuntimeError::JoinError`] if the spawned task panics, or
/// [`ManagedRuntimeError::ExecutionError`] if the future itself returns an error.
pub async fn run_record_batch_stream_on_runtime<Fut, M, E>(
    runtime_handle: Handle,
    request_context: Arc<RequestContext>,
    span: Span,
    start: StreamStart,
    future: Fut,
) -> Result<ManagedRecordBatchStream<M>, ManagedRuntimeError<E>>
where
    Fut: Future<Output = Result<(M, SendableRecordBatchStream), E>> + Send + 'static,
    M: Send + 'static,
    E: Send + 'static,
{
    let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(2);
    let (meta_tx, meta_rx) = oneshot::channel::<Result<(M, SchemaRef), E>>();
    let (demand_tx, demand_rx) = match start {
        StreamStart::Immediately => (None, None),
        StreamStart::OnFirstPoll => {
            let (tx, rx) = oneshot::channel::<()>();
            (Some(tx), Some(rx))
        }
    };

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

                // An error means the caller dropped the stream without polling it.
                if let Some(demand_rx) = demand_rx
                    && demand_rx.await.is_err()
                {
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

    let driver_stream = RuntimeDriverStream::new(batch_rx, driver_handle, demand_tx);
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
    /// Released on the first poll, for a driver started with [`StreamStart::OnFirstPoll`].
    demand: Option<oneshot::Sender<()>>,
}

impl RuntimeDriverStream {
    fn new(
        receiver: tokio::sync::mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
        driver_handle: JoinHandle<()>,
        demand: Option<oneshot::Sender<()>>,
    ) -> Self {
        Self {
            receiver: ReceiverStream::new(receiver),
            driver_handle: Some(driver_handle),
            demand,
        }
    }
}

impl Stream for RuntimeDriverStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(demand) = this.demand.take() {
            // The driver only stops waiting once this send lands or the stream is dropped, so
            // a failed send means the driver has already ended and the handle reports why.
            let _ = demand.send(());
        }

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

    fn test_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(values))];
        RecordBatch::try_new(schema, columns).expect("create record batch")
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
            StreamStart::Immediately,
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
            StreamStart::Immediately,
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
            StreamStart::Immediately,
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
    /// The sender is dropped as the panicking task's future unwinds, which closes the
    /// batch channel before the runtime publishes the task's outcome. Two one-shot
    /// barriers pin that ordering exactly rather than racing it: the driver closes the
    /// channel and signals, and only after the stream has been polled once does it get
    /// released to panic. So at the first poll the channel is provably closed and the
    /// handle is provably pending — the ordering that used to end the stream as an empty
    /// success, and the one a sleep only reaches by luck.
    ///
    /// Regression test for #13876.
    #[tokio::test]
    async fn driver_panic_after_stream_start_is_an_error_not_an_empty_success() {
        let runtime = test_runtime();
        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(2);
        let (closed_tx, closed_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let driver_handle = runtime.spawn(async move {
            drop(batch_tx);
            let _ = closed_tx.send(());
            let _ = release_rx.await;
            panic!("driver task panic after stream start");
        });

        closed_rx.await.expect("driver signalled the channel close");

        let mut stream = RuntimeDriverStream::new(batch_rx, driver_handle, None);
        assert!(
            matches!(futures::poll!(stream.next()), Poll::Pending),
            "the stream ended while the driver's outcome was still unknown — \
             a panicking query would be reported to the client as an empty success"
        );

        release_tx
            .send(())
            .expect("release the driver into its panic");
        let results: Vec<_> = stream.collect().await;

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

        let (closed_tx, closed_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let driver_handle = runtime.spawn(async move {
            batch_tx
                .send(Ok(test_batch(vec![1, 2, 3])))
                .await
                .expect("send batch");
            drop(batch_tx);
            let _ = closed_tx.send(());
            let _ = release_rx.await;
            panic!("driver task panic after one batch");
        });

        closed_rx.await.expect("driver signalled the channel close");

        let mut stream = RuntimeDriverStream::new(batch_rx, driver_handle, None);
        let first = futures::poll!(stream.next());
        assert!(
            matches!(first, Poll::Ready(Some(Ok(_)))),
            "the buffered batch should be drained before the driver's outcome is known, got {first:?}"
        );
        assert!(
            matches!(futures::poll!(stream.next()), Poll::Pending),
            "the stream ended after its last buffered batch while the driver's outcome was \
             still unknown — a panicking query would be truncated into a short success"
        );

        release_tx
            .send(())
            .expect("release the driver into its panic");
        let results: Vec<_> = stream.collect().await;

        let [Err(err)] = results.as_slice() else {
            panic!("expected exactly one error item after the batch, got {results:?}");
        };
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

        let results: Vec<_> = RuntimeDriverStream::new(batch_rx, driver_handle, None)
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
            batch_tx
                .send(Ok(test_batch(vec![7])))
                .await
                .expect("send batch");
        });

        let results: Vec<_> = RuntimeDriverStream::new(batch_rx, driver_handle, None)
            .collect()
            .await;

        let [Ok(batch)] = results.as_slice() else {
            panic!("expected exactly one batch and no error, got {results:?}");
        };
        assert_eq!(batch.num_rows(), 1);
        runtime.shutdown_background();
    }

    /// Starts a managed stream over one batch, reporting on `polled` each time the managed
    /// runtime polls the source.
    async fn managed_stream_reporting_polls(
        runtime: &tokio::runtime::Runtime,
        start: StreamStart,
    ) -> (
        SendableRecordBatchStream,
        tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        let (polled_tx, polled_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let managed = run_record_batch_stream_on_runtime(
            runtime.handle().clone(),
            test_request_context(),
            Span::current(),
            start,
            async move {
                let batch = test_batch(vec![1, 2, 3]);
                let schema = batch.schema();
                let mut batches = vec![Ok::<_, DataFusionError>(batch)].into_iter();
                let source = futures::stream::poll_fn(move |_| {
                    let _ = polled_tx.send(());
                    Poll::Ready(batches.next())
                });
                let stream: SendableRecordBatchStream =
                    Box::pin(RecordBatchStreamAdapter::new(schema, source.boxed()));
                Ok::<_, DataFusionError>(((), stream))
            },
        )
        .await
        .expect("managed stream");
        (managed.into_parts().1, polled_rx)
    }

    /// A driver started with `OnFirstPoll` reads nothing from the source until the caller asks
    /// for a batch — the guarantee a refresh relies on so its source cannot index rows before
    /// the sink opens the index write window (#14619).
    #[tokio::test]
    async fn on_first_poll_driver_does_not_read_the_source_before_the_caller_polls() {
        let runtime = test_runtime();
        let (stream, mut polled) =
            managed_stream_reporting_polls(&runtime, StreamStart::OnFirstPoll).await;

        // Asserting an absence needs a bounded wait: an eager driver polls the source within
        // microseconds of handing back the stream, far inside this window.
        let early =
            tokio::time::timeout(std::time::Duration::from_millis(200), polled.recv()).await;
        assert!(
            early.is_err(),
            "the driver read the source before the caller polled the stream"
        );

        let results: Vec<_> = stream.collect().await;
        let [Ok(batch)] = results.as_slice() else {
            panic!("expected exactly one batch and no error, got {results:?}");
        };
        assert_eq!(batch.num_rows(), 3);
        assert!(
            polled.try_recv().is_ok(),
            "the source was never polled, yet the stream yielded its batch"
        );
        runtime.shutdown_background();
    }

    /// The `Immediately` driver keeps pulling ahead of the caller, as queries expect.
    #[tokio::test]
    async fn immediate_driver_reads_the_source_before_the_caller_polls() {
        let runtime = test_runtime();
        let (stream, mut polled) =
            managed_stream_reporting_polls(&runtime, StreamStart::Immediately).await;

        tokio::time::timeout(std::time::Duration::from_secs(5), polled.recv())
            .await
            .expect("the driver reads the source without waiting for the caller")
            .expect("the source reports its poll");

        let results: Vec<_> = stream.collect().await;
        assert_eq!(results.len(), 1, "unexpected results: {results:?}");
        runtime.shutdown_background();
    }

    /// Dropping an `OnFirstPoll` stream that was never polled ends the waiting driver rather
    /// than leaving it parked on the managed runtime.
    #[tokio::test]
    async fn on_first_poll_driver_ends_when_the_stream_is_dropped_unpolled() {
        let runtime = test_runtime();
        let (stream, mut polled) =
            managed_stream_reporting_polls(&runtime, StreamStart::OnFirstPoll).await;
        drop(stream);

        // The driver owns the source, so its channel closes once the driver task ends.
        let closed = tokio::time::timeout(std::time::Duration::from_secs(5), polled.recv())
            .await
            .expect("the driver ended after the stream was dropped");
        assert!(
            closed.is_none(),
            "the dropped stream's source was still read"
        );
        runtime.shutdown_background();
    }
}
