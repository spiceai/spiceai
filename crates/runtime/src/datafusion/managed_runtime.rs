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
        match future.instrument(driver_span.clone()).await {
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

struct RuntimeDriverStream {
    receiver: ReceiverStream<Result<RecordBatch, DataFusionError>>,
    driver_handle: Option<JoinHandle<()>>,
    driver_error: Option<DataFusionError>,
}

impl RuntimeDriverStream {
    fn new(
        receiver: tokio::sync::mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
        driver_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            receiver: ReceiverStream::new(receiver),
            driver_handle: Some(driver_handle),
            driver_error: None,
        }
    }
}

impl Stream for RuntimeDriverStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(handle) = this.driver_handle.as_mut() {
            match Future::poll(Pin::new(handle), cx) {
                Poll::Ready(Ok(())) => {
                    this.driver_handle = None;
                }
                Poll::Ready(Err(err)) => {
                    this.driver_handle = None;
                    if err.is_panic() {
                        this.driver_error = Some(DataFusionError::Execution(format!(
                            "Query driver task panicked: {err}"
                        )));
                    } else if !err.is_cancelled() {
                        this.driver_error = Some(DataFusionError::Execution(format!(
                            "Query driver task failed: {err}"
                        )));
                    }
                }
                Poll::Pending => {}
            }
        }

        if let Some(err) = this.driver_error.take() {
            return Poll::Ready(Some(Err(err)));
        }

        Pin::new(&mut this.receiver).poll_next(cx)
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
}
