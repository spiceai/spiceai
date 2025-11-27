use std::error::Error;

use datafusion::error::DataFusionError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum RetriableError {
    #[snafu(display("{source}"))]
    DataRetrievalError {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("{source}"))]
    DataWriteError {
        source: Box<dyn Error + Send + Sync>,
    },
}

#[must_use]
pub fn is_retriable_error(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(err) => err.downcast_ref::<RetriableError>().is_some(),
        DataFusionError::Context(_, err) => is_retriable_error(err.as_ref()),
        _ => false,
    }
}

/// Checks if the data retrieval error is NOT related to invalid input (e.g., SQL, plan creation, schema issues).
/// In this case, the error is wrapped as `RetriableError::DataRetrievalError`
/// so we can detect this error and retry later at a higher level
#[must_use]
pub fn check_and_mark_retriable_error(err: DataFusionError) -> DataFusionError {
    // don't wrap as retriable errors related to invalid SQL, schema, query plan, etc.
    if is_invalid_query_error(&err) {
        return err;
    }

    // already wrapped RetriableError
    if is_retriable_error(&err) {
        return err;
    }

    DataFusionError::External(Box::new(RetriableError::DataRetrievalError { source: err }))
}

// Wraps error as `RetriableError::DataWriteError` so we can detect this error and retry later at a higher level
#[must_use]
pub fn to_retriable_data_write_error<E>(error: E) -> DataFusionError
where
    E: Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(RetriableError::DataWriteError {
        source: error.into(),
    }))
}

fn is_invalid_query_error(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::Context(_, err) => is_invalid_query_error(err.as_ref()),
        DataFusionError::SQL(..) | DataFusionError::Plan(..) | DataFusionError::SchemaError(..) => {
            true
        }
        _ => false,
    }
}
