/*
Copyright 2024 The Spice.ai OSS Authors

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

use datafusion::error::DataFusionError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum RetriableError {
    #[snafu(display("{source}"))]
    DataRetrievalError {
        source: datafusion::error::DataFusionError,
    },
}

#[must_use]
pub fn is_retriable_error(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(err) => return err.downcast_ref::<RetriableError>().is_some(),
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

fn is_invalid_query_error(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::Context(_, err) => is_invalid_query_error(err.as_ref()),
        DataFusionError::SQL(..) | DataFusionError::Plan(..) | DataFusionError::SchemaError(..) => {
            true
        }
        _ => false,
    }
}
