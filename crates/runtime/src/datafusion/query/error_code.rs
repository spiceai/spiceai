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

use datafusion::error::DataFusionError;

#[derive(Clone)]
pub enum ErrorCode {
    SyntaxError,
    QueryPlanningError,
    QueryExecutionError,
    InternalError,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::SyntaxError => write!(f, "SyntaxError"),
            ErrorCode::QueryPlanningError => write!(f, "QueryPlanningError"),
            ErrorCode::QueryExecutionError => write!(f, "QueryExecutionError"),
            ErrorCode::InternalError => write!(f, "InternalError"),
        }
    }
}

impl From<&ErrorCode> for i8 {
    fn from(code: &ErrorCode) -> Self {
        match code {
            ErrorCode::SyntaxError => -10,
            ErrorCode::QueryPlanningError => -20,
            ErrorCode::QueryExecutionError => -30,
            ErrorCode::InternalError => -120,
        }
    }
}

impl From<&DataFusionError> for ErrorCode {
    fn from(error: &DataFusionError) -> Self {
        match error {
            DataFusionError::SQL(..) => ErrorCode::SyntaxError,
            DataFusionError::Plan(..) | DataFusionError::SchemaError(..) => {
                ErrorCode::QueryPlanningError
            }
            DataFusionError::ObjectStore(..)
            | DataFusionError::External(..)
            | DataFusionError::Execution(..) => ErrorCode::QueryExecutionError,
            DataFusionError::Context(_, err) => ErrorCode::from(err.as_ref()),
            _ => ErrorCode::InternalError,
        }
    }
}
