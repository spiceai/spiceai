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

pub enum ErrorCode {
    SyntaxError,
    QueryPlanningError,
    DataReadError,
    UnexpectedError,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::SyntaxError => write!(f, "SyntaxError"),
            ErrorCode::QueryPlanningError => write!(f, "QueryPlanningError"),
            ErrorCode::DataReadError => write!(f, "DataReadError"),
            ErrorCode::UnexpectedError => write!(f, "UnexpectedError"),
        }
    }
}

impl From<&ErrorCode> for i8 {
    fn from(code: &ErrorCode) -> Self {
        match code {
            ErrorCode::SyntaxError => -10,
            ErrorCode::QueryPlanningError => -20,
            ErrorCode::DataReadError => -30,
            ErrorCode::UnexpectedError => -120,
        }
    }
}

#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn error_code_from_datafusion_error(error: &DataFusionError) -> ErrorCode {
    match error {
        DataFusionError::SQL(..) => ErrorCode::SyntaxError,
        DataFusionError::Plan(..) | DataFusionError::SchemaError(..) => {
            ErrorCode::QueryPlanningError
        }
        DataFusionError::ObjectStore(..) | DataFusionError::External(..) => {
            ErrorCode::DataReadError
        }
        DataFusionError::Context(_, err) => error_code_from_datafusion_error(err),
        _ => ErrorCode::UnexpectedError,
    }
}
