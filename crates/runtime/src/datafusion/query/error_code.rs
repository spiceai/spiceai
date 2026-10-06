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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    SyntaxError,
    QueryPlanningError,
    QueryExecutionError,
    /// The query was refused because it could not be given the memory it asked
    /// for — `runtime.query.memory_limit` (or a pool carved out of it) was
    /// exhausted.
    ///
    /// Kept distinct from [`ErrorCode::InternalError`] because the two need
    /// opposite responses: this one says the deployment needs more memory or
    /// fewer partitions, and an operator has to be able to alert on it without
    /// also alerting on runtime bugs.
    ResourcesExhausted,
    InternalError,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::SyntaxError => write!(f, "SyntaxError"),
            ErrorCode::QueryPlanningError => write!(f, "QueryPlanningError"),
            ErrorCode::QueryExecutionError => write!(f, "QueryExecutionError"),
            ErrorCode::ResourcesExhausted => write!(f, "ResourcesExhausted"),
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
            ErrorCode::ResourcesExhausted => -40,
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
            DataFusionError::ResourcesExhausted(..) => ErrorCode::ResourcesExhausted,
            DataFusionError::Context(_, err) => ErrorCode::from(err.as_ref()),
            // A join wraps its build-side failure in `Shared` before handing it
            // to each probe partition (datafusion physical-plan joins/utils.rs).
            // Without this arm a join's memory refusal is `InternalError`.
            DataFusionError::Shared(err) => ErrorCode::from(err.as_ref()),
            _ => ErrorCode::InternalError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ErrorCode;
    use datafusion::error::DataFusionError;
    use std::sync::Arc;

    /// A memory-pool refusal must not be labelled `InternalError`: that is the
    /// label for "spiced has a bug", and conflating the two leaves an operator
    /// unable to alert on capacity without also alerting on bugs.
    #[test]
    fn resources_exhausted_is_not_an_internal_error() {
        let err = DataFusionError::ResourcesExhausted(
            "Additional allocation failed for HashJoinInput[135]".to_string(),
        );

        assert_eq!(ErrorCode::from(&err), ErrorCode::ResourcesExhausted);
        assert_eq!(ErrorCode::from(&err).to_string(), "ResourcesExhausted");
    }

    /// Execution wraps the pool error in a `Context` often enough that the
    /// recursion has to keep working for the new arm.
    #[test]
    fn resources_exhausted_is_found_through_context() {
        let err = DataFusionError::Context(
            "Join Error".to_string(),
            Box::new(DataFusionError::ResourcesExhausted(
                "out of memory".to_string(),
            )),
        );

        assert_eq!(ErrorCode::from(&err), ErrorCode::ResourcesExhausted);
    }

    /// A join's build-side refusal reaches the runtime wrapped in `Shared`, and
    /// must classify the same as a bare one.
    #[test]
    fn resources_exhausted_is_found_through_shared() {
        let err = DataFusionError::Shared(Arc::new(DataFusionError::ResourcesExhausted(
            "Additional allocation failed for HashJoinInput[135]".to_string(),
        )));

        assert_eq!(ErrorCode::from(&err), ErrorCode::ResourcesExhausted);
    }

    /// `Shared` and `Context` nest in both orders, so the recursion has to
    /// survive either.
    #[test]
    fn resources_exhausted_is_found_through_shared_and_context() {
        let shared_in_context = DataFusionError::Context(
            "Join Error".to_string(),
            Box::new(DataFusionError::Shared(Arc::new(
                DataFusionError::ResourcesExhausted("out of memory".to_string()),
            ))),
        );
        assert_eq!(
            ErrorCode::from(&shared_in_context),
            ErrorCode::ResourcesExhausted
        );

        let context_in_shared = DataFusionError::Shared(Arc::new(DataFusionError::Context(
            "Join Error".to_string(),
            Box::new(DataFusionError::ResourcesExhausted(
                "out of memory".to_string(),
            )),
        )));
        assert_eq!(
            ErrorCode::from(&context_in_shared),
            ErrorCode::ResourcesExhausted
        );
    }

    /// Unwrapping `Shared` must not flatten every shared failure to one code.
    #[test]
    fn shared_preserves_the_inner_code() {
        assert_eq!(
            ErrorCode::from(&DataFusionError::Shared(Arc::new(
                DataFusionError::Execution("boom".to_string())
            ))),
            ErrorCode::QueryExecutionError
        );
        assert_eq!(
            ErrorCode::from(&DataFusionError::Shared(Arc::new(
                DataFusionError::Internal("bug".to_string())
            ))),
            ErrorCode::InternalError
        );
    }

    /// The codes are a stable external surface, so a new variant must not
    /// renumber the existing ones.
    #[test]
    fn error_codes_are_stable_and_distinct() {
        assert_eq!(i8::from(&ErrorCode::SyntaxError), -10);
        assert_eq!(i8::from(&ErrorCode::QueryPlanningError), -20);
        assert_eq!(i8::from(&ErrorCode::QueryExecutionError), -30);
        assert_eq!(i8::from(&ErrorCode::ResourcesExhausted), -40);
        assert_eq!(i8::from(&ErrorCode::InternalError), -120);
    }

    /// The variants the new arm sits between must keep their classification.
    #[test]
    fn other_errors_keep_their_codes() {
        assert_eq!(
            ErrorCode::from(&DataFusionError::Plan("bad plan".to_string())),
            ErrorCode::QueryPlanningError
        );
        assert_eq!(
            ErrorCode::from(&DataFusionError::Execution("boom".to_string())),
            ErrorCode::QueryExecutionError
        );
        assert_eq!(
            ErrorCode::from(&DataFusionError::Internal("bug".to_string())),
            ErrorCode::InternalError
        );
    }
}
