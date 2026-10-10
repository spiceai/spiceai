/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The five decision functions: one [`DecisionUdf`] per [`Kind`].

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use async_trait::async_trait;
use datafusion::common::{Result, exec_err, internal_err};
use datafusion::logical_expr::async_udf::AsyncScalarUDFImpl;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use crate::args::{CallArgs, Layout};
use crate::exec::Decider;
use crate::output;
use crate::{AI_CLASSIFY_NAME, AI_DECIDE_NAME, AI_IF_NAME, AI_PROBABILITY_NAME, AI_SCORE_NAME};

/// Rows a decision function receives per invocation. `DataFusion` slices each input
/// batch into invocations of this size and runs them one after another, so every slice
/// ends by waiting for its slowest request; a larger slice spends less of its time
/// draining (`benches/execution.rs`), and this one still bounds the input text held per
/// call. The size does not bound what a `LIMIT` discards: a whole input batch is
/// answered before any of its rows move on.
pub(crate) const ROWS_PER_INVOCATION: usize = 1024;

/// Which decision function a call is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    If,
    Probability,
    Classify,
    Score,
    Decide,
}

impl Kind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::If => AI_IF_NAME,
            Self::Probability => AI_PROBABILITY_NAME,
            Self::Classify => AI_CLASSIFY_NAME,
            Self::Score => AI_SCORE_NAME,
            Self::Decide => AI_DECIDE_NAME,
        }
    }

    /// The parameters, in position order, and how many are required.
    pub(crate) fn layout(self) -> Layout {
        match self {
            Self::If | Self::Probability => Layout {
                params: &["input", "condition", "model", "on_error"],
                required: 2,
            },
            Self::Classify => Layout {
                params: &["input", "labels", "instructions", "model", "on_error"],
                required: 2,
            },
            Self::Score => Layout {
                params: &["input", "instructions", "levels", "model", "on_error"],
                required: 3,
            },
            Self::Decide => Layout {
                params: &["input", "questions", "model", "on_error"],
                required: 2,
            },
        }
    }

    /// The usage line shown in argument errors.
    pub(crate) fn usage(self) -> &'static str {
        match self {
            Self::If => "ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])",
            Self::Probability => {
                "ai_probability(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])"
            }
            Self::Classify => {
                "ai_classify(input, labels[, instructions => '...'][, model => 'name'][, on_error => 'fail' | 'null'])"
            }
            Self::Score => {
                "ai_score(input, instructions, levels[, model => 'name'][, on_error => 'fail' | 'null'])"
            }
            Self::Decide => {
                "ai_decide(input, questions[, model => 'name'][, on_error => 'fail' | 'null'])"
            }
        }
    }

    /// The SQL type a typed function returns. `ai_decide`'s struct depends on its
    /// questions, so it has none here.
    pub(crate) fn typed_return(self) -> Option<DataType> {
        match self {
            Self::If => Some(DataType::Boolean),
            Self::Probability | Self::Score => Some(DataType::Float64),
            Self::Classify => Some(DataType::Utf8),
            Self::Decide => None,
        }
    }
}

/// One decision function. All five share a [`Decider`], which holds the model store.
#[derive(Debug)]
pub(crate) struct DecisionUdf {
    kind: Kind,
    signature: Signature,
    decider: Arc<Decider>,
}

impl DecisionUdf {
    pub(crate) fn new(kind: Kind, decider: Arc<Decider>) -> Self {
        let layout = kind.layout();
        // `Any` takes every argument as it is: the input may be any type, and the
        // constants are checked while the query is planned, with messages that name them.
        // Fixed arities let the parameters have names; `AsyncScalarUDF` does not
        // forward `coerce_types`, which a variable-arity signature would need.
        let arities: Vec<TypeSignature> = (layout.required..=layout.params.len())
            .map(TypeSignature::Any)
            .collect();
        let names: Vec<String> = layout.params.iter().map(ToString::to_string).collect();
        let signature = match Signature::one_of(arities.clone(), Volatility::Volatile)
            .with_parameter_names(names)
        {
            Ok(signature) => signature,
            Err(_) => Signature::one_of(arities, Volatility::Volatile),
        };
        Self {
            kind,
            signature,
            decider,
        }
    }
}

impl PartialEq for DecisionUdf {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && Arc::ptr_eq(&self.decider, &other.decider)
    }
}

impl Eq for DecisionUdf {}

impl Hash for DecisionUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        Arc::as_ptr(&self.decider).addr().hash(state);
    }
}

impl ScalarUDFImpl for DecisionUdf {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        if let Some(data_type) = self.kind.typed_return() {
            Ok(data_type)
        } else {
            internal_err!("{} derives its return type from its questions", self.name())
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        crate::args::check_planning_args(self.kind, args.arg_fields, args.scalar_arguments)?;
        let data_type = if let Some(data_type) = self.kind.typed_return() {
            data_type
        } else {
            let questions = crate::args::questions_from_scalar(
                self.kind,
                args.scalar_arguments.get(1).copied().flatten(),
            )?;
            output::decision_type(&questions)
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!(
            "{} calls a model and runs only as an async function",
            self.name()
        )
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for DecisionUdf {
    fn ideal_batch_size(&self) -> Option<usize> {
        Some(ROWS_PER_INVOCATION)
    }

    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let call = CallArgs::from_invocation(self.kind, &args)?;
        let input = match &args.args[0] {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(args.number_rows)?,
        };
        let function = match self.kind {
            Kind::Decide => crate::args::lowered_from(&call.questions)
                .unwrap_or_else(|| self.kind.name().to_string()),
            _ => self.kind.name().to_string(),
        };
        let answers = self
            .decider
            .decide(
                &function,
                &input,
                &call.questions,
                call.model.as_deref(),
                call.on_error,
            )
            .await?;
        let array = output::build(self.kind, &call.questions, &answers)?;
        Ok(ColumnarValue::Array(array))
    }
}
