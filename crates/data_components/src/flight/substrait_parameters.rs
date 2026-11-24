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

//! Parameter bindings for a Substrait plan sent as a
//! [`flight_client::parameterized_ticket`].

use datafusion::{
    common::{Result, ScalarValue, plan_err},
    execution::context::SessionState,
};
use datafusion_substrait::{
    logical_plan::producer::{DefaultSubstraitProducer, SubstraitProducer, from_literal},
    substrait::proto::{DynamicParameterBinding, Plan, expression::RexType},
};

/// Sets `plan`'s `parameter_bindings` to `values`, the value of `$N` at index
/// `N - 1`, each written as a Substrait literal the way `datafusion-substrait`
/// writes one in an expression.
///
/// # Errors
/// A value whose literal would need a type extension (such as `Float16`) is
/// refused: a binding cannot declare one, and the plan's own extensions were
/// written without it, so the upstream could not read the value back.
pub fn bind_parameters(plan: &mut Plan, values: &[ScalarValue], state: &SessionState) -> Result<()> {
    let mut bindings = Vec::with_capacity(values.len());
    for (anchor, value) in values.iter().enumerate() {
        let mut producer = DefaultSubstraitProducer::new(state);
        let literal = match from_literal(&mut producer, value)?.rex_type {
            Some(RexType::Literal(literal)) => literal,
            other => {
                return plan_err!(
                    "Parameter ${} ({}) did not encode as a Substrait literal: {other:?}",
                    anchor + 1,
                    value.data_type()
                );
            }
        };
        let extensions = producer.get_extensions();
        if !extensions.functions.is_empty()
            || !extensions.types.is_empty()
            || !extensions.type_variations.is_empty()
        {
            return plan_err!(
                "Parameter ${} has type {}, which a Substrait parameter binding cannot carry",
                anchor + 1,
                value.data_type()
            );
        }
        bindings.push(DynamicParameterBinding {
            parameter_anchor: u32::try_from(anchor).or_else(|_| {
                plan_err!("A Substrait plan cannot carry more than {} parameters", u32::MAX)
            })?,
            value: Some(literal),
        });
    }
    plan.parameter_bindings = bindings;
    Ok(())
}
