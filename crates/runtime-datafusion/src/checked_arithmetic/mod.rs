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

//! Checked integer `SUM` / `+` / `-` / `*` so overflow is an error, not a wrap.
//!
//! `DataFusion`'s built-in `sum` and integer `BinaryExpr` use wrapping Arrow
//! kernels (`apache/datafusion#3520`). Spice replaces `sum` with [`CheckedSum`]
//! and rewrites integer `+` / `-` / `*` to kernels that fail on overflow.
//! Federation still sees the original operators: the federation analyzer runs
//! before this rewrite. A table that renders `UPDATE` / `DELETE` expressions as
//! SQL gets them back through [`sql_rendered_dml`].

mod analyzer;
mod decomposability;
mod dml;
mod eager;
mod op;
mod single_distinct;
mod sum;

pub use analyzer::CheckedIntegerOverflow;
pub(crate) use analyzer::restore_integer_operators;
pub use decomposability::complete_preaggregate_would_narrow_early;
pub use dml::sql_rendered_dml;
pub use eager::{
    CheckedEagerAggregation, install_checked_eager_aggregation,
    install_checked_eager_aggregation_for_distributed,
};
pub use op::{CHECKED_ADD_UDF_NAME, CHECKED_MUL_UDF_NAME, CHECKED_SUB_UDF_NAME};
pub use single_distinct::{CheckedSingleDistinctToGroupBy, install_checked_single_distinct};
pub use sum::CheckedSum;

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};

use crate::checked_arithmetic::op::{checked_add_udf, checked_mul_udf, checked_sub_udf};

/// Register checked integer `SUM` and the scalar kernels the analyzer rewrites
/// `+` / `-` / `*` into.
///
/// # Errors
///
/// Returns an error when a function cannot be registered on `state`.
pub fn register_checked_arithmetic(state: &mut SessionState) -> Result<()> {
    for udf in checked_scalar_udfs() {
        state.register_udf(udf)?;
    }
    replace_sum(state)
}

/// The scalar kernels the analyzer rewrites integer `+` / `-` / `*` into.
///
/// A registry that is not a [`SessionState`] (a cluster executor decodes task
/// plans against its own) must hold these and [`checked_sum_udaf`] too, or a
/// plan built on the scheduler decodes to different functions.
#[must_use]
pub fn checked_scalar_udfs() -> [Arc<ScalarUDF>; 3] {
    [checked_add_udf(), checked_sub_udf(), checked_mul_udf()]
}

/// Checked integer `SUM`, registered under the built-in name `sum`.
///
/// Its partial state is `Decimal128(38, 0)` for integer input. A registry that
/// resolves `sum` to the built-in instead produces `Int64` state, which no
/// longer matches the schema the plan was serialized with.
#[must_use]
pub fn checked_sum_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::from(CheckedSum::new()))
}

fn replace_sum(state: &mut SessionState) -> Result<()> {
    let udaf = checked_sum_udaf();
    if state.register_udaf(Arc::clone(&udaf)).is_ok() {
        return Ok(());
    }
    let _previous = state.deregister_udaf("sum")?;
    state.register_udaf(udaf)?;
    Ok(())
}

#[must_use]
pub(crate) fn is_integer_arith_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

#[must_use]
pub(crate) fn is_checked_sum_return_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Int64 | DataType::UInt64)
}

#[cfg(test)]
mod perf;
#[cfg(test)]
pub(crate) mod tests;
