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

//! Single place that tells optimizers a checked integer aggregate must not
//! be completed as a pre-aggregate.
//!
//! `CheckedSum` keeps partial state as `Decimal128(38, 0)` and narrows only
//! in `evaluate()`. Any rewrite that runs a `Partial` → `Final`
//! below a later merge (eager aggregation below a join, single-distinct
//! `GROUP BY` the distinct key, …) would call `evaluate()` too soon and
//! false-error a representable total. Federation still needs the registry
//! name `"sum"`, so this is a type-and-name check, not a rename.
//!
//! Argument types must be the *source* types. `DataFusion` coerces
//! `SUM(INT)` to `SUM(CAST(INT AS BIGINT))`; looking at the casted type
//! would treat every 32-bit `SUM` as a 64-bit one and skip every
//! decomposable push, including the fork-patch guard that still has to
//! fire for `SUM(INT)`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, TryCastExpr};

use super::is_checked_sum_return_type;

/// True when finishing this aggregate now would narrow a checked 64-bit
/// integer total that a later merge still needs to cancel.
///
/// `arg_types` must be source types ([`physical_source_arg_types`] /
/// [`logical_source_arg_types`]), not the post-coercion `CAST` type.
/// `SUM(INT)` returns `BIGINT` but the inputs cannot overflow `i64` on a
/// realistic join-key group, so it can still be pre-aggregated. `SUM` /
/// `AVG` of `Int64` / `UInt64` cannot — those overflow a join-key group
/// and cancel later. `AVG` is included because `EagerAggregation::decompose`
/// builds its intermediate `SUM` with vanilla `sum_udaf()`, which wraps.
#[must_use]
pub fn complete_preaggregate_would_narrow_early(func_name: &str, arg_types: &[DataType]) -> bool {
    if !matches!(func_name, "sum" | "avg") {
        return false;
    }
    // Unknown / unresolved argument types must not look decomposable: a
    // `filter_map` that dropped a `data_type` error would otherwise let
    // `SUM(Int64)` through EagerAggregation and narrow a join-key group.
    arg_types.is_empty() || arg_types.iter().any(is_checked_sum_return_type)
}

/// Source argument types of a physical aggregate, stripping implicit
/// `CAST` / `TRY_CAST` so `SUM(INT)` is not treated as `SUM(BIGINT)`.
#[must_use]
pub fn physical_source_arg_types(
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Vec<DataType> {
    args.iter()
        .filter_map(|argument| unwrap_physical_casts(argument).data_type(input_schema).ok())
        .collect()
}

/// Source argument types of a logical aggregate, stripping `Alias` and
/// implicit `CAST` / `TRY_CAST`.
#[must_use]
pub fn logical_source_arg_types(args: &[Expr], schema: &DFSchema) -> Vec<DataType> {
    args.iter()
        .filter_map(|argument| unwrap_logical_casts(argument).get_type(schema).ok())
        .collect()
}

fn unwrap_physical_casts(expr: &Arc<dyn PhysicalExpr>) -> &Arc<dyn PhysicalExpr> {
    let mut current = expr;
    loop {
        if let Some(cast) = current.downcast_ref::<CastExpr>() {
            current = cast.expr();
            continue;
        }
        if let Some(try_cast) = current.downcast_ref::<TryCastExpr>() {
            current = try_cast.expr();
            continue;
        }
        return current;
    }
}

pub(crate) fn unwrap_logical_casts(expr: &Expr) -> &Expr {
    let mut current = expr;
    loop {
        match current {
            Expr::Alias(alias) => current = alias.expr.as_ref(),
            Expr::Cast(cast) => current = cast.expr.as_ref(),
            Expr::TryCast(try_cast) => current = try_cast.expr.as_ref(),
            _ => return current,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_sum_and_avg_are_not_eager_decomposable() {
        assert!(complete_preaggregate_would_narrow_early(
            "sum",
            &[DataType::Int64]
        ));
        assert!(complete_preaggregate_would_narrow_early(
            "sum",
            &[DataType::UInt64]
        ));
        assert!(complete_preaggregate_would_narrow_early(
            "avg",
            &[DataType::Int64]
        ));
        assert!(
            !complete_preaggregate_would_narrow_early("sum", &[DataType::Int32]),
            "SUM(INT) returns BIGINT but the inputs cannot overflow a join-key group"
        );
        assert!(!complete_preaggregate_would_narrow_early(
            "sum",
            &[DataType::Decimal128(38, 0)]
        ));
        assert!(!complete_preaggregate_would_narrow_early(
            "avg",
            &[DataType::Int32]
        ));
        assert!(!complete_preaggregate_would_narrow_early(
            "min",
            &[DataType::Int64]
        ));
        assert!(!complete_preaggregate_would_narrow_early("count", &[]));
        assert!(
            complete_preaggregate_would_narrow_early("sum", &[]),
            "unresolved SUM argument types must not look decomposable"
        );
        assert!(
            complete_preaggregate_would_narrow_early("avg", &[]),
            "unresolved AVG argument types must not look decomposable"
        );
    }

    #[test]
    fn sum_int32_coerced_to_bigint_is_still_eager_decomposable() {
        use arrow::datatypes::{Field, Schema};
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::cast;
        use datafusion::logical_expr::col;

        let schema = DFSchema::try_from(Schema::new(vec![Field::new("i", DataType::Int32, false)]))
            .expect("int32 schema");
        let coerced = cast(col("i"), DataType::Int64);
        let types = logical_source_arg_types(&[coerced], &schema);
        assert_eq!(
            types,
            vec![DataType::Int32],
            "must see INT under DataFusion's SUM(CAST(INT AS BIGINT)) coercion"
        );
        assert!(
            !complete_preaggregate_would_narrow_early("sum", &types),
            "SUM(INT) must stay eager-decomposable"
        );
    }
}
