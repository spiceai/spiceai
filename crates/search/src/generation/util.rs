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

use datafusion::{logical_expr::Operator, prelude::Expr};

/// For a set of binary filter [`Expr`] = {f1, f2, .., fn} and binary operation op, return expression: `(((f1 op f2) op ...) op fn)`.
#[must_use]
pub fn fold_binary(exprs: &[Expr], op: Operator) -> Option<Expr> {
    let mut iter = exprs.iter();
    let first = iter.next()?.clone();
    Some(iter.fold(first, |acc, expr| {
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(acc),
            op,
            Box::new(expr.clone()),
        ))
    }))
}
