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

//! Plain integer `+` / `-` / `*` in the `UPDATE` and `DELETE` another engine
//! evaluates.
//!
//! [`CheckedIntegerOverflow`](super::CheckedIntegerOverflow) rewrites the
//! operators in every plan, `UPDATE` and `DELETE` included, because the
//! analyzer cannot tell where a write's expressions are evaluated: an
//! accelerated table evaluates them itself under `write_mode: write_back`,
//! hands them to the source under `write_through`, and does both under
//! `dual_write`. A provider that renders its `SET` assignments and `WHERE`
//! filters as SQL for another engine would send `__spice_checked_add(..)`,
//! which that engine does not have. [`PlainArithmeticWrites`] restores the
//! operators at that boundary, so the engine applies its own overflow rules,
//! as it already does for the parts of a query federation sends it.
//!
//! A provider that renders SQL and lacks this layer fails loudly (the engine
//! rejects the unknown function); one that evaluates locally and has it would
//! wrap on overflow silently. So the layer goes only on providers that render.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::expr::{BinaryExpr, ScalarFunction};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

use super::op::CheckedIntegerOp;

/// `expr` with every checked integer `+` / `-` / `*` the analyzer introduced
/// turned back into the operator it replaced.
///
/// # Errors
///
/// Returns an error only if rewriting the expression tree fails.
pub fn plain_arithmetic(expr: Expr) -> Result<Expr> {
    expr.transform_up(|expr| Ok(restore_operator(expr)))
        .map(|transformed| transformed.data)
}

fn restore_operator(expr: Expr) -> Transformed<Expr> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Transformed::no(expr);
    };
    let Some(op) = func
        .inner()
        .downcast_ref::<CheckedIntegerOp>()
        .map(CheckedIntegerOp::op)
    else {
        return Transformed::no(Expr::ScalarFunction(ScalarFunction { func, args }));
    };
    match <[Expr; 2]>::try_from(args) {
        Ok([left, right]) => Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            op,
            Box::new(right),
        ))),
        Err(args) => Transformed::no(Expr::ScalarFunction(ScalarFunction { func, args })),
    }
}

/// Stacks [`PlainArithmeticWrites`] onto `provider`, a provider that renders
/// `UPDATE` and `DELETE` as SQL for another engine.
#[must_use]
pub fn with_plain_arithmetic_writes(provider: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
    SpiceTable::over(Arc::new(PlainArithmeticWrites), provider)
}

/// Restores plain integer operators in the `UPDATE` assignments and the
/// `UPDATE` / `DELETE` filters handed to the provider beneath. See the module
/// documentation.
#[derive(Debug)]
pub struct PlainArithmeticWrites;

#[async_trait]
impl TableLayer for PlainArithmeticWrites {
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        // Exhaustive on purpose: a wildcard would answer a future walk kind for this
        // layer without anyone deciding what it should say.
        match walk {
            // Scans and inserts reach the table beneath unchanged, and the rewrite
            // changes how an `UPDATE` or `DELETE` is written, not what it does.
            LayerWalk::Read
            | LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::Write
            | LayerWalk::RetentionDelete
            | LayerWalk::Index
            | LayerWalk::Passthrough => Some(below),
        }
    }

    async fn delete_from(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filters = filters
            .into_iter()
            .map(plain_arithmetic)
            .collect::<Result<_>>()?;
        below.delete_from(state, filters).await
    }

    async fn update(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let assignments = assignments
            .into_iter()
            .map(|(column, expr)| Ok((column, plain_arithmetic(expr)?)))
            .collect::<Result<_>>()?;
        let filters = filters
            .into_iter()
            .map(plain_arithmetic)
            .collect::<Result<_>>()?;
        below.update(state, assignments, filters).await
    }
}
