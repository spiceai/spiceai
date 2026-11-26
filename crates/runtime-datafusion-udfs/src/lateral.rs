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

//! Runs a table function once for each row of a `LATERAL` input, as in
//! `FROM q CROSS JOIN LATERAL text_search(docs, q.term)`.
//!
//! [`bind_per_row`] implements [`TableFunctionImpl::bind_with_input`] for a table function
//! whose provider path takes literal arguments. The arguments that read the input's columns
//! are evaluated by `DataFusion` for each input batch; every input row then calls the
//! function's provider path with those values as literals, scans the provider, and tags each
//! output row with the input row that produced it. `DataFusion` joins the rows back to the
//! input, applies `ON`, and adds the NULL row of a `LEFT JOIN LATERAL`.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::compute::cast;
use arrow_schema::{DataType, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, internal_err, plan_err};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::execution::{TaskContext, async_try_stream};
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::expr::FieldMetadata;
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::table_function::{
    TableFunctionBinding, TableFunctionEvaluator, TableFunctionRows,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use futures::stream::BoxStream;

/// What a row whose input arguments include a NULL produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullRows {
    /// No rows, without calling the function: a search for a NULL query finds nothing.
    Skip,
    /// Whatever the function returns for a NULL literal argument.
    Call,
}

/// Binds `function` to a `LATERAL` input. Returns `None` when the input has no columns, so a
/// call outside `LATERAL` is planned as a scan of the provider.
///
/// The output schema is the schema of the provider for the call with each input-dependent
/// argument replaced by a placeholder of its type (the empty string for text), so it must
/// not depend on those arguments' values. Building that provider also reports a bad table,
/// column or option at planning rather than for each row.
///
/// # Errors
///
/// Returns a plan error when an input-dependent argument nests a table function call, or
/// when the placeholder call fails.
pub fn bind_per_row(
    name: &str,
    function: Arc<dyn TableFunctionImpl>,
    args: &TableFunctionArgs,
    input: &DFSchema,
    null_rows: NullRows,
) -> Result<Option<TableFunctionBinding>> {
    if input.fields().is_empty() {
        return Ok(None);
    }
    let session = args
        .session()
        .as_any()
        .downcast_ref::<SessionState>()
        .map(|state| Arc::new(state.clone()));

    let mut call = args.exprs().to_vec();
    let mut row_arguments = Vec::new();
    let mut arguments = Vec::new();
    for (index, arg) in args.exprs().iter().enumerate() {
        if !arg.column_refs().iter().any(|c| input.has_column(c)) {
            continue;
        }
        // A named argument (`name => value`) keeps its name in the literal's metadata.
        let (expr, metadata) = match arg {
            Expr::Alias(alias) => (alias.expr.as_ref().clone(), alias.metadata.clone()),
            expr => (expr.clone(), None),
        };
        if let Some(session) = &session
            && let Some(nested) = nested_table_function(&expr, session)?
        {
            return plan_err!(
                "{name}() cannot take a nested {nested}() call whose arguments read columns of the LATERAL input; pass {nested}() literal arguments"
            );
        }
        let data_type = row_type(&expr.get_type(input)?);
        call[index] = Expr::Literal(ScalarValue::new_default(&data_type)?, metadata.clone());
        row_arguments.push(RowArgument {
            index,
            data_type,
            metadata,
        });
        arguments.push(expr);
    }

    let schema = function
        .call_with_args(TableFunctionArgs::new(&call, args.session()))?
        .schema();
    Ok(Some(TableFunctionBinding {
        arguments,
        evaluator: Arc::new(PerRowEvaluator {
            display: format!(
                "{name}({})",
                args.exprs()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            function,
            session,
            call,
            row_arguments,
            null_rows,
            schema,
        }),
    }))
}

/// The name of a table function that `expr` calls, as the nested search calls `rrf` and
/// `rerank` take. `DataFusion` cannot evaluate such a call for each input row.
fn nested_table_function(expr: &Expr, session: &SessionState) -> Result<Option<String>> {
    let mut nested = None;
    expr.apply(|e| {
        if let Expr::ScalarFunction(f) = e
            && session.table_functions().contains_key(f.name())
        {
            nested = Some(f.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(nested)
}

/// Every string type is passed as `Utf8`, the type of a SQL string literal, which is what
/// the table functions' argument parsers accept.
fn row_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        other => other.clone(),
    }
}

#[derive(Debug)]
struct RowArgument {
    /// Position in the call's arguments.
    index: usize,
    data_type: DataType,
    metadata: Option<FieldMetadata>,
}

struct PerRowEvaluator {
    function: Arc<dyn TableFunctionImpl>,
    /// The session that planned the call. Each row's provider is built and scanned in it.
    session: Option<Arc<SessionState>>,
    /// The call's arguments, with each row's values at the [`RowArgument`] positions.
    call: Vec<Expr>,
    row_arguments: Vec<RowArgument>,
    null_rows: NullRows,
    schema: SchemaRef,
    /// The call as written, for `EXPLAIN`.
    display: String,
}

impl Debug for PerRowEvaluator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

impl TableFunctionEvaluator for PerRowEvaluator {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn evaluate(
        &self,
        arguments: Vec<ArrayRef>,
        num_rows: usize,
        projection: &[usize],
        context: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<TableFunctionRows>>> {
        if arguments.len() != self.row_arguments.len() {
            return internal_err!(
                "{} expects {} argument(s) per input row, got {}",
                self.display,
                self.row_arguments.len(),
                arguments.len()
            );
        }
        let arrays = arguments
            .iter()
            .zip(&self.row_arguments)
            .map(|(array, arg)| cast(array, &arg.data_type))
            .collect::<Result<Vec<_>, _>>()?;
        let session = match &self.session {
            Some(session) => Arc::clone(session),
            None => Arc::new(
                SessionStateBuilder::new()
                    .with_config(context.session_config().clone())
                    .with_runtime_env(context.runtime_env())
                    .build(),
            ),
        };
        let function = Arc::clone(&self.function);
        let mut call = self.call.clone();
        let positions = self
            .row_arguments
            .iter()
            .map(|arg| (arg.index, arg.metadata.clone()))
            .collect::<Vec<_>>();
        let null_rows = self.null_rows;
        let schema = Arc::new(self.schema.project(projection)?);
        // An empty projection keeps only the row count. Scan every column for it rather than
        // ask the provider for a plan with no columns.
        let scan_projection = (!projection.is_empty()).then(|| projection.to_vec());

        Ok(Box::pin(async_try_stream(move |mut out| async move {
            for row in 0..num_rows {
                if null_rows == NullRows::Skip && arrays.iter().any(|array| array.is_null(row)) {
                    continue;
                }
                let Ok(input_row) = u32::try_from(row) else {
                    return internal_err!("Input row {row} exceeds the UInt32 range");
                };
                for (array, (index, metadata)) in arrays.iter().zip(&positions) {
                    call[*index] =
                        Expr::Literal(ScalarValue::try_from_array(array, row)?, metadata.clone());
                }
                let provider = function.call_with_args(TableFunctionArgs::new(&call, &*session))?;
                let plan = provider
                    .scan(&*session, scan_projection.as_ref(), &[], None)
                    .await?;
                let mut results = execute_stream(plan, Arc::clone(&context))?;
                while let Some(batch) = results.try_next().await? {
                    let num_rows = batch.num_rows();
                    let columns = if scan_projection.is_some() {
                        batch.columns().to_vec()
                    } else {
                        vec![]
                    };
                    // A plan's fields can differ from the declared schema in nullability or
                    // metadata, and `TableFunctionExec` requires the declared schema.
                    let batch = RecordBatch::try_new_with_options(
                        Arc::clone(&schema),
                        columns,
                        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                    )?;
                    out.emit(TableFunctionRows {
                        batch,
                        input_rows: UInt32Array::from_value(input_row, num_rows),
                    })
                    .await;
                }
            }
            Ok(())
        })))
    }
}
