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

//! Per file, a scan reads a JSON key column from the values the file stores for
//! it, or computes it from the file's JSON column when the file stores none.

use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_plan::PhysicalExpr;

use super::{JsonTopLevelValue, hidden_column_name, is_json_text_type};

/// Adapts a file scan's expressions to each file, as `DataFusion`'s default
/// adapter does, after replacing every `__cayenne_json_top_level_value(<column>,
/// '<key>')` with the column the file stores those values in, when it has one.
///
/// A file stores them under [`hidden_column_name`], a column the table itself does
/// not have; a column of that name that is also the table's is the table's data and
/// is never read in place of the computed values. A file without such a column
/// keeps the computation, which reads the file's JSON column.
#[derive(Debug)]
pub(crate) struct JsonKeyExprAdapterFactory {
    inner: Arc<dyn PhysicalExprAdapterFactory>,
}

impl JsonKeyExprAdapterFactory {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(DefaultPhysicalExprAdapterFactory),
        }
    }

    /// The factory, as a file scan configuration takes it.
    pub(crate) fn shared() -> Option<Arc<dyn PhysicalExprAdapterFactory>> {
        Some(Arc::new(Self::new()))
    }
}

impl PhysicalExprAdapterFactory for JsonKeyExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(JsonKeyExprAdapter {
            inner: self.inner.create(
                Arc::clone(&logical_file_schema),
                Arc::clone(&physical_file_schema),
            )?,
            logical_file_schema,
            physical_file_schema,
        }))
    }
}

#[derive(Debug)]
struct JsonKeyExprAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for JsonKeyExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = expr
            .transform_down(|expr| Ok(self.stored_values(&expr).map_or(Transformed::no(expr), |stored| {
                Transformed::new(stored, true, TreeNodeRecursion::Jump)
            })))
            .data()?;
        // The stored column is not in the logical schema; the default adapter
        // resolves such a column against the file's own schema.
        self.inner.rewrite(expr)
    }
}

impl JsonKeyExprAdapter {
    /// The file's stored values for `expr`, when `expr` computes a JSON key column
    /// and the file stores that column.
    fn stored_values(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<Arc<dyn PhysicalExpr>> {
        let call = ScalarFunctionExpr::try_downcast_func::<JsonTopLevelValue>(expr.as_ref())?;
        let [json, key] = call.args() else {
            return None;
        };
        let json = json.downcast_ref::<Column>()?;
        let ScalarValue::Utf8(Some(key)) = key.downcast_ref::<Literal>()?.value() else {
            return None;
        };
        let name = hidden_column_name(json.name(), key);
        if self.logical_file_schema.field_with_name(&name).is_ok() {
            return None;
        }
        let index = self.physical_file_schema.index_of(&name).ok()?;
        let data_type = self.physical_file_schema.field(index).data_type();
        if !is_json_text_type(data_type) {
            return None;
        }
        let stored: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&name, index));
        Some(if *data_type == DataType::Utf8View {
            stored
        } else {
            Arc::new(CastExpr::new(stored, DataType::Utf8View, None))
        })
    }
}
