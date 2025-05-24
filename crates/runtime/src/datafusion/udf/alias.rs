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

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::{
    common::{ExprSchema, Result as DataFusionResult},
    logical_expr::{
        ColumnarValue, Documentation, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs,
        ScalarUDFImpl, Signature,
        interval_arithmetic::Interval,
        simplify::{ExprSimplifyResult, SimplifyInfo},
        sort_properties::{ExprProperties, SortProperties},
    },
    prelude::Expr,
};

/// Aliases an existing Scalar UDF to a new name.
#[derive(Debug)]
pub struct ScalarUDFAlias {
    scalar_udf: Arc<dyn ScalarUDFImpl>,
    alias: &'static str,
}

impl ScalarUDFAlias {
    #[must_use]
    pub fn new(scalar_udf: Arc<dyn ScalarUDFImpl>, alias: &'static str) -> Self {
        Self { scalar_udf, alias }
    }
}

impl ScalarUDFImpl for ScalarUDFAlias {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        self.alias
    }

    fn signature(&self) -> &Signature {
        self.scalar_udf.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        self.scalar_udf.return_type(arg_types)
    }

    #[expect(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        self.scalar_udf.invoke(args)
    }

    fn display_name(&self, _args: &[Expr]) -> DataFusionResult<String> {
        Ok(self.alias.to_string())
    }

    #[expect(deprecated)]
    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> DataFusionResult<DataType> {
        self.scalar_udf
            .return_type_from_exprs(args, schema, arg_types)
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> DataFusionResult<ReturnInfo> {
        self.scalar_udf.return_type_from_args(args)
    }

    #[expect(deprecated)]
    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.scalar_udf.is_nullable(args, schema)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> DataFusionResult<ColumnarValue> {
        self.scalar_udf.invoke_batch(args, number_rows)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        self.scalar_udf.invoke_with_args(args)
    }

    #[expect(deprecated)]
    fn invoke_no_args(&self, number_rows: usize) -> DataFusionResult<ColumnarValue> {
        self.scalar_udf.invoke_no_args(number_rows)
    }

    fn aliases(&self) -> &[String] {
        self.scalar_udf.aliases()
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> DataFusionResult<ExprSimplifyResult> {
        self.scalar_udf.simplify(args, info)
    }

    fn short_circuits(&self) -> bool {
        self.scalar_udf.short_circuits()
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> DataFusionResult<Interval> {
        self.scalar_udf.evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> DataFusionResult<Option<Vec<Interval>>> {
        self.scalar_udf.propagate_constraints(interval, inputs)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> DataFusionResult<SortProperties> {
        self.scalar_udf.output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> DataFusionResult<bool> {
        self.scalar_udf.preserves_lex_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        self.scalar_udf.coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.scalar_udf.documentation()
    }
}
