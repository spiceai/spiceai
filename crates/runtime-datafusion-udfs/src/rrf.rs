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
use arrow_schema::DataType;
use datafusion::common::{DataFusionError, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, DocSection, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use std::any::Any;
use std::fmt::Debug;
use std::sync::LazyLock;

pub static RRF_UDF_NAME: &str = "reciprocal_rank_fusion";
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| Documentation {
    doc_section: DocSection::default(),
    description: "Merge and re-rank several search queries into one result set".to_string(),
    syntax_example: "rrf(query_1, query_2, ..., k)".to_string(),
    sql_example: Some("SELECT embed('hello world', 'potion_2m')".to_string()),
    arguments: Some(vec![
        (
            "query...".to_string(),
            "Varadic queries or table references".to_string(),
        ),
        ("k".to_string(), "RRF smoothing parameter".to_string()),
    ]),
    alternative_syntax: None,
    related_udfs: Some(vec!["text_search".to_string(), "vector_search".to_string()]),
});

pub static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::variadic_any(Volatility::Stable));

/// A no-op UDF detected by an Optimizer that subsequently implements RRF
/// using plain SQL
#[derive(Debug)]
pub struct ReciprocalRankFusion {}

impl ReciprocalRankFusion {
    fn default_error<T>() -> Result<T, DataFusionError> {
        exec_err!("This is a bug! {RRF_UDF_NAME} should be rewritten by an optimizer rule.")
    }
}

impl ScalarUDFImpl for ReciprocalRankFusion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        RRF_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Self::default_error()
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        Self::default_error()
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&*DOCUMENTATION)
    }
}
