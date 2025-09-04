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
use arrow_schema::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{
    ColumnarValue, DocSection, Documentation, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::SessionContext;
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};

pub static RRF_UDF_NAME: &str = "rrf";
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| Documentation {
    doc_section: DocSection::default(),
    description: "Merge and re-rank several search queries into one result set".to_string(),
    syntax_example: "rrf(query_1, query_2, ..., k)".to_string(),
    sql_example: None,
    arguments: Some(vec![
        (
            "query...".to_string(),
            "Inline text_search or vector_search UDTF invocations".to_string(),
        ),
        ("k".to_string(), "RRF smoothing parameter".to_string()),
    ]),
    alternative_syntax: None,
    related_udfs: Some(vec!["text_search".to_string(), "vector_search".to_string()]),
});

pub static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::variadic_any(Volatility::Stable));

/// A no-op UDTF detected by an Optimizer that subsequently implements RRF using plain SQL
pub struct ReciprocalRankFusion {
    pub args: Vec<Expr>,
    pub session_context: Arc<SessionContext>,
}

impl Debug for ReciprocalRankFusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReciprocalRankFusion {:?}", self.args)
    }
}

impl ReciprocalRankFusion {
    pub fn from_ctx(session_context: Arc<SessionContext>) -> Self {
        Self {
            args: vec![],
            session_context,
        }
    }

    #[must_use]
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    pub fn with_args(mut self, args: &[Expr]) -> Self {
        self.args = args.to_vec();
        self
    }

    fn default_error<T>() -> Result<T, DataFusionError> {
        exec_err!("This is a bug! {RRF_UDF_NAME} should be rewritten by an optimizer rule.")
    }
}

/// This is only implemented as a documentation stub, so that we show up in `SHOW FUNCTIONS`
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

impl TableFunctionImpl for ReciprocalRankFusion {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(
            ReciprocalRankFusion::from_ctx(Arc::clone(&self.session_context)).with_args(args),
        ))
    }
}

#[async_trait]
impl TableProvider for ReciprocalRankFusion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(self.schema())))
    }
}
