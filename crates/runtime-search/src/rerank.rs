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
#![allow(clippy::missing_errors_doc)]

//! `rerank(input, model => 'name', document => 'col', ...)` — a UDTF that
//! reorders a scored result set by a reranker model's relevance judgements.
//!
//! Accepts either a scored search result (nested `vector_search`,
//! `text_search`, or `rrf` invocation) or a bare table reference. When the
//! input is a nested search UDTF, the query string is auto-propagated to the
//! reranker; otherwise the caller must pass `query => '...'` explicitly.
//!
//! Model resolution falls back gracefully:
//!  1. `rerankers` store — used for native cross-encoder providers (Cohere,
//!     Voyage, Jina, local BGE). Empty on first boot; populated as native
//!     provider support lands.
//!  2. `chat_models` store — any registered chat model can be used as a
//!     reranker via [`llms::rerank::LlmRerank`] with a built-in listwise
//!     prompt template. No extra configuration required.
//!
//! Output schema: `schema(input) ∪ {rerank_score}` (after dropping the input's
//! `_score`/`_fused_score` to avoid confusion). Rows are sorted by
//! `rerank_score DESC` and limited to the requested `limit` (or all rows).

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Weak};

use arrow::array::{Array, ArrayRef, Float32Array, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Column, exec_err};
use datafusion::datasource::{DefaultTableSource, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_expr::TableProviderFilterPushDown;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDFImpl};
use futures::TryStreamExt;
use llms::rerank::{LlmRerank, LlmStrategy, Rerank, RerankerModelStore};
use tokio::sync::RwLock;
use tracing::Instrument;

use llms::chat::Chat;
use runtime_query_engine::query_engine::QueryEngine;

use crate::rrf::{RRF_FUSED_SCORE_COLUMN_NAME, RRF_UDF_NAME};
use crate::udtf::{TEXT_SEARCH_UDTF_NAME, VECTOR_SEARCH_UDTF_NAME, table_ref_from_column_expr};

pub const SPICE_DEFAULT_CATALOG: &str = "spice";
pub const SPICE_DEFAULT_SCHEMA: &str = "public";

pub type ChatModelStore = HashMap<String, Arc<dyn Chat>>;

pub static RERANK_UDTF_NAME: &str = "rerank";

/// Signature accepts a variadic positional input plus named arguments.
/// Parameter names are declared so `DataFusion` v51+ named-arg syntax works.
pub static RERANK_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    let param_names = vec![
        "input".to_string(),
        "model".to_string(),
        "document".to_string(),
        "query".to_string(),
        "limit".to_string(),
        "strategy".to_string(),
        "prompt_template".to_string(),
    ];
    match Signature::user_defined(Volatility::Volatile).with_parameter_names(param_names) {
        Ok(sig) => sig,
        Err(_) => Signature::variadic_any(Volatility::Volatile),
    }
});

/// Output column for reranker scores. Chosen distinct from `_score` /
/// `_fused_score` so downstream callers can tell which stage produced which
/// score. The reranker output drops the upstream score columns and keeps only
/// `rerank_score` — the fresh relevance judgement is what matters.
pub const RERANK_SCORE_COLUMN: &str = "rerank_score";

/// Upper bound on the number of candidate rows a single `rerank()` invocation
/// will score. Guards the bare-table path (`FROM rerank(tbl, ...)`) against
/// accidentally dispatching tens of thousands of remote LLM calls, and acts as
/// a defensive ceiling for nested search UDTFs whose `limit =>` isn't set.
/// The rerank's own `limit =>` governs output size only — it intentionally does
/// not shrink the candidate pool, so the recall-then-rerank workflow can pass
/// a larger inner `limit =>` and a smaller outer `limit =>`.
const DEFAULT_MAX_CANDIDATES: usize = 1000;

/// Parsed form of a `rerank(...)` invocation, with auto-propagation resolved.
#[derive(Debug, Clone)]
pub struct RerankTableFuncArgs {
    /// The input provider expression (either a nested search UDTF call or a
    /// table reference). Resolution happens lazily in `TableFunctionImpl::call`
    /// so parse errors surface independently of table-existence errors.
    pub input: RerankInput,
    /// Reranker model name. Optional when exactly one model is registered.
    pub model: Option<String>,
    /// Column containing the text to score. Required.
    pub document: String,
    /// Query string. Auto-propagated from a nested search UDTF if omitted.
    pub query: Option<String>,
    /// Max rows to return.
    pub limit: Option<usize>,
    /// LLM strategy when using an LLM-as-reranker (ignored by native rerankers).
    pub strategy: Option<LlmStrategy>,
    /// Optional override of the built-in prompt template for LLM rerankers.
    pub prompt_template: Option<String>,
}

#[derive(Debug, Clone)]
pub enum RerankInput {
    /// Nested UDTF call (`vector_search`, `text_search`, or `rrf`). Stored as
    /// the raw `ScalarFunction` expression so we can delegate to the session's
    /// table-function registry — same mechanism `rrf` uses for its children.
    NestedUdtf(ScalarFunction),
    /// Bare table reference.
    Table(TableReference),
}

/// Scan `input_expr` for a query string we can reuse as the reranker query.
///
/// The traversal matches the nesting rules `rerank` accepts: one level for
/// search UDTFs, an additional level through `rrf`. For `vector_search`, the
/// 2nd positional slot may be a plain `Utf8` literal (single-query) or a
/// `make_array(...)` / `List` literal (multi-query / late-interaction); in
/// both cases we pick the first string. Anything else returns `None` and the
/// caller must supply `query => '...'` explicitly.
fn extract_query_literal(input_expr: &Expr) -> Option<String> {
    let Expr::ScalarFunction(sf) = input_expr else {
        return None;
    };
    match sf.func.name() {
        name if name == VECTOR_SEARCH_UDTF_NAME || name == TEXT_SEARCH_UDTF_NAME => {
            // Skip named args (carrying spice.parameter_name metadata) when
            // counting positional slots so `vector_search(docs, 'q',
            // distance_metric => 'l2')` still surfaces `q` at the 2nd slot.
            let mut positional = sf.args.iter().filter(|a| !is_named_arg(a));
            let _tbl = positional.next()?;
            extract_first_utf8(positional.next()?)
        }
        name if name == RRF_UDF_NAME => {
            // rrf: find the first inner scalar-function and recurse. Alias
            // wrappers (DataFusion v51+ for named args) are unwrapped first.
            for arg in &sf.args {
                let unwrapped = match arg {
                    Expr::Alias(a) => a.expr.as_ref(),
                    other => other,
                };
                if matches!(unwrapped, Expr::ScalarFunction(_))
                    && let Some(q) = extract_query_literal(unwrapped)
                {
                    return Some(q);
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract the first string from a query argument. Handles the single-string
/// form (`'q'`), the `make_array(...)` form (SQL `ARRAY[...]`), and the
/// `ScalarValue::List` form that a literal array parses into. For multi-query
/// arrays we pick the first element — the reranker needs a single query and
/// the first query is the semantically primary one (matching how
/// `VectorSearchTableFuncArgs.query` is populated).
fn extract_first_utf8(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(q)), _) => Some(q.clone()),
        Expr::ScalarFunction(inner) if inner.func.name().eq_ignore_ascii_case("make_array") => {
            inner.args.iter().find_map(|a| match a {
                Expr::Literal(ScalarValue::Utf8(Some(q)), _) => Some(q.clone()),
                _ => None,
            })
        }
        Expr::Literal(ScalarValue::List(arr), _) => {
            if arr.is_empty() {
                return None;
            }
            let inner = arr.value(0);
            let strings = inner.as_any().downcast_ref::<StringArray>()?;
            if strings.is_empty() || strings.is_null(0) {
                None
            } else {
                Some(strings.value(0).to_string())
            }
        }
        _ => None,
    }
}

fn is_named_arg(e: &Expr) -> bool {
    match e {
        Expr::Literal(_, Some(meta)) => meta.inner().contains_key("spice.parameter_name"),
        Expr::Alias(alias) => alias
            .metadata
            .as_ref()
            .is_some_and(|m| m.inner().contains_key("spice.parameter_name")),
        _ => false,
    }
}

fn named_param(e: &Expr) -> Option<(&str, &Expr)> {
    match e {
        Expr::Literal(_, Some(meta)) => meta
            .inner()
            .get("spice.parameter_name")
            .map(|n| (n.as_str(), e)),
        Expr::Alias(alias) => alias
            .metadata
            .as_ref()
            .and_then(|m| m.inner().get("spice.parameter_name"))
            .map(|n| (n.as_str(), alias.expr.as_ref())),
        _ => None,
    }
}

fn parse_limit_scalar(scalar: &ScalarValue) -> DataFusionResult<usize> {
    match scalar {
        ScalarValue::Int64(Some(v)) => usize::try_from(*v).map_err(|_| {
            DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: limit value {v} is out of range (must be non-negative)."
            ))
        }),
        ScalarValue::UInt64(Some(v)) => usize::try_from(*v).map_err(|_| {
            DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: limit value {v} exceeds usize range."
            ))
        }),
        ScalarValue::Int32(Some(v)) => usize::try_from(*v).map_err(|_| {
            DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: limit value {v} is out of range (must be non-negative)."
            ))
        }),
        ScalarValue::UInt32(Some(v)) => Ok(*v as usize),
        _ => Err(DataFusionError::Plan(format!(
            "{RERANK_UDTF_NAME}: 'limit' must be a non-negative integer, got: {scalar}"
        ))),
    }
}

impl RerankTableFuncArgs {
    pub fn from_udtf_args(args: &[Expr]) -> DataFusionResult<Self> {
        let mut named: HashMap<&str, &Expr> = HashMap::new();
        let mut positional: Vec<&Expr> = Vec::with_capacity(args.len());
        for a in args {
            if let Some((name, inner)) = named_param(a) {
                named.insert(name, inner);
            } else {
                positional.push(a);
            }
        }

        let input_expr = positional.first().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME} requires an input as the first argument (a search result or table reference)."
            ))
        })?;
        // Reject extra positional args. Every optional parameter uses named
        // syntax (`document => ...`, `model => ...`, etc.); a second
        // positional slot means the caller misspelled a param name or forgot
        // the `=>` — silently dropping it would produce confusingly wrong
        // results. Fail fast with the list of recognized named args.
        if positional.len() > 1 {
            return Err(DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME} only accepts one positional argument (the input); additional parameters must be named. Got {} positional arg(s). Recognized named args: model, document, query, limit, strategy, prompt_template.",
                positional.len()
            )));
        }

        let (input, auto_query) = match input_expr {
            Expr::ScalarFunction(sf) => {
                let q = extract_query_literal(input_expr);
                (RerankInput::NestedUdtf(sf.clone()), q)
            }
            Expr::Column(c) => {
                let tbl_ref = table_ref_from_column_expr(c)
                    .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                    .into();
                (RerankInput::Table(tbl_ref), None)
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "{RERANK_UDTF_NAME}: first argument must be a search UDTF call (e.g. vector_search, text_search, rrf) or a table reference, got: {other:?}"
                )));
            }
        };

        let model = extract_string_named(&named, "model");
        let document = extract_string_named(&named, "document").ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: 'document => <column>' is required (the text column to score against the query)."
            ))
        })?;
        let query_override = extract_string_named(&named, "query");
        let query = query_override.or(auto_query);

        let limit = match named.get("limit") {
            Some(Expr::Literal(scalar, _)) => Some(parse_limit_scalar(scalar)?),
            Some(other) => {
                return Err(DataFusionError::Plan(format!(
                    "{RERANK_UDTF_NAME}: 'limit' must be a literal integer, got: {other:?}"
                )));
            }
            None => None,
        };

        let strategy = match extract_string_named(&named, "strategy") {
            Some(s) => Some(LlmStrategy::parse(&s).map_err(DataFusionError::Plan)?),
            None => None,
        };

        let prompt_template = extract_string_named(&named, "prompt_template");

        Ok(Self {
            input,
            model,
            document,
            query,
            limit,
            strategy,
            prompt_template,
        })
    }
}

fn extract_string_named(named: &HashMap<&str, &Expr>, key: &str) -> Option<String> {
    match named.get(key) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Some(s.clone()),
        // `column` / `strategy` accept identifiers too (`document => content`),
        // which the Spice DataFusion fork wraps as an Expr::Column under the
        // named-arg metadata.
        Some(Expr::Column(Column { name, .. })) => Some(name.clone()),
        _ => None,
    }
}

/// The UDTF scaffold. Analogous to [`VectorSearchTableFunc`] — holds a weak
/// reference to the [`DataFusion`] instance plus the model stores needed to
/// resolve the requested reranker at scan time.
pub struct RerankTableFunc {
    df: Weak<dyn QueryEngine>,
    df_ptr: u64,
    rerankers: Arc<RwLock<RerankerModelStore>>,
    chat_models: Arc<RwLock<ChatModelStore>>,
}

impl std::fmt::Debug for RerankTableFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RerankTableFunc")
            .field("df_ptr", &self.df_ptr)
            .finish_non_exhaustive()
    }
}

impl PartialEq for RerankTableFunc {
    fn eq(&self, other: &Self) -> bool {
        self.df_ptr == other.df_ptr
    }
}

impl Eq for RerankTableFunc {}

impl std::hash::Hash for RerankTableFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.df_ptr.hash(state);
    }
}

impl RerankTableFunc {
    #[must_use]
    pub fn new(
        df: Weak<dyn QueryEngine>,
        rerankers: Arc<RwLock<RerankerModelStore>>,
        chat_models: Arc<RwLock<ChatModelStore>>,
    ) -> Self {
        let df_ptr = df.as_ptr().addr() as u64;
        Self {
            df,
            df_ptr,
            rerankers,
            chat_models,
        }
    }

    fn scalar_invocation_error<T>() -> DataFusionResult<T> {
        exec_err!("{RERANK_UDTF_NAME} does not support scalar invocation.")
    }
}

impl TableFunctionImpl for RerankTableFunc {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let parsed = RerankTableFuncArgs::from_udtf_args(args)?;

        let df = self.df.upgrade().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: DataFusion instance has been dropped."
            ))
        })?;

        // Resolve the input to a concrete TableProvider. For nested UDTFs,
        // delegate to the session's table-function registry — exactly how
        // `rrf` composes its children, so any future search UDTF is picked up
        // for free.
        let input_is_nested = matches!(&parsed.input, RerankInput::NestedUdtf(_));
        let input_provider: Arc<dyn TableProvider> = match &parsed.input {
            RerankInput::NestedUdtf(sf) => df
                .session_context()
                .table_function(sf.func.name())
                .and_then(|udtf| udtf.create_table_provider(&sf.args))?,
            RerankInput::Table(tbl) => df.get_table_sync(tbl).ok_or_else(|| {
                DataFusionError::Plan(format!("{RERANK_UDTF_NAME}: table '{tbl}' does not exist."))
            })?,
        };

        Ok(Arc::new(RerankUDTFProvider {
            args: parsed,
            input: input_provider,
            input_is_nested,
            rerankers: Arc::clone(&self.rerankers),
            chat_models: Arc::clone(&self.chat_models),
        }))
    }
}

/// Scalar stub so `rerank(...)` can nest inside other UDTFs (same trick
/// `vector_search`/`text_search`/`rrf` use for their scalar projection).
impl ScalarUDFImpl for RerankTableFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        RERANK_UDTF_NAME
    }
    fn signature(&self) -> &Signature {
        &RERANK_SIGNATURE
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Self::scalar_invocation_error()
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Self::scalar_invocation_error()
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }
}

/// Materialization provider: scans the input, calls the reranker, and returns
/// an in-memory table sorted by `rerank_score DESC`. Materialization is the
/// right boundary for reranking — callers have already narrowed candidates
/// with a recall stage, and rerankers need to see the full candidate set
/// (listwise) or issue one call per candidate (pointwise).
pub struct RerankUDTFProvider {
    args: RerankTableFuncArgs,
    input: Arc<dyn TableProvider>,
    /// True when `args.input` was a nested search UDTF (`vector_search`,
    /// `text_search`, `rrf`). The UDTF already caps its own output via its
    /// `limit` argument, so we let it decide the candidate pool size rather
    /// than pushing the rerank's `limit` (which is an *output* cap) into the
    /// inner scan. Bare-table inputs have no such cap, so we apply
    /// `DEFAULT_MAX_CANDIDATES` defensively.
    input_is_nested: bool,
    rerankers: Arc<RwLock<RerankerModelStore>>,
    chat_models: Arc<RwLock<ChatModelStore>>,
}

impl std::fmt::Debug for RerankUDTFProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RerankUDTFProvider")
            .field("args", &self.args)
            .finish_non_exhaustive()
    }
}

impl RerankUDTFProvider {
    /// Build the output schema: input schema, minus `_score` / `_fused_score`
    /// (they'd be confusing alongside `rerank_score`), plus `rerank_score`.
    fn output_schema(input: &SchemaRef) -> SchemaRef {
        let mut fields: Vec<Field> = input
            .fields()
            .iter()
            .filter(|f| f.name() != "_score" && f.name() != RRF_FUSED_SCORE_COLUMN_NAME)
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(RERANK_SCORE_COLUMN, DataType::Float32, false));
        Arc::new(Schema::new(fields))
    }

    /// Build projection expressions that keep only the columns `RerankExec`
    /// needs: the document column plus any caller-projected output columns
    /// (excluding `rerank_score`, which is computed later).
    fn rerank_projection_exprs(
        projection: &[usize],
        document: &str,
        output_schema: &SchemaRef,
        available: &datafusion::common::DFSchema,
    ) -> Vec<Expr> {
        let mut needed: HashSet<&str> = HashSet::new();
        needed.insert(document);
        for &idx in projection {
            let name = output_schema.field(idx).name();
            if name != RERANK_SCORE_COLUMN {
                needed.insert(name.as_str());
            }
        }
        available
            .fields()
            .iter()
            .filter(|f| needed.contains(f.name().as_str()))
            .map(|f| Expr::Column(Column::new_unqualified(f.name().clone())))
            .collect()
    }

    /// Pick the reranker model to use. Checks the rerankers store first, then
    /// falls back to wrapping a chat model in `LlmRerank`.
    async fn resolve_reranker(&self) -> DataFusionResult<Arc<dyn Rerank>> {
        // 1. Explicit model name.
        if let Some(name) = &self.args.model {
            let rerankers = self.rerankers.read().await;
            if let Some(rr) = rerankers.get(name) {
                return Ok(Arc::clone(rr));
            }
            drop(rerankers);
            let chats = self.chat_models.read().await;
            if let Some(chat) = chats.get(name) {
                let mut adapter = LlmRerank::new(name, Arc::clone(chat));
                if let Some(strategy) = self.args.strategy {
                    adapter = adapter.with_strategy(strategy);
                }
                if let Some(tpl) = &self.args.prompt_template {
                    adapter = adapter.with_prompt_template(Some(tpl.clone()));
                }
                return Ok(Arc::new(adapter));
            }
            return Err(DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: model '{name}' not found in rerankers or chat_models."
            )));
        }

        // 2. No model name — auto-pick if exactly one is available.
        let rerankers = self.rerankers.read().await;
        let chats = self.chat_models.read().await;
        let total = rerankers.len() + chats.len();
        match total {
            0 => Err(DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: no rerankers or chat models configured. Add one to your Spicepod and reference it via `model => '<name>'`."
            ))),
            1 => {
                if let Some((name, rr)) = rerankers.iter().next() {
                    let _ = name;
                    Ok(Arc::clone(rr))
                } else if let Some((name, chat)) = chats.iter().next() {
                    let mut adapter = LlmRerank::new(name, Arc::clone(chat));
                    if let Some(strategy) = self.args.strategy {
                        adapter = adapter.with_strategy(strategy);
                    }
                    if let Some(tpl) = &self.args.prompt_template {
                        adapter = adapter.with_prompt_template(Some(tpl.clone()));
                    }
                    Ok(Arc::new(adapter))
                } else {
                    unreachable!("total == 1 but both stores empty")
                }
            }
            _ => Err(DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: multiple models configured. Specify which with `model => '<name>'`."
            ))),
        }
    }

    /// Extract the configured document column from a record batch. Returns a
    /// vector aligned with the batch rows — `None` for NULL cells so the
    /// caller can skip them in the reranker call and preserve NULL semantics
    /// instead of silently turning NULL into `""`.
    ///
    /// Chunked/list-typed columns aren't supported: the nested search UDTF
    /// handles chunking internally, so by the time results reach `rerank` the
    /// document column is scalar.
    fn extract_documents(
        batch: &RecordBatch,
        document_col: &str,
    ) -> DataFusionResult<Vec<Option<String>>> {
        let idx = batch.schema().index_of(document_col).map_err(|_| {
            DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: document column '{document_col}' not found in input schema. Available columns: {}.",
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        let col = batch.column(idx);
        if let Some(strings) = col.as_any().downcast_ref::<StringArray>() {
            return Ok((0..strings.len())
                .map(|i| {
                    if strings.is_null(i) {
                        None
                    } else {
                        Some(strings.value(i).to_string())
                    }
                })
                .collect());
        }
        if let Some(strings) = col.as_any().downcast_ref::<LargeStringArray>() {
            return Ok((0..strings.len())
                .map(|i| {
                    if strings.is_null(i) {
                        None
                    } else {
                        Some(strings.value(i).to_string())
                    }
                })
                .collect());
        }
        Err(DataFusionError::Plan(format!(
            "{RERANK_UDTF_NAME}: document column '{document_col}' must be Utf8 or LargeUtf8, got {:?}.",
            col.data_type()
        )))
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for RerankUDTFProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Self::output_schema(&self.input.schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // `rerank_score` is computed by the reranker and cannot be pushed
        // down. All other filters reference base columns that exist in the
        // inner provider's schema — we apply them via `LogicalPlanBuilder`
        // in `scan()`, so DataFusion's optimizer will push them into the
        // inner provider or add a FilterExec internally as needed.
        Ok(filters
            .iter()
            .map(|f| {
                let refs_computed = f
                    .column_refs()
                    .iter()
                    .any(|c| c.name() == RERANK_SCORE_COLUMN);

                if refs_computed {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    TableProviderFilterPushDown::Exact
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let query = self.args.query.clone().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{RERANK_UDTF_NAME}: query string is required. Either pass a nested search UDTF (e.g. vector_search/text_search/rrf) whose query we can auto-propagate, or set `query => '<text>'` explicitly."
            ))
        })?;

        let reranker = self.resolve_reranker().await?;

        let full_output_schema = self.schema();

        // Wrap in a LogicalPlan so DataFusion's optimizer can push filters
        // into the provider and add a FilterExec for unsupported ones.
        let source = Arc::new(DefaultTableSource::new(Arc::clone(&self.input)));
        let mut builder = LogicalPlanBuilder::scan("__rerank_candidates", source, None)?;

        if let Some(f) = filters.iter().cloned().reduce(Expr::and) {
            builder = builder.filter(f)?;
        }

        // Cap the candidate pool for bare-table inputs. Nested search UDTFs
        // manage their own candidate pool via their `limit =>` argument.
        if !self.input_is_nested {
            builder = builder.limit(0, Some(DEFAULT_MAX_CANDIDATES))?;
        }

        // Project to only the columns RerankExec needs: the document column
        // plus any output columns the caller requested. The optimizer will
        // automatically include filter columns in the scan.
        if let Some(proj) = projection {
            let exprs = Self::rerank_projection_exprs(
                proj,
                &self.args.document,
                &full_output_schema,
                builder.schema().as_ref(),
            );
            builder = builder.project(exprs)?;
        }

        let logical_plan = builder.build()?;
        let input_plan = state.create_physical_plan(&logical_plan).await?;

        // Use the actual physical plan's schema — may be reduced by projection
        // pushdown. Compute output schema from the reduced input so RerankExec's
        // output matches what it actually produces.
        let actual_input_schema = input_plan.schema();
        let output_schema = Self::output_schema(&actual_input_schema);

        // Effective output limit: the smaller of the rerank `limit =>` arg and
        // DataFusion's pushed-down LIMIT. Either may be None (= unlimited).
        let effective_limit = match (self.args.limit, limit) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (some, None) | (None, some) => some,
        };

        let rerank_exec: Arc<dyn ExecutionPlan> = Arc::new(RerankExec::new(
            input_plan,
            Arc::clone(&output_schema),
            actual_input_schema,
            query,
            self.args.document.clone(),
            reranker,
            effective_limit,
            self.input_is_nested,
        ));

        // Remap projection indices from the full output schema to the reduced
        // output schema. DataFusion passes indices based on `self.schema()`
        // (the full output), but RerankExec now produces a subset.
        if let Some(proj) = projection {
            let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                .iter()
                .map(|&idx| {
                    let name = full_output_schema.field(idx).name().clone();
                    let reduced_idx = output_schema
                        .index_of(&name)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    Ok((
                        Arc::new(PhysicalColumn::new(&name, reduced_idx)) as Arc<dyn PhysicalExpr>,
                        name,
                    ))
                })
                .collect::<DataFusionResult<Vec<_>>>()?;
            return Ok(Arc::new(ProjectionExec::try_new(proj_exprs, rerank_exec)?));
        }

        Ok(rerank_exec)
    }

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        None
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(
        &self,
    ) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn datafusion::catalog::Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> DataFusionResult<datafusion::catalog::ScanResult> {
        let filters = args.filters().unwrap_or(&[]);
        let projection = args.projection().map(<[usize]>::to_vec);
        let limit = args.limit();
        let plan = self
            .scan(state, projection.as_ref(), filters, limit)
            .await?;
        Ok(datafusion::catalog::ScanResult::new(plan))
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        None
    }

    async fn insert_into(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "RerankUDTFProvider does not support insert_into".to_string(),
        ))
    }

    async fn delete_from(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "RerankUDTFProvider does not support delete_from".to_string(),
        ))
    }

    async fn update(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "RerankUDTFProvider does not support update".to_string(),
        ))
    }

    async fn truncate(
        &self,
        _state: &dyn datafusion::catalog::Session,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "RerankUDTFProvider does not support truncate".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// RerankExec — deferred physical plan
// ---------------------------------------------------------------------------

/// Physical execution node that defers reranker API calls to `execute()` time.
///
/// At plan time (`TableProvider::scan`) only the *input* plan is built and
/// optimized; the actual data materialization + reranker invocation happens
/// when the query engine calls `execute()`. This makes `EXPLAIN` instant and
/// allows `DataFusion`'s optimizer to push filters/limits into the inner plan.
struct RerankExec {
    /// Optimized child plan that produces the candidate rows.
    input: Arc<dyn ExecutionPlan>,
    /// Output schema (base columns minus `_score`/`_fused_score`, plus `rerank_score`).
    output_schema: SchemaRef,
    /// Schema of the raw input (before column dropping).
    input_schema: SchemaRef,
    /// The query string passed to the reranker.
    query: String,
    /// Column name containing the document text to score.
    document: String,
    /// Resolved reranker model.
    reranker: Arc<dyn Rerank>,
    /// User-supplied output limit (caps output after sorting by `rerank_score`).
    rerank_limit: Option<usize>,
    /// Whether the input is a nested search UDTF (affects defensive cap).
    input_is_nested: bool,
    /// Cached plan properties.
    properties: PlanProperties,
}

impl RerankExec {
    #[expect(clippy::too_many_arguments)]
    fn new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        input_schema: SchemaRef,
        query: String,
        document: String,
        reranker: Arc<dyn Rerank>,
        rerank_limit: Option<usize>,
        input_is_nested: bool,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            output_schema,
            input_schema,
            query,
            document,
            reranker,
            rerank_limit,
            input_is_nested,
            properties,
        }
    }
}

impl std::fmt::Debug for RerankExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RerankExec")
            .field("query", &self.query)
            .field("document", &self.document)
            .field("rerank_limit", &self.rerank_limit)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for RerankExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let model = self.reranker.model_name().unwrap_or("unknown");
                write!(
                    f,
                    "RerankExec: model={model}, document={}, limit={:?}",
                    self.document, self.rerank_limit,
                )
            }
        }
    }
}

impl ExecutionPlan for RerankExec {
    fn name(&self) -> &'static str {
        "RerankExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "RerankExec expected 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.output_schema),
            Arc::clone(&self.input_schema),
            self.query.clone(),
            self.document.clone(),
            Arc::clone(&self.reranker),
            self.rerank_limit,
            self.input_is_nested,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "RerankExec only supports 1 partition, got partition {partition}"
            )));
        }
        let input = Arc::clone(&self.input);
        let output_schema = Arc::clone(&self.output_schema);
        let input_schema = Arc::clone(&self.input_schema);
        let query = self.query.clone();
        let document = self.document.clone();
        let reranker = Arc::clone(&self.reranker);
        let rerank_limit = self.rerank_limit;
        let input_is_nested = self.input_is_nested;
        let model_name = self.reranker.model_name().unwrap_or("unknown").to_string();

        let stream = futures::stream::once(async move {
            // 1. Execute the child plan and collect candidate batches.
            tracing::debug!(document = %document, "RerankExec: collecting candidate batches..");
            let child_stream = datafusion::physical_plan::execute_stream(input, context)?;
            let mut batches: Vec<RecordBatch> = child_stream.try_collect().await?;

            // Defensive hard cap on materialized candidates.
            let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
            if !input_is_nested && total > DEFAULT_MAX_CANDIDATES {
                batches = truncate_batches(batches, DEFAULT_MAX_CANDIDATES);
            }

            tracing::debug!(candidates = total, document = %document, "RerankExec: collected candidate batches");

            // Start the task_history span after candidate collection so `execution_duration_ms` measures only
            // the reranker work (extract docs → API call → sort), not the child plan execution which has its own traces.
            let span = tracing::span!(target: "task_history", tracing::Level::INFO, "rerank", input = %query);
            async {
            tracing::info!(target: "task_history", model = %model_name, document = %document, candidates = total, "labels");

            // 2. Concatenate into a single batch for the reranker.
            let concatenated = if batches.is_empty() {
                RecordBatch::new_empty(Arc::clone(&input_schema))
            } else {
                arrow::compute::concat_batches(&input_schema, batches.iter())
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
            };

            // 3. Extract document texts, skipping NULLs.
            let docs_opt = RerankUDTFProvider::extract_documents(&concatenated, &document)?;
            let (non_null_indices, non_null_docs): (Vec<usize>, Vec<String>) = docs_opt
                .iter()
                .enumerate()
                .filter_map(|(i, d)| d.as_ref().map(|s| (i, s.clone())))
                .unzip();

            // 4. Call the reranker.
            tracing::debug!(
                non_null = non_null_docs.len(),
                "RerankExec: calling reranker"
            );
            let non_null_scores = if non_null_docs.is_empty() {
                Vec::new()
            } else {
                reranker
                    .rerank(&query, &non_null_docs)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            tracing::debug!(
                scores = non_null_scores.len(),
                "RerankExec: reranker returned candidate scores"
            );

            if non_null_scores.len() != non_null_docs.len() {
                return Err(DataFusionError::Execution(format!(
                    "{RERANK_UDTF_NAME}: reranker returned {} scores for {} documents.",
                    non_null_scores.len(),
                    non_null_docs.len()
                )));
            }

            // 5. Scatter scores. NULL rows keep `NEG_INFINITY` so they always
            // sort below every scored document, even when a reranker
            // (e.g. a cross-encoder) returns negative relevance scores.
            let mut scores = vec![f32::NEG_INFINITY; docs_opt.len()];
            for (idx, score) in non_null_indices.iter().zip(non_null_scores.iter()) {
                scores[*idx] = *score;
            }

            // 6. Build output batch: drop _score/_fused_score, append rerank_score.
            let keep_cols: Vec<usize> = input_schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    if f.name() == "_score" || f.name() == RRF_FUSED_SCORE_COLUMN_NAME {
                        None
                    } else {
                        Some(i)
                    }
                })
                .collect();
            let mut columns: Vec<ArrayRef> = keep_cols
                .iter()
                .map(|&i| Arc::clone(concatenated.column(i)))
                .collect();
            columns.push(Arc::new(Float32Array::from(scores)) as ArrayRef);

            let unsorted = RecordBatch::try_new(Arc::clone(&output_schema), columns)?;

            // 7. Sort by rerank_score DESC, apply output limit.
            sort_by_rerank_score_desc(&unsorted, rerank_limit)
                .inspect(|batch| {
                    tracing::info!(target: "task_history", rows_produced = batch.num_rows(), "labels");
                    let preview = batch.slice(0, batch.num_rows().min(3));
                    let captured_output = search::aggregation::write_to_json_string(&[preview]).unwrap_or_default();
                    tracing::info!(target: "task_history", captured_output = %captured_output);
                })
                .inspect_err(|e| {
                    tracing::error!(target: "task_history", "{e}");
                })
            }.instrument(span).await
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream,
        )))
    }
}

/// Keep only the first `limit` rows across a sequence of batches. Drops
/// whole batches once the budget is exhausted, and slices the last partial
/// batch. Used as a defensive hard cap when the input provider doesn't honor
/// a pushed-down scan limit.
fn truncate_batches(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
    let mut remaining = limit;
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let n = batch.num_rows();
        if n <= remaining {
            remaining -= n;
            out.push(batch);
        } else {
            out.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

fn sort_by_rerank_score_desc(
    batch: &RecordBatch,
    limit: Option<usize>,
) -> DataFusionResult<RecordBatch> {
    use arrow::compute::{SortOptions, sort_to_indices, take};

    let score_idx = batch
        .schema()
        .index_of(RERANK_SCORE_COLUMN)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let indices = sort_to_indices(
        batch.column(score_idx),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
        limit,
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let sorted_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| {
            take(c, &indices, None).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    RecordBatch::try_new(batch.schema(), sorted_columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rrf;
    use arrow::array::{Float32Array, StringArray};
    use arrow::util::pretty::pretty_format_batches;
    use async_trait::async_trait;
    use datafusion::logical_expr::expr::FieldMetadata;
    use datafusion::logical_expr::{Volatility, create_udf};
    use futures::TryStreamExt;
    use runtime_request_context::{Protocol, RequestContext};
    use std::collections::BTreeMap;

    fn lit_utf8(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }

    fn named_lit_utf8(name: &str, value: &str) -> Expr {
        let meta = FieldMetadata::new(BTreeMap::from([(
            "spice.parameter_name".to_string(),
            name.to_string(),
        )]));
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), Some(meta))
    }

    fn stub_udf(name: &str) -> Arc<datafusion::logical_expr::ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![],
            DataType::Utf8,
            Volatility::Stable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".into()))))),
        ))
    }

    fn vector_search_call(query: &str) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            stub_udf(VECTOR_SEARCH_UDTF_NAME),
            vec![
                Expr::Column(Column::new_unqualified("docs")),
                lit_utf8(query),
            ],
        ))
    }

    fn text_search_call(query: &str) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            stub_udf(TEXT_SEARCH_UDTF_NAME),
            vec![
                Expr::Column(Column::new_unqualified("docs")),
                lit_utf8(query),
            ],
        ))
    }

    fn rrf_call(children: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(stub_udf(RRF_UDF_NAME), children))
    }

    #[test]
    fn extract_query_from_vector_search() {
        let q = extract_query_literal(&vector_search_call("ocelots"));
        assert_eq!(q.as_deref(), Some("ocelots"));
    }

    #[test]
    fn extract_query_from_text_search() {
        let q = extract_query_literal(&text_search_call("ferrets"));
        assert_eq!(q.as_deref(), Some("ferrets"));
    }

    #[test]
    fn extract_query_from_rrf_uses_first_child() {
        let q = extract_query_literal(&rrf_call(vec![
            vector_search_call("badgers"),
            text_search_call("badgers"),
        ]));
        assert_eq!(q.as_deref(), Some("badgers"));
    }

    #[test]
    fn extract_query_skips_named_args_in_vector_search() {
        // vector_search(docs, distance_metric => 'l2', 'pangolins')
        // — named arg lands at index 1 in the raw arg list, but the query
        // is still the Utf8 literal at the 2nd positional slot.
        let named = named_lit_utf8("distance_metric", "l2");
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            stub_udf(VECTOR_SEARCH_UDTF_NAME),
            vec![
                Expr::Column(Column::new_unqualified("docs")),
                named,
                lit_utf8("pangolins"),
            ],
        ));
        assert_eq!(extract_query_literal(&expr).as_deref(), Some("pangolins"));
    }

    #[test]
    fn extract_query_returns_none_for_non_search_call() {
        let unknown = Expr::ScalarFunction(ScalarFunction::new_udf(
            stub_udf("some_other_fn"),
            vec![lit_utf8("x")],
        ));
        assert_eq!(extract_query_literal(&unknown), None);
    }

    #[test]
    fn extract_query_returns_none_for_column_ref() {
        let col = Expr::Column(Column::new_unqualified("docs"));
        assert_eq!(extract_query_literal(&col), None);
    }

    #[test]
    fn parse_args_requires_document() {
        let args = vec![vector_search_call("q"), named_lit_utf8("model", "my_model")];
        let err =
            RerankTableFuncArgs::from_udtf_args(&args).expect_err("must require document column");
        assert!(err.to_string().contains("'document =>"));
    }

    #[test]
    fn parse_args_auto_propagates_query() {
        let args = vec![
            vector_search_call("llamas"),
            named_lit_utf8("document", "content"),
            named_lit_utf8("model", "my_model"),
        ];
        let parsed = RerankTableFuncArgs::from_udtf_args(&args).expect("parse ok");
        assert_eq!(parsed.query.as_deref(), Some("llamas"));
        assert_eq!(parsed.model.as_deref(), Some("my_model"));
        assert_eq!(parsed.document, "content");
    }

    #[test]
    fn parse_args_explicit_query_overrides_auto() {
        let args = vec![
            vector_search_call("auto_query"),
            named_lit_utf8("document", "content"),
            named_lit_utf8("query", "explicit_query"),
        ];
        let parsed = RerankTableFuncArgs::from_udtf_args(&args).expect("parse ok");
        assert_eq!(parsed.query.as_deref(), Some("explicit_query"));
    }

    #[test]
    fn parse_args_bare_table_needs_explicit_query_at_scan() {
        // A bare Column is a valid input, but `query` stays None until
        // explicitly provided. Scan-time enforcement is where the error
        // surfaces; parse-time just records what we have.
        let args = vec![
            Expr::Column(Column::new_unqualified("tickets")),
            named_lit_utf8("document", "body"),
        ];
        let parsed = RerankTableFuncArgs::from_udtf_args(&args).expect("parse ok");
        assert!(matches!(parsed.input, RerankInput::Table(_)));
        assert_eq!(parsed.query, None);
    }

    #[test]
    fn parse_args_parses_strategy() {
        let args = vec![
            vector_search_call("q"),
            named_lit_utf8("document", "content"),
            named_lit_utf8("strategy", "pointwise"),
        ];
        let parsed = RerankTableFuncArgs::from_udtf_args(&args).expect("parse ok");
        assert_eq!(parsed.strategy, Some(LlmStrategy::Pointwise));
    }

    #[test]
    fn parse_args_parses_limit() {
        let meta = FieldMetadata::new(BTreeMap::from([(
            "spice.parameter_name".to_string(),
            "limit".to_string(),
        )]));
        let args = vec![
            vector_search_call("q"),
            named_lit_utf8("document", "content"),
            Expr::Literal(ScalarValue::UInt64(Some(10)), Some(meta)),
        ];
        let parsed = RerankTableFuncArgs::from_udtf_args(&args).expect("parse ok");
        assert_eq!(parsed.limit, Some(10));
    }

    #[test]
    fn output_schema_drops_score_columns_and_adds_rerank_score() {
        let input_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
            Field::new("_score", DataType::Float32, true),
            // RRF emits its fused score as `_fused_score` (see
            // `RRF_FUSED_SCORE_COLUMN_NAME`); the UDTF must drop it alongside
            // `_score` so downstream rows only see `rerank_score`.
            Field::new(RRF_FUSED_SCORE_COLUMN_NAME, DataType::Float64, true),
        ]));
        let out = RerankUDTFProvider::output_schema(&input_schema);
        let names: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "content", RERANK_SCORE_COLUMN]);
    }

    #[test]
    fn sort_by_rerank_score_orders_desc_and_applies_limit() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(RERANK_SCORE_COLUMN, DataType::Float32, false),
        ]));
        let ids = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let scores = Arc::new(Float32Array::from(vec![0.1, 0.9, 0.5])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![ids, scores]).expect("batch");

        let sorted = sort_by_rerank_score_desc(&batch, Some(2)).expect("sort");
        assert_eq!(sorted.num_rows(), 2);
        let id_col = sorted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("strings");
        assert_eq!(id_col.value(0), "b");
        assert_eq!(id_col.value(1), "c");
    }

    #[test]
    fn parse_args_rejects_extra_positional_args() {
        // `rerank(vs, 'not_named', document => 'content')` — the second
        // positional looks like a typoed/forgotten `=>`; silently dropping it
        // would produce misleading output. Fail fast.
        let args = vec![
            vector_search_call("q"),
            lit_utf8("not_named"),
            named_lit_utf8("document", "content"),
        ];
        let err = RerankTableFuncArgs::from_udtf_args(&args)
            .expect_err("extra positional arg should be rejected");
        assert!(
            err.to_string()
                .contains("only accepts one positional argument"),
            "got: {err}"
        );
    }

    #[test]
    fn extract_query_handles_make_array() {
        // `vector_search(docs, make_array('a', 'b'))` — multi-query path;
        // auto-propagation should pick the first string.
        use datafusion::functions_nested::make_array::make_array_udf;
        let make_array = make_array_udf();
        let multi_query = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(&make_array),
            vec![lit_utf8("red"), lit_utf8("round")],
        ));
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            stub_udf(VECTOR_SEARCH_UDTF_NAME),
            vec![Expr::Column(Column::new_unqualified("docs")), multi_query],
        ));
        assert_eq!(extract_query_literal(&expr).as_deref(), Some("red"));
    }

    #[test]
    fn extract_query_handles_list_literal() {
        // SQL `ARRAY['a', 'b']` parses as ScalarValue::List.
        use arrow::datatypes::DataType as ArrowDataType;
        let scalars = vec![
            ScalarValue::Utf8(Some("climate".to_string())),
            ScalarValue::Utf8(Some("economy".to_string())),
        ];
        let arr = ScalarValue::new_list_nullable(&scalars, &ArrowDataType::Utf8);
        let multi_query = Expr::Literal(ScalarValue::List(arr), None);
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            stub_udf(VECTOR_SEARCH_UDTF_NAME),
            vec![Expr::Column(Column::new_unqualified("docs")), multi_query],
        ));
        assert_eq!(extract_query_literal(&expr).as_deref(), Some("climate"));
    }

    #[test]
    fn extract_documents_preserves_nulls() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "content",
            DataType::Utf8,
            true,
        )]));
        let col = Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![col]).expect("batch");
        let docs = RerankUDTFProvider::extract_documents(&batch, "content").expect("extract");
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].as_deref(), Some("hello"));
        assert!(
            docs[1].is_none(),
            "NULL row must stay None, not become an empty string"
        );
        assert_eq!(docs[2].as_deref(), Some("world"));
    }

    #[test]
    fn truncate_batches_caps_at_limit() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let batch = |vals: &[&str]| -> RecordBatch {
            let col = Arc::new(StringArray::from(vals.to_vec())) as ArrayRef;
            RecordBatch::try_new(Arc::clone(&schema), vec![col]).expect("batch")
        };

        // Two 3-row batches, cap at 4 rows → first batch fully, second sliced
        // to one row, total 4.
        let out = truncate_batches(vec![batch(&["a", "b", "c"]), batch(&["d", "e", "f"])], 4);
        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4);
        assert_eq!(out[0].num_rows(), 3);
        assert_eq!(out[1].num_rows(), 1);

        // Cap at 0 → no batches returned.
        let none = truncate_batches(vec![batch(&["a"])], 0);
        assert!(none.is_empty());

        // Cap ≥ total → all batches returned unchanged.
        let all = truncate_batches(vec![batch(&["a"]), batch(&["b"])], 10);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].num_rows(), 1);
        assert_eq!(all[1].num_rows(), 1);
    }

    #[test]
    fn null_rows_sort_below_negative_scores() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(RERANK_SCORE_COLUMN, DataType::Float32, false),
        ]));
        let ids = Arc::new(StringArray::from(vec![
            "valid_neg",
            "null_row",
            "valid_pos",
        ])) as ArrayRef;
        let scores = Arc::new(Float32Array::from(vec![-0.5, f32::NEG_INFINITY, 0.3])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![ids, scores]).expect("batch");

        let sorted = sort_by_rerank_score_desc(&batch, None).expect("sort");
        let id_col = sorted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("strings");
        assert_eq!(id_col.value(0), "valid_pos");
        assert_eq!(id_col.value(1), "valid_neg");
        assert_eq!(
            id_col.value(2),
            "null_row",
            "NULL rows must sort last even with negative reranker scores"
        );
    }

    /// Deterministic reranker for tests. Returns pre-configured scores in order, sliced to `documents.len()`.
    #[derive(Debug)]
    struct MockRerank {
        scores: Vec<f32>,
    }

    #[async_trait]
    impl llms::rerank::Rerank for MockRerank {
        async fn rerank(
            &self,
            _query: &str,
            documents: &[String],
        ) -> llms::rerank::Result<Vec<f32>> {
            Ok(self.scores[..documents.len()].to_vec())
        }

        fn model_name(&self) -> Option<&str> {
            Some("mock_reranker")
        }
    }

    async fn make_rerank_session() -> DataFusionResult<(
        Arc<SessionContext>,
        Arc<RwLock<llms::rerank::RerankerModelStore>>,
        Arc<RwLock<ChatModelStore>>,
    )> {
        let ctx = Arc::new(SessionContext::new());
        ctx.state().config_mut().set_extension(Arc::new(
            RequestContext::builder(Protocol::Internal).build(),
        ));

        // Register RRF
        ctx.register_udf(rrf::ReciprocalRankFusion::from_ctx(&ctx).into());
        ctx.register_udtf(
            rrf::RRF_UDF_NAME,
            Arc::new(rrf::ReciprocalRankFusion::from_ctx(&ctx)),
        );

        let rerankers: Arc<RwLock<llms::rerank::RerankerModelStore>> =
            Arc::new(RwLock::new(HashMap::new()));
        let chat_models: Arc<RwLock<ChatModelStore>> = Arc::new(RwLock::new(HashMap::new()));

        // Register rerank UDTF
        let weak_ctx: std::sync::Weak<dyn runtime_query_engine::query_engine::QueryEngine> =
            Arc::downgrade(
                &(Arc::new(runtime_query_engine::session::QuerySession::new(
                    Arc::clone(&ctx),
                )) as Arc<dyn runtime_query_engine::query_engine::QueryEngine>),
            );
        ctx.register_udf(
            RerankTableFunc::new(
                weak_ctx.clone(),
                Arc::clone(&rerankers),
                Arc::clone(&chat_models),
            )
            .into(),
        );
        ctx.register_udtf(
            RERANK_UDTF_NAME,
            Arc::new(RerankTableFunc::new(
                weak_ctx,
                Arc::clone(&rerankers),
                Arc::clone(&chat_models),
            )),
        );

        Ok((ctx, rerankers, chat_models))
    }

    /// Register a small test table and insert a mock reranker.
    async fn setup_test_table(
        ctx: &SessionContext,
        rerankers: &Arc<RwLock<llms::rerank::RerankerModelStore>>,
        scores: Vec<f32>,
    ) -> DataFusionResult<()> {
        ctx.sql(
            "CREATE TABLE test_docs AS SELECT * FROM (VALUES
                (1, 'great battery life and performance', 'electronics'),
                (2, 'terrible battery drains fast', 'electronics'),
                (3, 'amazing screen quality', 'displays'),
                (4, 'average product nothing special', 'general'),
                (5, 'best purchase ever made', 'general')
            ) AS t(id, content, category)",
        )
        .await?;

        let mock: Arc<dyn llms::rerank::Rerank> = Arc::new(MockRerank { scores });
        rerankers
            .write()
            .await
            .insert("mock_reranker".to_string(), mock);

        Ok(())
    }

    macro_rules! execute_query {
        ($ctx:expr, $sql:expr) => {{
            let df = $ctx.sql($sql).await.expect("query must parse");
            df.collect().await
        }};
    }

    async fn query_snapshot(ctx: &SessionContext, sql: &str) -> String {
        let batches = execute_query!(ctx, sql).expect("query must succeed");
        pretty_format_batches(&batches)
            .expect("format query result")
            .to_string()
    }

    async fn explain_query_snapshot(ctx: &SessionContext, sql: &str) -> String {
        query_snapshot(ctx, &format!("EXPLAIN {sql}")).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rerank_bare_table_ordering_and_limit() -> DataFusionResult<()> {
        let (ctx, rerankers, _chat_models) = make_rerank_session().await?;
        setup_test_table(&ctx, &rerankers, vec![0.1, 0.9, 0.5, 0.3, 0.7]).await?;

        let sql = "SELECT id, rerank_score FROM rerank(test_docs, document => 'content', query => 'battery', model => 'mock_reranker', limit => 3)";

        insta::assert_snapshot!(
            "bare_table_explain",
            explain_query_snapshot(&ctx, sql).await
        );
        insta::assert_snapshot!("bare_table_result", query_snapshot(&ctx, sql).await);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rerank_filter_pushdown_reduces_candidates() -> DataFusionResult<()> {
        let (ctx, rerankers, _chat_models) = make_rerank_session().await?;
        // 5 scores but only 2 rows match category='electronics' (ids 1,2).
        // MockRerank returns scores in positional order of the filtered input.
        setup_test_table(&ctx, &rerankers, vec![0.4, 0.8, 0.0, 0.0, 0.0]).await?;

        let sql = "SELECT id, rerank_score FROM rerank(test_docs, document => 'content', query => 'battery', model => 'mock_reranker') WHERE category = 'electronics'";

        insta::assert_snapshot!(
            "filter_pushdown_explain",
            explain_query_snapshot(&ctx, sql).await
        );
        insta::assert_snapshot!("filter_pushdown_result", query_snapshot(&ctx, sql).await);

        Ok(())
    }
}
