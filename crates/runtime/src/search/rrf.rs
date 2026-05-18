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
use ::search::aggregation::reciprocal_rank::DEFAULT_RRF_K;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{
    Column, Constraint, DataFusionError, JoinType, Result, ScalarValue, TableReference, exec_err,
    not_impl_err,
};
use datafusion::datasource::TableType;
use datafusion::functions_aggregate::expr_fn::{first_value, max};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    ColumnarValue, DocSection, Documentation, Expr, Partitioning, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionContext, coalesce, exp, greatest, now, to_unixtime};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ExprFunctionExt, TableProviderFilterPushDown, col, ident, lit};
use itertools::Itertools;
use runtime_datafusion_udfs::digest_many::digest_many;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use crate::cluster::datafusion::codec::udtf_args::{
    RrfArgs as SerializableRrfArgs, RrfNestedQuery, RrfNestedQueryExt, TextSearchArgs,
    VectorSearchArgs,
};
use crate::embeddings::udtf::VECTOR_SEARCH_UDTF_NAME;
use crate::search::full_text::udtf::TEXT_SEARCH_UDTF_NAME;
use crate::search::util::table_ref_from_column_expr;

pub static RRF_UDF_NAME: &str = "rrf";

/// Column name for the fused score computed by RRF.
pub static RRF_FUSED_SCORE_COLUMN_NAME: &str = "_fused_score";

/// Internal column name for the synthetic row ID used when no user-provided join key exists.
const RRF_ROW_ID_COLUMN_NAME: &str = "__spice_rrf_row_id";

/// When the user sets a fused-result `limit` on `rrf()`, each underlying search
/// subquery is asked for `limit * RRF_CANDIDATE_POOL_FACTOR` rows so the
/// rank-fusion has a wider pool of candidates to combine. The post-fuse
/// `.limit(0, Some(l))` still caps the final result to exactly `l` rows. This
/// trades a small amount of extra index work for materially better recall.
pub const RRF_CANDIDATE_POOL_FACTOR: usize = 4;
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation {
    doc_section: DocSection::default(),
    description: "Merge several search queries by re-ranking them into a single result set considering each result set's orders, rank weights (if requested), and recency (if requested).".to_string(),
    syntax_example: "rrf(query_1, query_2, ..., [named_arguments])".to_string(),
    sql_example: None,
    arguments: Some(vec![
        (
            "query...".to_string(),
            "Inline text_search or vector_search UDTF invocations".to_string(),
        ),
        ("k".to_string(), "RRF smoothing parameter (default: 60.0)".to_string()),
        ("limit".to_string(), "Upper bound on fused result rows. Also propagated to any nested search query that does not specify its own limit, reducing work in the underlying search indexes.".to_string()),
        ("join_key".to_string(), "Column name to use for joining results instead of auto-generated row ID".to_string()),
        ("time_column".to_string(), "Column name containing timestamps for recency boosting".to_string()),
        ("recency_decay".to_string(), "Type of decay function: 'linear' or 'exponential' (default: 'exponential')".to_string()),
        ("decay_constant".to_string(), "Decay rate constant for exponential decay (default: 0.01)".to_string()),
        ("decay_scale_secs".to_string(), "Time scale in seconds for decay calculation (default: 86400)".to_string()),
        ("decay_window_secs".to_string(), "Window size for linear decay function (default: 86400)".to_string()),
        ("rank_weight".to_string(), "Per-query rank weighting factor (used within individual search queries)".to_string()),
    ]),
    alternative_syntax: None,
    related_udfs: Some(vec!["text_search".to_string(), "vector_search".to_string()]),
}
});

pub static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::variadic_any(Volatility::Stable));

macro_rules! extract_scalar_base {
    ($map:expr, $key:literal, $datatype:expr, $pattern:pat => $value:expr) => {
        $map.get($key)
            .and_then(|sv| sv.cast_to(&$datatype).ok())
            .and_then(|sv| match sv {
                $pattern => Some($value),
                _ => None,
            })
    };
}

macro_rules! extract_f64 {
      ($map:expr, $key:literal) => {
          extract_scalar_base!(
              $map,
              $key,
              DataType::Float64,
              ScalarValue::Float64(Some(val), ..) => val
          )
      };
  }

macro_rules! extract_string {
      ($map:expr, $key:literal) => {
          extract_scalar_base!(
              $map,
              $key,
              DataType::Utf8,
              ScalarValue::Utf8(Some(val), ..) => val
          )
      };
  }

macro_rules! col_qualified {
    ($column:expr) => {
        Expr::Column(Column::from_qualified_name_ignore_case($column))
    };
    ($table:expr, $column:expr) => {
        Expr::Column(Column::new(Some($table), $column))
    };
}

#[derive(Debug, Clone)]
enum RecencyDecay {
    Linear,
    Exponential,
}

impl FromStr for RecencyDecay {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "linear" => Ok(RecencyDecay::Linear),
            "exponential" => Ok(RecencyDecay::Exponential),
            other => not_impl_err!("{RRF_UDF_NAME} does not implement decay function {other}"),
        }
    }
}

#[derive(Debug, Default)]
struct ReciprocalRankFusionSubqueryArgs {
    pub rank_weight: Option<f64>,
}

/// Checks if an expression is a scalar function, possibly wrapped in an `Alias`.
/// In `DataFusion` v51+, function calls with named arguments may be wrapped in `Alias`.
fn is_scalar_function_expr(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarFunction(_) => true,
        Expr::Alias(alias) => matches!(alias.expr.as_ref(), Expr::ScalarFunction(_)),
        _ => false,
    }
}

/// Unwraps an expression that may be wrapped in an `Alias` to get the inner `ScalarFunction`.
fn unwrap_scalar_function(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => {
            if matches!(alias.expr.as_ref(), Expr::ScalarFunction(_)) {
                (*alias.expr).clone()
            } else {
                Expr::Alias(alias)
            }
        }
        other => other,
    }
}

impl ReciprocalRankFusionSubqueryArgs {
    pub fn from_scalar_function_expr(
        expr: &Expr,
    ) -> Result<(Expr, ReciprocalRankFusionSubqueryArgs)> {
        if let Expr::ScalarFunction(ScalarFunction { args, func }) = expr {
            let mut args = args.clone();
            let rrf_args = args
                .extract_if(.., |arg| {
                    matches!(arg, Expr::Literal(_, Some(meta)) if meta.inner().contains_key("spice.parameter_name"))
                })
                .filter_map(|arg| match arg {
                    Expr::Literal(value, Some(meta)) => meta
                        .inner()
                        .get("spice.parameter_name")
                        .map(|name| (name.clone(), value)),
                    _ => None,
                })
                .collect::<HashMap<String, ScalarValue>>();

            Ok((
                Expr::ScalarFunction(ScalarFunction {
                    args,
                    func: Arc::clone(func),
                }),
                ReciprocalRankFusionSubqueryArgs {
                    rank_weight: extract_f64!(rrf_args, "rank_weight"),
                },
            ))
        } else {
            not_impl_err!("{RRF_UDF_NAME} subquery arguments require a scalar function invocation.")
        }
    }
}

#[derive(Debug, Default)]
struct ReciprocalRankFusionArgs {
    pub search_udtf_exprs: Vec<Expr>,
    pub rrf_subquery_arguments: Vec<ReciprocalRankFusionSubqueryArgs>,
    pub k: f64,
    pub join_key: Option<Expr>,
    pub time_column: Option<Expr>,
    pub recency_decay: Option<RecencyDecay>,
    pub decay_constant: Option<f64>,
    pub decay_scale_secs: Option<f64>,
    pub decay_window_secs: Option<f64>,
    /// Optional upper bound on the fused result set. When set, also used as a
    /// default limit for any nested search subquery that does not specify its own.
    pub limit: Option<usize>,
}

type SearchUdtfArgs = (
    String,
    String,
    Option<String>,
    Option<usize>,
    Option<bool>,
    Option<String>,
);

impl ReciprocalRankFusionArgs {
    /// Constructs `ReciprocalRankFusionArgs` from an rrf UDTF invocation, which is a `TableScan` node
    /// that looks like this...
    /// ```text
    /// TableScan: rrf(text_search(wiki_a_potion, Utf8("apple")), vector_search(wiki_a_potion, Utf8("apple")))
    /// ```
    /// ...into a neat struct of subquery expressions and an optional user-provided smoothing parameter.
    ///
    /// # Arguments
    /// * `args` - A slice of `Expr` containing search UDTF invocations and optional named arguments
    ///
    /// # Returns
    /// * `Ok(ReciprocalRankFusionArgs)` - Successfully parsed arguments
    /// * `Err` - If fewer than 2 search queries are provided or if unparsing fails
    pub fn from_udtf_exprs(args: &[Expr]) -> Result<ReciprocalRankFusionArgs> {
        let mut rrf_args = args.to_vec();

        // In DataFusion v51+, function calls with named arguments may be wrapped in Alias.
        // Use is_scalar_function_expr to handle both wrapped and unwrapped cases.
        let (search_udtfs, subquery_args): (Vec<_>, Vec<_>) = rrf_args
            .extract_if(.., |arg| is_scalar_function_expr(arg))
            .map(|e| {
                let unwrapped = unwrap_scalar_function(e);
                ReciprocalRankFusionSubqueryArgs::from_scalar_function_expr(&unwrapped)
            })
            .collect::<Result<Vec<(Expr, ReciprocalRankFusionSubqueryArgs)>>>()?
            .into_iter()
            .unzip();

        let rrf_args = rrf_args
            .iter()
            .map(|arg| match arg {
                Expr::Literal(value, Some(meta)) => {
                    match meta.inner().get("spice.parameter_name") {
                        Some(name) => Ok((name.clone(), value.clone())),
                        None => {
                            not_impl_err!("{RRF_UDF_NAME} does not yet support {arg} arguments.")
                        }
                    }
                }
                // Identifier passed as a named argument (e.g. `time_column => mycol`).
                // The Spice DataFusion fork wraps non-literal named arguments in an
                // `Expr::Alias` carrying the parameter name in `spice.parameter_name`.
                // Treat the column's name as the string value so it slots into the
                // existing string-based extraction (`time_column`, `join_key`, ...).
                Expr::Alias(alias) => {
                    if let (Expr::Column(column), Some(name)) = (
                        alias.expr.as_ref(),
                        alias
                            .metadata
                            .as_ref()
                            .and_then(|m| m.inner().get("spice.parameter_name")),
                    ) {
                        Ok((name.clone(), ScalarValue::Utf8(Some(column.name.clone()))))
                    } else {
                        not_impl_err!("{RRF_UDF_NAME} does not yet support {arg} arguments.")
                    }
                }
                // Show a useful error for the rest
                other_expr => {
                    not_impl_err!("{RRF_UDF_NAME} does not yet support {other_expr} arguments.")
                }
            })
            .collect::<Result<HashMap<String, ScalarValue>>>()?;

        if search_udtfs.len() < 2 {
            return Err(DataFusionError::Plan(format!(
                "{RRF_UDF_NAME} needs at least 2 search queries to fuse results."
            )));
        }

        Ok(Self {
            search_udtf_exprs: search_udtfs,
            rrf_subquery_arguments: subquery_args,
            k: extract_f64!(rrf_args, "k").unwrap_or(DEFAULT_RRF_K),
            join_key: extract_string!(rrf_args, "join_key").map(ident),
            time_column: extract_string!(rrf_args, "time_column").map(ident),
            recency_decay: extract_string!(rrf_args, "recency_decay")
                .map(|rd| RecencyDecay::from_str(&rd))
                .transpose()?,
            decay_constant: extract_f64!(rrf_args, "decay_constant"),
            decay_scale_secs: extract_f64!(rrf_args, "decay_scale_secs"),
            decay_window_secs: extract_f64!(rrf_args, "decay_window_secs"),
            // Validate `limit` explicitly so bad values fail fast instead of
            // silently becoming `None` (cast failure) or `usize::MAX`
            // (try_from saturation). Reuses the same helpers the nested search
            // UDTFs use for their positional `limit` argument.
            limit: match rrf_args.get("limit") {
                Some(ScalarValue::UInt64(Some(v))) => Some(Self::parse_limit_u64(*v)?),
                Some(ScalarValue::Int64(Some(v))) => Some(Self::parse_limit_i64(*v)?),
                Some(ScalarValue::Int32(Some(v))) => Some(Self::parse_limit_i64(i64::from(*v))?),
                Some(ScalarValue::UInt32(Some(v))) => Some(Self::parse_limit_u64(u64::from(*v))?),
                Some(other) => {
                    return exec_err!(
                        "{RRF_UDF_NAME} 'limit' must be a non-negative integer, got: {other}"
                    );
                }
                None => None,
            },
        })
    }

    /// Converts the internal RRF arguments to a serializable form for distributed execution.
    ///
    /// This extracts the nested search UDTF invocations and converts them to `RrfNestedQuery`
    /// variants that can be serialized and sent to remote executors.
    fn to_serializable(&self) -> Result<SerializableRrfArgs> {
        let queries = self
            .search_udtf_exprs
            .iter()
            .zip(self.rrf_subquery_arguments.iter())
            .map(|(expr, subquery_args)| {
                Self::expr_to_nested_query(expr, subquery_args.rank_weight)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(SerializableRrfArgs {
            queries,
            k: Some(self.k),
            join_key: self.join_key.as_ref().map(|e| e.qualified_name().1),
            time_column: self.time_column.as_ref().map(|e| e.qualified_name().1),
            recency_decay: self.recency_decay.as_ref().map(|rd| match rd {
                RecencyDecay::Linear => "linear".to_string(),
                RecencyDecay::Exponential => "exponential".to_string(),
            }),
            decay_constant: self.decay_constant,
            decay_scale_secs: self.decay_scale_secs,
            decay_window_secs: self.decay_window_secs,
            limit: self
                .limit
                .map(|l| {
                    u64::try_from(l).map_err(|_| {
                        DataFusionError::Plan(format!(
                            "{RRF_UDF_NAME}: limit value {l} cannot be represented as u64 for serialization."
                        ))
                    })
                })
                .transpose()?,
        })
    }

    /// Converts a scalar function expression to an `RrfNestedQuery`.
    fn expr_to_nested_query(expr: &Expr, rank_weight: Option<f64>) -> Result<RrfNestedQuery> {
        let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
            return exec_err!("Expected scalar function expression for nested RRF query");
        };

        let func_name = func.name();
        match func_name {
            name if name == TEXT_SEARCH_UDTF_NAME => {
                let search_args = Self::parse_search_args(args)?;
                Ok(RrfNestedQuery::text_search(
                    TextSearchArgs {
                        table: search_args.0,
                        query: search_args.1,
                        column: search_args.2,
                        limit: search_args.3.map(|l| l as u64),
                        include_score: search_args.4,
                    },
                    rank_weight,
                ))
            }
            name if name == VECTOR_SEARCH_UDTF_NAME => {
                let search_args = Self::parse_search_args(args)?;
                Ok(RrfNestedQuery::vector_search(
                    VectorSearchArgs {
                        table: search_args.0,
                        query: search_args.1,
                        column: search_args.2,
                        limit: search_args.3.map(|l| l as u64),
                        include_score: search_args.4,
                        distance_metric: search_args.5,
                    },
                    rank_weight,
                ))
            }
            _ => exec_err!(
                "{RRF_UDF_NAME} only supports {TEXT_SEARCH_UDTF_NAME} and {VECTOR_SEARCH_UDTF_NAME} nested queries, got: {func_name}"
            ),
        }
    }

    fn parse_limit_u64(value: u64) -> Result<usize> {
        usize::try_from(value).map_err(|_| {
            DataFusionError::Plan(format!(
                "{RRF_UDF_NAME} limit value {value} exceeds usize range."
            ))
        })
    }

    fn parse_limit_i64(value: i64) -> Result<usize> {
        if value < 0 {
            return exec_err!("{RRF_UDF_NAME} limit value {value} must be non-negative");
        }

        let value = u64::try_from(value).map_err(|_| {
            DataFusionError::Plan(format!(
                "{RRF_UDF_NAME} limit value {value} exceeds usize range."
            ))
        })?;
        Self::parse_limit_u64(value)
    }

    /// Parses common search UDTF arguments from expressions.
    ///
    /// Returns: (table, query, column, limit, `include_score`, `distance_metric`)
    ///
    /// Handles both positional arguments and named arguments (which the Spice
    /// `DataFusion` fork wraps in `Expr::Literal` / `Expr::Alias` with a
    /// `spice.parameter_name` metadata key). Named args are extracted into a
    /// map first so they are not silently dropped — otherwise a caller like
    /// `text_search(foo, 'q', limit => 1000)` or
    /// `vector_search(foo, 'q', distance_metric => 'l2')` nested inside
    /// `rrf(...)` would lose the named arg on the way through and execute
    /// with the wrong semantics.
    fn parse_search_args(args: &[Expr]) -> Result<SearchUdtfArgs> {
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

        // Split args into positional (no spice.parameter_name) and named.
        let mut named: std::collections::HashMap<&str, &Expr> = std::collections::HashMap::new();
        let mut positional: Vec<&Expr> = Vec::with_capacity(args.len());
        for a in args {
            if let Some((name, inner)) = named_param(a) {
                named.insert(name, inner);
            } else {
                positional.push(a);
            }
        }

        // Extract table reference from first positional arg
        let table = match positional.first() {
            Some(Expr::Column(c)) => table_ref_from_column_expr(c).to_quoted_string(),
            _ => {
                return exec_err!("First argument to search UDTF must be a table reference");
            }
        };

        // Extract query string from second positional arg
        let query = match positional.get(1) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(q)), _)) => q.clone(),
            _ => {
                return exec_err!("Second argument to search UDTF must be a query string");
            }
        };

        // Extract optional column, limit, include_score from remaining positional args
        let mut column = None;
        let mut limit = None;
        let mut include_score = None;

        for arg in positional.iter().skip(2) {
            match arg {
                Expr::Column(Column { name, .. }) => {
                    column = Some(name.clone());
                }
                Expr::Literal(ScalarValue::UInt64(Some(l)), _) => {
                    limit = Some(Self::parse_limit_u64(*l)?);
                }
                Expr::Literal(ScalarValue::Int64(Some(l)), _) => {
                    limit = Some(Self::parse_limit_i64(*l)?);
                }
                Expr::Literal(ScalarValue::Boolean(Some(b)), _) => {
                    include_score = Some(*b);
                }
                _ => {}
            }
        }

        // Merge in named args for any field not already set positionally.
        if column.is_none() {
            if let Some(Expr::Column(Column { name, .. })) = named.get("column") {
                column = Some(name.clone());
            } else if let Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) = named.get("column") {
                column = Some(s.clone());
            }
        }
        if limit.is_none() {
            if let Some(Expr::Literal(ScalarValue::UInt64(Some(l)), _)) = named.get("limit") {
                limit = Some(Self::parse_limit_u64(*l)?);
            } else if let Some(Expr::Literal(ScalarValue::Int64(Some(l)), _)) = named.get("limit") {
                limit = Some(Self::parse_limit_i64(*l)?);
            }
        }
        if include_score.is_none()
            && let Some(Expr::Literal(ScalarValue::Boolean(Some(b)), _)) =
                named.get("include_score")
        {
            include_score = Some(*b);
        }

        let distance_metric = match named.get("distance_metric") {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Some(s.clone()),
            _ => None,
        };

        Ok((table, query, column, limit, include_score, distance_metric))
    }
}

impl Debug for ReciprocalRankFusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReciprocalRankFusion")
    }
}

pub struct ReciprocalRankFusion {
    pub session_context: Arc<SessionContext>,
    // store a pointer to use for Hash/Eq since UDTF impls require this trait bound but we cannot feasibly make `SessionContext` implement them.
    session_ptr: u64,
    /// Output schema, derived once in `call()` by building the fused `DataFrame`.
    schema: Option<SchemaRef>,
    /// Stores the original RRF arguments so that `scan()` can rebuild the
    /// fused `DataFrame` with pre-filters injected into each sub-query.
    rrf_args: Option<Arc<ReciprocalRankFusionArgs>>,
    /// Stores the original RRF arguments for distributed serialization.
    ///
    /// This is set when the provider is created via the `rrf()` UDTF and enables
    /// `SpiceLogicalCodec` to serialize and reconstruct this provider on remote executors.
    pub rrf_source: Option<SerializableRrfArgs>,
}

impl PartialEq for ReciprocalRankFusion {
    fn eq(&self, other: &Self) -> bool {
        self.session_ptr == other.session_ptr
    }
}

impl Eq for ReciprocalRankFusion {}

impl std::hash::Hash for ReciprocalRankFusion {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.session_ptr.hash(state);
    }
}

// TODO: DF support for nested UDTF calls without ScalarUDF "hack"
impl ReciprocalRankFusion {
    #[must_use]
    pub fn from_ctx(session_context: &Arc<SessionContext>) -> Self {
        let ptr = Arc::as_ptr(session_context) as u64;

        Self {
            session_context: Arc::clone(session_context),
            session_ptr: ptr,
            schema: None,
            rrf_args: None,
            rrf_source: None,
        }
    }

    #[must_use]
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    #[must_use]
    fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    fn with_rrf_args(mut self, args: Arc<ReciprocalRankFusionArgs>) -> Self {
        self.rrf_args = Some(args);
        self
    }

    /// Sets the RRF source for distributed serialization.
    #[must_use]
    pub fn with_rrf_source(mut self, source: SerializableRrfArgs) -> Self {
        self.rrf_source = Some(source);
        self
    }

    fn scalar_stub_error<T>() -> Result<T, DataFusionError> {
        exec_err!(
            "{RRF_UDF_NAME} is a table function with a scalar stub. Please call as a table function."
        )
    }

    fn compute_score_expr(
        args: &ReciprocalRankFusionArgs,
        subquery_dfs: &[DataFrame],
    ) -> Result<Expr> {
        // Compute base score expression with boost
        let score_expr = (0..subquery_dfs.len())
            .map(|i| {
                // Either use 1 as dividend or user provided boost
                let dividend: f64 = args
                    .rrf_subquery_arguments
                    .get(i)
                    .and_then(|args| args.rank_weight)
                    .unwrap_or(1.0f64);

                lit(dividend)
                    / (lit(args.k)
                        + coalesce(vec![
                            col_qualified!(format!("search_{i}.rank")),
                            lit(f64::INFINITY),
                        ]))
            })
            .reduce(|a, b| a + b);

        let score_expr = if let Some(score_expr) = score_expr {
            score_expr.alias(RRF_FUSED_SCORE_COLUMN_NAME)
        } else {
            return exec_err!("{RRF_UDF_NAME} unable to compute {RRF_FUSED_SCORE_COLUMN_NAME}");
        };

        // If user specifies a recency column, we enable recency boosting
        let qualified_recency_col = if let Some(recency_col) = args.time_column.clone() {
            let (_, qname) = recency_col.qualified_name();
            let cols = subquery_dfs
                .iter()
                .enumerate()
                .map(|(i, df)| {
                    Self::first_qualified_field(df, &qname)
                        .map(|(_, q)| col_qualified!(format!("search_{i}"), q))
                })
                .collect::<Result<Vec<_>>>()?;
            coalesce(cols)
        } else {
            return Ok(score_expr);
        };

        // Defaults: exponential decay over days (86400s)
        let recency_decay = args
            .recency_decay
            .clone()
            .unwrap_or(RecencyDecay::Exponential);
        let decay_scale_secs = args.decay_scale_secs.unwrap_or(86400.0);

        // Lots of casting annoyances are avoided by treating everything as `long`
        let today_epoch = to_unixtime(vec![now()]);
        let recency_col_epoch = to_unixtime(vec![qualified_recency_col]);
        let age_in_units = (today_epoch - recency_col_epoch) / lit(decay_scale_secs);

        let recency_expr = match recency_decay {
            // e^(-alpha * age units)
            RecencyDecay::Exponential => {
                let decay_constant = args.decay_constant.unwrap_or(0.01);
                #[expect(clippy::neg_multiply)]
                exp(lit(-1.0f64 * decay_constant) * age_in_units)
            }
            // 1 - (age units / boost window)
            RecencyDecay::Linear => {
                let decay_window_secs = args.decay_window_secs.unwrap_or(86400.0);
                let boost = lit(1) - (age_in_units / lit(decay_window_secs));
                greatest(vec![lit(0), boost])
            }
        };

        // Fall back to the original score expression if a recency boost cannot be computed
        Ok(
            (score_expr * coalesce(vec![recency_expr, lit(1.0)]))
                .alias(RRF_FUSED_SCORE_COLUMN_NAME),
        )
    }

    // Given arguments to n search calls: execute searches, generate row IDs, rank by score, JOIN,
    // then finally re-rank and sort fused results
    fn rerank_and_fuse_df(
        &self,
        args: &ReciprocalRankFusionArgs,
        filters: &[Expr],
    ) -> Result<DataFrame> {
        let (subquery_dfs, join_key) = self.prepare_and_execute_subqueries(args, filters)?;
        let score_expr = Self::compute_score_expr(args, &subquery_dfs)?;

        // Create column expressions for final projection.
        // Only include columns that exist in *all* subquery schemas so that
        // cross-backend RRF (e.g. vector_search + Elasticsearch text_search)
        // doesn't fail when one backend returns extra columns (e.g. `_value`,
        // `content_offset`) that the other doesn't.
        let mut columns: Vec<Expr> = vec![score_expr];
        columns.extend(subquery_dfs[0].schema().columns().iter().filter_map(|c| {
            match c.name.as_str() {
                "rank" | "_score" => None,
                // TODO: do we want the embedding in the final projection?
                other if other.ends_with("_embedding") => None,
                other => {
                    // Skip columns that don't exist in every subquery schema.
                    let in_all = subquery_dfs
                        .iter()
                        .all(|df| df.schema().has_column_with_unqualified_name(other));
                    if !in_all {
                        return None;
                    }
                    Some(
                        coalesce(
                            (0..subquery_dfs.len())
                                .map(|i| col_qualified!(format!("search_{i}"), other))
                                .collect(),
                        )
                        .alias(other),
                    )
                }
            }
        }));

        let mut join_err: Option<DataFusionError> = None;
        let maybe_joined = subquery_dfs.into_iter().reduce(|a, b| {
            let joined = Self::fold_join(a, b, join_key.qualified_name().1.as_str());

            // No way to short circuit reduce, so we will surface the error at the end
            match joined {
                Ok(joined) => joined,
                Err(e) => {
                    join_err = Some(e);
                    self.session_context
                        .read_empty()
                        .unwrap_or_else(|_| unreachable!("must be able to make an empty DataFrame"))
                }
            }
        });

        if let Some(error) = join_err {
            return Err(error);
        }

        if let Some(joined) = maybe_joined {
            tracing::trace!("{RRF_UDF_NAME} made reranked & fused DF for: {args:?}");
            // Take the highest scores from multiple matches
            let mut agg_cols =
                vec![max(col(RRF_FUSED_SCORE_COLUMN_NAME)).alias(RRF_FUSED_SCORE_COLUMN_NAME)];

            // The first column is the score_expr, which gets special treatment above.
            // These are unaliased, because they get flattened by coalesce() in the first select.
            agg_cols.extend(columns.iter().skip(1).filter_map(|c| {
                let (_, cname) = c.qualified_name();

                // Do not aggregate the join key
                if cname == join_key.qualified_name().1 {
                    None
                } else {
                    Some(
                        first_value(
                            ident(&cname),
                            vec![col(RRF_FUSED_SCORE_COLUMN_NAME).sort(false, false)],
                        )
                        .alias(&cname),
                    )
                }
            }));

            let sorted = joined
                .select(columns)?
                .aggregate(vec![join_key], agg_cols)?
                .drop_columns(&[RRF_ROW_ID_COLUMN_NAME])?
                .sort(vec![col(RRF_FUSED_SCORE_COLUMN_NAME).sort(false, false)])?;

            // Apply the RRF-level limit so `FROM rrf(..., limit => N)` alone is
            // sufficient — users don't have to add an outer LIMIT clause.
            if let Some(l) = args.limit {
                sorted.limit(0, Some(l))
            } else {
                Ok(sorted)
            }
        } else {
            exec_err!("{RRF_UDF_NAME}: Unable to join result sets")
        }
    }

    /// For a set of primary keys, return an inferred join key expression if all tables share the same PK.
    ///
    /// Only single-column PKs are supported for now.
    fn infer_join_key(table_pks: &[Option<Vec<String>>]) -> Option<Expr> {
        let inferred_key = table_pks.iter().find(|&s| s.is_some())?.clone()?;
        if table_pks
            .iter()
            .all(|pk_opt| pk_opt.as_ref() == Some(&inferred_key))
            && inferred_key.len() == 1
        {
            Some(ident(&inferred_key[0]))
        } else {
            None
        }
    }

    // Given RRF args with unparsed search udtf exprs, turn each subquery into a DF,
    // add a hashed row ID, rank it, then give it an alias of `search_{i_in_argv}`
    //
    // When `filters` is non-empty, each filter whose referenced columns all
    // exist in a sub-query's schema is applied as a pre-filter on that
    // sub-query *before* ranking. This narrows the candidate set so that
    // ranks reflect position within the filtered population — matching user
    // intent for queries like `WHERE review_date > '2015-06-15' AND product_category = 'some category'`.
    fn prepare_and_execute_subqueries(
        &self,
        args: &ReciprocalRankFusionArgs,
        filters: &[Expr],
    ) -> Result<(Vec<DataFrame>, Expr)> {
        tracing::trace!("{RRF_UDF_NAME} preparing subqueries for: {:?}", args);

        let (search_dfs, per_table_pks): (Vec<DataFrame>, Vec<Option<Vec<String>>>) = args
            .search_udtf_exprs
            .iter()
            .map(|expr| {
                let Expr::ScalarFunction(sf) = expr else {
                    unreachable!("Must be a scalar function node")
                };
                self.session_context
                    .table_function(sf.name())
                    .and_then(|udtf| udtf.create_table_provider(&sf.args))
                    .and_then(|provider| {
                        let pk = provider.constraints().as_ref().and_then(|&cs| {
                            cs.iter().find_map(|c| match c {
                                Constraint::PrimaryKey(pk) => provider
                                    .schema()
                                    .project(pk)
                                    .map(|x| {
                                        x.fields()
                                            .iter()
                                            .map(|f| f.name().clone())
                                            .collect::<Vec<_>>()
                                    })
                                    .ok(),
                                Constraint::Unique(_) => None,
                            })
                        });
                        Ok((self.session_context.read_table(provider)?, pk))
                    })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        let join_key = args
            .join_key
            .clone()
            .or(Self::infer_join_key(per_table_pks.as_slice()));

        let prepared_dfs: Vec<DataFrame> = search_dfs
            .into_iter()
            .enumerate()
            .map(|(i, df)| {
                // Ensure that all projections have a score column
                if !df.schema().has_column_with_unqualified_name("_score") {
                    return exec_err!(
                        "{RRF_UDF_NAME}: Query at position {i} does not have a `_score` column."
                    );
                }

                // Apply pre-filters: only keep filters whose columns all exist
                // in this sub-query's schema. This narrows the candidate set
                // *before* ranking so ranks reflect position within the
                // filtered population.
                let df = if filters.is_empty() {
                    df
                } else {
                    let schema = df.schema();
                    if let Some(combined) = filters
                        .iter()
                        .filter(|f| {
                            f.column_refs()
                                .iter()
                                .all(|c| schema.has_column_with_unqualified_name(c.name()))
                        })
                        .cloned()
                        .reduce(Expr::and)
                    {
                        df.filter(combined)?
                    } else {
                        df
                    }
                };

                // Propagate the RRF-level `limit` into each subquery as a wider
                // candidate pool — but only when the subquery itself does not
                // already specify an explicit `limit` positional argument. We
                // must not override a user-provided limit (e.g.
                // `text_search(..., 1000)` nested inside `rrf(..., limit => 25)`).
                //
                // The subquery search providers honor scan-level limits (pushed
                // down through DataFusion's optimizer), so this reduces work in
                // the underlying search indexes and network overhead. We
                // multiply by RRF_CANDIDATE_POOL_FACTOR so the rank-fusion has
                // enough overlap candidates to produce a stable top-`l` result;
                // the final `.limit(0, Some(l))` after fusion still caps the
                // user-visible output exactly.
                let subquery_has_explicit_limit = args
                    .search_udtf_exprs
                    .get(i)
                    .and_then(|e| match e {
                        Expr::ScalarFunction(sf) => {
                            ReciprocalRankFusionArgs::parse_search_args(&sf.args)
                                .ok()
                                .map(|a| a.3.is_some())
                        }
                        _ => None,
                    })
                    .unwrap_or(false);

                let df = match args.limit {
                    Some(l) if !subquery_has_explicit_limit => {
                        let pool = l.saturating_mul(RRF_CANDIDATE_POOL_FACTOR);
                        df.limit(0, Some(pool))?
                    }
                    _ => df,
                };

                let df_with_id = match join_key {
                    Some(_) => Ok(df),
                    None => Self::with_rrf_rowid(df),
                }?
                // Normalize to a single partition before window ranking.
                // This avoids downstream order-property mismatches when
                // subqueries carry conflicting pre-existing sort orders.
                .repartition(Partitioning::RoundRobinBatch(1))?;

                Self::with_rank(df_with_id).and_then(|df| df.alias(&format!("search_{i}")))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((
            prepared_dfs,
            join_key.unwrap_or(col(RRF_ROW_ID_COLUMN_NAME)),
        ))
    }

    // Given a DF with overlapping unqualified names (as produced by JOIN), where column values
    // are equivalent, return the first (arbitrary) qualified name.
    fn first_qualified_field(df: &DataFrame, name: &str) -> Result<(TableReference, String)> {
        df.schema()
            .qualified_fields_with_unqualified_name(name)
            .first()
            .and_then(|(maybe_table_reference, f)| {
                maybe_table_reference.map(|tr| (tr.clone(), f.name().clone()))
            })
            .ok_or(DataFusionError::Execution(format!(
                "{RRF_UDF_NAME}: Cannot resolve column {name} when fusing results"
            )))
    }

    // Reduces 2 or more search subquery DFs into a single one
    fn fold_join(a: DataFrame, b: DataFrame, join_key: &str) -> Result<DataFrame> {
        let (tbl_a, id_a) = Self::first_qualified_field(&a, join_key)?;
        let (tbl_b, id_b) = Self::first_qualified_field(&b, join_key)?;

        // Drop embedding columns before the full outer join — they are non-nullable
        // in Arrow but the join produces nulls for unmatched rows, causing a schema
        // violation. They are excluded from the final projection anyway.
        let drop_embedding_cols = |df: DataFrame| -> Result<DataFrame> {
            let to_drop: Vec<String> = df
                .schema()
                .fields()
                .iter()
                .filter(|f| f.name().ends_with("_embedding"))
                .map(|f| f.name().clone())
                .collect();
            if to_drop.is_empty() {
                Ok(df)
            } else {
                let to_drop_refs: Vec<&str> = to_drop.iter().map(String::as_str).collect();
                df.drop_columns(&to_drop_refs)
            }
        };
        let a = drop_embedding_cols(a)?;
        let b = drop_embedding_cols(b)?;

        a.join_on(
            b,
            JoinType::Full,
            vec![col_qualified!(tbl_a, id_a).eq(col_qualified!(tbl_b, id_b))],
        )
    }

    // Window and rank a search subquery by its `_score` field.
    fn with_rank(df: DataFrame) -> Result<DataFrame> {
        let rank_expr = row_number()
            .order_by(vec![col("_score").sort(false, false)])
            .build()?
            .alias("rank");

        df.window(vec![rank_expr])
    }

    // Create an internal row ID for rows that don't have a user-provided join key.
    //
    // To keep this cheap, only hash columns whose data type is identity-like
    // (ints, floats, bool, date/time, decimal, fixed/variable-size binary,
    // and strings — which commonly back UUID/hash primary keys). Primary keys
    // are nearly always one of these, and these types serialize compactly
    // compared to list/struct columns.
    //
    // If a schema has no identity-like columns (rare — e.g. a table of only
    // list/struct columns), fall back to hashing all non-score, non-embedding
    // columns so correctness is preserved.
    fn with_rrf_rowid(df: DataFrame) -> Result<DataFrame> {
        let schema = df.schema();
        let is_identity_like = |dt: &DataType| -> bool {
            matches!(
                dt,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Timestamp(_, _)
                    | DataType::Duration(_)
                    | DataType::Interval(_)
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
                    | DataType::FixedSizeBinary(_)
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::BinaryView
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
            )
        };

        let candidates: Vec<(&str, &DataType)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .filter(|(name, _)| *name != "_score" && !name.ends_with("_embedding"))
            .collect();

        let mut selected: Vec<Expr> = candidates
            .iter()
            .filter(|(_, dt)| is_identity_like(dt))
            .sorted_by_key(|(name, _)| *name)
            .map(|(name, _)| ident(*name))
            .collect();

        // Fallback: no scalar identity columns, hash everything (minus embeddings/score).
        if selected.is_empty() {
            selected = candidates
                .iter()
                .sorted_by_key(|(name, _)| *name)
                .map(|(name, _)| ident(*name))
                .collect();
        }

        df.with_column(RRF_ROW_ID_COLUMN_NAME, digest_many(selected, "md5"))
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
        Self::scalar_stub_error()
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Self::scalar_stub_error()
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&*DOCUMENTATION)
    }
}

impl TableFunctionImpl for ReciprocalRankFusion {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let rrf_args = Arc::new(ReciprocalRankFusionArgs::from_udtf_exprs(args)?);
        let serializable_args = rrf_args.to_serializable()?;
        let schema_df = self.rerank_and_fuse_df(&rrf_args, &[])?;
        let schema = Arc::clone(schema_df.schema().inner());
        Ok(Arc::new(
            ReciprocalRankFusion::from_ctx(&self.session_context)
                .with_schema(schema)
                .with_rrf_args(rrf_args)
                .with_rrf_source(serializable_args),
        ))
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for ReciprocalRankFusion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        match self.schema.as_ref() {
            Some(schema) => Arc::clone(schema),
            None => panic!("ReciprocalRankFusion schema is not set. This is a bug in Spice.ai"),
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let schema = self.schema();
        // `fused_score` is computed by RRF; all other columns come from the
        // underlying search subqueries and are safe to push down.
        let base_field_names: HashSet<&str> = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .filter(|name| *name != RRF_FUSED_SCORE_COLUMN_NAME)
            .collect();

        filters
            .iter()
            .map(|f| {
                let pushable = f
                    .column_refs()
                    .iter()
                    .all(|c| base_field_names.contains(c.name()));

                Ok(if pushable {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                })
            })
            .collect()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(ref rrf_args) = self.rrf_args else {
            return exec_err!(
                "ReciprocalRankFusion has no stored args — cannot create physical plan"
            );
        };
        let mut df = self.rerank_and_fuse_df(rrf_args, filters)?;

        // Also apply filters post-fusion as a safety net: pre-filters are
        // only injected into sub-queries whose schema contains the
        // referenced columns. If sub-queries have different schemas a
        // filter may be skipped for some, letting unfiltered rows leak
        // through the FULL JOIN. The post-fusion filter guarantees correctness.
        if let Some(post_filter) = filters.iter().cloned().reduce(Expr::and) {
            df = df.filter(post_filter)?;
        }

        if let Some(projection) = projection {
            df = df.select(
                self.schema()
                    .project(projection)?
                    .fields
                    .iter()
                    .map(|f| ident(f.name())),
            )?;
        }

        df.limit(0, limit)?.create_physical_plan().await
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

    fn get_column_default(&self, _column: &str) -> Option<&datafusion::prelude::Expr> {
        None
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn datafusion::catalog::Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> Result<datafusion::catalog::ScanResult> {
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
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        not_impl_err!("ReciprocalRankFusion does not support insert_into")
    }

    async fn delete_from(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _filters: Vec<datafusion::prelude::Expr>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        not_impl_err!("ReciprocalRankFusion does not support delete_from")
    }

    async fn update(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _assignments: Vec<(String, datafusion::prelude::Expr)>,
        _filters: Vec<datafusion::prelude::Expr>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        not_impl_err!("ReciprocalRankFusion does not support update")
    }

    async fn truncate(
        &self,
        _state: &dyn datafusion::catalog::Session,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        not_impl_err!("ReciprocalRankFusion does not support truncate")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "models")]
    use crate::Runtime;
    #[cfg(feature = "models")]
    use crate::builder::RuntimeBuilder;
    #[cfg(feature = "models")]
    use crate::datafusion::query::QueryBuilder;
    #[cfg(feature = "models")]
    use crate::datafusion::udf::register_udfs;
    #[cfg(feature = "models")]
    use crate::embeddings::table::EmbeddingColumnConfig;
    #[cfg(feature = "models")]
    use crate::embeddings::table::EmbeddingTable;
    use crate::search::rrf::ReciprocalRankFusionArgs;
    #[cfg(feature = "models")]
    use arrow::array::as_string_array;
    #[cfg(feature = "models")]
    use arrow::record_batch::RecordBatch;
    #[cfg(feature = "models")]
    use async_graphql::futures_util::TryStreamExt;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::catalog::TableProvider;
    #[cfg(feature = "models")]
    use datafusion::common::Result;
    #[cfg(feature = "models")]
    use datafusion::common::cast::{as_float64_array, as_uint64_array};
    #[cfg(feature = "models")]
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::col;
    use datafusion::logical_expr::expr::FieldMetadata;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    #[cfg(feature = "models")]
    use datafusion::prelude::{DataFrame, named_struct, now, to_unixtime};
    use datafusion::scalar::ScalarValue;
    #[cfg(feature = "models")]
    use datafusion_expr::ExprFunctionExt;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::lit;
    #[cfg(feature = "models")]
    use llms::model2vec::Model2Vec;
    #[cfg(feature = "models")]
    use runtime_request_context::{Protocol, RequestContext};
    use std::collections::BTreeMap;
    #[cfg(feature = "models")]
    use std::collections::HashMap;
    #[cfg(feature = "models")]
    use std::process::ExitCode;
    use std::sync::Arc;
    #[cfg(feature = "models")]
    use std::sync::LazyLock;

    #[cfg(feature = "models")]
    pub static TEST_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> =
        LazyLock::new(|| Arc::new(RequestContext::builder(Protocol::Internal).build()));

    /// Shared embedding model for tests to avoid concurrent file lock contention.
    /// Model loading from `HuggingFace` uses file locks; creating multiple models in parallel
    /// can fail with lock acquisition errors.
    #[cfg(feature = "models")]
    static TEST_EMBEDDING_MODEL: LazyLock<Arc<Model2Vec>> = LazyLock::new(|| {
        Arc::new(
            Model2Vec::from_params(
                "minishlab/potion-base-2M",
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("Must make embedding model"),
        )
    });

    macro_rules! spice_named_lit {
        ($name:literal, $value:expr) => {{
            let scalar_value = ScalarValue::from($value);
            let spice_metadata = FieldMetadata::new(BTreeMap::from([(
                "spice.parameter_name".to_string(),
                $name.to_string(),
            )]));
            Expr::Literal(scalar_value, Some(spice_metadata))
        }};
    }

    #[cfg(feature = "models")]
    macro_rules! extract_column {
        ($batches:expr, $column_name:expr, $array_cast_fn:ident, $nth:expr) => {
            $array_cast_fn(
                $batches[$nth]
                    .column_by_name($column_name)
                    .expect(format!("Must have {}", $column_name).as_str()),
            )
        };

        ($batches:expr, $column_name:expr, $array_cast_fn:ident) => {
            extract_column!($batches, $column_name, $array_cast_fn, 0)
        };
    }

    #[cfg(feature = "models")]
    macro_rules! test_query {
        ($runtime:ident, $query:expr) => {{
            let query = QueryBuilder::new($query, $runtime.datafusion()).build();
            query
                .run()
                .await
                .expect("Must run query")
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await?
        }};
    }

    // Assumes column "content" is embedded
    #[cfg(feature = "models")]
    fn df_as_embedding_table(runtime: &Runtime, df: DataFrame) -> Arc<dyn TableProvider> {
        let mut embedded_columns = HashMap::new();
        embedded_columns.insert(
            "content".to_string(),
            EmbeddingColumnConfig {
                model_name: "test_model".to_string(),
                vector_size: 64,
                in_base_table: true,
                chunker: None,
                input_mode: crate::embeddings::table::EmbeddingInputMode::Scalar,
            },
        );

        Arc::new(EmbeddingTable {
            base_table: df.into_view(),
            embedded_columns,
            embedding_models: Arc::clone(&runtime.embeds),
        })
    }

    #[cfg(feature = "models")]
    async fn make_test_runtime() -> Result<Runtime> {
        let rt = RuntimeBuilder::new().build().await;
        rt.df
            .ctx
            .state()
            .config_mut()
            .set_extension(Arc::clone(&TEST_REQUEST_CONTEXT));

        // Use shared embedding model to avoid file lock contention in parallel tests
        let embedding_model: Arc<dyn llms::embeddings::Embed> =
            Arc::clone(&TEST_EMBEDDING_MODEL) as Arc<dyn llms::embeddings::Embed>;
        rt.embeds
            .write()
            .await
            .insert("test_model".to_string(), embedding_model);

        register_udfs(&rt).await;
        Ok(rt)
    }

    #[cfg(feature = "models")]
    async fn make_fruit_dataframe(runtime: &Runtime) -> Result<DataFrame> {
        let rowid_expr = row_number()
            .order_by(vec![col("content").sort(false, false)])
            .build()?
            .alias("id");

        let df = runtime
            .df
            .ctx
            .sql(
                "SELECT
              unnest([
                  'banana yellow curved fruit',
                  'orange citrus round juicy',
                  'apple fruit sweet red crispy'
              ]) as content",
            )
            .await?;

        let embed_expr = df.parse_sql_expr("embed(content, 'test_model')")?;

        df.window(vec![rowid_expr])?
            .with_column("content_embedding", embed_expr)
    }

    fn stub_scalar_function(name: &str) -> Expr {
        let stub_udf = create_udf(
            name,
            vec![DataType::Utf8; 0],
            DataType::Utf8,
            Volatility::Stable,
            Arc::new(|_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "stub".to_string(),
                ))))
            }),
        );

        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(stub_udf), vec![]))
    }

    #[cfg(feature = "models")]
    #[tokio::test(flavor = "multi_thread")]
    // #[ignore = "https://github.com/spiceai/spiceai/issues/7861"] // For some reason, BytesProcessedExec is failing to acquire a RequestContext even though the other RRF tests do fine
    // https://github.com/spiceai/spiceai/issues/7861
    async fn test_recency_scoring() -> Result<ExitCode> {
        let runtime = make_test_runtime().await?;

        let fruit_df = make_fruit_dataframe(&runtime)
            .await?
            .with_column("picked_at", now())?
            .with_column(
                "picked_at",
                to_unixtime(vec![col("picked_at")]) - (lit(43200) * col("id")),
            )?;

        let picked_at_expr = fruit_df.parse_sql_expr("to_timestamp(cast(picked_at as bigint))")?;

        let fruit_df = fruit_df
            .with_column("picked_at", picked_at_expr)?
            .sort(vec![col("picked_at").sort(false, false)])?;

        let fruit_embedding_table = df_as_embedding_table(&runtime, fruit_df.clone());

        runtime
            .df
            .ctx
            .register_table("foo", fruit_embedding_table)?;

        // decay_constant is made more aggressive in this query to further deprioritize
        // old results. The test will/should fail if you use the default of 0.01.
        let results = test_query!(
            runtime,
            "select * from rrf(vector_search(foo, 'red crispy'), vector_search(foo, 'fruit'), time_column => 'picked_at', decay_constant => 0.1)"
        );

        let content = extract_column!(results, "content", as_string_array);

        let fruit_df_batches = fruit_df.collect().await?;
        let fruit_df_recent = extract_column!(fruit_df_batches, "content", as_string_array);

        // fruit_df.show() to debug me
        assert_eq!(content.value(0), fruit_df_recent.value(0));

        Ok(ExitCode::SUCCESS)
    }

    #[cfg(feature = "models")]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Temporarily disabled due DataFusion order-property planning instability; covered by integration regression test test_rrf_recency_unboosting_disjoint_regression"]
    async fn test_recency_unboosting_disjoint() -> Result<ExitCode> {
        let runtime = make_test_runtime().await?;

        let fruit_df = make_fruit_dataframe(&runtime)
            .await?
            .with_column("picked_at", now())?
            .with_column(
                "picked_at",
                to_unixtime(vec![col("picked_at")]) - (lit(86400) * col("id")),
            )?;

        let picked_at_expr = fruit_df.parse_sql_expr("to_timestamp(cast(picked_at as bigint))")?;

        // Rows ordered picked_at DESC
        let fruit_df = fruit_df
            .with_column("picked_at", picked_at_expr)?
            .sort(vec![col("picked_at").sort(false, false)])?;

        // left_fruit: id (2, 3) with (now() - 1 day, now() - 2 day) respectively
        let left_fruit = df_as_embedding_table(&runtime, fruit_df.clone().limit(1, Some(2))?);
        // right_fruit: id (1) with timestamp 1970-01-01
        let right_fruit = df_as_embedding_table(
            &runtime,
            fruit_df.clone().limit(0, Some(1))?.with_column(
                "picked_at",
                fruit_df.parse_sql_expr("to_timestamp(cast(0 as timestamp))")?,
            )?,
        );

        runtime.df.ctx.register_table("left_fruit", left_fruit)?;

        runtime.df.ctx.register_table("right_fruit", right_fruit)?;

        // Baseline: query against self to obtain fused score with recency decay
        let results = test_query!(
            runtime,
            "select * from rrf(vector_search(left_fruit, 'red crispy'), vector_search(right_fruit, 'red crispy'), k => 0, time_column => 'picked_at', decay_constant => 0.25)"
        );

        /*
        Prior to fix:

        Base RRF score for id=1, k=0 = 1/(k + rank) = 1/(0+1) = 1/1
        But should be unboosted!
        | _fused_score       | content                      | id | picked_at           |
        +--------------------+------------------------------+----+---------------------+
        | 1.0                | orange citrus round juicy    | 1  | 1970-01-01T00:00:00 |
        | 0.4723665527410147 | apple fruit sweet red crispy | 3  | 2025-09-23T14:26:15 |
        | 0.3032653298563167 | banana yellow curved fruit   | 2  | 2025-09-24T14:26:15 |
        +--------------------+------------------------------+----+---------------------+

        After:
        | _fused_score       | content                      | id | picked_at           |
        +--------------------+------------------------------+----+---------------------+
        | 0.4723665527410147 | apple fruit sweet red crispy | 3  | 2025-09-23T14:32:53 |
        | 0.3032653298563167 | banana yellow curved fruit   | 2  | 2025-09-24T14:32:53 |
        | 0.0                | orange citrus round juicy    | 1  | 1970-01-01T00:00:00 |
        +--------------------+------------------------------+----+---------------------+
         */
        assert_ne!(extract_column!(results, "id", as_uint64_array)?.value(0), 1);

        Ok(ExitCode::SUCCESS)
    }

    #[cfg(feature = "models")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_rank_weighting() -> Result<ExitCode> {
        let runtime = make_test_runtime().await?;

        let fruit_df = make_fruit_dataframe(&runtime).await?;
        let fruit_embedding_table = df_as_embedding_table(&runtime, fruit_df);

        runtime
            .df
            .ctx
            .register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            runtime,
            "select * from rrf(vector_search(foo, 'yellow', rank_weight => 100), vector_search(foo, 'red', rank_weight => 10))"
        );

        assert_eq!(
            extract_column!(results, "content", as_string_array).value(0),
            "banana yellow curved fruit"
        );

        Ok(ExitCode::SUCCESS)
    }

    #[cfg(feature = "models")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_queries() -> Result<ExitCode> {
        let runtime = make_test_runtime().await?;

        let fruit_df = make_fruit_dataframe(&runtime).await?;
        let fruit_embedding_table = df_as_embedding_table(&runtime, fruit_df);

        runtime
            .df
            .ctx
            .register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            runtime,
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), join_key => 'id', k => 600.0)"
        );

        assert_eq!(
            extract_column!(results, "content", as_string_array).value(0),
            "apple fruit sweet red crispy"
        );

        Ok(ExitCode::SUCCESS)
    }

    #[cfg(feature = "models")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_queries_auto_hash_and_special_idents() -> Result<ExitCode> {
        let runtime = make_test_runtime().await?;

        let fruit_df = make_fruit_dataframe(&runtime)
            .await?
            .with_column("meta_a", named_struct(vec![lit("k1"), lit("v1")]))?
            .with_column("meta_b.special", named_struct(vec![lit("k2"), lit(133.7)]))?;
        let fruit_embedding_table = df_as_embedding_table(&runtime, fruit_df);

        runtime
            .df
            .ctx
            .register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            runtime,
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), k => 600.0)"
        );

        assert_eq!(
            extract_column!(results, "content", as_string_array).value(0),
            "apple fruit sweet red crispy"
        );

        Ok(ExitCode::SUCCESS)
    }

    #[cfg(feature = "models")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_with_case_sensitive_columns() -> Result<ExitCode> {
        let runtime = make_test_runtime().await?;

        let fruit_df = make_fruit_dataframe(&runtime).await?.select(vec![
            col("id").alias("Id"),
            col("content"),
            col("content_embedding"),
            now().alias("pIckEd_AT"),
        ])?;

        let fruit_embedding_table = df_as_embedding_table(&runtime, fruit_df);

        runtime
            .df
            .ctx
            .register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            runtime,
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), join_key => 'Id', k => 600.0, time_column => 'pIckEd_AT')"
        );

        assert_eq!(
            extract_column!(results, "content", as_string_array).value(0),
            "apple fruit sweet red crispy"
        );

        Ok(ExitCode::SUCCESS)
    }

    #[cfg(feature = "models")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_with_dupes() -> Result<ExitCode> {
        let runtime = make_test_runtime().await?;

        let fruit_df = make_fruit_dataframe(&runtime).await?;
        let fruit_df = fruit_df.clone().union(fruit_df)?;
        let fruit_embedding_table = df_as_embedding_table(&runtime, fruit_df);

        runtime
            .df
            .ctx
            .register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            runtime,
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), join_key => 'id', k => 600.0)"
        );

        // There are only 3 unique rows for (id)
        assert_eq!(results[0].num_rows(), 3);

        Ok(ExitCode::SUCCESS)
    }

    #[cfg(feature = "models")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_score_computation() -> Result<ExitCode> {
        let runtime = make_test_runtime().await?;

        let fruit_df = make_fruit_dataframe(&runtime)
            .await?
            .with_column("timestamp", now())?;
        let fruit_table = df_as_embedding_table(&runtime, fruit_df.clone());

        let no_fruit_df = fruit_df
            .clone()
            .limit(0, Some(0))
            .expect("Must have fruit DF");
        let no_fruit_table = df_as_embedding_table(&runtime, no_fruit_df);

        runtime.df.ctx.register_table("foo", fruit_table)?;
        runtime.df.ctx.register_table("bar", no_fruit_table)?;

        let query_empty_red_results = test_query!(
            runtime,
            "select * from rrf(vector_search(bar, 'empty'), vector_search(foo, 'red')) order by _fused_score desc"
        );
        let query_empty_red_content = extract_column!(
            query_empty_red_results,
            RRF_FUSED_SCORE_COLUMN_NAME,
            as_float64_array
        )?;
        let query_empty_red_score = query_empty_red_content.value(0);

        let query_red_empty_results = test_query!(
            runtime,
            "select * from rrf(vector_search(foo, 'red'), vector_search(bar, 'empty'))"
        );
        let query_red_empty_content = extract_column!(
            query_red_empty_results,
            RRF_FUSED_SCORE_COLUMN_NAME,
            as_float64_array
        )?;
        let query_red_empty_score = query_red_empty_content.value(0);

        // Compare permutation of RRF invocations to ensure score is consistent regardless of order
        let score_diff = (query_red_empty_score - query_empty_red_score).abs();
        assert!(score_diff < 0.0001f64);

        // If timestamp column is missing due to FULL OUTER JOIN, ensure a score is still output
        let query_empty_red_recency_results = test_query!(
            runtime,
            "select * from rrf(vector_search(bar, 'empty'), vector_search(foo, 'red'), time_column => 'timestamp')"
        );
        let query_empty_red_recency_scores = extract_column!(
            query_empty_red_recency_results,
            RRF_FUSED_SCORE_COLUMN_NAME,
            as_float64_array
        )?;

        assert!(
            query_empty_red_recency_scores
                .into_iter()
                .all(|f| f.is_some())
        );
        Ok(ExitCode::SUCCESS)
    }

    #[test]
    fn test_parse_argument_exprs() {
        // Empty call
        let empty_args = ReciprocalRankFusionArgs::from_udtf_exprs(&[]);
        assert!(empty_args.is_err());
        assert_eq!(
            empty_args.err().map(|e| e.to_string()),
            Some(
                "Error during planning: rrf needs at least 2 search queries to fuse results."
                    .to_string()
            )
        );

        // Call with at least 2 arguments, but one of them overrides k only
        let one_search_with_k = ReciprocalRankFusionArgs::from_udtf_exprs(&[
            stub_scalar_function("one_search_with_k"),
            spice_named_lit!("k", 42.0),
        ]);
        assert!(one_search_with_k.is_err());
        assert_eq!(
            one_search_with_k.err().map(|e| e.to_string()),
            Some(
                "Error during planning: rrf needs at least 2 search queries to fuse results."
                    .to_string()
            )
        );

        // Call with many searches
        let mut many_search_exprs: Vec<_> = (0..100)
            .map(|i| stub_scalar_function(&format!("fn_{i}")))
            .collect::<Vec<_>>();

        let many_searches = ReciprocalRankFusionArgs::from_udtf_exprs(&many_search_exprs);
        assert!(many_searches.is_ok());
        assert_eq!(
            many_searches
                .expect("Must make args")
                .search_udtf_exprs
                .len(),
            100
        );

        // Call with many searches + k override
        many_search_exprs.push(spice_named_lit!("k", 1337.0f64));
        let many_with_k = ReciprocalRankFusionArgs::from_udtf_exprs(&many_search_exprs);
        assert!(many_with_k.is_ok());

        let many_with_k = many_with_k.expect("Must make args");
        assert_eq!(many_with_k.search_udtf_exprs.len(), 100);
        // assert_eq!(many_with_k.k, 1337.0f64);

        // Call with many searches + k override + join key specified
        many_search_exprs.push(spice_named_lit!("join_key", "hello"));
        let many_with_k_and_column = ReciprocalRankFusionArgs::from_udtf_exprs(&many_search_exprs);
        assert!(many_with_k_and_column.is_ok());

        let many_with_k_and_column = many_with_k_and_column.expect("Must make args");
        assert_eq!(many_with_k_and_column.search_udtf_exprs.len(), 100);
        // assert_eq!(many_with_k_and_column.k, 1337.0f64);
        assert_eq!(many_with_k_and_column.join_key, Some(col("hello")));
    }

    /// Build the `Expr::Alias { expr: Column(name), metadata: { "spice.parameter_name": param } }`
    /// shape that the Spice `DataFusion` fork emits for non-literal named arguments.
    fn aliased_column_named(param: &str, column_name: &str) -> Expr {
        let metadata = FieldMetadata::new(BTreeMap::from([(
            "spice.parameter_name".to_string(),
            param.to_string(),
        )]));
        col(column_name).alias_with_metadata(param, Some(metadata))
    }

    #[test]
    fn test_parse_limit_named_arg() {
        let exprs = vec![
            stub_scalar_function("a"),
            stub_scalar_function("b"),
            spice_named_lit!("limit", 25_u64),
        ];
        let args = ReciprocalRankFusionArgs::from_udtf_exprs(&exprs).expect("Must parse args");
        assert_eq!(args.limit, Some(25));
    }

    #[test]
    fn test_parse_limit_accepts_int64() {
        // SQL integer literals are typically parsed as Int64.
        let exprs = vec![
            stub_scalar_function("a"),
            stub_scalar_function("b"),
            spice_named_lit!("limit", 25_i64),
        ];
        let args = ReciprocalRankFusionArgs::from_udtf_exprs(&exprs).expect("Must parse args");
        assert_eq!(args.limit, Some(25));
    }

    #[test]
    fn test_parse_limit_rejects_negative() {
        let exprs = vec![
            stub_scalar_function("a"),
            stub_scalar_function("b"),
            spice_named_lit!("limit", -1_i64),
        ];
        let err = ReciprocalRankFusionArgs::from_udtf_exprs(&exprs)
            .expect_err("Negative limit should be rejected, not silently ignored");
        assert!(
            err.to_string().contains("non-negative") || err.to_string().contains("must be"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_limit_rejects_non_integer() {
        let exprs = vec![
            stub_scalar_function("a"),
            stub_scalar_function("b"),
            spice_named_lit!("limit", "not a number"),
        ];
        let err = ReciprocalRankFusionArgs::from_udtf_exprs(&exprs)
            .expect_err("Non-integer limit should be rejected");
        assert!(
            err.to_string().contains("must be"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_time_column_as_identifier() {
        // SQL: rrf(s1, s2, time_column => picked_at, join_key => id)
        let exprs = vec![
            stub_scalar_function("a"),
            stub_scalar_function("b"),
            aliased_column_named("time_column", "picked_at"),
            aliased_column_named("join_key", "id"),
        ];
        let args =
            ReciprocalRankFusionArgs::from_udtf_exprs(&exprs).expect("Must parse identifier args");
        assert_eq!(args.time_column, Some(col("picked_at")));
        assert_eq!(args.join_key, Some(col("id")));
    }

    #[test]
    fn test_identifier_and_string_named_args_are_equivalent() {
        let from_string = ReciprocalRankFusionArgs::from_udtf_exprs(&[
            stub_scalar_function("a"),
            stub_scalar_function("b"),
            spice_named_lit!("time_column", "picked_at"),
        ])
        .expect("Must parse string form");
        let from_ident = ReciprocalRankFusionArgs::from_udtf_exprs(&[
            stub_scalar_function("a"),
            stub_scalar_function("b"),
            aliased_column_named("time_column", "picked_at"),
        ])
        .expect("Must parse identifier form");
        assert_eq!(from_string.time_column, from_ident.time_column);
    }

    #[test]
    fn test_unsupported_named_arg_returns_error() {
        // A bare Column expression (no Alias wrapper, no metadata) is not a valid named arg.
        let exprs = vec![
            stub_scalar_function("a"),
            stub_scalar_function("b"),
            col("not_a_named_arg"),
        ];
        let err = ReciprocalRankFusionArgs::from_udtf_exprs(&exprs)
            .expect_err("Bare column should not be accepted as a named arg");
        assert!(
            err.to_string().contains("does not yet support"),
            "unexpected error: {err}"
        );
    }

    // -- filter pushdown tests --------------------------------------------------

    use super::{RRF_FUSED_SCORE_COLUMN_NAME, ReciprocalRankFusion};
    use arrow::datatypes::{Field, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::TableProviderFilterPushDown;

    /// Create a minimal `ReciprocalRankFusion` provider with the given schema,
    /// suitable for testing `supports_filters_pushdown`.
    fn make_rrf_provider(schema: Schema) -> ReciprocalRankFusion {
        let ctx = Arc::new(SessionContext::new());
        ReciprocalRankFusion::from_ctx(&ctx).with_schema(Arc::new(schema))
    }

    #[test]
    fn test_rrf_filter_pushdown_base_column() {
        let provider = make_rrf_provider(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, false),
        ]));

        let filter = col("id").gt(lit(10));
        let result = provider
            .supports_filters_pushdown(&[&filter])
            .expect("pushdown check should succeed");

        assert_eq!(result, vec![TableProviderFilterPushDown::Exact]);
    }

    #[test]
    fn test_rrf_filter_pushdown_fused_score_unsupported() {
        let provider = make_rrf_provider(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(RRF_FUSED_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));

        let filter = col(RRF_FUSED_SCORE_COLUMN_NAME).gt(lit(0.5));
        let result = provider
            .supports_filters_pushdown(&[&filter])
            .expect("pushdown check should succeed");

        assert_eq!(result, vec![TableProviderFilterPushDown::Unsupported]);
    }

    #[test]
    fn test_rrf_filter_pushdown_mixed_filters() {
        let provider = make_rrf_provider(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, false),
            Field::new(RRF_FUSED_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));

        let base_filter = col("id").gt(lit(10));
        let score_filter = col(RRF_FUSED_SCORE_COLUMN_NAME).gt(lit(0.5));
        let content_filter = col("content").eq(lit("test"));

        let result = provider
            .supports_filters_pushdown(&[&base_filter, &score_filter, &content_filter])
            .expect("pushdown check should succeed");

        assert_eq!(
            result,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Exact,
            ]
        );
    }

    #[test]
    fn test_rrf_filter_pushdown_mixed_column_expr_unsupported() {
        let provider = make_rrf_provider(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(RRF_FUSED_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));

        // A filter referencing both a base column and fused_score should not be pushed down.
        let filter = col("id")
            .gt(lit(10))
            .and(col(RRF_FUSED_SCORE_COLUMN_NAME).gt(lit(0.5)));
        let result = provider
            .supports_filters_pushdown(&[&filter])
            .expect("pushdown check should succeed");

        assert_eq!(result, vec![TableProviderFilterPushDown::Unsupported]);
    }

    #[test]
    fn test_rrf_filter_pushdown_constant_expr_exact() {
        let provider =
            make_rrf_provider(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // A constant expression with no column refs is safe to push down.
        let filter = lit(true);
        let result = provider
            .supports_filters_pushdown(&[&filter])
            .expect("pushdown check should succeed");

        assert_eq!(result, vec![TableProviderFilterPushDown::Exact]);
    }
}
