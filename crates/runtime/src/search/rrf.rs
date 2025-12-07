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
    ColumnarValue, DocSection, Documentation, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionContext, coalesce, exp, greatest, now, to_unixtime};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ExprFunctionExt, col, ident, lit};
use itertools::Itertools;
use runtime_datafusion_udfs::digest_many::digest_many;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

pub static RRF_UDF_NAME: &str = "rrf";
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
}

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

        let (search_udtfs, subquery_args): (Vec<_>, Vec<_>) = rrf_args
            .extract_if(.., |arg| matches!(arg, Expr::ScalarFunction(_)))
            .map(|e| ReciprocalRankFusionSubqueryArgs::from_scalar_function_expr(&e))
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
            k: extract_f64!(rrf_args, "k").unwrap_or(60.0),
            join_key: extract_string!(rrf_args, "join_key").map(ident),
            time_column: extract_string!(rrf_args, "time_column").map(ident),
            recency_decay: extract_string!(rrf_args, "recency_decay")
                .and_then(|rd| RecencyDecay::from_str(&rd).ok()),
            decay_constant: extract_f64!(rrf_args, "decay_constant"),
            decay_scale_secs: extract_f64!(rrf_args, "decay_scale_secs"),
            decay_window_secs: extract_f64!(rrf_args, "decay_window_secs"),
        })
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
    df: Option<DataFrame>,
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
            df: None,
        }
    }

    #[must_use]
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    #[must_use]
    pub fn with_df(mut self, df: DataFrame) -> Self {
        self.df = Some(df);
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
            score_expr.alias("fused_score")
        } else {
            return exec_err!("{RRF_UDF_NAME} unable to compute fused_score");
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
        Ok((score_expr * coalesce(vec![recency_expr, lit(1.0)])).alias("fused_score"))
    }

    // Given arguments to n search calls: execute searches, generate row IDs, rank by score, JOIN,
    // then finally re-rank and sort fused results
    fn rerank_and_fuse_df(&self, args: &ReciprocalRankFusionArgs) -> Result<DataFrame> {
        let (subquery_dfs, join_key) = self.prepare_and_execute_subqueries(args)?;
        let score_expr = Self::compute_score_expr(args, &subquery_dfs)?;

        // Create column expressions for final projection
        let mut columns: Vec<Expr> = vec![score_expr];
        columns.extend(subquery_dfs[0].schema().columns().iter().filter_map(|c| {
            match c.name.as_str() {
                "rank" | "score" => None,
                // TODO: do we want the embedding in the final projection?
                other if other.ends_with("_embedding") => None,
                other => Some(
                    coalesce(
                        (0..subquery_dfs.len())
                            .map(|i| col_qualified!(format!("search_{i}"), other))
                            .collect(),
                    )
                    .alias(other),
                ),
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
            let mut agg_cols = vec![max(col("fused_score")).alias("fused_score")];

            // The first column is the score_expr, which gets special treatment above.
            // These are unaliased, because they get flattened by coalesce() in the first select
            agg_cols.extend(columns.iter().skip(1).filter_map(|c| {
                let (_, cname) = c.qualified_name();

                // Do not aggregate the join key
                if cname == join_key.qualified_name().1 {
                    None
                } else {
                    Some(
                        first_value(ident(&cname), vec![col("fused_score").sort(false, false)])
                            .alias(&cname),
                    )
                }
            }));

            joined
                .select(columns)?
                .aggregate(vec![join_key], agg_cols)?
                .drop_columns(&["__spice_rrf_row_id"])?
                .sort(vec![col("fused_score").sort(false, false)])
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
    fn prepare_and_execute_subqueries(
        &self,
        args: &ReciprocalRankFusionArgs,
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
                if !df.schema().has_column_with_unqualified_name("score") {
                    return exec_err!(
                        "{RRF_UDF_NAME}: Query at position {i} does not have a `score` column."
                    );
                }

                let df_with_id = match join_key {
                    Some(_) => Ok(df),
                    None => Self::with_rrf_rowid(df),
                };

                df_with_id
                    .and_then(Self::with_rank)
                    .and_then(|df| df.alias(&format!("search_{i}")))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((prepared_dfs, join_key.unwrap_or(col("__spice_rrf_row_id"))))
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

        a.join_on(
            b,
            JoinType::Full,
            vec![col_qualified!(tbl_a, id_a).eq(col_qualified!(tbl_b, id_b))],
        )
    }

    // Window and rank a search subquery by its `score` field, exposing a `rank` column
    fn with_rank(df: DataFrame) -> Result<DataFrame> {
        let rank_expr = row_number()
            .order_by(vec![col("score").sort(false, false)])
            .build()?
            .alias("rank");

        df.window(vec![rank_expr])
    }

    // Create an internal row ID by hashing all pieces of the row
    fn with_rrf_rowid(df: DataFrame) -> Result<DataFrame> {
        let bin_columns: Vec<Expr> = df
            .schema()
            .columns()
            .iter()
            .sorted_by_key(|c| c.name())
            // Don't hash embeddings or scores
            .filter_map(|c| match c.name() {
                "score" => None,
                name if name.ends_with("_embedding") => None,
                name => Some(ident(name)),
            })
            .collect::<Vec<_>>();

        df.with_column("__spice_rrf_row_id", digest_many(bin_columns, "md5"))
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
        let rrf_args = ReciprocalRankFusionArgs::from_udtf_exprs(args)?;
        let rerank_and_fuse_df = self.rerank_and_fuse_df(&rrf_args)?;
        Ok(Arc::new(
            ReciprocalRankFusion::from_ctx(&self.session_context).with_df(rerank_and_fuse_df),
        ))
    }
}

#[async_trait]
impl TableProvider for ReciprocalRankFusion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        match self.df.as_ref() {
            Some(df) => Arc::clone(df.schema().inner()),
            None => panic!("ReciprocalRankFusion schema is not set. This is a bug in Spice.ai"),
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(ref df) = self.df {
            let mut df = df.clone();

            if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
                df = df.filter(filter)?;
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
        } else {
            exec_err!("ReciprocalRankFusion could not create physical plan")
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Runtime;
    use crate::builder::RuntimeBuilder;
    use crate::datafusion::query::QueryBuilder;
    use crate::datafusion::udf::register_udfs;
    use crate::embeddings::table::EmbeddingColumnConfig;
    use crate::embeddings::table::EmbeddingTable;
    use crate::search::rrf::ReciprocalRankFusionArgs;
    use arrow::array::as_string_array;
    use arrow::record_batch::RecordBatch;
    use async_graphql::futures_util::TryStreamExt;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::catalog::TableProvider;
    use datafusion::common::Result;
    use datafusion::common::cast::{as_float64_array, as_uint64_array};
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::col;
    use datafusion::logical_expr::expr::FieldMetadata;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion::prelude::{DataFrame, named_struct, now, to_unixtime};
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{ExprFunctionExt, lit};
    use llms::model2vec::Model2Vec;
    use runtime_request_context::{Protocol, RequestContext};
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::process::ExitCode;
    use std::sync::{Arc, LazyLock};

    pub static TEST_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> =
        LazyLock::new(|| Arc::new(RequestContext::builder(Protocol::Internal).build()));

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
    fn df_as_embedding_table(runtime: &Runtime, df: DataFrame) -> Arc<dyn TableProvider> {
        let mut embedded_columns = HashMap::new();
        embedded_columns.insert(
            "content".to_string(),
            EmbeddingColumnConfig {
                model_name: "test_model".to_string(),
                vector_size: 64,
                in_base_table: true,
                chunker: None,
            },
        );

        Arc::new(EmbeddingTable {
            base_table: df.into_view(),
            embedded_columns,
            embedding_models: Arc::clone(&runtime.embeds),
        })
    }

    async fn make_test_runtime() -> Result<Runtime> {
        let rt = RuntimeBuilder::new().build().await;
        rt.df
            .ctx
            .state()
            .config_mut()
            .set_extension(Arc::clone(&TEST_REQUEST_CONTEXT));

        let embedding_model = Arc::new(
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
        );
        rt.embeds
            .write()
            .await
            .insert("test_model".to_string(), embedding_model);

        register_udfs(&rt).await;
        Ok(rt)
    }

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

    #[tokio::test(flavor = "multi_thread")]
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
        | fused_score        | content                      | id | picked_at           |
        +--------------------+------------------------------+----+---------------------+
        | 1.0                | orange citrus round juicy    | 1  | 1970-01-01T00:00:00 |
        | 0.4723665527410147 | apple fruit sweet red crispy | 3  | 2025-09-23T14:26:15 |
        | 0.3032653298563167 | banana yellow curved fruit   | 2  | 2025-09-24T14:26:15 |
        +--------------------+------------------------------+----+---------------------+

        After:
        | fused_score        | content                      | id | picked_at           |
        +--------------------+------------------------------+----+---------------------+
        | 0.4723665527410147 | apple fruit sweet red crispy | 3  | 2025-09-23T14:32:53 |
        | 0.3032653298563167 | banana yellow curved fruit   | 2  | 2025-09-24T14:32:53 |
        | 0.0                | orange citrus round juicy    | 1  | 1970-01-01T00:00:00 |
        +--------------------+------------------------------+----+---------------------+
         */
        assert_ne!(extract_column!(results, "id", as_uint64_array)?.value(0), 1);

        Ok(ExitCode::SUCCESS)
    }

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
            "select * from rrf(vector_search(bar, 'empty'), vector_search(foo, 'red')) order by fused_score desc"
        );
        let query_empty_red_content =
            extract_column!(query_empty_red_results, "fused_score", as_float64_array)?;
        let query_empty_red_score = query_empty_red_content.value(0);

        let query_red_empty_results = test_query!(
            runtime,
            "select * from rrf(vector_search(foo, 'red'), vector_search(bar, 'empty'))"
        );
        let query_red_empty_content =
            extract_column!(query_red_empty_results, "fused_score", as_float64_array)?;
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
            "fused_score",
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
}
