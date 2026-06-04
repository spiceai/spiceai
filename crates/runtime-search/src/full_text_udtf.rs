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

//! A user-defined table function (UDTF) for performing full text search on a preexisting table that has an associated [`crate::datafusion::indexes::full_text::FullTextIndex`] in [`DataFusion`].
//!
//! `text_search(tbl`: `TableReference`, query: &str, col: Option<str>, limit: Option<usize>, `include_score`: Option<bool>)
//!
//! - tbl: Table to perform full text search upon. If the table does not support it (i.e. no index), and empty table is returned.
//! - query: Query to perform full text search against.
//! - col: If provided, use this column to compare vector search results against.
//! - limit:
//! - `include_score` (default true): If false, do not return `score` in the table projection.
//!
//! The schema of the resultant table will be: `schema(tbl) ∪ {score}`, where:
//!  - `score` (f32): The similarity score of the row with the request `query`.

#[cfg(feature = "text_search")]
use arrow_schema::DataType;
#[cfg(feature = "text_search")]
use datafusion::common::exec_err;
#[cfg(feature = "text_search")]
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
#[cfg(feature = "text_search")]
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::Column,
    error::{DataFusionError, Result as DataFusionResult},
    prelude::Expr,
    scalar::ScalarValue,
};
#[cfg(feature = "text_search")]
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};

#[cfg(feature = "text_search")]
use futures::FutureExt;
#[cfg(feature = "text_search")]
use search::generation::text_search::index::FullTextDatabaseIndex;
#[cfg(feature = "elasticsearch")]
use search::index::elasticsearch::ElasticsearchTextIndex;
#[cfg(feature = "text_search")]
use search::{
    index::SearchIndex,
    provider::{SearchQueryProvider, UdtfSource},
};
#[cfg(feature = "text_search")]
use std::any::Any;
#[cfg(feature = "text_search")]
use std::sync::LazyLock;
#[cfg(feature = "text_search")]
use std::sync::{Arc, Weak};

#[cfg(not(feature = "text_search"))]
use crate::table_provider_explorer::TableProviderExplorer;
#[cfg(feature = "text_search")]
use crate::table_provider_explorer::TableProviderExplorer;
#[cfg(feature = "text_search")]
use crate::udtf::{
    TEXT_SEARCH_UDTF_NAME, TextSearchTableFuncArgs, parse_limit_scalar, table_ref_from_column_expr,
};
#[cfg(feature = "text_search")]
use runtime_query_engine::query_engine::QueryEngine;
#[cfg(feature = "text_search")]
use runtime_request_context::{AsyncMarker, RequestContext};

#[cfg(feature = "text_search")]
pub const SPICE_DEFAULT_CATALOG: &str = "spice";
#[cfg(feature = "text_search")]
pub const SPICE_DEFAULT_SCHEMA: &str = "public";

#[cfg(feature = "text_search")]
/// Creates a `UserDefined` signature that allows named parameters (like `rank_weight => X`)
/// to pass through for RRF (Reciprocal Rank Fusion) operations.
///
/// This is required because `DataFusion` v51+ rejects named arguments for functions that use
/// `VariadicAny` signature. The `UserDefined` signature type allows us to:
/// 1. Accept any types (like `VariadicAny`)
/// 2. Support named parameters via `with_parameter_names()`
pub static TEXT_SEARCH_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    // Parameter names that can be passed as named arguments.
    // These are passthrough parameters used by RRF and other table functions.
    let param_names = vec![
        "tbl".to_string(),
        "query".to_string(),
        "column".to_string(),
        "limit".to_string(),
        "include_score".to_string(),
        "rank_weight".to_string(),
    ];
    // SAFETY: These are valid ASCII parameter names
    Signature::user_defined(Volatility::Stable)
        .with_parameter_names(param_names)
        .unwrap_or_else(|_| unreachable!("valid parameter names for text_search"))
});

/// Collect the sorted, deduped set of indexed column names across every FTS index on a
/// table. Used on error paths to build "Indexed column(s): …" messages — never on the
/// happy query path.
#[cfg(feature = "text_search")]
fn all_indexed_fields(fts_indexes: &[&FullTextDatabaseIndex]) -> Vec<String> {
    let mut fields: Vec<String> = fts_indexes
        .iter()
        .flat_map(|idx| idx.search_fields.iter().cloned())
        .collect();
    fields.sort();
    fields.dedup();
    fields
}

/// Suggest the closest indexed column for a misspelled column name using
/// case-insensitive Levenshtein distance. Returns `None` if no column is reasonably close.
#[cfg(feature = "text_search")]
fn suggest_column(target: &str, candidates: &[String]) -> Option<String> {
    crate::udtf::closest_column(target, candidates)
}

#[cfg(feature = "text_search")]
#[derive(Debug)]
pub struct TextSearchTableFunc<E: TableProviderExplorer> {
    df: Weak<dyn QueryEngine>,
    df_ptr: u64,
    explorer: E,
}

#[cfg(not(feature = "text_search"))]
#[derive(Debug)]
pub struct TextSearchTableFunc<E: TableProviderExplorer> {
    _marker: std::marker::PhantomData<E>,
}

#[cfg(feature = "text_search")]
impl<E: TableProviderExplorer> PartialEq for TextSearchTableFunc<E> {
    fn eq(&self, other: &Self) -> bool {
        self.df_ptr == other.df_ptr
    }
}

#[cfg(feature = "text_search")]
impl<E: TableProviderExplorer> Eq for TextSearchTableFunc<E> {}

#[cfg(feature = "text_search")]
impl<E: TableProviderExplorer> std::hash::Hash for TextSearchTableFunc<E> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.df_ptr.hash(state);
    }
}

#[cfg(feature = "text_search")]
impl<E: TableProviderExplorer> TextSearchTableFunc<E> {
    #[must_use]
    pub fn new(df: Weak<dyn QueryEngine>, explorer: E) -> Self {
        let ptr = df.as_ptr().addr() as u64;

        Self {
            df,
            df_ptr: ptr,
            explorer,
        }
    }

    fn scalar_invocation_error<T>() -> Result<T, DataFusionError> {
        exec_err!("{TEXT_SEARCH_UDTF_NAME} does not support scalar invocation.")
    }

    /// Dispatch `text_search()` against a set of [`ElasticsearchTextIndex`] instances.
    /// Mirrors the Tantivy dispatch path but uses `search_fields` directly instead of
    /// `FullTextDatabaseIndex`-specific internals.
    #[cfg(feature = "elasticsearch")]
    fn call_with_es_indexes(
        es_indexes: &[&ElasticsearchTextIndex],
        args: &TextSearchTableFuncArgs,
        table_provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> DataFusionResult<Arc<dyn datafusion::catalog::TableProvider>> {
        if es_indexes.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not have a full text search index.",
                args.tbl
            )));
        }

        let es_index: &ElasticsearchTextIndex = if let Some(ref requested) = args.column {
            es_indexes
                .iter()
                .copied()
                .find(|idx| idx.search_fields.contains(requested))
                .ok_or_else(|| {
                    let all: Vec<String> = es_indexes
                        .iter()
                        .flat_map(|idx| idx.search_fields.iter().cloned())
                        .collect();
                    DataFusionError::Plan(format!(
                        "User function 'text_search' is called on table '{}' that does not have a full text search index on '{}' column. Indexed column(s): {}.{}",
                        args.tbl,
                        requested,
                        all.join(", "),
                        suggest_column(requested, &all)
                            .map(|s| format!(" Did you mean '{s}'?"))
                            .unwrap_or_default()
                    ))
                })?
        } else if es_indexes.len() == 1 {
            es_indexes[0]
        } else {
            let all: Vec<String> = es_indexes
                .iter()
                .flat_map(|idx| idx.search_fields.iter().cloned())
                .collect();
            return Err(DataFusionError::Plan(format!(
                "User function 'text_search' is called on table '{}' that has {} full text search column(s) ({}). Must call 'text_search' with a column parameter, e.g. `text_search(\"my table\", 'my query', my_search_col)`",
                args.tbl,
                all.len(),
                all.join(", "),
            )));
        };

        let column = args.column(&es_index.search_fields)?;
        let mut owned = (*es_index).clone();
        owned.search_fields = vec![column.clone()];
        owned.search_column_name.clone_from(&column);

        let udtf_source = UdtfSource::TextSearch {
            table: args.tbl.to_string(),
            query: args.query.clone(),
            column: Some(column),
            limit: args.limit,
            include_score: args.include_score,
        };

        let normalized_table = owned.normalize_source_table(table_provider)?;

        Ok(Arc::new(
            SearchQueryProvider::try_from_index(
                &(Arc::new(owned) as Arc<dyn SearchIndex>),
                normalized_table,
                args.query.as_str(),
                args.limit,
            )?
            .with_udtf_source(udtf_source)
            .with_include_score(args.include_score.unwrap_or(true))
            .call_on_scan(Arc::new(|| {
                async {
                    let request_context = RequestContext::current(AsyncMarker::new().await);
                    telemetry::track_text_search(&request_context.to_dimensions());
                }
                .boxed()
            })),
        ))
    }
}

#[cfg(feature = "text_search")]
impl<E: TableProviderExplorer> TextSearchTableFunc<E> {
    fn parse_args(args: &[Expr]) -> DataFusionResult<TextSearchTableFuncArgs> {
        // Split args into positional and named. Named args carry a
        // `spice.parameter_name` metadata key (set by the Spice DataFusion
        // fork for `name => value` syntax). Historically we dropped every
        // such arg before positional parsing, which silently discarded
        // user-supplied named args like `limit => 10` or
        // `include_score => false`. Extract them into a map instead and
        // merge into the parsed args after positional parsing.
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
        let mut named: std::collections::HashMap<&str, &Expr> = std::collections::HashMap::new();
        let positional: Vec<&Expr> = args
            .iter()
            .filter(|a| {
                if let Some((name, inner)) = named_param(a) {
                    named.insert(name, inner);
                    false
                } else {
                    true
                }
            })
            .collect();
        let mut args = positional.into_iter();

        let tbl = args.next();
        let Some(Expr::Column(c)) = tbl else {
            return Err(DataFusionError::Plan(format!(
                "First argument must be a table reference, but got a different expression: {tbl:?}."
            )));
        };
        let tbl_ref = table_ref_from_column_expr(c);

        let query = args.next();
        let Some(Expr::Literal(ScalarValue::Utf8(Some(q)), None)) = query else {
            return Err(DataFusionError::Plan(format!(
                "Second argument must be a query string, but got {query:?}."
            )));
        };

        // Positional parsing yields `None` for any optional arg the user did not
        // pass positionally. That way, `include_score => false` as a named arg
        // can override the default; if the default were baked in here as
        // `Some(true)`, the later `include_score.is_none()` guard would never
        // fire and the named value would be silently dropped.
        let (column, limit, include_score) = match (args.next(), args.next(), args.next()) {
            // No arguments
            (None, None, None) => (None, None, None),

            // Single argument cases
            (Some(Expr::Column(Column { name: col, .. })), None, None) => {
                (Some(col.clone()), None, None)
            }
            (Some(Expr::Literal(scalar, None)), None, None) => {
                if let ScalarValue::Boolean(Some(include_score)) = *scalar {
                    (None, None, Some(include_score))
                } else {
                    (None, Some(parse_limit_scalar(scalar)?), None)
                }
            }

            // 2 of 3 arguments. When user provides two of three arguments, they must still be in correct order (i.e. no limit before column)
            (
                Some(Expr::Column(Column { name: col, .. })),
                Some(Expr::Literal(scalar, None)),
                None,
            ) => {
                if let ScalarValue::Boolean(Some(include_score)) = *scalar {
                    (Some(col.clone()), None, Some(include_score))
                } else {
                    (Some(col.clone()), Some(parse_limit_scalar(scalar)?), None)
                }
            }
            (
                Some(Expr::Literal(scalar, None)),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)), None)),
                None,
            ) => (
                None,
                Some(parse_limit_scalar(scalar)?),
                Some(*include_score),
            ),

            // All three arguments provided
            (
                Some(Expr::Column(Column { name: col, .. })),
                Some(Expr::Literal(scalar, None)),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)), None)),
            ) => (
                Some(col.clone()),
                Some(parse_limit_scalar(scalar)?),
                Some(*include_score),
            ),

            // Invalid argument combinations
            (a, b, c) => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid arguments: ({tbl_ref:?}, {q}, {a:?}, {b:?}, {c:?}. Expected (table, query, [column, limit, include_score])."
                )));
            }
        };
        // Merge in named args for any optional field not set positionally.
        let (mut column, mut limit, mut include_score) = (column, limit, include_score);
        if column.is_none() {
            if let Some(Expr::Column(Column { name, .. })) = named.get("column") {
                column = Some(name.clone());
            } else if let Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) = named.get("column") {
                column = Some(s.clone());
            }
        }
        if limit.is_none()
            && let Some(Expr::Literal(scalar, _)) = named.get("limit")
        {
            limit = Some(parse_limit_scalar(scalar)?);
        }
        if include_score.is_none()
            && let Some(Expr::Literal(ScalarValue::Boolean(Some(b)), _)) =
                named.get("include_score")
        {
            include_score = Some(*b);
        }
        // Apply the `include_score` default once named-arg merge is done, so a
        // user-provided `include_score => false` can still flip the default.
        let include_score = include_score.or(Some(true));

        let limit_usize = limit
            .map(|l| {
                usize::try_from(l).map_err(|_| {
                    DataFusionError::Plan(format!(
                        "text_search: limit value {l} is out of range for usize."
                    ))
                })
            })
            .transpose()?;
        Ok(TextSearchTableFuncArgs {
            tbl: tbl_ref
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                .into(),
            query: q.clone(),
            column,
            limit: limit_usize,
            include_score,
        })
    }
}

#[cfg(feature = "text_search")]
impl<E: TableProviderExplorer + 'static> TableFunctionImpl for TextSearchTableFunc<E> {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let args = Self::parse_args(args)?;

        let df = self.df.upgrade().ok_or_else(|| {
            DataFusionError::Plan("An unexpected error occurred when calling text_search(). Report an issue on GitHub: https://github.com/spiceai/spiceai/issues.\nDetails: DataFusion instance has been dropped.".to_string())
        })?;

        let Some(table_provider) = df.get_table_sync(&args.tbl) else {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not exist.",
                args.tbl
            )));
        };

        let fts_indexes = self
            .explorer
            .find_index::<FullTextDatabaseIndex>(&table_provider);

        // Phase 2: try Elasticsearch-backed text indexes if Tantivy found nothing.
        #[cfg(feature = "elasticsearch")]
        if fts_indexes.is_none()
            && let Some((es_indexes, _)) = self
                .explorer
                .find_index::<ElasticsearchTextIndex>(&table_provider)
            && !es_indexes.is_empty()
        {
            return Self::call_with_es_indexes(&es_indexes, &args, Arc::clone(&table_provider));
        }

        let fts_indexes = fts_indexes
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Table '{}' does not have a full text search index.",
                    args.tbl.clone()
                ))
            })?
            .0;

        if fts_indexes.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not have a full text search index.",
                args.tbl.clone()
            )));
        }

        // Pick the index that actually contains the user-requested column. This matters
        // when a table has multiple FTS indexes: previously we popped the last index and
        // then validated the column against only that index, producing incorrect
        // "no index on column" errors for columns that are in fact indexed.
        //
        // `all_search_fields` is only needed on error/disambiguation paths; defer its
        // computation so the happy path (requested column matches the first index, or a
        // single-index table) stays free of extra work.
        let fts_index: FullTextDatabaseIndex = if let Some(ref requested) = args.column {
            if let Some(idx) = fts_indexes
                .iter()
                .find(|idx| idx.search_fields.contains(requested))
            {
                (*idx).clone()
            } else {
                let all_search_fields = all_indexed_fields(&fts_indexes);
                return Err(DataFusionError::Plan(format!(
                    "User function 'text_search' is called on table '{}' that does not have a full text search index on '{requested}' column. Indexed column(s): {}.{}",
                    args.tbl,
                    all_search_fields.join(", "),
                    suggest_column(requested, &all_search_fields)
                        .map(|s| format!(" Did you mean '{s}'?"))
                        .unwrap_or_default()
                )));
            }
        } else if fts_indexes.len() == 1 {
            // Exactly one FTS index. Defer to `column()` which picks the single search
            // field or errors with a helpful message listing the available columns.
            fts_indexes[0].clone()
        } else {
            let all_search_fields = all_indexed_fields(&fts_indexes);
            return Err(DataFusionError::Plan(format!(
                "User function 'text_search' is called on table '{}' that has {} full text search column(s) ({}). Must call 'text_search' with a column parameter, e.g. `text_search(\"my table\", 'my query', my_search_col)`",
                args.tbl,
                all_search_fields.len(),
                all_search_fields.join(", "),
            )));
        };

        // Select single column if needed.
        let column = args.column(&fts_index.search_fields)?;
        let mut fts_index = fts_index;
        fts_index.search_fields = vec![column.clone()];

        // Create UDTF source for distributed serialization
        let udtf_source = UdtfSource::TextSearch {
            table: args.tbl.to_string(),
            query: args.query.clone(),
            column: Some(column),
            limit: args.limit,
            include_score: args.include_score,
        };

        Ok(Arc::new(
            SearchQueryProvider::try_from_index(
                &(Arc::new(fts_index) as Arc<dyn SearchIndex>),
                table_provider,
                args.query.as_str(),
                args.limit,
            )?
            .with_udtf_source(udtf_source)
            .with_include_score(args.include_score.unwrap_or(true))
            .call_on_scan(Arc::new(|| {
                async {
                    let request_context = RequestContext::current(AsyncMarker::new().await);
                    telemetry::track_text_search(&request_context.to_dimensions());
                }
                .boxed()
            })),
        ))
    }
}

#[cfg(feature = "text_search")]
impl<E: TableProviderExplorer + 'static> ScalarUDFImpl for TextSearchTableFunc<E> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        TEXT_SEARCH_UDTF_NAME
    }

    fn signature(&self) -> &Signature {
        &TEXT_SEARCH_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Self::scalar_invocation_error()
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Self::scalar_invocation_error()
    }

    /// Required for `UserDefined` signature - accepts any types like `VariadicAny` would.
    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }
}

#[cfg(all(test, feature = "text_search"))]
mod tests {
    use super::{TextSearchTableFunc, TextSearchTableFuncArgs, suggest_column};
    use crate::table_provider_explorer::TableProviderExplorer;
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::FieldMetadata;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::TableReference;
    use runtime_datafusion_index::Index;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[derive(Debug)]
    struct NoopExplorer;

    impl TableProviderExplorer for NoopExplorer {
        fn find_concrete<'a, T: datafusion::datasource::TableProvider + 'static>(
            &self,
            _tbl: &'a Arc<dyn datafusion::datasource::TableProvider>,
        ) -> Option<&'a T> {
            None
        }

        fn find_index<'a, T: Index + 'static>(
            &self,
            _tbl: &'a Arc<dyn datafusion::datasource::TableProvider>,
        ) -> Option<(Vec<&'a T>, Arc<dyn datafusion::datasource::TableProvider>)> {
            None
        }
    }

    fn fields(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    fn args_with_column(column: Option<&str>) -> TextSearchTableFuncArgs {
        TextSearchTableFuncArgs {
            tbl: TableReference::bare("docs"),
            query: "hello".to_string(),
            column: column.map(str::to_string),
            limit: None,
            include_score: Some(true),
        }
    }

    #[test]
    fn suggest_column_returns_close_match() {
        let cands = fields(&["title", "body", "summary"]);
        assert_eq!(suggest_column("titel", &cands), Some("title".to_string()));
        // Case-insensitive
        assert_eq!(suggest_column("TITLE", &cands), Some("title".to_string()));
        // Exact
        assert_eq!(suggest_column("body", &cands), Some("body".to_string()));
    }

    #[test]
    fn suggest_column_returns_none_for_distant() {
        let cands = fields(&["title", "body"]);
        assert_eq!(suggest_column("completely_unrelated", &cands), None);
    }

    #[test]
    fn suggest_column_handles_empty_candidates() {
        assert_eq!(suggest_column("anything", &[]), None);
    }

    #[test]
    fn args_column_picks_unique_search_field() {
        let args = args_with_column(None);
        let picked = args
            .column(&fields(&["body"]))
            .expect("Single search field should be picked automatically");
        assert_eq!(picked, "body");
    }

    #[test]
    fn args_column_errors_on_no_search_fields() {
        let args = args_with_column(None);
        let err = args.column(&[]).expect_err("No search fields should error");
        assert!(
            err.to_string()
                .contains("no associated full text search index")
        );
    }

    #[test]
    fn args_column_errors_when_multiple_and_unspecified() {
        let args = args_with_column(None);
        let err = args
            .column(&fields(&["title", "body"]))
            .expect_err("Multiple search fields without explicit column should error");
        let msg = err.to_string();
        assert!(msg.contains("title"));
        assert!(msg.contains("body"));
        assert!(msg.contains("Must call 'text_search' with column parameter"));
    }

    #[test]
    fn args_column_validates_explicit_choice() {
        let args = args_with_column(Some("body"));
        let picked = args
            .column(&fields(&["title", "body"]))
            .expect("Explicit valid column should succeed");
        assert_eq!(picked, "body");
    }

    #[test]
    fn args_column_errors_with_did_you_mean_for_typo() {
        let args = args_with_column(Some("titel"));
        let err = args
            .column(&fields(&["title", "body"]))
            .expect_err("Unknown column should error");
        let msg = err.to_string();
        assert!(msg.contains("'title'"), "expected suggestion in: {msg}");
        assert!(
            msg.contains("Did you mean"),
            "expected suggestion in: {msg}"
        );
    }

    #[test]
    fn args_column_errors_without_suggestion_for_distant_typo() {
        let args = args_with_column(Some("completely_unrelated"));
        let err = args
            .column(&fields(&["title", "body"]))
            .expect_err("Unknown column should error");
        let msg = err.to_string();
        assert!(
            !msg.contains("Did you mean"),
            "should not suggest a far match: {msg}"
        );
    }

    fn passthrough_lit(name: &str) -> Expr {
        let meta = FieldMetadata::new(BTreeMap::from([(
            "spice.parameter_name".to_string(),
            name.to_string(),
        )]));
        Expr::Literal(ScalarValue::Float64(Some(2.0)), Some(meta))
    }

    #[test]
    fn parse_args_filters_passthrough_named_args() {
        // Args: tbl, query, then a passthrough `rank_weight` literal that
        // belongs to RRF, not to text_search itself. Must be ignored.
        let exprs = vec![
            Expr::Column(Column::new_unqualified("docs")),
            Expr::Literal(ScalarValue::Utf8(Some("hello".to_string())), None),
            passthrough_lit("rank_weight"),
        ];
        let parsed = TextSearchTableFunc::<NoopExplorer>::parse_args(&exprs)
            .expect("Passthrough named arg should be ignored");
        assert_eq!(parsed.query, "hello");
        assert_eq!(parsed.column, None);
        assert_eq!(parsed.limit, None);
        assert_eq!(parsed.include_score, Some(true));
    }

    #[test]
    fn parse_args_passthrough_does_not_consume_real_args() {
        // Same as above but with an explicit column following the passthrough.
        let exprs = vec![
            Expr::Column(Column::new_unqualified("docs")),
            Expr::Literal(ScalarValue::Utf8(Some("hello".to_string())), None),
            passthrough_lit("rank_weight"),
            Expr::Column(Column::new_unqualified("body")),
        ];
        let parsed = TextSearchTableFunc::<NoopExplorer>::parse_args(&exprs)
            .expect("Passthrough named arg should be ignored");
        assert_eq!(parsed.column.as_deref(), Some("body"));
    }

    fn named_arg(name: &str, value: ScalarValue) -> Expr {
        let meta = FieldMetadata::new(BTreeMap::from([(
            "spice.parameter_name".to_string(),
            name.to_string(),
        )]));
        Expr::Literal(value, Some(meta))
    }

    #[test]
    fn parse_args_named_include_score_overrides_default() {
        // `text_search(docs, 'hello', include_score => false)` must honor the
        // named arg. Previously, the positional matcher baked in `Some(true)`
        // when the user omitted it positionally, so the named value was
        // silently dropped.
        let exprs = vec![
            Expr::Column(Column::new_unqualified("docs")),
            Expr::Literal(ScalarValue::Utf8(Some("hello".to_string())), None),
            named_arg("include_score", ScalarValue::Boolean(Some(false))),
        ];
        let parsed = TextSearchTableFunc::<NoopExplorer>::parse_args(&exprs)
            .expect("Named include_score should parse");
        assert_eq!(parsed.include_score, Some(false));
    }
}
