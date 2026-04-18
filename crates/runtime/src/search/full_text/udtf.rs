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

use arrow_schema::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::Column,
    error::{DataFusionError, Result as DataFusionResult},
    prelude::Expr,
    scalar::ScalarValue,
    sql::TableReference,
};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};

use moka::future::FutureExt;
use search::{
    generation::text_search::index::FullTextDatabaseIndex,
    index::SearchIndex,
    provider::{SearchQueryProvider, UdtfSource},
};
use std::any::Any;
use std::sync::LazyLock;
use std::sync::{Arc, Weak};

use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::{
    datafusion::DataFusion,
    embeddings::udtf::parse_limit_scalar,
    search::util::{find_index_in_table_provider, table_ref_from_column_expr, to_column_expr},
};
use runtime_request_context::{AsyncMarker, RequestContext};

pub static TEXT_SEARCH_UDTF_NAME: &str = "text_search";

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

#[derive(Debug, PartialEq, Clone)]
pub struct TextSearchTableFuncArgs {
    pub tbl: TableReference,
    pub query: String,

    pub column: Option<String>,
    pub limit: Option<usize>,
    pub include_score: Option<bool>,
}

impl TextSearchTableFuncArgs {
    // Find column to perform full text search upon. Use either column specified in
    // [`TextSearchTableFuncArgs`] or if there is only one column in `search_fields`.
    fn column(&self, search_fields: &[String]) -> datafusion::error::Result<String> {
        let TextSearchTableFuncArgs { column, tbl, .. } = &self;
        let col: String = if let Some(col) = column {
            if !search_fields.contains(col) {
                return Err(DataFusionError::Execution(format!(
                    "User function 'text_search' is called on table '{tbl}' that does not have a full text search index on '{col}' column. Index is on column(s): {}.{}",
                    search_fields.join(", "),
                    suggest_column(col, search_fields)
                        .map(|s| format!(" Did you mean '{s}'?"))
                        .unwrap_or_default()
                )));
            }
            col.clone()
        } else {
            let mut fields = search_fields.iter();

            match (fields.next(), fields.next()) {
                (Some(field), None) => field.clone(),
                (Some(_), Some(_)) => {
                    return Err(DataFusionError::Execution(format!(
                        "User function 'text_search' is called on table '{tbl}' that has {} full text search columns ({}). Must call 'text_search' with column parameter, e.g. `text_search(\"my table\", 'my query', my_search_col)`",
                        search_fields.len(),
                        search_fields.join(", ")
                    )));
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "User function 'text_search' is called on table '{tbl}' that has no associated full text search index"
                    )));
                }
            }
        };
        Ok(col)
    }
}

/// Suggest the closest indexed column for a misspelled column name using
/// case-insensitive Levenshtein distance. Returns `None` if no column is reasonably close.
fn suggest_column(target: &str, candidates: &[String]) -> Option<String> {
    let target_lower = target.to_lowercase();
    let (best, distance) = candidates
        .iter()
        .map(|c| (c, levenshtein(&target_lower, &c.to_lowercase())))
        .min_by_key(|(_, d)| *d)?;

    // Only suggest if the edit distance is small relative to the target length.
    // This avoids nonsense suggestions for wildly different names.
    let threshold = target.len().div_ceil(2).max(2);
    if distance <= threshold {
        Some(best.clone())
    } else {
        None
    }
}

/// Compute Levenshtein edit distance between two byte strings. Operates on UTF-8
/// code units which is sufficient for column-name typo detection.
fn levenshtein(a: &str, b: &str) -> usize {
    if a == b {
        return 0;
    }
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.is_empty() {
        return b_bytes.len();
    }
    if b_bytes.is_empty() {
        return a_bytes.len();
    }

    let mut prev: Vec<usize> = (0..=b_bytes.len()).collect();
    let mut curr: Vec<usize> = vec![0; b_bytes.len() + 1];
    for (i, &ac) in a_bytes.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &bc) in b_bytes.iter().enumerate() {
            let cost = usize::from(ac != bc);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b_bytes.len()]
}

#[derive(Debug)]
pub struct TextSearchTableFunc {
    // This needs to be a weak reference because the DataFusion instance contains the SessionContext which contains this UDTF.
    df: Weak<DataFusion>,
    // store a pointer to use for Hash/Eq since UDTF impls require this trait bound but we cannot feasibly make `DataFusion` implement them.
    df_ptr: u64,
}

impl PartialEq for TextSearchTableFunc {
    fn eq(&self, other: &Self) -> bool {
        self.df_ptr == other.df_ptr
    }
}

impl Eq for TextSearchTableFunc {}

impl std::hash::Hash for TextSearchTableFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.df_ptr.hash(state);
    }
}

impl TextSearchTableFunc {
    #[must_use]
    pub fn new(df: Weak<DataFusion>) -> Self {
        let ptr = df.as_ptr().addr() as u64;

        Self { df, df_ptr: ptr }
    }

    fn scalar_invocation_error<T>() -> Result<T, DataFusionError> {
        exec_err!("{TEXT_SEARCH_UDTF_NAME} does not support scalar invocation.")
    }
}

impl TextSearchTableFunc {
    pub(crate) fn to_expr(args: &TextSearchTableFuncArgs) -> Vec<Expr> {
        let mut expr = vec![
            Expr::Column(to_column_expr(&args.tbl)),
            Expr::Literal(ScalarValue::Utf8(Some(args.query.clone())), None),
        ];

        if let Some(col) = args.column.as_ref() {
            expr.push(Expr::Column(Column::new_unqualified(col)));
        }

        if let Some(limit) = args.limit {
            expr.push(Expr::Literal(
                ScalarValue::UInt64(Some(u64::try_from(limit).unwrap_or(u64::MAX))),
                None,
            ));
        }

        if let Some(include_score) = args.include_score {
            expr.push(Expr::Literal(
                ScalarValue::Boolean(Some(include_score)),
                None,
            ));
        }

        expr
    }

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
        let mut named: std::collections::HashMap<&str, &Expr> =
            std::collections::HashMap::new();
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

        let (column, limit, include_score) = match (args.next(), args.next(), args.next()) {
            // No arguments, provides defaults
            (None, None, None) => (None, None, Some(true)),

            // Single argument cases
            (Some(Expr::Column(Column { name: col, .. })), None, None) => {
                (Some(col.clone()), None, Some(true))
            }
            (Some(Expr::Literal(scalar, None)), None, None) => {
                if let ScalarValue::Boolean(Some(include_score)) = *scalar {
                    (None, None, Some(include_score))
                } else {
                    (None, Some(parse_limit_scalar(scalar)?), Some(true))
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
                    (
                        Some(col.clone()),
                        Some(parse_limit_scalar(scalar)?),
                        Some(true),
                    )
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
        Ok(TextSearchTableFuncArgs {
            tbl: tbl_ref
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                .into(),
            query: q.clone(),
            column,
            limit: limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
            include_score,
        })
    }
}

impl TableFunctionImpl for TextSearchTableFunc {
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

        let fts_indexes = find_index_in_table_provider::<FullTextDatabaseIndex>(&table_provider)
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

        // Aggregate all indexed columns across every FTS index on this table so we can
        // both match the user's requested column to the correct index and surface a
        // helpful error that lists every searchable column.
        let mut all_search_fields: Vec<String> = fts_indexes
            .iter()
            .flat_map(|idx| idx.search_fields.iter().cloned())
            .collect();
        all_search_fields.sort();
        all_search_fields.dedup();

        // Pick the index that actually contains the user-requested column. This matters
        // when a table has multiple FTS indexes: previously we popped the last index and
        // then validated the column against only that index, producing incorrect
        // "no index on column" errors for columns that are in fact indexed.
        let fts_index: FullTextDatabaseIndex = if let Some(ref requested) = args.column {
            fts_indexes
                .iter()
                .find(|idx| idx.search_fields.contains(requested))
                .copied()
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "User function 'text_search' is called on table '{}' that does not have a full text search index on '{requested}' column. Indexed column(s): {}.{}",
                        args.tbl,
                        all_search_fields.join(", "),
                        suggest_column(requested, &all_search_fields)
                            .map(|s| format!(" Did you mean '{s}'?"))
                            .unwrap_or_default()
                    ))
                })?
        } else if fts_indexes.len() == 1 {
            // Exactly one FTS index. Defer to `column()` which picks the single search
            // field or errors with a helpful message listing the available columns.
            fts_indexes[0].clone()
        } else {
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

impl ScalarUDFImpl for TextSearchTableFunc {
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

#[cfg(test)]
mod tests {
    use super::{TextSearchTableFunc, TextSearchTableFuncArgs, levenshtein, suggest_column};
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::FieldMetadata;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::TableReference;
    use std::collections::BTreeMap;

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
    fn levenshtein_basic_cases() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("title", "titel"), 2);
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
        let parsed = TextSearchTableFunc::parse_args(&exprs)
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
        let parsed = TextSearchTableFunc::parse_args(&exprs)
            .expect("Passthrough named arg should be ignored");
        assert_eq!(parsed.column.as_deref(), Some("body"));
    }
}
