# Tantivy DataFusion Filter Pushdown

Translates Apache DataFusion `Expr` filters into tantivy `Query`s, so a full-text search index can
apply SQL predicates *inside* the tantivy scan instead of re-checking them above the candidate set.

## Pushdown classes

Every filter is classified for `TableProvider::supports_filters_pushdown` as one of:

- **`Exact`** — the tantivy query matches exactly the SQL predicate (e.g. equality on an indexed
  integer/bool/untokenized-string column, or a numeric range/`BETWEEN`).
- **`Inexact`** — the tantivy query matches a *superset* of the SQL predicate, which DataFusion
  re-checks above the scan (e.g. float equality, any negation over a nullable column, prefix
  `LIKE 'x%'`).
- **`Unsupported`** — the filter cannot be pushed (e.g. term filters against a tokenized text
  column, `IS NULL`, unknown columns).

`classify_filter` and `translate_filter` are two views over the same translation pass, so a filter
advertised as pushable always translates, and a translation is never a subset of the predicate
(which would silently drop rows).

## Usage

```rust
use tantivy_datafusion_filter::{classify_filter, translate_filter};

// `schema` is the `tantivy::schema::Schema` of the index; `expr` a DataFusion `Expr`.
let support = classify_filter(&schema, &expr);
if let Some(query) = translate_filter(&schema, &expr) {
    // `Must`-combine `query` with the full-text query and run it inside the index.
}
```

The crate also re-exports the generic tantivy/Arrow helpers the translation relies on:
`array_to_terms` (Arrow-array → tantivy `Term` encoding) and `is_tokenized` / `text_tokenizer`
(tantivy text-field analysis inspection).
