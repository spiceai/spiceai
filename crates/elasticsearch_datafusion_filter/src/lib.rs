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

//! # Elasticsearch `DataFusion` filter pushdown
//!
//! Translates `DataFusion` SQL filter [`Expr`](datafusion::logical_expr::Expr)s into Elasticsearch
//! query DSL clauses (in a non-scoring `bool.filter` context) so that predicates evaluate inside
//! Elasticsearch instead of being fetched and re-filtered in `DataFusion`.
//!
//! The two entry points are two views over one internal translation pass, so they never
//! disagree:
//! - [`classify_filter`] returns a
//!   [`TableProviderFilterPushDown`](datafusion::logical_expr::TableProviderFilterPushDown) for a
//!   provider's `supports_filters_pushdown`.
//! - [`translate_filter`] returns the emitted Elasticsearch clause (or `None`) for the scan.
//!
//! ## Exactness invariant (correctness-critical)
//!
//! - `Exact` ⟹ the emitted clause matches the SQL predicate *exactly* under Elasticsearch
//!   semantics. `DataFusion` drops the predicate above the scan.
//! - `Inexact` ⟹ the clause matches a *superset* of the predicate. `DataFusion` keeps the
//!   predicate above the scan and re-checks every returned row.
//! - The clause is **never** a subset — that would silently drop matching rows.
//!
//! When it is not provably exact, the pass downgrades to `Inexact` or `Unsupported`, never a
//! wrong `Exact`.
//!
//! ## Predicate → Elasticsearch DSL
//!
//! | SQL predicate | Elasticsearch clause | Exactness |
//! |---|---|---|
//! | `col = v` (integer/boolean/keyword) | `term` | Exact |
//! | `col = v` (float / analyzed `text`) | `term` (on `.keyword` for text) | Inexact |
//! | `col IN (..)` | `terms` | Exact/Inexact (as `=`) |
//! | `col < v`, `<=`, `>`, `>=` (integer) | `range` | Exact |
//! | `col < v`, .. (float / keyword) | `range` | Inexact |
//! | `col BETWEEN a AND b` | `range` `gte`/`lte` | Exact/Inexact (as range) |
//! | `col IS NULL` | `bool.must_not` `exists` | Inexact |
//! | `col IS NOT NULL` | `exists` | Inexact |
//! | `col LIKE 'x%'` (keyword) | `prefix` | Inexact |
//! | `col <> v`, `NOT p`, `NOT IN`, `NOT BETWEEN` | `bool.must_not` (only if base is Exact) | Inexact |
//! | `a AND b` | `bool.filter` | Exact if both Exact |
//! | `a OR b` | `bool.should` + `minimum_should_match: 1` | Exact if both Exact |
//!
//! Anything else — a predicate on an unmapped/non-indexed column, a type-mismatched literal, a
//! negation of a superset clause, a partial `OR`, dates/timestamps — is `Unsupported`.

pub mod error;
pub mod schema;
mod translate;

pub use error::{Error, Result};
pub use schema::{EsFieldType, EsFilterSchema, EsMappingField};
pub use translate::{classify_filter, translate_filter};
