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

//! `DataFusion` filter pushdown into tantivy full-text queries.
//!
//! This crate owns the translation of a `DataFusion` [`datafusion::logical_expr::Expr`] filter into
//! a tantivy [`tantivy::query::Query`], and the matching
//! [`datafusion::logical_expr::TableProviderFilterPushDown`] classification, so a full-text index
//! can apply SQL filters *inside* the tantivy scan rather than above it.
//!
//! Correctness invariant: [`classify_filter`] and [`translate_filter`] are two views over the same
//! translation pass, so a filter reported `Exact`/`Inexact` is exactly the one the executor can
//! build a tantivy query for, and one reported `Unsupported` never is. An `Exact` filter's tantivy
//! query matches exactly the SQL predicate; an `Inexact` filter's query matches a *superset*
//! (`DataFusion` re-checks it above the scan); neither ever matches a subset (which would drop rows).
//!
//! It also re-exports the generic tantivy/Arrow helpers the translation relies on:
//! [`array_to_terms`] (Arrow-array → tantivy [`tantivy::Term`] encoding, so literal encoding
//! matches index-write encoding) and [`is_tokenized`]/[`text_tokenizer`] (tantivy text-field
//! analysis inspection).

mod filter;
mod schema;
mod terms;

pub use filter::{classify_filter, translate_filter};
pub use schema::{is_tokenized, text_tokenizer};
pub use terms::array_to_terms;
