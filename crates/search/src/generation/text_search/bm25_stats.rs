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

//! Collection statistics for BM25 scoring, gathered across the partitions of a
//! distributed full-text index.
//!
//! In a single-node index, Tantivy scores a query with the collection statistics
//! of the one local index: the number of documents `N`, the per-term document
//! frequency `df(term)`, and the total number of tokens in the search field
//! (from which the average document length is derived). In a multi-node
//! accelerated table, each executor holds only its own partition, so its local
//! statistics differ from every other executor's. A BM25 score computed with
//! local statistics is therefore not comparable across executors.
//!
//! `N`, `df(term)`, and the total token count are additive over disjoint
//! partitions, so the global value of each is the sum of the local values. This
//! module carries that sum ([`GlobalBm25Stats`]) and exposes it to Tantivy as a
//! [`tantivy::query::Bm25StatisticsProvider`] ([`GlobalBm25Provider`]), so each
//! executor scores its own partition with the same global statistics and the
//! resulting scores are comparable.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use arrow::error::ArrowError;
use serde::{Deserialize, Serialize};
use tantivy::query::Bm25StatisticsProvider;
use tantivy::schema::Field;
use tantivy::{Searcher, Term};

/// Column of the `text_search_stats` output: the analyzed query term.
pub const STATS_TERM_COLUMN: &str = "term";
/// Column of the `text_search_stats` output: the term's local document frequency.
pub const STATS_DOC_FREQ_COLUMN: &str = "doc_freq";
/// Column of the `text_search_stats` output: the partition's document count `N`.
pub const STATS_TOTAL_NUM_DOCS_COLUMN: &str = "total_num_docs";
/// Column of the `text_search_stats` output: the partition's total token count.
pub const STATS_TOTAL_NUM_TOKENS_COLUMN: &str = "total_num_tokens";

/// Collection statistics for BM25 scoring over a single search field.
///
/// A `GlobalBm25Stats` value is either the local statistics of one partition
/// (as produced by the `text_search_stats` UDTF on an executor) or the sum of
/// the local statistics across every partition (the global statistics used to
/// score). Because the statistics are additive over disjoint partitions, the
/// same type represents both, and [`GlobalBm25Stats::add`] sums them.
///
/// `doc_freq` is keyed by the analyzed (tokenized and stemmed) term text, so
/// the key of a query term matches the term Tantivy scores against, provided
/// every partition shares the index analyzer (which they do — the index schema
/// and tokenizer are identical across executors).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalBm25Stats {
    /// The number of documents in the collection (`N`). Tantivy counts this as
    /// the sum of each segment's `max_doc`, which includes documents superseded
    /// by a later update until a merge removes them.
    pub total_num_docs: u64,

    /// The total number of tokens in the search field across every document.
    /// Tantivy derives the average document length as
    /// `total_num_tokens / total_num_docs`.
    pub total_num_tokens: u64,

    /// The document frequency of each analyzed query term: the number of
    /// documents that contain the term. Absent terms are omitted (an absent key
    /// scores as `df = 0`). A `BTreeMap` keeps the encoding stable.
    pub doc_freq: BTreeMap<String, u64>,
}

impl GlobalBm25Stats {
    /// Add the statistics of another partition into these, in place. The sum of
    /// two partitions' statistics is the statistics of their union, provided the
    /// partitions hold disjoint documents.
    pub fn add(&mut self, other: &GlobalBm25Stats) {
        self.total_num_docs = self.total_num_docs.saturating_add(other.total_num_docs);
        self.total_num_tokens = self.total_num_tokens.saturating_add(other.total_num_tokens);
        for (term, df) in &other.doc_freq {
            let entry = self.doc_freq.entry(term.clone()).or_insert(0);
            *entry = entry.saturating_add(*df);
        }
    }

    /// Encode as a compact JSON string for transport as a UDTF argument.
    ///
    /// # Errors
    ///
    /// Returns an error when the statistics cannot be serialized to JSON.
    pub fn encode(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Decode from the JSON string produced by [`GlobalBm25Stats::encode`].
    ///
    /// # Errors
    ///
    /// Returns an error when the string is not the JSON encoding of a
    /// [`GlobalBm25Stats`].
    pub fn decode(encoded: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(encoded)
    }

    /// The Arrow schema of the `text_search_stats` UDTF output: one row per
    /// query term carrying the term's local document frequency and the
    /// partition's document and token counts.
    #[must_use]
    pub fn stats_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ArrowField::new(STATS_TERM_COLUMN, DataType::Utf8, false),
            ArrowField::new(STATS_DOC_FREQ_COLUMN, DataType::UInt64, false),
            ArrowField::new(STATS_TOTAL_NUM_DOCS_COLUMN, DataType::UInt64, false),
            ArrowField::new(STATS_TOTAL_NUM_TOKENS_COLUMN, DataType::UInt64, false),
        ]))
    }

    /// Encode these local statistics as one row per term for the
    /// `text_search_stats` UDTF. The document and token counts repeat on every
    /// row so a downstream `SUM ... GROUP BY term` sums each of `N`, the token
    /// count, and per-term `df` across partitions into the global statistics.
    ///
    /// # Errors
    ///
    /// Returns an error when the Arrow arrays cannot be assembled.
    pub fn to_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        let terms: Vec<&str> = self.doc_freq.keys().map(String::as_str).collect();
        let dfs: Vec<u64> = self.doc_freq.values().copied().collect();
        let n = terms.len();

        let term_col: ArrayRef = Arc::new(StringArray::from(terms));
        let df_col: ArrayRef = Arc::new(UInt64Array::from(dfs));
        let docs_col: ArrayRef = Arc::new(UInt64Array::from(vec![self.total_num_docs; n]));
        let tokens_col: ArrayRef = Arc::new(UInt64Array::from(vec![self.total_num_tokens; n]));

        RecordBatch::try_new(
            Self::stats_schema(),
            vec![term_col, df_col, docs_col, tokens_col],
        )
    }

    /// Reconstruct global statistics from the aggregated `text_search_stats`
    /// output (after `SUM ... GROUP BY term`). `total_num_docs` and
    /// `total_num_tokens` are the same on every row, so the maximum is taken to
    /// tolerate an empty batch collapsing to zero rows.
    ///
    /// # Errors
    ///
    /// Returns an error when a column is missing or has an unexpected type.
    pub fn from_aggregated_batches(batches: &[RecordBatch]) -> Result<Self, ArrowError> {
        let mut stats = GlobalBm25Stats::default();
        for batch in batches {
            let terms = downcast_string(batch, STATS_TERM_COLUMN)?;
            let dfs = downcast_u64(batch, STATS_DOC_FREQ_COLUMN)?;
            let docs = downcast_u64(batch, STATS_TOTAL_NUM_DOCS_COLUMN)?;
            let tokens = downcast_u64(batch, STATS_TOTAL_NUM_TOKENS_COLUMN)?;
            for row in 0..batch.num_rows() {
                stats
                    .doc_freq
                    .insert(terms.value(row).to_string(), dfs.value(row));
                // Every row carries the same collection totals; take the max so
                // reordering or a partial batch cannot lower them.
                stats.total_num_docs = stats.total_num_docs.max(docs.value(row));
                stats.total_num_tokens = stats.total_num_tokens.max(tokens.value(row));
            }
        }
        Ok(stats)
    }
}

fn downcast_string<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> Result<&'a StringArray, ArrowError> {
    batch
        .column_by_name(column)
        .ok_or_else(|| ArrowError::SchemaError(format!("text_search_stats missing '{column}'")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::CastError(format!("text_search_stats '{column}' not Utf8")))
}

fn downcast_u64<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a UInt64Array, ArrowError> {
    batch
        .column_by_name(column)
        .ok_or_else(|| ArrowError::SchemaError(format!("text_search_stats missing '{column}'")))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ArrowError::CastError(format!("text_search_stats '{column}' not UInt64")))
}

/// Presents [`GlobalBm25Stats`] to Tantivy as the collection statistics for one
/// search field, delegating any other field or non-text term to the underlying
/// searcher.
///
/// Pass this to [`tantivy::Searcher::search_with_statistics_provider`] so BM25
/// scores with the global statistics while the search still runs against the
/// local segments.
pub struct GlobalBm25Provider<'a> {
    stats: &'a GlobalBm25Stats,
    /// The search field the global statistics describe.
    field: Field,
    /// Fallback for any field or term the global statistics do not cover.
    searcher: &'a Searcher,
}

impl<'a> GlobalBm25Provider<'a> {
    #[must_use]
    pub fn new(stats: &'a GlobalBm25Stats, field: Field, searcher: &'a Searcher) -> Self {
        Self {
            stats,
            field,
            searcher,
        }
    }
}

impl Bm25StatisticsProvider for GlobalBm25Provider<'_> {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        if field == self.field {
            Ok(self.stats.total_num_tokens)
        } else {
            // A query term on another field is not covered by the gathered
            // statistics; fall back to the local searcher rather than score it
            // with the wrong field's token count.
            self.searcher.total_num_tokens(field)
        }
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.stats.total_num_docs)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        if term.field() == self.field
            && let Some(text) = term.value().as_str()
        {
            return Ok(self.stats.doc_freq.get(text).copied().unwrap_or(0));
        }
        self.searcher.doc_freq(term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_sums_disjoint_partitions() {
        let mut a = GlobalBm25Stats {
            total_num_docs: 10,
            total_num_tokens: 100,
            doc_freq: BTreeMap::from([("run".to_string(), 3), ("jump".to_string(), 1)]),
        };
        let b = GlobalBm25Stats {
            total_num_docs: 5,
            total_num_tokens: 40,
            doc_freq: BTreeMap::from([("run".to_string(), 2), ("swim".to_string(), 4)]),
        };
        a.add(&b);
        assert_eq!(a.total_num_docs, 15);
        assert_eq!(a.total_num_tokens, 140);
        assert_eq!(a.doc_freq.get("run"), Some(&5));
        assert_eq!(a.doc_freq.get("jump"), Some(&1));
        assert_eq!(a.doc_freq.get("swim"), Some(&4));
    }

    #[test]
    fn encode_decode_round_trips() {
        let stats = GlobalBm25Stats {
            total_num_docs: 42,
            total_num_tokens: 512,
            doc_freq: BTreeMap::from([("whale".to_string(), 7)]),
        };
        let encoded = stats.encode().expect("encode");
        let decoded = GlobalBm25Stats::decode(&encoded).expect("decode");
        assert_eq!(stats, decoded);
    }
}
