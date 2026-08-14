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

use std::cmp::min;
use std::path::{Path, PathBuf};
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{any::Any, collections::HashSet, sync::Arc};

use arrow::{array::RecordBatch, datatypes::DataType};
use arrow_schema::Field;
use async_trait::async_trait;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use snafu::{ResultExt, ensure};
use spice_table::{Index, WriteWindow};
use tantivy::merge_policy::LogMergePolicy;
use tantivy::schema::{
    DocParsingError, FieldEntry, FieldType, IndexRecordOption, Schema, SchemaBuilder,
    TextFieldIndexing, TextOptions, Type,
};
use tantivy::{TantivyDocument, TantivyError};
use tokio::sync::Mutex;

use crate::aggregation::write_to_json_string;
use crate::generation::text_search::query::FullTextSearchQuery;
use crate::generation::text_search::util::{array_to_terms, with_json_subset_column};
use crate::generation::text_search::{
    FailedToInsertDataIntoIndexSnafu, FullTextSearchFieldIndex, IndexCreationSnafu,
    InvalidIndexingSnafu, PersistedIndexColumnChangedSnafu, PersistedIndexMissingColumnsSnafu,
    TextSearchIndexingSnafu,
};
use crate::generation::util::get_primary_keys;
use crate::index::SearchIndex;

/// The heap budget for the [`tantivy::IndexWriter`] (150 MiB).
/// A larger budget reduces the number of segment flushes and subsequent merges,
/// significantly improving bulk-indexing throughput.
pub static MEMORY_BUDGET_FOR_INDEX_WRITER: usize = 150 * 1024 * 1024;
pub static INDEX_UNIQUE_FIELD_NAME: &str = "__spice.unique_field";

/// Tantivy's built-in English Snowball-stemmed tokenizer.
static EN_STEM_TOKENIZER_NAME: &str = "en_stem";

/// A [`TextOptions`] for [`tantivy::schema::TEXT`] with [`EN_STEM_TOKENIZER_NAME`] tokenization.
fn tokenized_text_options() -> TextOptions {
    TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
            .set_tokenizer(EN_STEM_TOKENIZER_NAME),
    )
}

/// Checks the schema a persisted index was created with against the one the current
/// configuration asks for.
///
/// [`tantivy::Index::open_in_dir`] loads the schema recorded in the index directory and the
/// schema built from the current configuration is discarded, so a configuration change would
/// otherwise take effect nowhere and say nothing: [`TantivyDocument::from_json_object`] drops
/// every column the persisted schema does not declare, and the query parser and the
/// primary-key delete terms are both built from the persisted schema.
///
/// A difference that breaks addressing or filtering — a column the index does not have, a
/// changed value type, an indexed/not-indexed flip, or a tokenized/untokenized flip — is an
/// error naming the directory to delete, because searches and deletes over that column cannot
/// work at all. A difference that only changes text analysis (a different tokenizer, e.g. an
/// index predating a change to the tokenizer the runtime configures) still answers queries
/// consistently with what was indexed, so it warns and continues rather than refusing to load
/// a working index.
///
/// A column the persisted schema has and the configuration no longer asks for is left alone:
/// nothing queries it.
fn ensure_persisted_schema_matches(
    path: &Path,
    persisted: &Schema,
    configured: &Schema,
) -> Result<(), super::Error> {
    let path = path.display().to_string();

    let mut missing = Vec::new();
    let mut shared = Vec::new();
    for (_, configured_entry) in configured.fields() {
        match persisted.get_field(configured_entry.name()) {
            Ok(field) => shared.push((persisted.get_field_entry(field), configured_entry)),
            Err(_) => missing.push(configured_entry.name().to_string()),
        }
    }
    ensure!(
        missing.is_empty(),
        PersistedIndexMissingColumnsSnafu {
            path: &path,
            columns: missing,
        }
    );

    for (persisted_entry, configured_entry) in shared {
        ensure!(
            addressing_shape(persisted_entry) == addressing_shape(configured_entry),
            PersistedIndexColumnChangedSnafu {
                path: &path,
                column: configured_entry.name(),
                persisted: describe_indexing(persisted_entry),
                configured: describe_indexing(configured_entry),
            }
        );

        let persisted_tokenizer = text_tokenizer(persisted_entry.field_type());
        let configured_tokenizer = text_tokenizer(configured_entry.field_type());
        if persisted_tokenizer != configured_tokenizer {
            tracing::warn!(
                "The full text search index at '{path}' indexes column '{}' with the '{}' tokenizer, but '{}' is now configured. Queries stay consistent with what the index holds; delete '{path}' so the index is rebuilt with the configured tokenizer.",
                configured_entry.name(),
                persisted_tokenizer.unwrap_or("none"),
                configured_tokenizer.unwrap_or("none"),
            );
        }
    }

    Ok(())
}

/// The properties a persisted field must share with the configured one for term addressing and
/// filtering to behave as configured. Text analysis (the tokenizer) is deliberately excluded —
/// see [`ensure_persisted_schema_matches`].
fn addressing_shape(entry: &FieldEntry) -> (Type, bool, bool) {
    let field_type = entry.field_type();
    (
        field_type.value_type(),
        field_type.is_indexed(),
        is_tokenized(field_type),
    )
}

/// The tokenizer a text field is analyzed with, or [`None`] for any other field type.
fn text_tokenizer(field_type: &FieldType) -> Option<&str> {
    match field_type {
        FieldType::Str(options) => options
            .get_indexing_options()
            .map(TextFieldIndexing::tokenizer),
        _ => None,
    }
}

/// Whether a text field is analyzed into multiple terms, rather than indexed as the single term
/// that [`tantivy::schema::STRING`] (and so a primary-key lookup) relies on.
fn is_tokenized(field_type: &FieldType) -> bool {
    // Compare against tantivy's own untokenized text options rather than naming its tokenizer,
    // which tantivy does not export.
    let untokenized = FieldType::Str(tantivy::schema::STRING);
    match (text_tokenizer(field_type), text_tokenizer(&untokenized)) {
        (Some(tokenizer), Some(untokenized)) => tokenizer != untokenized,
        _ => false,
    }
}

/// Describes how a field is indexed, for the error naming a column whose indexing changed.
fn describe_indexing(entry: &FieldEntry) -> String {
    let field_type = entry.field_type();
    let indexing = if !field_type.is_indexed() {
        "not indexed"
    } else if text_tokenizer(field_type).is_none() {
        "indexed"
    } else if is_tokenized(field_type) {
        "tokenized"
    } else {
        "untokenized"
    };
    format!("{:?} ({indexing})", field_type.value_type()).to_lowercase()
}

/// The fraction of a tantivy segment's documents that may be superseded/deleted, but
/// still physically present, before the segment is rewritten by a merge.
///
/// Tantivy BM25 collection size statistics includes these superseded documents. A merge
/// is the only mechanism to expunge documents. The default, [`LogMergePolicy`], does
/// not merge on deletions, only when the ratio of deleted to collected documents is above a threshold.
///
/// [`MAX_SUPERSEDED_DOCS_RATIO_PER_SEGMENT`] is the ratio to use in [`LogMergePolicy`]/
const MAX_SUPERSEDED_DOCS_RATIO_PER_SEGMENT: f32 = 0.25;

/// The merge policy for the index writer, which differs from tantivy's default only in
/// capping superseded documents per segment — see
/// [`MAX_SUPERSEDED_DOCS_RATIO_PER_SEGMENT`].
fn index_merge_policy() -> LogMergePolicy {
    let mut policy = LogMergePolicy::default();
    policy.set_del_docs_ratio_before_merge(MAX_SUPERSEDED_DOCS_RATIO_PER_SEGMENT);
    policy
}

/// Perform a [`tantivy::IndexWriter::rollback`] and preserve the [`MergePolicy`] from `index_merge_policy`.
///
/// [`tantivy::IndexWriter::rollback`] overwrites any custom [`MergePolicy`] with the default.
fn rollback_writer(writer: &mut tantivy::IndexWriter) -> Result<(), TantivyError> {
    writer.rollback()?;
    writer.set_merge_policy(Box::new(index_merge_policy()));
    Ok(())
}

#[derive(Clone)]
pub struct FullTextDatabaseIndex {
    pub search_fields: Vec<String>,
    pub primary_key: Vec<String>,
    pub base_table: Arc<dyn TableProvider>,

    pub writer: Arc<Mutex<tantivy::IndexWriter>>,
    pub reader: tantivy::IndexReader,

    /// When `true`, `update_index` stages documents into the tantivy writer
    /// without committing, so a sink-driven full refresh or append commits
    /// **once** for the whole write window, in `on_write_complete`.
    /// `on_write_start` sets it; `on_write_complete`/`on_write_failed` clear it.
    defer_commit: Arc<AtomicBool>,

    /// Set when this index is also fed by a change-data-capture stream, which
    /// drives `compute_index` outside the sink write lifecycle.
    ///
    /// A single [`tantivy::IndexWriter`] stages every pending operation together,
    /// so a commit cannot be scoped to one caller's documents: committing inside a
    /// deferred window would publish a partially-written refresh, and rolling the
    /// window back would discard CDC documents staged alongside it. Deferral is
    /// therefore disabled outright for a CDC-fed index — correctness over the
    /// one-commit-per-refresh optimization.
    cdc_attached: Arc<AtomicBool>,
}

impl std::fmt::Debug for FullTextDatabaseIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextDatabaseIndex")
            .field("base_table", &self.base_table)
            .field("search_fields", &self.search_fields)
            .field("primary_key", &self.primary_key)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Index for FullTextDatabaseIndex {
    fn name(&self) -> &'static str {
        "full_text"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        // Return both the primary key and search fields, deduplicated.
        let mut required_columns = HashSet::new();
        required_columns.extend(self.primary_key.iter().cloned());
        required_columns.extend(self.search_fields.iter().cloned());
        required_columns.into_iter().collect()
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if let Err(e) = self.update_index(batches.as_slice()).await {
            tracing::error!("Failed to update full text search index: {e}");
            return Err(DataFusionError::External(Box::new(e)));
        }
        Ok(batches)
    }

    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<(), DataFusionError> {
        self.delete_terms_for(&keys)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn write_start_failure_is_fatal(&self) -> bool {
        // `on_write_start` rolls the writer back to discard operations an earlier abandoned
        // window left staged. If that rollback fails those stale operations are still in the
        // writer, and continuing would sweep them into this window's commit — publishing
        // index state the write never asked for. The window is also left undeferred, so the
        // deferral this write is written against is not the one in effect.
        true
    }

    fn write_complete_failure_is_fatal(&self) -> bool {
        // Documents are only searchable once the tantivy writer commits them, and
        // staged-but-uncommitted documents are discarded. A finalize that fails to
        // commit therefore drops the write's documents while the underlying rows are
        // already visible, so the write must not report success.
        true
    }

    async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
        // A CDC-fed index never defers: its change stream calls `compute_index`
        // outside this lifecycle, and the shared writer cannot commit one caller's
        // documents without also publishing (or, on rollback, discarding) the other's.
        //
        // That also rules out the `ReplaceAll` clear below, whose atomicity depends on the
        // deferred window: an immediately-committed clear would publish an empty index for the
        // length of the refresh, and would discard change-stream documents staged alongside it.
        // A CDC-fed index is told about deletions explicitly by its change stream, so it does
        // not depend on the replace-window clear to drop rows the source removed.
        if self.cdc_attached.load(Ordering::Acquire) {
            return Ok(());
        }

        // Begin a deferred-commit window: subsequent `compute_index` calls stage
        // documents into the writer without committing until `on_write_complete`.
        //
        // Discard anything staged but never committed first. A previous window that
        // was cancelled or aborted before `on_write_complete`/`on_write_failed` ran
        // can leave uncommitted operations in the writer, and they would otherwise be
        // swept into this window's commit. Taking the writer lock before setting the
        // flag also keeps the reset and the flag store atomic w.r.t. `compute_index`.
        // A rollback failure is fatal for the window: proceeding would let stale
        // operations be swept into this window's commit and publish an incorrect
        // index state, so surface the error and leave `defer_commit` unset (the
        // writer keeps its per-write commit behavior rather than deferring on a
        // writer in an unknown state).
        // The writer operations below (rollback, delete_all_documents) are
        // synchronous tantivy work, so run them off the async runtime thread. The
        // writer lock is taken and the flag stored inside the blocking task, so the
        // reset + clear + flag store stay atomic w.r.t. `compute_index` exactly as
        // before.
        let writer = Arc::clone(&self.writer);
        let defer_commit = Arc::clone(&self.defer_commit);
        let is_replace_all = window == WriteWindow::ReplaceAll;
        tokio::task::spawn_blocking(move || -> Result<(), super::Error> {
            let mut index_writer = writer.blocking_lock();
            rollback_writer(&mut index_writer).context(TextSearchIndexingSnafu)?;

            // A replacing write reproduces the table's whole contents, so every document this
            // index already holds is either re-sent by this window or belongs to a row the source
            // dropped. Stage the clear *inside* the window that is about to open:
            // `delete_all_documents` needs a commit to take effect, so the wipe and the
            // repopulation land in the single `on_write_complete` commit and a searcher never
            // observes an empty index (#12066).
            //
            // Ordering matters. The rollback above discards operations staged by an abandoned
            // window, and it must happen before the clear so it cannot revert it.
            if is_replace_all {
                index_writer
                    .delete_all_documents()
                    .context(TextSearchIndexingSnafu)?;
            }

            defer_commit.store(true, Ordering::Release);
            Ok(())
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        // End the deferred-commit window with a single commit + reader reload for
        // the whole refresh/append. Take the writer lock *before* clearing the flag so
        // no concurrent `compute_index` (e.g. the CDC path) can observe a cleared flag
        // and commit the staged window before it is finalized here; clearing it under
        // the lock also ensures a later CDC write is never stuck deferring. Committing
        // when nothing was staged (e.g. an empty refresh) is a harmless no-op.
        // `commit()` fsyncs the file-backed index and the reader reload remaps
        // segments — both synchronous. Run them off the async runtime thread. The
        // flag is cleared under the writer lock inside the task, preserving the
        // ordering guarantee w.r.t. a concurrent `compute_index`.
        let writer = Arc::clone(&self.writer);
        let defer_commit = Arc::clone(&self.defer_commit);
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || -> Result<(), super::Error> {
            let mut index_writer = writer.blocking_lock();
            defer_commit.store(false, Ordering::Release);

            let commit_result = index_writer
                .commit()
                .map(|_| ())
                .context(FailedToInsertDataIntoIndexSnafu);
            if let Err(e) = &commit_result {
                tracing::warn!("Rolling back full-text index writer after failed commit: {e}");
                if let Err(rb_err) = rollback_writer(&mut index_writer) {
                    tracing::error!("Failed to rollback full-text index writer: {rb_err}");
                }
            }
            drop(index_writer);
            commit_result?;

            reader
                .reload()
                .boxed()
                .context(InvalidIndexingSnafu {
                    context: "Full-text index committed, but failed to reload the reader to the latest revision. Queries will be served from the previous revision until the next update.".to_string(),
                })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        // Discard everything staged in the current window so a partial refresh is
        // never committed, and reset the flag. Take the writer lock *before* clearing
        // the flag: otherwise a concurrent `compute_index` could observe the cleared
        // flag, acquire the lock first, and commit the staged partial refresh — making
        // a failed write visible to queries.
        // Rollback is synchronous tantivy work; run it off the async runtime
        // thread. The flag is cleared under the writer lock inside the task,
        // preserving the ordering guarantee w.r.t. a concurrent `compute_index`.
        let writer = Arc::clone(&self.writer);
        let defer_commit = Arc::clone(&self.defer_commit);
        tokio::task::spawn_blocking(move || -> Result<(), super::Error> {
            let mut index_writer = writer.blocking_lock();
            defer_commit.store(false, Ordering::Release);

            // A rollback failure must reach the caller: staged operations that could not
            // be discarded may leak into a later commit and make a partial refresh
            // visible.
            rollback_writer(&mut index_writer).context(TextSearchIndexingSnafu)
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

impl FullTextDatabaseIndex {
    pub fn try_new(
        inner: Arc<dyn TableProvider>,
        search_fields: Vec<String>,
        primary_key_override: Option<Vec<String>>,
        directory: Option<PathBuf>,
        store_field: &[String],
    ) -> Result<Self, super::Error> {
        let pks = Self::validate_primary_key(&inner, primary_key_override)?;
        let tantivy_schema = Self::create_tantivy_schema(
            &inner,
            search_fields.as_slice(),
            pks.as_slice(),
            store_field,
        )?;

        let index = if let Some(path) = directory {
            match tantivy::Index::create_in_dir(&path, tantivy_schema.clone()) {
                Ok(idx) => idx,
                Err(TantivyError::IndexAlreadyExists) => {
                    let persisted = tantivy::index::Index::open_in_dir(&path)
                        .context(TextSearchIndexingSnafu)?;
                    ensure_persisted_schema_matches(&path, &persisted.schema(), &tantivy_schema)?;
                    persisted
                }
                Err(e) => return Err(e).context(TextSearchIndexingSnafu),
            }
        } else {
            tantivy::Index::create_in_ram(tantivy_schema)
        };
        let reader = index.reader().context(TextSearchIndexingSnafu)?;
        let writer = index
            .writer(MEMORY_BUDGET_FOR_INDEX_WRITER)
            .context(IndexCreationSnafu)?;
        writer.set_merge_policy(Box::new(index_merge_policy()));

        Ok(Self {
            base_table: inner,
            search_fields,
            writer: Arc::new(Mutex::new(writer)),
            primary_key: pks,
            reader,
            defer_commit: Arc::new(AtomicBool::new(false)),
            cdc_attached: Arc::new(AtomicBool::new(false)),
        })
    }

    fn validate_primary_key(
        inner: &Arc<dyn TableProvider>,
        primary_key_override: Option<Vec<String>>,
    ) -> Result<Vec<String>, super::Error> {
        // Use 'primary_key_override', fallback to underlying in table.
        let pks = match (primary_key_override, get_primary_keys(inner)) {
            // LHS takes precedence.
            (Some(pks), _) | (_, Ok(pks)) if !pks.is_empty() => pks,
            (_, Err(e)) => {
                return Err(super::Error::FailedToRetrievePrimaryKey { source: e });
            }
            _ => return Err(super::Error::NoPrimaryKey),
        };

        // INDEX_UNIQUE_FIELD_NAME is a reserved field name.
        if pks.contains(&INDEX_UNIQUE_FIELD_NAME.to_string()) {
            return Err(super::Error::PrimaryKeyInvalidName {
                column: INDEX_UNIQUE_FIELD_NAME.to_string(),
            });
        }
        Ok(pks)
    }

    pub fn full_text_search_field_index(
        &self,
        search_field: &str,
    ) -> Result<FullTextSearchFieldIndex, super::Error> {
        let mut search_index = FullTextSearchFieldIndex::try_new(
            self.reader.searcher(),
            search_field.to_string(),
            self.primary_key.clone(),
        )?;
        search_index.add_type_hints(&self.underlying_table().schema());
        Ok(search_index)
    }

    /// Given a [`RecordBatch`] of new data, find all [`Term`]s we need to delete. These terms are
    /// an exact match on either a primary key (if one primary key column), or `INDEX_UNIQUE_FIELD_NAME`.
    fn existing_terms_to_delete(
        &self,
        index_schema: &tantivy::schema::Schema,
        rb: &[RecordBatch],
    ) -> Result<Vec<tantivy::Term>, super::Error> {
        let Some(pk) = self.primary_key.first() else {
            // Should not occur, but no primary key implies none must be deleted.
            return Ok(vec![]);
        };

        let (pk_field, pk) = if self.primary_key.len() == 1 {
            let Some((pk_field, _)) = index_schema.find_field(pk.as_str()) else {
                return Err(super::Error::FailedToRetrieveDataFromIndex {
                    source: TantivyError::FieldNotFound(pk.clone()),
                });
            };
            (pk_field, pk.clone())
        } else {
            // Primary key has multiple columns. Therefore tantivy::Index has derived field `INDEX_UNIQUE_FIELD_NAME`.
            let Some((pk_field, _)) = index_schema.find_field(INDEX_UNIQUE_FIELD_NAME) else {
                return Err(super::Error::InvalidIndexingError {
                    source: Box::from(TantivyError::FieldNotFound(pk.clone())),
                    context: format!(
                        "Full text search has multiple primary key columns, so the column '{INDEX_UNIQUE_FIELD_NAME}' should be present, but is not.",
                    ),
                });
            };
            (pk_field, INDEX_UNIQUE_FIELD_NAME.to_string())
        };

        Ok(rb
            .iter()
            .filter_map(|r| r.column_by_name(pk.as_str()))
            .map(|arr| array_to_terms(pk_field, arr))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| super::Error::FailedToRetrieveDataFromSource {
                source: DataFusionError::ArrowError(Box::new(e), None),
            })?
            .into_iter()
            .flatten()
            .collect())
    }

    /// Update the underlying [`tantivy::Index`] with new data from [`RecordBatch`]s. Additional
    /// columns present will be ignored.
    ///
    /// If there is a multi-column primary key (as specified by [`Self::primary_key`]), an additional column is used in the [`tantivy::Index`] for unique lookup (required since updates = deletion -> insertion).
    async fn update_index(&self, rb: &[RecordBatch]) -> Result<(), super::Error> {
        // Construct column for `INDEX_UNIQUE_FIELD_NAME` if needed.
        let rb = if self.primary_key.len() > 1 {
            rb.iter()
                .map(|r| with_json_subset_column(r, &self.primary_key, INDEX_UNIQUE_FIELD_NAME))
                .collect::<Result<Vec<RecordBatch>, _>>()
                .context(InvalidIndexingSnafu {
                    context: "An error occured creating the a unique column for the full text search index".to_string(),
                })?
        } else {
            rb.to_vec()
        };

        // Updates in tantivy are a deletion then insertion.
        // Prepare documents to insert/delete with read lock.
        let index_schema = self.reader.searcher().schema().clone();
        let terms_to_delete = self.existing_terms_to_delete(&index_schema, &rb)?;
        let doc_json = write_to_json_string(&rb).context(InvalidIndexingSnafu {
            context: "Failed to write data to intermediate JSON string for indexing".to_string(),
        })?;
        let docs = parse_json_array(&index_schema, doc_json.as_str())
            .context(FailedToInsertDataIntoIndexSnafu)?;

        // The writer operations (delete_term, add_document, commit) and the reader
        // reload are synchronous tantivy work — commit fsyncs the file-backed index —
        // so run them off the async runtime thread. The deferral flag is read while
        // holding the writer lock inside the task so the decision to commit stays
        // serialized with `on_write_complete`/`on_write_failed` closing the window.
        let writer = Arc::clone(&self.writer);
        let defer_commit_flag = Arc::clone(&self.defer_commit);
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || -> Result<(), super::Error> {
            let mut index_writer = writer.blocking_lock();
            let defer_commit = defer_commit_flag.load(Ordering::Acquire);
            // Deletion.
            for t in terms_to_delete {
                index_writer.delete_term(t);
            }
            // Insertion. In a sink-driven full refresh or append, `on_write_start` has
            // set `defer_commit`, so documents are staged and the single commit happens
            // once in `on_write_complete` — one fsync barrier per refresh instead of one
            // per record batch. Otherwise (the CDC path, which drives `compute_index`
            // directly without the lifecycle hooks) commit immediately. On failure,
            // rollback to discard staged operations so they don't leak into a later commit.
            let write_result = (|| {
                for doc in docs {
                    index_writer.add_document(doc).context(IndexCreationSnafu)?;
                }
                if defer_commit {
                    Ok(())
                } else {
                    index_writer
                        .commit()
                        .map(|_| ())
                        .context(FailedToInsertDataIntoIndexSnafu)
                }
            })();
            if let Err(e) = &write_result {
                tracing::warn!("Rolling back index writer after failed write: {e}");
                if let Err(rb_err) = rollback_writer(&mut index_writer) {
                    tracing::error!("Failed to rollback index writer: {rb_err}");
                }
            }
            drop(index_writer);
            write_result?;

            if defer_commit {
                // The reader is reloaded once in `on_write_complete`, after the commit.
                return Ok(());
            }

            reader.reload().boxed().context(InvalidIndexingSnafu {
                context: "Data successfully written to full-text index, but failed to update search path to reference the latest commit. Queries will be served from previous revision until the next update.".to_string(),
            })
        })
        .await
        .map_err(|e| super::Error::InvalidIndexingError {
            source: Box::new(e),
            context: "The full-text index write task failed to complete".to_string(),
        })?
    }

    /// Deletes every document whose primary key matches a row of `keys` — the tantivy
    /// counterpart of `update_index`'s delete-then-insert, minus the insert.
    async fn delete_terms_for(&self, keys: &RecordBatch) -> Result<(), super::Error> {
        let rb = if self.primary_key.len() > 1 {
            vec![with_json_subset_column(
                keys,
                &self.primary_key,
                INDEX_UNIQUE_FIELD_NAME,
            )
            .context(InvalidIndexingSnafu {
                context: "An error occurred creating the unique column for the full text search index".to_string(),
            })?]
        } else {
            vec![keys.clone()]
        };

        let index_schema = self.reader.searcher().schema().clone();
        let terms_to_delete = self.existing_terms_to_delete(&index_schema, &rb)?;
        if terms_to_delete.is_empty() {
            return Ok(());
        }

        // delete_term/commit and the reader reload are synchronous tantivy work
        // (commit fsyncs), so run them off the async runtime thread. The deferral
        // flag is read under the writer lock inside the task, exactly as
        // `update_index` does.
        let writer = Arc::clone(&self.writer);
        let defer_commit_flag = Arc::clone(&self.defer_commit);
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || -> Result<(), super::Error> {
            let mut index_writer = writer.blocking_lock();
            // A sink write window shares this one writer, so committing here would publish
            // whatever that window has staged: a partially rewritten table, or the whole-index
            // clear that `on_write_start` stages for a `WriteWindow::ReplaceAll`. Stage the
            // deletes and let the window's own `on_write_complete` commit publish them together.
            let defer_commit = defer_commit_flag.load(Ordering::Acquire);
            for t in terms_to_delete {
                index_writer.delete_term(t);
            }
            if defer_commit {
                // The reader is reloaded once in `on_write_complete`, after the commit.
                return Ok(());
            }

            let commit_result = index_writer
                .commit()
                .context(FailedToInsertDataIntoIndexSnafu);
            if let Err(e) = &commit_result {
                tracing::warn!("Rolling back index writer after failed delete commit: {e}");
                if let Err(rb_err) = index_writer.rollback() {
                    tracing::error!("Failed to rollback index writer: {rb_err}");
                }
            }
            drop(index_writer);
            commit_result?;

            reader.reload().boxed().context(InvalidIndexingSnafu {
                context: "Deleted from full-text index, but failed to update search path to reference the latest commit. Queries may still return deleted rows until the next update.".to_string(),
            })
        })
        .await
        .map_err(|e| super::Error::InvalidIndexingError {
            source: Box::new(e),
            context: "The full-text index delete task failed to complete".to_string(),
        })?
    }

    #[must_use]
    pub fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    /// Record that a change-data-capture stream also writes to this index, which
    /// permanently disables the deferred-commit window (see `cdc_attached`).
    ///
    /// Called when the change stream that includes this index is constructed.
    pub fn mark_cdc_attached(&self) {
        self.cdc_attached.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn underlying_table(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.base_table)
    }

    /// Construct a new [`FullTextDatabaseIndex`] with an updated [`TableProvider`].
    ///
    /// No Checks are done to confirm compatibility between the current index and the provided [`TableProvider`].
    #[must_use]
    pub fn with_new_base(&self, base_table: Arc<dyn TableProvider>) -> Self {
        Self {
            search_fields: self.search_fields.clone(),
            primary_key: self.primary_key.clone(),
            writer: Arc::clone(&self.writer),
            base_table,
            reader: self.reader.clone(),
            // Share the deferred-commit flag: this handle wraps the *same*
            // tantivy writer (Arc::clone above), so both handles must observe a
            // single deferral state or a sink write window could desync.
            defer_commit: Arc::clone(&self.defer_commit),
            // Shared for the same reason as `defer_commit`: both handles drive the
            // same tantivy writer and must agree on whether deferral is allowed.
            cdc_attached: Arc::clone(&self.cdc_attached),
        }
    }

    // Adds the Arrow [`Field`] as a stored and indexed field.
    //
    // Note: for Utf8, does not tokenize.
    fn add_to_tantivy_schema(
        schema_builder: &mut SchemaBuilder,
        field: &Field,
    ) -> Result<(), super::Error> {
        match field.data_type() {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                schema_builder.add_f64_field(
                    field.name().as_str(),
                    tantivy::schema::STORED | tantivy::schema::INDEXED,
                );
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                schema_builder.add_u64_field(
                    field.name().as_str(),
                    tantivy::schema::STORED | tantivy::schema::INDEXED,
                );
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                schema_builder.add_i64_field(
                    field.name().as_str(),
                    tantivy::schema::STORED | tantivy::schema::INDEXED,
                );
            }
            DataType::Boolean => {
                schema_builder.add_bool_field(
                    field.name().as_str(),
                    tantivy::schema::STORED | tantivy::schema::INDEXED,
                );
            }

            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                // [`tantivy::schema::STRING`] means we won't tokenize, important for primary key lookup via [`TermQuery`].
                schema_builder.add_text_field(
                    field.name().as_str(),
                    tantivy::schema::STORED | tantivy::schema::STRING,
                );
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                schema_builder.add_bytes_field(
                    field.name().as_str(),
                    tantivy::schema::STORED | tantivy::schema::INDEXED,
                );
            }
            dt => {
                return Err(super::Error::PrimaryKeyInvalidType {
                    data_type: dt.clone(),
                    column: field.name().clone(),
                });
            }
        }
        Ok(())
    }

    fn create_tantivy_schema(
        base_table: &Arc<dyn TableProvider>,
        search_fields: &[String],
        primary_key: &[String],
        store_field: &[String],
    ) -> Result<tantivy::schema::Schema, super::Error> {
        let schema = base_table.schema();
        let mut schema_builder = tantivy::schema::Schema::builder();
        for p in primary_key {
            if search_fields.contains(p) {
                // Added below, tokenized.
                continue;
            }
            let Some((_, field)) = schema.column_with_name(p) else {
                return Err(super::Error::PrimaryKeyNotFound { column: p.clone() });
            };
            Self::add_to_tantivy_schema(&mut schema_builder, field)?;
        }

        // If we need `INDEX_UNIQUE_FIELD_NAME`, add to schema.
        if primary_key.len() > 1 {
            schema_builder.add_text_field(INDEX_UNIQUE_FIELD_NAME, tantivy::schema::STRING);
        }

        for s in search_fields {
            let mut text_opts = tokenized_text_options();
            if store_field.contains(s) || primary_key.contains(s) {
                text_opts = text_opts | tantivy::schema::STORED;
            }
            schema_builder.add_text_field(s, text_opts);
        }

        for f in store_field {
            if !primary_key.contains(f)
                && !search_fields.contains(f)
                && let Some((_, field)) = schema.column_with_name(f)
            {
                Self::add_to_tantivy_schema(&mut schema_builder, field)?;
            }
        }

        Ok(schema_builder.build())
    }

    #[must_use]
    pub fn column_is_part_of_pk(&self, column: &str) -> bool {
        self.primary_key.contains(&column.to_string())
    }
}

#[async_trait]
impl SearchIndex for FullTextDatabaseIndex {
    /// Currently multi-column uses of [`FullTextDatabaseIndex`] do either:
    ///   1. `TextSearchTableFunc` chooses a column from its UDTF params and overrides `self.search_fields` at query time.
    ///   2. `as_candidate_generations` in `crates/runtime/src/search/full_text/mod.rs` creates [`FullTextSearchFieldIndex`].
    fn search_column(&self) -> String {
        // For FTS, return the first search field as the primary search column
        self.search_fields.first().cloned().unwrap_or_default()
    }

    fn primary_fields(&self) -> Vec<Field> {
        // Convert primary key names to Field objects by looking them up in the base table schema
        let schema = self.base_table.schema();
        self.primary_key
            .iter()
            .filter_map(|pk_name| {
                schema
                    .column_with_name(pk_name)
                    .map(|(_, field)| (*field).clone())
            })
            .collect()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        self.update_index(slice::from_ref(&record)).await.boxed()?;
        Ok(record)
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let field_index = self
            .full_text_search_field_index(&self.search_column())
            .boxed()
            .map_err(DataFusionError::External)?;

        Ok(Arc::new(
            LogicalPlanBuilder::scan(
                self.name(),
                Arc::new(DefaultTableSource::new(Arc::new(FullTextSearchQuery {
                    index: Arc::new(field_index),
                    query: query.to_string(),
                    pre_limit: None,
                }))),
                None,
            )?
            .build()?,
        ))
    }
}

/// An implementation of [`TantivyDocument::parse_json`] that can parse a JSON array of JSON
/// objects that will deserialize to [`TantivyDocument`].
fn parse_json_array(
    schema: &tantivy::schema::Schema,
    doc_json: &str,
) -> Result<Vec<TantivyDocument>, TantivyError> {
    let json_obj: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_str(doc_json)
        .map_err(|_| {
            Into::<TantivyError>::into(DocParsingError::InvalidJson(
                doc_json[0..min(20, doc_json.len())].to_string(),
            ))
        })?;

    Ok(json_obj
        .into_iter()
        .map(|obj| TantivyDocument::from_json_object(schema, obj))
        .collect::<Result<Vec<_>, _>>()?)
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow::{
        array::{Array, StringArray, record_batch},
        util::pretty::pretty_format_batches,
    };
    use arrow_schema::{ArrowError, Schema};
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use futures::{StreamExt, TryStreamExt};
    use spice_table::{Index, WriteWindow};
    use std::time::Duration;

    /// Create a basic [`MemTable`] with fields: `id`, `content`.
    fn create_test_table() -> Arc<dyn TableProvider> {
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            (
                "content",
                Utf8,
                ["test content 1", "test content 2", "test content 3"]
            )
        )
        .expect("Failed to create test batch");

        Arc::new(
            MemTable::try_new(batch.schema(), vec![vec![batch]])
                .expect("Failed to create test table"),
        )
    }

    /// Returns a [`RecordBatch`] where the fields are sorted into alphabetical order.
    ///
    /// An error is returned only if [`RecordBatch::try_new`] returns an error (which it should not).
    fn sort_columns_alphabetically(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let mut fields_with_indices: Vec<(usize, Field)> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| (idx, field.as_ref().clone()))
            .collect();

        fields_with_indices.sort_by(|a, b| a.1.name().cmp(b.1.name()));

        RecordBatch::try_new(
            Arc::new(Schema::new(
                fields_with_indices
                    .iter()
                    .map(|(_, field)| field.clone())
                    .collect::<Vec<_>>(),
            )),
            fields_with_indices
                .iter()
                .map(|(original_idx, _)| Arc::clone(batch.column(*original_idx)))
                .collect::<Vec<_>>(),
        )
    }

    /// The collection size tantivy uses for BM25: the sum of every segment's `max_doc`,
    /// which counts superseded documents until a merge expunges them.
    fn bm25_collection_size(index: &FullTextDatabaseIndex) -> u32 {
        index
            .reader
            .searcher()
            .segment_readers()
            .iter()
            .map(tantivy::SegmentReader::max_doc)
            .sum()
    }

    /// Poll the index's segment set until `reached` holds, reloading the reader each
    /// time. Merges run on their own threads, so a caller that depends on their outcome
    /// has to wait for it rather than assume it has already happened.
    async fn wait_for_segments(
        index: &FullTextDatabaseIndex,
        expectation: &str,
        reached: impl Fn(&[tantivy::SegmentReader]) -> bool,
    ) {
        const ATTEMPTS: usize = 150;
        const INTERVAL: Duration = Duration::from_millis(20);

        let mut observed = "<never sampled>".to_string();
        for _ in 0..ATTEMPTS {
            index
                .reader
                .reload()
                .expect("failed to reload the full-text index reader");
            let searcher = index.reader.searcher();
            if reached(searcher.segment_readers()) {
                return;
            }
            observed = searcher
                .segment_readers()
                .iter()
                .map(|reader| format!("max_doc={} live={}", reader.max_doc(), reader.num_docs()))
                .collect::<Vec<_>>()
                .join(" | ");
            tokio::time::sleep(INTERVAL).await;
        }
        panic!("timed out waiting for {expectation}; last observed segments [{observed}]");
    }

    /// Wait until no segment still holds a superseded document, which is when BM25's
    /// collection size finally matches the live rows.
    async fn wait_for_superseded_docs_expunged(index: &FullTextDatabaseIndex) {
        wait_for_segments(index, "superseded documents to be expunged", |segments| {
            segments
                .iter()
                .all(|reader| reader.max_doc() == reader.num_docs())
        })
        .await;
    }

    /// A full-text index over [`create_test_table`], keyed on `id` and searching `content`.
    fn new_test_index() -> FullTextDatabaseIndex {
        FullTextDatabaseIndex::try_new(
            create_test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("Failed to create FullTextDatabaseIndex")
    }

    const EXPUNGE_FIXTURE_IDS: [i32; 8] = [1, 2, 3, 4, 5, 6, 7, 8];

    /// Drive an index into the one state in which superseded documents can linger — a
    /// single consolidated segment, half of whose documents have been replaced — and
    /// assert they leave BM25's collection size.
    async fn supersede_half_of_a_consolidated_segment(index: &FullTextDatabaseIndex) {
        // Every document is two tokens long, so its field length equals the collection
        // average and BM25's term-frequency factor cancels to exactly 1. A score is then
        // purely the term's inverse document frequency, and so a direct reading of the
        // collection size the index scored against.
        //
        // One commit per row leaves eight single-document segments, which the merge
        // policy's segment-count rule consolidates into one. That is the shape a
        // long-lived index settles into, and the only shape in which a superseded
        // document has nowhere to go but a segment it shares with live documents: alone
        // in its own segment it is dropped outright at commit and never reaches the
        // statistics at all.
        for id in EXPUNGE_FIXTURE_IDS {
            index
                .compute_index(vec![batch(&[id], &[&format!("term{id} shared")])])
                .await
                .expect("failed to compute_index");
        }
        wait_for_segments(
            index,
            "the single-document segments to consolidate",
            |segments| segments.len() == 1,
        )
        .await;
        assert_eq!(
            bm25_collection_size(index),
            8,
            "the eight rows should have consolidated into one eight-document segment"
        );

        // Supersede half of them: the consolidated segment is now half superseded, and is
        // still the only segment at its size level, so nothing but the deleted-document
        // ratio can select it for a merge.
        index
            .compute_index(vec![batch(
                &[1, 2, 3, 4],
                &[
                    "replaced1 shared",
                    "replaced2 shared",
                    "replaced3 shared",
                    "replaced4 shared",
                ],
            )])
            .await
            .expect("failed to compute_index");

        wait_for_superseded_docs_expunged(index).await;
        assert_eq!(
            bm25_collection_size(index),
            8,
            "the four superseded documents must not be counted in the collection size"
        );
    }

    fn batch(ids: &[i32], contents: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("content", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(arrow::array::Int32Array::from(ids.to_vec())),
                Arc::new(arrow::array::StringArray::from(contents.to_vec())),
            ],
        )
        .expect("Failed to create test batch")
    }

    async fn search_and_format(idx: &FullTextSearchFieldIndex, query: impl Into<String>) -> String {
        let rb: Vec<RecordBatch> = idx
            .search(query.into(), &[], 1000)
            .expect("Failed to search")
            .map(|res| match res {
                Ok(rb) => sort_columns_alphabetically(&rb)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None)),
                Err(e) => Err(e),
            })
            .try_collect()
            .await
            .expect("Failed to collect search results");

        format!("{}", pretty_format_batches(&rb).expect("failed to format"))
    }

    /// Search `content` for `query` and return the sorted `id`s of the matching documents.
    /// Reloads the reader first so the searcher reflects the most recent commit (a delete
    /// commits synchronously, so a reload after it is deterministic — no fixed sleep).
    async fn search_ids(index: &FullTextDatabaseIndex, query: &str) -> Vec<i32> {
        index.reader.reload().expect("failed to reload the reader");
        let search_index = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        let batches: Vec<RecordBatch> = search_index
            .search(query.to_string(), &[], 1000)
            .expect("Failed to search")
            .try_collect()
            .await
            .expect("Failed to collect search results");

        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .expect("results carry the id column")
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .expect("id is Int32")
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// A direct `delete_by_keys` removes the matching documents from the tantivy index — the
    /// deleted row stops matching a search, and the other rows still do.
    #[tokio::test]
    async fn delete_by_keys_removes_matching_documents() {
        let index = new_test_index();
        index
            .compute_index(vec![
                record_batch!(
                    ("id", Int32, [1, 2, 3]),
                    (
                        "content",
                        Utf8,
                        ["apple banana", "cherry date", "elderberry fig"]
                    )
                )
                .expect("Failed to create test batch"),
            ])
            .await
            .expect("failed to compute_index");

        assert_eq!(search_ids(&index, "apple").await, vec![1]);
        assert_eq!(search_ids(&index, "cherry").await, vec![2]);
        assert_eq!(search_ids(&index, "elderberry").await, vec![3]);

        index
            .delete_by_keys(record_batch!(("id", Int32, [2])).expect("key batch"))
            .await
            .expect("failed to delete_by_keys");

        assert!(
            search_ids(&index, "cherry").await.is_empty(),
            "the deleted document must no longer match"
        );
        assert_eq!(search_ids(&index, "apple").await, vec![1], "id 1 stays");
        assert_eq!(
            search_ids(&index, "elderberry").await,
            vec![3],
            "id 3 stays"
        );
    }

    // Regression test for #12228.
    #[tokio::test]
    async fn test_search_preserves_all_nullable_stored_columns() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("title", DataType::Utf8, false),
                Field::new("subtitle", DataType::Utf8, true),
                Field::new("body", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["first title", "second title"])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec![
                    "matching body one",
                    "matching body two",
                ])),
            ],
        )
        .expect("Failed to create test batch");
        let table = Arc::new(
            MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .expect("Failed to create test table"),
        );
        let index = FullTextDatabaseIndex::try_new(
            table,
            vec!["body".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &[
                "title".to_string(),
                "subtitle".to_string(),
                "body".to_string(),
            ],
        )
        .expect("Failed to create FullTextDatabaseIndex");
        index
            .compute_index(vec![batch])
            .await
            .expect("failed to compute_index");

        let search_index = index
            .full_text_search_field_index("body")
            .expect("Failed to create FullTextSearchFieldIndex");
        let batches = search_index
            .search("matching".to_string(), &[], 100)
            .expect("Failed to search")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect search results");

        assert_eq!(batches.len(), 1, "expected one result page");
        let result = &batches[0];
        assert_eq!(result.schema(), search_index.result_schema());
        let titles = result
            .column_by_name("title")
            .expect("result schema must retain title")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("title must retain its Utf8 type");
        let subtitles = result
            .column_by_name("subtitle")
            .expect("result schema must retain subtitle")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("subtitle must retain its Utf8 type");
        let bodies = result
            .column_by_name("body")
            .expect("result schema must retain body")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body must retain its Utf8 type");
        assert!(titles.iter().any(|title| title == Some("first title")));
        assert!(bodies.iter().any(|body| body == Some("matching body one")));
        assert_eq!(subtitles.null_count(), 2);
        assert!(subtitles.is_null(0));
        assert!(subtitles.is_null(1));
    }

    // Regression test for #12228: exercises the execution path (`FullTextSearchQuery::scan`
    // -> `FullTextSearchExec::execute`), not just `FullTextSearchFieldIndex::search`, since the
    // reported bug was in `FullTextSearchExec`'s positional projection shifting columns.
    #[tokio::test]
    async fn test_search_exec_projection_does_not_shift_columns() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("title", DataType::Utf8, false),
                Field::new("subtitle", DataType::Utf8, true),
                Field::new("body", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["first title", "second title"])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec![
                    "matching body one",
                    "matching body two",
                ])),
            ],
        )
        .expect("Failed to create test batch");
        let table = Arc::new(
            MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .expect("Failed to create test table"),
        );
        let index = FullTextDatabaseIndex::try_new(
            table,
            vec!["body".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &[
                "title".to_string(),
                "subtitle".to_string(),
                "body".to_string(),
            ],
        )
        .expect("Failed to create FullTextDatabaseIndex");
        index
            .compute_index(vec![batch])
            .await
            .expect("failed to compute_index");

        let search_index = Arc::new(
            index
                .full_text_search_field_index("body")
                .expect("Failed to create FullTextSearchFieldIndex"),
        );
        let query = FullTextSearchQuery {
            index: search_index,
            query: "matching".to_string(),
            pre_limit: None,
        };

        let ctx = SessionContext::new();
        let full_schema = query.schema();
        // Project onto a subset that skips the leading nullable `subtitle` column, so a
        // positional (rather than name-based) projection would shift `body` into
        // `title`'s slot -- the exact regression from #12228.
        let projection = vec![
            full_schema.index_of("body").expect("body in schema"),
            full_schema.index_of("title").expect("title in schema"),
        ];

        let plan = query
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .expect("scan should succeed");
        let batches = collect(plan, ctx.task_ctx())
            .await
            .expect("execution should succeed");

        assert_eq!(batches.len(), 1, "expected one result page");
        let result = &batches[0];
        assert_eq!(result.schema().field(0).name(), "body");
        assert_eq!(result.schema().field(1).name(), "title");

        let bodies = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body must retain its Utf8 type");
        let titles = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("title must retain its Utf8 type");
        assert!(bodies.iter().any(|body| body == Some("matching body one")));
        assert!(titles.iter().any(|title| title == Some("first title")));
    }

    #[tokio::test]
    async fn test_updates_overwrites_on_compute_index() {
        let index = FullTextDatabaseIndex::try_new(
            create_test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("Failed to create FullTextDatabaseIndex");

        // Use distinct content so each document is independently verifiable.
        index
            .compute_index(vec![
                record_batch!(
                    ("id", Int32, [1, 2, 3]),
                    (
                        "content",
                        Utf8,
                        [
                            "apple banana cherry",
                            "dog elephant frog",
                            "guitar harmonica instrument"
                        ]
                    )
                )
                .expect("Failed to create test batch"),
            ])
            .await
            .expect("failed to compute_index");

        // All three documents are indexed
        {
            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");

            insta::assert_snapshot!(
                "initial_apple",
                search_and_format(&search_index, "apple").await
            );
            insta::assert_snapshot!(
                "initial_elephant",
                search_and_format(&search_index, "elephant").await
            );
            insta::assert_snapshot!(
                "initial_guitar",
                search_and_format(&search_index, "guitar").await
            );
        }

        // Overwrite id=1 and id=3 with new content
        {
            index
                .compute_index(vec![
                    record_batch!(
                        ("id", Int32, [1, 3]),
                        (
                            "content",
                            Utf8,
                            ["mango nectarine orange", "piano quartet rhythm"]
                        )
                    )
                    .expect("Failed to create test record_batch"),
                ])
                .await
                .expect("failed to compute_index");

            // The `_score` column below is a BM25 score, so it is only stable once the
            // superseded documents have left the collection statistics.
            wait_for_superseded_docs_expunged(&index).await;

            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");

            // Old content for id=1 and id=3 should be gone (expects empty results).
            insta::assert_snapshot!(
                "after_update_apple",
                search_and_format(&search_index, "apple").await
            );
            insta::assert_snapshot!(
                "after_update_guitar",
                search_and_format(&search_index, "guitar").await
            );

            // id=2 unchanged.
            insta::assert_snapshot!(
                "after_update_elephant",
                search_and_format(&search_index, "elephant").await
            );

            // New content is searchable.
            insta::assert_snapshot!(
                "after_update_mango",
                search_and_format(&search_index, "mango").await
            );
            insta::assert_snapshot!(
                "after_update_piano",
                search_and_format(&search_index, "piano").await
            );
        }
    }

    #[tokio::test]
    async fn test_updates_overwrites_on_compute_index_composite_pk() {
        let batch = record_batch!(
            ("id1", Utf8, ["a", "a", "b"]),
            ("id2", Int32, [1, 2, 1]),
            (
                "content",
                Utf8,
                [
                    "apple banana cherry",
                    "dog elephant frog",
                    "guitar harmonica instrument"
                ]
            )
        )
        .expect("Failed to create test batch");

        let index = FullTextDatabaseIndex::try_new(
            Arc::new(
                MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                    .expect("Failed to create test table"),
            ),
            vec!["content".to_string()],
            Some(vec!["id1".to_string(), "id2".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("Failed to create FullTextDatabaseIndex");

        // Initial table
        index
            .compute_index(vec![batch])
            .await
            .expect("failed to compute_index");

        // All three documents are indexed
        {
            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");

            insta::assert_snapshot!(
                "cpk_initial_apple",
                search_and_format(&search_index, "apple").await
            );
            insta::assert_snapshot!(
                "cpk_initial_elephant",
                search_and_format(&search_index, "elephant").await
            );
            insta::assert_snapshot!(
                "cpk_initial_guitar",
                search_and_format(&search_index, "guitar").await
            );
        }

        // Overwrite (a,1) and (b,1) with new content
        {
            index
                .compute_index(vec![
                    record_batch!(
                        ("id1", Utf8, ["a", "b"]),
                        ("id2", Int32, [1, 1]),
                        (
                            "content",
                            Utf8,
                            ["mango nectarine orange", "piano quartet rhythm"]
                        )
                    )
                    .expect("Failed to create test record_batch"),
                ])
                .await
                .expect("failed to compute_index");

            // See `test_updates_overwrites_on_compute_index`: BM25 scores are only
            // stable once the superseded documents have left the collection statistics.
            wait_for_superseded_docs_expunged(&index).await;

            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");

            // Old content for (a,1) and (b,1) should be gone (expects empty results).
            insta::assert_snapshot!(
                "cpk_after_update_apple",
                search_and_format(&search_index, "apple").await
            );
            insta::assert_snapshot!(
                "cpk_after_update_guitar",
                search_and_format(&search_index, "guitar").await
            );

            // (a,2) unchanged.
            insta::assert_snapshot!(
                "cpk_after_update_elephant",
                search_and_format(&search_index, "elephant").await
            );

            // New content is searchable.
            insta::assert_snapshot!(
                "cpk_after_update_mango",
                search_and_format(&search_index, "mango").await
            );
            insta::assert_snapshot!(
                "cpk_after_update_piano",
                search_and_format(&search_index, "piano").await
            );
        }
    }

    /// A segment big enough to sit alone at its size level still has to be rewritten once
    /// most of its documents have been superseded. Otherwise tantivy keeps counting those
    /// documents in BM25's collection size and the index scores every query against rows
    /// it has already replaced.
    ///
    /// Regression test for #12053.
    #[tokio::test]
    async fn test_superseded_documents_leave_the_bm25_collection_size() {
        let index = new_test_index();
        supersede_half_of_a_consolidated_segment(&index).await;

        // An index updated in place must score exactly as one rebuilt from the same final
        // rows — the comparison a user makes when they check a refreshed dataset against a
        // freshly loaded one.
        let rebuilt = new_test_index();
        rebuilt
            .compute_index(vec![batch(
                &EXPUNGE_FIXTURE_IDS,
                &[
                    "replaced1 shared",
                    "replaced2 shared",
                    "replaced3 shared",
                    "replaced4 shared",
                    "term5 shared",
                    "term6 shared",
                    "term7 shared",
                    "term8 shared",
                ],
            )])
            .await
            .expect("failed to compute_index");

        let updated_search = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        let rebuilt_search = rebuilt
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        assert_eq!(
            search_and_format(&updated_search, "term8").await,
            search_and_format(&rebuilt_search, "term8").await,
            "an updated index must score identically to one rebuilt from the same rows"
        );
    }

    /// `on_write_start` and `on_write_failed` both roll the writer back, and a tantivy
    /// rollback swaps in a freshly built writer carrying the default merge policy. If the
    /// policy is not reinstated, an index stops expunging superseded documents the moment
    /// its first write window opens — which is every refresh.
    #[tokio::test]
    async fn test_merge_policy_survives_a_writer_rollback() {
        let index = new_test_index();
        index
            .on_write_start(WriteWindow::Append)
            .await
            .expect("on_write_start failed");
        index
            .on_write_failed()
            .await
            .expect("on_write_failed failed");

        // Re-run the whole expunge fixture on the rolled-back writer.
        supersede_half_of_a_consolidated_segment(&index).await;
    }

    /// `on_write_start` returns an error precisely so the write is abandoned: the rollback it
    /// performs is what discards operations an earlier abandoned window left staged. The sink
    /// only honours that by declaring the failure fatal — without this the sink logs a warning
    /// and writes anyway, and those stale operations land in this window's commit (#12421).
    #[tokio::test]
    async fn test_a_failed_write_start_is_fatal() {
        let index = new_test_index();
        assert!(index.write_start_failure_is_fatal());
    }

    #[tokio::test]
    async fn test_compute_index_returns_batches_unchanged() {
        let index = FullTextDatabaseIndex::try_new(
            create_test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &[],
        )
        .expect("Failed to create index");

        let input_batch = record_batch!(
            ("id", Int32, [4, 5]),
            ("content", Utf8, ["new content 1", "new content 2"])
        )
        .expect("Failed to create test batch");

        let input_batches = vec![input_batch.clone()];
        let result_batches = index
            .compute_index(input_batches.clone())
            .await
            .expect("Failed to compute index");

        assert_eq!(input_batches.len(), result_batches.len());

        for (input, result) in input_batches.iter().zip(result_batches.iter()) {
            assert_eq!(input.schema(), result.schema());
            assert_eq!(input.num_rows(), result.num_rows());
            assert_eq!(input.num_columns(), result.num_columns());

            for col_idx in 0..input.num_columns() {
                let input_col = input.column(col_idx);
                let result_col = result.column(col_idx);
                assert_eq!(input_col, result_col);
            }
        }
    }

    /// A sink-driven refresh (`on_write_start` .. `on_write_complete`) must defer the
    /// tantivy commit: staged documents are invisible to searches until the window is
    /// closed by `on_write_complete`, at which point they all become visible at once.
    #[tokio::test]
    async fn test_deferred_commit_defers_visibility_until_write_complete() {
        let index = FullTextDatabaseIndex::try_new(
            create_test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("Failed to create FullTextDatabaseIndex");

        // Open a deferred-commit window, mirroring the sink's on_write_start hook.
        index
            .on_write_start(WriteWindow::Append)
            .await
            .expect("on_write_start failed");

        index
            .compute_index(vec![
                record_batch!(
                    ("id", Int32, [1, 2, 3]),
                    (
                        "content",
                        Utf8,
                        ["apple banana", "dog elephant", "guitar harmonica"]
                    )
                )
                .expect("Failed to create test batch"),
            ])
            .await
            .expect("compute_index failed");

        // Before the window closes, the staged documents are not committed and so are
        // not visible to a freshly obtained searcher.
        {
            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");
            let results = search_and_format(&search_index, "apple").await;
            assert!(
                !results.contains("apple banana"),
                "documents must not be visible before on_write_complete, got:\n{results}"
            );
        }

        // Closing the window performs the single commit + reader reload.
        index
            .on_write_complete()
            .await
            .expect("on_write_complete failed");

        {
            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");
            let results = search_and_format(&search_index, "apple").await;
            assert!(
                results.contains("apple banana"),
                "documents must be visible after on_write_complete, got:\n{results}"
            );
        }
    }

    /// Run one sink-driven write window end to end: `on_write_start(window)`, one batch,
    /// `on_write_complete`.
    async fn write_window(index: &FullTextDatabaseIndex, window: WriteWindow, rb: RecordBatch) {
        index
            .on_write_start(window)
            .await
            .expect("on_write_start failed");
        index
            .compute_index(vec![rb])
            .await
            .expect("compute_index failed");
        index
            .on_write_complete()
            .await
            .expect("on_write_complete failed");
    }

    fn replace_all_tier() -> FullTextDatabaseIndex {
        FullTextDatabaseIndex::try_new(
            create_test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("Failed to create FullTextDatabaseIndex")
    }

    fn three_rows() -> RecordBatch {
        record_batch!(
            ("id", Int32, [1, 2, 3]),
            (
                "content",
                Utf8,
                [
                    "apple banana cherry",
                    "dog elephant frog",
                    "guitar harmonica instrument"
                ]
            )
        )
        .expect("Failed to create test batch")
    }

    /// Regression test for #12066: a `refresh_mode: full` refresh replaces the table's rows, so
    /// a row the source deleted is simply absent from the second window. `compute_index` only
    /// deletes the keys it is handed, so before the `ReplaceAll` clear the dropped row stayed
    /// searchable forever with its stale stored content.
    #[tokio::test]
    async fn replace_all_window_drops_documents_for_rows_the_source_dropped() {
        let index = replace_all_tier();

        // Refresh 1: the source has ids 1, 2, 3.
        write_window(&index, WriteWindow::ReplaceAll, three_rows()).await;

        // Refresh 2: id=2 was deleted at the source, so the refresh returns only 1 and 3.
        write_window(
            &index,
            WriteWindow::ReplaceAll,
            record_batch!(
                ("id", Int32, [1, 3]),
                (
                    "content",
                    Utf8,
                    ["apple banana cherry", "guitar harmonica instrument"]
                )
            )
            .expect("Failed to create test batch"),
        )
        .await;

        let search_index = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");

        let dropped = search_and_format(&search_index, "elephant").await;
        assert!(
            !dropped.contains("dog elephant frog"),
            "a row dropped by a full refresh must not remain searchable, got:\n{dropped}"
        );

        // The rows the refresh *did* re-send must survive the clear.
        let kept = search_and_format(&search_index, "apple").await;
        assert!(
            kept.contains("apple banana cherry"),
            "a row re-sent by the refresh must still be searchable, got:\n{kept}"
        );
    }

    /// The clear must be scoped to a replacing window. An append adds rows and says nothing
    /// about the ones it omits, so wiping on `Append` would delete live rows' documents —
    /// which is also why `InsertOp::Replace` (an upsert) maps to `WriteWindow::Append`.
    #[tokio::test]
    async fn append_window_keeps_documents_absent_from_the_batch() {
        let index = replace_all_tier();

        write_window(&index, WriteWindow::ReplaceAll, three_rows()).await;
        write_window(
            &index,
            WriteWindow::Append,
            record_batch!(("id", Int32, [4]), ("content", Utf8, ["jackal koala"]))
                .expect("Failed to create test batch"),
        )
        .await;

        let search_index = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");

        let results = search_and_format(&search_index, "elephant").await;
        assert!(
            results.contains("dog elephant frog"),
            "an append must not clear rows it does not mention, got:\n{results}"
        );
    }

    /// The clear is staged inside the deferred-commit window rather than applied eagerly, so
    /// the wipe and the repopulation land in one commit. Queries running during the refresh
    /// must keep seeing the *previous* contents, never an empty index.
    #[tokio::test]
    async fn replace_all_clear_is_invisible_until_write_complete() {
        let index = replace_all_tier();
        write_window(&index, WriteWindow::ReplaceAll, three_rows()).await;

        // Open a replacing window and stage its rows, but do not close it.
        index
            .on_write_start(WriteWindow::ReplaceAll)
            .await
            .expect("on_write_start failed");
        index
            .compute_index(vec![
                record_batch!(("id", Int32, [1]), ("content", Utf8, ["apple banana"]))
                    .expect("Failed to create test batch"),
            ])
            .await
            .expect("compute_index failed");

        {
            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");
            let mid = search_and_format(&search_index, "elephant").await;
            assert!(
                mid.contains("dog elephant frog"),
                "previous contents must stay readable until the window commits, got:\n{mid}"
            );
        }

        index
            .on_write_complete()
            .await
            .expect("on_write_complete failed");

        // A `FullTextSearchFieldIndex` snapshots the searcher when it is built, so the
        // post-commit revision needs a freshly built one.
        {
            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");
            let after = search_and_format(&search_index, "elephant").await;
            assert!(
                !after.contains("dog elephant frog"),
                "the clear must take effect once the window commits, got:\n{after}"
            );
        }
    }

    /// A failed replacing window must leave the index exactly as it was: the staged clear is
    /// rolled back along with the staged documents, so a refresh that dies partway does not
    /// empty the index.
    #[tokio::test]
    async fn failed_replace_all_window_leaves_the_index_intact() {
        let index = replace_all_tier();
        write_window(&index, WriteWindow::ReplaceAll, three_rows()).await;

        index
            .on_write_start(WriteWindow::ReplaceAll)
            .await
            .expect("on_write_start failed");
        index
            .on_write_failed()
            .await
            .expect("on_write_failed failed");

        let search_index = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        let results = search_and_format(&search_index, "elephant").await;
        assert!(
            results.contains("dog elephant frog"),
            "a failed refresh must not empty the index, got:\n{results}"
        );
    }

    /// `delete_by_keys` shares the one tantivy writer with an open sink window, so it must not
    /// commit while a window is staged: committing would publish that window's staged
    /// `ReplaceAll` clear and empty the index. This matters because a window can be abandoned
    /// without `on_write_failed` running (an upstream stream error returns early from
    /// `MultiSink::insert_into`), leaving the clear staged until the next `on_write_start`
    /// rolls it back.
    #[tokio::test]
    async fn delete_by_keys_does_not_publish_a_staged_replace_all_clear() {
        let index = replace_all_tier();
        write_window(&index, WriteWindow::ReplaceAll, three_rows()).await;

        // Open a replacing window, staging the clear, and never close it.
        index
            .on_write_start(WriteWindow::ReplaceAll)
            .await
            .expect("on_write_start failed");

        // A delete arrives while that window is open.
        index
            .delete_by_keys(record_batch!(("id", Int32, [3])).expect("Failed to create keys"))
            .await
            .expect("delete_by_keys failed");

        let search_index = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        let results = search_and_format(&search_index, "elephant").await;
        assert!(
            results.contains("dog elephant frog"),
            "a delete must not publish the window's staged clear, got:\n{results}"
        );
    }

    /// The CDC path drives `compute_index` directly without the sink lifecycle hooks,
    /// so each call must still commit immediately (no deferral) and be visible at once.
    #[tokio::test]
    async fn test_compute_index_commits_immediately_without_write_hooks() {
        let index = FullTextDatabaseIndex::try_new(
            create_test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("Failed to create FullTextDatabaseIndex");

        index
            .compute_index(vec![
                record_batch!(("id", Int32, [1]), ("content", Utf8, ["apple banana"]))
                    .expect("Failed to create test batch"),
            ])
            .await
            .expect("compute_index failed");

        let search_index = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        let results = search_and_format(&search_index, "apple").await;
        assert!(
            results.contains("apple banana"),
            "CDC-style compute_index must commit immediately, got:\n{results}"
        );
    }

    /// A failed sink write (`on_write_failed`) must roll back everything staged in the
    /// deferred window so a partial refresh never becomes visible.
    #[tokio::test]
    async fn test_on_write_failed_discards_deferred_documents() {
        let index = FullTextDatabaseIndex::try_new(
            create_test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("Failed to create FullTextDatabaseIndex");

        index
            .on_write_start(WriteWindow::Append)
            .await
            .expect("on_write_start failed");

        index
            .compute_index(vec![
                record_batch!(
                    ("id", Int32, [1, 2]),
                    ("content", Utf8, ["apple banana", "dog elephant"])
                )
                .expect("Failed to create test batch"),
            ])
            .await
            .expect("compute_index failed");

        // The write failed: discard the staged window instead of committing it.
        index
            .on_write_failed()
            .await
            .expect("on_write_failed failed");

        let search_index = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        let results = search_and_format(&search_index, "apple").await;
        assert!(
            !results.contains("apple banana"),
            "on_write_failed must discard staged documents, got:\n{results}"
        );
    }

    /// A CDC-fed index shares one tantivy writer with the sink write path, so it must
    /// never defer: a window commit would publish a partial refresh, and a window
    /// rollback would discard the change stream's documents.
    #[tokio::test]
    async fn test_cdc_attached_index_never_defers_commits() {
        let index = FullTextDatabaseIndex::try_new(
            create_test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("Failed to create FullTextDatabaseIndex");

        index.mark_cdc_attached();

        // Opening a window is a no-op for a CDC-fed index.
        index
            .on_write_start(WriteWindow::Append)
            .await
            .expect("on_write_start failed");

        index
            .compute_index(vec![
                record_batch!(("id", Int32, [1]), ("content", Utf8, ["apple banana"]))
                    .expect("Failed to create test batch"),
            ])
            .await
            .expect("compute_index failed");

        // Visible immediately, without waiting for on_write_complete.
        {
            let search_index = index
                .full_text_search_field_index("content")
                .expect("Failed to create FullTextSearchFieldIndex");
            let results = search_and_format(&search_index, "apple").await;
            assert!(
                results.contains("apple banana"),
                "a CDC-fed index must commit immediately even inside a write window, got:\n{results}"
            );
        }

        // And a failed window must not discard those committed documents.
        index
            .on_write_failed()
            .await
            .expect("on_write_failed failed");

        let search_index = index
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        let results = search_and_format(&search_index, "apple").await;
        assert!(
            results.contains("apple banana"),
            "committed CDC documents must survive a failed write window, got:\n{results}"
        );
    }

    /// A warm full-text tier can be registered inside a
    /// [`CompoundSearchIndex`](crate::index::compound::CompoundSearchIndex) rather than
    /// directly, with writes routed to it via [`SearchIndex::write`]. Once that tier is marked
    /// CDC-attached, it must stop deferring commits regardless of whether writes reach it
    /// directly or through the compound, or a failed write window discards the change
    /// stream's documents for good.
    #[tokio::test]
    async fn test_cdc_attached_compound_primary_never_defers_commits() {
        use crate::index::compound::{CompoundReadMode, CompoundSearchIndex};

        let new_tier = || {
            FullTextDatabaseIndex::try_new(
                create_test_table(),
                vec!["content".to_string()],
                Some(vec!["id".to_string()]),
                None,
                &["content".to_string()],
            )
            .expect("Failed to create FullTextDatabaseIndex")
        };

        // Keep a handle on the warm tier: it shares the tantivy writer and reader with the
        // clone held by the compound, so searching it observes the compound's writes.
        let warm = new_tier();
        let compound = CompoundSearchIndex::try_new(
            Arc::new(warm.clone()) as Arc<dyn SearchIndex>,
            Arc::new(new_tier()) as Arc<dyn SearchIndex>,
            CompoundReadMode::PrimaryOnly,
        )
        .expect("two full-text tiers over the same table are compatible");

        warm.mark_cdc_attached();

        // A sink-driven refresh opens a write window on both tiers.
        compound
            .on_write_start(WriteWindow::Append)
            .await
            .expect("on_write_start failed");

        // A change-stream document arrives while that window is open.
        compound
            .compute_index(vec![
                record_batch!(("id", Int32, [1]), ("content", Utf8, ["apple banana"]))
                    .expect("Failed to create test batch"),
            ])
            .await
            .expect("compute_index failed");

        // The refresh then fails, discarding whatever the window staged.
        compound
            .on_write_failed()
            .await
            .expect("on_write_failed failed");

        warm.reader
            .reload()
            .expect("failed to reload the warm tier's reader");
        let search_index = warm
            .full_text_search_field_index("content")
            .expect("Failed to create FullTextSearchFieldIndex");
        let results = search_and_format(&search_index, "apple").await;
        assert!(
            results.contains("apple banana"),
            "a change-stream document written through a compound must be committed, not staged in the failed window, got:\n{results}"
        );
    }

    /// A table with a second text column, so a search configuration can grow by one column
    /// between two [`FullTextDatabaseIndex`] instances over the same directory.
    fn create_two_column_test_table() -> Arc<dyn TableProvider> {
        let batch = record_batch!(
            ("id", Int32, [1, 2]),
            ("content", Utf8, ["test content 1", "test content 2"]),
            ("title", Utf8, ["first title", "second title"])
        )
        .expect("failed to create test batch");
        Arc::new(
            MemTable::try_new(batch.schema(), vec![vec![batch]])
                .expect("failed to create test table"),
        )
    }

    fn file_backed_index(
        directory: &Path,
        search_fields: &[&str],
        primary_key: &[&str],
    ) -> Result<FullTextDatabaseIndex, super::super::Error> {
        FullTextDatabaseIndex::try_new(
            create_two_column_test_table(),
            search_fields.iter().map(|f| (*f).to_string()).collect(),
            Some(primary_key.iter().map(|p| (*p).to_string()).collect()),
            Some(directory.to_path_buf()),
            &[],
        )
    }

    // Regression test for #12274.
    #[test]
    fn test_persisted_index_rejects_a_newly_configured_search_column() {
        let directory = tempfile::tempdir().expect("failed to create a temporary directory");

        drop(
            file_backed_index(directory.path(), &["content"], &["id"])
                .expect("failed to create the index"),
        );

        // Reopening with `title` added to the search configuration cannot serve a search over
        // `title`: the persisted schema has no such field, and tantivy silently drops document
        // values for fields its schema does not declare.
        let error = file_backed_index(directory.path(), &["content", "title"], &["id"])
            .expect_err("a persisted index missing a configured search column must be rejected");
        let message = error.to_string();
        assert!(
            message.contains("title"),
            "the error must name the column the persisted index is missing, got: {message}"
        );
        assert!(
            message.contains(&directory.path().display().to_string()),
            "the error must name the index directory to delete, got: {message}"
        );
        assert!(
            error.is_user_error(),
            "a persisted index the configuration no longer matches is fixable from the spicepod"
        );
    }

    // Regression test for #12274.
    #[test]
    fn test_persisted_index_rejects_a_primary_key_that_became_a_search_column() {
        let directory = tempfile::tempdir().expect("failed to create a temporary directory");

        // `title` is the primary key and not a search column, so it is indexed untokenized —
        // what a primary-key term lookup relies on.
        drop(
            file_backed_index(directory.path(), &["content"], &["title"])
                .expect("failed to create the index"),
        );

        // Configuring `title` as a search column asks for it tokenized instead, so the terms
        // the persisted index holds for it are no longer the ones a delete would address.
        let error = file_backed_index(directory.path(), &["content", "title"], &["title"])
            .expect_err(
                "a persisted index whose primary key is indexed differently must be rejected",
            );
        let message = error.to_string();
        assert!(
            message.contains("(untokenized)") && message.contains("(tokenized)"),
            "the error must say how the column's indexing changed, got: {message}"
        );
        assert!(
            error.is_user_error(),
            "a persisted index the configuration no longer matches is fixable from the spicepod"
        );
    }

    // Regression test for #12274.
    #[test]
    fn test_persisted_index_reopens_with_a_compatible_configuration() {
        let directory = tempfile::tempdir().expect("failed to create a temporary directory");

        drop(
            file_backed_index(directory.path(), &["content", "title"], &["id"])
                .expect("failed to create the index"),
        );
        drop(
            file_backed_index(directory.path(), &["content", "title"], &["id"])
                .expect("reopening an unchanged persisted index must succeed"),
        );

        // A column the configuration no longer searches is left alone: nothing queries it.
        drop(
            file_backed_index(directory.path(), &["content"], &["id"])
                .expect("dropping a search column must not reject the persisted index"),
        );
    }

    // Regression test for #12274: an index created before the runtime changed the tokenizer it
    // configures still answers queries consistently with what it indexed, so it must keep
    // loading rather than fail the dataset on upgrade.
    #[test]
    fn test_persisted_index_reopens_when_only_the_tokenizer_differs() {
        let directory = tempfile::tempdir().expect("failed to create a temporary directory");

        // Build the directory with tantivy's stock `TEXT` options — the tokenizer
        // `tokenized_text_options` replaced.
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_i64_field("id", tantivy::schema::STORED | tantivy::schema::INDEXED);
        schema_builder.add_text_field("content", tantivy::schema::TEXT);
        drop(
            tantivy::Index::create_in_dir(directory.path(), schema_builder.build())
                .expect("failed to create a stock-tokenizer index"),
        );

        drop(
            file_backed_index(directory.path(), &["content"], &["id"])
                .expect("a tokenizer-only difference must not reject the persisted index"),
        );
    }
}
