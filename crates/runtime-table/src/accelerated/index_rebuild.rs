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

//! Rebuilds, at startup, an index that holds none of the rows its accelerator already holds.
//!
//! An index whose entries live outside the accelerated table — an in-memory full-text index,
//! or a file-backed one whose directory a snapshot bootstrap did not restore — is filled only
//! by the acceleration write path. An accelerator that keeps its rows across a restart is
//! refreshed with only what it is missing (or, for a checkpointed full-refresh dataset with no
//! `refresh_check_interval`, not at all), so nothing would ever write the rows it already holds
//! into that index, and every search would answer from an empty or partial index (#14618).

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::execute_stream;
use futures::TryStreamExt;
use spice_table::{Index, WriteWindow};

use super::sink::{finalize_indexes, prepare_indexes, rollback_indexes};

const SINK_NAME: &str = "IndexRebuild";

/// Replays every row `accelerator` holds through each of `indexes` that reports
/// [`Index::requires_rebuild`], returning how many rows were replayed (`None` when no index
/// needed it).
///
/// The replay runs in a [`WriteWindow::Rebuild`] window rather than a replacing one: an index
/// that needs a rebuild holds nothing to clear, and an index composed with a durable store
/// (a compound over Elasticsearch) must not have that store wiped to rebuild its other half.
/// The window also tells a stream-attached index it may defer its commit, so a replay that
/// fails part-way leaves nothing committed and the next startup replays it again.
///
/// # Errors
///
/// Returns the first error from scanning the accelerator, preparing, computing or finalizing an
/// index. The indexes that opened a window are rolled back first, so a partial replay is never
/// published.
pub(crate) async fn rebuild_indexes_from_accelerator(
    dataset_name: &TableReference,
    accelerator: &Arc<dyn TableProvider>,
    indexes: &[Arc<dyn Index + Send + Sync>],
) -> Result<Option<usize>, DataFusionError> {
    let indexes: Vec<Arc<dyn Index + Send + Sync>> = indexes
        .iter()
        .filter(|index| index.requires_rebuild())
        .map(Arc::clone)
        .collect();
    if indexes.is_empty() {
        return Ok(None);
    }

    tracing::debug!(
        "Rebuilding {} index(es) of dataset {dataset_name} from its acceleration",
        indexes.len()
    );

    prepare_indexes(SINK_NAME, indexes.iter(), WriteWindow::Rebuild).await?;

    match replay(accelerator, &indexes).await {
        Ok(rows) => {
            finalize_indexes(SINK_NAME, indexes.iter()).await?;
            Ok(Some(rows))
        }
        Err(e) => {
            rollback_indexes(SINK_NAME, indexes.iter()).await;
            Err(e)
        }
    }
}

async fn replay(
    accelerator: &Arc<dyn TableProvider>,
    indexes: &[Arc<dyn Index + Send + Sync>],
) -> Result<usize, DataFusionError> {
    let ctx = util::session_state::session_context();
    let state = ctx.state();
    let plan = accelerator.scan(&state, None, &[], None).await?;
    let mut stream = execute_stream(plan, ctx.task_ctx())?;

    let mut rows = 0;
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        rows += batch.num_rows();
        for index in indexes {
            index.compute_index(vec![batch.clone()]).await?;
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::datasource::MemTable;
    use parking_lot::Mutex;

    use super::*;

    /// Records what the rebuild drives through it.
    #[derive(Debug, Default)]
    struct RecordingIndex {
        requires_rebuild: bool,
        fail_compute: bool,
        started: Mutex<Vec<WriteWindow>>,
        rows: AtomicUsize,
        completed: AtomicBool,
        failed: AtomicBool,
    }

    impl RecordingIndex {
        fn new(requires_rebuild: bool) -> Arc<Self> {
            Arc::new(Self {
                requires_rebuild,
                ..Self::default()
            })
        }
    }

    #[async_trait]
    impl Index for RecordingIndex {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn required_columns(&self) -> Vec<String> {
            vec!["id".to_string()]
        }

        async fn compute_index(
            &self,
            batches: Vec<RecordBatch>,
        ) -> Result<Vec<RecordBatch>, DataFusionError> {
            if self.fail_compute {
                return Err(DataFusionError::Execution("index write failed".to_string()));
            }
            self.rows.fetch_add(
                batches.iter().map(RecordBatch::num_rows).sum(),
                Ordering::SeqCst,
            );
            Ok(batches)
        }

        async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
            self.started.lock().push(window);
            Ok(())
        }

        async fn on_write_complete(&self) -> Result<(), DataFusionError> {
            self.completed.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn on_write_failed(&self) -> Result<(), DataFusionError> {
            self.failed.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn requires_rebuild(&self) -> bool {
            self.requires_rebuild
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// An accelerator already holding `ids`, split over two batches so the replay has to
    /// stream rather than see a single batch.
    fn accelerator(ids: &[i32]) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let (first, second) = ids.split_at(ids.len() / 2);
        let batches = [first, second]
            .into_iter()
            .map(|ids| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids.to_vec()))],
                )
                .expect("valid batch")
            })
            .collect();
        Arc::new(MemTable::try_new(schema, vec![batches]).expect("valid table"))
    }

    fn erase(indexes: &[&Arc<RecordingIndex>]) -> Vec<Arc<dyn Index + Send + Sync>> {
        indexes
            .iter()
            .map(|index| Arc::clone(*index) as Arc<dyn Index + Send + Sync>)
            .collect()
    }

    /// Regression test for #14618: an index that starts empty over an accelerator that kept
    /// its rows is given every one of them, in one rebuild window that is then committed.
    #[tokio::test]
    async fn an_empty_index_is_given_every_row_the_accelerator_holds() {
        let empty = RecordingIndex::new(true);
        let rows = rebuild_indexes_from_accelerator(
            &TableReference::bare("docs"),
            &accelerator(&[1, 2, 3, 4, 5]),
            &erase(&[&empty]),
        )
        .await
        .expect("rebuild succeeds");

        assert_eq!(rows, Some(5));
        assert_eq!(empty.rows.load(Ordering::SeqCst), 5);
        assert_eq!(*empty.started.lock(), vec![WriteWindow::Rebuild]);
        assert!(empty.completed.load(Ordering::SeqCst));
        assert!(!empty.failed.load(Ordering::SeqCst));
    }

    /// An index whose entries already exist (co-located vectors, a surviving file index) is
    /// neither opened nor written: replaying through it would recompute what it holds.
    #[tokio::test]
    async fn an_index_that_holds_its_rows_is_left_alone() {
        let populated = RecordingIndex::new(false);
        let empty = RecordingIndex::new(true);
        rebuild_indexes_from_accelerator(
            &TableReference::bare("docs"),
            &accelerator(&[1, 2]),
            &erase(&[&populated, &empty]),
        )
        .await
        .expect("rebuild succeeds");

        assert_eq!(populated.rows.load(Ordering::SeqCst), 0);
        assert!(populated.started.lock().is_empty());
        assert!(!populated.completed.load(Ordering::SeqCst));
        assert_eq!(empty.rows.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn nothing_runs_when_no_index_needs_a_rebuild() {
        let populated = RecordingIndex::new(false);
        let rows = rebuild_indexes_from_accelerator(
            &TableReference::bare("docs"),
            &accelerator(&[1, 2]),
            &erase(&[&populated]),
        )
        .await
        .expect("rebuild succeeds");

        assert_eq!(rows, None);
        assert!(populated.started.lock().is_empty());
    }

    /// An empty accelerator still opens and commits the window, so the index ends in a known
    /// (empty) state rather than a dangling one.
    #[tokio::test]
    async fn an_empty_accelerator_commits_an_empty_window() {
        let empty = RecordingIndex::new(true);
        let rows = rebuild_indexes_from_accelerator(
            &TableReference::bare("docs"),
            &accelerator(&[]),
            &erase(&[&empty]),
        )
        .await
        .expect("rebuild succeeds");

        assert_eq!(rows, Some(0));
        assert!(empty.completed.load(Ordering::SeqCst));
    }

    /// A failed replay is rolled back and reported, never committed as a partial index.
    #[tokio::test]
    async fn a_failed_replay_is_rolled_back_and_reported() {
        let failing = Arc::new(RecordingIndex {
            requires_rebuild: true,
            fail_compute: true,
            ..RecordingIndex::default()
        });
        let err = rebuild_indexes_from_accelerator(
            &TableReference::bare("docs"),
            &accelerator(&[1, 2]),
            &erase(&[&failing]),
        )
        .await
        .expect_err("a failed index write fails the rebuild");

        assert!(err.to_string().contains("index write failed"), "{err}");
        assert!(failing.failed.load(Ordering::SeqCst));
        assert!(!failing.completed.load(Ordering::SeqCst));
    }

    /// The error a failed rebuild surfaces names the dataset, says it is not loaded, and links
    /// the docs, so a reword cannot quietly drop the resource, the consequence, or the fix.
    #[test]
    fn a_failed_rebuild_names_the_dataset_and_that_it_is_not_loaded() {
        let err = super::super::Error::FailedToRebuildIndex {
            dataset_name: "docs".to_string(),
            source: DataFusionError::Execution("index write failed".to_string()),
        };

        assert_eq!(
            err.to_string(),
            "Failed to rebuild the search index of dataset 'docs' from its acceleration, so the \
             dataset is not loaded rather than answering searches from an incomplete index. Fix \
             the cause below and restart; for details, visit: \
             https://spiceai.org/docs/features/search/full-text-search. Cause: Execution error: \
             index write failed"
        );
    }
}
