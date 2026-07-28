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

use datafusion::{
    catalog::TableProvider, error::DataFusionError, logical_expr::dml::InsertOp,
    physical_plan::RecordBatchStream,
};
use multi::MultiSink;
use runtime_datafusion_index::Index;
use std::{pin::Pin, sync::Arc};
use table::TableSink;
use util::RetryError;

use super::synchronized_table::SynchronizedTable;

pub(crate) mod multi;
pub(crate) mod table;

#[derive(Debug)]
pub enum AccelerationSink {
    Table(TableSink),
    Multi(MultiSink),
}

impl AccelerationSink {
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self::Table(TableSink::new(table_provider))
    }

    pub fn with_sink_indexes(self, indexes: Vec<Arc<dyn Index + Send + Sync>>) -> Self {
        match self {
            AccelerationSink::Table(sink) => {
                AccelerationSink::Table(sink.with_sink_indexes(indexes))
            }
            AccelerationSink::Multi(sink) => {
                AccelerationSink::Multi(sink.with_sink_indexes(indexes))
            }
        }
    }

    // Adds a table provider to the AccelerationSink, converting a TableSink to a MultiSink if necessary
    pub fn add_synchronized_table(&mut self, synchronized_table: SynchronizedTable) {
        match self {
            AccelerationSink::Table(table_sink) => {
                let table_provider = Arc::clone(&table_sink.table_provider);
                let sink_indexes = std::mem::take(&mut table_sink.sink_indexes);
                let multi_sink = MultiSink::new(table_provider, vec![synchronized_table])
                    .with_sink_indexes(sink_indexes);
                *self = AccelerationSink::Multi(multi_sink);
            }
            AccelerationSink::Multi(sink) => sink.add_synchronized_table(synchronized_table),
        }
    }

    pub fn synchronized_tables(&self) -> Vec<&SynchronizedTable> {
        match self {
            AccelerationSink::Table(_) => vec![],
            AccelerationSink::Multi(sink) => sink.synchronized_tables().iter().collect(),
        }
    }

    pub async fn insert_into(
        &self,
        record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        match self {
            AccelerationSink::Table(sink) => sink.insert_into(record_batch_stream, overwrite).await,
            AccelerationSink::Multi(sink) => sink.insert_into(record_batch_stream, overwrite).await,
        }
    }
}

/// Runs [`Index::on_write_complete`] for every index after a successful write, and
/// reports the first failure from an index that declares a finalize failure fatal.
///
/// Every index is finalized even once one has failed — stopping early would leave
/// more indexes stale than the failure requires. A failure on an index with
/// [`Index::write_complete_failure_is_fatal`] unset is best-effort: it is logged and
/// the write still succeeds, because that index rebuilds on the next refresh. A fatal
/// one fails the write, since reporting success would let the index serve stale or
/// missing results for data the caller believes was indexed.
pub(crate) async fn finalize_indexes<'a>(
    sink: &str,
    indexes: impl Iterator<Item = &'a Arc<dyn Index + Send + Sync>>,
) -> Result<(), DataFusionError> {
    let mut fatal: Option<DataFusionError> = None;

    for index in indexes {
        tracing::debug!(
            "{sink}: running on_write_complete for index '{}'",
            index.name()
        );
        let Err(e) = index.on_write_complete().await else {
            continue;
        };

        if index.write_complete_failure_is_fatal() {
            tracing::error!(
                "{sink}: on_write_complete failed for index '{}': {e}. Failing the write - the index would otherwise be left stale while the write reported success.",
                index.name()
            );
            // Carry the index name in the message rather than wrapping in
            // `DataFusionError::Context`: `find_datafusion_root` unwraps `Context` and
            // discards its description, so the name would never reach the caller.
            fatal.get_or_insert_with(|| {
                DataFusionError::Execution(format!(
                    "Failed to finalize index '{}' after writing: {e}. The write was rejected because the index would otherwise be left stale.",
                    index.name()
                ))
            });
        } else {
            tracing::warn!(
                "{sink}: on_write_complete failed for index '{}': {e}. Index may be stale until next refresh.",
                index.name()
            );
        }
    }

    fatal.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crate::datafusion::error::find_datafusion_root;
    use datafusion::error::{DataFusionError, Result as DataFusionResult};

    use super::{Arc, Index, finalize_indexes};

    /// An [`Index`] whose finalize outcome and fatality are configurable, recording how
    /// many times it was finalized.
    #[derive(Debug)]
    struct FinalizeIndex {
        name: &'static str,
        fails: bool,
        fatal: bool,
        finalize_calls: AtomicUsize,
    }

    impl FinalizeIndex {
        fn new(name: &'static str, fails: bool, fatal: bool) -> Arc<Self> {
            Arc::new(Self {
                name,
                fails,
                fatal,
                finalize_calls: AtomicUsize::new(0),
            })
        }

        fn finalize_calls(&self) -> usize {
            self.finalize_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Index for FinalizeIndex {
        fn name(&self) -> &'static str {
            self.name
        }

        fn required_columns(&self) -> Vec<String> {
            vec![]
        }

        async fn on_write_complete(&self) -> DataFusionResult<()> {
            self.finalize_calls.fetch_add(1, Ordering::SeqCst);
            if self.fails {
                return Err(DataFusionError::Execution(format!(
                    "{} could not finalize",
                    self.name
                )));
            }
            Ok(())
        }

        fn write_complete_failure_is_fatal(&self) -> bool {
            self.fatal
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn erase(indexes: &[Arc<FinalizeIndex>]) -> Vec<Arc<dyn Index + Send + Sync>> {
        indexes
            .iter()
            .map(|i| Arc::clone(i) as Arc<dyn Index + Send + Sync>)
            .collect()
    }

    #[tokio::test]
    async fn every_index_finalizes_when_none_fail() {
        let indexes = vec![
            FinalizeIndex::new("a", false, false),
            FinalizeIndex::new("b", false, true),
        ];
        let erased = erase(&indexes);

        finalize_indexes("TestSink", erased.iter())
            .await
            .expect("no index failed to finalize");

        for index in &indexes {
            assert_eq!(index.finalize_calls(), 1, "index '{}'", index.name);
        }
    }

    #[tokio::test]
    async fn a_non_fatal_finalize_failure_leaves_the_write_successful() {
        let indexes = vec![FinalizeIndex::new("best_effort", true, false)];
        let erased = erase(&indexes);

        finalize_indexes("TestSink", erased.iter())
            .await
            .expect("a best-effort finalize failure must not fail the write");
        assert_eq!(indexes[0].finalize_calls(), 1);
    }

    /// Regression test for #12038: a fatal finalize failure must fail the write rather
    /// than let a stale index report a successful refresh.
    #[tokio::test]
    async fn a_fatal_finalize_failure_fails_the_write() {
        let indexes = vec![FinalizeIndex::new("full_text", true, true)];
        let erased = erase(&indexes);

        let err = finalize_indexes("TestSink", erased.iter())
            .await
            .expect_err("a fatal finalize failure must fail the write");

        let message = err.to_string();
        assert!(
            message.contains("Failed to finalize index 'full_text'"),
            "error should name the index that failed: {message}"
        );
        assert!(
            message.contains("full_text could not finalize"),
            "error should keep the index's own message: {message}"
        );
    }

    /// The reported error must survive `find_datafusion_root`, which the sinks apply via
    /// `retry_from_df_error` — a `DataFusionError::Context` wrapper would have its
    /// description discarded there, losing the index name before the caller sees it.
    #[tokio::test]
    async fn the_reported_error_keeps_the_index_name_through_find_datafusion_root() {
        let indexes = vec![FinalizeIndex::new("full_text", true, true)];
        let erased = erase(&indexes);

        let err = finalize_indexes("TestSink", erased.iter())
            .await
            .expect_err("a fatal finalize failure must fail the write");

        let rooted = find_datafusion_root(err).to_string();
        assert!(
            rooted.contains("Failed to finalize index 'full_text'"),
            "the index name must survive root-cause unwrapping: {rooted}"
        );
    }

    /// A fatal failure must not short-circuit the loop: the indexes after it still need
    /// their finalize, or the failure leaves more indexes stale than it has to.
    #[tokio::test]
    async fn a_fatal_failure_still_finalizes_the_remaining_indexes() {
        let indexes = vec![
            FinalizeIndex::new("fatal_first", true, true),
            FinalizeIndex::new("best_effort", true, false),
            FinalizeIndex::new("healthy", false, false),
        ];
        let erased = erase(&indexes);

        finalize_indexes("TestSink", erased.iter())
            .await
            .expect_err("a fatal finalize failure must fail the write");

        for index in &indexes {
            assert_eq!(index.finalize_calls(), 1, "index '{}'", index.name);
        }
    }

    #[tokio::test]
    async fn the_first_fatal_failure_is_the_reported_one() {
        let indexes = vec![
            FinalizeIndex::new("best_effort", true, false),
            FinalizeIndex::new("fatal_first", true, true),
            FinalizeIndex::new("fatal_second", true, true),
        ];
        let erased = erase(&indexes);

        let err = finalize_indexes("TestSink", erased.iter())
            .await
            .expect_err("a fatal finalize failure must fail the write");

        let message = err.to_string();
        assert!(
            message.contains("fatal_first"),
            "the first fatal failure should be reported: {message}"
        );
        assert!(
            !message.contains("fatal_second"),
            "a later fatal failure must not replace the first: {message}"
        );
    }

    #[tokio::test]
    async fn no_indexes_is_a_successful_finalize() {
        let erased: Vec<Arc<dyn Index + Send + Sync>> = vec![];
        finalize_indexes("TestSink", erased.iter())
            .await
            .expect("an empty index set cannot fail");
    }
}
