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

//! Arrow-native row store backing [`super::MemoryVectorIndex`].
//!
//! Rows are held as [`RecordBatch`]es conforming to a fixed stored schema
//! (primary-key fields + metadata fields + the embedding column). Each batch
//! keeps the formatted primary key of every row alongside it so upserts can
//! drop superseded rows with a single Arrow `filter` pass — zero-copy for
//! untouched rows.

use std::collections::{HashMap, HashSet};

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;

/// A stored batch and the formatted primary key of each of its rows.
///
/// `keys.len() == batch.num_rows()`; every key is valid (rows with null
/// primary keys are rejected before storage).
#[derive(Debug, Clone)]
struct StoredBatch {
    batch: RecordBatch,
    keys: Vec<String>,
}

/// In-memory, MemTable-equivalent store of indexed rows.
///
/// Memory growth is unbounded by design: the store is a caller-managed
/// building block, and eviction/compaction policy belongs to the caller.
#[derive(Debug)]
pub(crate) struct MemoryVectorStore {
    /// Primary-key fields + metadata fields + `{search_column}_embedding`,
    /// alphabetically sorted by name (the same order the index's `write()`
    /// output uses, as required by `VectorScanTableProvider`).
    pub(crate) stored_schema: SchemaRef,
    batches: Vec<StoredBatch>,
    /// Rows written since a replace window opened, held aside from [`Self::batches`].
    ///
    /// `None` outside a replace window, which is every append and every CDC write. While it
    /// is `Some`, writes land here and reads still see [`Self::batches`], so the wipe and the
    /// repopulation of a full refresh become visible together at
    /// [`Self::commit_replace_window`] rather than a searcher observing an empty index for
    /// the length of the refresh.
    replacement: Option<Vec<StoredBatch>>,
}

impl MemoryVectorStore {
    pub(crate) fn new(stored_schema: SchemaRef) -> Self {
        Self {
            stored_schema,
            batches: Vec::new(),
            replacement: None,
        }
    }

    /// Open a replace window: stage subsequent writes instead of adding them to the rows
    /// readers see.
    ///
    /// A replacing write reproduces the table's whole contents, so every row this store
    /// already holds is either re-sent inside the window or belongs to a row the source
    /// dropped. Discards anything staged by a window that was abandoned without either
    /// terminator running, so it cannot be swept into this one.
    pub(crate) fn begin_replace_window(&mut self) {
        self.replacement = Some(Vec::new());
    }

    /// Close a replace window by publishing what it staged, replacing the previous contents
    /// in one step. A no-op when no window is open — the terminators run after an append too.
    pub(crate) fn commit_replace_window(&mut self) {
        if let Some(staged) = self.replacement.take() {
            self.batches = staged;
        }
    }

    /// Close a replace window by discarding what it staged, leaving the previous contents
    /// readable. A no-op when no window is open.
    pub(crate) fn abandon_replace_window(&mut self) {
        self.replacement = None;
    }

    /// The batches a write acts on: the staged set inside a replace window, else the rows
    /// readers see.
    fn write_target(&mut self) -> &mut Vec<StoredBatch> {
        self.replacement.as_mut().unwrap_or(&mut self.batches)
    }

    /// Replace-on-rewrite insert: drops any stored row whose formatted primary
    /// key appears in `keys` or `evicted`, then appends the new batch. `keys`
    /// must be parallel to `batch` rows.
    ///
    /// `evicted` names keys this write could not index at all, so `batch` carries
    /// no row for them and only the delete applies. Deleting them here rather
    /// than in a separate call keeps the store's all-or-nothing delete over the
    /// whole set, so a failure leaves every row the store held before the call.
    pub(crate) fn upsert(
        &mut self,
        batch: RecordBatch,
        keys: Vec<String>,
        evicted: &[String],
    ) -> Result<(), DataFusionError> {
        debug_assert_eq!(batch.num_rows(), keys.len());

        // The deletes below clear what the store already held for these keys, but `batch`
        // is then pushed whole — so a key this batch carries twice would land twice and the
        // store would hold one key with a row per occurrence. That happens on the ordinary
        // CDC path: a change envelope holds every change the source produced in one poll,
        // so two updates to one row inside that window arrive as two rows of one batch. A
        // search would then answer from a row the table has already replaced (#13713).
        let (batch, keys) = keep_deciding_row_per_key(batch, keys)?;

        if evicted.is_empty() {
            self.delete_by_keys(&keys)?;
        } else {
            let mut all = keys.clone();
            all.extend_from_slice(evicted);
            self.delete_by_keys(&all)?;
        }
        if batch.num_rows() > 0 {
            self.write_target().push(StoredBatch { batch, keys });
        }
        Ok(())
    }

    /// Remove every stored row whose formatted primary key appears in `keys`.
    ///
    /// All-or-nothing: on `Err` the store still holds exactly the rows it held before the
    /// call, so a delete that cannot be applied does not take the rows it was filtering
    /// with it.
    pub(crate) fn delete_by_keys(&mut self, keys: &[String]) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }

        let delete_keys: HashSet<&str> = keys.iter().map(String::as_str).collect();

        // Filter every overlapping batch before touching the stored ones. This store holds
        // the only copy of the rows it is filtering, so a partially applied delete would
        // lose the batches it had already consumed. `None` marks a batch with no overlap.
        let target = self.write_target();
        let mut filtered: Vec<Option<RecordBatch>> = Vec::with_capacity(target.len());
        for stored in target.iter() {
            if !stored
                .keys
                .iter()
                .any(|key| delete_keys.contains(key.as_str()))
            {
                filtered.push(None);
                continue;
            }
            let mask: BooleanArray = stored
                .keys
                .iter()
                .map(|key| Some(!delete_keys.contains(key.as_str())))
                .collect();
            let kept = filter_record_batch(&stored.batch, &mask)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            filtered.push(Some(kept));
        }

        // Every fallible step is done, so the store can be rebuilt without dropping rows.
        let target = self.write_target();
        let mut retained = Vec::with_capacity(target.len());
        for (stored, filtered) in target.drain(..).zip(filtered) {
            let Some(batch) = filtered else {
                // No overlap — keep the batch untouched (zero-copy).
                retained.push(stored);
                continue;
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let kept_keys = stored
                .keys
                .into_iter()
                .filter(|key| !delete_keys.contains(key.as_str()))
                .collect::<Vec<_>>();
            retained.push(StoredBatch {
                batch,
                keys: kept_keys,
            });
        }
        *target = retained;
        Ok(())
    }

    /// Current contents as batches conforming to [`Self::stored_schema`].
    /// Cheap: Arrow buffers are shared, not copied.
    ///
    /// Always the published rows. Rows staged by an open replace window are deliberately
    /// invisible here: a query during a full refresh reads the previous contents rather than
    /// the partially rebuilt ones.
    pub(crate) fn batches(&self) -> Vec<RecordBatch> {
        self.batches.iter().map(|s| s.batch.clone()).collect()
    }
}

/// Reduce `batch` to one row per key: the last row each key occurs at, which is the row the
/// table resolved that key to. Returns its inputs untouched when every key is already
/// distinct, so an ordinary write copies nothing.
fn keep_deciding_row_per_key(
    batch: RecordBatch,
    keys: Vec<String>,
) -> Result<(RecordBatch, Vec<String>), DataFusionError> {
    let mut last: HashMap<&str, usize> = HashMap::with_capacity(keys.len());
    for (row, key) in keys.iter().enumerate() {
        last.insert(key.as_str(), row);
    }
    if last.len() == keys.len() {
        return Ok((batch, keys));
    }

    let decides: Vec<bool> = keys
        .iter()
        .enumerate()
        .map(|(row, key)| last.get(key.as_str()) == Some(&row))
        .collect();
    drop(last);

    let mask: BooleanArray = decides.iter().map(|&keep| Some(keep)).collect();
    let filtered = filter_record_batch(&batch, &mask)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let kept = keys
        .into_iter()
        .zip(&decides)
        .filter_map(|(key, &keep)| keep.then_some(key))
        .collect();
    Ok((filtered, kept))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::{MemoryVectorStore, RecordBatch, SchemaRef, StoredBatch};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
        ]))
    }

    fn batch(ids: &[i64]) -> RecordBatch {
        let bodies: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(bodies)),
            ],
        )
        .expect("test columns match the test schema")
    }

    fn keys(ids: &[i64]) -> Vec<String> {
        ids.iter().map(i64::to_string).collect()
    }

    /// A store holding one batch per slice, each with keys parallel to its rows.
    fn store_of(batches: &[&[i64]]) -> MemoryVectorStore {
        let mut store = MemoryVectorStore::new(schema());
        for ids in batches {
            store.batches.push(StoredBatch {
                batch: batch(ids),
                keys: keys(ids),
            });
        }
        store
    }

    /// The ids the store currently holds, one inner `Vec` per batch.
    fn stored_ids(store: &MemoryVectorStore) -> Vec<Vec<i64>> {
        store
            .batches()
            .iter()
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column 0 is the id column")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// A batch built with a distinct body per row, so which row survived is visible.
    fn batch_of(rows: &[(i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, body)| *body).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("test columns match the test schema")
    }

    /// The (id, body) pairs the store holds, flattened across its batches.
    fn stored_rows(store: &MemoryVectorStore) -> Vec<(i64, String)> {
        store
            .batches()
            .iter()
            .flat_map(|b| {
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column 0 is the id column");
                let bodies = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("column 1 is the body column");
                (0..b.num_rows())
                    .map(|r| (ids.value(r), bodies.value(r).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// One batch can carry the same key more than once: a change envelope holds every
    /// change the source produced in one poll. The store is keyed, so it must end up
    /// holding the row the table resolved that key to — its last. Regression test for
    /// #13713.
    #[test]
    fn a_key_a_batch_carries_twice_is_stored_at_its_last_row() {
        let mut store = MemoryVectorStore::new(schema());

        store
            .upsert(
                batch_of(&[(1, "first"), (2, "other"), (1, "last")]),
                keys(&[1, 2, 1]),
                &[],
            )
            .expect("the batch's keys are parallel to its rows");

        assert_eq!(
            stored_rows(&store),
            vec![(2, "other".to_string()), (1, "last".to_string())],
            "the key carried twice must be stored once, at the row that decides it"
        );
    }

    /// The control for the case above: with every key distinct, nothing is dropped.
    #[test]
    fn an_upsert_with_no_repeated_key_stores_every_row() {
        let mut store = MemoryVectorStore::new(schema());

        store
            .upsert(batch_of(&[(1, "a"), (2, "b")]), keys(&[1, 2]), &[])
            .expect("the batch's keys are parallel to its rows");

        assert_eq!(
            stored_rows(&store),
            vec![(1, "a".to_string()), (2, "b".to_string())]
        );
    }

    #[test]
    fn a_delete_removes_only_the_named_rows() {
        let mut store = store_of(&[&[1, 2, 3], &[4, 5]]);

        store
            .delete_by_keys(&keys(&[2, 4]))
            .expect("every batch's keys are parallel to its rows");

        assert_eq!(stored_ids(&store), vec![vec![1, 3], vec![5]]);
    }

    #[test]
    fn a_batch_a_delete_empties_is_dropped() {
        let mut store = store_of(&[&[1], &[2, 3]]);

        store
            .delete_by_keys(&keys(&[1]))
            .expect("every batch's keys are parallel to its rows");

        assert_eq!(
            stored_ids(&store),
            vec![vec![2, 3]],
            "a batch with no rows left should not be retained"
        );
    }

    #[test]
    fn an_empty_key_list_leaves_the_store_alone() {
        let mut store = store_of(&[&[1, 2]]);

        store
            .delete_by_keys(&[])
            .expect("deleting nothing succeeds");

        assert_eq!(stored_ids(&store), vec![vec![1, 2]]);
    }

    /// A store whose third batch carries one more key than it has rows, so the mask built
    /// from those keys is longer than the batch and `filter_record_batch` fails on it.
    ///
    /// Deliberately violates [`StoredBatch`]'s parallel-keys invariant: a length mismatch is
    /// the only way to make Arrow's filter fail, and the point is what the store does when
    /// it does — the first two batches must survive the failure of the third.
    fn store_whose_third_batch_cannot_be_filtered() -> MemoryVectorStore {
        let mut store = store_of(&[&[1, 2], &[3, 4]]);
        store.batches.push(StoredBatch {
            batch: batch(&[5, 6]),
            keys: keys(&[5, 6, 7]),
        });
        store
    }

    #[test]
    fn a_failed_delete_leaves_every_stored_row_in_place() {
        let mut store = store_whose_third_batch_cannot_be_filtered();

        // "3" overlaps the second batch, so it filters successfully before "5" reaches the
        // third and fails — the case that used to leave the store empty.
        store
            .delete_by_keys(&keys(&[3, 5]))
            .expect_err("a batch whose mask does not match its rows cannot be filtered");

        assert_eq!(
            stored_ids(&store),
            vec![vec![1, 2], vec![3, 4], vec![5, 6]],
            "a delete that could not be applied must not remove any row"
        );
    }

    #[test]
    fn a_failed_upsert_leaves_every_stored_row_in_place() {
        let mut store = store_whose_third_batch_cannot_be_filtered();

        // `upsert` deletes the superseded rows first, so it inherits the same failure.
        store
            .upsert(batch(&[5]), keys(&[5]), &[])
            .expect_err("a batch whose mask does not match its rows cannot be filtered");

        assert_eq!(
            stored_ids(&store),
            vec![vec![1, 2], vec![3, 4], vec![5, 6]],
            "an upsert that could not delete the rows it supersedes must not add its own"
        );
    }

    #[test]
    fn a_failed_delete_inside_a_replace_window_leaves_every_staged_row_in_place() {
        let mut store = store_of(&[&[1, 2]]);
        store.begin_replace_window();
        store
            .upsert(batch(&[3, 4]), keys(&[3, 4]), &[])
            .expect("staging a batch inside a replace window succeeds");
        store.write_target().push(StoredBatch {
            batch: batch(&[5, 6]),
            keys: keys(&[5, 6, 7]),
        });

        // "3" overlaps the first staged batch, so it filters successfully before "5" reaches
        // the second and fails.
        store
            .delete_by_keys(&keys(&[3, 5]))
            .expect_err("a batch whose mask does not match its rows cannot be filtered");

        assert_eq!(
            stored_ids(&store),
            vec![vec![1, 2]],
            "a failed delete inside a window must leave the published rows alone"
        );

        store.commit_replace_window();
        assert_eq!(
            stored_ids(&store),
            vec![vec![3, 4], vec![5, 6]],
            "a delete that could not be applied must not remove any staged row"
        );
    }
}
