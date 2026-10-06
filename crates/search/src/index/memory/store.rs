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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::{cast, filter_record_batch};
use arrow::row::{RowConverter, SortField};
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

        self.retain_rows(|stored| {
            // `None` marks a batch with no overlap, which is kept untouched.
            if !stored
                .keys
                .iter()
                .any(|key| delete_keys.contains(key.as_str()))
            {
                return Ok(None);
            }
            Ok(Some(
                stored
                    .keys
                    .iter()
                    .map(|key| Some(!delete_keys.contains(key.as_str())))
                    .collect(),
            ))
        })
    }

    /// Remove every stored row that agrees with a row of `members` on `group_columns` but whose
    /// formatted primary key is not in `member_keys` — the rest of each group `members` names,
    /// with the members themselves kept. The store half of
    /// [`spice_table::Index::delete_group_remainder`].
    ///
    /// Group membership is decided on the Arrow values of `group_columns`, compared through the
    /// row encoding after casting `members` to the stored types, so it holds for any key type
    /// and does not depend on how a composite key is formatted. All-or-nothing, like
    /// [`Self::delete_by_keys`].
    pub(crate) fn delete_group_remainder(
        &mut self,
        group_columns: &[String],
        members: &RecordBatch,
        member_keys: &HashSet<&str>,
    ) -> Result<(), DataFusionError> {
        if members.num_rows() == 0 || group_columns.is_empty() {
            return Ok(());
        }

        let stored_schema = Arc::clone(&self.stored_schema);
        let group_indices = group_columns
            .iter()
            .map(|name| stored_schema.index_of(name))
            .collect::<Result<Vec<_>, _>>()?;
        let converter = RowConverter::new(
            group_indices
                .iter()
                .map(|&i| SortField::new(stored_schema.field(i).data_type().clone()))
                .collect(),
        )?;

        let member_columns = group_columns
            .iter()
            .zip(&group_indices)
            .map(|(name, &i)| {
                let column = members.column_by_name(name).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "the rows handed to the memory vector index do not carry the key column '{name}' its entries are grouped by"
                    ))
                })?;
                let stored_type = stored_schema.field(i).data_type();
                if column.data_type() == stored_type {
                    Ok(Arc::clone(column))
                } else {
                    Ok(cast(column, stored_type)?)
                }
            })
            .collect::<Result<Vec<ArrayRef>, DataFusionError>>()?;
        let member_rows = converter.convert_columns(&member_columns)?;
        let groups: HashSet<_> = member_rows.iter().collect();

        self.retain_rows(|stored| {
            let stored_rows =
                converter.convert_columns(stored.batch.project(&group_indices)?.columns())?;
            let mask: BooleanArray = (0..stored.batch.num_rows())
                .map(|i| {
                    let doomed = groups.contains(&stored_rows.row(i))
                        && !stored
                            .keys
                            .get(i)
                            .is_some_and(|key| member_keys.contains(key.as_str()));
                    Some(!doomed)
                })
                .collect();
            Ok((mask.false_count() > 0).then_some(mask))
        })
    }

    /// Rebuild the write target from `keep`'s answer for each stored batch: `Ok(None)` leaves a
    /// batch untouched (zero-copy), `Ok(Some(mask))` keeps the rows the mask selects.
    ///
    /// Every batch is filtered before any is replaced. This store holds the only copy of the
    /// rows it is filtering, so a partially applied pass would lose the batches it had already
    /// consumed; on `Err` the store holds exactly the rows it held before the call.
    fn retain_rows(
        &mut self,
        mut keep: impl FnMut(&StoredBatch) -> Result<Option<BooleanArray>, DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let target = self.write_target();
        let mut filtered: Vec<Option<(RecordBatch, BooleanArray)>> =
            Vec::with_capacity(target.len());
        for stored in target.iter() {
            let Some(mask) = keep(stored)? else {
                filtered.push(None);
                continue;
            };
            let batch = filter_record_batch(&stored.batch, &mask)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            filtered.push(Some((batch, mask)));
        }

        // Every fallible step is done, so the store can be rebuilt without dropping rows — and
        // now that the batches are owned, the surviving keys move rather than clone.
        let mut retained = Vec::with_capacity(target.len());
        for (stored, filtered) in target.drain(..).zip(filtered) {
            let Some((batch, mask)) = filtered else {
                // No overlap — keep the batch untouched (zero-copy).
                retained.push(stored);
                continue;
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let keys = stored
                .keys
                .into_iter()
                .zip(mask.iter())
                .filter(|(_, kept)| *kept == Some(true))
                .map(|(key, _)| key)
                .collect();
            retained.push(StoredBatch { batch, keys });
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
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
        stored_ids_of(&store.batches)
    }

    /// The ids of `batches`, one inner `Vec` per batch.
    fn stored_ids_of(batches: &[StoredBatch]) -> Vec<Vec<i64>> {
        batches
            .iter()
            .map(|stored| {
                stored
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column 0 is the id column")
                    .values()
                    .to_vec()
            })
            .collect()
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
    fn a_group_remainder_delete_drops_the_named_groups_but_keeps_their_members() {
        let mut store = store_of(&[&[1, 2, 3], &[4, 5]]);

        // Groups 2 and 4 are named; "4" is a member, so only row 2 is the remainder.
        let member_keys: HashSet<&str> = ["4"].into_iter().collect();
        store
            .delete_group_remainder(&["id".to_string()], &batch(&[2, 4]), &member_keys)
            .expect("the group columns are in the stored schema");

        assert_eq!(stored_ids(&store), vec![vec![1, 3], vec![4, 5]]);
    }

    #[test]
    fn a_group_remainder_delete_casts_the_members_to_the_stored_key_type() {
        let mut store = store_of(&[&[1, 2, 3]]);

        // Members carry the key as Int32 where the store holds Int64.
        let narrow = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(arrow::array::Int32Array::from(vec![2]))],
        )
        .expect("valid member batch");
        store
            .delete_group_remainder(&["id".to_string()], &narrow, &HashSet::new())
            .expect("a member key type the stored type can be cast from is accepted");

        assert_eq!(stored_ids(&store), vec![vec![1, 3]]);
    }

    #[test]
    fn a_group_remainder_delete_of_no_members_leaves_the_store_alone() {
        let mut store = store_of(&[&[1, 2]]);

        store
            .delete_group_remainder(&["id".to_string()], &batch(&[]), &HashSet::new())
            .expect("deleting the remainder of no group succeeds");

        assert_eq!(stored_ids(&store), vec![vec![1, 2]]);
    }

    /// A group-remainder delete compares the group columns through a row encoding built from
    /// the stored schema, so a stored batch whose column at the group position has another type
    /// cannot be compared and fails the pass. Deliberately violates the invariant that every
    /// stored batch conforms to [`MemoryVectorStore::stored_schema`]: it is the only way to make
    /// that pass fail, and the point is what the store does when it does — the batches masked
    /// before the failure must survive it.
    #[test]
    fn a_failed_group_remainder_delete_leaves_every_stored_row_in_place() {
        let mut store = store_of(&[&[1, 2], &[3, 4]]);
        store.batches.push(StoredBatch {
            batch: RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)])),
                vec![Arc::new(StringArray::from(vec!["row-5", "row-6"]))],
            )
            .expect("valid batch"),
            keys: keys(&[5, 6]),
        });

        // Group 3 overlaps the second batch, so it is masked successfully before the third
        // batch's mismatched column fails the pass.
        store
            .delete_group_remainder(&["id".to_string()], &batch(&[3, 5]), &HashSet::new())
            .expect_err("a stored batch whose group column has another type cannot be compared");

        assert_eq!(
            store.batches.len(),
            3,
            "a delete that could not be applied must not remove any batch"
        );
        assert_eq!(
            stored_ids_of(&store.batches[..2]),
            vec![vec![1, 2], vec![3, 4]],
            "a delete that could not be applied must not remove any row"
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
