/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Index from a key to every row that holds it.

use arrow::array::RecordBatch;
use parking_lot::RwLock;
use snafu::OptionExt;

use crate::extract::create_key_extractor;
use crate::{IndexOverflowSnafu, Result, RowLocation};

/// Index from a key hash to every row holding that key, for keys that may repeat.
///
/// [`HashIndex`](crate::HashIndex) keeps one row per key, so a lookup on a
/// repeated key would miss rows. Here entries are sorted by key hash, so all
/// rows of a key are adjacent and a lookup returns all of them. Rows are hashed
/// with the same extractor as `HashIndex`, and as with it, callers must verify
/// the key of each returned row to rule out hash collisions.
#[derive(Debug)]
pub struct MultiHashIndex {
    key_columns: Vec<String>,
    entries: RwLock<Vec<(u64, RowLocation)>>,
}

impl MultiHashIndex {
    /// Creates an empty index on `key_columns`.
    #[must_use]
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            key_columns,
            entries: RwLock::new(Vec::new()),
        }
    }

    /// Returns the number of indexed rows. Rows with a null key are not indexed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns true if no rows are indexed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the location of every row whose key hashes to `hash`, in storage order.
    #[must_use]
    pub fn get_by_hash(&self, hash: u64) -> Vec<RowLocation> {
        let entries = self.entries.read();
        let start = entries.partition_point(|(h, _)| *h < hash);
        entries[start..]
            .iter()
            .take_while(|(h, _)| *h == hash)
            .map(|(_, location)| *location)
            .collect()
    }

    /// Replaces the index with one built from `partitions`.
    ///
    /// # Errors
    ///
    /// Returns an error if key extraction fails, or if an index exceeds `u32::MAX`.
    pub fn rebuild(&self, partitions: &[Vec<RecordBatch>]) -> Result<()> {
        let mut entries = Vec::new();
        for (partition, batches) in partitions.iter().enumerate() {
            let partition = to_u32("partition", partition)?;
            for (batch_idx, batch) in batches.iter().enumerate() {
                if batch.num_rows() == 0 {
                    continue;
                }
                let batch_idx = to_u32("batch", batch_idx)?;
                let extractor = create_key_extractor(batch, &self.key_columns)?;
                for row in 0..extractor.len() {
                    if let Some(hash) = extractor.hash_key(row) {
                        let location = RowLocation::new(partition, batch_idx, to_u32("row", row)?);
                        entries.push((hash, location));
                    }
                }
            }
        }
        // Location breaks ties, so the rows of a key are returned in storage order.
        entries.sort_unstable_by_key(|(hash, l)| (*hash, l.partition, l.batch, l.row));
        *self.entries.write() = entries;
        Ok(())
    }
}

fn to_u32(context: &'static str, value: usize) -> Result<u32> {
    u32::try_from(value)
        .ok()
        .context(IndexOverflowSnafu { context, value })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::hash_key;

    fn batch(ids: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(ids))]).expect("valid batch")
    }

    #[test]
    fn returns_every_row_of_a_repeated_key() {
        let index = MultiHashIndex::new(vec!["id".to_string()]);
        let partitions = vec![
            vec![batch(vec![Some("a"), Some("b"), None])],
            vec![batch(vec![Some("a")]), batch(vec![Some("c"), Some("a")])],
        ];
        index.rebuild(&partitions).expect("rebuild");

        assert_eq!(index.len(), 5, "null keys are not indexed");
        assert_eq!(
            index.get_by_hash(hash_key(&"a")),
            vec![
                RowLocation::new(0, 0, 0),
                RowLocation::new(1, 0, 0),
                RowLocation::new(1, 1, 1),
            ]
        );
        assert_eq!(
            index.get_by_hash(hash_key(&"b")),
            vec![RowLocation::new(0, 0, 1)]
        );
        assert!(index.get_by_hash(hash_key(&"missing")).is_empty());
    }

    #[test]
    fn rebuild_replaces_previous_entries() {
        let index = MultiHashIndex::new(vec!["id".to_string()]);
        index
            .rebuild(&[vec![batch(vec![Some("a"), Some("a")])]])
            .expect("rebuild");
        index
            .rebuild(&[vec![batch(vec![Some("b")])]])
            .expect("rebuild");

        assert!(index.get_by_hash(hash_key(&"a")).is_empty());
        assert_eq!(index.get_by_hash(hash_key(&"b")).len(), 1);
    }
}
