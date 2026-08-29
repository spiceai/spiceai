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

//! Last-write-wins coalescing of the rows a single index write is given.
//!
//! A write batch can carry more than one change for the same primary key: a CDC change envelope
//! holds every change the source produced in one poll, so two updates to one row inside that
//! window arrive as two rows of one batch. The accelerated table resolves such a key to the last
//! change; an index that writes every row does not, and then disagrees with the table it indexes.
//!
//! The disagreement takes a different shape per index but has one cause, so the repair is applied
//! once here and shared by every [`crate::index::SearchIndex`] that maintains a store keyed by
//! primary key:
//!
//! - a store whose upsert deletes the batch's keys and appends the batch keeps *both* rows, so one
//!   primary key ends up with two entries;
//! - a chunked index chunks each row independently, so the superseded text contributes chunks the
//!   winning text never overwrites — the row stays searchable by words it no longer contains.
//!
//! Row identity is the primary key's [`arrow::row`] encoding of the declared
//! [`crate::index::SearchIndex::primary_fields`], which is the same notion of identity the stores'
//! upsert and `delete_by_keys` paths already assume. A row whose key is NULL has no identity to
//! resolve — SQL never treats NULL as equal to NULL — so it is never coalesced with anything.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, RecordBatch, UInt32Array},
    compute::take_record_batch,
    row::{Row, RowConverter, SortField},
};
use arrow_schema::Field;
use datafusion::error::DataFusionError;

/// Coordinates of a row within a slice of batches: `(batch, row)`.
type RowRef = (usize, usize);

/// For every row of every batch, the coordinates of the row that supersedes it — itself when it
/// is the last change for its key.
type Winners = Vec<Vec<RowRef>>;

fn as_u32(index: usize) -> Result<u32, DataFusionError> {
    u32::try_from(index).map_err(|_| {
        DataFusionError::Execution(format!(
            "Row index {index} does not fit in a 32-bit take index"
        ))
    })
}

/// The primary-key columns of every batch, or `None` when there is no key to resolve rows by:
/// no declared primary key, or a batch that does not carry one of its columns.
fn primary_key_columns(
    primary_key: &[Field],
    batches: &[RecordBatch],
) -> Option<Vec<Vec<ArrayRef>>> {
    if primary_key.is_empty() {
        return None;
    }
    batches
        .iter()
        .map(|batch| {
            primary_key
                .iter()
                .map(|field| batch.column_by_name(field.name()).map(Arc::clone))
                .collect::<Option<Vec<_>>>()
        })
        .collect()
}

/// Resolve every row to the last change carrying the same primary key.
///
/// `None` when nothing repeats, so the common case pays only for the key encoding and callers
/// can hand the batches on untouched.
fn winners(
    primary_key: &[Field],
    batches: &[RecordBatch],
) -> Result<Option<Winners>, DataFusionError> {
    let Some(key_columns) = primary_key_columns(primary_key, batches) else {
        return Ok(None);
    };
    if batches.iter().map(RecordBatch::num_rows).sum::<usize>() < 2 {
        return Ok(None);
    }

    // The encoding is only ever compared for equality, so the sort options are irrelevant; the
    // fields come from the batch rather than the declared key so a widened column still encodes.
    let Some(first) = key_columns.first() else {
        return Ok(None);
    };
    let converter = RowConverter::new(
        first
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect(),
    )
    .map_err(|e| {
        DataFusionError::Execution(format!(
            "Cannot resolve rows by primary key ({}): {e}",
            primary_key
                .iter()
                .map(Field::name)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;

    let mut encoded = Vec::with_capacity(key_columns.len());
    for columns in &key_columns {
        encoded.push(
            converter
                .convert_columns(columns)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        );
    }

    // A NULL anywhere in the key leaves the row without an identity to resolve, so it stands
    // alone rather than joining a group.
    let unresolvable: Vec<Vec<bool>> = key_columns
        .iter()
        .zip(batches)
        .map(|(columns, batch)| {
            if columns.iter().all(|c| c.null_count() == 0) {
                return vec![false; batch.num_rows()];
            }
            (0..batch.num_rows())
                .map(|row| columns.iter().any(|c| c.is_null(row)))
                .collect()
        })
        .collect();

    let mut last: HashMap<Row<'_>, RowRef> = HashMap::new();
    for (b, rows) in encoded.iter().enumerate() {
        for row in 0..rows.num_rows() {
            if unresolvable[b][row] {
                continue;
            }
            last.insert(rows.row(row), (b, row));
        }
    }

    let mut winners: Winners = Vec::with_capacity(encoded.len());
    let mut superseded = false;
    for (b, rows) in encoded.iter().enumerate() {
        let mut batch_winners = Vec::with_capacity(rows.num_rows());
        for row in 0..rows.num_rows() {
            let winner = if unresolvable[b][row] {
                (b, row)
            } else {
                *last.get(&rows.row(row)).unwrap_or(&(b, row))
            };
            superseded |= winner != (b, row);
            batch_winners.push(winner);
        }
        winners.push(batch_winners);
    }

    Ok(superseded.then_some(winners))
}

/// Reduce `batches` to one row per primary key — the last change for that key across the whole
/// slice — dropping any batch left with no rows.
///
/// `None` when no key repeats. For a caller that indexes the batches as a set and returns its
/// input unchanged (there is no per-row output to map back), which is why this has no counterpart
/// to [`LastWriteWins::expand`].
pub(crate) fn reduce_batches(
    primary_key: &[Field],
    batches: &[RecordBatch],
) -> Result<Option<Vec<RecordBatch>>, DataFusionError> {
    let Some(winners) = winners(primary_key, batches)? else {
        return Ok(None);
    };

    let mut reduced = Vec::with_capacity(batches.len());
    for (b, (batch, batch_winners)) in batches.iter().zip(&winners).enumerate() {
        let mut keep = Vec::new();
        for (row, winner) in batch_winners.iter().enumerate() {
            if *winner == (b, row) {
                keep.push(as_u32(row)?);
            }
        }
        if keep.is_empty() {
            continue;
        }
        reduced.push(
            take_record_batch(batch, &UInt32Array::from(keep))
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        );
    }
    Ok(Some(reduced))
}

/// One batch's rows reduced to the last change per primary key, and the map back to the batch's
/// original shape.
struct LastWriteWins {
    keep: UInt32Array,
    expand: UInt32Array,
}

impl LastWriteWins {
    /// `None` when no key repeats within `record`.
    fn plan(primary_key: &[Field], record: &RecordBatch) -> Result<Option<Self>, DataFusionError> {
        let Some(winners) = winners(primary_key, std::slice::from_ref(record))? else {
            return Ok(None);
        };
        let Some(batch_winners) = winners.first() else {
            return Ok(None);
        };

        let mut keep = Vec::new();
        let mut position = vec![u32::MAX; batch_winners.len()];
        for (row, winner) in batch_winners.iter().enumerate() {
            if *winner == (0, row) {
                position[row] = as_u32(keep.len())?;
                keep.push(as_u32(row)?);
            }
        }

        let mut expand = Vec::with_capacity(batch_winners.len());
        for (_, winner_row) in batch_winners {
            // Every winner kept itself in the loop above, so its position is assigned.
            expand.push(position[*winner_row]);
        }

        Ok(Some(Self {
            keep: UInt32Array::from(keep),
            expand: UInt32Array::from(expand),
        }))
    }

    fn reduce(&self, record: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        take_record_batch(record, &self.keep)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn expand(&self, written: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        if written.num_rows() != self.keep.len() {
            return Err(DataFusionError::Execution(format!(
                "An index write returned {} rows for the {} rows it was given, so its output \
                 cannot be mapped back onto the batch it came from",
                written.num_rows(),
                self.keep.len()
            )));
        }
        take_record_batch(written, &self.expand)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Write `record` to an index, indexing only the last change carried for each primary key.
///
/// `write` sees one row per key; the batch handed back has `record`'s original rows in their
/// original order, each carrying the columns the write derived for the change that supersedes it.
/// A superseded row's own derived values are never observable — the accelerated table resolves
/// that key to the same winning change — so taking the winner's is what keeps the two agreeing.
pub(crate) async fn write_last_write_wins<F, Fut>(
    primary_key: &[Field],
    record: RecordBatch,
    write: F,
) -> Result<RecordBatch, DataFusionError>
where
    F: FnOnce(RecordBatch) -> Fut,
    Fut: Future<Output = Result<RecordBatch, DataFusionError>>,
{
    let Some(plan) = LastWriteWins::plan(primary_key, &record)? else {
        return write(record).await;
    };
    let written = write(plan.reduce(&record)?).await?;
    plan.expand(&written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Schema};

    fn pk() -> Vec<Field> {
        vec![Field::new("id", DataType::Int64, false)]
    }

    /// `id` nullable so a NULL key can be expressed; `content` carries the change.
    fn batch(rows: &[(Option<i64>, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("content", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, c)| *c).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("valid batch")
    }

    fn contents(record: &RecordBatch) -> Vec<Option<String>> {
        record
            .column_by_name("content")
            .expect("content column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("content is Utf8")
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    async fn written_and_returned(
        primary_key: &[Field],
        record: RecordBatch,
    ) -> (Vec<Option<String>>, Vec<Option<String>>) {
        let seen = std::sync::Mutex::new(Vec::new());
        let returned = write_last_write_wins(primary_key, record, |b| async {
            seen.lock().expect("mutex").extend(contents(&b));
            Ok(b)
        })
        .await
        .expect("write succeeds");
        let seen = seen.lock().expect("mutex").clone();
        (seen, contents(&returned))
    }

    #[tokio::test]
    async fn a_repeated_key_is_written_once_as_its_last_change() {
        let (written, returned) = written_and_returned(
            &pk(),
            batch(&[(Some(1), "aaa"), (Some(2), "bbb"), (Some(1), "ccc")]),
        )
        .await;

        assert_eq!(
            written,
            vec![Some("bbb".to_string()), Some("ccc".to_string())],
            "the index must see one row per key, carrying the last change for that key"
        );
        // The batch handed back keeps every input row so the caller's row-for-row contract with
        // the change batch still holds; a superseded row carries its winner's derived values.
        assert_eq!(
            returned,
            vec![
                Some("ccc".to_string()),
                Some("bbb".to_string()),
                Some("ccc".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn a_batch_with_no_repeated_key_is_handed_over_untouched() {
        let (written, returned) =
            written_and_returned(&pk(), batch(&[(Some(1), "aaa"), (Some(2), "bbb")])).await;

        assert_eq!(
            written,
            vec![Some("aaa".to_string()), Some("bbb".to_string())]
        );
        assert_eq!(
            returned,
            vec![Some("aaa".to_string()), Some("bbb".to_string())]
        );
    }

    /// The last change for a key is what the index must hold even when it is the shorter or
    /// empty one — the NULL case in #13713, where the superseded text otherwise stays searchable.
    #[tokio::test]
    async fn a_key_superseded_by_a_null_search_value_is_written_as_the_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        let record = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 1])),
                Arc::new(StringArray::from(vec![Some("value"), None])),
            ],
        )
        .expect("valid batch");

        let (written, _) = written_and_returned(&pk(), record).await;
        assert_eq!(written, vec![None]);
    }

    /// SQL never treats NULL as equal to NULL, so two NULL-keyed rows are two rows.
    #[tokio::test]
    async fn null_keys_are_not_coalesced_with_each_other() {
        let (written, _) = written_and_returned(
            &pk(),
            batch(&[(None, "aaa"), (None, "bbb"), (Some(1), "ccc")]),
        )
        .await;

        assert_eq!(
            written,
            vec![
                Some("aaa".to_string()),
                Some("bbb".to_string()),
                Some("ccc".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn an_index_with_no_declared_primary_key_writes_every_row() {
        let (written, _) =
            written_and_returned(&[], batch(&[(Some(1), "aaa"), (Some(1), "ccc")])).await;

        assert_eq!(
            written,
            vec![Some("aaa".to_string()), Some("ccc".to_string())]
        );
    }

    /// A key column the batch does not carry leaves nothing to resolve rows by, so the write is
    /// handed the batch as it stands rather than failing it.
    #[tokio::test]
    async fn a_primary_key_column_missing_from_the_batch_writes_every_row() {
        let absent = vec![Field::new("missing", DataType::Int64, false)];
        let (written, _) =
            written_and_returned(&absent, batch(&[(Some(1), "aaa"), (Some(1), "ccc")])).await;

        assert_eq!(
            written,
            vec![Some("aaa".to_string()), Some("ccc".to_string())]
        );
    }

    #[tokio::test]
    async fn a_multi_column_key_resolves_on_the_whole_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        let record = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Int64Array::from(vec![1_i64, 1, 1])),
                Arc::new(StringArray::from(vec!["first", "other tenant", "last"])),
            ],
        )
        .expect("valid batch");

        let key = vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ];
        let (written, _) = written_and_returned(&key, record).await;
        assert_eq!(
            written,
            vec![Some("other tenant".to_string()), Some("last".to_string())],
            "(a, 1) is superseded but (b, 1) is a different key"
        );
    }

    /// The mapping back is by key, not by position: an index that reorders nothing still has to
    /// survive the reduced batch being shorter than the one the caller handed in.
    #[tokio::test]
    async fn a_write_returning_the_wrong_row_count_is_refused() {
        let record = batch(&[(Some(1), "aaa"), (Some(1), "ccc")]);
        let err = write_last_write_wins(&pk(), record, |b| async move { Ok(b.slice(0, 0)) })
            .await
            .expect_err("a row count the map cannot be applied to must fail the write");
        assert!(
            err.to_string().contains("cannot be mapped back"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reduce_batches_resolves_a_key_repeated_across_batches() {
        let batches = vec![
            batch(&[(Some(1), "aaa"), (Some(2), "bbb")]),
            batch(&[(Some(1), "ccc")]),
        ];
        let reduced = reduce_batches(&pk(), &batches)
            .expect("reduce succeeds")
            .expect("a repeated key must reduce");

        assert_eq!(
            reduced.iter().flat_map(contents).collect::<Vec<_>>(),
            vec![Some("bbb".to_string()), Some("ccc".to_string())]
        );
    }

    /// A batch every one of whose rows is superseded later carries nothing to index.
    #[test]
    fn reduce_batches_drops_a_batch_left_with_no_rows() {
        let batches = vec![batch(&[(Some(1), "aaa")]), batch(&[(Some(1), "ccc")])];
        let reduced = reduce_batches(&pk(), &batches)
            .expect("reduce succeeds")
            .expect("a repeated key must reduce");

        assert_eq!(reduced.len(), 1);
        assert_eq!(contents(&reduced[0]), vec![Some("ccc".to_string())]);
    }

    #[test]
    fn reduce_batches_reports_nothing_to_do_when_no_key_repeats() {
        let batches = vec![batch(&[(Some(1), "aaa")]), batch(&[(Some(2), "bbb")])];
        assert!(
            reduce_batches(&pk(), &batches)
                .expect("reduce succeeds")
                .is_none()
        );
    }
}
