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

//! Construction of resident sorted keys and shared Arrow payload pages.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::alloc::Allocation;
use arrow::array::{Array, ArrayData, ArrayRef, RecordBatch, RecordBatchOptions, make_array};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};

use super::super::memory_account::{CayenneMemoryAccount, LookupIndexReservation};
use super::{
    AllocationOwner, CoveredRowRef, EncodedKey, Error, IndexDefinition, IndexRun, IndexedSource,
    KeyDirectory, KeyDirectoryEntry, KeyPage, KeyPageId, KeyPageLease, MemoryPageStore, PageLease,
    PayloadPage, PayloadPageId, PayloadPageLease, ReservationToken, Result, RunId, SourceId,
};

/// Target retained Arrow bytes in one payload page.
const PAYLOAD_PAGE_TARGET_BYTES: usize = 64 * 1024;
/// Maximum number of source rows in one payload page.
const PAYLOAD_PAGE_MAX_ROWS: usize = 1024;
/// Target encoded bytes in one sorted key page.
const KEY_PAGE_TARGET_BYTES: usize = 32 * 1024;
/// Maximum entries in one sorted key page.
const KEY_PAGE_MAX_ENTRIES: usize = 256;
/// Maximum entries sorted in one independently bounded run.
const SORT_RUN_MAX_ENTRIES: usize = 65_536;

/// A complete, unpublished resident representation of one immutable source.
///
/// The store owns payload pages once, while every run owns only key navigation
/// pages and row references into those shared values. A caller publishes all
/// three components together or drops this value, never a partial source.
#[derive(Debug)]
pub(crate) struct BuiltCoveredSource {
    source: IndexedSource,
    runs: Arc<[IndexRun]>,
    page_store: Arc<MemoryPageStore>,
}

impl BuiltCoveredSource {
    /// Source metadata proved against all constructed payload pages.
    #[must_use]
    pub(crate) fn source(&self) -> &IndexedSource {
        &self.source
    }

    /// Complete sorted runs, including an explicit empty run where necessary.
    #[must_use]
    pub(crate) fn runs(&self) -> &[IndexRun] {
        &self.runs
    }

    /// Store owning every referenced key and payload page.
    #[must_use]
    pub(crate) fn page_store(&self) -> &Arc<MemoryPageStore> {
        &self.page_store
    }

    /// Split this unpublished source into the atomically publishable pieces.
    #[must_use]
    pub(crate) fn into_parts(self) -> (IndexedSource, Arc<[IndexRun]>, Arc<MemoryPageStore>) {
        (self.source, self.runs, self.page_store)
    }
}

#[derive(Debug)]
struct BuildEntry {
    key: EncodedKey,
    row_ref: CoveredRowRef,
}

/// Build payload pages once and all sorted key pages for one index definition.
///
/// Construction runs on the bounded Rayon pool. It is optional: any admission,
/// Arrow ownership, or schema failure drops every unpublished allocation and
/// returns a typed error for the caller to turn into unavailable coverage.
pub(crate) async fn build_source(
    source: SourceId,
    definition: IndexDefinition,
    batches: Vec<RecordBatch>,
    account: Arc<CayenneMemoryAccount>,
) -> Result<BuiltCoveredSource> {
    super::CayenneIndexExecutor::shared()?
        .execute(move || build_source_sync(source, &definition, batches, &account))
        .await
}

/// Build several index definitions over one source without copying payloads.
///
/// Each returned store has independent key pages and directories but clones the
/// same immutable payload leases, so Arrow allocation charges remain owned once
/// by their buffer-backed allocation owners.
pub(crate) async fn build_sources(
    source: SourceId,
    definitions: Vec<IndexDefinition>,
    batches: Vec<RecordBatch>,
    account: Arc<CayenneMemoryAccount>,
) -> Result<Vec<BuiltCoveredSource>> {
    let (first, rest) = definitions
        .split_first()
        .ok_or_else(|| Error::InvalidContract {
            message: "covering source build requires at least one index definition".to_string(),
        })?;
    let first_built = build_source(source, first.clone(), batches, Arc::clone(&account)).await?;
    let mut built = Vec::with_capacity(definitions.len());
    for definition in rest {
        if !definition.schema().matches(first_built.source().schema()) {
            return Err(Error::InvalidContract {
                message: "covering indexes over one source require the exact same schema"
                    .to_string(),
            });
        }
        let source = first_built.source().clone();
        let payload_leases = first_built.page_store().payload_page_leases();
        let definition = definition.clone();
        let account = Arc::clone(&account);
        let additional = super::CayenneIndexExecutor::shared()?
            .execute(move || {
                build_index_over_shared_payload(source, &definition, payload_leases, &account)
            })
            .await?;
        built.push(additional);
    }
    built.insert(0, first_built);
    Ok(built)
}

fn build_source_sync(
    source: SourceId,
    definition: &IndexDefinition,
    batches: Vec<RecordBatch>,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<BuiltCoveredSource> {
    let schema = definition.schema().clone();
    let mut payload_pages = Vec::new();
    let mut payload_leases = BTreeMap::new();
    let mut entries = Vec::new();
    let mut row_count = 0usize;

    for batch in batches {
        if batch.schema_ref().as_ref() != schema.schema().as_ref() {
            return Err(Error::InvalidContract {
                message: "source batch schema differs from its exact captured schema".to_string(),
            });
        }

        let mut start = 0usize;
        while start < batch.num_rows() {
            let remaining = batch.num_rows().checked_sub(start).ok_or(Error::Overflow {
                operation: "payload-page remaining rows",
            })?;
            let (owned, consumed) = build_payload_batch(&batch, start, remaining, account)?;
            let page_number = u32::try_from(payload_pages.len()).map_err(|_| Error::Overflow {
                operation: "payload page identifier",
            })?;
            let page_id = PayloadPageId::new(source.clone(), page_number);
            let encoded = definition.encode_source_batch(&owned)?;
            if encoded.len() != owned.num_rows() {
                return Err(Error::InvalidContract {
                    message: "source key encoder returned a mismatched row count".to_string(),
                });
            }

            for (row_in_page, key) in encoded.into_iter().enumerate() {
                let ordinal = u64::try_from(row_count).map_err(|_| Error::Overflow {
                    operation: "source row ordinal",
                })?;
                row_count = row_count.checked_add(1).ok_or(Error::Overflow {
                    operation: "source row count",
                })?;
                if let Some(key) = key {
                    entries.push(BuildEntry {
                        key,
                        row_ref: CoveredRowRef::new(
                            source.clone(),
                            page_id.clone(),
                            row_in_page,
                            ordinal,
                        )?,
                    });
                }
            }

            let page = Arc::new(PayloadPage::new(schema.clone(), owned)?);
            let page_token = reserve_token(
                account,
                std::mem::size_of::<PayloadPage>()
                    .checked_add(std::mem::size_of::<PayloadPageId>())
                    .ok_or(Error::Overflow {
                        operation: "payload-page bookkeeping bytes",
                    })?,
                "payload-page bookkeeping",
            )?;
            payload_leases.insert(page_id.clone(), PageLease::new(page, page_token));
            payload_pages.push(page_id);
            start = start.checked_add(consumed).ok_or(Error::Overflow {
                operation: "payload-page source offset",
            })?;
        }
    }

    let (key_leases, runs) = build_key_runs(&source, entries, account)?;
    let indexed_source = IndexedSource::new(source, schema, row_count, payload_pages)?;
    let store = Arc::new(MemoryPageStore::new(key_leases, payload_leases)?);
    validate_complete_source(&indexed_source, &runs, &store)?;
    Ok(BuiltCoveredSource {
        source: indexed_source,
        runs: runs.into(),
        page_store: store,
    })
}

fn build_index_over_shared_payload(
    source: IndexedSource,
    definition: &IndexDefinition,
    payload_leases: BTreeMap<PayloadPageId, PayloadPageLease>,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<BuiltCoveredSource> {
    if !source.schema().matches(definition.schema()) {
        return Err(Error::InvalidContract {
            message: "shared payload schema differs from the index definition".to_string(),
        });
    }
    let mut entries = Vec::new();
    let mut expected_ordinal = 0usize;
    for page_id in source.payload_pages() {
        let lease = payload_leases
            .get(page_id)
            .ok_or_else(|| Error::InvalidContract {
                message: format!("shared payload page {page_id:?} is absent from its store"),
            })?;
        let batch = lease.page().batch();
        let keys = definition.encode_source_batch(batch)?;
        for (row_in_page, key) in keys.into_iter().enumerate() {
            let ordinal = u64::try_from(expected_ordinal).map_err(|_| Error::Overflow {
                operation: "shared payload source row ordinal",
            })?;
            expected_ordinal = expected_ordinal.checked_add(1).ok_or(Error::Overflow {
                operation: "shared payload source row count",
            })?;
            if let Some(key) = key {
                entries.push(BuildEntry {
                    key,
                    row_ref: CoveredRowRef::new(
                        source.source().clone(),
                        page_id.clone(),
                        row_in_page,
                        ordinal,
                    )?,
                });
            }
        }
    }
    if expected_ordinal != source.row_count() {
        return Err(Error::InvalidContract {
            message: "shared payload row count differs from its source metadata".to_string(),
        });
    }
    let (key_leases, runs) = build_key_runs(source.source(), entries, account)?;
    let store = Arc::new(MemoryPageStore::new(key_leases, payload_leases)?);
    validate_complete_source(&source, &runs, &store)?;
    Ok(BuiltCoveredSource {
        source,
        runs: runs.into(),
        page_store: store,
    })
}

/// Compact and independently own the largest page that fits the row/byte limit.
fn build_payload_batch(
    batch: &RecordBatch,
    start: usize,
    remaining: usize,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<(RecordBatch, usize)> {
    let mut rows = remaining.min(PAYLOAD_PAGE_MAX_ROWS);
    loop {
        let slice = batch.slice(start, rows);
        // The temporary reservation is held over the copy and final ownership
        // transfer. This is deliberately conservative: a sliced parent can be
        // much larger than the values retained by its child page.
        let scratch = reserve_token(
            account,
            retained_buffer_bytes(&slice)?,
            "payload-page compaction scratch",
        )?;
        let compacted = arrow_tools::record_batch::compact_retained_buffers(&slice);
        if arrow_tools::record_batch::rests_on_unowned_memory(&compacted) {
            return Err(Error::Unavailable {
                operation: "payload page still retains unowned Arrow memory after compaction"
                    .to_string(),
            });
        }
        let bytes = retained_buffer_bytes(&compacted)?;
        if bytes <= PAYLOAD_PAGE_TARGET_BYTES || rows == 1 {
            let owned = own_batch_buffers(&compacted, account)?;
            drop(scratch);
            return Ok((owned, rows));
        }
        drop(scratch);
        rows /= 2;
    }
}

fn build_key_runs(
    source: &SourceId,
    mut entries: Vec<BuildEntry>,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<(BTreeMap<KeyPageId, KeyPageLease>, Vec<IndexRun>)> {
    let mut pages = BTreeMap::new();
    let mut runs = Vec::new();
    let mut next_page = 0u32;

    if entries.is_empty() {
        let directory = KeyDirectory::new(source.clone(), Vec::new())?;
        let token = reserve_token(
            account,
            std::mem::size_of::<KeyDirectory>(),
            "empty key directory",
        )?;
        runs.push(IndexRun::new(source.clone(), RunId::new(0), directory)?.with_metadata(0, token));
        return Ok((pages, runs));
    }

    for (run_index, unsorted) in entries.chunks_mut(SORT_RUN_MAX_ENTRIES).enumerate() {
        unsorted.sort_unstable_by(|left, right| {
            left.key.cmp(&right.key).then_with(|| {
                left.row_ref
                    .source_row_ordinal()
                    .cmp(&right.row_ref.source_row_ordinal())
            })
        });
        let max_duplicates = max_duplicate_key_count(unsorted)?;
        let mut directory_entries = Vec::new();
        let mut page_start = 0usize;
        while page_start < unsorted.len() {
            let mut page_end = page_start;
            let mut key_bytes = 0usize;
            while page_end < unsorted.len() && page_end - page_start < KEY_PAGE_MAX_ENTRIES {
                let next_len = unsorted[page_end].key.as_bytes().len();
                let next_total = key_bytes.checked_add(next_len).ok_or(Error::Overflow {
                    operation: "key-page encoded byte count",
                })?;
                if page_end > page_start && next_total > KEY_PAGE_TARGET_BYTES {
                    break;
                }
                key_bytes = next_total;
                page_end = page_end.checked_add(1).ok_or(Error::Overflow {
                    operation: "key-page entry count",
                })?;
            }
            let page_id = KeyPageId::new(source.clone(), next_page);
            next_page = next_page.checked_add(1).ok_or(Error::Overflow {
                operation: "key page identifier",
            })?;
            let page = build_key_page(&unsorted[page_start..page_end])?;
            let first = EncodedKey::from_page_bytes(page.key(0)?);
            let last_index = page.len().checked_sub(1).ok_or(Error::InvalidContract {
                message: "constructed key page is empty".to_string(),
            })?;
            let last = EncodedKey::from_page_bytes(page.key(last_index)?);
            let entry_count = page.len();
            let token = reserve_token(account, page.retained_bytes()?, "key-page bytes")?;
            pages.insert(page_id.clone(), PageLease::new(Arc::new(page), token));
            directory_entries.push(KeyDirectoryEntry::new(page_id, first, last, entry_count)?);
            page_start = page_end;
        }

        let directory_bytes = directory_retained_bytes(&directory_entries)?;
        let directory = KeyDirectory::new(source.clone(), directory_entries)?;
        let token = reserve_token(account, directory_bytes, "key-directory bytes")?;
        let run = u32::try_from(run_index).map_err(|_| Error::Overflow {
            operation: "sorted run identifier",
        })?;
        runs.push(
            IndexRun::new(source.clone(), RunId::new(run), directory)?
                .with_metadata(max_duplicates, token),
        );
    }
    Ok((pages, runs))
}

fn build_key_page(entries: &[BuildEntry]) -> Result<KeyPage> {
    if entries.is_empty() {
        return Err(Error::InvalidContract {
            message: "cannot construct an empty key page".to_string(),
        });
    }
    let key_bytes = entries.iter().try_fold(0usize, |total, entry| {
        total
            .checked_add(entry.key.as_bytes().len())
            .ok_or(Error::Overflow {
                operation: "key-page encoded allocation",
            })
    })?;
    let mut encoded = Vec::with_capacity(key_bytes);
    let mut offsets = Vec::with_capacity(entries.len().checked_add(1).ok_or(Error::Overflow {
        operation: "key-page offset allocation",
    })?);
    let mut row_refs = Vec::with_capacity(entries.len());
    offsets.push(0);
    for entry in entries {
        encoded.extend_from_slice(entry.key.as_bytes());
        offsets.push(encoded.len());
        row_refs.push(entry.row_ref.clone());
    }
    KeyPage::new(encoded.into(), offsets, row_refs)
}

fn max_duplicate_key_count(entries: &[BuildEntry]) -> Result<usize> {
    let mut maximum = 0usize;
    let mut current = 0usize;
    let mut prior: Option<&EncodedKey> = None;
    for entry in entries {
        if prior == Some(&entry.key) {
            current = current.checked_add(1).ok_or(Error::Overflow {
                operation: "duplicate-key multiplicity",
            })?;
        } else {
            current = 1;
            prior = Some(&entry.key);
        }
        maximum = maximum.max(current);
    }
    Ok(maximum)
}

fn directory_retained_bytes(entries: &[KeyDirectoryEntry]) -> Result<usize> {
    let entry_bytes = entries
        .len()
        .checked_mul(std::mem::size_of::<KeyDirectoryEntry>())
        .ok_or(Error::Overflow {
            operation: "key-directory entry bytes",
        })?;
    entries.iter().try_fold(entry_bytes, |total, entry| {
        total
            .checked_add(entry.first_key().as_bytes().len())
            .and_then(|bytes| bytes.checked_add(entry.last_key().as_bytes().len()))
            .ok_or(Error::Overflow {
                operation: "key-directory bound bytes",
            })
    })
}

fn validate_complete_source(
    source: &IndexedSource,
    runs: &[IndexRun],
    store: &MemoryPageStore,
) -> Result<()> {
    let mut total_entries = 0usize;
    for run in runs {
        if run.source() != source.source() {
            return Err(Error::InvalidContract {
                message: "constructed run belongs to a different source".to_string(),
            });
        }
        for directory in run.directory().entries() {
            if !store.contains_key_page(directory.page()) {
                return Err(Error::InvalidContract {
                    message: format!(
                        "constructed directory references missing page {:?}",
                        directory.page()
                    ),
                });
            }
            total_entries =
                total_entries
                    .checked_add(directory.entry_count())
                    .ok_or(Error::Overflow {
                        operation: "constructed key entry count",
                    })?;
        }
    }
    if total_entries > source.row_count() {
        return Err(Error::InvalidContract {
            message: "constructed key entries exceed source row count".to_string(),
        });
    }
    if source.row_count() > 0 && runs.is_empty() {
        return Err(Error::InvalidContract {
            message: "nonempty source has no complete key run".to_string(),
        });
    }
    Ok(())
}

fn reserve_token(
    account: &Arc<CayenneMemoryAccount>,
    bytes: usize,
    operation: &str,
) -> Result<ReservationToken> {
    let reservation =
        account
            .try_reserve_lookup_index(bytes)
            .ok_or_else(|| Error::Unavailable {
                operation: format!("unable to admit {bytes} bytes for {operation}"),
            })?;
    Ok(AllocationOwner::new(reservation).token())
}

/// Allocation owner for one immutable Arrow buffer.
///
/// Field order releases the retained backing buffer before its reservation.
/// It carries no page reference, so result arrays retaining this allocation
/// cannot create a page/lease ownership cycle.
#[derive(Debug)]
struct BufferAllocation {
    _backing: Buffer,
    _reservation: LookupIndexReservation,
}

impl std::panic::RefUnwindSafe for BufferAllocation {}

fn own_batch_buffers(
    batch: &RecordBatch,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<RecordBatch> {
    let mut allocations = HashMap::new();
    let columns = batch
        .columns()
        .iter()
        .map(|array| own_array_buffers(array, account, &mut allocations))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|source| Error::Arrow { source })
}

fn own_array_buffers(
    array: &ArrayRef,
    account: &Arc<CayenneMemoryAccount>,
    allocations: &mut HashMap<(usize, usize), Buffer>,
) -> Result<ArrayRef> {
    Ok(make_array(own_array_data(
        &array.to_data(),
        account,
        allocations,
    )?))
}

fn own_array_data(
    data: &ArrayData,
    account: &Arc<CayenneMemoryAccount>,
    allocations: &mut HashMap<(usize, usize), Buffer>,
) -> Result<ArrayData> {
    let buffers = data
        .buffers()
        .iter()
        .map(|buffer| own_buffer(buffer, account, allocations))
        .collect::<Result<Vec<_>>>()?;
    let children = data
        .child_data()
        .iter()
        .map(|child| own_array_data(child, account, allocations))
        .collect::<Result<Vec<_>>>()?;
    let nulls = data
        .nulls()
        .map(|nulls| {
            let buffer = own_buffer(nulls.buffer(), account, allocations)?;
            Ok::<NullBuffer, Error>(NullBuffer::new(BooleanBuffer::new(
                buffer,
                nulls.inner().offset(),
                nulls.len(),
            )))
        })
        .transpose()?;
    ArrayData::builder(data.data_type().clone())
        .len(data.len())
        .offset(data.offset())
        .buffers(buffers)
        .child_data(children)
        .nulls(nulls)
        .build()
        .map_err(|source| Error::Arrow { source })
}

fn own_buffer(
    buffer: &Buffer,
    account: &Arc<CayenneMemoryAccount>,
    allocations: &mut HashMap<(usize, usize), Buffer>,
) -> Result<Buffer> {
    let capacity = buffer.capacity();
    if capacity == 0 {
        if buffer.is_empty() {
            return Ok(buffer.clone());
        }
        return Err(Error::Unavailable {
            operation: "Arrow buffer has no accountable owned capacity".to_string(),
        });
    }
    let offset = buffer.ptr_offset();
    let end = offset.checked_add(buffer.len()).ok_or(Error::Overflow {
        operation: "Arrow buffer slice bounds",
    })?;
    if end > capacity {
        return Err(Error::InvalidContract {
            message: "Arrow buffer slice exceeds its backing allocation".to_string(),
        });
    }
    let key = (buffer.data_ptr().as_ptr() as usize, capacity);
    let root = if let Some(existing) = allocations.get(&key) {
        existing.clone()
    } else {
        let reservation =
            account
                .try_reserve_lookup_index(capacity)
                .ok_or_else(|| Error::Unavailable {
                    operation: format!(
                        "unable to admit {capacity} bytes for an owned Arrow payload buffer"
                    ),
                })?;
        let allocation: Arc<dyn Allocation> = Arc::new(BufferAllocation {
            _backing: buffer.clone(),
            _reservation: reservation,
        });
        // SAFETY: `BufferAllocation` owns an immutable clone of the original
        // backing allocation. `data_ptr` and `capacity` describe that exact
        // allocation, and all returned slices stay within its checked bounds.
        let wrapped =
            unsafe { Buffer::from_custom_allocation(buffer.data_ptr(), capacity, allocation) };
        allocations.insert(key, wrapped.clone());
        wrapped
    };
    Ok(root.slice_with_length(offset, buffer.len()))
}

fn retained_buffer_bytes(batch: &RecordBatch) -> Result<usize> {
    let mut buffers = HashSet::new();
    let mut total = 0usize;
    for column in batch.columns() {
        collect_buffer_bytes(&column.to_data(), &mut buffers, &mut total)?;
    }
    Ok(total)
}

fn collect_buffer_bytes(
    data: &ArrayData,
    seen: &mut HashSet<(usize, usize)>,
    total: &mut usize,
) -> Result<()> {
    for buffer in data.buffers() {
        account_buffer_bytes(buffer, seen, total)?;
    }
    if let Some(nulls) = data.nulls() {
        account_buffer_bytes(nulls.buffer(), seen, total)?;
    }
    for child in data.child_data() {
        collect_buffer_bytes(child, seen, total)?;
    }
    Ok(())
}

fn account_buffer_bytes(
    buffer: &Buffer,
    seen: &mut HashSet<(usize, usize)>,
    total: &mut usize,
) -> Result<()> {
    let capacity = buffer.capacity();
    if capacity == 0 && !buffer.is_empty() {
        return Err(Error::Unavailable {
            operation: "Arrow buffer has no accountable owned capacity".to_string(),
        });
    }
    if seen.insert((buffer.data_ptr().as_ptr() as usize, capacity)) {
        *total = total.checked_add(capacity).ok_or(Error::Overflow {
            operation: "retained Arrow buffer bytes",
        })?;
    }
    Ok(())
}
