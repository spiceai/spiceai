// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, path::Path};
use prost::Message;

use super::{
    Batch, Head, HeadState, Result, decode_head, decode_record, invalid, load_checkpoint, numbered,
    read_head, spilled_path,
};

/// Default bound on a reader's concurrent GETs (batches and spilled payloads).
pub const DEFAULT_READ_CONCURRENCY: usize = 32;

/// One accepted record with its dense commit number.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordWithCommit {
    pub commit: u64,
    pub record: super::Record,
}

/// Replay starts at the root (if any), then applies these records in order.
#[derive(Debug)]
pub struct Opened {
    pub checkpoint_root: Option<Bytes>,
    pub floor: u64,
    pub records: Vec<RecordWithCommit>,
}

#[derive(Debug, PartialEq)]
pub enum Poll {
    Unchanged,
    Records(Vec<RecordWithCommit>),
    /// The current floor passed this reader. Call open again; never skip replay.
    Reopen,
}

/// A conditional-polling reader. A failed poll never advances its cursor.
#[derive(Debug)]
pub struct Reader {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    state: HeadState,
    concurrency: usize,
}

impl Reader {
    /// GET head, then fetch the checkpoint and the listed batches, at most
    /// [`DEFAULT_READ_CONCURRENCY`] GETs at a time. When a listed object was
    /// collected under a newer floor meanwhile, opens from the newer head.
    /// # Errors
    /// Missing referenced objects, malformed history, and transport failures are errors.
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> Result<(Self, Opened)> {
        Self::open_with_concurrency(store, prefix, DEFAULT_READ_CONCURRENCY).await
    }

    /// [`Self::open`] with an explicit bound on concurrent GETs.
    /// # Errors
    /// Missing referenced objects, malformed history, and transport failures are errors.
    pub async fn open_with_concurrency(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        concurrency: usize,
    ) -> Result<(Self, Opened)> {
        if concurrency == 0 {
            return invalid("read concurrency must be at least 1");
        }
        let mut state = read_head(store.as_ref(), &prefix).await?;
        let mut retries = 0;
        loop {
            match load(store.as_ref(), &prefix, &state, concurrency).await {
                Ok(opened) => {
                    return Ok((
                        Self {
                            store,
                            prefix,
                            state,
                            concurrency,
                        },
                        opened,
                    ));
                }
                // An object the head lists is gone. If the floor has moved
                // since, a collector deleted it below the newer floor: open
                // from the newer head. Otherwise it is missing, an error.
                Err(error) if retries < OPEN_RETRIES && is_not_found(&error) => {
                    let latest = read_head(store.as_ref(), &prefix).await?;
                    if latest.head.incarnation != state.head.incarnation
                        || latest.head.floor <= state.head.floor
                    {
                        return Err(error);
                    }
                    state = latest;
                    retries += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[must_use]
    pub fn state(&self) -> &HeadState {
        &self.state
    }

    /// Conditional GET; fetch batches only if the cursor is behind the window.
    /// # Errors
    /// Returns an error on missing objects, changed incarnation, or invalid history.
    pub async fn poll(&mut self) -> Result<Poll> {
        let options = GetOptions {
            if_none_match: self.state.version.e_tag.clone(),
            ..GetOptions::default()
        };
        let result = match self
            .store
            .get_opts(&self.prefix.clone().join("head"), options)
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotModified { .. }) => return Ok(Poll::Unchanged),
            Err(error) => return Err(error.into()),
        };
        let state = decode_head(result).await?;
        if state.head.incarnation != self.state.head.incarnation
            || state.head.commit < self.state.head.commit
            || state.head.floor < self.state.head.floor
        {
            return invalid("reader observed a replaced or regressing log");
        }
        if state.head.floor > self.state.head.commit {
            return Ok(Poll::Reopen);
        }
        let records = records_since(
            self.store.as_ref(),
            &self.prefix,
            &state.head,
            self.state.head.commit,
            self.concurrency,
        )
        .await?;
        let records =
            resolve_spilled(self.store.as_ref(), &self.prefix, records, self.concurrency).await?;
        self.state = state;
        Ok(Poll::Records(records))
    }
}

/// How many times a cold open starts over from a newer head after a listed
/// object was collected under it.
const OPEN_RETRIES: usize = 3;

fn is_not_found(error: &super::Error) -> bool {
    matches!(
        error,
        super::Error::Storage {
            source: object_store::Error::NotFound { .. }
        }
    )
}

/// Fetch what `state` lists: its checkpoint and the records after its floor.
async fn load(
    store: &dyn ObjectStore,
    prefix: &Path,
    state: &HeadState,
    concurrency: usize,
) -> Result<Opened> {
    let head = &state.head;
    let checkpoint = async {
        if head.floor == 0 {
            Ok(None)
        } else {
            load_checkpoint(store, prefix, head)
                .await
                .map(|c| Some(c.root))
        }
    };
    let records = async {
        let records = records_since(store, prefix, head, head.floor, concurrency).await?;
        resolve_spilled(store, prefix, records, concurrency).await
    };
    let (checkpoint_root, records) = futures::try_join!(checkpoint, records)?;
    Ok(Opened {
        checkpoint_root,
        floor: head.floor,
        records,
    })
}

pub(super) async fn records_since(
    store: &dyn ObjectStore,
    prefix: &Path,
    head: &Head,
    after: u64,
    concurrency: usize,
) -> Result<Vec<RecordWithCommit>> {
    let mut first = head.floor + 1;
    let mut reads = Vec::new();
    for &last in &head.batches {
        let start = first;
        first = last + 1;
        if last <= after {
            continue;
        }
        reads.push(async move {
            let bytes = store
                .get(&numbered(prefix, super::BATCHES, start))
                .await?
                .bytes()
                .await?;
            let batch = Batch::decode(bytes)?;
            if batch.records.len() as u64 != last - start + 1 {
                return invalid("batch length disagrees with head index");
            }
            batch
                .records
                .iter()
                .enumerate()
                .map(|(index, encoded)| {
                    let record = decode_record(encoded)?;
                    if record.generation > head.lease {
                        return invalid("batch record newer than head lease");
                    }
                    Ok(RecordWithCommit {
                        commit: start + index as u64,
                        record,
                    })
                })
                .collect::<Result<Vec<_>>>()
        });
    }
    let mut records = futures::stream::iter(reads)
        .buffered(concurrency)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .filter(|entry| entry.commit > after)
        .collect::<Vec<_>>();
    for (index, entry) in head.window.iter().enumerate() {
        let commit = head.window_start() + index as u64;
        if commit > after {
            records.push(RecordWithCommit {
                commit,
                record: decode_record(&entry.encoded)?,
            });
        }
    }
    if records.len() as u64 != head.commit.saturating_sub(after) {
        return invalid("replay does not cover requested commit range");
    }
    Ok(records)
}

/// Replace each spilled reference with its payload, verified by length and
/// BLAKE3 digest. Order is preserved.
async fn resolve_spilled(
    store: &dyn ObjectStore,
    prefix: &Path,
    records: Vec<RecordWithCommit>,
    concurrency: usize,
) -> Result<Vec<RecordWithCommit>> {
    futures::stream::iter(records.into_iter().map(|mut entry| async move {
        if let Some(spilled) = entry.record.spilled.take() {
            let path = spilled_path(prefix, entry.commit, &entry.record.attempt_id);
            let bytes = store.get(&path).await?.bytes().await?;
            if bytes.len() as u64 != spilled.len
                || blake3::hash(&bytes).as_bytes()[..] != spilled.digest[..]
            {
                return invalid("spilled payload does not match its reference");
            }
            entry.record.payload = bytes;
        }
        Ok(entry)
    }))
    .buffered(concurrency)
    .try_collect()
    .await
}
