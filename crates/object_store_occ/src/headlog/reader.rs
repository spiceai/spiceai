// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, path::Path};
use prost::Message;

use super::{
    Batch, Head, HeadState, Result, decode_head, decode_record, invalid, load_checkpoint, numbered,
    read_head,
};

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
}

impl Reader {
    /// GET head, then fetch the checkpoint and all listed batches concurrently.
    /// # Errors
    /// Missing referenced objects, malformed history, and transport failures are errors.
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> Result<(Self, Opened)> {
        let state = read_head(store.as_ref(), &prefix).await?;
        let head = &state.head;
        let checkpoint = async {
            if head.floor == 0 {
                Ok(None)
            } else {
                load_checkpoint(store.as_ref(), &prefix, head)
                    .await
                    .map(|c| Some(c.root))
            }
        };
        let (checkpoint_root, records) = futures::try_join!(
            checkpoint,
            records_since(store.as_ref(), &prefix, head, head.floor)
        )?;
        let opened = Opened {
            checkpoint_root,
            floor: head.floor,
            records,
        };
        Ok((
            Self {
                store,
                prefix,
                state,
            },
            opened,
        ))
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
        )
        .await?;
        self.state = state;
        Ok(Poll::Records(records))
    }
}

pub(super) async fn records_since(
    store: &dyn ObjectStore,
    prefix: &Path,
    head: &Head,
    after: u64,
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
                .get(&numbered(prefix, "wal", start))
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
    let mut records = try_join_all(reads)
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
