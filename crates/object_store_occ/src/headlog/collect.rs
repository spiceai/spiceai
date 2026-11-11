// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Deletion of a log's objects below its floor.
//!
//! When a checkpoint's floor is published, the batches it covers, the payloads
//! their records spilled and the previous checkpoint leave the head, but a
//! reader that read an older head may still be fetching them. Every age here
//! is the head's store time (`LastModified`), never a local clock:
//!
//! - A floor is stamped with the store time of the first collection that sees
//!   it. The head that collection read was written no earlier than the floor's
//!   publication, so the stamp is no earlier than the publication either.
//! - Objects below a floor are deleted once the head's store time is a
//!   retention past the floor's stamp. A reader that read an older head read it
//!   before the floor was published, so it has had at least the retention to
//!   fetch what that head lists.
//! - A collector that starts knows no stamps. It stamps the current floor
//!   afresh, which covers every floor below it with a later, so safe, stamp.
//!
//! A reader that takes longer than the retention can find an object gone; it
//! gets an error, never other rows, and [`super::Reader::open`] retries from
//! the newer head. Resolution of an uncertain commit never needs a deleted
//! object: below the floor it uses only the current checkpoint's evidence.
//!
//! The collector deletes by name, so it also removes an object that a fenced
//! writer re-created below the floor after an earlier collection.
//! The protocol is the `window` family of `cayenne-metastore-model`, whose
//! traces replay against this collector.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, path::Path};

use super::{BATCHES, CHECKPOINTS, Result, SPILLED, read_head_at};

/// How long objects stay after their floor is published, by default. A reader
/// that has not fetched an older head's objects by then retries from a newer
/// head.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(600);

/// What one collection deleted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Collected {
    /// The floor objects were deleted below.
    pub floor: u64,
    /// Objects deleted, including ones another collector deleted first.
    pub deleted: usize,
}

/// Deletes a log's batches, spilled payloads and checkpoints below its floor
/// once no reader within the retention can still need them. One collector per
/// log per process; several collectors on one log are safe, since each
/// deletes only below a floor it has itself seen published a retention ago.
pub struct Collector {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    retention_ms: u64,
    incarnation: Vec<u8>,
    /// Floors seen published, oldest first, with their stamps (store time,
    /// Unix milliseconds).
    retired: VecDeque<(u64, u64)>,
    /// The floor objects were last deleted below.
    collected: u64,
}

impl Collector {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path, retention: Duration) -> Self {
        Self {
            store,
            prefix,
            retention_ms: u64::try_from(retention.as_millis()).unwrap_or(u64::MAX),
            incarnation: Vec::new(),
            retired: VecDeque::new(),
            collected: 0,
        }
    }

    /// Read the head, stamp its floor if it is new, and delete below the
    /// newest floor stamped a retention ago. One head `GET`, and when a floor
    /// is due one `LIST` per object directory and a bulk delete.
    /// # Errors
    /// Returns an error when the head cannot be read or a request fails; what
    /// was already deleted stays deleted, and the next call deletes the rest.
    pub async fn collect(&mut self) -> Result<Option<Collected>> {
        let (state, modified) = read_head_at(self.store.as_ref(), &self.prefix).await?;
        let head = state.head;
        if head.incarnation != self.incarnation {
            self.incarnation = head.incarnation;
            self.retired.clear();
            self.collected = 0;
        }
        let newest = self.retired.back().map_or(self.collected, |&(f, _)| f);
        if head.floor > newest {
            self.retired.push_back((head.floor, modified));
        }
        let due = self
            .retired
            .iter()
            .filter(|&&(_, stamp)| stamp.saturating_add(self.retention_ms) <= modified)
            .map(|&(floor, _)| floor)
            .max();
        let Some(due) = due else {
            return Ok(None);
        };
        let deleted = self.delete_below(due).await?;
        self.collected = due;
        self.retired.retain(|&(floor, _)| floor > due);
        Ok(Some(Collected {
            floor: due,
            deleted,
        }))
    }

    /// Delete every batch that starts at or below `floor`, every payload
    /// spilled by a record at or below it, and every checkpoint below it. A
    /// floor is a batch end, so no batch that starts below it ends above it.
    async fn delete_below(&self, floor: u64) -> Result<usize> {
        let mut doomed = Vec::new();
        for (directory, keep) in [
            (BATCHES, floor + 1),
            (SPILLED, floor + 1),
            (CHECKPOINTS, floor),
        ] {
            let listed: Vec<_> = self
                .store
                .list(Some(&self.prefix.clone().join(directory)))
                .try_collect()
                .await?;
            doomed.extend(
                listed
                    .into_iter()
                    .filter(|meta| {
                        meta.location
                            .filename()
                            .and_then(|name| name.split('-').next())
                            .and_then(|number| number.parse::<u64>().ok())
                            .is_some_and(|number| number < keep)
                    })
                    .map(|meta| meta.location),
            );
        }
        let count = doomed.len();
        let mut results = self
            .store
            .delete_stream(futures::stream::iter(doomed.into_iter().map(Ok)).boxed());
        while let Some(result) = results.next().await {
            match result {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(count)
    }
}
