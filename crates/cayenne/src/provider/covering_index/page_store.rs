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

//! Immutable key/payload pages and their pinning contract.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use super::super::memory_account::LookupIndexReservation;
use super::{CoveredRowRef, Error, KeyPageId, PayloadPageId, Result, SchemaIdentity};

/// A key page's encoded full keys and matching physical row references.
#[derive(Clone, Debug)]
pub(crate) struct KeyPage {
    encoded_keys: Arc<[u8]>,
    offsets: Arc<[usize]>,
    row_refs: Arc<[CoveredRowRef]>,
}

impl KeyPage {
    /// Validate and create immutable key-page contents.
    pub(crate) fn new(
        encoded_keys: Arc<[u8]>,
        offsets: Vec<usize>,
        row_refs: Vec<CoveredRowRef>,
    ) -> Result<Self> {
        let expected_offsets = row_refs.len().checked_add(1).ok_or(Error::Overflow {
            operation: "key-page offset count",
        })?;
        if offsets.len() != expected_offsets {
            return Err(Error::InvalidContract {
                message: format!(
                    "key page has {} row references but {} offsets",
                    row_refs.len(),
                    offsets.len()
                ),
            });
        }
        if offsets.first() != Some(&0) {
            return Err(Error::InvalidContract {
                message: "key-page offsets must begin at zero".to_string(),
            });
        }
        let mut previous = 0usize;
        for offset in &offsets {
            if *offset < previous || *offset > encoded_keys.len() {
                return Err(Error::InvalidContract {
                    message: format!(
                        "key-page offset {offset} is not within 0..={}",
                        encoded_keys.len()
                    ),
                });
            }
            previous = *offset;
        }
        if offsets.last() != Some(&encoded_keys.len()) {
            return Err(Error::InvalidContract {
                message: format!(
                    "key-page final offset {:?} does not equal encoded byte length {}",
                    offsets.last(),
                    encoded_keys.len()
                ),
            });
        }

        Ok(Self {
            encoded_keys,
            offsets: offsets.into(),
            row_refs: row_refs.into(),
        })
    }

    /// Number of full-key entries in this page.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.row_refs.len()
    }

    /// Whether the page has no entries.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.row_refs.is_empty()
    }

    /// The comparable full-key bytes for `entry`.
    pub(crate) fn key(&self, entry: usize) -> Result<&[u8]> {
        let end_index = entry.checked_add(1).ok_or(Error::Overflow {
            operation: "key-page entry offset",
        })?;
        let start = *self
            .offsets
            .get(entry)
            .ok_or_else(|| Error::InvalidContract {
                message: format!("key-page entry {entry} is out of range"),
            })?;
        let end = *self
            .offsets
            .get(end_index)
            .ok_or_else(|| Error::InvalidContract {
                message: format!("key-page entry {entry} is out of range"),
            })?;
        Ok(&self.encoded_keys[start..end])
    }

    /// The row reference paired with `entry`.
    pub(crate) fn row_ref(&self, entry: usize) -> Result<&CoveredRowRef> {
        self.row_refs
            .get(entry)
            .ok_or_else(|| Error::InvalidContract {
                message: format!("key-page entry {entry} is out of range"),
            })
    }

    /// Bytes retained directly by this page's navigation data.
    pub(crate) fn retained_bytes(&self) -> Result<usize> {
        self.encoded_keys
            .len()
            .checked_add(
                self.offsets
                    .len()
                    .checked_mul(std::mem::size_of::<usize>())
                    .ok_or(Error::Overflow {
                        operation: "key-page offset bytes",
                    })?,
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    self.row_refs
                        .len()
                        .checked_mul(std::mem::size_of::<CoveredRowRef>())?,
                )
            })
            .ok_or(Error::Overflow {
                operation: "key-page retained bytes",
            })
    }
}

/// A payload page with the exact source schema and owned Arrow values.
#[derive(Clone, Debug)]
pub(crate) struct PayloadPage {
    schema: SchemaIdentity,
    batch: RecordBatch,
}

impl PayloadPage {
    /// Capture a batch only when it structurally matches its source schema.
    pub(crate) fn new(schema: SchemaIdentity, batch: RecordBatch) -> Result<Self> {
        if batch.schema_ref().as_ref() != schema.schema().as_ref() {
            return Err(Error::InvalidContract {
                message: "payload batch schema differs from its captured schema identity"
                    .to_string(),
            });
        }
        Ok(Self { schema, batch })
    }

    /// The schema captured with this page.
    #[must_use]
    pub(crate) fn schema(&self) -> &SchemaIdentity {
        &self.schema
    }

    /// The owned Arrow batch. Consumers retaining child arrays must retain the
    /// lease's reservation token too; step 03 supplies the private Arrow owner
    /// wrapper that makes that retention automatic for shared output buffers.
    #[must_use]
    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }
}

/// Allocation ownership detached from page ownership.
///
/// It owns only a resident-memory reservation and deliberately has no
/// back-reference to a page or lease, making cycles impossible. The forthcoming
/// Arrow custom-buffer wrapper will retain a [`ReservationToken`] from this
/// owner for every buffer, child buffer, and slice that shares source memory.
pub(crate) struct AllocationOwner {
    reservation: LookupIndexReservation,
}

impl AllocationOwner {
    /// Bind an admitted allocation reservation to one independent Arrow backing allocation.
    #[must_use]
    pub(crate) fn new(reservation: LookupIndexReservation) -> Arc<Self> {
        Arc::new(Self { reservation })
    }

    /// Obtain a token that keeps this allocation's accounting alive.
    #[must_use]
    pub(crate) fn token(self: &Arc<Self>) -> ReservationToken {
        ReservationToken(Arc::clone(self))
    }

    /// The resident bytes admitted for this allocation.
    #[must_use]
    pub(crate) fn bytes(&self) -> usize {
        self.reservation.bytes()
    }
}

impl Debug for AllocationOwner {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AllocationOwner")
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

/// Cloneable proof that a retained buffer still owns its allocation charge.
///
/// A lease is sufficient while an operator merely reads a page. Once Arrow
/// output retains an input buffer past the operator, the output must carry this
/// token instead. It is intentionally not convertible back into a page lease.
#[derive(Clone, Debug)]
pub(crate) struct ReservationToken(Arc<AllocationOwner>);

impl ReservationToken {
    /// The retained allocation's admitted byte count.
    #[must_use]
    pub(crate) fn bytes(&self) -> usize {
        self.0.bytes()
    }
}

/// A page retained with an owning pin and its allocation token.
#[derive(Clone, Debug)]
pub(crate) struct PageLease<Page> {
    page: Arc<Page>,
    token: ReservationToken,
}

impl<Page> PageLease<Page> {
    /// Create a pin for an immutable page whose allocation was admitted already.
    #[must_use]
    pub(crate) fn new(page: Arc<Page>, token: ReservationToken) -> Self {
        Self { page, token }
    }

    /// The pinned page.
    #[must_use]
    pub(crate) fn page(&self) -> &Arc<Page> {
        &self.page
    }

    /// Token a result buffer must retain when it shares this page's allocations.
    #[must_use]
    pub(crate) fn reservation_token(&self) -> ReservationToken {
        self.token.clone()
    }
}

/// Lease for an immutable key page.
pub(crate) type KeyPageLease = PageLease<KeyPage>;
/// Lease for an immutable Arrow payload page.
pub(crate) type PayloadPageLease = PageLease<PayloadPage>;

/// Batch page loading contract for every covering-index page store.
///
/// Implementations must return one lease in caller order for every requested
/// identifier, including repeated identifiers. A missing page is an error, not
/// an empty result; implementations may deduplicate physical work internally.
#[async_trait]
pub(crate) trait CoveringPageStore: Debug + Send + Sync {
    /// Load each requested key page in exactly the input order.
    async fn load_key_pages(&self, ids: &[KeyPageId]) -> Result<Vec<KeyPageLease>>;

    /// Load each requested payload page in exactly the input order.
    async fn load_payload_pages(&self, ids: &[PayloadPageId]) -> Result<Vec<PayloadPageLease>>;
}
