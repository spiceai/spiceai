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

use std::collections::BTreeMap;
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

/// Immutable in-memory page store for a complete covered source.
///
/// The maps own one lease per page. Loading clones the lease, so repeated page
/// identifiers preserve caller order while every consumer retains the page's
/// accounting pin.
#[derive(Debug, Default)]
pub(crate) struct MemoryPageStore {
    key_pages: BTreeMap<KeyPageId, KeyPageLease>,
    payload_pages: BTreeMap<PayloadPageId, PayloadPageLease>,
}

impl MemoryPageStore {
    /// Construct a store, rejecting duplicate IDs before publishing ownership.
    pub(crate) fn new(
        key_pages: impl IntoIterator<Item = (KeyPageId, KeyPageLease)>,
        payload_pages: impl IntoIterator<Item = (PayloadPageId, PayloadPageLease)>,
    ) -> Result<Self> {
        let mut store = Self::default();
        for (id, lease) in key_pages {
            if store.key_pages.insert(id.clone(), lease).is_some() {
                return Err(Error::InvalidContract {
                    message: format!("memory page store contains key page {id:?} more than once"),
                });
            }
        }
        for (id, lease) in payload_pages {
            if store.payload_pages.insert(id.clone(), lease).is_some() {
                return Err(Error::InvalidContract {
                    message: format!(
                        "memory page store contains payload page {id:?} more than once"
                    ),
                });
            }
        }
        Ok(store)
    }

    /// Whether this immutable store owns a payload page ID.
    #[must_use]
    pub(crate) fn contains_payload_page(&self, id: &PayloadPageId) -> bool {
        self.payload_pages.contains_key(id)
    }

    /// Whether this immutable store owns a key page ID.
    #[must_use]
    pub(crate) fn contains_key_page(&self, id: &KeyPageId) -> bool {
        self.key_pages.contains_key(id)
    }

    /// Clone every payload lease for another index over the same source.
    ///
    /// The leases share immutable pages and their buffer-owned charges; only
    /// the other index's key pages need additional resident memory.
    #[must_use]
    pub(crate) fn payload_page_leases(&self) -> BTreeMap<PayloadPageId, PayloadPageLease> {
        self.payload_pages.clone()
    }
}

#[async_trait]
impl CoveringPageStore for MemoryPageStore {
    async fn load_key_pages(&self, ids: &[KeyPageId]) -> Result<Vec<KeyPageLease>> {
        ids.iter()
            .map(|id| {
                self.key_pages
                    .get(id)
                    .cloned()
                    .ok_or_else(|| Error::MissingPage {
                        page: format!("{id:?}"),
                    })
            })
            .collect()
    }

    async fn load_payload_pages(&self, ids: &[PayloadPageId]) -> Result<Vec<PayloadPageLease>> {
        ids.iter()
            .map(|id| {
                self.payload_pages
                    .get(id)
                    .cloned()
                    .ok_or_else(|| Error::MissingPage {
                        page: format!("{id:?}"),
                    })
            })
            .collect()
    }
}

/// Controlled asynchronous page-store implementation used to verify that the
/// probe and gather code depend only on page leases, rather than on the
/// resident store's synchronous implementation details.
///
/// Each requested page waits on an explicit test-controlled gate. Requests are
/// polled concurrently and can therefore finish in a different order from the
/// caller's IDs, while `try_join_all` restores the contract's caller order.
/// This is test support only: it deliberately does not model persistence or a
/// cache policy.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use futures::future::try_join_all;
    use parking_lot::Mutex;
    use tokio::sync::Notify;

    use super::{
        CoveringPageStore, Error, KeyPageId, KeyPageLease, MemoryPageStore, PayloadPageId,
        PayloadPageLease, Result,
    };

    #[derive(Debug, Default)]
    struct PageGate {
        released: AtomicBool,
        notify: Notify,
    }

    impl PageGate {
        fn release(&self) {
            self.released.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }

        async fn wait(&self) {
            loop {
                let notified = self.notify.notified();
                if self.released.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }
    }

    /// An in-memory store whose individual page operations can be explicitly
    /// delayed or failed. It wraps the resident store, but implements both
    /// trait methods itself so the contract suite also exercises asynchronous
    /// completion and result reordering.
    #[derive(Debug)]
    pub(crate) struct DelayedPageStore {
        resident: MemoryPageStore,
        key_gates: Mutex<BTreeMap<KeyPageId, Arc<PageGate>>>,
        payload_gates: Mutex<BTreeMap<PayloadPageId, Arc<PageGate>>>,
        key_failures: Mutex<BTreeMap<KeyPageId, String>>,
        payload_failures: Mutex<BTreeMap<PayloadPageId, String>>,
    }

    impl DelayedPageStore {
        /// Construct a delayed wrapper with the same immutable resident pages.
        pub(crate) fn new(
            key_pages: impl IntoIterator<Item = (KeyPageId, KeyPageLease)>,
            payload_pages: impl IntoIterator<Item = (PayloadPageId, PayloadPageLease)>,
        ) -> Result<Self> {
            Ok(Self {
                resident: MemoryPageStore::new(key_pages, payload_pages)?,
                key_gates: Mutex::new(BTreeMap::new()),
                payload_gates: Mutex::new(BTreeMap::new()),
                key_failures: Mutex::new(BTreeMap::new()),
                payload_failures: Mutex::new(BTreeMap::new()),
            })
        }

        /// Block a key-page request until [`Self::release_key_page`] is called.
        pub(crate) fn block_key_page(&self, id: KeyPageId) {
            self.key_gates
                .lock()
                .insert(id, Arc::new(PageGate::default()));
        }

        /// Permit all waiting requests for one key page to complete.
        pub(crate) fn release_key_page(&self, id: &KeyPageId) {
            if let Some(gate) = self.key_gates.lock().get(id) {
                gate.release();
            }
        }

        /// Cause a key-page request to fail after its optional delay.
        pub(crate) fn fail_key_page(&self, id: KeyPageId, message: impl Into<String>) {
            self.key_failures.lock().insert(id, message.into());
        }

        /// Block a payload-page request until [`Self::release_payload_page`] is called.
        pub(crate) fn block_payload_page(&self, id: PayloadPageId) {
            self.payload_gates
                .lock()
                .insert(id, Arc::new(PageGate::default()));
        }

        /// Permit all waiting requests for one payload page to complete.
        pub(crate) fn release_payload_page(&self, id: &PayloadPageId) {
            if let Some(gate) = self.payload_gates.lock().get(id) {
                gate.release();
            }
        }

        /// Cause a payload-page request to fail after its optional delay.
        pub(crate) fn fail_payload_page(&self, id: PayloadPageId, message: impl Into<String>) {
            self.payload_failures.lock().insert(id, message.into());
        }

        async fn load_one_key_page(&self, id: KeyPageId) -> Result<KeyPageLease> {
            let gate = self.key_gates.lock().get(&id).cloned();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            if let Some(message) = self.key_failures.lock().get(&id).cloned() {
                return Err(Error::Unavailable { operation: message });
            }
            let mut pages = self
                .resident
                .load_key_pages(std::slice::from_ref(&id))
                .await?;
            pages.pop().ok_or_else(|| Error::InvalidContract {
                message: "resident key-page store returned no page for one ID".to_string(),
            })
        }

        async fn load_one_payload_page(&self, id: PayloadPageId) -> Result<PayloadPageLease> {
            let gate = self.payload_gates.lock().get(&id).cloned();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            if let Some(message) = self.payload_failures.lock().get(&id).cloned() {
                return Err(Error::Unavailable { operation: message });
            }
            let mut pages = self
                .resident
                .load_payload_pages(std::slice::from_ref(&id))
                .await?;
            pages.pop().ok_or_else(|| Error::InvalidContract {
                message: "resident payload-page store returned no page for one ID".to_string(),
            })
        }
    }

    #[async_trait]
    impl CoveringPageStore for DelayedPageStore {
        async fn load_key_pages(&self, ids: &[KeyPageId]) -> Result<Vec<KeyPageLease>> {
            try_join_all(ids.iter().cloned().map(|id| self.load_one_key_page(id))).await
        }

        async fn load_payload_pages(&self, ids: &[PayloadPageId]) -> Result<Vec<PayloadPageLease>> {
            try_join_all(ids.iter().cloned().map(|id| self.load_one_payload_page(id))).await
        }
    }
}
