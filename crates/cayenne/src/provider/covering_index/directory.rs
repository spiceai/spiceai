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

//! Immutable sorted-run directory metadata.

use std::sync::Arc;

use super::{EncodedKey, Error, KeyPageId, Result, SourceId};

/// Identifier of one sorted key run within a source generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RunId(u32);

impl RunId {
    /// Create a source-local run identifier.
    #[must_use]
    pub(crate) const fn new(value: u32) -> Self {
        Self(value)
    }

    /// The source-local numeric run identifier.
    #[must_use]
    pub(crate) const fn get(self) -> u32 {
        self.0
    }
}

/// Exact key bounds and entry count for one immutable key page.
#[derive(Clone, Debug)]
pub(crate) struct KeyDirectoryEntry {
    page: KeyPageId,
    first_key: EncodedKey,
    last_key: EncodedKey,
    entry_count: usize,
}

impl KeyDirectoryEntry {
    /// Create a nonempty directory entry with sorted inclusive bounds.
    pub(crate) fn new(
        page: KeyPageId,
        first_key: EncodedKey,
        last_key: EncodedKey,
        entry_count: usize,
    ) -> Result<Self> {
        if entry_count == 0 {
            return Err(Error::InvalidContract {
                message: "a key-directory entry cannot describe an empty page".to_string(),
            });
        }
        if first_key > last_key {
            return Err(Error::InvalidContract {
                message: "a key-directory entry's first key sorts after its last key".to_string(),
            });
        }
        Ok(Self {
            page,
            first_key,
            last_key,
            entry_count,
        })
    }

    /// Generation-qualified page identity.
    #[must_use]
    pub(crate) fn page(&self) -> &KeyPageId {
        &self.page
    }

    /// Inclusive first key on the page.
    #[must_use]
    pub(crate) fn first_key(&self) -> &EncodedKey {
        &self.first_key
    }

    /// Inclusive last key on the page.
    #[must_use]
    pub(crate) fn last_key(&self) -> &EncodedKey {
        &self.last_key
    }

    /// Number of entries in the page.
    #[must_use]
    pub(crate) const fn entry_count(&self) -> usize {
        self.entry_count
    }
}

/// Ordered directory for every key page of one sorted run.
#[derive(Clone, Debug)]
pub(crate) struct KeyDirectory {
    source: SourceId,
    entries: Arc<[KeyDirectoryEntry]>,
}

impl KeyDirectory {
    /// Validate directory ownership and nondecreasing page bounds.
    pub(crate) fn new(source: SourceId, entries: Vec<KeyDirectoryEntry>) -> Result<Self> {
        let mut prior_last: Option<&EncodedKey> = None;
        for entry in &entries {
            if entry.page().source() != &source {
                return Err(Error::InvalidContract {
                    message: format!(
                        "key page {:?} is listed under source {:?}",
                        entry.page(),
                        source
                    ),
                });
            }
            if prior_last.is_some_and(|last| last > entry.first_key()) {
                return Err(Error::InvalidContract {
                    message: "key-directory pages are not ordered by their key bounds".to_string(),
                });
            }
            prior_last = Some(entry.last_key());
        }
        Ok(Self {
            source,
            entries: entries.into(),
        })
    }

    /// Source generation owning every directory entry.
    #[must_use]
    pub(crate) fn source(&self) -> &SourceId {
        &self.source
    }

    /// Ordered directory entries.
    #[must_use]
    pub(crate) fn entries(&self) -> &[KeyDirectoryEntry] {
        &self.entries
    }

    /// Candidate pages whose exact bounds may contain key.
    ///
    /// A run may split duplicate keys across page boundaries, so the predicate
    /// intentionally includes each overlapping page rather than relying on a
    /// single binary-search hit.
    #[must_use]
    pub(crate) fn candidate_pages(&self, key: &EncodedKey) -> Vec<&KeyDirectoryEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.first_key() <= key && key <= entry.last_key())
            .collect()
    }
}

/// One ordered sorted run for one index definition and source generation.
#[derive(Clone, Debug)]
pub(crate) struct IndexRun {
    source: SourceId,
    run: RunId,
    directory: KeyDirectory,
}

impl IndexRun {
    /// Create a run only when its directory belongs to the same source.
    pub(crate) fn new(source: SourceId, run: RunId, directory: KeyDirectory) -> Result<Self> {
        if directory.source() != &source {
            return Err(Error::InvalidContract {
                message: format!(
                    "run {} source {:?} differs from directory source {:?}",
                    run.get(),
                    source,
                    directory.source()
                ),
            });
        }
        Ok(Self {
            source,
            run,
            directory,
        })
    }

    /// Source generation owning this run.
    #[must_use]
    pub(crate) fn source(&self) -> &SourceId {
        &self.source
    }

    /// Source-local run identifier.
    #[must_use]
    pub(crate) const fn run(&self) -> RunId {
        self.run
    }

    /// Exact page-bound directory.
    #[must_use]
    pub(crate) fn directory(&self) -> &KeyDirectory {
        &self.directory
    }
}

/// Compact page/range description of one literal key's span in one run.
///
/// Offsets are entry indexes, with `end_offset` exclusive. They name only key
/// page ranges; they never materialize all matching row references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LiteralSeekSpan {
    /// Source generation owning this span.
    pub(crate) source: SourceId,
    /// Sorted run containing the span.
    pub(crate) run: RunId,
    /// First page included in the span.
    pub(crate) first_page: KeyPageId,
    /// First included entry offset on `first_page`.
    pub(crate) first_offset: usize,
    /// Last page included in the span.
    pub(crate) last_page: KeyPageId,
    /// Exclusive end offset on `last_page`.
    pub(crate) end_offset: usize,
}

impl LiteralSeekSpan {
    /// Validate source ownership and a nonempty single-page range.
    pub(crate) fn single_page(
        source: SourceId,
        run: RunId,
        page: KeyPageId,
        first_offset: usize,
        end_offset: usize,
    ) -> Result<Self> {
        if page.source() != &source {
            return Err(Error::InvalidContract {
                message: format!("literal seek page {page:?} does not belong to {source:?}"),
            });
        }
        if first_offset >= end_offset {
            return Err(Error::InvalidContract {
                message: "literal seek spans must contain at least one entry".to_string(),
            });
        }
        Ok(Self {
            source,
            run,
            first_page: page.clone(),
            first_offset,
            last_page: page,
            end_offset,
        })
    }
}

/// Prepared compact result of a literal-key seek over a pinned read view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedLiteralSeek {
    spans: Arc<[LiteralSeekSpan]>,
    raw_entry_count: usize,
}

impl PreparedLiteralSeek {
    /// Create a prepared seek after checked exact entry counting.
    pub(crate) fn new(spans: Vec<LiteralSeekSpan>, raw_entry_count: usize) -> Result<Self> {
        if spans.is_empty() && raw_entry_count != 0 {
            return Err(Error::InvalidContract {
                message: "a literal seek with no spans cannot report matched entries".to_string(),
            });
        }
        Ok(Self {
            spans: spans.into(),
            raw_entry_count,
        })
    }

    /// Compact descriptors in source/run/page order.
    #[must_use]
    pub(crate) fn spans(&self) -> &[LiteralSeekSpan] {
        &self.spans
    }

    /// Exact raw matching-entry count before visibility filtering.
    #[must_use]
    pub(crate) const fn raw_entry_count(&self) -> usize {
        self.raw_entry_count
    }
}
