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

//! An index whose layers follow a table's storage tiers.
//!
//! Each write that produces data files produces one [`IndexRun`] over exactly
//! those files, built from the rows as they are written ([`RunBuilder`]).
//! Rows not in files (a memory tier, inlined rows) are not indexed: a reader
//! reads them in full. [`TieredIndex::publish`] adds the runs a write
//! produced and retires the runs of the files it replaced, in one atomic
//! swap — so it is called in the same critical section that makes the write
//! visible, and a reader never sees files without their run.
//!
//! A reader takes one [`IndexView`], which holds the run set and the filter
//! over their keys captured together.
//!
//! A lookup answers *candidate* row positions: every row holding the key in
//! the index's live files. A run survives until every file it covers is
//! retired, across as many publishes as that takes; a view skips the rows of
//! its retired files.
//!
//! # Bounded run count
//!
//! Every write adds a run, and a lookup of a key some run may hold probes
//! every run's filter (one filter over all runs' keys turns the others away
//! with a single probe), so [`TieredIndex::merge_step`] merges runs of about
//! the same size (a size-tiered policy): the data files do not change, only which run indexes
//! them, so a merge only renumbers file ids — no position moves. It runs off
//! to the side and swaps the merged run in under a short lock, using the
//! source runs' liveness as of the swap, so files retired while it ran stay
//! retired.
//!
//! # Contract
//!
//! Every `TieredIndex` method is safe to call concurrently with any other;
//! changes to the runs serialize on a short lock, and views and lookups are
//! lock-free.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use arrow_array::ArrayRef;
use hash_index::SplitBlockBloomFilter;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu, ensure};

use crate::word_proof::{self, MULTI};
use crate::{BoundKeyColumns, KeyEncoder, varint};

/// Bits of a file-local row position. A run stores a row as the mixed-radix
/// posting `position * files + file`, so a posting takes the few bytes its
/// position needs.
pub const POSITION_BITS: u32 = 40;
const POSITION_MASK: u64 = (1 << POSITION_BITS) - 1;
/// Files one run can cover: 2^23, so a posting `position * files + file`
/// stays below 2^63.
pub const MAX_RUN_FILES: usize = 1 << (63 - POSITION_BITS);
/// Runs of one size tier that [`TieredIndex::merge_step`] merges at once, and
/// the size ratio between tiers.
pub const MERGE_FANOUT: usize = 4;
/// Rows below which runs share the smallest tier.
const SMALLEST_TIER_ROWS: usize = 16 * 1024;
/// Words between a run's directory entries: a lookup binary-searches the
/// directory, then one stretch of this many words.
const DIRECTORY_STRIDE: usize = 128;

/// Errors raised while building runs or updating a [`TieredIndex`].
#[derive(Debug, Snafu)]
pub enum Error {
    /// The key columns did not match the index's key.
    #[snafu(display("Failed to index written rows: {source}"))]
    Key {
        /// The encoding failure.
        source: crate::Error,
    },
    /// A row position does not fit a run posting.
    #[snafu(display(
        "Failed to index written rows of '{file}': row position {position} exceeds the 40-bit limit."
    ))]
    Position {
        /// The file being indexed.
        file: String,
        /// The rejected position.
        position: u64,
    },
    /// A batch gave a different number of row positions than rows.
    #[snafu(display("Failed to index written rows: {rows} rows but {positions} row positions."))]
    PositionCount {
        /// Rows in the batch.
        rows: usize,
        /// Positions supplied.
        positions: usize,
    },
    /// A run covers more files than a posting can address.
    #[snafu(display(
        "Failed to index written rows: one write produced more than {MAX_RUN_FILES} files."
    ))]
    TooManyFiles,
    /// A run's row addresses do not fit its 31-bit offsets.
    #[snafu(display("Failed to index written rows: one write's row addresses exceed 2 GiB."))]
    TooLarge,
}

/// Result alias for this module.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// The verified encodings in `word_proof` are proved for exactly these limits.
const _: () = assert!(
    (1_u64 << POSITION_BITS) == word_proof::POSITION_LIMIT
        && MAX_RUN_FILES as u64 == word_proof::FILE_LIMIT
);

/// A word's filter hash. A word is either a hash already or the bytes of a
/// small fixed-width key, which have few random bits, so it is mixed first
/// with the `SplitMix64` finalizer.
fn word_hash(word: u64) -> u64 {
    let mut x = word ^ (word >> 30);
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Whether a run's `slots` and `postings` are laid out as `RunWriter` writes
/// them, so every lookup reads exactly its own word's rows: each
/// multi-posting word's stream starts where the previous one ended, in word
/// order, and the streams fill `postings` exactly; every stream decodes in
/// full, ascending, with positions in range; and the postings add up to
/// `rows`. A stream that ended early would skip rows after the damage, and two
/// words sharing one would give the second the first's rows.
fn postings_intact(files: usize, slots: &[u32], postings: &[u8], rows: usize) -> bool {
    // A posting names one of the run's files, so a run of none holds none.
    if files == 0 {
        return slots.is_empty() && postings.is_empty() && rows == 0;
    }
    let files = files as u64;
    // Where the next word's stream has to start.
    let mut end = 0_usize;
    let mut total = 0_usize;
    for &slot in slots {
        // A lone posting is below `MULTI`, so its position is in range.
        if word_proof::slot_is_lone(slot) {
            total += 1;
            continue;
        }
        if word_proof::slot_offset(slot) as usize != end {
            return false;
        }
        let Some(mut stream) = varint::PostingList::at(postings, end) else {
            return false;
        };
        // Postings strictly ascend: a repeated one is a gap of zero.
        let mut greatest: Option<u64> = None;
        for posting in stream.by_ref() {
            match posting {
                Ok(posting) if greatest.is_none_or(|before| posting > before) => {
                    greatest = Some(posting);
                }
                _ => return false,
            }
        }
        // The last posting holds the largest position; none at all is an
        // empty list, which a word never has. Each posting took at least one
        // byte, so the count fits a `usize` and the total cannot overflow.
        let (Some(greatest), Ok(count)) = (greatest, usize::try_from(stream.postings())) else {
            return false;
        };
        if greatest / files > POSITION_MASK {
            return false;
        }
        total += count;
        end = stream.end();
    }
    end == postings.len() && total == rows
}

/// An immutable index over exactly the files one write produced: every
/// distinct key word ([`KeyEncoder::key_word`]) of their rows, ascending, each
/// with its rows' postings.
#[derive(Debug)]
pub struct IndexRun {
    /// [`KeyEncoder::word_identity`] of the encoder whose words the run
    /// holds; an index publishes the run only if it is its own.
    encoding: u64,
    files: Box<[Arc<str>]>,
    words: Box<[u64]>,
    /// Per word: its only posting, when it has one below [`MULTI`] (a unique
    /// key's row, the common case); otherwise [`MULTI`] and the offset in
    /// `postings` of its postings, a varint count then ascending delta
    /// varints.
    slots: Box<[u32]>,
    postings: Box<[u8]>,
    /// Postings the run holds.
    rows: usize,
    /// `words[i * DIRECTORY_STRIDE]`, so a lookup searches a small array
    /// before one stretch of `words`. Derived; never persisted.
    directory: Box<[u64]>,
    /// Every word the run holds, so a lookup skips a run without the word for
    /// the cost of one filter probe.
    filter: SplitBlockBloomFilter,
}

impl IndexRun {
    fn from_parts(
        encoding: u64,
        files: Box<[Arc<str>]>,
        words: Box<[u64]>,
        slots: Box<[u32]>,
        postings: Box<[u8]>,
        rows: usize,
    ) -> Self {
        let directory = words.iter().step_by(DIRECTORY_STRIDE).copied().collect();
        // Built from the words, never stored: a filter read back from disk
        // could disagree with them, and a word it lacked would be missed.
        let mut filter = SplitBlockBloomFilter::new(words.len());
        filter.extend(words.iter().map(|&word| word_hash(word)));
        Self {
            encoding,
            files,
            words,
            slots,
            postings,
            rows,
            directory,
            filter,
        }
    }

    /// `(file, position)` of a stored posting.
    fn decode(&self, posting: u64) -> (usize, u64) {
        let (file, position) = word_proof::decode_posting(posting, self.files.len().max(1) as u64);
        (usize::try_from(file).unwrap_or(usize::MAX), position)
    }

    /// [`KeyEncoder::word_identity`] of the encoder that built the run.
    #[must_use]
    pub fn encoding(&self) -> u64 {
        self.encoding
    }

    /// The files this run covers, in the order their ids were assigned.
    #[must_use]
    pub fn files(&self) -> &[Arc<str>] {
        &self.files
    }

    /// Number of `(key, row)` entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Whether the run indexes no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Distinct key words the run holds.
    fn keys(&self) -> usize {
        self.words.len()
    }

    /// Heap bytes the run owns.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.words.len() * size_of::<u64>()
            + self.slots.len() * size_of::<u32>()
            + self.postings.len()
            + self.directory.len() * size_of::<u64>()
            + self.filter.memory_usage_bytes()
            + self
                .files
                .iter()
                // The `Arc<str>` itself, and its allocation: the two reference
                // counts, then the name.
                .map(|f| size_of::<Arc<str>>() + 2 * size_of::<usize>() + f.len())
                .sum::<usize>()
    }

    /// Where word `word` is in `words`, if the run holds it.
    fn find(&self, word: u64) -> Option<usize> {
        let stretch = self.directory.partition_point(|&first| first <= word);
        let start = stretch.checked_sub(1)? * DIRECTORY_STRIDE;
        let end = (start + DIRECTORY_STRIDE).min(self.words.len());
        self.words[start..end]
            .binary_search(&word)
            .ok()
            .map(|at| start + at)
    }

    /// Call `f(file, position)` with each row of the word at `at`, in posting
    /// order: `file` indexes [`Self::files`].
    fn rows_at(&self, at: usize, mut f: impl FnMut(usize, u64)) {
        let mut f = |posting| {
            let (file, position) = self.decode(posting);
            f(file, position);
        };
        let slot = self.slots[at];
        if word_proof::slot_is_lone(slot) {
            f(u64::from(slot));
            return;
        }
        // `from_bytes` checks that every list decodes in full, and a built run
        // is written by `RunWriter`, so neither ends early here.
        let Some(list) =
            varint::PostingList::at(&self.postings, word_proof::slot_offset(slot) as usize)
        else {
            return;
        };
        for posting in list.map_while(Result::ok) {
            f(posting);
        }
    }

    /// Call `f(word, file, position)` for every row the run indexes, in word
    /// order.
    pub fn for_each_row(&self, mut f: impl FnMut(u64, &str, u64)) {
        for (at, &word) in self.words.iter().enumerate() {
            self.rows_at(at, |file, position| {
                if let Some(path) = self.files.get(file) {
                    f(word, path, position);
                }
            });
        }
    }

    /// Call `f(file, position)` for every row of these files whose key has
    /// the word `word`, ascending by position then file. Tests probe one run
    /// with it; a reader goes through [`IndexView::candidates`].
    #[cfg(test)]
    pub(crate) fn lookup(&self, word: u64, mut f: impl FnMut(&str, u64)) {
        if !self.filter.might_contain(word_hash(word)) {
            return;
        }
        let Some(at) = self.find(word) else {
            return;
        };
        self.rows_at(at, |file, position| {
            if let Some(path) = self.files.get(file) {
                f(path, position);
            }
        });
    }
}

impl IndexRun {
    /// Persist as a standalone file (see [`crate::persist`]).
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a run covers far fewer than 2^32 files, each named in far fewer than 4 GiB"
    )]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.heap_bytes() + 64);
        crate::persist::header(&mut out, crate::persist::KIND_RUN);
        out.extend_from_slice(&self.encoding.to_le_bytes());
        out.extend_from_slice(&(self.files.len() as u32).to_le_bytes());
        for file in &self.files {
            out.extend_from_slice(&(file.len() as u32).to_le_bytes());
            out.extend_from_slice(file.as_bytes());
        }
        out.extend_from_slice(&(self.rows as u64).to_le_bytes());
        out.extend_from_slice(&(self.words.len() as u64).to_le_bytes());
        crate::persist::put_u64s(&mut out, &self.words);
        crate::persist::put_u32s(&mut out, &self.slots);
        out.extend_from_slice(&(self.postings.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.postings);
        crate::persist::seal(&mut out);
        out
    }

    /// Checks the encoded layout without allocating and bounds the working
    /// memory for decoding and publishing this run, excluding `bytes` itself.
    ///
    /// The file allowance covers name reference counts, the duplicate-name
    /// set, vector growth, file states and both publication coverage sets.
    /// The word allowance covers words, slots, the directory and both Bloom
    /// filters. Postings can coexist with their replacement allocation.
    /// Fixed headroom includes minimum Bloom blocks and publication metadata,
    /// including when the run has no files or words.
    ///
    /// # Errors
    ///
    /// Returns an error for a truncated layout or overflowing allocation sizes.
    pub fn decode_memory_bound(bytes: &[u8]) -> crate::persist::Result<usize> {
        Self::decode_memory_bound_from_reader(crate::persist::open(
            bytes,
            crate::persist::KIND_RUN,
        )?)
    }

    fn decode_memory_bound_from_reader(
        mut reader: crate::persist::Reader<'_>,
    ) -> crate::persist::Result<usize> {
        use crate::persist::Error;
        reader.u64()?;
        let files = reader.u32()? as usize;
        if files > MAX_RUN_FILES {
            return Err(Error::Corrupt);
        }
        let mut names = 0_usize;
        for _ in 0..files {
            let len = reader.u32()? as usize;
            reader.bytes(len)?;
            names = names.checked_add(len).ok_or(Error::Corrupt)?;
        }
        reader.len()?;
        let words = reader.len()?;
        reader.bytes(words.checked_mul(8).ok_or(Error::Corrupt)?)?;
        reader.bytes(words.checked_mul(4).ok_or(Error::Corrupt)?)?;
        let postings = reader.len()?;
        reader.bytes(postings)?;
        if !reader.is_empty() {
            return Err(Error::Corrupt);
        }
        files
            .checked_mul(16 * size_of::<Arc<str>>())
            .and_then(|size| size.checked_add(names))
            .and_then(|size| {
                words
                    .checked_mul(32)
                    .and_then(|words| size.checked_add(words))
            })
            .and_then(|size| {
                postings
                    .checked_mul(2)
                    .and_then(|postings| size.checked_add(postings))
            })
            .and_then(|size| size.checked_add(8192))
            .ok_or(Error::Corrupt)
    }

    /// Resident bytes plus temporary metadata and the table filter needed to
    /// publish this decoded run. Decoder buffers no longer contribute.
    ///
    /// # Errors
    ///
    /// Returns an error if the allocation sizes overflow.
    pub fn publication_memory_bound(&self) -> crate::persist::Result<usize> {
        use crate::persist::Error;
        self.files
            .len()
            .checked_mul(8 * size_of::<Arc<str>>())
            .and_then(|files| self.heap_bytes().checked_add(files))
            .and_then(|size| {
                self.keys()
                    .checked_mul(2)
                    .and_then(|filter| size.checked_add(filter))
            })
            .and_then(|size| size.checked_add(4096))
            .ok_or(Error::Corrupt)
    }

    /// Read a run written by [`Self::to_bytes`]. Its words must ascend and
    /// its offsets must fall inside its postings: a run read out of order
    /// would miss rows.
    ///
    /// # Errors
    ///
    /// When `bytes` are not a complete, intact run of this kind and
    /// version.
    pub fn from_bytes(bytes: &[u8]) -> crate::persist::Result<Self> {
        use crate::persist::Error;
        let mut reader = crate::persist::open(bytes, crate::persist::KIND_RUN)?;
        Self::decode_memory_bound_from_reader(reader)?;
        let encoding = reader.u64()?;
        let count = reader.u32()? as usize;
        // No builder writes more, and a merge involving more could never be
        // built.
        if count > MAX_RUN_FILES {
            return Err(Error::Corrupt);
        }
        let mut files: Vec<Arc<str>> = Vec::with_capacity(count.min(1 << 20));
        // A run covers each file once, as a builder writes it: a name given
        // twice would let one row be reached through both, and returned twice.
        let mut names: HashSet<&str> = HashSet::with_capacity(count.min(1 << 20));
        for _ in 0..count {
            let len = reader.u32()? as usize;
            let name = std::str::from_utf8(reader.bytes(len)?).map_err(|_| Error::Corrupt)?;
            if !names.insert(name) {
                return Err(Error::Corrupt);
            }
            files.push(Arc::from(name));
        }
        let rows = reader.len()?;
        let word_count = reader.len()?;
        let words = reader.u64s(word_count)?;
        let slots = reader.u32s(word_count)?;
        let postings_len = reader.len()?;
        let postings = reader.bytes(postings_len)?.to_vec();
        let ordered = words.windows(2).all(|pair| pair[0] < pair[1]);
        if !ordered || !reader.is_empty() || !postings_intact(files.len(), &slots, &postings, rows)
        {
            return Err(Error::Corrupt);
        }
        Ok(Self::from_parts(
            encoding,
            files.into(),
            words.into(),
            slots.into(),
            postings.into(),
            rows,
        ))
    }
}

/// Appends words, in ascending order, with their postings, into a run.
struct RunWriter {
    encoding: u64,
    words: Vec<u64>,
    slots: Vec<u32>,
    postings: Vec<u8>,
    rows: usize,
}

impl RunWriter {
    /// A writer of a run of `encoding`'s words, with room for `words` of them.
    fn with_capacity(encoding: u64, words: usize) -> Self {
        Self {
            encoding,
            words: Vec::with_capacity(words),
            slots: Vec::with_capacity(words),
            postings: Vec::new(),
            rows: 0,
        }
    }

    /// Appends `word`, above every word so far, with its `postings`,
    /// ascending, distinct and at least one.
    fn push(&mut self, word: u64, postings: &[u64]) -> Result<()> {
        let slot = match postings {
            [only] if *only < u64::from(MULTI) => word_proof::lone_slot(*only),
            _ => {
                let offset = u32::try_from(self.postings.len())
                    .ok()
                    .filter(|&offset| offset < MULTI)
                    .ok_or(Error::TooLarge)?;
                varint::put_postings(&mut self.postings, postings);
                word_proof::offset_slot(offset)
            }
        };
        self.words.push(word);
        self.slots.push(slot);
        self.rows += postings.len();
        Ok(())
    }

    fn finish(self, files: Box<[Arc<str>]>) -> IndexRun {
        IndexRun::from_parts(
            self.encoding,
            files,
            self.words.into(),
            self.slots.into(),
            self.postings.into(),
            self.rows,
        )
    }
}

/// Entries in each chunk of a [`RunBuilder`] after its first: 1 MiB.
const CHUNK_ENTRIES: usize = 1 << 16;

/// One indexed row: `(word, file << POSITION_BITS | position)` as it is added,
/// and `(word, posting)` once [`RunBuilder::finish`] knows the run's files.
type Entry = (u64, u64);

/// Builds an [`IndexRun`] from rows as a writer emits them, in any order: the
/// word and posting of each row.
///
/// Entries append into fixed chunks of [`CHUNK_ENTRIES`] (the first grows as a
/// small write needs), so no entry is ever copied to grow storage and at most
/// one chunk is part-filled: a single growing vector would, at its last
/// doubling, hold up to twice the entries and briefly both copies.
/// [`Self::finish`] moves the chunks, freeing each as it goes, into one vector
/// of exactly the entries' size and sorts it: faster than merging the sorted
/// chunks, and never holding more than one chunk twice.
#[derive(Debug)]
pub struct RunBuilder {
    encoder: KeyEncoder,
    /// The run's files, in id order; `file_ids` shares each name.
    files: Vec<Arc<str>>,
    file_ids: HashMap<Arc<str>, u64>,
    chunks: Vec<Vec<Entry>>,
    rows: usize,
    scratch: Vec<u8>,
}

impl RunBuilder {
    /// A builder for runs of `encoder`'s keys.
    #[must_use]
    pub fn new(encoder: KeyEncoder) -> Self {
        Self {
            encoder,
            files: Vec::new(),
            file_ids: HashMap::new(),
            chunks: Vec::new(),
            rows: 0,
            scratch: Vec::new(),
        }
    }

    /// Index `columns`, the key columns of rows written to `file` at
    /// positions `first_position..`. Rows with a NULL key column are skipped:
    /// no equality predicate matches them.
    ///
    /// # Errors
    ///
    /// When the columns do not match the key, a position exceeds 40 bits, or
    /// the write produced too many files.
    pub fn add_batch(
        &mut self,
        file: &str,
        first_position: u64,
        columns: &[ArrayRef],
    ) -> Result<()> {
        let bound = self.encoder.bind(columns).context(KeySnafu)?;
        let rows = bound.num_rows();
        if let Some(offset) = (rows as u64).checked_sub(1) {
            let last = first_position.saturating_add(offset);
            ensure!(
                last <= POSITION_MASK,
                PositionSnafu {
                    file: file.to_string(),
                    position: last,
                }
            );
        }
        // Registered only once the batch is accepted: a run covers each of its
        // files, so a refused batch must not add one.
        let file_id = self.file_id(file)?;
        self.ingest(&bound, file_id, |row| first_position + row as u64);
        Ok(())
    }

    /// [`Self::add_batch`] for rows at explicit positions: row `i` of
    /// `columns` is at `positions[i]` in `file` (what a read-back of the file
    /// reports, rather than what a writer counts).
    ///
    /// # Errors
    ///
    /// As [`Self::add_batch`], and when `positions` does not have one entry
    /// per row.
    pub fn add_batch_at(
        &mut self,
        file: &str,
        positions: &[u64],
        columns: &[ArrayRef],
    ) -> Result<()> {
        let bound = self.encoder.bind(columns).context(KeySnafu)?;
        ensure!(
            positions.len() == bound.num_rows(),
            PositionCountSnafu {
                rows: bound.num_rows(),
                positions: positions.len(),
            }
        );
        if let Some(&position) = positions.iter().find(|&&p| p > POSITION_MASK) {
            return PositionSnafu {
                file: file.to_string(),
                position,
            }
            .fail();
        }
        let file_id = self.file_id(file)?;
        self.ingest(&bound, file_id, |row| positions[row]);
        Ok(())
    }

    fn file_id(&mut self, file: &str) -> Result<u64> {
        if let Some(&id) = self.file_ids.get(file) {
            return Ok(id);
        }
        ensure!(self.files.len() < MAX_RUN_FILES, TooManyFilesSnafu);
        let id = self.files.len() as u64;
        let name: Arc<str> = Arc::from(file);
        self.files.push(Arc::clone(&name));
        self.file_ids.insert(name, id);
        Ok(id)
    }

    /// Declare `file` part of the run although it may hold no indexed row
    /// (every key NULL, or no rows at all), so the run still covers it.
    ///
    /// # Errors
    ///
    /// When the run already has the most files it can hold.
    pub fn add_file(&mut self, file: &str) -> Result<()> {
        self.file_id(file).map(|_| ())
    }

    fn ingest(
        &mut self,
        bound: &BoundKeyColumns<'_>,
        file_id: u64,
        position: impl Fn(usize) -> u64,
    ) {
        for row in 0..bound.num_rows() {
            if bound.has_null(row) {
                continue;
            }
            self.scratch.clear();
            bound.encode_row(row, &mut self.scratch);
            let entry = (
                self.encoder.key_word(&self.scratch),
                (file_id << POSITION_BITS) | position(row),
            );
            match self.chunks.last_mut() {
                Some(chunk) if chunk.len() < CHUNK_ENTRIES => chunk.push(entry),
                // The first chunk grows as it fills, so a small write holds
                // only what it indexes; later ones are allocated whole.
                None => self.chunks.push(vec![entry]),
                Some(_) => {
                    let mut chunk = Vec::with_capacity(CHUNK_ENTRIES);
                    chunk.push(entry);
                    self.chunks.push(chunk);
                }
            }
            self.rows += 1;
        }
    }

    /// Files the run covers so far.
    #[must_use]
    pub fn files(&self) -> &[Arc<str>] {
        &self.files
    }

    /// Rows indexed so far.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Heap bytes the builder holds so far, for reserving its working
    /// memory. [`Self::finish`] sorts them in place and writes a run about
    /// as large alongside.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.chunks
            .iter()
            .map(|chunk| chunk.capacity() * size_of::<Entry>())
            .sum::<usize>()
            + self.scratch.capacity()
            // Per file: one allocation holding the two reference counts and
            // the name, and about 64 bytes of `files` slot and `file_ids`
            // entry.
            + self
                .files
                .iter()
                .map(|file| 2 * size_of::<usize>() + file.len() + 64)
                .sum::<usize>()
    }

    /// Sort the accumulated rows and build the run.
    ///
    /// # Errors
    ///
    /// When the run's row addresses exceed 2 GiB.
    pub fn finish(self) -> Result<IndexRun> {
        // One copy of the entries as `(word, posting)`, built by moving the
        // chunks in and freeing each as it is moved, so no more than one
        // chunk is held twice. A posting orders a word's rows as the run
        // stores them, so one sort orders the whole run.
        let files = self.files.len().max(1) as u64;
        let mut entries: Vec<Entry> = Vec::with_capacity(self.rows);
        for chunk in self.chunks {
            // A file id is below `files`, a position below 2^40 (checked as
            // rows were added), and `files` at most `MAX_RUN_FILES`.
            entries.extend(chunk.iter().map(|&(word, raw)| {
                (
                    word,
                    word_proof::posting(raw & POSITION_MASK, raw >> POSITION_BITS, files),
                )
            }));
        }
        entries.sort_unstable();
        let by_word = || entries.chunk_by(|a, b| a.0 == b.0);
        let mut writer = RunWriter::with_capacity(self.encoder.word_identity(), by_word().count());
        let mut group: Vec<u64> = Vec::new();
        for rows in by_word() {
            group.clear();
            group.extend(rows.iter().map(|&(_, posting)| posting));
            // A row added twice is stored once.
            group.dedup();
            writer.push(rows[0].0, &group)?;
        }
        Ok(writer.finish(self.files.into_boxed_slice()))
    }
}

/// Reconciles a run's file may stay unseen before it is retired. Runs are
/// published when their write returns, just before the write becomes
/// visible, so a live file is seen by the next reconcile after that; the
/// grace only has to cover writes that commit in between. Retiring one early
/// costs a read of the file in full, never a wrong answer.
pub const UNSEEN_GRACE: u8 = 4;

/// Where one of a run's files stands. A file seen live by a
/// [`TieredIndex::reconcile`] and later missing is retired, so a run published
/// before its write became visible is kept; one never seen is retired after
/// [`UNSEEN_GRACE`] reconciles, so a write that never became visible (it
/// failed, or its files were replaced before a reconcile saw them) does not
/// keep its run forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileState {
    /// Live, and not yet seen by a reconcile: how many reconciles have not
    /// seen it.
    Pending(u8),
    /// Live, and seen live by a reconcile.
    Seen,
    /// No longer live: a view skips its rows.
    Retired,
}

impl FileState {
    fn is_live(self) -> bool {
        self != Self::Retired
    }

    /// The state after a reconcile that did (`present`) or did not see the
    /// file in the live set.
    fn reconciled(self, present: bool) -> Self {
        match self {
            Self::Retired => Self::Retired,
            _ if present => Self::Seen,
            Self::Seen => Self::Retired,
            Self::Pending(unseen) => {
                let unseen = unseen.saturating_add(1);
                if unseen < UNSEEN_GRACE {
                    Self::Pending(unseen)
                } else {
                    Self::Retired
                }
            }
        }
    }
}

/// A published run and where each of its files stands. Retiring some of a
/// run's files replaces this entry, not the run.
#[derive(Debug, Clone)]
struct RunEntry {
    run: Arc<IndexRun>,
    /// Per file of the run, in order.
    files: Arc<[FileState]>,
}

/// What a change of file states did to a [`RunEntry`].
enum Transition {
    /// No file changed state.
    Unchanged,
    /// Some file changed state and at least one is still live.
    Changed(RunEntry),
    /// No file is live any more, so the run is dropped.
    Dropped,
}

impl RunEntry {
    fn any_live(states: &[FileState]) -> bool {
        states.iter().any(|state| state.is_live())
    }

    /// This entry with each file's state replaced by `next(file, state)`.
    /// Allocates only once a state changes.
    fn transition(&self, mut next: impl FnMut(&str, FileState) -> FileState) -> Transition {
        let mut changed: Option<Vec<FileState>> = None;
        for (i, (file, &state)) in self.run.files.iter().zip(self.files.iter()).enumerate() {
            let after = next(file, state);
            match &mut changed {
                Some(states) => states.push(after),
                None if after != state => {
                    let mut states = Vec::with_capacity(self.files.len());
                    states.extend_from_slice(&self.files[..i]);
                    states.push(after);
                    changed = Some(states);
                }
                None => {}
            }
        }
        match changed {
            None => Transition::Unchanged,
            Some(states) if Self::any_live(&states) => Transition::Changed(Self {
                run: Arc::clone(&self.run),
                files: states.into(),
            }),
            Some(_) => Transition::Dropped,
        }
    }
}

/// `runs` with each file's state replaced by `next(file, state)`, dropping
/// the runs none of whose files stays live, and whether any state changed.
fn transition_runs(
    runs: &[RunEntry],
    mut next: impl FnMut(&str, FileState) -> FileState,
) -> (Vec<RunEntry>, bool) {
    let mut kept = Vec::with_capacity(runs.len());
    let mut changed = false;
    for entry in runs {
        match entry.transition(&mut next) {
            Transition::Unchanged => kept.push(entry.clone()),
            Transition::Changed(entry) => {
                changed = true;
                kept.push(entry);
            }
            Transition::Dropped => changed = true,
        }
    }
    (kept, changed)
}

/// A filter over the keys of every run, so a key no run holds costs one
/// probe instead of one per run: a batch of new rows is mostly such keys.
/// It admits a superset — a retired run's keys stay in it until a rebuild —
/// which costs a lookup the per-run probes, never a candidate.
#[derive(Debug)]
struct TableFilter {
    filter: SplitBlockBloomFilter,
    /// Keys the filter was sized for.
    capacity: usize,
    /// Inserts that set a bit not already set: about the distinct keys the
    /// filter holds. A key inserted again (an updated row, a rewrite's run
    /// over keys already indexed) sets no new bit and does not count.
    distinct: AtomicUsize,
}

impl TableFilter {
    /// A filter over no keys, for an index with no run.
    fn empty() -> Self {
        Self {
            filter: SplitBlockBloomFilter::new(0),
            capacity: 0,
            distinct: AtomicUsize::new(0),
        }
    }

    /// A filter sized for, and holding, the keys of `runs`, filled before any
    /// reader can see it.
    fn of_runs<'a>(runs: impl Iterator<Item = &'a IndexRun> + Clone) -> Self {
        let keys = runs.clone().map(IndexRun::keys).sum();
        let mut filter = SplitBlockBloomFilter::new(keys);
        let distinct =
            filter.extend(runs.flat_map(|run| run.words.iter().map(|&word| word_hash(word))));
        Self {
            filter,
            capacity: keys,
            distinct: AtomicUsize::new(distinct),
        }
    }

    fn insert(&self, hash: u64) {
        if self.filter.insert_new(hash) {
            self.distinct.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn insert_run(&self, run: &IndexRun) {
        for &word in &run.words {
            self.insert(word_hash(word));
        }
    }

    /// Whether rebuilding over `runs` would help: the filter holds more than
    /// twice the distinct keys it was sized for, so its false-positive rate
    /// has grown; or more than twice the keys `runs` still hold, so most of
    /// it is keys of retired runs, which a Bloom filter cannot remove. The
    /// runs' key count overstates their distinct keys (a key in two runs
    /// counts twice), which only makes the second test later, never early.
    fn needs_rebuild(&self, runs: &[RunEntry]) -> bool {
        let distinct = self.distinct.load(Ordering::Relaxed);
        let live: usize = runs.iter().map(|entry| entry.run.keys()).sum();
        distinct > self.capacity.saturating_mul(2) || distinct > live.saturating_mul(2)
    }
}

/// What a reader sees: the runs and the filter over their keys, captured
/// together.
#[derive(Debug)]
struct Layers {
    runs: Vec<RunEntry>,
    /// Holds every key of every run in `runs`.
    filter: Arc<TableFilter>,
    /// Every file some run covers and has not retired.
    covered: HashSet<Arc<str>>,
}

impl Layers {
    fn new(runs: Vec<RunEntry>, filter: Arc<TableFilter>) -> Self {
        // No run left: the filter holds only retired keys, so let it go.
        let filter = if runs.is_empty() && filter.capacity > 0 {
            Arc::new(TableFilter::empty())
        } else {
            filter
        };
        let covered = runs
            .iter()
            .flat_map(|entry| {
                entry
                    .run
                    .files
                    .iter()
                    .zip(entry.files.iter())
                    .filter(|&(_, state)| state.is_live())
                    .map(|(file, _)| Arc::clone(file))
            })
            .collect();
        Self {
            runs,
            filter,
            covered,
        }
    }

    /// The filter after publishing `add` into `current`, whose other live runs
    /// are `kept`. The new runs' words go into the current filter, except that
    /// an index with no other run gets a filter sized for the new runs. An
    /// overfull filter is otherwise left for
    /// [`TieredIndex::rebuild_overfull_filter`]: rebuilding it reads every
    /// run, and only its false-positive rate depends on it, so it is never
    /// done while a write waits to become visible.
    fn filter_after_publish(
        current: &Arc<TableFilter>,
        kept: &[RunEntry],
        add: &[IndexRun],
    ) -> Arc<TableFilter> {
        if kept.is_empty() {
            return Arc::new(TableFilter::of_runs(add.iter()));
        }
        for run in add {
            current.insert_run(run);
        }
        Arc::clone(current)
    }
}

fn tier(rows: usize) -> u32 {
    let mut tier = 0;
    let mut bound = SMALLEST_TIER_ROWS;
    while rows > bound {
        bound = bound.saturating_mul(MERGE_FANOUT);
        tier += 1;
    }
    tier
}

/// The runs to merge: [`MERGE_FANOUT`] of the lowest size tier that has as
/// many, smallest first.
fn pick_merge(runs: &[RunEntry]) -> Option<Vec<usize>> {
    let mut by_tier: std::collections::BTreeMap<u32, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (i, entry) in runs.iter().enumerate() {
        by_tier.entry(tier(entry.run.len())).or_default().push(i);
    }
    by_tier
        .into_values()
        .find(|runs| runs.len() >= MERGE_FANOUT)
        .map(|mut picked| {
            picked.sort_by_key(|&i| runs[i].run.len());
            picked.truncate(MERGE_FANOUT);
            picked
        })
}

/// One run of `encoding`'s words over `sources`' files that are still live,
/// by a k-way merge of their words. A retired file is left out, rows and
/// name: another run may cover a file of that name again, and a run naming a
/// file twice is one [`IndexRun::from_bytes`] rejects.
fn merge_runs(encoding: u64, sources: &[&RunEntry]) -> Result<IndexRun> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    let mut files: Vec<Arc<str>> = Vec::new();
    // Per source, each file's id in the merged run, or `None` once retired.
    let mut ids: Vec<Vec<Option<u64>>> = Vec::with_capacity(sources.len());
    for source in sources {
        let mut source_ids = Vec::with_capacity(source.run.files.len());
        for (file, state) in source.run.files.iter().zip(source.files.iter()) {
            source_ids.push(state.is_live().then(|| {
                files.push(Arc::clone(file));
                files.len() as u64 - 1
            }));
        }
        ids.push(source_ids);
    }
    ensure!(files.len() <= MAX_RUN_FILES, TooManyFilesSnafu);
    let merged_files = files.len() as u64;
    // A source that lost a file is renumbered by more than an offset, so its
    // postings are sorted again rather than trusted to keep their order.
    let compacted: Vec<bool> = ids
        .iter()
        .map(|source_ids| source_ids.iter().any(Option::is_none))
        .collect();
    // Per source, the index of its next word.
    let mut next: Vec<usize> = vec![0; sources.len()];
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = sources
        .iter()
        .enumerate()
        .filter_map(|(i, source)| source.run.words.first().map(|&word| Reverse((word, i))))
        .collect();
    // The merged run holds every word of its largest source, unless a retired
    // file held a word's only rows.
    let mut writer = RunWriter::with_capacity(
        encoding,
        sources
            .iter()
            .map(|source| source.run.keys())
            .max()
            .unwrap_or(0),
    );
    let mut group: Vec<u64> = Vec::new();
    // Appends source `i`'s postings for its next word to `group`, renumbered
    // into the merged run's files, and moves past it; returns the source's
    // following word, if any.
    let take = |i: usize, next: &mut [usize], group: &mut Vec<u64>| {
        let run = &sources[i].run;
        let at = next[i];
        run.rows_at(at, |file, position| {
            if let Some(&Some(id)) = ids[i].get(file) {
                group.push(word_proof::posting(position, id, merged_files));
            }
        });
        next[i] = at + 1;
        run.words.get(at + 1).copied()
    };
    while let Some(Reverse((word, first))) = heap.pop() {
        group.clear();
        let mut following = take(first, &mut next, &mut group);
        // Every other source holding this word contributes its rows.
        let mut shared = false;
        while let Some(&Reverse((other, i))) = heap.peek() {
            if other != word {
                break;
            }
            heap.pop();
            shared = true;
            if let Some(after) = take(i, &mut next, &mut group) {
                heap.push(Reverse((after, i)));
            }
        }
        if !group.is_empty() {
            // One source's postings are already ascending: renumbering its
            // files by an offset keeps their order.
            if shared || compacted[first] {
                group.sort_unstable();
            }
            writer.push(word, &group)?;
        }
        // A source ahead of every other keeps the lead for as long as its
        // words stay below the heap's least: take them without the heap.
        let top = heap.peek().map(|Reverse((top, _))| *top);
        while let Some(after) = following {
            if top.is_some_and(|top| after >= top) {
                break;
            }
            group.clear();
            following = take(first, &mut next, &mut group);
            if !group.is_empty() {
                if compacted[first] {
                    group.sort_unstable();
                }
                writer.push(after, &group)?;
            }
        }
        if let Some(after) = following {
            heap.push(Reverse((after, first)));
        }
    }
    // The merged run's words are its sources', already in the index's filter.
    Ok(writer.finish(files.into_boxed_slice()))
}

/// A candidate row for a key: row `position` of data file `file`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Candidate<'a> {
    /// The file's path.
    pub file: &'a str,
    /// The file-local row position.
    pub position: u64,
}

/// A consistent view of a [`TieredIndex`], valid for as long as it is held.
#[derive(Debug, Clone)]
pub struct IndexView {
    layers: Arc<Layers>,
    encoder: Arc<KeyEncoder>,
}

impl IndexView {
    /// Call `f` with every candidate row for the encoded `key`, run by run.
    /// Candidates are rows whose key has the same [word](KeyEncoder::key_word),
    /// so a key whose word it shares with another key also gets that key's
    /// rows; a caller filters them.
    pub fn candidates(&self, key: &[u8], f: impl FnMut(Candidate<'_>)) {
        Self::candidates_in(&self.layers, self.encoder.key_word(key), f);
    }

    fn candidates_in(layers: &Layers, word: u64, mut f: impl FnMut(Candidate<'_>)) {
        let hash = word_hash(word);
        if !layers.filter.filter.might_contain(hash) {
            return;
        }
        for entry in &layers.runs {
            let (states, run) = (&entry.files, &entry.run);
            if !run.filter.might_contain(hash) {
                continue;
            }
            let Some(at) = run.find(word) else {
                continue;
            };
            run.rows_at(at, |file, position| {
                if states.get(file).is_some_and(|state| state.is_live())
                    && let Some(path) = run.files.get(file)
                {
                    f(Candidate {
                        file: path,
                        position,
                    });
                }
            });
        }
    }

    /// [`Self::candidates`] for many keys at once: `f(i, candidate)` for
    /// every candidate row of `keys[i]`. A key given twice gets its candidates
    /// under each index.
    pub fn candidates_batch(&self, keys: &[&[u8]], mut f: impl FnMut(usize, Candidate<'_>)) {
        let layers = &self.layers;
        if layers.runs.is_empty() {
            return;
        }
        for (i, key) in keys.iter().enumerate() {
            Self::candidates_in(layers, self.encoder.key_word(key), |candidate| {
                f(i, candidate);
            });
        }
    }

    /// Whether a live run covers every row of `file`: a reader may then take
    /// the view's candidates as the file's complete answer. A file not covered
    /// must be read in full.
    #[must_use]
    pub fn covers(&self, file: &str) -> bool {
        self.layers.covered.contains(file)
    }

    /// Number of runs a lookup probes.
    #[must_use]
    pub fn runs(&self) -> usize {
        self.layers.runs.len()
    }

    /// The view's runs, for persisting them.
    #[must_use]
    pub fn run_list(&self) -> Vec<Arc<IndexRun>> {
        self.layers
            .runs
            .iter()
            .map(|entry| Arc::clone(&entry.run))
            .collect()
    }

    /// Heap bytes of the runs and the filter over their keys.
    #[must_use]
    pub fn run_heap_bytes(&self) -> usize {
        // With no run, the filter is the empty one a new index starts with.
        let filter = if self.layers.runs.is_empty() {
            0
        } else {
            self.layers.filter.filter.memory_usage_bytes()
        };
        self.layers
            .runs
            .iter()
            .map(|entry| entry.run.heap_bytes())
            .sum::<usize>()
            + filter
    }
}

/// An index over one key of one table, layered by its storage tiers. See the
/// module docs.
#[derive(Debug)]
pub struct TieredIndex {
    encoder: Arc<KeyEncoder>,
    layers: ArcSwap<Layers>,
    /// Serializes the read-modify-write of `layers` by `publish` and the swap
    /// at the end of `merge_step`.
    swap: Mutex<()>,
    /// One merge at a time.
    merging: Mutex<()>,
}

impl TieredIndex {
    /// An empty index over `encoder`'s keys.
    #[must_use]
    pub fn new(encoder: KeyEncoder) -> Self {
        Self {
            encoder: Arc::new(encoder),
            layers: ArcSwap::from_pointee(Layers::new(Vec::new(), Arc::new(TableFilter::empty()))),
            swap: Mutex::new(()),
            merging: Mutex::new(()),
        }
    }

    /// The key encoding.
    #[must_use]
    pub fn encoder(&self) -> &KeyEncoder {
        &self.encoder
    }

    /// A consistent view for many lookups (a scan). Taking one clones a
    /// shared reference count; for a single lookup, [`Self::candidates`]
    /// borrows the current layers without writing shared memory.
    #[must_use]
    pub fn view(&self) -> IndexView {
        IndexView {
            layers: self.layers.load_full(),
            encoder: Arc::clone(&self.encoder),
        }
    }

    /// Call `f` with every candidate row for the encoded `key` in the current
    /// layers, as [`IndexView::candidates`] does.
    pub fn candidates(&self, key: &[u8], f: impl FnMut(Candidate<'_>)) {
        let layers = self.layers.load();
        IndexView::candidates_in(&layers, self.encoder.key_word(key), f);
    }

    /// Make a write visible to the index: add the runs it produced and retire
    /// the files in `retired`, dropping each run once all its files are. One
    /// atomic swap: call it where the write becomes visible.
    pub fn publish(&self, add: Vec<IndexRun>, retired: &[&str]) {
        let retired: HashSet<&str> = retired.iter().copied().collect();
        self.publish_runs(
            add,
            |file, state| {
                if retired.contains(file) {
                    FileState::Retired
                } else {
                    state
                }
            },
            |_| FileState::Pending(0),
            |_| true,
        );
    }

    /// [`Self::publish`] for runs over files that are already visible, where
    /// `live` is the complete file set a reader can see now. Each file is
    /// marked seen, so a later [`Self::reconcile`] retires it once it leaves
    /// the set, and a file already gone from `live` (compacted or replaced
    /// between its write and this call) is retired at once instead of being
    /// covered forever. A run none of whose files is live is dropped.
    pub fn publish_visible(&self, add: Vec<IndexRun>, live: &HashSet<&str>) {
        self.publish_runs(
            add,
            |_, state| state,
            |file| {
                if live.contains(file) {
                    FileState::Seen
                } else {
                    FileState::Retired
                }
            },
            RunEntry::any_live,
        );
    }

    /// Publishes `add` in one swap: each published file's state becomes
    /// `retire(file, state)`, dropping a published run none of whose files
    /// stays live, and each new file starts in `initial(file)`. A new run is
    /// added when `admit` accepts its files' states, it holds this index's
    /// words ([`IndexRun::encoding`]), and none of its files is already
    /// covered by a live run (including one added before it in `add`): each
    /// live file is covered by one run, so a lookup returns each of its rows
    /// once. A run that fails either is dropped whole, and those of its files
    /// no other run covers are read in full.
    fn publish_runs(
        &self,
        add: Vec<IndexRun>,
        retire: impl Fn(&str, FileState) -> FileState,
        initial: impl Fn(&str) -> FileState,
        admit: impl Fn(&[FileState]) -> bool,
    ) {
        let _swap = self.swap.lock();
        let current = self.layers.load_full();
        let (mut runs, _) = transition_runs(&current.runs, retire);
        let encoding = self.encoder.word_identity();
        let mut covered: HashSet<Arc<str>> = runs
            .iter()
            .flat_map(|entry| {
                entry
                    .run
                    .files
                    .iter()
                    .zip(entry.files.iter())
                    .filter(|&(_, state)| state.is_live())
                    .map(|(file, _)| Arc::clone(file))
            })
            .collect();
        let mut admitted: Vec<(IndexRun, Arc<[FileState]>)> = Vec::with_capacity(add.len());
        for run in add.into_iter().filter(|run| run.encoding == encoding) {
            let states: Arc<[FileState]> = run.files.iter().map(|file| initial(file)).collect();
            let overlaps = run
                .files
                .iter()
                .zip(states.iter())
                .any(|(file, state)| state.is_live() && covered.contains(file));
            if overlaps || !admit(&states) {
                continue;
            }
            covered.extend(
                run.files
                    .iter()
                    .zip(states.iter())
                    .filter(|&(_, state)| state.is_live())
                    .map(|(file, _)| Arc::clone(file)),
            );
            admitted.push((run, states));
        }
        let (add, states): (Vec<IndexRun>, Vec<Arc<[FileState]>>) = admitted.into_iter().unzip();
        // Into the filter before the runs are visible, so no reader sees a
        // run whose keys the filter rejects.
        let filter = Layers::filter_after_publish(&current.filter, &runs, &add);
        runs.reserve(add.len());
        runs.extend(add.into_iter().zip(states).map(|(run, files)| RunEntry {
            run: Arc::new(run),
            files,
        }));
        self.layers.store(Arc::new(Layers::new(runs, filter)));
    }

    /// Whether the index's filter is due for a rebuild, so
    /// [`Self::rebuild_overfull_filter`] has work to do: it holds more than
    /// twice the distinct keys it was sized for, or more than twice the keys
    /// the live runs hold, so most of it is keys of retired runs.
    #[must_use]
    pub fn filter_overfull(&self) -> bool {
        let layers = self.layers.load();
        layers.filter.needs_rebuild(&layers.runs)
    }

    /// Rebuild the filter over the live runs' keys if it is due for a rebuild
    /// ([`Self::filter_overfull`]), as
    /// [`Self::merge_step`] does first. Returns whether it rebuilt. Meant for a
    /// background task: it reads every run.
    pub fn rebuild_overfull_filter(&self) -> bool {
        let _merging = self.merging.lock();
        self.refilter()
    }

    /// Rebuild the filter over every run's keys when it is overfull. The
    /// rebuild reads a snapshot of the runs; runs published meanwhile went
    /// into the old filter only, so they are added to the new one under the
    /// swap lock, before it becomes visible. Called with `merging` held, so
    /// no merge replaces runs meanwhile.
    fn refilter(&self) -> bool {
        let snapshot = self.layers.load_full();
        if !snapshot.filter.needs_rebuild(&snapshot.runs) {
            return false;
        }
        let rebuilt = TableFilter::of_runs(snapshot.runs.iter().map(|entry| &*entry.run));
        let _swap = self.swap.lock();
        let now = self.layers.load_full();
        for entry in &now.runs {
            if !snapshot
                .runs
                .iter()
                .any(|seen| Arc::ptr_eq(&seen.run, &entry.run))
            {
                rebuilt.insert_run(&entry.run);
            }
        }
        self.layers
            .store(Arc::new(Layers::new(now.runs.clone(), Arc::new(rebuilt))));
        true
    }

    /// Merge [`MERGE_FANOUT`] runs of one size tier into one, if a tier has
    /// that many, and first rebuild the filter over every run's keys if it is
    /// overfull. Returns whether it did either. Meant for a background task.
    ///
    /// # Errors
    ///
    /// When the merged run cannot be built.
    pub fn merge_step(&self) -> Result<bool> {
        let _merging = self.merging.lock();
        let refiltered = self.refilter();
        let current = self.layers.load_full();
        let Some(picked) = pick_merge(&current.runs) else {
            return Ok(refiltered);
        };
        self.merge_picked(&current, &picked)
    }

    /// Merge every run into one, in a single pass. Returns whether it merged
    /// anything: false when there is at most one run.
    ///
    /// # Errors
    ///
    /// When the merged run cannot be built.
    pub fn merge_all(&self) -> Result<bool> {
        let _merging = self.merging.lock();
        self.refilter();
        let current = self.layers.load_full();
        if current.runs.len() < 2 {
            return Ok(false);
        }
        let picked: Vec<usize> = (0..current.runs.len()).collect();
        self.merge_picked(&current, &picked)
    }

    /// Merges `current`'s runs `picked` into one and swaps it in. The caller
    /// holds `merging`.
    fn merge_picked(&self, current: &Layers, picked: &[usize]) -> Result<bool> {
        let sources: Vec<&RunEntry> = picked.iter().map(|&i| &current.runs[i]).collect();
        let merged = merge_runs(self.encoder.word_identity(), &sources)?;

        let _swap = self.swap.lock();
        let now = self.layers.load_full();
        // The merged run's files (the sources' files live when the merge
        // started, in source order), as live as the sources are now: a source
        // dropped meanwhile had all its files retired.
        let mut states: Vec<FileState> = Vec::with_capacity(merged.files.len());
        for source in &sources {
            let now_states = now
                .runs
                .iter()
                .find(|entry| Arc::ptr_eq(&entry.run, &source.run))
                .map(|entry| &entry.files);
            for (at, then) in source.files.iter().enumerate() {
                if then.is_live() {
                    states.push(
                        now_states
                            .and_then(|states| states.get(at).copied())
                            .unwrap_or(FileState::Retired),
                    );
                }
            }
        }
        let mut runs: Vec<RunEntry> = now
            .runs
            .iter()
            .filter(|entry| {
                !sources
                    .iter()
                    .any(|source| Arc::ptr_eq(&entry.run, &source.run))
            })
            .cloned()
            .collect();
        if RunEntry::any_live(&states) {
            runs.push(RunEntry {
                run: Arc::new(merged),
                files: states.into(),
            });
        }
        // The merged run's keys are its sources', already in the filter.
        self.layers
            .store(Arc::new(Layers::new(runs, Arc::clone(&now.filter))));
        Ok(true)
    }

    /// Bring the index in line with `live`, the complete set of files a
    /// reader can now see: a file seen live by an earlier call and missing
    /// from this one is retired (a refresh, compaction or promotion replaced
    /// it), and a run is dropped once all its files are. A file not yet seen
    /// is kept, since its write may not be visible yet, for up to
    /// [`UNSEEN_GRACE`] calls. Call it once per distinct file set: every call
    /// counts toward the grace. Returns the number of files retired.
    pub fn reconcile(&self, live: &HashSet<&str>) -> usize {
        let _swap = self.swap.lock();
        let current = self.layers.load_full();
        let mut retired = 0;
        let (runs, changed) = transition_runs(&current.runs, |file, state| {
            let next = state.reconciled(live.contains(file));
            if state.is_live() && !next.is_live() {
                retired += 1;
            }
            next
        });
        if changed {
            self.layers
                .store(Arc::new(Layers::new(runs, Arc::clone(&current.filter))));
        }
        retired
    }
}

#[cfg(test)]
mod tests;
