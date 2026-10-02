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
//! the same size (a
//! size-tiered policy): the data files do not change, only which run indexes
//! them, so a merge only renumbers file ids — no position moves. It runs off
//! to the side and swaps the merged run in under a short lock, using the
//! source runs' liveness as of the swap, so files retired while it ran stay
//! retired.
//!
//! # Contract
//!
//! `publish`, `publish_visible`, `reconcile` and `merge_step` run
//! concurrently with one another, and views and lookups are lock-free.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use arrow_array::ArrayRef;
use hash_index::SplitBlockBloomFilter;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu, ensure};

use crate::{BoundKeyColumns, KeyEncoder, varint};

/// Bits of a file-local row position. A run stores a row as the mixed-radix
/// posting `position * files + file`, so a posting takes the few bytes its
/// position needs rather than a 40-bit-shifted file id's.
pub const POSITION_BITS: u32 = 40;
const POSITION_MASK: u64 = (1 << POSITION_BITS) - 1;
/// Files one run can cover: the rest of a posting below the value tag bit.
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

use crate::word_proof::{self, MULTI};

// The verified encodings in `word_proof` are proved for exactly these limits.
const _: () = assert!(
    (1_u64 << POSITION_BITS) == word_proof::POSITION_LIMIT
        && MAX_RUN_FILES as u64 == word_proof::FILE_LIMIT
);

/// A word's filter hash. A word is either a hash already or the bytes of a
/// small fixed-width key, which have few random bits, so it is mixed first.
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
    let files = files.max(1) as u64;
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
        let Some(mut list) = varint::PostingList::at(postings, end) else {
            return false;
        };
        // Postings strictly ascend: a repeated one is a gap of zero.
        let mut last: Option<u64> = None;
        for posting in list.by_ref() {
            match posting {
                Ok(posting) if last.is_none_or(|last| posting > last) => last = Some(posting),
                _ => return false,
            }
        }
        // The last posting holds the largest position; none at all is an
        // empty list, which a word never has. Each posting took at least one
        // byte, so the count fits a `usize` and the total cannot overflow.
        let (Some(last), Ok(count)) = (last, usize::try_from(list.postings())) else {
            return false;
        };
        if last / files > POSITION_MASK {
            return false;
        }
        total += count;
        end = list.end();
    }
    end == postings.len() && total == rows
}

/// An immutable index over exactly the files one write produced: every
/// distinct key word ([`KeyEncoder::key_word`]) of their rows, ascending, each
/// with its rows' postings.
#[derive(Debug)]
pub struct IndexRun {
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

    /// Call `f` with each posting of the word at `at`, ascending.
    fn postings_at(&self, at: usize, mut f: impl FnMut(u64)) {
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
            self.postings_at(at, |posting| {
                let (file, position) = self.decode(posting);
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
        self.postings_at(at, |posting| {
            let (file, position) = self.decode(posting);
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
        let count = reader.u32()? as usize;
        let mut files = Vec::with_capacity(count.min(1 << 20));
        for _ in 0..count {
            let len = reader.u32()? as usize;
            let name = std::str::from_utf8(reader.bytes(len)?).map_err(|_| Error::Corrupt)?;
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
    words: Vec<u64>,
    slots: Vec<u32>,
    postings: Vec<u8>,
    rows: usize,
}

impl RunWriter {
    /// A writer with room for `words` words.
    fn with_capacity(words: usize) -> Self {
        Self {
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
    files: Vec<String>,
    file_ids: HashMap<String, u64>,
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
        self.files.push(file.to_string());
        self.file_ids.insert(file.to_string(), id);
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
    pub fn files(&self) -> &[String] {
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
            + self
                .files
                .iter()
                .map(|file| 2 * file.capacity() + 64)
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
        let words = 1 + entries
            .windows(2)
            .filter(|pair| pair[0].0 != pair[1].0)
            .count();
        let mut writer = RunWriter::with_capacity(if entries.is_empty() { 0 } else { words });
        let mut group: Vec<u64> = Vec::new();
        let mut i = 0;
        while i < entries.len() {
            let word = entries[i].0;
            group.clear();
            while i < entries.len() && entries[i].0 == word {
                // A row added twice is stored once.
                let posting = entries[i].1;
                if group.last() != Some(&posting) {
                    group.push(posting);
                }
                i += 1;
            }
            writer.push(word, &group)?;
        }
        Ok(writer.finish(self.files.into_iter().map(Arc::from).collect()))
    }
}

/// [`RunEntry::seen`] of a file a reconcile has seen live.
const SEEN: u8 = u8::MAX;

/// Reconciles a run's file may stay unseen before it is retired. Runs are
/// published when their write returns, just before the write becomes
/// visible, so a live file is seen by the next reconcile after that; the
/// grace only has to cover writes that commit in between. Retiring one early
/// costs a read of the file in full, never a wrong answer.
pub const UNSEEN_GRACE: u8 = 4;

/// A published run and which of its files are still live. Retiring some of
/// a run's files replaces this entry, not the run.
#[derive(Debug, Clone)]
struct RunEntry {
    run: Arc<IndexRun>,
    live: Arc<[bool]>,
    /// Per file, [`SEEN`] once a [`TieredIndex::reconcile`] has seen it in
    /// the live file set, and otherwise how many reconciles have not. A file
    /// seen there and later missing is retired, so a run published before its
    /// write became visible is kept; one never seen is retired after
    /// [`UNSEEN_GRACE`] reconciles, so a write that never became visible (it
    /// failed, or its files were replaced before a reconcile saw them) does
    /// not keep its run forever.
    seen: Arc<[u8]>,
}

impl RunEntry {
    fn new(run: IndexRun) -> Self {
        let files = run.files.len();
        Self {
            run: Arc::new(run),
            live: vec![true; files].into(),
            seen: vec![0; files].into(),
        }
    }
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
                    .zip(entry.live.iter())
                    .filter(|&(_, &live)| live)
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

/// One run over all of `sources`' files, dropping the rows of files already
/// retired, by a k-way merge of their words.
fn merge_runs(sources: &[&RunEntry]) -> Result<IndexRun> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    let mut files: Vec<Arc<str>> = Vec::new();
    let mut offsets: Vec<u64> = Vec::with_capacity(sources.len());
    for source in sources {
        offsets.push(files.len() as u64);
        files.extend(source.run.files.iter().cloned());
    }
    ensure!(files.len() <= MAX_RUN_FILES, TooManyFilesSnafu);
    let merged_files = files.len() as u64;
    // Sources every file of which is live need no per-posting check.
    let all_live: Vec<bool> = sources
        .iter()
        .map(|source| source.live.iter().all(|&live| live))
        .collect();
    // Per source, the index of its next word.
    let mut next: Vec<usize> = vec![0; sources.len()];
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = sources
        .iter()
        .enumerate()
        .filter_map(|(i, source)| source.run.words.first().map(|&word| Reverse((word, i))))
        .collect();
    // The merged run holds at least the words of its largest source.
    let mut writer = RunWriter::with_capacity(
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
        let (source, offset) = (sources[i], offsets[i]);
        let run = &source.run;
        let at = next[i];
        run.postings_at(at, |posting| {
            let (file, position) = run.decode(posting);
            if all_live[i] || source.live.get(file).copied().unwrap_or(false) {
                group.push(word_proof::posting(
                    position,
                    file as u64 + offset,
                    merged_files,
                ));
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
            // files keeps their order.
            if shared {
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
            let (live, run) = (&entry.live, &entry.run);
            if !run.filter.might_contain(hash) {
                continue;
            }
            let Some(at) = run.find(word) else {
                continue;
            };
            run.postings_at(at, |posting| {
                let (file, position) = run.decode(posting);
                if live.get(file).copied().unwrap_or(false)
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
        let _swap = self.swap.lock();
        let current = self.layers.load_full();
        let retired: HashSet<&str> = retired.iter().copied().collect();
        let mut runs: Vec<RunEntry> = Vec::with_capacity(current.runs.len() + add.len());
        for entry in &current.runs {
            let touched = entry
                .run
                .files
                .iter()
                .zip(entry.live.iter())
                .any(|(file, &live)| live && retired.contains(&**file));
            if !touched {
                runs.push(entry.clone());
                continue;
            }
            let live: Arc<[bool]> = entry
                .run
                .files
                .iter()
                .zip(entry.live.iter())
                .map(|(file, &live)| live && !retired.contains(&**file))
                .collect();
            // Dropped once none of its files is live.
            if live.iter().any(|&live| live) {
                runs.push(RunEntry {
                    run: Arc::clone(&entry.run),
                    live,
                    seen: Arc::clone(&entry.seen),
                });
            }
        }
        // Into the filter before the runs are visible, so no reader sees a
        // run whose keys the filter rejects.
        let filter = Layers::filter_after_publish(&current.filter, &runs, &add);
        runs.extend(add.into_iter().map(RunEntry::new));
        self.layers.store(Arc::new(Layers::new(runs, filter)));
    }

    /// [`Self::publish`] for runs over files that are already visible, where
    /// `live` is the complete file set a reader can see now. Each file is
    /// marked seen, so a later [`Self::reconcile`] retires it once it leaves
    /// the set, and a file already gone from `live` (compacted or replaced
    /// between its write and this call) is retired at once instead of being
    /// covered forever. A run none of whose files is live is dropped.
    pub fn publish_visible(&self, add: Vec<IndexRun>, live: &HashSet<&str>) {
        let _swap = self.swap.lock();
        let current = self.layers.load_full();
        let mut admitted: Vec<(IndexRun, Arc<[bool]>)> = Vec::with_capacity(add.len());
        for run in add {
            let live: Arc<[bool]> = run
                .files
                .iter()
                .map(|file| live.contains(&**file))
                .collect();
            if live.iter().any(|&live| live) {
                admitted.push((run, live));
            }
        }
        let (add, lives): (Vec<IndexRun>, Vec<Arc<[bool]>>) = admitted.into_iter().unzip();
        // Into the filter before the runs are visible.
        let filter = Layers::filter_after_publish(&current.filter, &current.runs, &add);
        let mut runs = current.runs.clone();
        for (run, live) in add.into_iter().zip(lives) {
            let seen = vec![SEEN; run.files.len()].into();
            runs.push(RunEntry {
                run: Arc::new(run),
                live,
                seen,
            });
        }
        self.layers.store(Arc::new(Layers::new(runs, filter)));
    }

    /// Whether the index's filter holds more than twice the keys it was sized
    /// for, so [`Self::rebuild_overfull_filter`] has work to do.
    #[must_use]
    pub fn filter_overfull(&self) -> bool {
        let layers = self.layers.load();
        layers.filter.needs_rebuild(&layers.runs)
    }

    /// Rebuild the filter over the live runs' keys if it is overfull, as
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
        let merged = merge_runs(&sources)?;

        let _swap = self.swap.lock();
        let now = self.layers.load_full();
        // The merged run's files, in source order, as live as the sources are
        // now: a source dropped meanwhile had all its files retired.
        let mut live: Vec<bool> = Vec::with_capacity(merged.files.len());
        let mut seen: Vec<u8> = Vec::with_capacity(merged.files.len());
        for source in &sources {
            if let Some(entry) = now
                .runs
                .iter()
                .find(|entry| Arc::ptr_eq(&entry.run, &source.run))
            {
                live.extend(entry.live.iter());
                seen.extend(entry.seen.iter());
            } else {
                live.extend(std::iter::repeat_n(false, source.run.files.len()));
                seen.extend(std::iter::repeat_n(SEEN, source.run.files.len()));
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
        if live.iter().any(|&live| live) {
            runs.push(RunEntry {
                run: Arc::new(merged),
                live: live.into(),
                seen: seen.into(),
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
    pub fn reconcile(&self, live: &std::collections::HashSet<&str>) -> usize {
        let _swap = self.swap.lock();
        let current = self.layers.load_full();
        let mut changed = false;
        let mut retired = 0;
        let mut runs: Vec<RunEntry> = Vec::with_capacity(current.runs.len());
        for entry in &current.runs {
            let now_live: Vec<bool> = entry
                .run
                .files
                .iter()
                .map(|file| live.contains(&**file))
                .collect();
            let next_seen: Vec<u8> = entry
                .seen
                .iter()
                .zip(&now_live)
                .map(|(&seen, &now)| {
                    if now || seen == SEEN {
                        SEEN
                    } else {
                        seen.saturating_add(1)
                    }
                })
                .collect();
            let next_live: Vec<bool> = entry
                .live
                .iter()
                .zip(entry.seen.iter())
                .zip(next_seen.iter())
                .zip(&now_live)
                // Retired: seen live before and missing now, or never seen
                // within the grace.
                .map(|(((&was, &seen), &next), &now)| {
                    was && (now || (seen != SEEN && next < UNSEEN_GRACE))
                })
                .collect();
            if next_live.as_slice() == &*entry.live && next_seen.as_slice() == &*entry.seen {
                runs.push(entry.clone());
                continue;
            }
            changed = true;
            retired += entry
                .live
                .iter()
                .zip(&next_live)
                .filter(|&(&was, &now)| was && !now)
                .count();
            if next_live.iter().any(|&live| live) {
                runs.push(RunEntry {
                    run: Arc::clone(&entry.run),
                    live: next_live.into(),
                    seen: next_seen.into(),
                });
            }
        }
        if changed {
            self.layers
                .store(Arc::new(Layers::new(runs, Arc::clone(&current.filter))));
        }
        retired
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::DataType;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::EncodedKey;
    use crate::KeyField;

    fn encoder() -> KeyEncoder {
        KeyEncoder::new(vec![KeyField::new(DataType::Int64, false)]).expect("int64 key")
    }

    fn column(keys: &[i64]) -> Vec<ArrayRef> {
        vec![Arc::new(Int64Array::from(keys.to_vec()))]
    }

    fn encoded(k: i64) -> EncodedKey {
        let columns = column(&[k]);
        let encoder = encoder();
        let bound = encoder.bind(&columns).expect("bind");
        EncodedKey::from_row(&bound, 0)
    }

    fn word(k: i64) -> u64 {
        encoder().key_word(encoded(k).as_bytes())
    }

    /// Model: key -> set of (file, position) rows in live files.
    #[derive(Default)]
    struct Model {
        rows: BTreeMap<i64, BTreeSet<(String, u64)>>,
    }

    fn check(index: &TieredIndex, model: &Model, live: &BTreeSet<String>, keys: i64) {
        let view = index.view();
        // The batched lookup returns exactly the per-key candidates, for keys
        // in no particular order, with a duplicate and a missing key.
        let mut batch: Vec<i64> = (0..keys).rev().collect();
        batch.push(keys / 2);
        batch.push(keys + 7);
        let encoded_batch: Vec<EncodedKey> = batch.iter().map(|&k| encoded(k)).collect();
        let borrowed: Vec<&[u8]> = encoded_batch.iter().map(EncodedKey::as_bytes).collect();
        let mut batched: Vec<Vec<String>> = vec![Vec::new(); batch.len()];
        view.candidates_batch(&borrowed, |i, candidate| {
            batched[i].push(format!("{candidate:?}"));
        });
        for (i, key) in borrowed.iter().enumerate() {
            let mut single = Vec::new();
            view.candidates(key, |candidate| single.push(format!("{candidate:?}")));
            let (mut a, mut b) = (batched[i].clone(), single);
            a.sort();
            b.sort();
            assert_eq!(a, b, "batched candidates of key {}", batch[i]);
        }
        for k in 0..keys {
            let mut files = BTreeSet::new();
            view.candidates(encoded(k).as_bytes(), |Candidate { file, position }| {
                // A reader discards candidates of files it does not scan.
                if live.contains(file) {
                    assert!(
                        files.insert((file.to_string(), position)),
                        "duplicate candidate"
                    );
                }
            });
            assert_eq!(
                files,
                model.rows.get(&k).cloned().unwrap_or_default(),
                "file rows of key {k}"
            );
        }
    }

    /// A large run — rows arriving shuffled, in uneven batches, across
    /// files, with repeated keys — answers every key exactly as a model of
    /// the rows does.
    #[test]
    fn a_large_run_matches_a_model() {
        let mut rng = StdRng::seed_from_u64(0x5E6_3E27);
        let rows = 7 * 65_536 + 123;
        let mut builder = RunBuilder::new(encoder());
        let mut model: BTreeMap<i64, BTreeSet<(String, u64)>> = BTreeMap::new();
        let mut written = 0;
        let mut file_no = 0;
        while written < rows {
            let file = format!("f{file_no}.vortex");
            let batch = rng.random_range(1..=2 * 65_536).min(rows - written);
            let keys: Vec<i64> = (0..batch).map(|_| rng.random_range(0..5_000)).collect();
            for (row, &key) in keys.iter().enumerate() {
                model
                    .entry(key)
                    .or_default()
                    .insert((file.clone(), row as u64));
            }
            builder.add_batch(&file, 0, &column(&keys)).expect("batch");
            written += batch;
            file_no += 1;
        }
        let run = builder.finish().expect("run");
        assert_eq!(run.len(), rows);
        for key in -1..5_001 {
            let mut found = BTreeSet::new();
            run.lookup(word(key), |file, position| {
                assert!(
                    found.insert((file.to_string(), position)),
                    "key {key} repeated a row"
                );
            });
            assert_eq!(found, model.remove(&key).unwrap_or_default(), "key {key}");
        }
    }

    /// Keys that share a word get each other's rows as candidates, never
    /// fewer than their own: with 2-bit words, 100 keys share 4 words.
    #[test]
    fn keys_sharing_a_word_get_a_superset_of_their_rows() {
        let colliding = || encoder().with_word_bits(2);
        let keys: Vec<i64> = (0..100).flat_map(|k| [k, k]).collect();
        let index = TieredIndex::new(colliding());
        let mut builder = RunBuilder::new(colliding());
        builder.add_batch("f", 0, &column(&keys)).expect("add");
        index.publish(vec![builder.finish().expect("finish")], &[]);
        let mut extra = 0;
        for key in 0..100_i64 {
            let own: BTreeSet<u64> = keys
                .iter()
                .enumerate()
                .filter(|&(_, &k)| k == key)
                .map(|(position, _)| position as u64)
                .collect();
            let mut got = BTreeSet::new();
            index.candidates(encoded(key).as_bytes(), |Candidate { position, .. }| {
                got.insert(position);
            });
            assert!(own.is_subset(&got), "key {key} lost its own rows");
            extra += got.len() - own.len();
        }
        assert!(extra > 0, "2-bit words must collide");
    }

    /// A key's only row, at a position whose posting does not fit a slot's
    /// 31 bits, is kept in the postings instead, and found exactly.
    #[test]
    fn a_lone_posting_too_wide_for_its_slot_is_found() {
        let wide = 1_u64 << 33;
        for position in [0, (1 << 31) - 1, 1 << 31, wide] {
            let mut builder = RunBuilder::new(encoder());
            builder
                .add_batch_at("f", &[position], &column(&[42]))
                .expect("add");
            let run = builder.finish().expect("finish");
            let mut found = Vec::new();
            run.lookup(word(42), |_, position| found.push(position));
            assert_eq!(found, vec![position]);
            let restored = IndexRun::from_bytes(&run.to_bytes()).expect("round trip");
            let mut found = Vec::new();
            restored.lookup(word(42), |_, position| found.push(position));
            assert_eq!(found, vec![position], "after a round trip");
        }
    }

    /// A run over `keys`, all in `file`.
    fn run_of(file: &str, keys: &[i64]) -> IndexRun {
        let mut builder = RunBuilder::new(encoder());
        builder.add_batch(file, 0, &column(keys)).expect("add");
        builder.finish().expect("finish")
    }

    /// The filter counts distinct keys, not inserts: runs over keys it holds
    /// already (a rewrite's run over the rows it compacted, a batch of
    /// updates) never make it due for a rebuild, while new keys past twice
    /// its size and keys mostly retired do. A publish never rebuilds it.
    #[test]
    fn the_filter_is_rebuilt_for_new_or_retired_keys_not_repeated_ones() {
        let keys: Vec<i64> = (0..1_000).collect();
        let index = TieredIndex::new(encoder());
        index.publish(vec![run_of("a", &keys)], &[]);
        for file in ["b", "c", "d"] {
            index.publish(vec![run_of(file, &keys)], &[]);
        }
        assert!(
            !index.filter_overfull(),
            "republishing the same keys must not make the filter overfull"
        );

        let grown = TieredIndex::new(encoder());
        grown.publish(vec![run_of("a", &keys)], &[]);
        let more: Vec<i64> = (1_000..3_500).collect();
        grown.publish(vec![run_of("b", &more)], &[]);
        assert!(grown.filter_overfull(), "3.5x the keys it was sized for");
        let before = Arc::as_ptr(&grown.layers.load().filter);
        grown.publish(vec![run_of("c", &[10_000])], &[]);
        assert_eq!(
            Arc::as_ptr(&grown.layers.load().filter),
            before,
            "a publish must not rebuild the filter"
        );
        assert!(grown.rebuild_overfull_filter());
        assert!(!grown.filter_overfull());

        // With no other run left, retiring every file hands the next publish
        // a fresh filter; here a small run stays live alongside.
        let retired = TieredIndex::new(encoder());
        retired.publish(vec![run_of("a", &keys), run_of("c", &[7_000])], &[]);
        retired.publish(vec![run_of("b", &[5_000, 5_001])], &["a"]);
        assert!(
            retired.filter_overfull(),
            "a filter of mostly retired keys is due for a rebuild"
        );
        assert!(retired.rebuild_overfull_filter());
        assert!(!retired.filter_overfull());
        // Still answers for the live keys after the rebuild.
        let mut found = 0;
        retired
            .view()
            .candidates(encoded(5_001).as_bytes(), |_| found += 1);
        assert_eq!(found, 1);
    }

    /// Merging runs of hashed (string) keys, some keys in several runs, from
    /// single- and multi-file runs with retired files, keeps exactly the live
    /// rows, under each row's word.
    #[test]
    fn merging_runs_keeps_every_live_row() {
        let string_encoder =
            || KeyEncoder::new(vec![KeyField::new(DataType::Utf8, false)]).expect("utf8 key");
        let key_of = |k: u64| format!("service-{k:04}");
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let index = TieredIndex::new(string_encoder());
        let mut model: Vec<(u64, String, u64)> = Vec::new();
        let mut runs = Vec::new();
        for run_no in 0..6 {
            let mut builder = RunBuilder::new(string_encoder());
            // Runs 0 and 1 span two files each; the rest one file.
            let files = if run_no < 2 { 2 } else { 1 };
            for file_no in 0..files {
                let file = format!("r{run_no}f{file_no}");
                let keys: Vec<String> = (0..200).map(|_| key_of(next() % 300)).collect();
                let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(keys.clone()))];
                builder.add_batch(&file, 0, &columns).expect("add");
                let bound_columns = columns.clone();
                let encoder = string_encoder();
                let bound = encoder.bind(&bound_columns).expect("bind");
                for (position, _) in keys.iter().enumerate() {
                    let mut key_bytes = Vec::new();
                    bound.encode_row(position, &mut key_bytes);
                    model.push((encoder.key_word(&key_bytes), file.clone(), position as u64));
                }
            }
            runs.push(builder.finish().expect("finish"));
        }
        index.publish(runs, &[]);
        // Retire one file of a two-file run, and all of a one-file run.
        index.publish(vec![], &["r0f1", "r3f0"]);
        model.retain(|(_, file, _)| file != "r0f1" && file != "r3f0");
        model.sort();
        assert!(index.merge_all().expect("merge"));
        let runs = index.view().run_list();
        assert_eq!(runs.len(), 1);
        let mut got: Vec<(u64, String, u64)> = Vec::new();
        // The merge drops the rows of retired files, so every row is live.
        runs[0].for_each_row(|word, file, position| {
            got.push((word, file.to_string(), position));
        });
        got.sort();
        assert_eq!(got, model);
    }

    /// Rows at explicit positions (a read-back) index exactly like the same
    /// rows at contiguous ones (a write); a file declared with no indexed row
    /// is still covered; retiring a file ends its coverage.
    #[test]
    fn explicit_positions_and_coverage() {
        let keys: Vec<i64> = (0..100).map(|k| k % 17).collect();
        let mut written = RunBuilder::new(encoder());
        written
            .add_batch("a", 0, &column(&keys[..60]))
            .expect("add");
        written
            .add_batch("a", 60, &column(&keys[60..]))
            .expect("add");
        let mut read_back = RunBuilder::new(encoder());
        // A read-back returns the same rows in another order.
        let order: Vec<usize> = (0..100).rev().collect();
        let shuffled: Vec<i64> = order.iter().map(|&i| keys[i]).collect();
        let positions: Vec<u64> = order.iter().map(|&i| i as u64).collect();
        read_back
            .add_batch_at("a", &positions, &column(&shuffled))
            .expect("add");
        read_back.add_file("empty").expect("declare");
        assert!(matches!(
            read_back.add_batch_at("a", &positions[..3], &column(&shuffled)),
            Err(Error::PositionCount {
                rows: 100,
                positions: 3
            })
        ));
        let (written, read_back) = (TieredIndex::new(encoder()), {
            let index = TieredIndex::new(encoder());
            index.publish(vec![read_back.finish().expect("run")], &[]);
            index
        });
        written.publish(
            vec![{
                let mut b = RunBuilder::new(encoder());
                b.add_batch("a", 0, &column(&keys)).expect("add");
                b.finish().expect("run")
            }],
            &[],
        );
        for k in 0..20 {
            let collect = |index: &TieredIndex| {
                let mut rows = Vec::new();
                index.candidates(encoded(k).as_bytes(), |c| rows.push(format!("{c:?}")));
                rows.sort();
                rows
            };
            assert_eq!(collect(&written), collect(&read_back), "key {k}");
        }
        let view = read_back.view();
        assert!(view.covers("a") && view.covers("empty") && !view.covers("b"));
        read_back.publish(Vec::new(), &["empty"]);
        assert!(read_back.view().covers("a") && !read_back.view().covers("empty"));
        assert!(
            view.covers("empty"),
            "a view keeps the coverage it was taken with"
        );
    }

    /// `finish` indexes exactly the rows added, in word order, for shuffled,
    /// clustered, descending and duplicate-heavy input.
    #[test]
    fn a_run_holds_every_row_in_word_order() {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let shuffled: Vec<i64> = (0..3_000)
            .map(|_| (next() % 100_000).cast_signed())
            .collect();
        let duplicates: Vec<i64> = (0..3_000).map(|_| (next() % 7).cast_signed()).collect();
        for (name, keys) in [
            ("shuffled", shuffled),
            ("clustered", (0..3_000).collect::<Vec<i64>>()),
            ("descending", (0..3_000).rev().collect()),
            ("duplicates", duplicates),
        ] {
            let mut builder = RunBuilder::new(encoder());
            for (batch, chunk) in keys.chunks(97).enumerate() {
                builder
                    .add_batch("f", (batch * 97) as u64, &column(chunk))
                    .expect("add");
            }
            let run = builder.finish().expect("finish");
            let mut got: Vec<(u64, u64)> = Vec::new();
            run.for_each_row(|word, _, position| got.push((word, position)));
            let mut expected: Vec<(u64, u64)> = keys
                .iter()
                .enumerate()
                .map(|(position, &key)| (word(key), position as u64))
                .collect();
            expected.sort_unstable();
            assert_eq!(got, expected, "{name}");
        }
    }

    #[test]
    fn writes_and_compactions_match_a_model() {
        const KEYS: i64 = 50;
        for seed in 0..20 {
            let mut rng = StdRng::seed_from_u64(seed);
            let index = TieredIndex::new(encoder());
            let mut model = Model::default();
            let mut live: BTreeSet<String> = BTreeSet::new();
            let mut next_file = 0;
            for _ in 0..60 {
                match rng.random_range(0..7) {
                    // A write produces files and a run.
                    0..=3 | 6 => {
                        let mut builder = RunBuilder::new(encoder());
                        let mut keys: Vec<i64> = (0..rng.random_range(0..40))
                            .map(|_| rng.random_range(0..KEYS))
                            .collect();
                        for _ in 0..rng.random_range(1..4) {
                            let file = format!("f{next_file}");
                            next_file += 1;
                            let take = keys.len().min(rng.random_range(0..30));
                            let batch: Vec<i64> = keys.drain(..take).collect();
                            builder
                                .add_batch(&file, 0, &column(&batch))
                                .expect("add batch");
                            for (position, &k) in (0_u64..).zip(&batch) {
                                model
                                    .rows
                                    .entry(k)
                                    .or_default()
                                    .insert((file.clone(), position));
                            }
                            live.insert(file);
                        }
                        // Anything left over goes into one last file.
                        let file = format!("f{next_file}");
                        next_file += 1;
                        builder
                            .add_batch(&file, 0, &column(&keys))
                            .expect("add batch");
                        for (position, &k) in (0_u64..).zip(&keys) {
                            model
                                .rows
                                .entry(k)
                                .or_default()
                                .insert((file.clone(), position));
                        }
                        live.insert(file);
                        index.publish(vec![builder.finish().expect("run")], &[]);
                    }
                    // Background run maintenance.
                    4 => while index.merge_step().expect("merge") {},
                    // A compaction rewrites some live files into one new file.
                    _ => {
                        let victims: Vec<String> = live
                            .iter()
                            .filter(|_| rng.random_bool(0.4))
                            .cloned()
                            .collect();
                        // The victims' rows leave the live set; the rewritten
                        // file below holds new rows of its own.
                        for rows in model.rows.values_mut() {
                            rows.retain(|(file, _)| !victims.contains(file));
                        }
                        for victim in &victims {
                            live.remove(victim);
                        }
                        let file = format!("f{next_file}");
                        next_file += 1;
                        let rewritten: Vec<i64> = (0..rng.random_range(0..30))
                            .map(|_| rng.random_range(0..KEYS))
                            .collect();
                        let mut builder = RunBuilder::new(encoder());
                        builder
                            .add_batch(&file, 0, &column(&rewritten))
                            .expect("add batch");
                        for (position, &k) in (0_u64..).zip(&rewritten) {
                            model
                                .rows
                                .entry(k)
                                .or_default()
                                .insert((file.clone(), position));
                        }
                        live.insert(file);
                        let retired: Vec<&str> = victims.iter().map(String::as_str).collect();
                        index.publish(vec![builder.finish().expect("run")], &retired);
                    }
                }
                check(&index, &model, &live, KEYS);
            }
        }
    }

    #[test]
    fn a_run_is_retired_only_when_all_its_files_are() {
        let index = TieredIndex::new(encoder());
        let mut builder = RunBuilder::new(encoder());
        builder.add_batch("a", 0, &column(&[1, 2])).expect("a");
        builder.add_batch("b", 0, &column(&[1])).expect("b");
        index.publish(vec![builder.finish().expect("run")], &[]);
        index.publish(Vec::new(), &["a"]);
        assert_eq!(index.view().runs(), 1, "b is still live");
        index.publish(Vec::new(), &["b"]);
        assert_eq!(index.view().runs(), 0);
    }

    #[test]
    fn reconcile_retires_files_seen_live_and_then_gone() {
        let index = TieredIndex::new(encoder());
        let run = |file: &str, key: i64| {
            let mut builder = RunBuilder::new(encoder());
            builder.add_batch(file, 0, &column(&[key])).expect("batch");
            builder.finish().expect("run")
        };
        index.publish(vec![run("a", 1), run("b", 2)], &[]);
        let live = |names: &[&'static str]| {
            names
                .iter()
                .copied()
                .collect::<std::collections::HashSet<&str>>()
        };
        // `b` is written but not visible yet: never seen, so kept.
        assert_eq!(index.reconcile(&live(&["a"])), 0);
        assert_eq!(index.view().runs(), 2);
        // Both visible, then a refresh replaces them with `c`.
        assert_eq!(index.reconcile(&live(&["a", "b"])), 0);
        index.publish(vec![run("c", 3)], &[]);
        assert_eq!(index.reconcile(&live(&["c"])), 2);
        let view = index.view();
        assert_eq!(view.runs(), 1);
        let mut found = Vec::new();
        for k in 1..=3 {
            view.candidates(encoded(k).as_bytes(), |c| found.push((k, format!("{c:?}"))));
        }
        assert_eq!(found.len(), 1, "only c's key remains: {found:?}");
        // Unchanged file sets change nothing.
        assert_eq!(index.reconcile(&live(&["c"])), 0);
    }

    #[test]
    fn publish_visible_retires_files_already_gone() {
        let index = TieredIndex::new(encoder());
        let mut builder = RunBuilder::new(encoder());
        builder.add_batch("a", 0, &column(&[1])).expect("batch");
        builder.add_batch("b", 0, &column(&[2])).expect("batch");
        let live = |names: &[&'static str]| names.iter().copied().collect::<HashSet<&str>>();
        // `b` was compacted away before the run was published.
        index.publish_visible(vec![builder.finish().expect("run")], &live(&["a", "c"]));
        let view = index.view();
        assert!(view.covers("a"));
        assert!(
            !view.covers("b"),
            "a file gone before publish is never covered"
        );
        // `a` was seen at publish, so leaving the set retires it and drops the run.
        assert_eq!(index.reconcile(&live(&["c"])), 1);
        assert_eq!(index.view().runs(), 0);
        // A run with no live file is not published at all.
        let mut gone = RunBuilder::new(encoder());
        gone.add_batch("d", 0, &column(&[4])).expect("batch");
        index.publish_visible(vec![gone.finish().expect("run")], &live(&["c"]));
        assert_eq!(index.view().runs(), 0);
    }

    #[test]
    fn unseen_files_are_retired_after_the_grace() {
        let index = TieredIndex::new(encoder());
        let mut builder = RunBuilder::new(encoder());
        builder.add_batch("never", 0, &column(&[1])).expect("batch");
        index.publish(vec![builder.finish().expect("run")], &[]);
        let live: HashSet<&str> = std::iter::once("other").collect();
        for _ in 1..UNSEEN_GRACE {
            assert_eq!(index.reconcile(&live), 0);
            assert!(index.view().covers("never"), "kept within the grace");
        }
        assert_eq!(index.reconcile(&live), 1);
        assert_eq!(index.view().runs(), 0);
    }

    #[test]
    fn rejects_positions_beyond_forty_bits() {
        let mut builder = RunBuilder::new(encoder());
        assert!(matches!(
            builder.add_batch("a", POSITION_MASK, &column(&[1, 2])),
            Err(Error::Position { .. })
        ));
        builder
            .add_batch("a", POSITION_MASK, &column(&[1]))
            .expect("the largest position is accepted");
        // A first position so large that adding the row count overflows is
        // refused, not wrapped into range.
        assert!(matches!(
            builder.add_batch("a", u64::MAX, &column(&[1, 2])),
            Err(Error::Position {
                position: u64::MAX,
                ..
            })
        ));
        // A refused batch adds no file, so a run cannot claim to cover a
        // file whose rows it does not hold.
        let mut refused = RunBuilder::new(encoder());
        assert!(
            refused
                .add_batch("b", POSITION_MASK, &column(&[1, 2]))
                .is_err()
        );
        assert!(
            refused.files().is_empty(),
            "a refused batch's file is registered: {:?}",
            refused.files()
        );
    }
}

#[cfg(test)]
mod merge_tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arrow_array::Int64Array;
    use arrow_schema::DataType;
    use parking_lot::Mutex;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::EncodedKey;
    use crate::KeyField;

    fn encoder() -> KeyEncoder {
        KeyEncoder::new(vec![KeyField::new(DataType::Int64, false)]).expect("int64 key")
    }

    fn encoded(k: i64) -> EncodedKey {
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![k]))];
        let encoder = encoder();
        let bound = encoder.bind(&columns).expect("bind");
        EncodedKey::from_row(&bound, 0)
    }

    fn visible(view: &IndexView, k: i64) -> bool {
        let mut found = false;
        view.candidates(encoded(k).as_bytes(), |_| found = true);
        found
    }

    /// A merger loops while a writer publishes one-key runs and retires some
    /// of their files. A key whose file was never retired stays visible in
    /// every view, and at the end exactly those keys are.
    #[test]
    fn merges_racing_writes_and_retirements_keep_live_rows_only() {
        const RUNS: usize = 1_500;
        let index = TieredIndex::new(encoder());
        let committed = AtomicUsize::new(0);
        let retired: Mutex<BTreeSet<usize>> = Mutex::new(BTreeSet::new());
        let stop = AtomicBool::new(false);
        let merges = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    if index.merge_step().expect("merge") {
                        merges.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            for reader in 0..3 {
                let (index, committed, retired, stop) = (&index, &committed, &retired, &stop);
                scope.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(reader);
                    while !stop.load(Ordering::Relaxed) {
                        let upto = committed.load(Ordering::Acquire);
                        if upto == 0 {
                            continue;
                        }
                        let k = rng.random_range(0..upto);
                        // Retirements only happen to keys below `upto`; check
                        // the set before taking the view, so a key seen as
                        // unretired was unretired when the view was taken.
                        if retired.lock().contains(&k) {
                            continue;
                        }
                        let view = index.view();
                        if !retired.lock().contains(&k) {
                            assert!(
                                visible(&view, i64::try_from(k).expect("key")),
                                "live key {k} lost"
                            );
                        }
                    }
                });
            }
            let mut rng = StdRng::seed_from_u64(99);
            for r in 0..RUNS {
                let mut builder = RunBuilder::new(encoder());
                let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![
                    i64::try_from(r).expect("key"),
                ]))];
                builder
                    .add_batch(&format!("f{r}"), 0, &columns)
                    .expect("batch");
                index.publish(vec![builder.finish().expect("run")], &[]);
                committed.store(r + 1, Ordering::Release);
                if r > 0 && rng.random_bool(0.3) {
                    let victim = rng.random_range(0..r);
                    let newly = retired.lock().insert(victim);
                    if newly {
                        index.publish(Vec::new(), &[&format!("f{victim}")]);
                    }
                }
            }
            stop.store(true, Ordering::Relaxed);
        });
        assert!(merges.load(Ordering::Relaxed) > 20, "the merger barely ran");
        while index.merge_step().expect("merge") {}
        let view = index.view();
        let retired = retired.into_inner();
        for r in 0..RUNS {
            assert_eq!(
                visible(&view, i64::try_from(r).expect("key")),
                !retired.contains(&r),
                "key {r}"
            );
        }
        assert!(
            view.runs() < 40,
            "{} runs remain after merging",
            view.runs()
        );
    }
}

#[cfg(test)]
mod persist_tests {
    use std::collections::BTreeMap;

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::DataType;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::KeyField;
    use crate::persist::Error;

    fn run(rng: &mut StdRng, rows: usize) -> IndexRun {
        let encoder = KeyEncoder::new(vec![
            KeyField::new(DataType::Int64, true),
            KeyField::new(DataType::Utf8, false),
        ])
        .expect("key types");
        let mut builder = RunBuilder::new(encoder);
        for file in 0..3 {
            let ids: Int64Array = (0..rows)
                .map(|_| rng.random_bool(0.9).then(|| rng.random_range(0..200_i64)))
                .collect();
            let names: StringArray = (0..rows)
                .map(|_| Some(["", "a", "a\0", "ab"][rng.random_range(0..4)]))
                .collect();
            let columns: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(names)];
            builder
                .add_batch(&format!("f{file}.vortex"), 0, &columns)
                .expect("add");
        }
        builder.finish().expect("finish")
    }

    fn contents(run: &IndexRun) -> BTreeMap<u64, Vec<(String, u64)>> {
        let mut out: BTreeMap<u64, Vec<(String, u64)>> = BTreeMap::new();
        run.for_each_row(|word, file, position| {
            out.entry(word)
                .or_default()
                .push((file.to_string(), position));
        });
        out
    }

    #[test]
    fn runs_round_trip_through_bytes() {
        let mut rng = StdRng::seed_from_u64(7);
        for rows in [0, 1, 50, 5_000] {
            let original = run(&mut rng, rows);
            let bytes = original.to_bytes();
            let restored = IndexRun::from_bytes(&bytes).expect("round trip");
            assert_eq!(contents(&original), contents(&restored), "{rows} rows");
            assert_eq!(restored.files(), original.files());
            // Lookups (filter included) answer the same.
            for (word, rows) in contents(&original) {
                let mut got = Vec::new();
                restored.lookup(word, |file, position| {
                    got.push((file.to_string(), position));
                });
                assert_eq!(got, rows);
            }
        }
    }

    #[test]
    fn corrupt_or_truncated_bytes_are_rejected() {
        let mut rng = StdRng::seed_from_u64(8);
        let bytes = run(&mut rng, 2_000).to_bytes();
        for at in [0, 5, 13, bytes.len() / 2, bytes.len() - 9, bytes.len() - 1] {
            let mut flipped = bytes.clone();
            flipped[at] ^= 0x10;
            assert!(
                IndexRun::from_bytes(&flipped).is_err(),
                "flip at {at} accepted"
            );
        }
        for len in [0, 10, 19, bytes.len() / 3, bytes.len() - 1] {
            assert!(
                IndexRun::from_bytes(&bytes[..len]).is_err(),
                "truncation to {len} accepted"
            );
        }
        // A run of another format version is rejected, not misread.
        let mut other = run(&mut rng, 10).to_bytes();
        other[4..8].copy_from_slice(&(crate::persist::VERSION + 1).to_le_bytes());
        assert_eq!(
            IndexRun::from_bytes(&other).err(),
            Some(Error::Format {
                kind: crate::persist::KIND_RUN
            })
        );
    }

    /// A run whose only word has `postings` as its posting stream, sealed
    /// with a valid checksum, as a writer bug or a crafted file could leave.
    fn sealed_with_postings(postings: Vec<u8>, rows: usize) -> Vec<u8> {
        IndexRun::from_parts(
            vec![Arc::from("a")].into(),
            vec![7].into(),
            vec![word_proof::offset_slot(0)].into(),
            postings.into(),
            rows,
        )
        .to_bytes()
    }

    fn varints(values: &[u64]) -> Vec<u8> {
        let mut out = Vec::new();
        for &value in values {
            varint::put(&mut out, value);
        }
        out
    }

    /// A posting stream that passes the checksum but would overflow, end
    /// early, repeat a posting or disagree with the row count is rejected: a
    /// lookup over it would panic, or skip rows after the damage.
    #[test]
    fn a_run_whose_postings_do_not_decode_in_full_is_rejected() {
        let intact = sealed_with_postings(varints(&[2, 0, 5]), 2);
        let run = IndexRun::from_bytes(&intact).expect("an intact stream loads");
        let mut got = Vec::new();
        run.lookup(7, |file, position| got.push((file.to_string(), position)));
        assert_eq!(got, vec![("a".to_string(), 0), ("a".to_string(), 5)]);

        for (what, postings, rows) in [
            ("overflowing deltas", varints(&[2, u64::MAX, 5]), 2),
            ("a count past the bytes", varints(&[3, 0, 5]), 3),
            ("a repeated posting", varints(&[2, 4, 0]), 2),
            ("a zero count", varints(&[0]), 0),
            ("a wrong row count", varints(&[2, 0, 5]), 3),
            ("a position past 40 bits", varints(&[2, 0, 1 << 41]), 2),
        ] {
            assert_eq!(
                IndexRun::from_bytes(&sealed_with_postings(postings, rows)).err(),
                Some(Error::Corrupt),
                "{what} accepted"
            );
        }
    }

    /// Two words whose slots share one posting stream, leaving the stream of
    /// the second unreferenced, are rejected even when the row count adds
    /// up: the second word's lookups would read the first's rows and miss
    /// its own.
    #[test]
    fn a_run_whose_words_share_a_posting_stream_is_rejected() {
        let mut postings = varints(&[2, 0, 5]);
        let second = u32::try_from(postings.len()).expect("small");
        postings.extend(varints(&[2, 1, 2]));
        let sealed = |slots: Vec<u32>, rows: usize| {
            IndexRun::from_parts(
                vec![Arc::from("a")].into(),
                vec![7, 9].into(),
                slots.into(),
                postings.clone().into(),
                rows,
            )
            .to_bytes()
        };
        let intact = IndexRun::from_bytes(&sealed(
            vec![word_proof::offset_slot(0), word_proof::offset_slot(second)],
            4,
        ))
        .expect("streams laid out in word order load");
        let mut got = Vec::new();
        intact.lookup(9, |file, position| got.push((file.to_string(), position)));
        assert_eq!(got, vec![("a".to_string(), 1), ("a".to_string(), 3)]);

        let shared = sealed(
            vec![word_proof::offset_slot(0), word_proof::offset_slot(0)],
            4,
        );
        assert_eq!(IndexRun::from_bytes(&shared).err(), Some(Error::Corrupt));
    }

    /// The persisted bytes of runs a fixed workload builds and merges, as
    /// `(length, digest)`. A change to how runs are built, merged or written
    /// that alters a single byte fails here; the format itself is versioned
    /// by [`crate::persist::VERSION`], which this test does not replace.
    #[test]
    fn run_bytes_are_pinned() {
        let digest = |run: &IndexRun| {
            let bytes = run.to_bytes();
            (bytes.len(), hash_index::hash_key_bytes_oneshot(&bytes))
        };
        let mut rng = StdRng::seed_from_u64(0xB17E5);
        // Three files of a nullable compound key with escaped strings.
        let compound = run(&mut rng, 5_000);
        let empty = run(&mut rng, 0);

        // Exact words, repeated keys across files, explicit positions with a
        // repeated row and one too wide for a slot, and a file with no row.
        let int = KeyEncoder::new(vec![KeyField::new(DataType::Int64, false)]).expect("int64");
        let int_column =
            |keys: Vec<i64>| -> Vec<ArrayRef> { vec![Arc::new(Int64Array::from(keys))] };
        let mut builder = RunBuilder::new(int.clone());
        for file in 0..4 {
            let keys: Vec<i64> = (0..3_000).map(|_| rng.random_range(0..2_000)).collect();
            builder
                .add_batch(
                    &format!("i{file}"),
                    rng.random_range(0..1_000),
                    &int_column(keys),
                )
                .expect("add");
        }
        builder
            .add_batch_at("g", &[5, 5, 1 << 33, 9], &int_column(vec![1, 1, 2, 3]))
            .expect("add at");
        builder.add_file("empty").expect("file");
        let exact = builder.finish().expect("finish");

        // Hashed words merged from single- and multi-file runs, one with a
        // retired file and one with every file live.
        let utf8 = KeyEncoder::new(vec![KeyField::new(DataType::Utf8, false)]).expect("utf8");
        let index = TieredIndex::new(utf8.clone());
        let mut runs = Vec::new();
        for run_no in 0..7 {
            let mut builder = RunBuilder::new(utf8.clone());
            for file_no in 0..if run_no < 2 { 2 } else { 1 } {
                let keys: Vec<String> = (0..400)
                    .map(|_| format!("svc\0{}", rng.random_range(0..600)))
                    .collect();
                let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(keys))];
                builder
                    .add_batch(&format!("r{run_no}f{file_no}"), 0, &columns)
                    .expect("add");
            }
            runs.push(builder.finish().expect("finish"));
        }
        index.publish(runs, &[]);
        index.publish(vec![], &["r0f1", "r4f0"]);
        assert!(index.merge_all().expect("merge"));
        let merged = index.view().run_list();
        assert_eq!(merged.len(), 1);

        assert_eq!(
            [
                digest(&compound),
                digest(&empty),
                digest(&exact),
                digest(&merged[0])
            ],
            [
                (35_584, 0xBF9B_0B3C_93C3_4467),
                (87, 0x1B08_B93F_A4DA_911D),
                (49_415, 0xEFBA_2365_1D7F_F00C),
                (12_741, 0x4AC0_9C32_E0D6_B532),
            ]
        );
    }

    /// Words out of order, or offsets that do not span the postings, are
    /// rejected even under a valid checksum: a run read out of order would
    /// miss rows.
    #[test]
    fn a_run_with_unordered_words_is_rejected() {
        let mut rng = StdRng::seed_from_u64(9);
        let mut original = run(&mut rng, 500);
        original.words.swap(0, 1);
        assert_eq!(
            IndexRun::from_bytes(&original.to_bytes()).err(),
            Some(Error::Corrupt)
        );
    }
}
