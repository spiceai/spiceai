// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Data files uploaded before the commit that names them. A writer holding the
//! dataset's lease uploads each data file to a fresh final key under the
//! dataset prefix, `<generation>-<id>`, and only after every upload it names has
//! returned does it send a commit: a conditional publication of the head, on
//! the exact head version it read and its lease generation, that adds the
//! files to the committed set. Readers read the head and fetch the files it
//! names; they never list the store. A commit's response can be lost: the
//! writer then holds an unknown outcome, may resend the identical request, and
//! resolves it from the head alone: committed when the head names its files,
//! aborted once the head has moved past the request's base (the exact-base
//! condition can then never pass), and otherwise still unknown. A takeover is
//! a head commit that raises the lease generation, so it fences the previous
//! holder, whose uploads may still be in flight and land afterwards. A writer
//! can crash, losing what it knew; its requests and uploads already sent can
//! still land. An upload no committed head names is an orphan: readers never
//! see it, and once its generation is fenced it can be deleted, because no
//! commit naming it can pass the lease condition. (The `retention` runs model
//! age-based collection; this graph uses the generation alone.)
//!
//! In the `intent` runs the sweep is the lease holder's, and it uses no
//! generation in the key: it deletes an unnamed file unless the holder's own
//! intent protects it. A writer records the key it is about to upload before
//! it sends the upload, and keeps it through the upload and its commit until
//! the outcome resolves; the record lives in the writer's process, so a crash
//! or a takeover loses it, and then the lease condition is what keeps the file
//! out of every commit. (Cayenne's orphan sweep adds an age floor on the store's
//! clock as a backstop; the `retention` runs model age.)
//!
//! Controls: `no-upload-before-commit` lets a commit name a file whose upload
//! has not returned; `no-committed-read` makes the reader list the store
//! instead of reading the head; `no-resolve-by-head` resolves an unknown
//! outcome as aborted when the head lacks its files, without waiting for the
//! head to move past the request's base; `sweep-ignores-intent` lets the
//! holder's sweep delete a file its own write in flight still holds (reclaiming
//! too early); `adopt-by-listing` lets a commit name every unnamed file the
//! store lists, as a maintenance pass that derives its file set from a listing
//! would.
use stateright::{Model, Property};

/// Writer processes.
const WRITERS: usize = 2;
/// Lease takeovers.
const TAKES: u8 = 2;
/// Writer crashes.
const CRASHES: u8 = 1;
/// Commit attempts across writers.
const ATTEMPTS: u8 = 3;
/// Identical resends of an attempt with an unknown outcome.
const RETRIES: u8 = 1;
/// Head reads by the reader.
const READS: u8 = 2;

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    /// Uploads across writers.
    uploads: u8,
}

/// A data file's final key: the lease generation it was uploaded under and a
/// unique id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    pub generation: u8,
    pub id: u8,
}
/// A key and the bytes stored under it, as the uploading writer's index: keys
/// are unique, so the bytes are fixed by the upload that created the key.
pub type File = (Key, u8);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Request {
    attempt: u8,
    writer: usize,
    generation: u8,
    base: u8,
    files: Vec<File>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Writer {
    /// The lease generation this writer believes it holds.
    generation: Option<u8>,
    /// Uploads sent whose response has not arrived.
    started: Vec<File>,
    /// Uploads whose response arrived and that no commit has named yet.
    finished: Vec<File>,
    /// The outstanding commit attempt, and whether its response was lost.
    pending: Option<(Request, bool)>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    version: u8,
    lease: u8,
    /// The committed file set the head names.
    head: Vec<File>,
    /// The committed file set at every head version (a ghost for the checks).
    history: Vec<Vec<File>>,
    /// The object store's data files.
    store: Vec<File>,
    /// Upload requests in flight, by writer.
    puts: Vec<(usize, File)>,
    /// Commit requests in flight.
    net: Vec<Request>,
    writers: Vec<Writer>,
    /// The reader's head version and the keys it will fetch, and the next one.
    snapshot: Option<(u8, Vec<Key>)>,
    fetched: usize,
    /// Every fetch: snapshot version, key and the bytes returned, if any.
    observed: Vec<(u8, Key, Option<u8>)>,
    /// Keys of attempts refused or resolved as aborted (a ghost).
    aborted: Vec<Key>,
    /// Keys a writer dropped unnamed at a crash, takeover or fence (a ghost).
    dropped: Vec<Key>,
    /// Each resolved unknown outcome: its files and whether it resolved committed.
    resolutions: Vec<(Vec<File>, bool)>,
    uploads: u8,
    attempts: u8,
    takes: u8,
    crashes: u8,
    retries: u8,
    reads: u8,
    accepted: bool,
    fenced_refused: bool,
    /// A fenced writer, still running, had an upload land after the takeover.
    fenced_landed: bool,
    reclaimed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    StartUpload(usize),
    FinishUpload(usize),
    Commit(usize),
    Deliver(usize),
    Drop(usize),
    Timeout(usize),
    Retry(usize),
    Resolve(usize),
    Takeover(usize),
    Crash(usize),
    Read,
    Fetch,
    Sweep(usize),
}

fn keys(files: &[File]) -> impl Iterator<Item = Key> + '_ {
    files.iter().map(|(k, _)| *k)
}
fn insert_sorted<T: Ord + Copy>(set: &mut Vec<T>, items: impl IntoIterator<Item = T>) {
    for item in items {
        if let Err(at) = set.binary_search(&item) {
            set.insert(at, item);
        }
    }
}

impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            uploads: u8::try_from(bound.max(1)).unwrap_or(1),
        }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    /// The store's files the committed head does not name, sorted.
    fn unnamed(s: &State) -> Vec<File> {
        s.store
            .iter()
            .filter(|f| !s.head.contains(f))
            .copied()
            .collect()
    }
    /// The writer holding the current lease, if one runs.
    fn holder(s: &State) -> Option<&Writer> {
        s.writers.iter().find(|w| w.generation == Some(s.lease))
    }
    /// Whether `writer` recorded `file` before uploading it and has not seen the
    /// outcome of the commit that names it yet.
    fn intends(writer: &Writer, file: File) -> bool {
        writer.started.contains(&file)
            || writer.finished.contains(&file)
            || writer
                .pending
                .as_ref()
                .is_some_and(|(request, _)| request.files.contains(&file))
    }
    /// How many files of `files` the committed head names.
    fn named(s: &State, files: &[File]) -> usize {
        files.iter().filter(|f| s.head.contains(f)).count()
    }
    /// Whether `action` is enabled in `s`. The model lists only enabled
    /// actions, so a seeded walk (`WALK`) runs to its length rather than
    /// stopping at the first disabled pick.
    fn enabled(&self, s: &State, action: &Action) -> bool {
        let writer = |w: usize| &s.writers[w];
        match *action {
            Action::StartUpload(w) => {
                writer(w).generation.is_some()
                    && writer(w).pending.is_none()
                    && s.uploads < self.uploads
            }
            Action::FinishUpload(i) => i < s.puts.len(),
            Action::Commit(w) => {
                writer(w).generation.is_some()
                    && writer(w).pending.is_none()
                    && s.attempts < ATTEMPTS
                    && (!writer(w).finished.is_empty()
                        || self.has("no-upload-before-commit") && !writer(w).started.is_empty()
                        || self.has("adopt-by-listing") && !Self::unnamed(s).is_empty())
            }
            Action::Deliver(i) | Action::Drop(i) => i < s.net.len(),
            Action::Timeout(w) => matches!(writer(w).pending, Some((_, false))),
            Action::Retry(w) => match &writer(w).pending {
                Some((request, true)) => {
                    s.retries < RETRIES && !s.net.iter().any(|r| r.attempt == request.attempt)
                }
                _ => false,
            },
            // The request could still land while the head is at its base, so
            // the outcome stays unknown unless the head names its files.
            Action::Resolve(w) => match &writer(w).pending {
                Some((request, true)) => {
                    Self::named(s, &request.files) == request.files.len()
                        || s.version != request.base
                        || self.has("no-resolve-by-head")
                }
                _ => false,
            },
            Action::Takeover(w) => s.takes < TAKES && writer(w).generation != Some(s.lease),
            Action::Crash(w) => s.crashes < CRASHES && *writer(w) != Writer::default(),
            Action::Read => s.reads < READS,
            Action::Fetch => s
                .snapshot
                .as_ref()
                .is_some_and(|(_, listed)| s.fetched < listed.len()),
            // An unnamed file of a fenced generation: no commit naming it can
            // pass the lease condition. In the `intent` runs: an unnamed file
            // the holder's own intent does not protect.
            Action::Sweep(i) => s.store.get(i).is_some_and(|f| {
                !s.head.contains(f)
                    && if self.has("intent") {
                        Self::holder(s).is_some_and(|holder| {
                            self.has("sweep-ignores-intent") || !Self::intends(holder, *f)
                        })
                    } else {
                        f.0.generation < s.lease
                    }
            }),
        }
    }
}

impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let mut writers = vec![Writer::default(); WRITERS];
        writers[0].generation = Some(1);
        vec![State {
            version: 0,
            lease: 1,
            head: vec![],
            history: vec![vec![]],
            store: vec![],
            puts: vec![],
            net: vec![],
            writers,
            snapshot: None,
            fetched: 0,
            observed: vec![],
            aborted: vec![],
            dropped: vec![],
            resolutions: vec![],
            uploads: 0,
            attempts: 0,
            takes: 0,
            crashes: 0,
            retries: 0,
            reads: 0,
            accepted: false,
            fenced_refused: false,
            fenced_landed: false,
            reclaimed: false,
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for w in 0..WRITERS {
            a.extend([
                Action::StartUpload(w),
                Action::Commit(w),
                Action::Timeout(w),
                Action::Retry(w),
                Action::Resolve(w),
                Action::Takeover(w),
                Action::Crash(w),
            ]);
        }
        a.extend((0..s.puts.len()).map(Action::FinishUpload));
        for i in 0..s.net.len() {
            a.extend([Action::Deliver(i), Action::Drop(i)]);
        }
        a.extend([Action::Read, Action::Fetch]);
        a.extend((0..s.store.len()).map(Action::Sweep));
        a.retain(|action| self.enabled(s, action));
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        if !self.enabled(s, &action) {
            return None;
        }
        let mut next = s.clone();
        match action {
            Action::StartUpload(w) => {
                // A writer that believes it holds the lease uploads to a fresh
                // final key, fenced or not.
                let generation = s.writers[w].generation?;
                let file = (
                    Key {
                        generation,
                        id: s.uploads,
                    },
                    u8::try_from(w).ok()?,
                );
                next.uploads += 1;
                next.puts.push((w, file));
                next.writers[w].started.push(file);
            }
            Action::FinishUpload(i) => {
                let (w, file) = *s.puts.get(i)?;
                next.puts.remove(i);
                insert_sorted(&mut next.store, [file]);
                // The response reaches the writer only if it still knows the upload.
                let writer = &mut next.writers[w];
                if let Some(at) = writer.started.iter().position(|f| *f == file) {
                    writer.started.remove(at);
                    writer.finished.push(file);
                    // A running writer, fenced by another's takeover, is told
                    // its upload landed.
                    next.fenced_landed |= file.0.generation < s.lease;
                }
            }
            Action::Commit(w) => {
                let writer = &s.writers[w];
                let generation = writer.generation?;
                let mut files = writer.finished.clone();
                if self.has("no-upload-before-commit") {
                    files.extend(writer.started.iter().copied());
                }
                if self.has("adopt-by-listing") {
                    let adopted: Vec<File> = Self::unnamed(s)
                        .into_iter()
                        .filter(|f| !files.contains(f))
                        .collect();
                    files.extend(adopted);
                }
                files.sort_unstable();
                let request = Request {
                    attempt: s.attempts,
                    writer: w,
                    generation,
                    base: s.version,
                    files,
                };
                next.attempts += 1;
                let writer = &mut next.writers[w];
                writer.finished.clear();
                if self.has("no-upload-before-commit") {
                    writer.started.clear();
                }
                writer.pending = Some((request.clone(), false));
                next.net.push(request);
            }
            Action::Deliver(i) => {
                // The store evaluates the conditional publication: the exact
                // base version and the lease generation.
                let request = s.net.get(i)?.clone();
                next.net.remove(i);
                let accepted = s.version == request.base && s.lease == request.generation;
                if accepted {
                    next.version += 1;
                    insert_sorted(&mut next.head, request.files.iter().copied());
                    next.history.push(next.head.clone());
                    next.accepted = true;
                }
                let writer = &mut next.writers[request.writer];
                let waiting = writer
                    .pending
                    .as_ref()
                    .is_some_and(|(p, lost)| p.attempt == request.attempt && !lost);
                if waiting {
                    writer.pending = None;
                    if !accepted {
                        insert_sorted(&mut next.aborted, keys(&request.files));
                        if s.lease != request.generation {
                            // The refusal tells a fenced writer it lost the lease.
                            next.fenced_refused = true;
                            let unnamed: Vec<Key> = keys(&writer.started)
                                .chain(keys(&writer.finished))
                                .collect();
                            *writer = Writer::default();
                            insert_sorted(&mut next.dropped, unnamed);
                        }
                    }
                }
            }
            Action::Drop(i) => {
                s.net.get(i)?;
                next.net.remove(i);
            }
            Action::Timeout(w) => {
                let (request, _) = s.writers[w].pending.as_ref()?;
                next.writers[w].pending = Some((request.clone(), true));
            }
            Action::Retry(w) => {
                let (request, _) = s.writers[w].pending.as_ref()?;
                next.retries += 1;
                next.net.push(request.clone());
            }
            Action::Resolve(w) => {
                let (request, _) = s.writers[w].pending.as_ref()?;
                let committed = Self::named(s, &request.files) == request.files.len();
                if !committed {
                    insert_sorted(&mut next.aborted, keys(&request.files));
                }
                next.resolutions.push((request.files.clone(), committed));
                next.writers[w].pending = None;
            }
            Action::Takeover(w) => {
                // A head commit raising the generation; the new holder starts
                // with nothing of its own, and its earlier sends stay in flight.
                next.takes += 1;
                next.lease += 1;
                next.version += 1;
                next.history.push(next.head.clone());
                let old = &s.writers[w];
                insert_sorted(
                    &mut next.dropped,
                    keys(&old.started).chain(keys(&old.finished)),
                );
                next.writers[w] = Writer {
                    generation: Some(next.lease),
                    ..Writer::default()
                };
            }
            Action::Crash(w) => {
                let old = &s.writers[w];
                next.crashes += 1;
                insert_sorted(
                    &mut next.dropped,
                    keys(&old.started).chain(keys(&old.finished)),
                );
                next.writers[w] = Writer::default();
            }
            Action::Read => {
                next.reads += 1;
                let listed = if self.has("no-committed-read") {
                    keys(&s.store).collect()
                } else {
                    keys(&s.head).collect()
                };
                next.snapshot = Some((s.version, listed));
                next.fetched = 0;
            }
            Action::Fetch => {
                let (version, listed) = s.snapshot.as_ref()?;
                let key = *listed.get(s.fetched)?;
                let bytes = s.store.iter().find(|(k, _)| *k == key).map(|(_, b)| *b);
                next.observed.push((*version, key, bytes));
                next.fetched += 1;
            }
            Action::Sweep(i) => {
                next.store.remove(i);
                next.reclaimed = true;
            }
        }
        (next != *s).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("committed files exist", |_, s: &State| {
                s.head.iter().all(|f| s.store.contains(f))
            }),
            Property::always("reader sees only committed files", |_, s: &State| {
                s.observed.iter().all(|&(version, key, bytes)| {
                    bytes.is_some_and(|b| {
                        s.history
                            .get(usize::from(version))
                            .is_some_and(|set| set.contains(&(key, b)))
                    })
                })
            }),
            Property::always(
                "aborted or abandoned upload never visible to a reader",
                |_, s: &State| {
                    s.observed.iter().all(|(_, key, _)| {
                        s.aborted.binary_search(key).is_err()
                            && s.dropped.binary_search(key).is_err()
                    })
                },
            ),
            Property::always("unknown outcome resolves all-or-nothing", |_, s: &State| {
                s.resolutions.iter().all(|(files, committed)| {
                    let named = Self::named(s, files);
                    if *committed {
                        named == files.len()
                    } else {
                        named == 0
                    }
                })
            }),
            Property::sometimes("commit accepted", |_, s: &State| s.accepted),
            Property::sometimes("orphan upload after an aborted commit", |_, s: &State| {
                s.store
                    .iter()
                    .any(|f| s.aborted.binary_search(&f.0).is_ok() && !s.head.contains(f))
            }),
            Property::sometimes("fenced commit refused", |_, s: &State| s.fenced_refused),
            Property::sometimes(
                "fenced writer's upload lands after a takeover",
                |_, s: &State| s.fenced_landed,
            ),
            Property::sometimes("unknown outcome resolved as committed", |_, s: &State| {
                s.resolutions.iter().any(|(_, committed)| *committed)
            }),
            Property::sometimes("unknown outcome resolved as aborted", |_, s: &State| {
                s.resolutions.iter().any(|(_, committed)| !*committed)
            }),
            Property::sometimes("reader fetches a committed file", |_, s: &State| {
                s.observed.iter().any(|(_, _, bytes)| bytes.is_some())
            }),
            Property::sometimes("orphan reclaimed", |_, s: &State| s.reclaimed),
        ]
    }
}

/// The real operation one action of a `data-files` trace stands for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// `StartUpload(w)`: writer `w` sends a PUT of a fresh data file to its
    /// final key under the dataset prefix (the replay names `key` uniquely).
    PutSent { writer: usize, key: Key },
    /// `FinishUpload(i)`: that PUT lands in the store; `acknowledged` when the
    /// writer still knows the upload and records it as uploaded.
    PutLanded {
        writer: usize,
        key: Key,
        acknowledged: bool,
    },
    /// `Commit(w)`: writer `w` sends a conditional head publication on
    /// `base_version` and `generation` that adds `files`.
    CommitSent {
        writer: usize,
        attempt: u8,
        generation: u8,
        base_version: u8,
        files: Vec<Key>,
    },
    /// `Deliver(i)`: the store evaluates a commit request; `accepted` when it
    /// published, `fenced` when the lease generation refused it, `answered`
    /// when the writer was waiting for the response (otherwise it was lost).
    CommitApplied {
        writer: usize,
        attempt: u8,
        accepted: bool,
        fenced: bool,
        answered: bool,
    },
    /// `Drop(i)`: a commit request is lost before reaching the store.
    CommitDropped { writer: usize, attempt: u8 },
    /// `Timeout(w)`: the writer gives up on the response: an unknown outcome.
    ResponseLost { writer: usize, attempt: u8 },
    /// `Retry(w)`: the identical request is sent again.
    CommitResent { writer: usize, attempt: u8 },
    /// `Resolve(w)`: the writer reads the head and resolves its unknown outcome.
    Resolved {
        writer: usize,
        attempt: u8,
        committed: bool,
    },
    /// `Takeover(w)`: writer `w` takes the lease by a head commit raising the
    /// generation to `generation`.
    Takeover { writer: usize, generation: u8 },
    /// `Crash(w)`: the writer process restarts and forgets its state.
    Crash { writer: usize },
    /// `Read`: the reader reads the head (`version`, `files`).
    ReadHead { version: u8, files: Vec<Key> },
    /// `Fetch`: the reader GETs the next file of its snapshot; `found` when the
    /// store returned it.
    Fetch { key: Key, found: bool },
    /// `Sweep(i)`: the orphan sweep deletes an unnamed fenced-generation file.
    Swept { key: Key },
}

/// What one action of a `data-files` trace does, as the model takes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub action: String,
    pub op: Op,
    /// The head after the action: version, lease generation and file set.
    pub version: u8,
    pub lease: u8,
    pub head: Vec<File>,
    /// The store's data files after the action.
    pub store: Vec<File>,
}

/// Step `actions` through the model in `mode`, or `None` when one is not
/// enabled where it occurs.
#[must_use]
pub fn steps(mode: &str, bound: usize, actions: &[String]) -> Option<Vec<Step>> {
    let protocol = Protocol::new(mode, bound);
    let mut state = protocol.init_states().pop()?;
    let mut out = Vec::with_capacity(actions.len());
    for text in actions {
        let (name, arguments) = match text.split_once('(') {
            Some((name, rest)) => (
                name,
                rest.strip_suffix(')')?
                    .split(',')
                    .map(|part| part.trim().parse::<usize>().ok())
                    .collect::<Option<Vec<_>>>()?,
            ),
            None => (text.as_str(), Vec::new()),
        };
        let action = match (name, arguments.as_slice()) {
            ("StartUpload", &[w]) => Action::StartUpload(w),
            ("FinishUpload", &[i]) => Action::FinishUpload(i),
            ("Commit", &[w]) => Action::Commit(w),
            ("Deliver", &[i]) => Action::Deliver(i),
            ("Drop", &[i]) => Action::Drop(i),
            ("Timeout", &[w]) => Action::Timeout(w),
            ("Retry", &[w]) => Action::Retry(w),
            ("Resolve", &[w]) => Action::Resolve(w),
            ("Takeover", &[w]) => Action::Takeover(w),
            ("Crash", &[w]) => Action::Crash(w),
            ("Read", &[]) => Action::Read,
            ("Fetch", &[]) => Action::Fetch,
            ("Sweep", &[i]) => Action::Sweep(i),
            _ => return None,
        };
        let mut enabled = vec![];
        protocol.actions(&state, &mut enabled);
        if !enabled.contains(&action) {
            return None;
        }
        let next = protocol.next_state(&state, action.clone())?;
        let pending = |w: usize| state.writers[w].pending.as_ref().map(|(p, _)| p.attempt);
        let op = match action {
            Action::StartUpload(w) => Op::PutSent {
                writer: w,
                key: next.writers[w].started.last()?.0,
            },
            Action::FinishUpload(i) => {
                let (w, (key, _)) = state.puts[i];
                Op::PutLanded {
                    writer: w,
                    key,
                    acknowledged: next.writers[w].finished.iter().any(|f| f.0 == key),
                }
            }
            Action::Commit(w) => {
                let request = next.net.last()?;
                Op::CommitSent {
                    writer: w,
                    attempt: request.attempt,
                    generation: request.generation,
                    base_version: request.base,
                    files: keys(&request.files).collect(),
                }
            }
            Action::Deliver(i) => {
                let request = &state.net[i];
                Op::CommitApplied {
                    writer: request.writer,
                    attempt: request.attempt,
                    accepted: next.version > state.version,
                    fenced: request.generation != state.lease,
                    answered: state.writers[request.writer]
                        .pending
                        .as_ref()
                        .is_some_and(|(p, lost)| p.attempt == request.attempt && !lost),
                }
            }
            Action::Drop(i) => Op::CommitDropped {
                writer: state.net[i].writer,
                attempt: state.net[i].attempt,
            },
            Action::Timeout(w) => Op::ResponseLost {
                writer: w,
                attempt: pending(w)?,
            },
            Action::Retry(w) => Op::CommitResent {
                writer: w,
                attempt: pending(w)?,
            },
            Action::Resolve(w) => Op::Resolved {
                writer: w,
                attempt: pending(w)?,
                committed: next.resolutions.last()?.1,
            },
            Action::Takeover(w) => Op::Takeover {
                writer: w,
                generation: next.lease,
            },
            Action::Crash(w) => Op::Crash { writer: w },
            Action::Read => {
                let (version, files) = next.snapshot.clone()?;
                Op::ReadHead { version, files }
            }
            Action::Fetch => {
                let &(_, key, bytes) = next.observed.last()?;
                Op::Fetch {
                    key,
                    found: bytes.is_some(),
                }
            }
            Action::Sweep(i) => Op::Swept {
                key: state.store[i].0,
            },
        };
        out.push(Step {
            action: text.clone(),
            op,
            version: next.version,
            lease: next.lease,
            head: next.head.clone(),
            store: next.store.clone(),
        });
        state = next;
    }
    Some(out)
}
