// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

// Tuple fields encode kind, outcome, attempt identity, inline bytes and length.
pub type Record = (u8, u8, usize, [u8; 2], usize);
pub fn record(k: u8, o: u8, id: usize) -> Record {
    let b = k.wrapping_add(o);
    (k, o, id, [b, b.wrapping_add(1)], 1 + id % 2)
}
pub fn payload(r: Record) -> Vec<u8> {
    r.3[..r.4].to_vec()
}
pub fn size(r: Record) -> usize {
    r.4
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub struct Store {
    pub window: Vec<Record>,
    pub first: usize,
    pub index: Vec<(usize, usize)>,
    pub batches: Vec<(usize, Vec<Record>)>,
    pub pending: Vec<(usize, Vec<Record>)>,
    pub root: Vec<Record>,
    pub floor: usize,
    pub byte_pressure: bool,
    pub count_pressure: bool,
    pub byte_cut: bool,
    pub count_cut: bool,
    pub deleted: Vec<usize>,
    /// A checkpoint root written at a batch end, which the next commit publishes.
    pub staged: Option<(usize, Vec<Record>)>,
    pub revived: bool,
    pub conflict: bool,
}
impl Store {
    pub fn cut(&self) -> (usize, bool) {
        let mut bytes = 0;
        let mut c = 0;
        for r in &self.window {
            if c == 3 || bytes + size(*r) > 3 {
                break;
            }
            bytes += size(*r);
            c += 1;
        }
        (c, c == 3 || c < self.window.len())
    }
    pub fn observe(&mut self, mode: &str) {
        let (c, finalized) = self.cut();
        let c = if mode.contains("no-stable-cut") {
            self.window.len().saturating_sub(1)
        } else {
            c
        };
        if c > 0 && (finalized || mode.contains("no-final")) {
            let v = (self.first, self.window[..c].to_vec());
            if !self.pending.contains(&v) {
                self.pending.push(v);
                self.pending.sort();
            }
        }
    }
    pub fn create(&mut self, j: usize) {
        let (action, v) = self.pending[j].clone();
        if let Some((_, old)) = self.batches.iter().find(|(b, _)| *b == action) {
            self.conflict |= *old != v;
        } else {
            self.revived |= self.deleted.contains(&action);
            self.batches.push((action, v));
            self.batches.sort();
        }
    }
    pub fn append(&mut self, r: Record, mode: &str) -> bool {
        if self.window.len() + 1 > 3
            || self.window.iter().map(|r| size(*r)).sum::<usize>() + size(r) > 5
        {
            let (c, f) = self.cut();
            if c == 0 || !f {
                return false;
            }
            if !mode.contains("no-exists")
                && !self.batches.iter().any(|(action, _)| *action == self.first)
            {
                return false;
            }
            self.byte_pressure |= self.window.iter().map(|r| size(*r)).sum::<usize>() + size(r) > 5;
            self.count_pressure |= self.window.len() + 1 > 3;
            self.count_cut |= c == 3;
            self.byte_cut |= c < 3;
            self.index.push((
                self.first,
                self.first + c - usize::from(mode.contains("no-range")),
            ));
            self.window.drain(..c);
            self.first += c;
        }
        self.window.push(r);
        if let Some((c, root)) = self.staged.take() {
            self.root = root;
            self.floor = c;
            self.index.retain(|&(_, b)| b > c);
        }
        true
    }
    pub fn decode(&self) -> Option<Vec<Record>> {
        self.read("")
    }
    pub fn read(&self, _mode: &str) -> Option<Vec<Record>> {
        let mut v = self.root[..self.floor].to_vec();
        let mut p = self.floor;
        for &(action, c) in &self.index {
            if action != p {
                return None;
            }
            let b = &self.batches.iter().find(|(x, _)| *x == action)?.1;
            if b.len() != c - action {
                return None;
            }
            v.extend(b);
            p = c;
        }
        if p != self.first {
            return None;
        }
        v.extend(&self.window);
        Some(v)
    }
    /// The commit numbers a checkpoint may name: a batch end above the floor,
    /// or the record before the window, so the floor never splits a batch.
    pub fn checkpoint_ends(&self) -> Vec<usize> {
        (self.floor + 1..=self.first)
            .filter(|&c| c == self.first || self.index.iter().any(|&(_, b)| b == c))
            .collect()
    }
    /// Writes the checkpoint root at `c`; the floor moves with the next commit.
    pub fn checkpoint(&mut self, c: usize) -> bool {
        if self.staged.is_some() || !self.checkpoint_ends().contains(&c) {
            return false;
        }
        let Some(v) = self.decode() else { return false };
        self.staged = Some((c, v[..c].to_vec()));
        true
    }
    pub fn publish(&mut self, c: usize, _mode: &str) -> bool {
        let Some(v) = self.decode() else { return false };
        if c <= self.floor
            || c > self.first
            || !(c == self.first || self.index.iter().any(|&(_, b)| b == c))
        {
            return false;
        }
        self.root = v[..c].to_vec();
        self.floor = c;
        self.index.retain(|&(_, b)| b > c);
        true
    }
    pub fn gc(&mut self) {
        for (action, b) in &self.batches {
            if *action + b.len() <= self.floor && !self.deleted.contains(action) {
                self.deleted.push(*action);
            }
        }
        self.batches
            .retain(|(action, b)| *action + b.len() > self.floor);
    }
}

/// The conditional object's identity. Every mutation increases at least one
/// component and decreases none; the protocol needs no extra nonce.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Head {
    pub(crate) pos: usize,
    pub(crate) lease: usize,
    pub(crate) floor: usize,
}
impl Head {
    /// A bounded symbolic attempt identifier, used only in record payloads.
    /// Exact CAS comparisons always compare the complete head.
    pub(crate) fn identity(self) -> usize {
        self.pos + self.lease + self.floor
    }
    pub(crate) fn accepts(self, base: Self) -> bool {
        self == base
    }
}

/// Shared storage-based classification: absence alone is never rejection while
/// the original condition can still win. Retained evidence takes precedence.
pub(crate) fn resolve_attempt(evidence: Option<bool>, same_base: bool) -> u8 {
    match evidence {
        Some(true) => 1,
        Some(false) => 2,
        None if !same_base => 2,
        None => 0,
    }
}

pub(crate) fn pin_admitted(base: usize, floor: usize) -> bool {
    base >= floor
}

/// A closed-generation reference can be admitted only from the CAS base's
/// reachable set, never from the uploader's earlier observation.
pub(crate) fn admits_reference(closed: bool, reachable: bool) -> bool {
    !closed || reachable
}
