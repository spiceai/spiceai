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

//! `MySQL` GTID set — the failover-safe replacement for a file+offset cursor.
//!
//! A GTID (`server_uuid:sequence`) is a globally unique transaction identity:
//! the same transaction carries the same GTID on every server that applied it.
//! An *executed set* is the union of applied GTIDs, grouped by source UUID with
//! per-UUID sequence ranges — e.g.
//! `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:8-10,…:1-3`. Resuming with
//! `COM_BINLOG_DUMP_GTID` + this set lets any server in a replication topology
//! compute the correct start point, so a persisted position survives failover.
//!
//! Text ranges are **inclusive** (`1-5` = 1,2,3,4,5); the wire type
//! [`GnoInterval`] is **half-open `[start, end)`**. [`GtidSet::to_sids`] performs
//! that conversion (`1-5` → `[1, 6)`), which is the single easiest place to
//! introduce an off-by-one — hence the dedicated tests below.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use mysql_async::{GnoInterval, Sid};
use uuid::Uuid;

/// An inclusive `[start, end]` range of GNOs (transaction sequence numbers)
/// for a single source UUID. `start <= end` is an invariant maintained by
/// [`GtidSet`].
type Interval = (u64, u64);

/// A `MySQL` executed GTID set: per-source-UUID inclusive sequence ranges,
/// coalesced and sorted. Cloneable and cheap to union into as transactions
/// commit.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GtidSet {
    /// `BTreeMap` keeps UUIDs in a stable order so [`Self::to_string`] is
    /// deterministic (round-trips, comparable in tests/logs).
    intervals: BTreeMap<Uuid, Vec<Interval>>,
}

impl GtidSet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.intervals.values().all(Vec::is_empty)
    }

    /// Number of distinct source UUIDs in the set.
    #[must_use]
    pub fn uuid_count(&self) -> usize {
        self.intervals.values().filter(|v| !v.is_empty()).count()
    }

    /// Parse a `MySQL` GTID-set string (`SHOW BINARY LOG STATUS`'s
    /// `Executed_Gtid_Set`, `@@GLOBAL.gtid_executed`, or a persisted value).
    ///
    /// Whitespace and newlines between UUID blocks are tolerated — the server
    /// pretty-prints long sets across lines. An empty/whitespace-only string is
    /// the empty set. Any malformed block is a hard error rather than silent
    /// partial data (a truncated set would under-report applied transactions
    /// and re-stream — or, worse, over-report and skip — on resume).
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut set = Self::new();
        for block in raw.split(',') {
            let block = block.trim();
            if block.is_empty() {
                // Trailing comma, or an all-whitespace/empty input.
                continue;
            }
            let mut parts = block.split(':');
            let uuid_str = parts
                .next()
                .ok_or_else(|| format!("empty GTID block in {raw:?}"))?
                .trim();
            let uuid = Uuid::parse_str(uuid_str)
                .map_err(|e| format!("invalid GTID source uuid {uuid_str:?}: {e}"))?;
            let mut had_range = false;
            for range in parts {
                let range = range.trim();
                if range.is_empty() {
                    continue;
                }
                let (start, end) = parse_range(range, raw)?;
                set.add_interval(uuid, start, end);
                had_range = true;
            }
            if !had_range {
                return Err(format!(
                    "GTID block for {uuid_str:?} has no sequence interval in {raw:?}"
                ));
            }
        }
        Ok(set)
    }

    /// Fold a single committed transaction's GTID into the set.
    pub fn add(&mut self, uuid: Uuid, gno: u64) {
        self.add_interval(uuid, gno, gno);
    }

    /// Insert an inclusive `[start, end]` range, coalescing with overlapping or
    /// adjacent existing ranges for the same UUID.
    fn add_interval(&mut self, uuid: Uuid, start: u64, end: u64) {
        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        let ranges = self.intervals.entry(uuid).or_default();
        ranges.push((start, end));
        coalesce(ranges);
    }

    /// Convert to the `COM_BINLOG_DUMP_GTID` request representation.
    ///
    /// Each inclusive text range `[start, end]` becomes a half-open wire
    /// interval `[start, end + 1)`. Any interval that cannot be represented on
    /// the wire (only possible for a corrupt/hand-edited executed set — e.g. a
    /// GNO beyond `i64::MAX` — since [`Self::parse`] rejects zero and a real
    /// server never issues such values) is a hard error: dropping it would
    /// silently under-report the applied set and make the source re-stream
    /// already-applied transactions on resume. Fail loudly instead.
    pub fn to_sids(&self) -> Result<Vec<Sid<'static>>, String> {
        self.intervals
            .iter()
            .filter(|(_, ranges)| !ranges.is_empty())
            .map(|(uuid, ranges)| {
                let gno_intervals = ranges
                    .iter()
                    .map(|&(start, end)| {
                        // Inclusive [start, end] -> half-open [start, end + 1).
                        // `end` is a real GNO (<= i64::MAX in practice); saturate
                        // rather than wrap should it ever be u64::MAX.
                        GnoInterval::check_and_new(start, end.saturating_add(1)).map_err(|e| {
                            format!("GTID interval {start}-{end} for source {uuid} is not representable on the wire: {e}")
                        })
                    })
                    .collect::<Result<Vec<GnoInterval>, String>>()?;
                Ok(Sid::new(*uuid.as_bytes()).with_intervals(gno_intervals))
            })
            .collect()
    }

    /// The intersection of two executed sets: the sequences present in BOTH.
    ///
    /// Used by the shared binlog pump to compute a resume set across its members
    /// — the GTIDs that *every* member has durably applied. Positioning a
    /// `COM_BINLOG_DUMP_GTID` from this intersection re-sends any transaction a
    /// member is still missing (members already ahead suppress the replay via
    /// their own committed floor), the GTID analog of resuming from the minimum
    /// file+offset across members. A UUID present in only one set, or ranges
    /// that do not overlap, contribute nothing.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let mut out = Self::new();
        for (uuid, a_ranges) in &self.intervals {
            let Some(b_ranges) = other.intervals.get(uuid) else {
                continue;
            };
            // Two-pointer sweep over sorted, coalesced, inclusive ranges. The
            // output is naturally sorted and disjoint, so no re-coalescing.
            let mut merged: Vec<Interval> = Vec::new();
            let (mut i, mut j) = (0usize, 0usize);
            while i < a_ranges.len() && j < b_ranges.len() {
                let (a1, a2) = a_ranges[i];
                let (b1, b2) = b_ranges[j];
                let lo = a1.max(b1);
                let hi = a2.min(b2);
                if lo <= hi {
                    merged.push((lo, hi));
                }
                // Advance whichever interval ends first (ties advance `a`).
                if a2 <= b2 {
                    i += 1;
                } else {
                    j += 1;
                }
            }
            if !merged.is_empty() {
                out.intervals.insert(*uuid, merged);
            }
        }
        out
    }

    /// Whether `self` is a subset of `other`: every transaction in `self` is
    /// also in `other`.
    ///
    /// This is the GTID analog of "is the persisted binlog file still present
    /// on the source": a resume checkpoint's executed set must be a subset of
    /// the source's current `@@gtid_executed`. When it is not — a `RESET
    /// MASTER`, a rebuilt server with a fresh `server_uuid`, or a different
    /// source entirely — the source no longer contains the checkpoint's
    /// transactions, so resuming from it would position `COM_BINLOG_DUMP_GTID`
    /// from a set the server cannot honor and silently stream against a
    /// diverged history. The empty set is a subset of anything (`gtid_mode =
    /// ON` with no transactions applied is always resumable).
    #[must_use]
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.intervals.iter().all(|(uuid, ranges)| {
            if ranges.is_empty() {
                return true;
            }
            let Some(other_ranges) = other.intervals.get(uuid) else {
                return false;
            };
            // Both sides are sorted, coalesced, disjoint inclusive ranges, so a
            // single forward sweep suffices: each `self` interval must fall
            // entirely within one `other` interval. `j` only advances (both
            // ascending), so cover-checking is linear.
            let mut j = 0usize;
            ranges.iter().all(|&(start, end)| {
                while j < other_ranges.len() && other_ranges[j].1 < start {
                    j += 1;
                }
                j < other_ranges.len() && other_ranges[j].0 <= start && end <= other_ranges[j].1
            })
        })
    }
}

impl std::fmt::Display for GtidSet {
    /// Canonical `MySQL` text form: `uuid:1-5:8-10,uuid2:1-3`. Single-value
    /// ranges render as `n`, not `n-n`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (uuid, ranges) in &self.intervals {
            if ranges.is_empty() {
                continue;
            }
            if !first {
                f.write_char(',')?;
            }
            first = false;
            // MySQL renders UUIDs lowercase, hyphenated.
            write!(f, "{uuid}")?;
            for &(start, end) in ranges {
                if start == end {
                    write!(f, ":{start}")?;
                } else {
                    write!(f, ":{start}-{end}")?;
                }
            }
        }
        Ok(())
    }
}

/// Parse a `start-end` or single-`n` inclusive range.
fn parse_range(range: &str, raw: &str) -> Result<Interval, String> {
    if let Some((start, end)) = range.split_once('-') {
        let start: u64 = start
            .trim()
            .parse()
            .map_err(|e| format!("invalid GTID range start {start:?} in {raw:?}: {e}"))?;
        let end: u64 = end
            .trim()
            .parse()
            .map_err(|e| format!("invalid GTID range end {end:?} in {raw:?}: {e}"))?;
        if start == 0 || end == 0 {
            return Err(format!("GTID sequence numbers start at 1, got {range:?}"));
        }
        Ok((start, end))
    } else {
        let n: u64 = range
            .parse()
            .map_err(|e| format!("invalid GTID sequence {range:?} in {raw:?}: {e}"))?;
        if n == 0 {
            return Err(format!("GTID sequence numbers start at 1, got {range:?}"));
        }
        Ok((n, n))
    }
}

/// Sort and merge overlapping/adjacent inclusive ranges in place. Adjacency
/// merges too: `[1,5]` + `[6,8]` → `[1,8]` (the two describe a contiguous run).
fn coalesce(ranges: &mut Vec<Interval>) {
    if ranges.len() <= 1 {
        return;
    }
    ranges.sort_unstable();
    let mut merged: Vec<Interval> = Vec::with_capacity(ranges.len());
    for &(start, end) in ranges.iter() {
        match merged.last_mut() {
            // Overlap or adjacency: extend. `prev_end + 1 >= start` covers the
            // adjacent case; saturating_add guards the u64::MAX edge.
            Some(last) if last.1.saturating_add(1) >= start => {
                if end > last.1 {
                    last.1 = end;
                }
            }
            _ => merged.push((start, end)),
        }
    }
    *ranges = merged;
}

#[cfg(test)]
mod tests {
    use super::*;

    const U1: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const U2: &str = "5d1c0d8c-71ca-11e1-9e33-c80aa9429999";

    #[test]
    fn parse_and_display_round_trip() {
        let s = format!("{U1}:1-5:8-10,{U2}:1-3");
        let set = GtidSet::parse(&s).expect("parse");
        assert_eq!(set.to_string(), s);
    }

    #[test]
    fn parse_empty_is_empty_set() {
        assert!(GtidSet::parse("").expect("parse").is_empty());
        assert!(GtidSet::parse("   \n  ").expect("parse").is_empty());
    }

    #[test]
    fn intersect_keeps_only_ranges_in_both() {
        // Overlapping ranges on a shared UUID intersect to their overlap; a
        // UUID present in only one set drops out entirely.
        let a = GtidSet::parse(&format!("{U1}:1-10,{U2}:1-5")).expect("parse a");
        let b = GtidSet::parse(&format!("{U1}:4-20")).expect("parse b");
        assert_eq!(a.intersect(&b).to_string(), format!("{U1}:4-10"));
        // Intersection is commutative.
        assert_eq!(b.intersect(&a).to_string(), format!("{U1}:4-10"));
    }

    #[test]
    fn intersect_handles_multi_range_and_gaps() {
        // Split ranges intersect piecewise; non-overlapping ranges contribute
        // nothing (the gap in `a` is preserved).
        let a = GtidSet::parse(&format!("{U1}:1-5:8-12")).expect("parse a");
        let b = GtidSet::parse(&format!("{U1}:3-9:11-20")).expect("parse b");
        assert_eq!(a.intersect(&b).to_string(), format!("{U1}:3-5:8-9:11-12"));
    }

    #[test]
    fn intersect_with_empty_or_disjoint_is_empty() {
        let a = GtidSet::parse(&format!("{U1}:1-10")).expect("parse a");
        // Empty set: the most-behind member drives resume from nothing.
        assert!(a.intersect(&GtidSet::new()).is_empty());
        assert!(GtidSet::new().intersect(&a).is_empty());
        // Disjoint UUIDs share nothing.
        let other = GtidSet::parse(&format!("{U2}:1-10")).expect("parse other");
        assert!(a.intersect(&other).is_empty());
    }

    #[test]
    fn subset_of_covers_reset_and_divergence() {
        let checkpoint = GtidSet::parse(&format!("{U1}:1-100")).expect("parse checkpoint");

        // Normal restart: the source has advanced past the checkpoint.
        let advanced = GtidSet::parse(&format!("{U1}:1-150")).expect("parse advanced");
        assert!(
            checkpoint.is_subset_of(&advanced),
            "a checkpoint the source has kept and grown past is resumable"
        );

        // Exact match resumes (subset is reflexive).
        assert!(checkpoint.is_subset_of(&checkpoint));

        // RESET MASTER / rebuilt server: the source's executed set is under a
        // brand-new server_uuid, so the checkpoint's UUID is absent entirely.
        let reset = GtidSet::parse(&format!("{U2}:1-3")).expect("parse reset");
        assert!(
            !checkpoint.is_subset_of(&reset),
            "a source whose executed set no longer contains the checkpoint UUID is not resumable"
        );

        // Divergence / restore-from-older-backup: same UUID, but the source has
        // fewer transactions than the checkpoint claims were applied.
        let rolled_back = GtidSet::parse(&format!("{U1}:1-50")).expect("parse rolled_back");
        assert!(
            !checkpoint.is_subset_of(&rolled_back),
            "a source missing transactions the checkpoint already applied is not resumable"
        );

        // A hole inside the covered range is not a subset either.
        let holed = GtidSet::parse(&format!("{U1}:1-40:60-150")).expect("parse holed");
        assert!(
            !checkpoint.is_subset_of(&holed),
            "a gap inside the source's set breaks coverage of the checkpoint range"
        );
    }

    #[test]
    fn empty_subset_of_anything_and_multi_uuid() {
        let empty = GtidSet::new();
        let any = GtidSet::parse(&format!("{U1}:1-10")).expect("parse");
        // gtid_mode = ON with zero applied transactions is always resumable.
        assert!(empty.is_subset_of(&any));
        assert!(empty.is_subset_of(&empty));
        // A non-empty set is never a subset of the empty set.
        assert!(!any.is_subset_of(&empty));

        // Every UUID block must be covered: one missing block fails the whole
        // check even when the other is present.
        let two = GtidSet::parse(&format!("{U1}:1-5,{U2}:1-5")).expect("parse two");
        let only_one = GtidSet::parse(&format!("{U1}:1-9")).expect("parse only_one");
        assert!(!two.is_subset_of(&only_one));
        let both = GtidSet::parse(&format!("{U1}:1-9,{U2}:1-9")).expect("parse both");
        assert!(two.is_subset_of(&both));
    }

    #[test]
    fn parse_tolerates_newlines_between_blocks() {
        // The server pretty-prints long sets across lines.
        let s = format!("{U1}:1-5,\n{U2}:1-3");
        let set = GtidSet::parse(&s).expect("parse");
        assert_eq!(set.uuid_count(), 2);
    }

    #[test]
    fn single_value_range_renders_without_dash() {
        let set = GtidSet::parse(&format!("{U1}:7")).expect("parse");
        assert_eq!(set.to_string(), format!("{U1}:7"));
    }

    #[test]
    fn add_coalesces_adjacent_and_overlapping() {
        let uuid = Uuid::parse_str(U1).expect("uuid");
        let mut set = GtidSet::new();
        for gno in [1, 2, 3, 5, 4] {
            set.add(uuid, gno);
        }
        // 1..=5 collapses to a single range regardless of insertion order.
        assert_eq!(set.to_string(), format!("{U1}:1-5"));
        // Adjacent range merges.
        set.add(uuid, 6);
        assert_eq!(set.to_string(), format!("{U1}:1-6"));
        // A gap creates a second range.
        set.add(uuid, 9);
        assert_eq!(set.to_string(), format!("{U1}:1-6:9"));
    }

    #[test]
    fn add_is_idempotent() {
        let uuid = Uuid::parse_str(U1).expect("uuid");
        let mut set = GtidSet::new();
        set.add(uuid, 3);
        set.add(uuid, 3);
        assert_eq!(set.to_string(), format!("{U1}:3"));
    }

    #[test]
    fn to_sids_converts_inclusive_to_half_open() {
        let set = GtidSet::parse(&format!("{U1}:1-5")).expect("parse");
        let sids = set.to_sids().expect("to_sids");
        assert_eq!(sids.len(), 1);
        let intervals = sids[0].intervals();
        assert_eq!(intervals.len(), 1);
        // Inclusive 1-5 -> half-open [1, 6). The wire `end` is exclusive.
        assert_eq!(intervals[0], GnoInterval::new(1, 6));
    }

    #[test]
    fn to_sids_round_trips_uuid_bytes() {
        let set = GtidSet::parse(&format!("{U1}:1")).expect("parse");
        let sids = set.to_sids().expect("to_sids");
        let uuid = Uuid::parse_str(U1).expect("uuid");
        assert_eq!(sids[0].uuid(), *uuid.as_bytes());
    }

    #[test]
    fn to_sids_errors_on_unrepresentable_interval() {
        // A GNO at u64::MAX can only come from a corrupt/hand-edited set (a real
        // server never issues it). `to_sids` must surface it as an error, not
        // silently drop the interval and under-report the applied set.
        let set = GtidSet::parse(&format!("{U1}:18446744073709551615")).expect("parse");
        set.to_sids()
            .expect_err("an unrepresentable GTID interval must error, not be dropped");
    }

    #[test]
    fn parse_rejects_bad_uuid() {
        GtidSet::parse("not-a-uuid:1-5").expect_err("invalid uuid must be rejected");
    }

    #[test]
    fn parse_rejects_zero_gno() {
        GtidSet::parse(&format!("{U1}:0")).expect_err("zero gno must be rejected");
        GtidSet::parse(&format!("{U1}:0-5")).expect_err("zero range start must be rejected");
    }

    #[test]
    fn parse_rejects_block_without_interval() {
        GtidSet::parse(U1).expect_err("block without a sequence interval must be rejected");
    }

    #[test]
    fn parse_rejects_non_numeric_range() {
        GtidSet::parse(&format!("{U1}:a-b")).expect_err("non-numeric range must be rejected");
    }
}
