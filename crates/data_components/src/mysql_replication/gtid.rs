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
    /// interval `[start, end + 1)`. Zero-GNO ranges cannot occur (GNOs start at
    /// 1) and are skipped defensively rather than sent as an invalid interval.
    #[must_use]
    pub fn to_sids(&self) -> Vec<Sid<'static>> {
        self.intervals
            .iter()
            .filter(|(_, ranges)| !ranges.is_empty())
            .map(|(uuid, ranges)| {
                let gno_intervals: Vec<GnoInterval> = ranges
                    .iter()
                    .filter_map(|&(start, end)| {
                        // Inclusive [start, end] -> half-open [start, end + 1).
                        // `end` is a real GNO (<= i64::MAX in practice); saturate
                        // rather than wrap should it ever be u64::MAX.
                        GnoInterval::check_and_new(start, end.saturating_add(1)).ok()
                    })
                    .collect();
                Sid::new(*uuid.as_bytes()).with_intervals(gno_intervals)
            })
            .collect()
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
        let sids = set.to_sids();
        assert_eq!(sids.len(), 1);
        let intervals = sids[0].intervals();
        assert_eq!(intervals.len(), 1);
        // Inclusive 1-5 -> half-open [1, 6). The wire `end` is exclusive.
        assert_eq!(intervals[0], GnoInterval::new(1, 6));
    }

    #[test]
    fn to_sids_round_trips_uuid_bytes() {
        let set = GtidSet::parse(&format!("{U1}:1")).expect("parse");
        let sids = set.to_sids();
        let uuid = Uuid::parse_str(U1).expect("uuid");
        assert_eq!(sids[0].uuid(), *uuid.as_bytes());
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
