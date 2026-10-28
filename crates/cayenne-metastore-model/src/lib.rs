// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use stateright::{Checker, Expectation, Model};
use std::time::Instant;
mod action_compatibility;
mod commits;
mod concurrent;
mod core;
mod dataset_lifecycle;
mod floors;
mod generations;
mod orphan_cleanup;
mod position_deletes;
mod read_programs;
mod removal;
mod seq_order;
mod snapshot_leases;
mod source_positions;
mod state_rules;
mod transactions;
mod window;

/// A finite checker invocation with an explicit expected downstream failure.
#[derive(Clone, Copy, Debug)]
pub struct Run {
    pub name: &'static str,
    family: &'static str,
    mode: &'static str,
    bound: usize,
    seed: Option<u64>,
    failure: &'static str,
}
/// All runs of the design, including safeguard-removal controls.
#[must_use]
pub fn runs() -> Vec<Run> {
    let mut declared = vec![
        Run {
            name: "dataset-lifecycle",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "dataset-lifecycle-no-pin-log-retention",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle-no-pin-log-retention",
            bound: 2,
            seed: None,
            failure: "I7 composed query snapshots",
        },
        Run {
            name: "dataset-lifecycle-no-index-checkpoint",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle-no-index-checkpoint",
            bound: 2,
            seed: None,
            failure: "I8 composed checkpoint and PK recovery",
        },
        Run {
            name: "dataset-lifecycle-no-coverage",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle-no-coverage",
            bound: 2,
            seed: None,
            failure: "I9 composed table rows",
        },
        Run {
            name: "dataset-lifecycle-no-retirement-interlock",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle-no-retirement-interlock",
            bound: 2,
            seed: None,
            failure: "I4 composed retained objects",
        },
        Run {
            name: "dataset-lifecycle-no-pin-recheck",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle-no-pin-recheck",
            bound: 2,
            seed: None,
            failure: "I7 composed query snapshots",
        },
        Run {
            name: "dataset-lifecycle-no-table-scope",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle-no-table-scope",
            bound: 2,
            seed: None,
            failure: "per-table composed allocation admitted",
        },
        Run {
            name: "dataset-lifecycle-no-commit-number",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle-no-commit-number",
            bound: 2,
            seed: None,
            failure: "I8 composed commit names one state",
        },
        Run {
            name: "dataset-lifecycle-4-simulation-7",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle",
            bound: 4,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "dataset-lifecycle-4-simulation-42",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle",
            bound: 4,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "dataset-lifecycle-4-simulation-20261024",
            family: "dataset-lifecycle",
            mode: "dataset-lifecycle",
            bound: 4,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "pin-expiry",
            family: "snapshot-leases",
            mode: "expiry",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "pin-expiry-missing-as-empty",
            family: "snapshot-leases",
            mode: "expiry-missing-as-empty",
            bound: 2,
            seed: None,
            failure: "snapshot rows or error",
        },
        Run {
            name: "pin-expiry-no-lapse-recheck",
            family: "snapshot-leases",
            mode: "expiry-no-lapse-recheck",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "transaction-begin-pin",
            family: "snapshot-leases",
            mode: "transaction",
            bound: 1,
            seed: None,
            failure: "",
        },
        Run {
            name: "transaction-begin-pin-ignore-transactions",
            family: "snapshot-leases",
            mode: "transaction-ignore-transactions",
            bound: 1,
            seed: None,
            failure: "I7 transaction begin snapshot readable",
        },
        Run {
            name: "source-positions",
            family: "source-positions",
            mode: "source",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "source-ack-before-acceptance",
            family: "source-positions",
            mode: "source-ack-before-acceptance",
            bound: 2,
            seed: None,
            failure: "I2 source acknowledgement covered",
        },
        Run {
            name: "source-position-before-data",
            family: "source-positions",
            mode: "source-position-before-data",
            bound: 2,
            seed: None,
            failure: "I10 logged source position has its rows",
        },
        Run {
            name: "source-position-after-data",
            family: "source-positions",
            mode: "source-position-after-data",
            bound: 2,
            seed: None,
            failure: "I10 source append prefix exactly once",
        },
        Run {
            name: "source-no-redelivery-skip",
            family: "source-positions",
            mode: "source-no-redelivery-skip",
            bound: 2,
            seed: None,
            failure: "I10 source append prefix exactly once",
        },
        Run {
            name: "commits-restart-turnover",
            family: "commits",
            mode: "commit-restart",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "commits-restart-no-persist-before-send",
            family: "commits",
            mode: "commit-restart-no-persist-before-send",
            bound: 2,
            seed: None,
            failure: "I2 acknowledged logical write exactly once",
        },
        Run {
            name: "unknown-action-refusal",
            family: "action-compatibility",
            mode: "refuse",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "unknown-action-skip",
            family: "action-compatibility",
            mode: "skip",
            bound: 2,
            seed: None,
            failure: "I11 compatible replay or refusal",
        },
        Run {
            name: "transactions-three-participants-1-simulation-7",
            family: "transactions",
            mode: "resolution-txn-three-participants",
            bound: 1,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "transactions-three-participants-1-simulation-42",
            family: "transactions",
            mode: "resolution-txn-three-participants",
            bound: 1,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "transactions-three-participants-1-simulation-20261024",
            family: "transactions",
            mode: "resolution-txn-three-participants",
            bound: 1,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "orphan-repeated-sweep",
            family: "orphan-cleanup",
            mode: "repeated-sweep",
            bound: 1,
            seed: None,
            failure: "",
        },
        Run {
            name: "orphan-single-sweep",
            family: "orphan-cleanup",
            mode: "single-sweep",
            bound: 1,
            seed: None,
            failure: "orphan eventually collected",
        },
        Run {
            name: "removal-reuse",
            family: "removal",
            mode: "reuse",
            bound: 3,
            seed: None,
            failure: "I4 removal retains accepted objects",
        },
        Run {
            name: "removal-serialized",
            family: "removal",
            mode: "serialized",
            bound: 3,
            seed: None,
            failure: "",
        },
        Run {
            name: "removal-serialized-own-sweep",
            family: "removal",
            mode: "serialized-own-sweep",
            bound: 3,
            seed: None,
            failure: "I4 removal retains accepted objects",
        },
        Run {
            name: "removal-serialized-no-pins",
            family: "removal",
            mode: "serialized-no-pins",
            bound: 3,
            seed: None,
            failure: "I7 removal retains active queries",
        },
        Run {
            name: "removal-serialized-5-simulation-7",
            family: "removal",
            mode: "serialized",
            bound: 5,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "removal-serialized-5-simulation-42",
            family: "removal",
            mode: "serialized",
            bound: 5,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "removal-serialized-5-simulation-20261024",
            family: "removal",
            mode: "serialized",
            bound: 5,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "naming",
            family: "state-rules",
            mode: "identity-design",
            bound: 3,
            seed: None,
            failure: "",
        },
        Run {
            name: "naming-no-commit-number",
            family: "state-rules",
            mode: "identity-design-no-number",
            bound: 3,
            seed: None,
            failure: "state name determines physical state",
        },
        Run {
            name: "table-seq",
            family: "state-rules",
            mode: "tables-design",
            bound: 3,
            seed: None,
            failure: "",
        },
        Run {
            name: "table-seq-no-table-scope",
            family: "state-rules",
            mode: "tables-design-no-table-scope",
            bound: 3,
            seed: None,
            failure: "per-table allocated record never refused",
        },
        Run {
            name: "delete-coverage",
            family: "state-rules",
            mode: "coverage-design",
            bound: 4,
            seed: None,
            failure: "",
        },
        Run {
            name: "delete-coverage-no-cold",
            family: "state-rules",
            mode: "coverage-design-no-cold",
            bound: 4,
            seed: None,
            failure: "I9 coverage equals serial oracle",
        },
        Run {
            name: "pins-low-water",
            family: "state-rules",
            mode: "pins-design",
            bound: 3,
            seed: None,
            failure: "",
        },
        Run {
            name: "pins-low-water-no-recheck",
            family: "state-rules",
            mode: "pins-design-no-recheck",
            bound: 3,
            seed: None,
            failure: "I7 every active query readable",
        },
        Run {
            name: "pins-low-water-past-active",
            family: "state-rules",
            mode: "pins-design-no-active-low",
            bound: 3,
            seed: None,
            failure: "I7 every active query readable",
        },
        Run {
            name: "pk-index-checkpoint",
            family: "state-rules",
            mode: "index-design",
            bound: 3,
            seed: None,
            failure: "",
        },
        Run {
            name: "pk-index-checkpoint-no-refresh",
            family: "state-rules",
            mode: "index-design-no-refresh",
            bound: 3,
            seed: None,
            failure: "I8 recovered index equals full key set",
        },
        Run {
            name: "delete-coverage-6-simulation-7",
            family: "state-rules",
            mode: "coverage-design",
            bound: 6,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "delete-coverage-6-simulation-42",
            family: "state-rules",
            mode: "coverage-design",
            bound: 6,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "delete-coverage-6-simulation-20261024",
            family: "state-rules",
            mode: "coverage-design",
            bound: 6,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "seq-order-2-exhaust",
            family: "seq-order",
            mode: "single",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "seq-order-two-writers-2-exhaust",
            family: "seq-order",
            mode: "two-writers",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "seq-order-maintenance-draw",
            family: "seq-order",
            mode: "single-maintenance-draw",
            bound: 1,
            seed: None,
            failure: "I9 per-key visibility equals commit-order reference",
        },
        Run {
            name: "seq-order-no-shift",
            family: "seq-order",
            mode: "two-writers-no-shift",
            bound: 2,
            seed: None,
            failure: "I9 per-key visibility equals commit-order reference",
        },
        Run {
            name: "seq-order-drawn-cutoff",
            family: "seq-order",
            mode: "single-drawn-cutoff",
            bound: 1,
            seed: None,
            failure: "I9 per-key visibility equals commit-order reference",
        },
        Run {
            name: "seq-order-non-uniform",
            family: "seq-order",
            mode: "two-writers-non-uniform",
            bound: 2,
            seed: None,
            failure: "I9 per-key visibility equals commit-order reference",
        },
        Run {
            name: "seq-order-two-writers-3-simulation-7",
            family: "seq-order",
            mode: "two-writers",
            bound: 3,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "seq-order-two-writers-3-simulation-42",
            family: "seq-order",
            mode: "two-writers",
            bound: 3,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "seq-order-two-writers-3-simulation-20261024",
            family: "seq-order",
            mode: "two-writers",
            bound: 3,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "commits-retry",
            family: "commits",
            mode: "commit-retry",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "commits-no-resolve-before-retry",
            family: "commits",
            mode: "commit-retry-no-resolve-before-retry",
            bound: 2,
            seed: None,
            failure: "I2 acknowledged logical write exactly once",
        },
        Run {
            name: "commits-etag",
            family: "commits",
            mode: "commit-etag",
            bound: 1,
            seed: None,
            failure: "",
        },
        Run {
            name: "commits-etag-repeat",
            family: "commits",
            mode: "commit-etag-repeat",
            bound: 1,
            seed: None,
            failure: "I1 unique accepted position",
        },
        Run {
            name: "concurrent-floor-aligned-no-pin-base-6-exhaust",
            family: "floors",
            mode: "floor-aligned-no-pin-base",
            bound: 6,
            seed: None,
            failure: "I7 validated pinned snapshot",
        },
        Run {
            name: "commit-guards",
            family: "commits",
            mode: "commit-guards",
            bound: 1,
            seed: None,
            failure: "",
        },
        Run {
            name: "commits-commit-2-exhaust",
            family: "commits",
            mode: "commit",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "commits-commit-4-simulation-20261024",
            family: "commits",
            mode: "commit",
            bound: 4,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "commits-commit-4-simulation-42",
            family: "commits",
            mode: "commit",
            bound: 4,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "commits-commit-4-simulation-7",
            family: "commits",
            mode: "commit",
            bound: 4,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "commits-commit-focus-copy-2-exhaust",
            family: "commits",
            mode: "commit-focus-copy",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "commits-commit-focus-observed-2-exhaust",
            family: "commits",
            mode: "commit-focus-observed",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "commits-commit-no-copy-2-exhaust",
            family: "commits",
            mode: "commit-no-copy",
            bound: 2,
            seed: None,
            failure: "I4 missing is error and retained references live",
        },
        Run {
            name: "commits-commit-no-observed-2-exhaust",
            family: "commits",
            mode: "commit-no-observed",
            bound: 2,
            seed: None,
            failure: "I8 replay equality",
        },
        Run {
            name: "commits-storage-2-exhaust",
            family: "commits",
            mode: "storage",
            bound: 2,
            seed: None,
            failure: "",
        },
        Run {
            name: "commits-storage-4-simulation-20261024",
            family: "commits",
            mode: "storage",
            bound: 4,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "commits-storage-4-simulation-42",
            family: "commits",
            mode: "storage",
            bound: 4,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "commits-storage-4-simulation-7",
            family: "commits",
            mode: "storage",
            bound: 4,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "commits-storage-no-missing-2-exhaust",
            family: "commits",
            mode: "storage-no-missing",
            bound: 2,
            seed: None,
            failure: "I8 replay equality",
        },
        Run {
            name: "commits-storage-no-published-2-exhaust",
            family: "commits",
            mode: "storage-no-published",
            bound: 2,
            seed: None,
            failure: "I4 missing is error and retained references live",
        },
        Run {
            name: "transactions-txn-1-exhaust",
            family: "transactions",
            mode: "resolution-txn",
            bound: 1,
            seed: None,
            failure: "",
        },
        Run {
            name: "transactions-txn-2-simulation-20261024",
            family: "transactions",
            mode: "resolution-txn",
            bound: 2,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "transactions-txn-2-simulation-42",
            family: "transactions",
            mode: "resolution-txn",
            bound: 2,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "transactions-txn-2-simulation-7",
            family: "transactions",
            mode: "resolution-txn",
            bound: 2,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "transactions-txn-3-simulation-20261024",
            family: "transactions",
            mode: "resolution-txn",
            bound: 3,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "transactions-txn-3-simulation-42",
            family: "transactions",
            mode: "resolution-txn",
            bound: 3,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "transactions-txn-3-simulation-7",
            family: "transactions",
            mode: "resolution-txn",
            bound: 3,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "transactions-txn-no-create-once-0-exhaust",
            family: "transactions",
            mode: "resolution-txn-no-create-once",
            bound: 0,
            seed: None,
            failure: "I6 participants never disagree",
        },
        Run {
            name: "transactions-txn-no-matching-0-exhaust",
            family: "transactions",
            mode: "resolution-txn-no-matching",
            bound: 0,
            seed: None,
            failure: "I8 visible replay equality",
        },
        Run {
            name: "transactions-txn-no-owner-resolve-1-exhaust",
            family: "transactions",
            mode: "resolution-txn-no-owner-resolve",
            bound: 1,
            seed: None,
            failure: "",
        },
        Run {
            name: "transactions-txn-no-reader-log-0-exhaust",
            family: "transactions",
            mode: "resolution-txn-no-reader-log",
            bound: 0,
            seed: None,
            failure: "I8 visible replay equality",
        },
        Run {
            name: "transactions-txn-no-reservation-0-exhaust",
            family: "transactions",
            mode: "resolution-txn-no-reservation",
            bound: 0,
            seed: None,
            failure: "I8 visible replay equality",
        },
        Run {
            name: "transactions-txn-no-roots-0-exhaust",
            family: "transactions",
            mode: "resolution-txn-no-roots",
            bound: 0,
            seed: None,
            failure: "I4 Prepare and visible objects survive",
        },
        Run {
            name: "window-storage-6-exhaust",
            family: "window",
            mode: "storage",
            bound: 6,
            seed: None,
            failure: "",
        },
        Run {
            name: "window-storage-7-simulation-20261024",
            family: "window",
            mode: "storage",
            bound: 7,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "window-storage-7-simulation-42",
            family: "window",
            mode: "storage",
            bound: 7,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "window-storage-7-simulation-7",
            family: "window",
            mode: "storage",
            bound: 7,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "window-storage-9-simulation-20261024",
            family: "window",
            mode: "storage",
            bound: 9,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "window-storage-9-simulation-42",
            family: "window",
            mode: "storage",
            bound: 9,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "window-storage-9-simulation-7",
            family: "window",
            mode: "storage",
            bound: 9,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "window-storage-no-exists-5-exhaust",
            family: "window",
            mode: "storage-no-exists",
            bound: 5,
            seed: None,
            failure: "I8 recovery replay equality including payload bytes",
        },
        Run {
            name: "window-storage-no-final-5-exhaust",
            family: "window",
            mode: "storage-no-final",
            bound: 5,
            seed: None,
            failure: "I8 recovery replay equality including payload bytes",
        },
        Run {
            name: "window-storage-no-range-5-exhaust",
            family: "window",
            mode: "storage-no-range",
            bound: 5,
            seed: None,
            failure: "I8 recovery replay equality including payload bytes",
        },
        Run {
            name: "window-storage-no-stable-cut-5-exhaust",
            family: "window",
            mode: "storage-no-stable-cut",
            bound: 5,
            seed: None,
            failure: "I8 recovery replay equality including payload bytes",
        },
        Run {
            name: "window-txn-0-exhaust",
            family: "transactions",
            mode: "txn",
            bound: 0,
            seed: None,
            failure: "",
        },
        Run {
            name: "window-txn-2-simulation-20261024",
            family: "transactions",
            mode: "txn",
            bound: 2,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "window-txn-2-simulation-42",
            family: "transactions",
            mode: "txn",
            bound: 2,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "window-txn-2-simulation-7",
            family: "transactions",
            mode: "txn",
            bound: 2,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "window-txn-3-simulation-20261024",
            family: "transactions",
            mode: "txn",
            bound: 3,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "window-txn-3-simulation-42",
            family: "transactions",
            mode: "txn",
            bound: 3,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "window-txn-3-simulation-7",
            family: "transactions",
            mode: "txn",
            bound: 3,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "concurrent-floor-aligned-6-exhaust",
            family: "floors",
            mode: "floor-aligned",
            bound: 6,
            seed: None,
            failure: "",
        },
        Run {
            name: "concurrent-floor-aligned-7-simulation-20261024",
            family: "floors",
            mode: "floor-aligned",
            bound: 7,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "concurrent-floor-aligned-7-simulation-42",
            family: "floors",
            mode: "floor-aligned",
            bound: 7,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "concurrent-floor-aligned-7-simulation-7",
            family: "floors",
            mode: "floor-aligned",
            bound: 7,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "concurrent-floor-aligned-9-simulation-20261024",
            family: "floors",
            mode: "floor-aligned",
            bound: 9,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "concurrent-floor-aligned-9-simulation-42",
            family: "floors",
            mode: "floor-aligned",
            bound: 9,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "concurrent-floor-aligned-9-simulation-7",
            family: "floors",
            mode: "floor-aligned",
            bound: 9,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "concurrent-txn-disjoint-0-exhaust",
            family: "concurrent",
            mode: "txn-disjoint",
            bound: 0,
            seed: None,
            failure: "",
        },
        Run {
            name: "concurrent-txn-disjoint-1-simulation-20261024",
            family: "concurrent",
            mode: "txn-disjoint",
            bound: 1,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "concurrent-txn-disjoint-1-simulation-42",
            family: "concurrent",
            mode: "txn-disjoint",
            bound: 1,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "concurrent-txn-disjoint-1-simulation-7",
            family: "concurrent",
            mode: "txn-disjoint",
            bound: 1,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "concurrent-txn-disjoint-2-simulation-20261024",
            family: "concurrent",
            mode: "txn-disjoint",
            bound: 2,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "concurrent-txn-disjoint-2-simulation-42",
            family: "concurrent",
            mode: "txn-disjoint",
            bound: 2,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "concurrent-txn-disjoint-2-simulation-7",
            family: "concurrent",
            mode: "txn-disjoint",
            bound: 2,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "concurrent-txn-overlap-0-exhaust",
            family: "concurrent",
            mode: "txn-overlap",
            bound: 0,
            seed: None,
            failure: "",
        },
        Run {
            name: "concurrent-txn-overlap-1-simulation-20261024",
            family: "concurrent",
            mode: "txn-overlap",
            bound: 1,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "concurrent-txn-overlap-1-simulation-42",
            family: "concurrent",
            mode: "txn-overlap",
            bound: 1,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "concurrent-txn-overlap-1-simulation-7",
            family: "concurrent",
            mode: "txn-overlap",
            bound: 1,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "concurrent-txn-overlap-2-simulation-20261024",
            family: "concurrent",
            mode: "txn-overlap",
            bound: 2,
            seed: Some(20_261_024),
            failure: "",
        },
        Run {
            name: "concurrent-txn-overlap-2-simulation-42",
            family: "concurrent",
            mode: "txn-overlap",
            bound: 2,
            seed: Some(42),
            failure: "",
        },
        Run {
            name: "concurrent-txn-overlap-2-simulation-7",
            family: "concurrent",
            mode: "txn-overlap",
            bound: 2,
            seed: Some(7),
            failure: "",
        },
        Run {
            name: "concurrent-txn-overlap-no-match-0-exhaust",
            family: "concurrent",
            mode: "txn-overlap-no-match",
            bound: 0,
            seed: None,
            failure: "I6 each applied outcome equals its own decision",
        },
        Run {
            name: "concurrent-txn-overlap-no-reservation-0-exhaust",
            family: "concurrent",
            mode: "txn-overlap-no-reservation",
            bound: 0,
            seed: None,
            failure: "I8 visible effects equal serial admissible prefix",
        },
        Run {
            name: "commit-no-cas",
            family: "commits",
            mode: "commit-guards-no-cas",
            bound: 1,
            seed: None,
            failure: "I1 unique accepted position",
        },
        Run {
            name: "commit-no-lease",
            family: "commits",
            mode: "commit-guards-no-lease",
            bound: 1,
            seed: None,
            failure: "I3 CAS fencing",
        },
        Run {
            name: "commit-no-identical",
            family: "commits",
            mode: "commit-guards-no-identical",
            bound: 1,
            seed: None,
            failure: "I5 resolution sound",
        },
        Run {
            name: "generations",
            family: "generations",
            mode: "generations",
            bound: 0,
            seed: None,
            failure: "",
        },
        Run {
            name: "generations-no-admission",
            family: "generations",
            mode: "generations-no-admission",
            bound: 0,
            seed: None,
            failure: "I4 admitted object survives GC",
        },
    ];
    declared.push(Run {
        name: "transactions-three-participants-projection-simulation-7",
        family: "transactions",
        mode: "resolution-txn-three-participants-projection",
        bound: 0,
        seed: Some(7),
        failure: "",
    });
    declared.extend(additional_runs());
    for (name, mode) in [
        ("read-rule2-minimal-blind", "rule2-minimal-blind"),
        ("read-rule2-select-minimal", "rule2-select"),
        ("read-rule2-absent-minimal", "rule2-absent"),
        ("read-rule2-delete-keys-minimal", "rule2-delete-keys"),
        (
            "read-rule2-select-same-table-minimal",
            "rule2-select-same-table",
        ),
        (
            "read-rule2-select-same-table",
            "rule2-full-select-same-table",
        ),
    ] {
        declared.push(Run {
            name,
            family: "read-programs",
            mode,
            bound: 3,
            seed: None,
            failure: "",
        });
    }

    declared.push(Run {
        name: "read-rule2-select-same-table-after-image-only",
        family: "read-programs",
        mode: "rule2-select-same-table-after-image-only",
        bound: 3,
        seed: None,
        failure: "commit-order program serializability",
    });

    for (name, mode, failure) in [
        ("read-rule2", "rule2-full", ""),
        (
            "read-rule2-stamp-at-commit",
            "rule2-full-stamp-at-commit",
            "",
        ),
        ("read-rule2-delete", "rule2-full-delete", ""),
        ("read-rule2-select", "rule2-full-select", ""),
        ("read-rule2-gate", "rule2-full-gate", ""),
        ("read-rule2-absent", "rule2-full-absent", ""),
        ("read-rule2-delete-keys", "rule2-full-delete-keys", ""),
        ("read-rule2-history", "rule2-history", ""),
        ("read-rule2-append", "rule2-full-append", ""),
        ("read-rule2-cross-table", "rule2-cross-table", ""),
        ("read-same-key", "rule2-same-key", ""),
    ] {
        declared.push(Run {
            name,
            family: "read-programs",
            mode,
            bound: 3,
            seed: None,
            failure,
        });
    }
    declared.push(Run {
        name: "read-progress",
        family: "predicate-progress",
        mode: "predicate",
        bound: 3,
        seed: None,
        failure: "",
    });

    for (name, mode, failure) in [
        ("read-rule2-minimal", "rule2-minimal", ""),
        (
            "read-rule2-select-after-image-only",
            "rule2-select-after-image-only",
            "commit-order program serializability",
        ),
        (
            "read-rule2-minimal-blind-before-image-only",
            "rule2-minimal-blind-before-image-only",
            "commit-order program serializability",
        ),
        (
            "read-rule2-absent-no-absent-lookup",
            "rule2-absent-no-absent-lookup",
            "commit-order program serializability",
        ),
        (
            "read-rule2-delete-keys-no-delete-keys",
            "rule2-delete-keys-no-delete-keys",
            "commit-order program serializability",
        ),
        (
            "read-rule2-minimal-no-conflict-replan",
            "rule2-minimal-no-conflict-replan",
            "commit-order program serializability",
        ),
        (
            "read-rule2-history-previous-winner-only",
            "rule2-history-previous-winner-only",
            "commit-order program serializability",
        ),
        (
            "read-same-key-no-inserted-keys",
            "rule2-same-key-no-inserted-keys",
            "at most one live row per table key",
        ),
    ] {
        declared.push(Run {
            name,
            family: "read-programs",
            mode,
            bound: 3,
            seed: None,
            failure,
        });
    }
    declared.extend(program_family_runs());
    declared.extend(concurrent_maintenance_runs());
    declared
}

fn concurrent_maintenance_runs() -> Vec<Run> {
    let mut runs = Vec::new();
    for (name, family, mode, bound, failure) in [
        (
            "remote-maintenance-minimal",
            "remote-maintenance",
            "minimal",
            3,
            "",
        ),
        (
            "remote-maintenance-minimal-no-stable-high-water",
            "remote-maintenance",
            "minimal-no-stable-high-water",
            3,
            "writer original sequences accepted",
        ),
        (
            "remote-maintenance-minimal-no-committed-cutoff",
            "remote-maintenance",
            "minimal-no-committed-cutoff",
            3,
            "I9 remote maintenance table rows",
        ),
        (
            "remote-maintenance-minimal-no-file-set-lease",
            "remote-maintenance",
            "minimal-no-file-set-lease",
            3,
            "",
        ),
        (
            "remote-maintenance-minimal-no-file-set-lease-no-guard-revalidation",
            "remote-maintenance",
            "minimal-two-maintainers-no-file-set-lease-no-guard-revalidation",
            3,
            "at most one live row per table key",
        ),
        (
            "remote-maintenance",
            "remote-maintenance",
            "two-maintainers",
            3,
            "",
        ),
        (
            "remote-maintenance-prepare",
            "remote-maintenance",
            "prepare",
            4,
            "",
        ),
        (
            "remote-maintenance-no-stable-high-water",
            "remote-maintenance",
            "no-stable-high-water",
            3,
            "writer original sequences accepted",
        ),
        (
            "remote-maintenance-no-committed-cutoff",
            "remote-maintenance",
            "no-committed-cutoff",
            3,
            "I9 remote maintenance table rows",
        ),
        (
            "remote-maintenance-no-file-set-lease",
            "remote-maintenance",
            "two-maintainers-no-file-set-lease",
            3,
            "",
        ),
        (
            "remote-maintenance-no-guard-revalidation",
            "remote-maintenance",
            "two-maintainers-no-guard-revalidation",
            3,
            "",
        ),
        (
            "read-transaction-minimal",
            "read-programs",
            "rule2-transaction",
            3,
            "",
        ),
        (
            "read-transaction-minimal-no-union-footprint",
            "read-programs",
            "rule2-transaction-no-union-footprint",
            3,
            "commit-order program serializability",
        ),
        (
            "read-transaction-minimal-no-overlay-read-keys",
            "read-programs",
            "rule2-transaction-no-overlay-read-keys",
            3,
            "",
        ),
        (
            "read-transaction-widened-1",
            "read-programs",
            "rule2-transaction-widened",
            1,
            "",
        ),
        (
            "read-transaction-widened-2",
            "read-programs",
            "rule2-transaction-widened",
            2,
            "",
        ),
        (
            "read-transaction-widened-no-union-footprint",
            "read-programs",
            "rule2-transaction-widened-no-union-footprint",
            2,
            "commit-order program serializability",
        ),
        (
            "read-transaction-widened-no-overlay-read-keys",
            "read-programs",
            "rule2-transaction-widened-no-overlay-read-keys",
            2,
            "",
        ),
        (
            "position-delete-minimal",
            "position-deletes",
            "minimal",
            1,
            "",
        ),
        (
            "position-delete-minimal-no-position-remap",
            "position-deletes",
            "minimal-no-position-remap",
            1,
            "I9 position delete exact target",
        ),
        (
            "position-delete-minimal-no-row-identity-mapping",
            "position-deletes",
            "minimal-no-row-identity-mapping",
            1,
            "I9 position delete exact target",
        ),
        ("position-delete", "position-deletes", "positions", 1, ""),
        (
            "position-delete-no-position-remap-silent-drop",
            "position-deletes",
            "no-position-remap-silent-drop",
            1,
            "I9 position delete exact target",
        ),
        (
            "position-delete-no-position-remap-silent-reject",
            "position-deletes",
            "no-position-remap-silent-reject",
            1,
            "I9 position delete exact target",
        ),
        (
            "position-delete-no-row-identity-mapping",
            "position-deletes",
            "no-row-identity-mapping",
            1,
            "I9 position delete exact target",
        ),
        (
            "position-delete-already-dropped",
            "position-deletes",
            "already-dropped",
            1,
            "",
        ),
        (
            "position-delete-already-dropped-no-row-identity-mapping",
            "position-deletes",
            "already-dropped-no-row-identity-mapping",
            1,
            "I9 position delete exact target",
        ),
        (
            "remote-maintenance-minimal-two-maintainers",
            "remote-maintenance",
            "minimal-two-maintainers",
            3,
            "",
        ),
        (
            "remote-maintenance-minimal-no-guard-revalidation",
            "remote-maintenance",
            "minimal-two-maintainers-no-guard-revalidation",
            3,
            "",
        ),
        (
            "remote-maintenance-no-file-set-lease-no-guard-revalidation",
            "remote-maintenance",
            "two-maintainers-no-file-set-lease-no-guard-revalidation",
            3,
            "at most one live row per table key",
        ),
        (
            "read-transaction-overlay-read-keys",
            "read-programs",
            "rule2-transaction-overlay-read-keys",
            3,
            "",
        ),
        (
            "read-transaction-widened-stamp-at-commit-2",
            "read-programs",
            "rule2-transaction-widened-stamp-at-commit",
            2,
            "",
        ),
        (
            "remote-maintenance-clear-retry",
            "remote-maintenance",
            "clear-retry-two-maintainers",
            4,
            "",
        ),
        (
            "remote-maintenance-minimal-publication-fence",
            "remote-maintenance",
            "minimal-publication-fence",
            3,
            "",
        ),
        (
            "remote-maintenance-minimal-no-publication-fence",
            "remote-maintenance",
            "minimal-no-publication-fence",
            3,
            "I3 remote maintenance publication fenced",
        ),
    ] {
        runs.push(Run {
            name,
            family,
            mode,
            bound,
            seed: None,
            failure,
        });
    }
    for (name, seed) in [
        ("read-transaction-widened-3-simulation-7", 7),
        ("read-transaction-widened-3-simulation-42", 42),
        ("read-transaction-widened-3-simulation-20261024", 20_261_024),
    ] {
        runs.push(Run {
            name,
            family: "read-programs",
            mode: "rule2-transaction-widened",
            bound: 3,
            seed: Some(seed),
            failure: "",
        });
    }
    runs
}

fn program_family_runs() -> Vec<Run> {
    let mut runs = Vec::new();
    for (name, mode, bound, failure) in [
        ("read-widened-rule2-2", "rule2-widened", 2, ""),
        ("read-widened-rule2-3", "rule2-widened", 3, ""),
        (
            "read-widened-stamp-at-commit-3",
            "rule2-widened-stamp-at-commit",
            3,
            "",
        ),
        (
            "read-widened-after-image-only",
            "rule2-widened-after-image-only",
            3,
            "commit-order program serializability",
        ),
        (
            "read-widened-before-image-only",
            "rule2-widened-before-image-only",
            3,
            "commit-order program serializability",
        ),
        (
            "read-widened-no-absent-lookup",
            "rule2-widened-no-absent-lookup",
            3,
            "commit-order program serializability",
        ),
        (
            "read-widened-no-delete-keys",
            "rule2-widened-no-delete-keys",
            3,
            "commit-order program serializability",
        ),
        (
            "read-widened-no-conflict-replan",
            "rule2-widened-no-conflict-replan",
            3,
            "commit-order program serializability",
        ),
        (
            "read-widened-previous-winner-only",
            "rule2-widened-previous-winner-only",
            3,
            "commit-order program serializability",
        ),
        (
            "read-widened-no-inserted-keys",
            "rule2-widened-no-inserted-keys",
            3,
            "at most one live row per table key",
        ),
    ] {
        runs.push(Run {
            name,
            family: "read-programs",
            mode,
            bound,
            seed: None,
            failure,
        });
    }
    for (name, mode, bound, failure) in [
        (
            "read-delivery-single-writer",
            "rule2-minimal-single-writer-delivery",
            3,
            "",
        ),
        (
            "read-delivery-no-resolve-before-replan",
            "rule2-minimal-single-writer-delivery-no-resolve-before-replan",
            3,
            "commit-order program serializability",
        ),
        (
            "read-delivery-no-acceptance-evidence",
            "rule2-minimal-single-writer-delivery-no-acceptance-evidence",
            3,
            "I2 acknowledged program accepted exactly once",
        ),
        (
            "read-delivery-no-exact-base",
            "rule2-minimal-single-writer-delivery-no-exact-base",
            3,
            "at most one live row per table key",
        ),
    ] {
        runs.push(Run {
            name,
            family: "read-delivery",
            mode,
            bound,
            seed: None,
            failure,
        });
    }
    for (name, mode, bound, failure) in [
        ("read-widened-delivery-1", "rule2-widened-delivery", 1, ""),
        ("read-widened-delivery-2", "rule2-widened-delivery", 2, ""),
        (
            "read-widened-delivery-no-acceptance-evidence-2",
            "rule2-widened-delivery-no-acceptance-evidence",
            2,
            "I2 acknowledged program accepted exactly once",
        ),
        (
            "read-widened-delivery-no-resolve-before-replan",
            "rule2-widened-delivery-no-resolve-before-replan",
            1,
            "commit-order program serializability",
        ),
        (
            "read-widened-delivery-no-acceptance-evidence",
            "rule2-widened-delivery-no-acceptance-evidence",
            1,
            "I2 acknowledged program accepted exactly once",
        ),
        (
            "read-widened-delivery-no-exact-base",
            "rule2-widened-delivery-no-exact-base",
            1,
            "at most one live row per table key",
        ),
    ] {
        runs.push(Run {
            name,
            family: "read-delivery",
            mode,
            bound,
            seed: None,
            failure,
        });
    }
    for (name, mode, failure) in [
        ("cold-promotion", "cold-promotion", ""),
        (
            "cold-promotion-no-input-removal",
            "cold-promotion-no-input-removal",
            "at most one live row per table key",
        ),
    ] {
        runs.push(Run {
            name,
            family: "cold-promotion",
            mode,
            bound: 1,
            seed: None,
            failure,
        });
    }
    for (name, seed) in [
        ("read-widened-rule2-6-simulation-7", 7),
        ("read-widened-rule2-6-simulation-42", 42),
        ("read-widened-rule2-6-simulation-20261024", 20_261_024),
    ] {
        runs.push(Run {
            name,
            family: "read-programs",
            mode: "rule2-widened",
            bound: 6,
            seed: Some(seed),
            failure: "",
        });
    }
    for (name, mode) in [
        (
            "dataset-lifecycle-no-warm-input-removal",
            "dataset-lifecycle-no-warm-input-removal",
        ),
        (
            "dataset-lifecycle-no-cold-input-removal",
            "dataset-lifecycle-no-cold-input-removal",
        ),
    ] {
        runs.push(Run {
            name,
            family: "dataset-lifecycle",
            mode,
            bound: 1,
            seed: None,
            failure: "at most one live row per table key",
        });
    }
    runs
}

fn additional_runs() -> Vec<Run> {
    let mut runs = vec![];
    for (name, mode, bound, failure) in [
        (
            "dataset-prepare-no-index-checkpoint",
            "prepare-cutoff-cap-no-index-checkpoint",
            6,
            "I8 lifecycle recovered state and PK",
        ),
        (
            "dataset-byte-window-metadata-prefix-no-index-checkpoint",
            "byte-window-metadata-prefix-no-index-checkpoint",
            5,
            "I8 byte checkpoint PK collection",
        ),
        (
            "dataset-byte-window-metadata-prefix-no-sealed-floor-without-readers",
            "byte-window-metadata-prefix-no-sealed-floor-without-readers",
            3,
            "",
        ),
        (
            "dataset-prepare-no-prepare-roots",
            "prepare-cutoff-cap-no-prepare-roots",
            6,
            "I4 lifecycle objects",
        ),
        (
            "dataset-prepare-no-write-reservation",
            "prepare-cutoff-cap-no-write-reservation",
            6,
            "I9 lifecycle logical rows",
        ),
        (
            "dataset-prepare-no-reader-record",
            "prepare-cutoff-cap-no-reader-record",
            6,
            "I6 lifecycle prefix visibility",
        ),
        ("dataset-prepare-cutoff-cap", "prepare-cutoff-cap", 6, ""),
        (
            "dataset-prepare-no-cutoff-cap",
            "prepare-no-cutoff-cap",
            6,
            "I9 lifecycle logical rows",
        ),
        ("dataset-pin-expiry", "expiry", 2, ""),
        (
            "dataset-pin-expiry-missing-as-empty",
            "expiry-missing-as-empty",
            2,
            "snapshot rows or error",
        ),
        (
            "dataset-pin-expiry-no-delete-selection",
            "expiry-no-delete-selection",
            2,
            "",
        ),
        (
            "dataset-pin-expiry-no-log-pin-expiry",
            "expiry-no-log-pin-expiry",
            2,
            "",
        ),
        (
            "dataset-byte-window-metadata-prefix",
            "byte-window-metadata-prefix",
            5,
            "",
        ),
        (
            "dataset-byte-window-metadata-prefix-no-pin-batch",
            "byte-window-metadata-prefix-no-pin-batch",
            5,
            "I7 byte lifecycle pinned recovery",
        ),
        (
            "dataset-byte-window-metadata-prefix-no-sealed-floor",
            "byte-window-metadata-prefix-no-sealed-floor",
            3,
            "I7 byte lifecycle pinned recovery",
        ),
    ] {
        runs.push(Run {
            name,
            family: "lifecycle-composed",
            mode,
            bound,
            seed: None,
            failure,
        });
    }
    for (name, mode, bound, failure) in [
        (
            "transactions-two-participants-0-exhaust",
            "resolution-txn",
            0,
            "",
        ),
        (
            "transactions-two-participants-symmetry-0-exhaust",
            "resolution-txn-symmetry",
            0,
            "",
        ),
        (
            "transactions-two-participants-symmetry-no-create-once",
            "resolution-txn-symmetry-no-create-once",
            0,
            "I6 participants never disagree",
        ),
        (
            "transactions-three-participants-symmetry-0-exhaust",
            "resolution-txn-three-participants-symmetry",
            0,
            "",
        ),
    ] {
        runs.push(Run {
            name,
            family: "transactions",
            mode,
            bound,
            seed: None,
            failure,
        });
    }
    runs
}

/// Appends one JSON line to the file named by `TRACE_JSON`, if set: the
/// action sequences that trace replay maps onto real storage code.
fn trace_json(line: &str) {
    use std::io::Write as _;
    let Ok(path) = std::env::var("TRACE_JSON") else {
        return;
    };
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(file, "{line}");
    }
}

fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_actions<A: std::fmt::Debug>(actions: &[A]) -> String {
    let items: Vec<String> = actions
        .iter()
        .map(|action| json_string(&format!("{action:?}")))
        .collect();
    format!("[{}]", items.join(","))
}

/// Seeded random walks over `model` (`WALK=<seed>:<count>:<max_steps>`), each
/// written as a JSON line with the properties that failed along it, if any.
fn walk<M>(model: &M, run: &Run, spec: &str) -> bool
where
    M: Model,
    M::State: Clone + std::fmt::Debug,
    M::Action: Clone + PartialEq + std::fmt::Debug,
{
    let mut parts = spec.split(':').map(|part| part.parse::<u64>().unwrap_or(0));
    let seed = parts.next().unwrap_or(1).max(1);
    let count = parts.next().unwrap_or(100);
    let max_steps = parts.next().unwrap_or(50);
    let mut rng = seed;
    let mut next = move |bound: usize| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        usize::try_from(rng % (bound as u64).max(1)).unwrap_or(0)
    };
    let properties = model.properties();
    let initials = model.init_states();
    let mut good = true;
    for walk_index in 0..count {
        let initial_index = next(initials.len());
        let mut state = initials[initial_index].clone();
        let mut actions = Vec::new();
        let mut failed: Vec<&str> = Vec::new();
        for _ in 0..max_steps {
            let mut enabled = vec![];
            model.actions(&state, &mut enabled);
            if enabled.is_empty() {
                break;
            }
            let action = enabled[next(enabled.len())].clone();
            let Some(after) = model.next_state(&state, action.clone()) else {
                break;
            };
            state = after;
            actions.push(action);
            for p in &properties {
                if p.expectation == Expectation::Always
                    && !(p.condition)(model, &state)
                    && !failed.contains(&p.name)
                {
                    failed.push(p.name);
                }
            }
        }
        good &= failed.is_empty() || !run.failure.is_empty();
        let failed: Vec<String> = failed.iter().map(|name| json_string(name)).collect();
        trace_json(&format!(
            "{{\"kind\":\"walk\",\"run\":{},\"family\":{},\"mode\":{},\"seed\":{seed},\"walk\":{walk_index},\"initial_index\":{initial_index},\"failed\":[{}],\"actions\":{}}}",
            json_string(run.name),
            json_string(run.family),
            json_string(run.mode),
            failed.join(","),
            json_actions(&actions)
        ));
    }
    println!("WALKS {} seed={seed} count={count} max_steps={max_steps} good={good}", run.name);
    good
}

fn replay<M: Model>(model: &M, initial: &M::State, actions: &[M::Action]) -> Option<M::State>
where
    M::Action: Clone + PartialEq,
    M::State: Clone,
{
    let mut state = initial.clone();
    for action in actions {
        let mut enabled = vec![];
        model.actions(&state, &mut enabled);
        if !enabled.contains(action) {
            return None;
        }
        state = model.next_state(&state, action.clone())?;
    }
    Some(state)
}
fn minimize<M: Model>(
    model: &M,
    initial: &M::State,
    mut action: Vec<M::Action>,
    test: fn(&M, &M::State) -> bool,
    want: bool,
) -> Vec<M::Action>
where
    M::Action: Clone + PartialEq,
    M::State: Clone,
{
    loop {
        let mut changed = false;
        'outer: for size in (1..=action.len()).rev() {
            for start in 0..=action.len() - size {
                let mut b = action.clone();
                b.drain(start..start + size);
                if replay(model, initial, &b).is_some_and(|state| test(model, &state) == want) {
                    action = b;
                    changed = true;
                    break 'outer;
                }
            }
        }
        if !changed {
            return action;
        }
    }
}
fn check<M>(model: M, run: Run) -> bool
where
    M: Model + Send + Sync + 'static,
    M::State: Clone + std::fmt::Debug + Send + Sync + std::hash::Hash + PartialEq,
    M::Action: Clone + PartialEq + std::fmt::Debug + Send + Sync,
{
    check_with_symmetry(model, run, None)
}
type Representative<S> = fn(&S) -> S;
fn check_with_symmetry<M>(
    model: M,
    run: Run,
    representative: Option<Representative<M::State>>,
) -> bool
where
    M: Model + Send + Sync + 'static,
    M::State: Clone + std::fmt::Debug + Send + Sync + std::hash::Hash + PartialEq,
    M::Action: Clone + PartialEq + std::fmt::Debug + Send + Sync,
{
    if let Ok(spec) = std::env::var("WALK") {
        return walk(&model, &run, &spec);
    }
    println!("START {}", run.name);
    let start = Instant::now();
    // A timed-out Stateright broker is closed too. Terminate rather than using
    // broker closure as evidence that the search exhausted its graph.
    let (finished, watch) = std::sync::mpsc::channel::<()>();
    let name = run.name;
    let watchdog = std::thread::spawn(move || {
        if matches!(
            watch.recv_timeout(std::time::Duration::from_mins(9)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ) {
            eprintln!("INCOMPLETE {name}: checker exceeded 540 seconds");
            std::process::exit(1);
        }
    });
    let builder = model
        .checker()
        .threads(1)
        .finish_when(stateright::HasDiscoveries::AnyOf(
            std::collections::BTreeSet::default(),
        ));
    let builder = if let Some(f) = representative {
        builder.symmetry_fn(f)
    } else {
        builder
    };
    let result = if let Some(seed) = run.seed {
        output(
            &builder
                .target_state_count(100_000)
                .target_max_depth(200)
                .spawn_simulation(seed, stateright::UniformChooser)
                .join(),
            &run,
            start,
        )
    } else if run.failure.is_empty()
        || run.family == "dataset-lifecycle"
        || run.family == "lifecycle-composed"
        || run.family == "read-programs"
        || run.family == "read-delivery"
        || run.family == "remote-maintenance"
        || run.family == "position-deletes"
        || run.family.ends_with("-progress")
        || run.mode.contains("symmetry")
    {
        output(&builder.spawn_dfs().join(), &run, start)
    } else {
        output(&builder.spawn_bfs().join(), &run, start)
    };
    drop(finished);
    let joined = watchdog.join().is_ok();
    result && joined
}
fn output<M: Model, C: Checker<M>>(c: &C, run: &Run, start: Instant) -> bool
where
    M::State: Clone + PartialEq + std::fmt::Debug,
    M::Action: Clone + PartialEq + std::fmt::Debug,
{
    println!(
        "CONFIG {} mode={} bound={} seed={} states={} unique_or_sampled_visits={} depth={}",
        run.name,
        if run.seed.is_some() {
            "SIMULATION"
        } else {
            "EXHAUSTIVE"
        },
        run.bound,
        run.seed.unwrap_or(0),
        c.state_count(),
        c.unique_state_count(),
        c.max_depth()
    );
    let discoveries = c.discoveries();
    let complete = run.seed.is_some() || c.is_done();
    let mut good = complete;
    if !complete {
        println!("INCOMPLETE checker reached time limit");
    }
    let mut expected_found = run.failure.is_empty();
    for p in c.model().properties() {
        let found = discoveries.get(p.name);
        let symmetry_required = run.mode.contains("symmetry")
            && run.failure.is_empty()
            && matches!(
                p.name,
                "both participants commit"
                    | "both participants abort"
                    | "all three participants commit"
                    | "all three participants abort"
                    | "checkpoint rebuilds unresolved Prepare"
                    | "identical retry accepted"
            );
        let composed_required = run.family == "lifecycle-composed"
            && match p.name {
                "prepared commit visible"
                | "prepared abort discarded"
                | "maintenance while prepared"
                | "ordinary write while prepared"
                | "prepared delete commits"
                | "prepared upsert commits"
                | "two actions prepared together"
                | "abort after maintenance"
                | "checkpoint retains prepared actions"
                | "GC while prepared" => run.mode.contains("prepare"),
                "higher cutoff while prepared" => run.mode == "prepare-no-cutoff-cap",
                "warm subset rewrite" | "cold promotion" => true,
                "row object collected" => {
                    run.failure.is_empty() && !run.mode.contains("no-sealed-floor")
                }
                "missing delayed object errors" => {
                    run.mode.contains("expiry") && !run.mode.contains("missing-as-empty")
                }
                "expired delayed read completes"
                | "object deleted during dispatched read"
                | "renewed snapshot completes"
                | "renewal head recheck rejects" => run.mode.contains("expiry"),
                "pinned replay batch retained"
                | "checkpoint page collected"
                | "byte budget evicts records"
                | "count budget evicts records" => run.mode == "byte-window-metadata-prefix",
                _ => false,
            };
        let read_required = run.failure.is_empty()
            && match (run.family, p.name) {
                (
                    "read-programs",
                    "two read-dependent writers and blind commit"
                    | "conflict replans statement"
                    | "independent loser retries",
                ) => run.mode.contains("full"),
                _ => false,
            };
        let widened_required = run.failure.is_empty()
            && run.mode.contains("widened")
            && match (run.family, p.name) {
                ("read-programs", "all three writers commit") => run.bound >= 3,
                ("read-programs", "writer commits two statements") => {
                    run.bound >= 2 && !run.mode.contains("transaction")
                }
                ("read-programs", "conflict replans statement" | "independent loser retries") => {
                    run.bound >= 2
                }
                (
                    "read-delivery",
                    "lost response resolved by head read" | "delayed duplicate of accepted request",
                ) => true,
                _ => false,
            };
        let promotion_required = run.family == "cold-promotion"
            && run.failure.is_empty()
            && p.expectation == Expectation::Sometimes;
        let delivery_required = run.family == "read-delivery"
            && (run.failure.is_empty()
                && matches!(
                    p.name,
                    "lost response resolved by head read" | "delayed duplicate of accepted request"
                )
                || run.mode.contains("no-resolve-before-replan")
                    && p.name == "unknown relative update applied twice");
        let added_required = run.failure.is_empty()
            && run.seed.is_none()
            && match (run.family, p.name) {
                (
                    "remote-maintenance",
                    "writer retries original stamps then commits" | "maintenance retries guards",
                )
                | ("position-deletes", "compaction remaps winning position delete") => true,
                ("remote-maintenance", "covered clear retries and publishes") => {
                    run.mode.contains("clear-retry")
                }
                ("remote-maintenance", "checkpoint published") => !run.mode.contains("clear-retry"),
                (
                    "remote-maintenance",
                    "two tables acknowledged together"
                    | "maintenance lease fenced"
                    | "cold promotion publishes",
                ) => !run.mode.contains("minimal") && !run.mode.contains("clear-retry"),
                (
                    "remote-maintenance",
                    "maintenance while Prepare unresolved"
                    | "Prepare committed"
                    | "Prepare aborted",
                ) => run.mode.contains("prepare"),
                (
                    "remote-maintenance",
                    "overlapping maintenance planned" | "maintenance replans invalid guards",
                ) => run.mode.contains("no-file-set-lease"),
                ("read-programs", "winner commits between transaction statements") => {
                    run.mode.contains("transaction-widened") && run.bound >= 2
                }
                (
                    "read-programs",
                    "transaction reads its own writes"
                    | "transaction commits two nonempty statements",
                ) => run.mode.contains("transaction-widened"),
                ("position-deletes", "position delete remapped after compaction") => {
                    !run.mode.contains("already-dropped")
                }
                ("position-deletes", "already deleted target consumed") => {
                    run.mode == "already-dropped"
                }
                _ => false,
            };
        let required = added_required
            || promotion_required
            || delivery_required
            || widened_required
            || read_required
            || symmetry_required
            || composed_required
            || run.seed.is_none()
                && match run.name {
                    "dataset-lifecycle" | "source-positions" => {
                        p.expectation == Expectation::Sometimes
                    }
                    "commits-restart-turnover" => matches!(
                        p.name,
                        "restart and lease turnover" | "persisted attempt restored"
                    ),
                    "pin-expiry" => matches!(
                        p.name,
                        "lapsed query errors"
                            | "paused partial scan resumes"
                            | "renewed pin protects its snapshot past the first deadline"
                    ),
                    "pin-expiry-no-lapse-recheck" => p.name == "expired read without head recheck",
                    "transaction-begin-pin"
                    | "unknown-action-refusal"
                    | "orphan-repeated-sweep" => p.expectation == Expectation::Sometimes,
                    _ => false,
                };
        if required && found.is_none() {
            good = false;
            println!("MISSING_REQUIRED_WITNESS {}", p.name);
        }
        let status = match (&p.expectation, found.is_some()) {
            (Expectation::Always | Expectation::Eventually, true) => "FAIL",
            (Expectation::Always | Expectation::Eventually, false)
                if run.seed.is_none() && complete =>
            {
                "HOLDS"
            }
            (Expectation::Always | Expectation::Eventually, false) => "NOT_FOUND",
            (Expectation::Sometimes, true) => "REACHED",
            _ => "NOT_REACHED",
        };
        println!("PROPERTY {status} {}", p.name);
        if p.expectation != Expectation::Sometimes && found.is_some() {
            if run.failure.is_empty() {
                good = false;
            }
            if p.name == run.failure {
                expected_found = true;
            }
        }
        if let Some(path) = found {
            let initial = &path.clone().into_states()[0];
            let initial_index = c.model().init_states().iter().position(|s| s == initial);
            println!("TRACE_INITIAL index={initial_index:?}");
            let action = if p.expectation == Expectation::Eventually {
                path.clone().into_actions()
            } else {
                minimize(
                    c.model(),
                    initial,
                    path.clone().into_actions(),
                    p.condition,
                    p.expectation == Expectation::Sometimes,
                )
            };
            let valid = replay(c.model(), initial, &action).is_some_and(|state| {
                let mut enabled = vec![];
                c.model().actions(&state, &mut enabled);
                if p.expectation == Expectation::Eventually && !enabled.is_empty() {
                    return false;
                }
                (p.condition)(c.model(), &state) == (p.expectation == Expectation::Sometimes)
            });
            good &= valid;
            println!(
                "MINIMIZED enabled={valid} steps={} {action:?}",
                action.len()
            );
            trace_json(&format!(
                "{{\"kind\":\"witness\",\"run\":{},\"family\":{},\"mode\":{},\"property\":{},\"status\":{},\"valid\":{valid},\"initial_index\":{},\"actions\":{}}}",
                json_string(run.name),
                json_string(run.family),
                json_string(run.mode),
                json_string(p.name),
                json_string(status),
                initial_index.map_or_else(|| "null".to_string(), |i| i.to_string()),
                json_actions(&action)
            ));
            if (run.family.ends_with("-progress")
                || run.family == "read-programs"
                || run.family == "read-delivery"
                || run.family == "remote-maintenance"
                || run.family == "position-deletes"
                || run.family == "cold-promotion"
                || run.family == "seq-order"
                || run.family == "lifecycle-composed"
                || run.family == "state-rules"
                || run.family == "removal"
                || run.name.starts_with("dataset-lifecycle")
                || run.name.starts_with("source-")
                || run.family == "orphan-cleanup"
                || run.family == "snapshot-leases"
                || run.mode.contains("restart")
                || run.family == "action-compatibility")
                && p.expectation != Expectation::Sometimes
            {
                println!("WITNESS {:?}", replay(c.model(), initial, &action));
            }
        }
        if run.mode.ends_with("txn-no-owner-resolve")
            && p.name == "non-owner Resolve after turnover"
        {
            good &= found.is_none();
        }
    }
    good &= expected_found;
    println!(
        "RESULT {} expected={} matched={good} seconds={:.3}",
        run.name,
        if run.failure.is_empty() {
            "holds"
        } else {
            run.failure
        },
        start.elapsed().as_secs_f64()
    );
    good
}
/// Run a named configuration. Controls require `EXHAUST=1`.
///
/// # Errors
/// Returns an error for an unknown run, missing control opt-in, or an unexpected result.
pub fn run(name: &str) -> Result<(), String> {
    let r = runs()
        .into_iter()
        .find(|r| r.name == name)
        .ok_or_else(|| format!("Unknown run {name}; use list"))?;
    if (r.mode.contains("-no-") || !r.failure.is_empty())
        && std::env::var("EXHAUST").as_deref() != Ok("1")
    {
        return Err("Controls require EXHAUST=1".into());
    }
    let good = match r.family {
        "remote-maintenance" => check(
            dataset_lifecycle::remote_maintenance::Protocol::new(r.mode, r.bound),
            r,
        ),
        "position-deletes" => check(position_deletes::Protocol::new(r.mode), r),
        "lifecycle-composed" => check(
            dataset_lifecycle::composed::Protocol::new(r.mode, r.bound),
            r,
        ),
        "dataset-lifecycle" => check(dataset_lifecycle::Protocol::new(r.mode, r.bound), r),
        "snapshot-leases" => check(snapshot_leases::Protocol::new(r.mode), r),
        "source-positions" => check(source_positions::Protocol::new(r.mode, r.bound), r),
        "action-compatibility" => check(action_compatibility::Protocol::new(r.mode), r),
        "orphan-cleanup" => check(orphan_cleanup::Protocol::new(r.mode), r),
        "commits" => check(commits::Protocol::new(r.mode, r.bound), r),
        "transactions" => check_with_symmetry(
            transactions::Protocol::new(r.mode, r.bound),
            r,
            r.mode
                .contains("symmetry")
                .then_some(transactions::Protocol::representative),
        ),
        "window" => check(window::Protocol::new(r.mode, r.bound), r),
        "floors" => check(floors::Protocol::new(r.mode, r.bound), r),
        "concurrent" => check(concurrent::Protocol::new(r.mode, r.bound), r),
        "generations" => check(generations::Protocol::new(r.mode), r),
        "removal" => check(removal::Protocol::new(r.mode, r.bound), r),
        "state-rules" => check(state_rules::Protocol::new(r.mode, r.bound), r),
        "seq-order" => check(seq_order::Protocol::new(r.mode, r.bound), r),
        "predicate-progress" => check(read_programs::PredicateProgress::new(r.bound), r),
        "cold-promotion" => check(dataset_lifecycle::promotion::Protocol::new(r.mode), r),
        "read-delivery" => check(read_programs::delivery::Protocol::new(r.mode, r.bound), r),
        "read-programs" => {
            let model = read_programs::Protocol::new(r.mode, r.bound);
            let good = check(model.clone(), r);
            println!("COMMIT_COST_PROFILES {} {:?}", r.name, model.profiles());
            println!(
                "MODEL_COUNTS {} max_winners={} max_rows_evaluated={}",
                r.name,
                model.counts()[0],
                model.counts()[1]
            );
            good
        }
        _ => false,
    };
    if good {
        Ok(())
    } else {
        Err(format!("Unexpected outcome for {name}"))
    }
}
