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

//! Delta-write encoding levels (`cayenne_delta_encoding`).
//!
//! ## Why delta writes get their own encoding policy
//!
//! Fresh CDC/append snapshot files and compaction outputs are different
//! workloads wearing the same encoder. A staged CDC delta is small, written
//! on the ingestion hot path, read a handful of times, and then folded into a
//! properly-encoded file by the tiered compactor — its encoding quality buys
//! almost nothing. A compaction output is the long-lived artifact whose
//! encoding pays for scan throughput, storage footprint, and (on S3) egress.
//!
//! Profiling a 2,000-row staged upsert showed the dominant fixed cost was the
//! Vortex `BtrBlocks` per-file encoder-strategy search plus FSST symbol-table
//! training (~10-12 ms per file regardless of row count) — big-file
//! compression machinery paid on every small delta and then discarded at the
//! next compaction pass. This is the storage-engine-universal per-level
//! pattern: `RocksDB` flushes L0 with no/light compression and re-compresses
//! at deeper levels, `ClickHouse` re-compresses on merge, and lakehouse
//! `OPTIMIZE`/compaction rewrites small commits with proper encoding.
//!
//! ## The level scale
//!
//! zstd-style: higher level = more encode effort = better ratio.
//!
//! | level | scheme set |
//! |---|---|
//! | 0 | `Uncompressed` only (canonical arrays; zero search, zero transform) |
//! | 1 | + `Constant` / `Sparse` (near-free detection; common CDC shapes) |
//! | 2 | + `Dict` (cheap, high-value on repetitive CDC data) |
//! | 3 | + cheap numeric schemes (`For`, `BitPacking`, `ZigZag`, `RunEnd`, `Sequence`) |
//! | 4–6 | full default **minus FSST** (skips symbol-table training, keeps the rest) |
//! | 7–10 | full default `BtrBlocks` cascade (today's behavior; upper levels reserved) |
//!
//! `auto` (the default) size-gates: a delta smaller than a quarter of the
//! target file size encodes at [`AUTO_LIGHT_LEVEL`]; larger or unknown-size
//! writes use the full default. Maintenance writes ([`WriteClass::Maintenance`])
//! always use the full default regardless of the configured level.

use vortex::compressor::{BtrBlocksCompressorBuilder, FloatCode, IntCode, StringCode};
use vortex::file::WriteStrategyBuilder;

use crate::metadata::{DELTA_ENCODING_MAX_LEVEL, DeltaEncoding};

/// Classifies a snapshot write for encoding-policy purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteClass {
    /// A fresh delta: staged/inline CDC appends, on-conflict snapshot inserts,
    /// inline-memtable flushes. Subject to `cayenne_delta_encoding`.
    Delta,
    /// A maintenance rewrite: compaction outputs, sorted rewrites, overwrites.
    /// Always encoded with the full default strategy — the output is the
    /// long-lived artifact whose encoding quality pays for scan throughput.
    Maintenance,
}

/// Level used for every [`WriteClass::Maintenance`] write and for large /
/// unknown-size deltas under `auto`: the full default `BtrBlocks` cascade.
pub(crate) const FULL_LEVEL: u8 = 7;

/// Level chosen by `auto` for small deltas. `Constant + Sparse + Dict` keeps
/// the big repetitive-CDC wins (often 3-5×) while skipping the per-file
/// strategy search and FSST training that dominate small-write encode cost.
pub(crate) const AUTO_LIGHT_LEVEL: u8 = 2;

/// Under `auto`, a delta is "small" when its estimated bytes are below
/// `target_file_size / AUTO_LIGHT_DENOMINATOR` — the same quarter-of-a-target
/// classification the compaction picker uses for "small" files, on the
/// rationale that a write smaller than a target file is transient by
/// definition (compaction exists to fold it).
pub(crate) const AUTO_LIGHT_DENOMINATOR: u64 = 4;

/// Resolve the effective encoding level for one snapshot write.
///
/// `estimated_bytes` is the caller's pre-encode size estimate (`None` when
/// the stream size is unknown, e.g. opaque staged streams). Unknown sizes
/// resolve to [`FULL_LEVEL`] under `auto` — conservatively assuming large.
pub(crate) fn effective_level(
    encoding: DeltaEncoding,
    write_class: WriteClass,
    estimated_bytes: Option<u64>,
    target_size_bytes: usize,
) -> u8 {
    if write_class == WriteClass::Maintenance {
        return FULL_LEVEL;
    }
    match encoding {
        DeltaEncoding::Level(level) => level.min(DELTA_ENCODING_MAX_LEVEL),
        DeltaEncoding::Auto => {
            let threshold =
                u64::try_from(target_size_bytes).unwrap_or(u64::MAX) / AUTO_LIGHT_DENOMINATOR;
            match estimated_bytes {
                Some(bytes) if bytes < threshold.max(1) => AUTO_LIGHT_LEVEL,
                _ => FULL_LEVEL,
            }
        }
    }
}

/// Map an encoding level to a Vortex write strategy override.
///
/// Returns `None` for levels at or above [`FULL_LEVEL`] — the caller uses the
/// session's default strategy (the full `BtrBlocks` cascade), which is
/// byte-for-byte today's behavior. Lower levels return a
/// [`WriteStrategyBuilder`] whose compressor is restricted to the level's
/// scheme set (see the module table).
pub(crate) fn strategy_builder_for_level(level: u8) -> Option<WriteStrategyBuilder> {
    if level >= FULL_LEVEL {
        return None;
    }

    let compressor = match level {
        0 => BtrBlocksCompressorBuilder::empty()
            .include_int([IntCode::Uncompressed])
            .include_float([FloatCode::Uncompressed])
            .include_string([StringCode::Uncompressed])
            .build(),
        1 => BtrBlocksCompressorBuilder::empty()
            .include_int([IntCode::Uncompressed, IntCode::Constant, IntCode::Sparse])
            .include_float([
                FloatCode::Uncompressed,
                FloatCode::Constant,
                FloatCode::Sparse,
            ])
            .include_string([
                StringCode::Uncompressed,
                StringCode::Constant,
                StringCode::Sparse,
            ])
            .build(),
        2 => BtrBlocksCompressorBuilder::empty()
            .include_int([
                IntCode::Uncompressed,
                IntCode::Constant,
                IntCode::Sparse,
                IntCode::Dict,
            ])
            .include_float([
                FloatCode::Uncompressed,
                FloatCode::Constant,
                FloatCode::Sparse,
                FloatCode::Dict,
            ])
            .include_string([
                StringCode::Uncompressed,
                StringCode::Constant,
                StringCode::Sparse,
                StringCode::Dict,
            ])
            .build(),
        3 => BtrBlocksCompressorBuilder::empty()
            .include_int([
                IntCode::Uncompressed,
                IntCode::Constant,
                IntCode::Sparse,
                IntCode::Dict,
                IntCode::For,
                IntCode::BitPacking,
                IntCode::ZigZag,
                IntCode::RunEnd,
                IntCode::Sequence,
            ])
            .include_float([
                FloatCode::Uncompressed,
                FloatCode::Constant,
                FloatCode::Sparse,
                FloatCode::Dict,
                FloatCode::RunEnd,
            ])
            .include_string([
                StringCode::Uncompressed,
                StringCode::Constant,
                StringCode::Sparse,
                StringCode::Dict,
            ])
            .build(),
        // 4-6: everything in the default set except FSST — the symbol-table
        // training is the profiled dominant fixed cost on small string-bearing
        // deltas; numeric schemes keep their full default sets.
        _ => BtrBlocksCompressorBuilder::default()
            .exclude_string([StringCode::Fsst])
            .build(),
    };

    Some(WriteStrategyBuilder::default().with_compressor(compressor))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: usize = 256 * 1024 * 1024; // 256 MiB target file size

    #[test]
    fn parse_accepts_auto_and_levels() {
        assert_eq!("auto".parse::<DeltaEncoding>(), Ok(DeltaEncoding::Auto));
        assert_eq!("AUTO".parse::<DeltaEncoding>(), Ok(DeltaEncoding::Auto));
        assert_eq!("0".parse::<DeltaEncoding>(), Ok(DeltaEncoding::Level(0)));
        assert_eq!("10".parse::<DeltaEncoding>(), Ok(DeltaEncoding::Level(10)));
        assert!("11".parse::<DeltaEncoding>().is_err());
        assert!("fast".parse::<DeltaEncoding>().is_err());
        assert!("-1".parse::<DeltaEncoding>().is_err());
    }

    #[test]
    fn auto_gates_by_estimated_size() {
        // Small delta (1 MiB << 64 MiB threshold) -> light level.
        assert_eq!(
            effective_level(
                DeltaEncoding::Auto,
                WriteClass::Delta,
                Some(1024 * 1024),
                TARGET
            ),
            AUTO_LIGHT_LEVEL
        );
        // Large write (at the threshold) -> full.
        assert_eq!(
            effective_level(
                DeltaEncoding::Auto,
                WriteClass::Delta,
                Some(64 * 1024 * 1024),
                TARGET
            ),
            FULL_LEVEL
        );
        // Unknown size -> conservatively full.
        assert_eq!(
            effective_level(DeltaEncoding::Auto, WriteClass::Delta, None, TARGET),
            FULL_LEVEL
        );
    }

    #[test]
    fn maintenance_always_full_even_with_explicit_level() {
        assert_eq!(
            effective_level(
                DeltaEncoding::Level(0),
                WriteClass::Maintenance,
                Some(1),
                TARGET
            ),
            FULL_LEVEL
        );
    }

    #[test]
    fn explicit_level_applies_to_any_delta_size() {
        assert_eq!(
            effective_level(
                DeltaEncoding::Level(3),
                WriteClass::Delta,
                Some(u64::MAX),
                TARGET
            ),
            3
        );
        assert_eq!(
            effective_level(DeltaEncoding::Level(9), WriteClass::Delta, None, TARGET),
            9
        );
    }

    #[test]
    fn full_levels_use_default_strategy() {
        for level in FULL_LEVEL..=DELTA_ENCODING_MAX_LEVEL {
            assert!(
                strategy_builder_for_level(level).is_none(),
                "level {level} must use the session-default (full) strategy"
            );
        }
    }

    #[test]
    fn light_levels_produce_strategy_overrides() {
        for level in 0..FULL_LEVEL {
            assert!(
                strategy_builder_for_level(level).is_some(),
                "level {level} must produce a restricted-scheme strategy override"
            );
        }
    }
}
