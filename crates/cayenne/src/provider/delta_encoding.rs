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
//! Every delta write pays the Vortex `BtrBlocks` per-file encoder-strategy
//! search (per column, with FSST symbol-table training for strings) for an
//! encoding that is discarded at the next compaction pass. Local micro-benches
//! showed this cost to be small per 2k-row delta on a laptop; the per-level
//! design exists for the aggregate CPU spent across millions of CDC deltas at
//! production scale, and as an explicit A/B knob for measuring exactly that.
//! This is the storage-engine-universal per-level pattern: `RocksDB` flushes
//! L0 with no/light compression and re-compresses at deeper levels,
//! `ClickHouse` re-compresses on merge, and lakehouse `OPTIMIZE`/compaction
//! rewrites small commits with proper encoding.
//!
//! ## The level scale
//!
//! zstd-style: higher level = more encode effort = better ratio.
//!
//! | level | scheme set |
//! |---|---|
//! | 0 | `Uncompressed` only (canonical arrays; zero search, zero transform) |
//! | 1 | `Sparse` only (near-free detection; constant detection is built into the cascade as of Vortex 0.79) |
//! | 2 | string `Zstd` only — the `auto` light level (one entropy pass over the string residual; numerics stay canonical until compaction) |
//! | 3 | rich light: dictionaries + cheap numeric schemes (`For`, `BitPacking`, `ZigZag`, `RunEnd`, `Sequence`) + `Zstd` |
//! | 4–6 | full default **minus FSST** (skips symbol-table training, keeps the rest) |
//! | 7–10 | full default `BtrBlocks` cascade (today's behavior; upper levels reserved) |
//!
//! `auto` (the default) encodes every delta at [`AUTO_LIGHT_LEVEL`]: a delta is
//! a transient staged stream (e.g. the off-fence mem-tier checkpoint) that
//! compaction rewrites, so it skips the full cascade regardless of size. Level
//! `7` is the explicit opt-out (byte-for-byte the pre-feature behavior).
//! Maintenance writes ([`WriteClass::Maintenance`]) always use the full default
//! regardless of the configured level.

use vortex::file::WriteStrategyBuilder;
use vortex_btrblocks::schemes::{float, integer, string};
use vortex_btrblocks::{BtrBlocksCompressorBuilder, Scheme, SchemeExt};

/// Build a [`BtrBlocksCompressorBuilder`] restricted to exactly `schemes`.
///
/// Starts from [`BtrBlocksCompressorBuilder::empty`] (no schemes registered)
/// and registers each scheme in turn. Anything the registered schemes cannot
/// shrink falls back to the canonical (uncompressed) encoding — there is no
/// explicit "uncompressed" scheme to add in the pinned Vortex API.
///
/// Callers must pass a duplicate-free list:
/// [`BtrBlocksCompressorBuilder::with_new_scheme`] panics on a repeated
/// [`SchemeId`](vortex_btrblocks::SchemeId). The light-level sets below are
/// curated by hand and contain no duplicates.
fn builder_with_schemes(schemes: &[&'static dyn Scheme]) -> BtrBlocksCompressorBuilder {
    schemes
        .iter()
        .fold(BtrBlocksCompressorBuilder::empty(), |builder, &scheme| {
            builder.with_new_scheme(scheme)
        })
}

use crate::metadata::{
    CompressionStrategy, DELTA_ENCODING_FULL_LEVEL, DELTA_ENCODING_MAX_LEVEL, DeltaEncoding,
};

/// Map the table's [`CompressionStrategy`] to the FULL-tier write strategy —
/// the session-level strategy the base `VortexFormat` carries, used by
/// maintenance writes and by delta writes that resolve to a full level.
///
/// `Btrblocks` returns `None`: the Vortex session default IS the `BtrBlocks`
/// cascade, so no override is registered (byte-for-byte the pre-feature
/// behavior). `Zstd` extends the default search with the Zstd string schemes
/// (which the default deliberately excludes); the cascade then picks them per
/// column when they win. This is what makes the previously-dormant
/// `cayenne_compression_strategy=zstd` param real.
pub(crate) fn full_strategy_builder_for(
    strategy: &CompressionStrategy,
) -> Option<WriteStrategyBuilder> {
    match strategy {
        CompressionStrategy::Btrblocks => None,
        CompressionStrategy::Zstd => {
            // The default cascade deliberately excludes the Zstd string scheme;
            // add it so the per-column search can pick it when it wins.
            let builder =
                BtrBlocksCompressorBuilder::default().with_new_scheme(&string::ZstdScheme);
            Some(WriteStrategyBuilder::default().with_btrblocks_builder(builder))
        }
    }
}

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

/// How a snapshot write should be treated: the encoding policy it falls under,
/// and whether it may fan its encode across shards at all.
///
/// Bundled because both answer "what kind of write is this?", travel together
/// through every write path, and are decided by the same caller at the same
/// point — the one that built the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WritePolicy {
    pub(crate) class: WriteClass,
    pub(crate) fan_out: crate::provider::table::EncodeFanOut,
}

impl WritePolicy {
    /// A fresh CDC/inline delta, free to shard — the hot ingest shape.
    pub(crate) const DELTA: Self = Self {
        class: WriteClass::Delta,
        fan_out: crate::provider::table::EncodeFanOut::Sized,
    };

    /// A maintenance rewrite that may shard: compaction outputs and overwrites
    /// whose output shape carries nothing a reader depends on.
    pub(crate) const MAINTENANCE: Self = Self {
        class: WriteClass::Maintenance,
        fan_out: crate::provider::table::EncodeFanOut::Sized,
    };

    /// A maintenance rewrite that must emit ONE sequence of files, whatever the
    /// table is configured for. See [`crate::provider::table::EncodeFanOut`].
    pub(crate) const MAINTENANCE_SERIAL: Self = Self {
        class: WriteClass::Maintenance,
        fan_out: crate::provider::table::EncodeFanOut::Serial,
    };
}

/// Level used for every [`WriteClass::Maintenance`] write: the full default
/// `BtrBlocks` cascade. Aliases the metadata constant so the config default
/// and the mapping boundary can't drift apart.
pub(crate) const FULL_LEVEL: u8 = DELTA_ENCODING_FULL_LEVEL;

/// Level chosen by `auto` for every delta write: string `Zstd` only. A single
/// entropy-coding pass over the string residual captures most of the
/// transient-file byte win (417k-row harness, release: 103.1 MB vs 172.5 MB
/// for the prior Sparse+Dict set and 115.8 MB for the FULL cascade) at equal
/// light-path encode wall time, while skipping the per-file strategy search
/// and FSST training that dominates small-write encode cost. Every delta is
/// transient — compaction re-encodes it at [`FULL_LEVEL`] — so the light
/// scheme set trades a larger transient file (compaction folds it) for
/// materially cheaper CDC-hot-path encode. Numeric columns stay canonical on
/// this path; the full cascade is reserved for the durable
/// [`WriteClass::Maintenance`] artifacts whose encoding quality pays for scan
/// throughput.
pub(crate) const AUTO_LIGHT_LEVEL: u8 = 2;

/// Resolve the effective encoding level for one snapshot write.
///
/// Under `auto`, every [`WriteClass::Delta`] write encodes LIGHT
/// ([`AUTO_LIGHT_LEVEL`]): deltas are transient staged CDC streams (e.g. the
/// off-fence mem-tier checkpoint) that compaction rewrites at [`FULL_LEVEL`],
/// so paying the full `BtrBlocks` cascade (incl. the FSST symbol-table
/// double-train) on the CDC hot path is wasted work — the file is re-encoded
/// before it becomes long-lived. Only durable [`WriteClass::Maintenance`]
/// writes take [`FULL_LEVEL`] under `auto`; an explicit
/// [`DeltaEncoding::Level`] overrides the delta path with a fixed level.
pub(crate) fn effective_level(encoding: DeltaEncoding, write_class: WriteClass) -> u8 {
    if write_class == WriteClass::Maintenance {
        return FULL_LEVEL;
    }
    match encoding {
        DeltaEncoding::Level(level) => level.min(DELTA_ENCODING_MAX_LEVEL),
        DeltaEncoding::Auto => AUTO_LIGHT_LEVEL,
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

    // Pinned-Vortex `BtrBlocks` is scheme-list driven: an `empty()` builder has
    // no schemes, so the cascade falls back to canonical (uncompressed) arrays
    // for anything the registered schemes can't shrink — there is no explicit
    // "uncompressed" scheme to add. Each light level starts from `empty()` and
    // registers a widening subset; the full tier (handled above by the early
    // return) keeps the session-default cascade.
    let builder = match level {
        // 0: no schemes — pure canonical/uncompressed (zero search, zero transform).
        0 => BtrBlocksCompressorBuilder::empty(),
        // 1: + sparse detection (near-free; common CDC shapes). Constant
        // detection is built into the cascading compressor as of Vortex 0.79.
        1 => builder_with_schemes(&[
            &integer::SparseScheme,
            &float::NullDominatedSparseScheme,
            &string::NullDominatedSparseScheme,
        ]),
        // 2 (the `auto` light level): string Zstd only. One entropy-coding
        // pass over the string residual captures most of the transient-file
        // byte win at light-path encode cost — measured on the 417k-row
        // round-trip harness (release): zstd-only 103.1 MB vs Sparse+Dict
        // 172.5 MB vs the FULL cascade 115.8 MB, at statistically equal
        // encode wall time (~100 ms; FULL 126 ms). Numeric columns stay
        // canonical here; compaction's FULL re-encode optimizes them for the
        // long-lived artifact. Dict+numeric light sets remain at level 3 as
        // the explicit A/B rung (dict+zstd measured only ~4% smaller).
        2 => builder_with_schemes(&[&string::ZstdScheme]),
        // 3: rich light — dictionaries + cheap numeric schemes + Zstd.
        3 => builder_with_schemes(&[
            &integer::SparseScheme,
            &integer::IntDictScheme,
            &integer::FoRScheme,
            &integer::BitPackingScheme,
            &integer::ZigZagScheme,
            &integer::RunEndScheme,
            &integer::SequenceScheme,
            &float::NullDominatedSparseScheme,
            &float::FloatDictScheme,
            &float::FloatRLEScheme,
            &string::NullDominatedSparseScheme,
            &string::StringDictScheme,
            &string::ZstdScheme,
        ]),
        // 4-6: everything in the default set except FSST — the symbol-table
        // training is the profiled dominant fixed cost on small string-bearing
        // deltas; numeric schemes keep their full default sets.
        _ => BtrBlocksCompressorBuilder::default().exclude_schemes([string::FSSTScheme.id()]),
    };

    Some(WriteStrategyBuilder::default().with_btrblocks_builder(builder))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_auto_and_levels() {
        assert_eq!("auto".parse::<DeltaEncoding>(), Ok(DeltaEncoding::Auto));
        assert_eq!("AUTO".parse::<DeltaEncoding>(), Ok(DeltaEncoding::Auto));
        assert_eq!("0".parse::<DeltaEncoding>(), Ok(DeltaEncoding::Level(0)));
        assert_eq!("10".parse::<DeltaEncoding>(), Ok(DeltaEncoding::Level(10)));
        "11".parse::<DeltaEncoding>()
            .expect_err("level 11 must be rejected (max is 10)");
        "fast"
            .parse::<DeltaEncoding>()
            .expect_err("non-numeric, non-auto values must be rejected");
        "-1".parse::<DeltaEncoding>()
            .expect_err("negative levels must be rejected");
    }

    #[test]
    fn default_is_auto_with_light_deltas_and_full_opt_out() {
        // Product decision: `auto` ships as the default — every delta (a
        // transient, compaction-rewritten write) encodes light; maintenance
        // stays on the full cascade. Level 7 is the explicit opt-out.
        assert_eq!(DeltaEncoding::default(), DeltaEncoding::Auto);
        assert!(
            strategy_builder_for_level(effective_level(
                DeltaEncoding::default(),
                WriteClass::Delta,
            ))
            .is_some(),
            "default auto must light-encode a delta write"
        );
        assert!(
            strategy_builder_for_level(effective_level(
                DeltaEncoding::Level(FULL_LEVEL),
                WriteClass::Delta,
            ))
            .is_none(),
            "level 7 must be the explicit opt-out (full strategy) for a delta"
        );
    }

    #[test]
    fn auto_lights_every_delta_regardless_of_size() {
        // No size gate: under `auto` a delta encodes LIGHT whether it is a
        // small fresh write or a large mem-tier checkpoint. Every delta is
        // transient (compaction re-encodes it at FULL), so the CDC hot path
        // never pays the full BtrBlocks + FSST cascade.
        assert_eq!(
            effective_level(DeltaEncoding::Auto, WriteClass::Delta),
            AUTO_LIGHT_LEVEL,
            "auto must light-encode a delta write"
        );
        // Maintenance is the one class that keeps the full cascade under auto.
        assert_eq!(
            effective_level(DeltaEncoding::Auto, WriteClass::Maintenance),
            FULL_LEVEL,
            "auto maintenance must use the full cascade"
        );
    }

    #[test]
    fn maintenance_always_full_even_with_explicit_level() {
        assert_eq!(
            effective_level(DeltaEncoding::Level(0), WriteClass::Maintenance),
            FULL_LEVEL
        );
    }

    #[test]
    fn explicit_level_applies_to_a_delta() {
        assert_eq!(
            effective_level(DeltaEncoding::Level(3), WriteClass::Delta),
            3
        );
        assert_eq!(
            effective_level(DeltaEncoding::Level(9), WriteClass::Delta),
            9
        );
    }

    #[test]
    fn zstd_full_strategy_includes_zstd_string_schemes() {
        // Engagement at the mapping level: `zstd` must actually add the Zstd
        // string scheme to the search (the default set excludes it), and
        // `btrblocks` must register no override (session default = the
        // pre-feature cascade).
        assert!(
            full_strategy_builder_for(&CompressionStrategy::Btrblocks).is_none(),
            "btrblocks must use the session-default strategy (no override)"
        );
        assert!(
            full_strategy_builder_for(&CompressionStrategy::Zstd).is_some(),
            "zstd must register a full-tier strategy override"
        );

        // The pinned Vortex builder no longer exposes its scheme list, so probe
        // membership through `with_new_scheme`, which panics iff a scheme with
        // the same `SchemeId` is already registered.
        //
        // The default cascade must NOT contain the Zstd string scheme — adding
        // it must therefore succeed (no panic). This is the distinguishing
        // addition that makes `cayenne_compression_strategy=zstd` real.
        let added = std::panic::catch_unwind(|| {
            BtrBlocksCompressorBuilder::default().with_new_scheme(&string::ZstdScheme)
        });
        assert!(
            added.is_ok(),
            "the default cascade must NOT already include the Zstd string \
             scheme (it is the zstd param's distinguishing addition)"
        );

        // ...and once added, registering it a second time must panic, proving
        // the zstd full-tier builder genuinely carries the Zstd string scheme.
        let double_add = std::panic::catch_unwind(|| {
            BtrBlocksCompressorBuilder::default()
                .with_new_scheme(&string::ZstdScheme)
                .with_new_scheme(&string::ZstdScheme)
        });
        assert!(
            double_add.is_err(),
            "the zstd full-tier compressor must include the Zstd string scheme \
             in the search"
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
