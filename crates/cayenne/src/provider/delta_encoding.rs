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

use std::sync::LazyLock;

use vortex::VortexSessionDefault;
use vortex::array::ArrayId;
use vortex::editions::{ComponentKind, EditionSessionExt};
use vortex::file::WriteStrategyBuilder;
use vortex::utils::aliases::hash_set::HashSet;
use vortex_btrblocks::schemes::{float, integer, string};
use vortex_btrblocks::{BtrBlocksCompressorBuilder, Scheme, SchemeExt};
use vortex_session::VortexSession;

/// Keep only the schemes in `compressor` whose encodings a Cayenne write may
/// serialize.
///
/// A Vortex writer accepts only the encodings of its session's enabled
/// editions. It limits its own default cascade to them, but it uses an explicit
/// strategy as given and fails the write on any other encoding
/// (`Serialized array ID … not permitted by ctx`). The default scheme set
/// includes such schemes, for example the one that emits `fastlanes.delta`.
/// Cayenne builds every write session with `VortexSession::default()`, so the
/// permitted encodings are those of the default editions.
fn permitted_schemes(compressor: BtrBlocksCompressorBuilder) -> BtrBlocksCompressorBuilder {
    static PERMITTED: LazyLock<HashSet<ArrayId>> = LazyLock::new(|| {
        VortexSession::default()
            .enabled_component_ids(ComponentKind::Array)
            .into_iter()
            .collect()
    });
    compressor.retain_allowed_encodings(&PERMITTED)
}

/// The most uncompressed bytes one data block of a string or binary column may reference.
///
/// A data block is the smallest unit Vortex reads, and without a cap a string column's block holds
/// 8,192 rows whatever their width: about 12.5 MiB for ~3 KB JSON documents, all of which a
/// one-row lookup reads and copies (spiceai/spiceai#15323). Over the cap a block is split into
/// power-of-two row counts; encodings are still chosen once per 8,192 rows. At 4 MiB a ~3 KB/row
/// column reads ~7.5x fewer bytes per lookup for ~7% more bytes on disk; a column whose blocks
/// are under the cap (rows under 512 bytes) is written exactly as without it.
const DATA_BLOCK_MAX_BYTES: u64 = 4 << 20;

/// A write strategy that compresses with the permitted schemes of `compressor`.
fn write_strategy(compressor: BtrBlocksCompressorBuilder) -> WriteStrategyBuilder {
    WriteStrategyBuilder::default()
        .with_btrblocks_builder(permitted_schemes(compressor))
        .with_data_block_max_bytes(Some(DATA_BLOCK_MAX_BYTES))
}

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
/// `Btrblocks` is the `BtrBlocks` cascade the Vortex writer defaults to, with
/// [`DATA_BLOCK_MAX_BYTES`] applied. `Zstd` extends the default search with the
/// Zstd string schemes (which the default deliberately excludes); the cascade then
/// picks them per column when they win.
pub(crate) fn full_strategy_builder_for(strategy: &CompressionStrategy) -> WriteStrategyBuilder {
    match strategy {
        CompressionStrategy::Btrblocks => write_strategy(BtrBlocksCompressorBuilder::default()),
        CompressionStrategy::Zstd => {
            // The default cascade deliberately excludes the Zstd string scheme;
            // add it so the per-column search can pick it when it wins.
            write_strategy(
                BtrBlocksCompressorBuilder::default().with_new_scheme(&string::ZstdScheme),
            )
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

/// Whether a write's encode takes permits from the process-global encode budget
/// (see `write_budget`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EncodeBudgetUse {
    /// Takes its shards' permits, capped by its class.
    Shared,
    /// Takes none: the whole-table reclaim of an `auto` table, at any size and on
    /// either kind of delete (see
    /// `CayenneTableProvider::rewrite_current_snapshot_holding_bounded`).
    Exempt,
}

/// How a snapshot write should be treated: the encoding policy it falls under,
/// whether it may fan its encode across shards at all, and whether its encode
/// takes permits from the global encode budget.
///
/// Bundled because both answer "what kind of write is this?", travel together
/// through every write path, and are decided by the same caller at the same
/// point — the one that built the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WritePolicy {
    pub(crate) class: WriteClass,
    pub(crate) fan_out: crate::provider::table::EncodeFanOut,
    pub(crate) encode_budget: EncodeBudgetUse,
    pub(crate) routing: ShardRouting,
}

/// How a sharded write assigns its input to the shard encoders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardRouting {
    /// Split every batch row-wise on the shard key (configured, inferred, or
    /// the primary key), or on range bounds when the caller supplies them.
    Key,
    /// Hand whole batches to the shards in turn when the table requested no
    /// layout. A row-wise split hashes every row on the single task that feeds
    /// the encoders, which bounds a bulk load's throughput.
    WholeBatch,
}

impl WritePolicy {
    /// A fresh CDC/inline delta, free to shard — the hot ingest shape.
    pub(crate) const DELTA: Self = Self {
        class: WriteClass::Delta,
        fan_out: crate::provider::table::EncodeFanOut::Sized,
        encode_budget: EncodeBudgetUse::Shared,
        routing: ShardRouting::Key,
    };

    /// A load into an empty table, routed in whole batches. It writes the
    /// table's durable files, as a full refresh's replace does, so it is
    /// maintenance: it encodes at the full level, leaves the encode permits
    /// reserved for deltas free, and finishes its secondary index runs before
    /// it is visible.
    pub(crate) const BULK_LOAD: Self = Self {
        routing: ShardRouting::WholeBatch,
        ..Self::MAINTENANCE
    };

    /// A maintenance rewrite that may shard: compaction outputs and overwrites
    /// whose output shape carries nothing a reader depends on.
    pub(crate) const MAINTENANCE: Self = Self {
        class: WriteClass::Maintenance,
        fan_out: crate::provider::table::EncodeFanOut::Sized,
        encode_budget: EncodeBudgetUse::Shared,
        routing: ShardRouting::Key,
    };

    /// A maintenance rewrite that must emit ONE sequence of files, whatever the
    /// table is configured for. See [`crate::provider::table::EncodeFanOut`].
    pub(crate) const MAINTENANCE_SERIAL: Self = Self {
        class: WriteClass::Maintenance,
        fan_out: crate::provider::table::EncodeFanOut::Serial,
        encode_budget: EncodeBudgetUse::Shared,
        routing: ShardRouting::Key,
    };

    /// This policy, with an encode that takes no permit from the global budget.
    pub(crate) const fn without_encode_budget(self) -> Self {
        Self {
            encode_budget: EncodeBudgetUse::Exempt,
            ..self
        }
    }
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

    Some(write_strategy(builder))
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
        // string scheme to the search (the default set excludes it).
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

        // ...and once added, it must survive the edition filter: registering it
        // a second time must panic, proving the zstd full-tier builder
        // genuinely carries the Zstd string scheme.
        let double_add = std::panic::catch_unwind(|| {
            permitted_schemes(
                BtrBlocksCompressorBuilder::default().with_new_scheme(&string::ZstdScheme),
            )
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

    #[tokio::test(flavor = "multi_thread")]
    async fn light_levels_produce_strategy_overrides() {
        for level in 0..FULL_LEVEL {
            assert!(
                strategy_builder_for_level(level).is_some(),
                "level {level} must produce a restricted-scheme strategy override"
            );
        }

        // The overrides are not interchangeable. The pinned builder no longer exposes its
        // scheme list, so compare what each writes for the same column: level 0 registers no
        // scheme, so strings stay canonical — larger even than the full default cascade —
        // while the `auto` light level and the rich light level both carry string `Zstd`.
        let batch = compressible_strings();
        let canonical = encoded_file_size(0, &batch).await;
        let full = encoded_file_size(FULL_LEVEL, &batch).await;
        let auto_light = encoded_file_size(AUTO_LIGHT_LEVEL, &batch).await;
        let rich_light = encoded_file_size(3, &batch).await;
        assert!(
            full < canonical,
            "level 0 must leave strings uncompressed: {canonical} bytes vs {full} for the full cascade"
        );
        assert!(
            auto_light * 2 < canonical,
            "the auto light level must Zstd-encode strings: {auto_light} bytes vs {canonical} canonical"
        );
        assert!(
            rich_light * 2 < canonical,
            "level 3 must Zstd-encode strings: {rich_light} bytes vs {canonical} canonical"
        );
    }

    /// Writes `batch` through `session` and returns the file bytes and its segment count.
    async fn write_file(
        session: vortex_session::VortexSession,
        batch: &arrow::array::RecordBatch,
    ) -> (vortex::buffer::ByteBuffer, usize) {
        use vortex::array::stream::ArrayStreamAdapter;
        use vortex::arrow::ArrowSessionExt;
        use vortex::buffer::ByteBufferMut;
        use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};

        let schema = batch.schema();
        let arrow = session.arrow();
        let dtype = arrow
            .from_arrow_schema(&schema)
            .expect("schema converts to a Vortex dtype");
        let chunk = arrow.from_arrow_record_batch(batch.clone(), &schema);
        drop(arrow);
        let stream = ArrayStreamAdapter::new(dtype, futures::stream::iter([chunk]));
        let mut file = ByteBufferMut::empty();
        session
            .write_options()
            .write(&mut file, stream)
            .await
            .expect("write the probe table");
        let file = file.freeze();
        let segments = session
            .open_options()
            .open_buffer(file.clone())
            .expect("open the probe table")
            .footer()
            .segment_map()
            .len();
        (file, segments)
    }

    /// A table of `rows` rows: an integer key and a string column of `width`-byte values.
    fn keyed_strings(rows: usize, width: usize) -> arrow::array::RecordBatch {
        use std::fmt::Write as _;

        let ids: Vec<i64> = (0..i64::try_from(rows).expect("row count fits in i64")).collect();
        let values: Vec<String> = (0..rows)
            .map(|i| {
                let mut v = format!("{{\"id\":{i},\"body\":\"");
                while v.len() < width {
                    let token = (i * 31 + v.len()) % 977;
                    write!(v, "item-{token:x}-").expect("writing to a String cannot fail");
                }
                v
            })
            .collect();
        arrow::array::RecordBatch::try_from_iter([
            (
                "id",
                std::sync::Arc::new(arrow::array::Int64Array::from(ids)) as arrow::array::ArrayRef,
            ),
            (
                "s",
                std::sync::Arc::new(arrow::array::StringArray::from(values))
                    as arrow::array::ArrayRef,
            ),
        ])
        .expect("build the probe batch")
    }

    /// Below the data block cap the full tier is the Vortex writer's own default, to the byte.
    #[tokio::test]
    async fn btrblocks_full_tier_matches_the_writer_default_below_the_cap() {
        use vortex::VortexSessionDefault;
        use vortex_session::VortexSession;

        // 64-byte strings: an 8,192-row block references ~640 KiB, under the 4 MiB cap.
        let batch = keyed_strings(20_000, 64);
        let (default_file, _) = write_file(VortexSession::default(), &batch).await;
        let (full_file, _) = write_file(
            VortexSession::default()
                .set(full_strategy_builder_for(&CompressionStrategy::Btrblocks)),
            &batch,
        )
        .await;
        assert_eq!(full_file.len(), default_file.len());
        assert!(
            full_file.as_slice() == default_file.as_slice(),
            "the full tier must write the same bytes as the writer default"
        );
    }

    /// Over the cap a string column's blocks are split, and the file reads back as written.
    #[tokio::test]
    async fn btrblocks_full_tier_splits_wide_string_blocks() {
        use vortex::VortexSessionDefault;
        use vortex::array::stream::ArrayStreamExt;
        use vortex::arrow::ArrowSessionExt;
        use vortex::file::OpenOptionsSessionExt;
        use vortex_session::VortexSession;

        // ~3 KB strings: an 8,192-row block references ~24 MiB, so a 4 MiB cap
        // splits it into 1,024-row blocks.
        let batch = keyed_strings(16_384, 3000);
        let (default_file, default_segments) = write_file(VortexSession::default(), &batch).await;
        let session = VortexSession::default()
            .set(full_strategy_builder_for(&CompressionStrategy::Btrblocks));
        let (full_file, full_segments) = write_file(session.clone(), &batch).await;
        assert!(
            full_segments >= default_segments + 12,
            "a 4 MiB cap must split the two 8,192-row blocks of `s`: {full_segments} segments vs {default_segments}"
        );
        assert!(full_file.len() > default_file.len() / 2);

        let read = session
            .open_options()
            .open_buffer(full_file)
            .expect("open")
            .scan()
            .expect("scan")
            .into_array_stream()
            .expect("stream")
            .read_all()
            .await
            .expect("read back");
        let mut ctx = vortex::array::VortexSessionExecute::create_execution_ctx(&session);
        let target =
            arrow::datatypes::Field::new_struct("", batch.schema().fields().clone(), false);
        let read = session
            .arrow()
            .execute_arrow(read, Some(&target), &mut ctx)
            .expect("to arrow");
        let read = arrow::array::RecordBatch::from(
            arrow::array::AsArray::as_struct(read.as_ref()).clone(),
        );
        assert_eq!(read, batch);
    }

    /// One `Utf8` column of 8,192 distinct values that share long runs of text: no
    /// dictionary helps, but an entropy coder shrinks the shared text.
    fn compressible_strings() -> arrow::array::RecordBatch {
        let values: Vec<String> = (0..8192)
            .map(|i| format!("customer-{i:08}-segment-wholesale-region-north-america-padding"))
            .collect();
        arrow::array::RecordBatch::try_from_iter([(
            "s",
            std::sync::Arc::new(arrow::array::StringArray::from(values)) as arrow::array::ArrayRef,
        )])
        .expect("build the probe batch")
    }

    /// The size of the file `batch` is written to through the strategy `level` resolves
    /// to; a full level writes with the session-default cascade.
    async fn encoded_file_size(level: u8, batch: &arrow::array::RecordBatch) -> usize {
        use vortex::VortexSessionDefault;
        use vortex::array::stream::ArrayStreamAdapter;
        use vortex::arrow::ArrowSessionExt;
        use vortex::buffer::ByteBufferMut;
        use vortex::file::WriteOptionsSessionExt;
        use vortex_session::VortexSession;

        let mut session = VortexSession::default();
        if let Some(strategy) = strategy_builder_for_level(level) {
            session = session.set(strategy);
        }
        let schema = batch.schema();
        let arrow = session.arrow();
        let dtype = arrow
            .from_arrow_schema(&schema)
            .expect("schema converts to a Vortex dtype");
        let chunk = arrow.from_arrow_record_batch(batch.clone(), &schema);
        drop(arrow);
        let stream = ArrayStreamAdapter::new(dtype, futures::stream::iter([chunk]));
        let mut file = ByteBufferMut::empty();
        session
            .write_options()
            .write(&mut file, stream)
            .await
            .expect("write the probe column");
        file.len()
    }
}
