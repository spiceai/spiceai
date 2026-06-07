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

//! Hardware- and workload-aware auto-tuning for the Cayenne accelerator.
//!
//! Cayenne exposes many `cayenne_*` knobs whose optimal values depend on the
//! host (cores, RAM, storage medium) and the workload (refresh mode / ingest
//! shape). Hand-tuning them per deployment is brittle: a config tuned for a
//! 128-core box with local NVMe is wrong for a 4-core container on EBS, and a
//! config tuned for a bulk backfill is wrong for steady-state CDC. This module
//! derives a coherent default for the memory-/cpu-/storage-sensitive knobs from
//! a single [`HardwareProfile`] so the knobs move *together* for the host
//! instead of being set in isolation.
//!
//! Every numeric knob also accepts the literal `auto` (or being left unset) to
//! opt into this derivation — see [`read_knob`] / [`auto_or_usize`]. An explicit
//! value always overrides the derived one.
//!
//! ## Coherence (knobs working together)
//!
//! * **CPU.** The aggregate Vortex encode concurrency across *all* tables is
//!   bounded by a process-global semaphore sized to the host core count (see
//!   [`crate::dataaccelerator::cayenne`] startup wiring and
//!   `cayenne::provider::write_budget`). Per-table `write_concurrency` is then a
//!   request against that shared budget rather than an independent core grab, so
//!   a fleet of tables receiving CDC at once cannot oversubscribe the cores.
//! * **Memory.** The per-table caches ([`HardwareProfile::pk_keyset_cache_mb`],
//!   [`HardwareProfile::segment_cache_mb`], and the inline memtable from
//!   [`HardwareProfile::inline_flush_caps`]) are each sized as a *fraction* of
//!   host RAM, so a handful of tables stays bounded on any host size. The floors
//!   preserve historical behavior on small hosts; the ceilings bound per-table
//!   memory on very large hosts.
//!
//! The derivations are pure functions of the profile, so the whole matrix
//! (cores × memory × storage × refresh) is unit-testable without a live host —
//! see the tests at the bottom of this file, which are the local, deterministic
//! form of the CH-benCH host matrix.

use crate::component::dataset::acceleration::{Acceleration, StorageProfile};
use crate::dataaccelerator::storage::{
    ResolvedAccelerationStorage, resolve_acceleration_storage_async,
};

const MIB: u64 = 1024 * 1024;

/// Inline-memtable flush floor in bytes (2 MiB). A host at or under the scaling
/// threshold keeps this historical small-write cap. Coupled to
/// [`InlineFlushCaps::FLOOR`] (asserted in tests).
const FLOOR_FLUSH_BYTES: i64 = 2 * 1_048_576;
/// Inline-memtable flush floor in rows (≈ `FLOOR_FLUSH_BYTES` at ~1 KiB/row).
const FLOOR_FLUSH_ROWS: i64 = 2_048;
/// Inline-memtable flush floor in segments (merge fan-in floor).
const FLOOR_FLUSH_SEGMENTS: i64 = 16;

/// Memory-/storage-aware caps for the inline memtable (the CDC / small-write
/// path). The memtable accumulates CDC mutations as Arrow-IPC BLOBs in the local
/// metastore and is re-read on every scan until checkpointed to a Vortex file;
/// faster media tolerate a larger resident memtable. See
/// [`HardwareProfile::inline_flush_caps`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(clippy::struct_field_names)]
pub(crate) struct InlineFlushCaps {
    pub max_bytes: i64,
    pub max_rows: i64,
    pub max_segments: i64,
}

impl InlineFlushCaps {
    /// Historical flat small-write caps (2 MiB / 2048 rows / 16 segments). Used
    /// for non-inlining refresh profiles (where the caps are ignored) and as the
    /// derivation floor ([`HardwareProfile::inline_flush_caps`] never returns
    /// less than this), so no host regresses below the prior flat default.
    pub const FLOOR: Self = Self {
        max_bytes: FLOOR_FLUSH_BYTES,
        max_rows: FLOOR_FLUSH_ROWS,
        max_segments: FLOOR_FLUSH_SEGMENTS,
    };
}

/// Static host signals known at table-registration time.
///
/// All are cheap to read and container-aware where the OS exposes it:
/// [`crate::resource_monitor::get_total_memory`] honors cgroup v1/v2 memory
/// limits, and `std::thread::available_parallelism` honors CPU quotas on Linux.
/// Construct via [`HardwareProfile::detect`] in production or
/// [`HardwareProfile::new`] in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HardwareProfile {
    /// Logical cores available to the process (the encode-shard / write
    /// concurrency ceiling).
    pub cores: usize,
    /// Total memory available to the process in bytes (cgroup-aware).
    pub total_mem_bytes: u64,
    /// Storage medium backing the Vortex *data* files (drives target file size).
    pub data_storage: ResolvedAccelerationStorage,
    /// Storage medium backing the *metastore* (where inline-memtable BLOBs live
    /// and the per-scan re-read cost is paid).
    pub metastore_storage: ResolvedAccelerationStorage,
}

impl HardwareProfile {
    /// Construct from explicit signals. Used by tests and by callers that have
    /// already resolved the storage classes.
    #[must_use]
    pub fn new(
        cores: usize,
        total_mem_bytes: u64,
        data_storage: ResolvedAccelerationStorage,
        metastore_storage: ResolvedAccelerationStorage,
    ) -> Self {
        Self {
            cores: cores.max(1),
            total_mem_bytes,
            data_storage,
            metastore_storage,
        }
    }

    /// Detect the host profile. `data_path` / `metastore_path` are classified
    /// for storage medium off the async runtime when `storage_profile` is `Auto`
    /// (the probe does blocking `/proc` + `/sys` reads on Linux); explicit
    /// profiles short-circuit. Pass real filesystem paths (strip any `file://`
    /// scheme first); an empty or remote (`s3://`) data path classifies as
    /// `Unknown`, which is correct — the storage-aware file-size override is
    /// skipped for object stores anyway.
    pub async fn detect(
        storage_profile: StorageProfile,
        data_path: &str,
        metastore_path: &str,
    ) -> Self {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let total_mem_bytes = crate::resource_monitor::get_total_memory();
        let data_storage = resolve_acceleration_storage_async(storage_profile, data_path).await;
        let metastore_storage =
            resolve_acceleration_storage_async(storage_profile, metastore_path).await;
        Self::new(cores, total_mem_bytes, data_storage, metastore_storage)
    }

    /// Byte budget (in MB) for the in-memory primary-key keyset used to detect
    /// upsert conflicts during CDC ingestion. Within budget an exact keyset is
    /// kept; over budget, upsert tables fall back to a bounded bloom existence
    /// filter (avoiding the per-batch full-table rebuild).
    ///
    /// ~1/32 of host memory, clamped to `[256 MiB, 8 GiB]`: the floor preserves
    /// historical behavior on small hosts; the ceiling bounds per-table cache
    /// memory on very large hosts. Generous enough for SF100-class keysets
    /// (hundreds of MB to low GB) while leaving headroom for the query pool and
    /// sibling tables.
    #[must_use]
    pub fn pk_keyset_cache_mb(&self) -> usize {
        const FLOOR_MB: u64 = 256;
        const CEIL_MB: u64 = 8 * 1024;
        let scaled_mb = self.total_mem_bytes / 32 / MIB;
        usize::try_from(scaled_mb.clamp(FLOOR_MB, CEIL_MB)).unwrap_or(256)
    }

    /// Size (in MB) of the in-memory Vortex decompressed-segment cache, which
    /// accelerates repeated scans (the OLAP side of an HTAP workload).
    ///
    /// Scales up on memory-rich hosts (~1/128 of RAM) but never below the
    /// historical 256 MiB default, so no host regresses on the query path, and
    /// is capped at 1 GiB to bound per-table memory. A memory-rich host that
    /// previously left this cold cache pinned at 256 MiB now keeps more hot
    /// segments resident.
    #[must_use]
    pub fn segment_cache_mb(&self) -> usize {
        const FLOOR_MB: u64 = 256;
        const CEIL_MB: u64 = 1024;
        let scaled_mb = self.total_mem_bytes / 128 / MIB;
        usize::try_from(scaled_mb.clamp(FLOOR_MB, CEIL_MB)).unwrap_or(256)
    }

    /// Storage-aware target Vortex file size (MB), or `None` to keep the engine
    /// default (256 MB). Smaller files reduce write amplification on EBS-class
    /// network storage; larger files improve scan throughput on RAM-backed
    /// mounts. `LocalSsd`/`Unknown` keep the engine default.
    #[must_use]
    pub fn target_file_size_mb_override(&self) -> Option<usize> {
        match self.data_storage {
            ResolvedAccelerationStorage::Ebs => Some(256),
            ResolvedAccelerationStorage::Tmpfs => Some(64),
            ResolvedAccelerationStorage::LocalSsd | ResolvedAccelerationStorage::Unknown => None,
        }
    }

    /// Inline-memtable flush caps from host memory and the *metastore* storage
    /// medium. Deliberately more conservative than [`Self::pk_keyset_cache_mb`]
    /// (the keyset is a pruning structure, whereas the memtable is raw, un-pruned
    /// data re-read on every scan). The byte budget is the primary lever; rows
    /// and segments derive from it, preserving the floor ratios (2 MiB → 2048
    /// rows / 16 segments). `max_segments` is additionally capped at 256 to bound
    /// per-scan merge fan-in.
    #[must_use]
    pub fn inline_flush_caps(&self) -> InlineFlushCaps {
        const FLOOR_BYTES: u64 = 2 * MIB;
        // (divisor, ceiling) per metastore medium: faster re-read → larger
        // memtable. Tmpfs is RAM-backed, so the memtable double-counts against
        // memory — keep it smallest. Unknown falls back to the conservative EBS
        // profile.
        let (divisor, ceil_bytes): (u64, u64) = match self.metastore_storage {
            ResolvedAccelerationStorage::LocalSsd => (64, 256 * MIB),
            ResolvedAccelerationStorage::Tmpfs => (256, 64 * MIB),
            ResolvedAccelerationStorage::Ebs | ResolvedAccelerationStorage::Unknown => {
                (128, 128 * MIB)
            }
        };
        let bytes = (self.total_mem_bytes / divisor).clamp(FLOOR_BYTES, ceil_bytes);
        let rows = bytes / 1024; // ~1 KiB/row
        let segments = (bytes / (128 * 1024)).clamp(16, 256); // ~128 KiB/segment, fan-in cap
        InlineFlushCaps {
            max_bytes: i64::try_from(bytes).unwrap_or(i64::MAX),
            max_rows: i64::try_from(rows).unwrap_or(i64::MAX),
            max_segments: i64::try_from(segments).unwrap_or(i64::MAX),
        }
    }
}

/// A numeric `cayenne_*` knob's value as configured by the operator, after
/// honoring the literal `auto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Knob {
    /// Unset, explicitly `auto`, or unparseable — derive from the
    /// [`HardwareProfile`].
    Auto,
    /// An explicit integer. May be `0`; call sites that disallow `0` handle it
    /// (e.g. concurrency knobs clamp to a minimum of 1).
    Set(usize),
}

/// Read a numeric `cayenne_*` knob, honoring the literal `auto`.
///
/// The first matching key wins (the first is the canonical `cayenne_`-prefixed
/// name; later keys are accepted aliases). `auto` (any case) or an unset knob
/// returns [`Knob::Auto`]; a valid non-negative integer returns [`Knob::Set`];
/// an unparseable value warns and falls back to [`Knob::Auto`] (the safe derived
/// default) rather than erroring.
#[must_use]
pub(crate) fn read_knob(acceleration: &Acceleration, keys: &[&str]) -> Knob {
    for &key in keys {
        let Some(raw) = acceleration.params.get(key) else {
            continue;
        };
        let value = raw.trim();
        if value.eq_ignore_ascii_case("auto") {
            return Knob::Auto;
        }
        return match value.parse::<usize>() {
            Ok(n) => Knob::Set(n),
            Err(_) => {
                tracing::warn!(
                    "An invalid '{key}' value was provided: '{raw}'. Expected a non-negative integer or 'auto'; using the auto-derived default. For details, visit: https://spiceai.org/docs/components/data-accelerators/cayenne#configuration"
                );
                Knob::Auto
            }
        };
    }
    Knob::Auto
}

/// Resolve a `usize` knob against a derived default: `auto`/unset/invalid →
/// `derived`; an explicit integer → that value.
#[must_use]
pub(crate) fn auto_or_usize(acceleration: &Acceleration, keys: &[&str], derived: usize) -> usize {
    match read_knob(acceleration, keys) {
        Knob::Auto => derived,
        Knob::Set(n) => n,
    }
}

/// Resolve a `u64` knob against a derived default (knob values parse as `usize`,
/// which covers every Cayenne `u64` knob's range on 64-bit targets).
#[must_use]
pub(crate) fn auto_or_u64(acceleration: &Acceleration, keys: &[&str], derived: u64) -> u64 {
    match read_knob(acceleration, keys) {
        Knob::Auto => derived,
        Knob::Set(n) => u64::try_from(n).unwrap_or(derived),
    }
}

/// Resolve an `i64` knob against a derived default. Used by the inline-flush
/// caps, whose engine fields are `i64`. A value exceeding `i64::MAX` falls back
/// to the derived default.
#[must_use]
pub(crate) fn auto_or_i64(acceleration: &Acceleration, keys: &[&str], derived: i64) -> i64 {
    match read_knob(acceleration, keys) {
        Knob::Auto => derived,
        Knob::Set(n) => i64::try_from(n).unwrap_or(derived),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const MIB_U: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB_U;

    fn accel_with(params: &[(&str, &str)]) -> Acceleration {
        let mut p = HashMap::new();
        for (k, v) in params {
            p.insert((*k).to_string(), (*v).to_string());
        }
        Acceleration {
            params: p,
            ..Default::default()
        }
    }

    fn profile(cores: usize, mem: u64, storage: ResolvedAccelerationStorage) -> HardwareProfile {
        HardwareProfile::new(cores, mem, storage, storage)
    }

    // ---- read_knob / auto_or_* --------------------------------------------

    #[test]
    fn knob_unset_is_auto() {
        let accel = accel_with(&[]);
        assert_eq!(read_knob(&accel, &["cayenne_segment_cache_mb"]), Knob::Auto);
        assert_eq!(auto_or_usize(&accel, &["cayenne_segment_cache_mb"], 256), 256);
    }

    #[test]
    fn knob_literal_auto_is_auto_case_insensitive() {
        for v in ["auto", "AUTO", "Auto", " auto "] {
            let accel = accel_with(&[("cayenne_segment_cache_mb", v)]);
            assert_eq!(
                read_knob(&accel, &["cayenne_segment_cache_mb"]),
                Knob::Auto,
                "{v:?} should resolve to Auto"
            );
            assert_eq!(
                auto_or_usize(&accel, &["cayenne_segment_cache_mb"], 512),
                512,
                "auto must fall through to the derived default"
            );
        }
    }

    #[test]
    fn knob_explicit_value_overrides_derived() {
        let accel = accel_with(&[("cayenne_segment_cache_mb", "777")]);
        assert_eq!(
            read_knob(&accel, &["cayenne_segment_cache_mb"]),
            Knob::Set(777)
        );
        assert_eq!(auto_or_usize(&accel, &["cayenne_segment_cache_mb"], 256), 777);
    }

    #[test]
    fn knob_zero_is_set_not_auto() {
        // `0` is a meaningful value for several knobs (disable inlining, disable
        // background compaction); it must reach the call site, not become auto.
        let accel = accel_with(&[("cayenne_inline_max_rows", "0")]);
        assert_eq!(read_knob(&accel, &["cayenne_inline_max_rows"]), Knob::Set(0));
        assert_eq!(auto_or_usize(&accel, &["cayenne_inline_max_rows"], 1024), 0);
    }

    #[test]
    fn knob_invalid_falls_back_to_auto() {
        let accel = accel_with(&[("cayenne_segment_cache_mb", "lots")]);
        assert_eq!(read_knob(&accel, &["cayenne_segment_cache_mb"]), Knob::Auto);
        assert_eq!(auto_or_usize(&accel, &["cayenne_segment_cache_mb"], 256), 256);
    }

    #[test]
    fn knob_first_alias_wins() {
        let accel = accel_with(&[
            ("cayenne_inline_flush_max_rows", "5"),
            ("inline_memtable_max_rows", "9"),
        ]);
        assert_eq!(
            read_knob(
                &accel,
                &["cayenne_inline_flush_max_rows", "inline_memtable_max_rows"]
            ),
            Knob::Set(5)
        );
    }

    #[test]
    fn auto_or_u64_and_i64_resolve() {
        let auto = accel_with(&[("cayenne_compaction_background_interval_ms", "auto")]);
        assert_eq!(
            auto_or_u64(&auto, &["cayenne_compaction_background_interval_ms"], 30_000),
            30_000
        );
        let set = accel_with(&[("cayenne_compaction_background_interval_ms", "5000")]);
        assert_eq!(
            auto_or_u64(&set, &["cayenne_compaction_background_interval_ms"], 30_000),
            5_000
        );
        let i = accel_with(&[("cayenne_inline_flush_max_bytes", "auto")]);
        assert_eq!(auto_or_i64(&i, &["cayenne_inline_flush_max_bytes"], 999), 999);
    }

    // ---- pk_keyset_cache_mb -----------------------------------------------

    #[test]
    fn keyset_floor_and_ceiling() {
        // Small host clamps to the 256 MiB floor.
        assert_eq!(
            profile(4, 4 * GIB, ResolvedAccelerationStorage::Ebs).pk_keyset_cache_mb(),
            256
        );
        // Huge host clamps to the 8 GiB ceiling.
        assert_eq!(
            profile(128, 1024 * GIB, ResolvedAccelerationStorage::LocalSsd).pk_keyset_cache_mb(),
            8 * 1024
        );
        // Degenerate input never panics.
        assert_eq!(
            profile(1, u64::MAX, ResolvedAccelerationStorage::Unknown).pk_keyset_cache_mb(),
            8 * 1024
        );
        // Mid host scales to ~1/32 of RAM (64 GiB → 2 GiB).
        assert_eq!(
            profile(16, 64 * GIB, ResolvedAccelerationStorage::Ebs).pk_keyset_cache_mb(),
            2 * 1024
        );
    }

    // ---- segment_cache_mb -------------------------------------------------

    #[test]
    fn segment_cache_never_regresses_and_scales_up() {
        // Never below the historical 256 MiB default, on any host.
        for mem in [GIB, 8 * GIB, 32 * GIB] {
            assert_eq!(
                profile(4, mem, ResolvedAccelerationStorage::Ebs).segment_cache_mb(),
                256,
                "small/typical hosts keep the 256 MiB default (mem={mem})"
            );
        }
        // Scales up on memory-rich hosts (~1/128 RAM), capped at 1 GiB.
        assert_eq!(
            profile(64, 64 * GIB, ResolvedAccelerationStorage::LocalSsd).segment_cache_mb(),
            512
        );
        assert_eq!(
            profile(128, 128 * GIB, ResolvedAccelerationStorage::LocalSsd).segment_cache_mb(),
            1024
        );
        assert_eq!(
            profile(128, 1024 * GIB, ResolvedAccelerationStorage::LocalSsd).segment_cache_mb(),
            1024,
            "ceiling at 1 GiB"
        );
    }

    // ---- target_file_size_mb_override -------------------------------------

    #[test]
    fn target_file_size_override_per_storage() {
        let p = |s| profile(8, 32 * GIB, s).target_file_size_mb_override();
        assert_eq!(p(ResolvedAccelerationStorage::Ebs), Some(256));
        assert_eq!(p(ResolvedAccelerationStorage::Tmpfs), Some(64));
        assert_eq!(p(ResolvedAccelerationStorage::LocalSsd), None);
        assert_eq!(p(ResolvedAccelerationStorage::Unknown), None);
    }

    // ---- inline_flush_caps (relocated from mod.rs) ------------------------

    #[test]
    fn inline_flush_caps_scale_with_memory_and_storage() {
        let caps = |mem, storage| profile(8, mem, storage).inline_flush_caps();

        // Floor: hosts at/under the threshold keep the historical small-write
        // caps. Pins the FLOOR_BYTES ↔ InlineFlushCaps::FLOOR coupling.
        assert_eq!(
            caps(256 * MIB_U, ResolvedAccelerationStorage::Ebs),
            InlineFlushCaps::FLOOR
        );
        assert_eq!(InlineFlushCaps::FLOOR.max_bytes, 2_097_152); // 2 MiB
        assert_eq!(InlineFlushCaps::FLOOR.max_rows, 2_048);
        assert_eq!(InlineFlushCaps::FLOOR.max_segments, 16);

        // Degenerate inputs never panic; they clamp to floor / ceiling.
        assert_eq!(
            caps(0, ResolvedAccelerationStorage::Unknown),
            InlineFlushCaps::FLOOR
        );
        assert_eq!(
            caps(u64::MAX, ResolvedAccelerationStorage::LocalSsd).max_bytes,
            268_435_456 // 256 MiB ceiling, no overflow
        );

        // Per-class ceilings on a very large host.
        assert_eq!(
            caps(1024 * GIB, ResolvedAccelerationStorage::LocalSsd).max_bytes,
            268_435_456 // 256 MiB
        );
        assert_eq!(
            caps(1024 * GIB, ResolvedAccelerationStorage::Ebs).max_bytes,
            134_217_728 // 128 MiB
        );
        assert_eq!(
            caps(1024 * GIB, ResolvedAccelerationStorage::Unknown).max_bytes,
            134_217_728 // 128 MiB (== Ebs, the safe default)
        );
        assert_eq!(
            caps(1024 * GIB, ResolvedAccelerationStorage::Tmpfs).max_bytes,
            67_108_864 // 64 MiB (RAM-backed → smallest)
        );

        // Faster medium ⇒ strictly larger memtable at equal memory.
        let mem = 64 * GIB;
        let ssd = caps(mem, ResolvedAccelerationStorage::LocalSsd);
        let ebs = caps(mem, ResolvedAccelerationStorage::Ebs);
        let tmpfs = caps(mem, ResolvedAccelerationStorage::Tmpfs);
        assert!(ssd.max_bytes > ebs.max_bytes);
        assert!(ebs.max_bytes > tmpfs.max_bytes);

        // Mid-range scales between floor and ceiling (4 GiB on Ebs ⇒ 32 MiB),
        // and rows/segments stay derived from the byte budget.
        let mid = caps(4 * GIB, ResolvedAccelerationStorage::Ebs);
        assert_eq!(mid.max_bytes, 33_554_432); // 32 MiB
        assert_eq!(mid.max_rows, mid.max_bytes / 1024);
        assert_eq!(mid.max_segments, (mid.max_bytes / (128 * 1024)).clamp(16, 256));
    }

    // ---- the host matrix (local form of the CH-benCH host sweep) ----------

    /// Across a representative AWS-instance matrix (4–128 cores, 8 GiB–256 GiB,
    /// every storage class), the derived knobs stay within their documented
    /// bounds and never panic. This is the deterministic, host-independent proof
    /// that `auto` produces a sane config "regardless of the host machine".
    #[test]
    fn derived_config_is_sane_across_the_host_matrix() {
        let cores = [4_usize, 8, 16, 32, 64, 96, 128];
        let mems = [8 * GIB, 16 * GIB, 32 * GIB, 64 * GIB, 128 * GIB, 256 * GIB];
        let storages = [
            ResolvedAccelerationStorage::Ebs,
            ResolvedAccelerationStorage::LocalSsd,
            ResolvedAccelerationStorage::Tmpfs,
            ResolvedAccelerationStorage::Unknown,
        ];

        for &c in &cores {
            for &m in &mems {
                for &data in &storages {
                    for &meta in &storages {
                        let hw = HardwareProfile::new(c, m, data, meta);

                        let keyset = hw.pk_keyset_cache_mb();
                        assert!(
                            (256..=8 * 1024).contains(&keyset),
                            "keyset {keyset} out of bounds for {hw:?}"
                        );

                        let segment = hw.segment_cache_mb();
                        assert!(
                            (256..=1024).contains(&segment),
                            "segment {segment} out of bounds for {hw:?}"
                        );

                        let caps = hw.inline_flush_caps();
                        assert!(
                            (FLOOR_FLUSH_BYTES..=256 * 1_048_576).contains(&caps.max_bytes),
                            "flush bytes {} out of bounds for {hw:?}",
                            caps.max_bytes
                        );
                        assert!((16..=256).contains(&caps.max_segments));
                        assert!(caps.max_rows >= FLOOR_FLUSH_ROWS);

                        // Storage-aware file-size override is well-defined.
                        match data {
                            ResolvedAccelerationStorage::Ebs => {
                                assert_eq!(hw.target_file_size_mb_override(), Some(256));
                            }
                            ResolvedAccelerationStorage::Tmpfs => {
                                assert_eq!(hw.target_file_size_mb_override(), Some(64));
                            }
                            _ => assert_eq!(hw.target_file_size_mb_override(), None),
                        }
                    }
                }
            }
        }
    }

    /// Memory coherence: a realistic fleet of CDC tables, each at its derived
    /// per-table cache footprint, leaves comfortable headroom for the query
    /// memory pool on every host size — i.e. the per-table fractions compose
    /// without oversubscribing RAM. (CPU coherence is enforced separately by the
    /// process-global encode semaphore.)
    #[test]
    fn per_table_caches_compose_within_a_memory_budget() {
        const FLEET: u64 = 8; // a generous count of simultaneously-hot CDC tables
        for mem in [16 * GIB, 32 * GIB, 64 * GIB, 128 * GIB, 256 * GIB] {
            let hw = HardwareProfile::new(
                32,
                mem,
                ResolvedAccelerationStorage::Ebs,
                ResolvedAccelerationStorage::Ebs,
            );
            let keyset_bytes = u64::try_from(hw.pk_keyset_cache_mb()).unwrap_or(0) * MIB_U;
            let segment_bytes = u64::try_from(hw.segment_cache_mb()).unwrap_or(0) * MIB_U;
            let memtable_bytes = u64::try_from(hw.inline_flush_caps().max_bytes).unwrap_or(0);
            let per_table_bytes = keyset_bytes + segment_bytes + memtable_bytes;
            let fleet_bytes = per_table_bytes * FLEET;
            assert!(
                fleet_bytes < mem / 2,
                "fleet cache footprint {fleet_bytes} must stay under half of RAM {mem} (per-table {per_table_bytes})"
            );
        }
    }
}
