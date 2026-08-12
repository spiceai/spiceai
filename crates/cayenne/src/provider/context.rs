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

//! Shared context for Cayenne table operations.

use std::sync::Arc;

use datafusion_execution::{config::SessionConfig, runtime_env::RuntimeEnv};
use tokio::sync::Semaphore;
use vortex::VortexSessionDefault;
use vortex::file::WriteStrategyBuilder;
use vortex_datafusion::{ProjectionPushdown, VortexFormat, VortexTableOptions, WriteShardConfig};
use vortex_session::VortexSession;

use super::tuning::{self, ActuatorValues, IngestStats, LiveActuators, TuningBounds};
use crate::metadata::{
    DeletionMode, DeltaEncoding, PkConflictDetection, StorageClass, VortexConfig,
};

/// Shared context for Cayenne table operations.
///
/// Contains cached resources and configuration that can be shared across
/// multiple table providers (e.g., partitions of the same dataset).
///
/// # Sharing
///
/// The shared `RuntimeEnv` carries the `DataFusion` file metadata cache used by
/// Vortex for cached footer metadata. Sharing a `CayenneContext` across table
/// providers means they share that runtime-level cache, reducing repeated footer
/// reads when working with partitioned datasets.
#[derive(Debug)]
pub struct CayenneContext {
    /// Shared Vortex format for reading and writing data files.
    vortex_format: Arc<VortexFormat>,
    /// Configuration for encoding, compression, and file sizing.
    config: VortexConfig,
    /// Dataset label the formats are tagged with (metrics attribution).
    /// Retained so per-write format variants (e.g. delta-encoding strategy
    /// overrides) carry the same label as the shared base format.
    dataset: String,
    /// Session configuration for `DataFusion` listing options.
    session_config: SessionConfig,
    /// Shared semaphore for limiting concurrent file writes / uploads across all partitions.
    upload_semaphore: Arc<Semaphore>,
    /// Bounds concurrent inline-admission attempts on the OVERWRITE path across
    /// every table sharing this context — i.e. across the partition children of a
    /// partitioned dataset, whose overwrites all run at once under one
    /// coordinator. Exactly one slot, so the host-memory the runtime reserves for
    /// a single buffered admission (`inline_max_buffer_bytes`) plus its
    /// serialized blob (`inline_max_bytes`) per acceleration is TRUE rather than
    /// merely bigger.
    ///
    /// Acquired with `try_acquire`, never awaited: partition children are coupled
    /// writers fed by one routing demux, so parking here would stall the router
    /// and starve the slot-holding sibling of input — the hold-and-wait deadlock
    /// of spiceai/spiceai#11818. A child that cannot take the slot writes Vortex
    /// files instead, which is what every overwrite did before inlining existed.
    overwrite_inline_admission: Arc<Semaphore>,
    /// Shared `RuntimeEnv` from the main Spice runtime.
    ///
    /// Cayenne uses this `RuntimeEnv` for all internal `SessionContext`
    /// creation, ensuring that the `list_files_cache` (and other caches/object stores)
    /// are shared with the main query engine.
    runtime_env: Arc<RuntimeEnv>,
    /// Live, runtime-tunable copies of the per-operation actuators. Initialized from
    /// `config`, so reads are identical to the static config until the dynamic
    /// controller (if enabled) adjusts them. The hot-path accessors below read
    /// from here, making them the single choke point for dynamic tuning.
    live_actuators: Arc<LiveActuators>,
    /// Rolling CDC ingest accounting (input rate + runtime response) feeding the
    /// dynamic controller. Always recorded (cheap); only acted on when
    /// `dynamic_tuning` is enabled.
    ingest_stats: Arc<IngestStats>,
    /// Per-table query-side observations (p99 latency + QPH), shared with the
    /// process-global query registry so the runtime can push query metrics *down*
    /// into this table's tuner; read on the background tick to derive the
    /// query-latency / QPH goals. Registered (idempotently) at construction.
    query_observations: Arc<tuning::QueryObservations>,
    /// Operator-configured tuning goals (SLOs). When any is set, `retune` runs the
    /// goal-seeking controller; otherwise the legacy signal-driven one. Built once
    /// from `config` at construction.
    goals: tuning::Goals,
    /// Static `[floor, ceiling]` the controller may move each live actuator within.
    tuning_bounds: TuningBounds,
    /// Whether the closed-loop controller may mutate `live_actuators` (off by
    /// default; the accounting still records regardless).
    dynamic_tuning: bool,
    /// Wall-clock of the last applied dynamic adjustment, for the controller's
    /// dwell-time hysteresis. `None` until the first adjustment.
    last_adjust: parking_lot::Mutex<Option<std::time::Instant>>,
    /// Recorded-batch count at the last applied dynamic adjustment, for the
    /// controller's fresh-sample gate: the write-derived signals only advance on a
    /// CDC write, so a behind/bursty signal is only actionable when new batches
    /// have arrived since the last move (otherwise an idle table would ratchet its
    /// actuators to their extremes). `0` until the first adjustment.
    last_adjust_samples: std::sync::atomic::AtomicU64,
    /// Recorded-batch count at the last seq-prefix-bake back-pressure check, for
    /// that gate's own fresh-sample test (independent of the controller's
    /// `last_adjust_samples`). `0` until the first check.
    bake_gate_last_samples: std::sync::atomic::AtomicU64,
    /// Consecutive eligible control ticks on which a goal stayed violated while the
    /// controller had no move left — no eligible adjustment, whether because every
    /// relevant lever is clamped at its bound OR the helpful lever is blocked by a
    /// resource gate (e.g. memory/CPU pressure). At
    /// [`tuning::GOAL_INFEASIBLE_STUCK_TICKS`] the SLO is declared infeasible on the
    /// current hardware — surfaced via [`Self::goal_slo_infeasible`] (telemetry) and
    /// one operator warning per infeasible *episode* (fired on the crossing tick).
    /// Deliberately NOT a permanent latch: reset to 0 whenever the controller makes a
    /// move again or the goal is met, so the signal self-clears if the SLO becomes
    /// reachable (and re-warns only if a fresh episode crosses the threshold again).
    /// `0` when not goal-driven.
    goal_stuck_ticks: std::sync::atomic::AtomicU64,
    /// Wall-clock of the previous recorded CDC write, used to derive the
    /// inter-batch arrival interval (the offered-load signal). `None` until the
    /// first write.
    last_write: parking_lot::Mutex<Option<std::time::Instant>>,
    /// Whether this table's writes are COUPLED to sibling writers through a
    /// shared input demux — true for the partition child tables of a
    /// partitioned dataset, whose per-partition writes are all fed by one
    /// router over bounded channels. Coupled writes must never park on the
    /// global encode budget: a parked child stalls the router, which starves
    /// the permit-holding siblings of input — a hold-and-wait deadlock that
    /// left partitioned tables permanently unready (spiceai/spiceai#11818).
    /// Set only at construction ([`Self::new_for_partition_child`]); read by
    /// the write path to bypass `write_budget` permit acquisition. Runtime-only
    /// state — never persisted.
    coupled_writer: std::sync::atomic::AtomicBool,
    /// Set of data files whose integrity digest has already been verified this
    /// process, keyed by `"<snapshot_id>/<file_path>"`. Used only when
    /// `integrity_checksums` is enabled, to bound verification to one whole-file
    /// read per file per process ("verify on first read"). Data files are
    /// immutable once published, so a verified file never needs re-checking.
    verified_data_files: parking_lot::Mutex<std::collections::HashSet<String>>,
}

/// Default byte budget for the in-memory PK keyset cache when
/// `cayenne_pk_keyset_cache_mb` is unset. 256 MiB preserves historical behavior;
/// raise the param on memory-rich hosts so high-cardinality tables keep their
/// keyset resident instead of rebuilding it from a full-table scan every CDC
/// batch.
pub(crate) const DEFAULT_PK_KEYSET_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Hard ceiling on the configurable PK keyset cache budget. The budget bounds
/// the exact keyset's resident growth (and caps the right-sized conversion
/// blooms), so an out-of-range or typo'd `cayenne_pk_keyset_cache_mb` must not
/// be able to request a near-`usize::MAX` allocation. Matches the
/// auto-default's 8 GiB ceiling.
pub(crate) const PK_KEYSET_CACHE_MAX_CONFIGURABLE_BYTES: usize = 8 * 1024 * 1024 * 1024;

impl CayenneContext {
    /// Create a new Cayenne context from configuration.
    ///
    /// This creates a new `VortexFormat`. The shared runtime's file metadata cache
    /// is configured once by the owning runtime before table providers are created.
    #[must_use]
    pub fn new(config: &VortexConfig, runtime_env: Arc<RuntimeEnv>, dataset: &str) -> Arc<Self> {
        let vortex_format = Self::create_vortex_format(config, dataset);
        // Seed the live actuators from the static config so every hot-path accessor
        // reads exactly the static value until (and unless) the controller moves
        // it — enabling dynamic tuning is therefore a strict, bounded refinement,
        // never a behavior change on its own.
        let cores = cpu_budget::cpu_budget().cayenne_write_concurrency_ceiling();
        // When `write_concurrency` is unset, seed the live actuator to the SAME value
        // the write path resolves to (`DEFAULT_WRITE_CONCURRENCY` capped by host
        // cores), not 0. The controller grows from this real current value; a 0
        // seed (which the accessor and `decide()` both treat as 1) would make the
        // first "raise under backpressure" actually DECREASE effective concurrency
        // (e.g. default 4 → 2). Equivalent to unset for the write path, so this is
        // not a behavior change when dynamic tuning is off.
        let default_write_concurrency = super::table::DEFAULT_WRITE_CONCURRENCY.min(cores);
        let wc_init = config
            .write_concurrency
            .unwrap_or(default_write_concurrency);
        // Target Vortex file size as a byte budget for the adaptive controller,
        // seeded from the configured (storage-tier-aware) size. `0` keeps
        // size-rolling disabled.
        let target_file_size_bytes_init = i64::try_from(
            config
                .target_vortex_file_size_mb
                .saturating_mul(1024 * 1024),
        )
        .unwrap_or(i64::MAX);
        let live_actuators = Arc::new(LiveActuators::new(ActuatorValues {
            inline_flush_max_bytes: config.inline_flush_max_bytes,
            inline_flush_max_rows: config.inline_flush_max_rows,
            inline_flush_max_segments: config.inline_flush_max_segments,
            compaction_background_interval_ms: config.compaction_background_interval_ms,
            compaction_trigger_files: config.compaction_trigger_files,
            bake_deletion_index_trigger: config.bake_deletion_index_trigger,
            write_concurrency: wc_init,
            mem_tier_max_bytes: config.cdc_mem_tier_max_bytes,
            target_vortex_file_size_bytes: target_file_size_bytes_init,
            // Starts at 0 (no queries shed). Only a violated lag/freshness goal
            // under CPU contention drives it up — inert otherwise.
            query_admission_reserve: 0,
        }));
        // Bounds keep the controller within sane, memory-/cpu-safe ranges. The
        // memtable and mem-tier ceilings are derived from the runtime-installed
        // memory budget so adaptive can use more RAM on large hosts while still
        // shrinking first under observed memory pressure; concurrency is capped at
        // the core count (the global encode budget caps the aggregate).
        let inline_flush_bounds =
            tuning::adaptive_inline_flush_bounds(config.inline_flush_max_bytes);
        let mem_tier_bounds = tuning::adaptive_mem_tier_bounds(config.cdc_mem_tier_max_bytes);
        // A pinned (operator-set) actuator's bounds collapse to a single point so the
        // controller can never move it — that is how an explicit per-value
        // override is respected even in `adaptive` mode (`decide()` finds no room
        // and falls through to another, un-pinned lever).
        let pins = config.pinned_tuning_actuators;
        let tuning_bounds = TuningBounds {
            inline_flush_max_bytes: if pins.inline_flush {
                (config.inline_flush_max_bytes, config.inline_flush_max_bytes)
            } else {
                inline_flush_bounds
            },
            compaction_background_interval_ms: if pins.compaction_interval
                || config.compaction_background_interval_ms == 0
            {
                // A 0 interval means the background compactor was never spawned
                // (`spawn_background_compaction` returns early), so letting the
                // controller raise it off 0 would only make the reported actuator
                // value disagree with reality. Collapse the range instead.
                (
                    config.compaction_background_interval_ms,
                    config.compaction_background_interval_ms,
                )
            } else {
                (2_000, 60_000)
            },
            compaction_trigger_files: if pins.compaction_trigger {
                (
                    config.compaction_trigger_files,
                    config.compaction_trigger_files,
                )
            } else {
                (2, 32)
            },
            bake_deletion_index_trigger: if pins.bake_deletion_index_trigger {
                (
                    config.bake_deletion_index_trigger,
                    config.bake_deletion_index_trigger,
                )
            } else {
                (1_000, 5_000_000)
            },
            write_concurrency: if pins.write_concurrency {
                (wc_init, wc_init)
            } else {
                (1, cores)
            },
            mem_tier_max_bytes: if pins.mem_tier {
                (config.cdc_mem_tier_max_bytes, config.cdc_mem_tier_max_bytes)
            } else {
                mem_tier_bounds
            },
            // Pin (collapse the bounds) when the operator set the file size
            // explicitly — including `0` to disable size-rolling — so the
            // controller never enables or moves an operator-chosen value.
            target_vortex_file_size_bytes: if pins.target_file_size
                || target_file_size_bytes_init <= 0
            {
                (target_file_size_bytes_init, target_file_size_bytes_init)
            } else {
                tuning::adaptive_target_file_size_bounds(target_file_size_bytes_init)
            },
            // Reserve up to `cores` query-admission slots for CDC apply under
            // contention; the process-global governor re-clamps the reported demand
            // to the real admission pool's `max - 1`, so this cores-scale ceiling is
            // a safe upper bound regardless of `runtime.query.max_concurrent_queries`.
            query_admission_reserve: (0, cores),
        };
        // Register (idempotently) this table's query-observations handle in the
        // process-global registry so the runtime's query tracker can push p99
        // latency / QPH down into it (the `runtime` crate cannot be imported here).
        let query_observations = tuning::register_query_observations(dataset);
        // Resolve the operator-configured goals (SLOs). When none are set, the
        // controller stays on the legacy signal-driven path.
        let goals = tuning::Goals::from_targets(
            config.goal_replication_lag_secs,
            config.goal_freshness_secs,
            config.goal_query_latency_ms,
            config.goal_qph,
            config
                .goal_convergence_window_secs
                .filter(|s| *s > 0.0)
                .map_or(tuning::DEFAULT_GOAL_CONVERGENCE_WINDOW, |s| {
                    std::time::Duration::from_secs_f64(s)
                }),
        );
        Arc::new(Self {
            vortex_format,
            config: config.clone(),
            dataset: dataset.to_string(),
            session_config: SessionConfig::default(),
            upload_semaphore: Arc::new(Semaphore::new(config.upload_concurrency.max(1))),
            overwrite_inline_admission: Arc::new(Semaphore::new(1)),
            runtime_env,
            live_actuators,
            ingest_stats: Arc::new(IngestStats::new()),
            query_observations,
            goals,
            tuning_bounds,
            dynamic_tuning: config.dynamic_tuning,
            last_adjust: parking_lot::Mutex::new(None),
            last_adjust_samples: std::sync::atomic::AtomicU64::new(0),
            bake_gate_last_samples: std::sync::atomic::AtomicU64::new(0),
            goal_stuck_ticks: std::sync::atomic::AtomicU64::new(0),
            last_write: parking_lot::Mutex::new(None),
            coupled_writer: std::sync::atomic::AtomicBool::new(false),
            verified_data_files: parking_lot::Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// Create the shared context for the partition CHILD tables of a
    /// partitioned dataset. Identical to [`Self::new`] except the tables are
    /// marked as coupled writers (see [`Self::is_coupled_writer`]): their
    /// writes are all fed by one routing demux over bounded channels, so they
    /// must never park on the global encode budget.
    #[must_use]
    pub fn new_for_partition_child(
        config: &VortexConfig,
        runtime_env: Arc<RuntimeEnv>,
        dataset: &str,
    ) -> Arc<Self> {
        let context = Self::new(config, runtime_env, dataset);
        context
            .coupled_writer
            .store(true, std::sync::atomic::Ordering::Relaxed);
        context
    }

    /// Whether this table's writes are coupled to sibling writers through a
    /// shared input demux (partition child tables). Coupled writes bypass the
    /// global encode budget — parking there deadlocks the demux
    /// (spiceai/spiceai#11818).
    #[must_use]
    pub fn is_coupled_writer(&self) -> bool {
        self.coupled_writer
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Encoding effort configured for delta writes (`cayenne_delta_encoding`).
    #[must_use]
    pub fn delta_encoding(&self) -> DeltaEncoding {
        self.config.delta_encoding
    }

    /// Build a write-only `VortexFormat` whose session carries a
    /// [`WriteStrategyBuilder`] override — used by light delta-encoding levels
    /// (see `provider::delta_encoding`). The format mirrors the shared base
    /// format's table options and dataset label; `shard` optionally enables
    /// intra-write sharding exactly like
    /// `CayenneTableProvider::write_shard_format` does on the default path.
    ///
    /// Scans never observe these formats: the scan path keeps using
    /// [`Self::file_format`], so the strategy override affects only the files
    /// this write produces.
    #[must_use]
    pub(crate) fn write_format_with_strategy(
        &self,
        strategy: WriteStrategyBuilder,
        shard: Option<WriteShardConfig>,
    ) -> Arc<VortexFormat> {
        let session = VortexSession::default().set(strategy);
        let format =
            VortexFormat::new_with_options(session, Self::vortex_table_options(&self.config))
                .with_dataset_label(self.dataset.as_str());
        let format = match shard {
            Some(config) => format.with_write_shard(config),
            None => format,
        };
        Arc::new(format)
    }

    /// Build a write-only `VortexFormat` for the cold (datalake) tier whose
    /// file-rolling target is `cold_target_file_size_mb` rather than the warm
    /// `target_vortex_file_size_mb`.
    #[must_use]
    pub(crate) fn cold_write_format(
        &self,
        cold_target_file_size_mb: usize,
        shard: Option<WriteShardConfig>,
    ) -> Arc<VortexFormat> {
        let mut session = VortexSession::default();
        if let Some(full_strategy) =
            super::delta_encoding::full_strategy_builder_for(&self.config.compression_strategy)
        {
            session = session.set(full_strategy);
        }
        let mut options = Self::vortex_table_options(&self.config);
        options.target_file_size_mb = cold_target_file_size_mb;
        // Write-only format: it never scans, so drop the read-path segment cache
        // and avoid constructing a `SharedSegmentCache` (moka + metrics) per
        // promotion.
        options.segment_cache_size_bytes = None;
        let format = VortexFormat::new_with_options(session, options)
            .with_dataset_label(self.dataset.as_str());
        let format = match shard {
            Some(config) => format.with_write_shard(config),
            None => format,
        };
        Arc::new(format)
    }

    /// Get the Vortex file format for creating listing tables.
    ///
    /// The format uses the shared runtime cache for file metadata.
    #[must_use]
    pub fn file_format(&self) -> &Arc<VortexFormat> {
        &self.vortex_format
    }

    /// Get the Vortex configuration.
    #[must_use]
    pub fn config(&self) -> &VortexConfig {
        &self.config
    }

    /// Get the session configuration for `DataFusion` listing options.
    #[must_use]
    pub fn session_config(&self) -> &SessionConfig {
        &self.session_config
    }

    /// Get the target file size in bytes for chunking data files.
    #[must_use]
    pub fn target_file_size_bytes(&self) -> usize {
        // The live actuator value: seeded from the configured (tier-aware) size
        // and grown by the adaptive controller for query goals (bounded by the
        // static config). `<= 0` keeps size-rolling disabled.
        usize::try_from(self.live_actuators.target_vortex_file_size_bytes().max(0)).unwrap_or(0)
    }

    /// Get the sort columns if configured.
    #[must_use]
    pub fn sort_columns(&self) -> &[String] {
        &self.config.sort_columns
    }

    /// Check if sorting is enabled.
    ///
    /// True for BOTH user-configured and inference-derived sort columns, because
    /// every caller that asks this question is asking "will the rewrite produce
    /// a globally sorted snapshot?" — which is a property of the write, not of
    /// who chose the key. Callers deciding *precedence* (which key to sort by)
    /// must use [`Self::sort_columns_are_authoritative`] instead.
    #[must_use]
    pub fn has_sort_columns(&self) -> bool {
        !self.config.sort_columns.is_empty()
    }

    /// Whether [`Self::sort_columns`] is an operator statement of intent rather
    /// than a schema-inference guess.
    ///
    /// Only an authoritative sort order may shadow the hot filter columns
    /// observed on scans. An inferred order (the `PostgreSQL` CDC default, which
    /// resolves to the primary key) ranks *below* those observations, so the
    /// default-on adaptive layout can correct the guess.
    #[must_use]
    pub fn sort_columns_are_authoritative(&self) -> bool {
        !self.config.sort_columns.is_empty()
            && self.config.sort_columns_origin == crate::metadata::SortColumnsOrigin::User
    }

    /// Sort columns that schema inference supplied, if any — the lowest-priority
    /// rung of the layout precedence chain (below observed filter columns).
    /// Empty when the sort order is user-configured or absent.
    #[must_use]
    pub fn inferred_sort_columns(&self) -> &[String] {
        if self.config.sort_columns_origin == crate::metadata::SortColumnsOrigin::Inferred {
            &self.config.sort_columns
        } else {
            &[]
        }
    }

    /// Get the configured intra-write shard-key columns. Empty = derive the
    /// shard key from the primary key (the historical behavior).
    #[must_use]
    pub fn shard_key_columns(&self) -> &[String] {
        &self.config.shard_key_columns
    }

    /// Get the shared `RuntimeEnv`.
    #[must_use]
    pub fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    /// Get the maximum number of concurrent file uploads.
    #[must_use]
    pub fn upload_concurrency(&self) -> usize {
        self.config.upload_concurrency.max(1)
    }

    /// Get the writer partition override for unsorted snapshot writes (live:
    /// dynamic-tunable). `0` in the live actuator means "unset" (use the session
    /// default), mirroring `config.write_concurrency == None`.
    #[must_use]
    pub fn write_concurrency(&self) -> Option<usize> {
        let wc = self.live_actuators.write_concurrency();
        (wc != 0).then(|| wc.max(1))
    }

    /// Per-table in-memory CDC tier byte cap (`cdc_durability: memory`), read live
    /// so the controller can grow it under backpressure / shrink it under memory
    /// pressure. A non-positive live value means "no explicit per-table cap" and
    /// maps to `u64::MAX` (the process-global mem-tier budget still bounds
    /// aggregate RAM) — byte-identical to the static construction this replaced, so
    /// reading it is unchanged when dynamic tuning is off.
    #[must_use]
    pub(crate) fn mem_tier_max_bytes_capped(&self) -> u64 {
        let v = self.live_actuators.mem_tier_max_bytes();
        if v > 0 {
            u64::try_from(v).unwrap_or(u64::MAX)
        } else {
            u64::MAX
        }
    }

    /// Maximum rows in one write that may be inlined into the metastore.
    #[must_use]
    pub(crate) fn inline_max_rows(&self) -> usize {
        self.config.inline_max_rows
    }

    /// Maximum serialized IPC bytes in one inlined metastore entry.
    #[must_use]
    pub(crate) fn inline_max_bytes(&self) -> usize {
        self.config.inline_max_bytes
    }

    /// Maximum in-memory Arrow bytes buffered while deciding whether to inline.
    #[must_use]
    pub(crate) fn inline_max_buffer_bytes(&self) -> usize {
        self.config.inline_max_buffer_bytes
    }

    /// Maximum inline rows before checkpointing to Vortex (live: dynamic-tunable).
    #[must_use]
    pub(crate) fn inline_flush_max_rows(&self) -> i64 {
        self.live_actuators.inline_flush_max_rows().max(0)
    }

    /// Maximum inline entries before checkpointing to Vortex (live).
    #[must_use]
    pub(crate) fn inline_flush_max_segments(&self) -> i64 {
        self.live_actuators.inline_flush_max_segments().max(0)
    }

    /// Maximum inline IPC bytes before checkpointing to Vortex (live).
    #[must_use]
    pub(crate) fn inline_flush_max_bytes(&self) -> i64 {
        self.live_actuators.inline_flush_max_bytes().max(0)
    }

    /// Primary-key conflict detection behavior for inserts.
    #[must_use]
    pub(crate) fn pk_conflict_detection(&self) -> PkConflictDetection {
        self.config.pk_conflict_detection
    }

    /// How primary-key deletions are recorded and applied for PK tables.
    /// The default `auto` resolves to `position` (merge-on-read position-delete
    /// vectors); `key` is the opt-out that keeps the above-scan key-based filter.
    #[must_use]
    pub(crate) fn deletion_mode(&self) -> DeletionMode {
        self.config.deletion_mode
    }

    /// Byte budget for the in-memory PK keyset cache used during upsert conflict
    /// detection. See [`DEFAULT_PK_KEYSET_CACHE_MAX_BYTES`].
    #[must_use]
    pub(crate) fn pk_keyset_cache_max_bytes(&self) -> usize {
        self.config
            .pk_keyset_cache_mb
            .map_or(DEFAULT_PK_KEYSET_CACHE_MAX_BYTES, |mb| {
                // Cap the configured budget: it doubles as the bloom allocation
                // size, so a huge/typo'd value would otherwise saturate and try
                // to allocate ~`usize::MAX`. `0` is preserved (forces the bloom).
                mb.saturating_mul(1024 * 1024)
                    .min(PK_KEYSET_CACHE_MAX_CONFIGURABLE_BYTES)
            })
    }

    /// Build the compaction picker config from the underlying `VortexConfig`.
    #[must_use]
    pub(crate) fn compaction_picker_config(&self) -> super::compaction::CompactionPickerConfig {
        // `target_file_size_bytes` returns `usize`; widen via checked
        // conversion so a future 128-bit `usize` couldn't silently truncate
        // the tier thresholds. `u64::MAX` is a safe fallback because the
        // picker only ever asks "is bucket size < threshold".
        let target_bytes = u64::try_from(self.target_file_size_bytes()).unwrap_or(u64::MAX);
        super::compaction::CompactionPickerConfig::new(
            self.live_actuators.compaction_trigger_files(),
            self.config.compaction_max_files_per_pick,
            target_bytes,
        )
    }

    /// Deletion-index size (live PK tombstone count) at or above which the
    /// seq-prefix bake is triggered. Read live from the actuator so the adaptive
    /// controller can move it (`cayenne_bake_deletion_index_trigger`; seeded from
    /// the config, anchored to
    /// [`crate::provider::table::BAKE_DELETION_INDEX_TRIGGER`]).
    #[must_use]
    pub(crate) fn bake_deletion_index_trigger(&self) -> usize {
        self.live_actuators.bake_deletion_index_trigger()
    }

    /// Apply-back-pressure gate for the seq-prefix bake: `true` when the CDC
    /// apply is at or over capacity, so the background bake — whose merge
    /// (re-encode survivors + publish) competes with the apply for the shared
    /// write path (encode permits, the single-writer metastore, cores) — must
    /// DEFER this round and yield to the foreground writer. Keyed on
    /// `apply_vs_arrival` (per-batch apply latency ÷ offered-load interval;
    /// `>= 1` ⇒ no headroom, the apply is at or past break-even), gated on fresh ingest
    /// since the last check so a table that fell behind and then went idle does
    /// not block its bake forever on a stale EWMA. Returns `false` (allow the
    /// bake) before warmup or when ingest is idle — exactly the windows where
    /// baking is free.
    #[must_use]
    pub(crate) fn bake_should_defer_for_apply(&self) -> bool {
        use std::sync::atomic::Ordering;
        let snapshot = self.ingest_snapshot();
        let last = self
            .bake_gate_last_samples
            .swap(snapshot.samples, Ordering::Relaxed);
        let ingest_fresh = snapshot.samples > last;
        ingest_fresh
            && snapshot.samples >= tuning::WARMUP_BATCHES
            && snapshot.apply_vs_arrival >= tuning::BAKE_BACKPRESSURE_RATIO
    }

    /// Protected snapshot count that should trigger maintenance compaction.
    #[must_use]
    pub(crate) fn compaction_trigger_protected_snapshots(&self) -> usize {
        self.config.compaction_trigger_protected_snapshots.max(1)
    }

    /// Whether scans should resolve their file set from the per-snapshot
    /// manifest (`cayenne_snapshot_file`) rather than by listing the snapshot
    /// directory. Defaults to `false`; the scan falls back to directory listing
    /// for any snapshot whose manifest is empty even when this is `true`.
    #[must_use]
    pub(crate) fn scan_from_manifest(&self) -> bool {
        self.config.scan_from_manifest
    }

    /// Whether the query/scan path advertises and decodes `Utf8`/`Binary`
    /// columns as Arrow view types (`Utf8View`/`BinaryView`). See
    /// [`crate::metadata::VortexConfig::force_view_read_schema`].
    #[must_use]
    pub(crate) fn force_view_read_schema(&self) -> bool {
        self.config.force_view_read_schema
    }

    /// Whether end-to-end integrity checksums are enabled for the staging WAL
    /// and Vortex data files. See
    /// [`crate::metadata::VortexConfig::integrity_checksums`].
    #[must_use]
    pub(crate) fn integrity_checksums(&self) -> bool {
        self.config.integrity_checksums
    }

    /// Whether the data file keyed by `"<snapshot_id>/<file_path>"` has already
    /// had its integrity digest verified in this process.
    #[must_use]
    pub(crate) fn is_data_file_verified(&self, key: &str) -> bool {
        self.verified_data_files.lock().contains(key)
    }

    /// Record that the data file keyed by `"<snapshot_id>/<file_path>"` passed
    /// integrity verification, so it is not re-read on later scans.
    pub(crate) fn mark_data_file_verified(&self, key: String) {
        self.verified_data_files.lock().insert(key);
    }

    /// Maximum number of consecutive compaction passes per trigger.
    #[must_use]
    pub(crate) fn compaction_max_levels(&self) -> usize {
        self.config.compaction_max_levels.max(1)
    }

    /// Protected snapshot age that should trigger maintenance compaction.
    #[must_use]
    pub(crate) fn compaction_trigger_snapshot_age(&self) -> Option<std::time::Duration> {
        if self.config.compaction_trigger_snapshot_age_ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(
                self.config.compaction_trigger_snapshot_age_ms,
            ))
        }
    }

    /// Background compaction interval (live: dynamic-tunable). Returns `None`
    /// when disabled (interval = 0).
    #[must_use]
    pub(crate) fn compaction_background_interval(&self) -> Option<std::time::Duration> {
        let ms = self.live_actuators.compaction_background_interval_ms();
        if ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(ms))
        }
    }

    /// Periodic mem-tier checkpoint interval for `cdc_durability: memory`.
    /// Returns `None` when disabled (interval = 0). Read straight from the static
    /// config — unlike the compaction interval this is not a dynamically-tuned
    /// actuator, so the periodic mem-tier checkpoint cadence is fixed for the table's
    /// lifetime (the write-path byte/age caps absorb hot-table variation).
    #[must_use]
    pub(crate) fn mem_tier_checkpoint_interval(&self) -> Option<std::time::Duration> {
        let ms = self.config.cdc_mem_tier_checkpoint_interval_ms;
        if ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(ms))
        }
    }

    /// Max age of the ACTIVE ingestion piece before a **seal** durably shadows it
    /// and advances the source slot (`cdc_durability: memory`). Returns `None` when
    /// sealing is disabled (`cdc_mem_tier_seal_age_ms == 0`), in which case the
    /// slot ack reverts to the checkpoint cadence. Like the checkpoint interval
    /// this is a fixed time-domain durability-policy bound, not a tuned actuator.
    #[must_use]
    pub(crate) fn mem_tier_seal_age(&self) -> Option<std::time::Duration> {
        let ms = self.config.cdc_mem_tier_seal_age_ms;
        if ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(ms))
        }
    }

    /// Maximum age of buffered streaming-append data before the sink cuts the
    /// segment and publishes it. Returns `None` when disabled (interval = 0):
    /// the sink then publishes only when the input stream ends.
    #[must_use]
    pub(crate) fn stream_publish_interval(&self) -> Option<std::time::Duration> {
        let ms = self.config.stream_publish_interval_ms;
        if ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(ms))
        }
    }

    /// Get the shared semaphore for limiting concurrent file writes / uploads.
    #[must_use]
    pub fn upload_semaphore(&self) -> &Arc<Semaphore> {
        &self.upload_semaphore
    }

    /// Claim the single inline-admission slot for an overwrite, or `None` when a
    /// sibling table on this context already holds it. Never blocks — see
    /// [`Self::overwrite_inline_admission`]. The permit is held until the
    /// overwrite commits and publishes, because the buffered batches and the
    /// serialized blob stay resident for that whole span.
    pub(crate) fn try_acquire_overwrite_inline_admission(
        &self,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.overwrite_inline_admission)
            .try_acquire_owned()
            .ok()
    }

    /// Record one CDC write's measurements into the rolling ingest accounting.
    /// Always on (cheap: a few atomics + a short-held mutex per *batch*); feeds
    /// the dynamic controller and observability. The inter-batch arrival interval
    /// (offered-load signal) is derived here from the previous write's timestamp,
    /// so callers pass only what the write path already measures.
    pub(crate) fn record_ingest(
        &self,
        rows: u64,
        delete_rows: u64,
        bytes: u64,
        apply: std::time::Duration,
        source_commit_ts_ms: Option<i64>,
    ) {
        let now = std::time::Instant::now();
        let arrival_gap = {
            let mut last = self.last_write.lock();
            let gap = last.map(|t| now.saturating_duration_since(t));
            *last = Some(now);
            gap
        };
        self.ingest_stats.record_write(tuning::WriteSample {
            rows,
            bytes,
            apply,
            arrival_gap,
            delete_rows,
        });
        // Fold in the now-relative goal signals. Wall clock (epoch ms), NOT the
        // monotonic `Instant` above: lag/freshness are absolute "now − ts" ages.
        let now_ms = chrono::Utc::now().timestamp_millis();
        if let Some(ts_ms) = source_commit_ts_ms {
            self.ingest_stats.observe_source_commit_ts_ms(ts_ms);
        }
        // Fold this batch's end-to-end row freshness (apply wall-clock − the batch's
        // source-commit ts) into the rolling windowed PEAK — the worst-case
        // PG-commit→queryable lag the freshness SLO is stated against, and what the
        // freshness-goal shrink lever controls on. No-op when the source carries no
        // commit ts. Idle-immune (a post-idle batch measures its own small lag), so
        // it must be folded here on the apply path, before the visibility stamp.
        self.ingest_stats
            .fold_row_freshness(now_ms, source_commit_ts_ms);
        // Stamp freshness at apply time. Exact for the synchronous publish path;
        // for the backgrounded staged-CDC publish this trails true visibility by
        // the finalize latency, so it is a lower bound on staleness.
        self.ingest_stats.set_last_visible_ts_ms(now_ms);
    }

    /// Fold one CDC batch's object-store/disk write latency (the `vortex_write`
    /// phase) into the tuner's rolling EWMA. Called from the write path with the
    /// duration it already measured for telemetry.
    pub(crate) fn record_io_latency(&self, d: std::time::Duration) {
        self.ingest_stats.record_io_latency(d);
    }

    /// Fold one CDC batch's metastore publish latency (the `publish` phase — the
    /// single-writer commit) into the tuner's rolling EWMA.
    pub(crate) fn record_publish_latency(&self, d: std::time::Duration) {
        self.ingest_stats.record_publish_latency(d);
    }

    /// Current memory pressure (`used / budget`), or `None` when unsampled. A
    /// single relaxed atomic load — for hot paths (e.g. the checkpoint tick's
    /// critical-pressure check) that need only this one signal, not the full
    /// [`ingest_snapshot`](Self::ingest_snapshot) (mutex + clock + p99 + QPH).
    #[must_use]
    pub(crate) fn mem_pressure(&self) -> Option<f64> {
        self.ingest_stats.mem_pressure()
    }

    /// Test hook: inject a memory-pressure sample directly (production writes it
    /// via the controller's `observe_environment`).
    #[cfg(test)]
    pub(crate) fn set_mem_pressure_for_test(&self, fraction: f64) {
        self.ingest_stats.set_mem_pressure(fraction);
    }

    /// The detected storage medium backing this table's data files (from the
    /// runtime's acceleration-storage detection at registration, or the operator's
    /// `storage` param). A cheap field read — the compaction writer's tier gate
    /// uses it without building a full [`Self::ingest_snapshot`].
    #[must_use]
    pub(crate) fn data_storage_class(&self) -> StorageClass {
        self.config.data_storage_class
    }

    /// A snapshot of the current ingest accounting (rate + response), enriched with
    /// the now-relative CDC goal signals (replication lag, freshness) and the
    /// query-side goal signals (p99 latency, QPH) — the wall clock and the
    /// query-observations handle live here, keeping `IngestStats::snapshot` and
    /// `decide` clock-free/pure. For observability/logging and the control step.
    #[must_use]
    pub(crate) fn ingest_snapshot(&self) -> tuning::IngestSnapshot {
        let mut snap = self.ingest_stats.snapshot();
        let now_ms = chrono::Utc::now().timestamp_millis();
        snap.replication_lag_secs = self.ingest_stats.replication_lag_secs(now_ms);
        // `freshness_secs` carries the windowed-PEAK per-apply row freshness (worst
        // PG-commit→queryable lag over the last ~60s) rather than the instantaneous
        // `now − last_visible` age. The peak is the SLO signal — the instantaneous
        // value is sampled at a random phase (so it misses transient stalls the
        // freshness-goal shrink lever must react to) and ramps unbounded on an idle
        // table (so it reads as a false violation post-load). Both the freshness goal
        // and the `cayenne_ingest_freshness_seconds` gauge read this field, so they
        // share the robust signal. Falls back to the instantaneous age until the
        // first apply carrying a source-commit ts seeds the peak.
        snap.freshness_secs = self
            .ingest_stats
            .peak_row_freshness_secs(now_ms)
            .or_else(|| self.ingest_stats.freshness_secs(now_ms));
        snap.query_latency_p99_ms = self.query_observations.p99_latency_ms();
        // QPH is system-wide (a query spanning datasets counts once), so every
        // table's controller reads the process-global aggregate — NOT this table's
        // own rate, which would multiply-count joins across their participants.
        snap.qph = tuning::global_qph();
        // Per-table static storage classes + measured calibration-probe throughput
        // (detected at registration) — the loop reasons over them via
        // `IngestSnapshot` (the continuous slow-tier bias), keeping `decide` pure.
        snap.data_storage = self.config.data_storage_class;
        snap.metastore_storage = self.config.metastore_storage_class;
        snap.data_write_mbps = self.config.data_storage_write_mbps;
        snap.metastore_write_mbps = self.config.metastore_storage_write_mbps;
        snap
    }

    /// Current live actuator values (after any dynamic adjustments), for metrics.
    #[must_use]
    pub(crate) fn live_actuator_values(&self) -> tuning::ActuatorValues {
        self.live_actuators.values()
    }

    /// Whether closed-loop dynamic tuning is active for this table (an SLO goal is
    /// set / `cayenne_tuning: adaptive`). Gates the per-tick query-admission reserve
    /// report so it is a strict no-op for non-adaptive tables.
    #[must_use]
    pub(crate) fn dynamic_tuning_enabled(&self) -> bool {
        self.dynamic_tuning
    }

    /// The operator-configured tuning goals (for telemetry/observability — the
    /// control step reads them internally).
    #[must_use]
    pub(crate) fn goals(&self) -> tuning::Goals {
        self.goals
    }

    /// Refresh the externally-observed environment/response signals (read amp +
    /// cgroup memory pressure) so the next snapshot/control step sees fresh data.
    /// Called on the background tick regardless of whether tuning is enabled, so
    /// the accounting gauges stay live for observability.
    pub(crate) fn observe_environment(&self, read_amp: usize) {
        self.ingest_stats.set_read_amp(read_amp);
        tuning::sample_mem_pressure(&self.ingest_stats);
        // Process-global CPU busy-fraction (cgroup-aware); read by every table's
        // snapshot. Sampled here once per tick (before `retune`).
        tuning::sample_cpu_pressure();
    }

    /// Run one dynamic-tuning control step from the current accounting, applying
    /// at most one bounded actuator change to [`Self::live_actuators`]. Returns the
    /// adjustment made (for logging) or `None` when tuning is disabled or no
    /// change is warranted. Owns the dwell clock: `min_dwell` is the minimum
    /// spacing enforced between applied changes.
    pub(crate) fn retune(&self, min_dwell: std::time::Duration) -> Option<tuning::Adjustment> {
        use std::sync::atomic::Ordering;
        if !self.dynamic_tuning {
            return None;
        }
        // Memory pressure is sampled once per tick by `observe_environment`, which
        // the same tick body runs first: re-sampling here would re-read the cgroup
        // files microseconds later for no new information, and would let the
        // controller act on a different reading than the one the tick already
        // exported as a gauge.
        let now = std::time::Instant::now();
        let since_last = (*self.last_adjust.lock()).map_or(std::time::Duration::MAX, |t| {
            now.saturating_duration_since(t)
        });
        // Enriched snapshot (adds the now-relative lag/freshness + query p99/QPH
        // signals the goal controller reads); reduces to the legacy snapshot when
        // no goals are set.
        let snapshot = self.ingest_snapshot();
        // Relearn the observed mean row width from live ingest (EWMA bytes ÷ rows)
        // so a later inline-flush byte-budget move derives a row cap matching the
        // table's real rows, not a stale static estimate. Only with a confident
        // rate estimate; cheap (one atomic store).
        if snapshot.rows_per_sec > 1.0 && snapshot.bytes_per_sec > 0.0 {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "bytes-per-row is a small positive value; clamped in observe_mean_row_bytes"
            )]
            let bytes_per_row = (snapshot.bytes_per_sec / snapshot.rows_per_sec) as i64;
            self.live_actuators.observe_mean_row_bytes(bytes_per_row);
        }
        let samples_at_last_move = self.last_adjust_samples.load(Ordering::Relaxed);
        let adj = tuning::decide_with_goals(
            &snapshot,
            &self.live_actuators.values(),
            &self.tuning_bounds,
            since_last,
            min_dwell,
            samples_at_last_move,
            &self.goals,
        );

        // Infeasible-SLO tracking before we (maybe) apply: a goal that stays
        // violated on eligible ticks with NO move available means the controller has
        // run out of levers — surface it instead of crawling into the wall silently.
        self.track_goal_feasibility(
            &snapshot,
            since_last,
            min_dwell,
            samples_at_last_move,
            adj.is_some(),
        );

        let adj = adj?;
        self.live_actuators.apply(&adj);
        *self.last_adjust.lock() = Some(now);
        self.last_adjust_samples
            .store(snapshot.samples, Ordering::Relaxed);
        Some(adj)
    }

    /// Update the infeasible-SLO tracker for one control tick (see
    /// [`Self::goal_stuck_ticks`]). A made move or a met goal resets the counter; an
    /// eligible tick (past warmup + dwell) that is still violated with no move
    /// increments it. On crossing [`tuning::GOAL_INFEASIBLE_STUCK_TICKS`] it warns
    /// once for that episode, naming the binding constraint; the gauge
    /// ([`Self::goal_slo_infeasible`]) reflects current state and is read separately.
    fn track_goal_feasibility(
        &self,
        snapshot: &tuning::IngestSnapshot,
        since_last: std::time::Duration,
        min_dwell: std::time::Duration,
        samples_at_last_move: u64,
        moved: bool,
    ) {
        use std::sync::atomic::Ordering;
        if !self.goals.any_active() {
            return;
        }
        let ingest_fresh = snapshot.samples > samples_at_last_move;
        let violated = self.goals.any_actionable_violation(snapshot, ingest_fresh);
        if moved || !violated {
            self.goal_stuck_ticks.store(0, Ordering::Relaxed);
            return;
        }
        // Violated but no move: only "stuck" once we are eligible to have moved
        // (past warmup + the goal dwell) — otherwise `None` just means dwell/warmup.
        let dwell = self.goals.control_dwell(min_dwell);
        if snapshot.samples < tuning::WARMUP_BATCHES || since_last < dwell {
            return;
        }
        let stuck = self.goal_stuck_ticks.fetch_add(1, Ordering::Relaxed) + 1;
        if stuck == tuning::GOAL_INFEASIBLE_STUCK_TICKS {
            tracing::warn!(
                table = %self.dataset,
                constraint = tuning::binding_constraint(snapshot),
                "Cayenne adaptive tuning: SLO appears infeasible on this hardware — no further tuning adjustment is available (actuator bounds or resource gating) and the goal is still violated. See `constraint` for the binding resource; relax the goal or scale it."
            );
        }
    }

    /// Whether the goal-driven controller has declared the configured SLO infeasible
    /// on this hardware (no eligible adjustment available — actuator bounds or resource
    /// gating — and the goal still violated for
    /// ~[`tuning::GOAL_INFEASIBLE_STUCK_TICKS`] eligible ticks). Surfaced as a
    /// telemetry gauge so silent underperformance becomes visible.
    #[must_use]
    pub(crate) fn goal_slo_infeasible(&self) -> bool {
        self.goal_stuck_ticks
            .load(std::sync::atomic::Ordering::Relaxed)
            >= tuning::GOAL_INFEASIBLE_STUCK_TICKS
    }

    /// Create a `VortexFormat` from configuration.
    ///
    /// The format carries Vortex scan/write options, including the shared
    /// segment-cache capacity for scans created from this context.
    fn create_vortex_format(config: &VortexConfig, dataset: &str) -> Arc<VortexFormat> {
        // Create a Vortex session with default encodings. The session's write
        // strategy is the table's FULL encoding tier: the BtrBlocks cascade by
        // default, optionally extended with the Zstd string scheme when
        // `cayenne_compression_strategy=zstd` (this wiring is what makes that
        // param real — see `delta_encoding::full_strategy_builder_for`).
        // Maintenance writes and full-level delta writes inherit it; light
        // delta-encoding levels override it per write via
        // `write_format_with_strategy` (see `provider::delta_encoding`).
        let mut vortex_session = VortexSession::default();
        if let Some(full_strategy) =
            super::delta_encoding::full_strategy_builder_for(&config.compression_strategy)
        {
            vortex_session = vortex_session.set(full_strategy);
        }

        Arc::new(
            VortexFormat::new_with_options(vortex_session, Self::vortex_table_options(config))
                .with_dataset_label_and_retirement_tracking(dataset),
        )
    }

    /// Table options shared by the base format and any per-write format
    /// variants (delta-encoding strategy overrides) so write-only formats
    /// behave identically apart from the encoding strategy.
    fn vortex_table_options(config: &VortexConfig) -> VortexTableOptions {
        let segment_cache_size_bytes =
            config
                .segment_cache_mb
                .checked_mul(1024 * 1024)
                .or_else(|| {
                    tracing::warn!(
                        segment_cache_mb = config.segment_cache_mb,
                        "Vortex config `segment_cache_mb` is too large; disabling segment cache"
                    );
                    None
                });

        VortexTableOptions {
            target_file_size_mb: config.target_vortex_file_size_mb,
            projection_pushdown: ProjectionPushdown::On,
            segment_cache_size_bytes,
            ..VortexTableOptions::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cayenne_enables_vortex_projection_pushdown_by_default() {
        let runtime_env = Arc::new(RuntimeEnv::default());
        let context = CayenneContext::new(&VortexConfig::default(), runtime_env, "test");

        assert_eq!(
            context.file_format().options().projection_pushdown,
            ProjectionPushdown::On
        );
    }

    /// Partition child contexts are marked as coupled writers (their writes
    /// share one routing demux and must bypass the global encode budget —
    /// spiceai/spiceai#11818); ordinary contexts are not.
    #[test]
    fn partition_child_context_is_coupled_writer() {
        let runtime_env = Arc::new(RuntimeEnv::default());
        let ordinary =
            CayenneContext::new(&VortexConfig::default(), Arc::clone(&runtime_env), "test");
        assert!(!ordinary.is_coupled_writer());

        let child =
            CayenneContext::new_for_partition_child(&VortexConfig::default(), runtime_env, "test");
        assert!(child.is_coupled_writer());
    }

    /// Regression: the cold (datalake) promotion write must roll files at
    /// `cayenne_datalake_target_file_size_mb`, not the warm `target_vortex_file_size_mb`.
    ///
    /// Every sorted / PK-upsert table earns a single write shard, so the warm
    /// `write_shard_format` path returns the base format unchanged — whose
    /// `target_file_size_mb` is the warm size. Cold promotion therefore silently
    /// rolled files at the warm size, leaving `cayenne_datalake_target_file_size_mb`
    /// inert. `cold_write_format` must build a format carrying the cold size.
    #[test]
    fn cold_write_format_rolls_files_at_cold_target_size() {
        let runtime_env = Arc::new(RuntimeEnv::default());
        let config = VortexConfig {
            target_vortex_file_size_mb: 256,
            cold_target_file_size_mb: 1024,
            ..VortexConfig::default()
        };
        let context = CayenneContext::new(&config, runtime_env, "test");

        // Baseline: the warm/base format rolls at the warm target size.
        assert_eq!(
            context.file_format().options().target_file_size_mb,
            256,
            "base (warm) format must use target_vortex_file_size_mb"
        );

        // Datalake: the cold format rolls at the cold target size, independent of the warm size;
        // previously this file size was silently 256 (the warm size).
        let cold = context.cold_write_format(config.cold_target_file_size_mb, None);
        assert_eq!(
            cold.options().target_file_size_mb,
            1024,
            "cold format must use cayenne_datalake_target_file_size_mb, not the warm size"
        );
    }

    #[test]
    fn bake_back_pressure_gate_defers_only_when_apply_is_behind() {
        use std::time::Duration;
        let runtime_env = Arc::new(RuntimeEnv::default());
        let context = CayenneContext::new(&VortexConfig::default(), runtime_env, "test");

        // Warm up well past WARMUP_BATCHES with the apply far AHEAD of the
        // offered load (1ms apply per 20ms interval ⇒ apply_vs_arrival ≈ 0.05).
        for _ in 0..(tuning::WARMUP_BATCHES + 16) {
            context.ingest_stats.record_write(tuning::WriteSample {
                rows: 100,
                bytes: 10_000,
                apply: Duration::from_millis(1),
                arrival_gap: Some(Duration::from_millis(20)),
                delete_rows: 0,
            });
        }
        assert!(
            !context.bake_should_defer_for_apply(),
            "healthy apply (headroom) must allow the bake"
        );

        // Drive the apply well behind (200ms apply per 20ms interval), with
        // enough samples to dominate the EWMA ⇒ apply_vs_arrival ≫ 1.
        for _ in 0..(tuning::WARMUP_BATCHES * 3) {
            context.ingest_stats.record_write(tuning::WriteSample {
                rows: 100,
                bytes: 10_000,
                apply: Duration::from_millis(200),
                arrival_gap: Some(Duration::from_millis(20)),
                delete_rows: 0,
            });
        }
        assert!(
            context.bake_should_defer_for_apply(),
            "apply at/over capacity must defer the bake"
        );

        // No fresh ingest since the previous check: a stale "behind" EWMA must
        // NOT keep deferring forever — an idle table still gets to bake.
        assert!(
            !context.bake_should_defer_for_apply(),
            "stale signal (no fresh ingest) must allow the bake"
        );
    }
}
