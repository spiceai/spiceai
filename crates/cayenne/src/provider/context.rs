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
use crate::metadata::{DeletionMode, DeltaEncoding, PkConflictDetection, VortexConfig};

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
    /// Wall-clock of the previous recorded CDC write, used to derive the
    /// inter-batch arrival interval (the offered-load signal). `None` until the
    /// first write.
    last_write: parking_lot::Mutex<Option<std::time::Instant>>,
}

/// Default byte budget for the in-memory PK keyset cache when
/// `cayenne_pk_keyset_cache_mb` is unset. 256 MiB preserves historical behavior;
/// raise the param on memory-rich hosts so high-cardinality tables keep their
/// keyset resident instead of rebuilding it from a full-table scan every CDC
/// batch.
pub(crate) const DEFAULT_PK_KEYSET_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Hard ceiling on the configurable PK keyset cache budget. The budget doubles as
/// the bloom allocation size (`PkBloom::with_byte_budget`), so an out-of-range or
/// typo'd `cayenne_pk_keyset_cache_mb` must not be able to request a
/// near-`usize::MAX` allocation. Matches the auto-default's 8 GiB ceiling.
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
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
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
        let live_actuators = Arc::new(LiveActuators::new(ActuatorValues {
            inline_flush_max_bytes: config.inline_flush_max_bytes,
            inline_flush_max_rows: config.inline_flush_max_rows,
            inline_flush_max_segments: config.inline_flush_max_segments,
            compaction_background_interval_ms: config.compaction_background_interval_ms,
            compaction_trigger_files: config.compaction_trigger_files,
            write_concurrency: wc_init,
            mem_tier_max_bytes: config.cdc_mem_tier_max_bytes,
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
            compaction_background_interval_ms: if pins.compaction_interval {
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
        };
        Arc::new(Self {
            vortex_format,
            config: config.clone(),
            dataset: dataset.to_string(),
            session_config: SessionConfig::default(),
            upload_semaphore: Arc::new(Semaphore::new(config.upload_concurrency.max(1))),
            runtime_env,
            live_actuators,
            ingest_stats: Arc::new(IngestStats::new()),
            tuning_bounds,
            dynamic_tuning: config.dynamic_tuning,
            last_adjust: parking_lot::Mutex::new(None),
            last_adjust_samples: std::sync::atomic::AtomicU64::new(0),
            last_write: parking_lot::Mutex::new(None),
        })
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
        self.config.target_vortex_file_size_mb * 1024 * 1024
    }

    /// Get the sort columns if configured.
    #[must_use]
    pub fn sort_columns(&self) -> &[String] {
        &self.config.sort_columns
    }

    /// Check if sorting is enabled.
    #[must_use]
    pub fn has_sort_columns(&self) -> bool {
        !self.config.sort_columns.is_empty()
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

    /// Protected snapshot count that should trigger maintenance compaction.
    #[must_use]
    pub(crate) fn compaction_trigger_protected_snapshots(&self) -> usize {
        self.config.compaction_trigger_protected_snapshots.max(1)
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

    /// Get the shared semaphore for limiting concurrent file writes / uploads.
    #[must_use]
    pub fn upload_semaphore(&self) -> &Arc<Semaphore> {
        &self.upload_semaphore
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
    }

    /// A snapshot of the current ingest accounting (rate + response), for
    /// observability/logging.
    #[must_use]
    pub(crate) fn ingest_snapshot(&self) -> tuning::IngestSnapshot {
        self.ingest_stats.snapshot()
    }

    /// Current live actuator values (after any dynamic adjustments), for metrics.
    #[must_use]
    pub(crate) fn live_actuator_values(&self) -> tuning::ActuatorValues {
        self.live_actuators.values()
    }

    /// Refresh the externally-observed environment/response signals (read amp +
    /// cgroup memory pressure) so the next snapshot/control step sees fresh data.
    /// Called on the background tick regardless of whether tuning is enabled, so
    /// the accounting gauges stay live for observability.
    pub(crate) fn observe_environment(&self, read_amp: usize) {
        self.ingest_stats.set_read_amp(read_amp);
        tuning::sample_mem_pressure(&self.ingest_stats);
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
        // Detect the environment (cgroup-aware memory usage) and fold it in, so
        // the loop closes on memory as well as ingest/query behavior.
        tuning::sample_mem_pressure(&self.ingest_stats);
        let now = std::time::Instant::now();
        let since_last = (*self.last_adjust.lock()).map_or(std::time::Duration::MAX, |t| {
            now.saturating_duration_since(t)
        });
        let snapshot = self.ingest_stats.snapshot();
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
        let adj = tuning::decide(
            &snapshot,
            &self.live_actuators.values(),
            &self.tuning_bounds,
            since_last,
            min_dwell,
            samples_at_last_move,
        )?;
        self.live_actuators.apply(&adj);
        *self.last_adjust.lock() = Some(now);
        self.last_adjust_samples
            .store(snapshot.samples, Ordering::Relaxed);
        Some(adj)
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
                .with_dataset_label(dataset),
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
}
