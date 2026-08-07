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

//! How many CPUs `spiced` is entitled to, and every sizing decision derived from
//! that.
//!
//! `spiced` derived that count in fourteen separate places, each calling
//! `std::thread::available_parallelism` or `num_cpus::get` directly, with no way
//! to override it and no record of where the number came from. One of those
//! sites fed a control loop: the Cayenne tuner divided its CPU busy-fraction by
//! the detected core count, so a runtime sized for more cores than it can use
//! reports itself idle while saturated.
//!
//! Detection itself is unchanged — with nothing configured this resolves to the
//! same value `available_parallelism` returned (a cgroup quota, capped by
//! `sched_getaffinity`). What is new is that the value has one owner, a named
//! source, and an explicit override for deployments the host cannot describe.
//!
//! Sizing deliberately follows only a cgroup CPU *quota*, never a CPU *share*.
//! Under Kubernetes the kubelet derives the share from `requests.cpu`, but a
//! request is a scheduling floor, not a ceiling: a burstable pod is entitled to
//! every idle core on its node, and sizing from the request would take that
//! away. An operator who wants the runtime sized to the request says so
//! explicitly with `runtime.cpu.cores`. The share is still *read*, so the
//! startup log and the metrics can show the request and the limit next to the
//! budget actually chosen — that comparison is what makes a mis-sized pod
//! diagnosable.
//!
//! This crate owns both halves of the fix: [`HostReadings`] +
//! [`CpuBudget::resolve`] decide the entitlement, and the methods on
//! [`CpuBudget`] own every quantity derived from it. No caller does its own
//! arithmetic, so `rg 'cpu_budget()'` is a complete inventory of the runtime's
//! CPU sizing, and a future change to a sizing policy is a method body rather
//! than a sweep across a dozen crates.
//!
//! ```no_run
//! use cpu_budget::{CpuBudget, CpuConfig, HostReadings, cpu_budget};
//!
//! // Once, from `main`, before anything is sized.
//! let budget = CpuBudget::resolve(
//!     &CpuConfig::from_sources(None, None, Some("4")),
//!     &HostReadings::detect(),
//! )?;
//! budget.log_summary();
//! budget.install()?;
//!
//! // Everywhere else.
//! let workers = cpu_budget().dedicated_runtime_worker_threads();
//! # Ok::<(), cpu_budget::Error>(())
//! ```

pub mod cgroup;

use std::sync::OnceLock;

use snafu::{OptionExt, Snafu};

pub const DOCS_URL: &str = "https://spiceai.org/docs/reference/spicepod#runtimecpu";

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display(
        "Failed to configure the runtime CPU budget ({setting}): Invalid value '{value}'. Expected a positive CPU quantity such as `4`, `3.5`, or `3500m`, or `auto` to detect it. See: {DOCS_URL}"
    ))]
    InvalidCpuQuantity { setting: String, value: String },

    #[snafu(display(
        "Failed to configure the runtime CPU budget: a budget of {installed_cores} cores was already in effect, so the configured {requested_cores} cores cannot be applied. Restart spiced to apply the new value. See: {DOCS_URL}"
    ))]
    AlreadyInstalled {
        requested_cores: usize,
        installed_cores: usize,
    },
}

/// Where the effective CPU entitlement came from. Recorded so the startup log
/// and the `spiced_cpu_budget_cores` gauge can name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuSource {
    /// `--cpu-cores`, `SPICE_CPU_CORES`, or `runtime.cpu.cores`.
    Configured,
    /// A cgroup CPU quota (v2 `cpu.max`, v1 `cpu.cfs_quota_us`).
    CgroupQuota,
    /// `sched_getaffinity` — the cores the process may run on.
    Affinity,
    /// Nothing could be determined; one core.
    Fallback,
}

impl CpuSource {
    /// Stable identifier, used as the `source` metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configured => "configured",
            Self::CgroupQuota => "cgroup_quota",
            Self::Affinity => "affinity",
            Self::Fallback => "fallback",
        }
    }
}

/// The explicitly configured entitlement, if any, and which surface set it.
///
/// Precedence is `--cpu-cores` > `SPICE_CPU_CORES` > `runtime.cpu.cores` >
/// detection. A surface set to `auto` still wins over the surfaces below it; it
/// simply resolves to detection.
#[derive(Debug, Clone, Default)]
pub struct CpuConfig {
    cores: Option<(String, &'static str)>,
}

impl CpuConfig {
    pub const CLI_SETTING: &str = "--cpu-cores";
    pub const ENV_SETTING: &str = "SPICE_CPU_CORES";
    pub const SPICEPOD_SETTING: &str = "runtime.cpu.cores";

    /// Resolve the three configuration surfaces in precedence order.
    #[must_use]
    pub fn from_sources(cli: Option<&str>, env: Option<&str>, spicepod: Option<&str>) -> Self {
        let cores = cli
            .map(|v| (v.to_string(), Self::CLI_SETTING))
            .or_else(|| env.map(|v| (v.to_string(), Self::ENV_SETTING)))
            .or_else(|| spicepod.map(|v| (v.to_string(), Self::SPICEPOD_SETTING)));
        Self { cores }
    }

    /// Read `SPICE_CPU_CORES` from the process environment.
    #[must_use]
    pub fn env_cores() -> Option<String> {
        std::env::var(Self::ENV_SETTING)
            .ok()
            .filter(|v| !v.trim().is_empty())
    }
}

/// What the host reports about this process's CPU entitlement.
///
/// Split out from [`CpuBudget::resolve`] so the whole detection ladder is a pure
/// function of injected readings and can be exercised without a live cgroup.
#[derive(Debug, Clone, Default)]
pub struct HostReadings {
    /// `sched_getaffinity` / `available_parallelism`.
    pub affinity_cores: usize,
    /// Millicores implied by a cgroup CPU quota (`limits.cpu`). `None` when no
    /// quota is set, which includes every burstable pod.
    pub quota_millicores: Option<u64>,
    /// Millicores implied by the cgroup CPU share (`requests.cpu`). Reported
    /// only — never an input to [`CpuBudget::resolve`]'s ladder, because a
    /// request is a scheduling floor rather than a ceiling. `None` when no
    /// request is expressed.
    pub request_millicores: Option<u64>,
}

impl HostReadings {
    /// Read the host. Every field degrades to "unknown" rather than failing.
    #[must_use]
    #[expect(
        clippy::disallowed_methods,
        reason = "the one sanctioned detection site: `clippy.toml` forbids this everywhere else so a call site cannot size itself from the node's cores"
    )]
    pub fn detect() -> Self {
        Self {
            affinity_cores: std::thread::available_parallelism()
                .map_or(1, std::num::NonZeroUsize::get),
            quota_millicores: detect_quota_millicores(),
            request_millicores: detect_request_millicores(),
        }
    }
}

/// The cgroup CPU quota, in millicores.
///
/// Read along the whole cgroup path rather than at the leaf — see
/// [`cgroup::min_along_cgroup_path`] for why a quota on an ancestor is just as
/// binding.
fn detect_quota_millicores() -> Option<u64> {
    if let Some((mountpoint, cgroup_path)) = cgroup::v2_mount_and_path()
        && let Some(millicores) =
            cgroup::min_along_cgroup_path(&mountpoint, &cgroup_path, |mount, rel| {
                let path = cgroup::cgroup_file_path(mount, rel, "cpu.max");
                cgroup::parse_cpu_max(&std::fs::read_to_string(path).ok()?)
            })
    {
        return Some(millicores);
    }

    let (mountpoint, cgroup_path) = cgroup::v1_mount_and_path("cpu")?;
    cgroup::min_along_cgroup_path(&mountpoint, &cgroup_path, |mount, rel| {
        let quota =
            std::fs::read_to_string(cgroup::cgroup_file_path(mount, rel, "cpu.cfs_quota_us"))
                .ok()?;
        let period =
            std::fs::read_to_string(cgroup::cgroup_file_path(mount, rel, "cpu.cfs_period_us"))
                .ok()?;
        cgroup::parse_cfs_quota(&quota, &period)
    })
}

/// The `requests.cpu` behind this process's cgroup CPU share, in millicores.
///
/// Reporting only. Sizing never consults it — see [`CpuBudget::resolve`].
///
/// Read at the leaf, unlike the quota. A share is a relative weight *among
/// siblings at one level*, not a ceiling inherited down the tree, so the
/// smallest value along the path would mean nothing; the container's own share
/// is the one the kubelet derived from its `requests.cpu`.
fn detect_request_millicores() -> Option<u64> {
    if let Some((mountpoint, cgroup_path)) = cgroup::v2_mount_and_path()
        && let Ok(contents) = std::fs::read_to_string(cgroup::cgroup_file_path(
            &mountpoint,
            &cgroup_path,
            "cpu.weight",
        ))
        && let Some(millicores) = cgroup::parse_cpu_weight(&contents)
    {
        return Some(millicores);
    }
    let (mountpoint, cgroup_path) = cgroup::v1_mount_and_path("cpu")?;
    let shares = std::fs::read_to_string(cgroup::cgroup_file_path(
        &mountpoint,
        &cgroup_path,
        "cpu.shares",
    ))
    .ok()?;
    cgroup::parse_cpu_shares(&shares)
}

/// Query plans admitted concurrently per core when
/// `runtime.query.max_concurrent_queries` is unset. Above one per core so a
/// query blocked on I/O does not idle its core, low enough that the memory pool
/// is shared between a countable number of plans.
const QUERIES_PER_CORE: usize = 4;

/// A CPU request at or above this fraction of the effective core count is close
/// enough not to warn about. A half is loose enough to absorb cgroup
/// quantization (a `requests.cpu: 1` lands at 974m under cgroup v2), and tight
/// enough that a warning means the runtime sized itself for at least twice the
/// CPU the scheduler guarantees.
const REQUEST_SHORTFALL_NUM: u64 = 1;
/// Denominator of [`REQUEST_SHORTFALL_NUM`].
const REQUEST_SHORTFALL_DEN: u64 = 2;

/// The process-wide CPU entitlement and every sizing decision derived from it.
#[derive(Debug, Clone)]
pub struct CpuBudget {
    millicores: u64,
    cores: usize,
    source: CpuSource,
    /// The configuration surface that set the value, when `source` is
    /// [`CpuSource::Configured`].
    setting: Option<&'static str>,
    /// What `sched_getaffinity` saw, for the startup log.
    detected_cores: usize,
    /// The cgroup CPU limit, reported alongside the budget it produced.
    limit_millicores: Option<u64>,
    /// The cgroup CPU request. Reported, never an input to the ladder.
    request_millicores: Option<u64>,
}

impl CpuBudget {
    /// Resolve the effective entitlement.
    ///
    /// The ladder, first hit wins:
    ///
    /// 1. an explicit `cores` value -> [`CpuSource::Configured`]
    /// 2. a cgroup CPU quota (`limits.cpu`) -> [`CpuSource::CgroupQuota`]
    /// 3. `sched_getaffinity` -> [`CpuSource::Affinity`]
    /// 4. one core -> [`CpuSource::Fallback`]
    ///
    /// With nothing configured this resolves to exactly what
    /// `available_parallelism` already returned — quota capped by affinity — so
    /// a deployment that sets no CPU configuration is sized identically before
    /// and after this crate existed. What changes is only that the value now has
    /// one owner, a named source, and an override.
    ///
    /// A cgroup CPU *share* is deliberately not consulted. The kubelet derives
    /// it from `requests.cpu`, which is a scheduling floor rather than a
    /// ceiling: a pod with a request and no limit is entitled to burst across
    /// every idle core on its node, and inferring a limit from the request would
    /// silently remove that. Operators who want the runtime sized to the request
    /// set `runtime.cpu.cores` (or `SPICE_CPU_CORES`) explicitly.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidCpuQuantity`] when the configured value is not a positive
    /// CPU quantity or `auto`.
    pub fn resolve(cfg: &CpuConfig, host: &HostReadings) -> Result<Self, Error> {
        let detected_cores = host.affinity_cores.max(1);
        let affinity_millicores = u64::try_from(detected_cores)
            .unwrap_or(u64::MAX)
            .saturating_mul(1000);

        let configured = match &cfg.cores {
            Some((value, setting)) => parse_cpu_quantity(value)
                .context(InvalidCpuQuantitySnafu {
                    setting: (*setting).to_string(),
                    value: value.clone(),
                })?
                .map(|millicores| (millicores, *setting)),
            None => None,
        };

        let (millicores, source, setting) = if let Some((millicores, setting)) = configured {
            (millicores, CpuSource::Configured, Some(setting))
        } else if let Some(quota) = host.quota_millicores {
            // Capped by affinity: a quota wider than the cores the process may
            // run on cannot be used, and `available_parallelism` already takes
            // the minimum of the two. Without the cap a `--cpus=100` container
            // on a 16-core host would size itself for 100 cores.
            (
                quota.max(1).min(affinity_millicores),
                CpuSource::CgroupQuota,
                None,
            )
        } else if host.affinity_cores > 0 {
            (affinity_millicores, CpuSource::Affinity, None)
        } else {
            (1000, CpuSource::Fallback, None)
        };

        Ok(Self {
            millicores,
            cores: usize::try_from(millicores.div_ceil(1000))
                .unwrap_or(usize::MAX)
                .max(1),
            source,
            setting,
            detected_cores,
            limit_millicores: host.quota_millicores,
            request_millicores: host.request_millicores,
        })
    }

    /// Install as the process-wide budget, returned by [`cpu_budget`].
    ///
    /// Must run before anything reads [`cpu_budget`]: that accessor lazily
    /// detects into the same cell, so a read beforehand pins the detected value
    /// and makes this a no-op.
    ///
    /// # Errors
    ///
    /// [`Error::AlreadyInstalled`] if a budget is already in effect.
    pub fn install(self) -> Result<(), Error> {
        let requested_cores = self.cores;
        if let Err(rejected) = INSTALLED.set(self) {
            let installed_cores = INSTALLED.get().map_or(rejected.cores, |b| b.cores);
            return AlreadyInstalledSnafu {
                requested_cores,
                installed_cores,
            }
            .fail();
        }
        Ok(())
    }

    /// Log the effective budget and the defaults it implies. Call once, before
    /// anything is sized.
    ///
    /// The derived values are logged as their own line because the headline
    /// summary names only the three an operator recognizes. Every consumer's
    /// number is here, so a mis-sized pool can be diagnosed from a startup log
    /// alone rather than by correlating `/metrics` gauges — and so that a future
    /// change to a sizing policy is visible in the log diff.
    ///
    /// These are *defaults*, not necessarily the values in force. Several are
    /// overridable by their own setting (`runtime.query.target_partitions`,
    /// `runtime.query.max_concurrent_queries`, `DuckDB`'s `threads`, a model's
    /// parallelism), and this runs at startup, before that configuration is
    /// resolved — some of it per dataset. A consumer that can be overridden logs
    /// the value it actually used, and where it came from, where it applies it.
    pub fn log_summary(&self) {
        tracing::info!("{}", self.summary_line());
        tracing::info!("{}", self.derived_sizing_line());
        if let Some(warning) = self.request_shortfall_warning() {
            tracing::warn!("{warning}");
        }
    }

    /// Every quantity this budget derives, paired with the consumer it sizes.
    ///
    /// The list is the same inventory the methods are: adding a sizing method
    /// without adding it here means shipping a pool nothing reports on.
    #[must_use]
    pub fn derived_sizing(&self) -> Vec<(&'static str, usize)> {
        vec![
            (
                "main_runtime_worker_threads",
                self.main_runtime_worker_threads(),
            ),
            (
                "dedicated_runtime_worker_threads",
                self.dedicated_runtime_worker_threads(),
            ),
            ("target_partitions", self.target_partitions()),
            ("max_concurrent_queries", self.max_concurrent_queries()),
            ("cayenne_encode_permits", self.cayenne_encode_permits()),
            (
                "cayenne_write_concurrency_ceiling",
                self.cayenne_write_concurrency_ceiling(),
            ),
            (
                "cayenne_compaction_permits",
                self.cayenne_compaction_permits(),
            ),
            (
                "cayenne_upload_concurrency",
                self.cayenne_upload_concurrency(),
            ),
            (
                "cayenne_max_concurrent_file_scans",
                self.cayenne_max_concurrent_file_scans(),
            ),
            (
                "metastore_pool_connections",
                self.metastore_pool_connections(),
            ),
            ("embedding_pool_threads", self.embedding_pool_threads()),
            ("duckdb_threads", self.duckdb_threads()),
            (
                "cluster_executor_concurrent_tasks",
                self.cluster_executor_concurrent_tasks(),
            ),
        ]
    }

    /// [`Self::derived_sizing`] rendered as one `key=value` line.
    #[must_use]
    pub fn derived_sizing_line(&self) -> String {
        let values = self
            .derived_sizing()
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("CPU budget derived sizing: {values}")
    }

    /// Where the effective value came from, phrased for an operator.
    const fn origin(&self) -> &'static str {
        match self.source {
            CpuSource::Configured => match self.setting {
                Some(setting) => setting,
                None => CpuConfig::SPICEPOD_SETTING,
            },
            CpuSource::CgroupQuota => "cgroup CPU limit",
            // `available_parallelism`: the CPU affinity mask on Linux, the logical
            // CPU count elsewhere. Not the host's total either way — a cpuset or
            // `taskset` pins the process to a subset — so this names what the
            // process may use rather than what the machine has.
            CpuSource::Affinity => "the CPUs available to this process",
            CpuSource::Fallback => "fallback",
        }
    }

    /// The one-line startup summary: the effective value, where it came from,
    /// the cgroup request and limit it sits between, and what it sizes.
    #[must_use]
    pub fn summary_line(&self) -> String {
        let unset = "unset".to_string();
        format!(
            "CPU budget: {entitlement} (source: {origin}; host reports {detected}, \
             cgroup request {request}, cgroup limit {limit}) \u{2192} \
             {main} main worker threads, {dedicated} per dedicated runtime pool, \
             {partitions} target partitions",
            entitlement = format_millicores(self.millicores),
            origin = self.origin(),
            detected = self.detected_cores,
            request = self
                .request_millicores
                .map_or_else(|| unset.clone(), format_millicores),
            limit = self.limit_millicores.map_or(unset, format_millicores),
            main = self.main_runtime_worker_threads(),
            dedicated = self.dedicated_runtime_worker_threads(),
            partitions = self.target_partitions(),
        )
    }

    /// A warning when the CPU request sits well below the core count the runtime
    /// sized itself for, i.e. when sizing leans on CPU the scheduler does not
    /// guarantee.
    ///
    /// States the discrepancy and where the effective value came from, and stops
    /// there: which of the two numbers is wrong is the operator's call, and both
    /// answers (lower the setting, or raise the request) are legitimate.
    ///
    /// Fires for every source, and names the one responsible: a value read from
    /// an explicit `limits.cpu`, a value configured by hand, and — the case that
    /// motivates this crate — a request with no limit at all, where detection
    /// falls back to every CPU the process may use. An over-large configured value is just as
    /// wrong as an over-large inferred one, so the check is on the effective core
    /// count rather than on any particular rung of the ladder.
    ///
    /// `None` when there is no request to compare against, or when the request is
    /// at or above [`REQUEST_SHORTFALL_NUM`]/[`REQUEST_SHORTFALL_DEN`] of the
    /// effective core count.
    #[must_use]
    pub fn request_shortfall_warning(&self) -> Option<String> {
        let request = self.request_millicores?;
        if request.saturating_mul(REQUEST_SHORTFALL_DEN)
            >= self.millicores.saturating_mul(REQUEST_SHORTFALL_NUM)
        {
            return None;
        }
        // Detection is reached only when no limit was found, and that is the fact
        // behind this warning: a request with nothing capping it sizes for every CPU
        // the process may use. "detected" rather than "set" because a quota that
        // exists but cannot be read degrades to the same `None` as one that was
        // never set (see `detect_quota_millicores`), and the warning must not claim
        // the pod has no limit when it may only be unreadable. `summary_line`
        // carries the same fact, so the qualifier belongs here, not in `origin`.
        let unlimited = if matches!(self.source, CpuSource::Affinity) {
            ", no CPU limit detected"
        } else {
            ""
        };
        Some(format!(
            "Detected a cgroup CPU request of {request}, which is below the {effective} in effect \
             (from {origin}{unlimited})",
            request = format_millicores(request),
            effective = format_millicores(self.millicores),
            origin = self.origin(),
        ))
    }

    // ---- the entitlement ----

    /// The entitlement in whole cores, rounded up, at least 1.
    #[must_use]
    pub const fn cores(&self) -> usize {
        self.cores
    }

    /// The cgroup CPU limit (`limits.cpu`) in millicores, when one is set.
    #[must_use]
    pub const fn limit_millicores(&self) -> Option<u64> {
        self.limit_millicores
    }

    /// The cgroup CPU request (`requests.cpu`) in millicores, when one is
    /// expressed.
    ///
    /// Reported so an operator can see the request next to the budget; it is
    /// never an input to the detection ladder.
    #[must_use]
    pub const fn request_millicores(&self) -> Option<u64> {
        self.request_millicores
    }

    /// The entitlement in millicores, exact.
    #[must_use]
    pub const fn millicores(&self) -> u64 {
        self.millicores
    }

    #[must_use]
    pub const fn source(&self) -> CpuSource {
        self.source
    }

    /// What `sched_getaffinity` reported, regardless of the effective budget.
    #[must_use]
    pub const fn detected_cores(&self) -> usize {
        self.detected_cores
    }

    // ---- tokio runtimes ----

    /// Worker threads for the primary runtime (HTTP, control plane, queries).
    #[must_use]
    pub const fn main_runtime_worker_threads(&self) -> usize {
        self.cores
    }

    /// Worker threads for each dedicated runtime (`cpu`, `refresh`,
    /// `cdc_apply`, `compaction`). One core is reserved for the primary runtime.
    #[must_use]
    pub const fn dedicated_runtime_worker_threads(&self) -> usize {
        let reserved = self.cores.saturating_sub(1);
        if reserved == 0 { 1 } else { reserved }
    }

    // ---- query engine ----

    /// `DataFusion`'s local query fan-out when `runtime.query.target_partitions`
    /// is unset.
    #[must_use]
    pub const fn target_partitions(&self) -> usize {
        self.cores
    }

    /// Concurrently-executing query plans admitted when
    /// `runtime.query.max_concurrent_queries` is unset.
    ///
    /// Queues arrivals beyond a depth the runtime can still service instead of
    /// admitting every one of them: each admitted plan fans out into
    /// [`Self::target_partitions`] operator reservations against a fixed memory
    /// pool, so unbounded concurrency lets queries starve each other and, past a
    /// point, take the process down. A small multiple of the core count keeps
    /// enough in flight to hide I/O stalls without that.
    #[must_use]
    pub const fn max_concurrent_queries(&self) -> usize {
        self.cores.saturating_mul(QUERIES_PER_CORE)
    }

    // ---- Cayenne ----

    /// Aggregate Vortex encode shards across all Cayenne tables.
    ///
    /// The core count minus a query reserve (a quarter of the cores, at least
    /// 2): encode shards share the query runtime, and an HTAP burst that takes
    /// every core measurably starves concurrent scans.
    #[must_use]
    pub const fn cayenne_encode_permits(&self) -> usize {
        let reserve = if self.cores / 4 > 2 {
            self.cores / 4
        } else {
            2
        };
        let permits = self.cores.saturating_sub(reserve);
        if permits == 0 { 1 } else { permits }
    }

    /// Ceiling on a single Cayenne table's write concurrency, and the upper
    /// bound of the adaptive controller's concurrency actuator. A statement
    /// about the hardware, not about our thread count.
    #[must_use]
    pub const fn cayenne_write_concurrency_ceiling(&self) -> usize {
        self.cores
    }

    /// Permits on the per-accelerator background-compaction semaphore, so a
    /// fleet of Cayenne tables cannot oversubscribe the writer pool.
    #[must_use]
    pub const fn cayenne_compaction_permits(&self) -> usize {
        self.cores
    }

    /// Default multipart-upload fan-out for a Cayenne table.
    #[must_use]
    pub const fn cayenne_upload_concurrency(&self) -> usize {
        self.cores
    }

    /// Concurrent file scans a key-based deletion sink may have in flight.
    #[must_use]
    pub const fn cayenne_max_concurrent_file_scans(&self) -> usize {
        self.cores
    }

    /// Connections in the Cayenne `SQLite` metastore pool. Capped at 32 because
    /// each connection carries its own mmap and page cache; floored at 2 so a
    /// single-core host still has a slot for read-while-write.
    #[must_use]
    pub const fn metastore_pool_connections(&self) -> usize {
        let k = if self.cores > 32 { 32 } else { self.cores };
        if k < 2 { 2 } else { k }
    }

    /// The process's CPU busy fraction over a sampling window, on `0.0..=1.0`.
    ///
    /// Divides by the *entitlement*, which is the whole point: on a 4-core
    /// entitlement misread as 18 cores, a fully saturated process reports ~0.22
    /// and the Cayenne tuner concludes CPU is idle. Clamped, because an operator
    /// who configures `cores` below the true host count can legitimately consume
    /// more CPU than the budget names.
    #[must_use]
    pub fn cpu_busy_fraction(&self, busy_secs: f64, wall_secs: f64) -> f64 {
        #[expect(
            clippy::cast_precision_loss,
            reason = "millicores is a small integer; f64 is exact well past any real entitlement"
        )]
        let entitlement = self.millicores as f64 / 1000.0;
        if wall_secs <= 0.0 || entitlement <= 0.0 || !busy_secs.is_finite() {
            return 0.0;
        }
        (busy_secs / (wall_secs * entitlement)).clamp(0.0, 1.0)
    }

    // ---- other pools ----

    /// Threads in the rayon pool that runs embedding inference.
    #[must_use]
    pub const fn embedding_pool_threads(&self) -> usize {
        self.cores
    }

    /// `DuckDB`'s per-instance `threads` setting. `DuckDB` otherwise sizes its
    /// own pool from the host core count, the same over-commitment this crate
    /// exists to prevent.
    #[must_use]
    pub const fn duckdb_threads(&self) -> usize {
        self.cores
    }

    /// Tasks a cluster executor advertises it can run concurrently.
    #[must_use]
    pub const fn cluster_executor_concurrent_tasks(&self) -> usize {
        self.cores
    }
}

static INSTALLED: OnceLock<CpuBudget> = OnceLock::new();

/// The installed budget, or a lazily-detected one.
///
/// Embedders and unit tests that never call [`CpuBudget::install`] keep the
/// pre-existing detect-from-the-host behaviour.
#[must_use]
pub fn cpu_budget() -> &'static CpuBudget {
    INSTALLED.get_or_init(|| {
        let host = HostReadings::detect();
        // `CpuConfig::default()` carries no configured value, so the only
        // fallible branch of `resolve` is unreachable. Spell the one-core budget
        // out anyway rather than panicking on a path this crate exists to keep
        // infallible.
        CpuBudget::resolve(&CpuConfig::default(), &host).unwrap_or(CpuBudget {
            millicores: 1000,
            cores: 1,
            source: CpuSource::Fallback,
            setting: None,
            detected_cores: 1,
            limit_millicores: None,
            request_millicores: None,
        })
    })
}

/// Parse a Kubernetes-style CPU quantity into millicores.
///
/// Accepts `4`, `3.5`, `3500m`, and `auto`. The outer `Option` is validity —
/// `None` means the value is not a CPU quantity, which
/// [`CpuBudget::resolve`] turns into [`Error::InvalidCpuQuantity`]. The inner
/// `Option` distinguishes an explicit entitlement from `auto` ("detect it").
/// Zero and negative values are invalid, not silently clamped.
#[must_use]
pub fn parse_cpu_quantity(value: &str) -> Option<Option<u64>> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("auto") {
        return Some(None);
    }
    let millicores = if let Some(millis) = value.strip_suffix('m') {
        // Kubernetes millicores are integral; `3.5m` is not a valid quantity.
        millis.trim().parse::<u64>().ok()?
    } else {
        let cores: f64 = value.parse().ok()?;
        if !cores.is_finite() || cores <= 0.0 {
            return None;
        }
        let millis = (cores * 1000.0).round();
        if millis > 4_294_967_296.0 {
            return None;
        }
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "bounds-checked immediately above; the value is a rounded, positive f64"
        )]
        let millis = millis as u64;
        millis
    };
    (millicores > 0).then_some(Some(millicores))
}

/// Render millicores the way an operator writes them: `4 cores`, `3.5 cores`,
/// `500m`.
fn format_millicores(millicores: u64) -> String {
    if millicores.is_multiple_of(1000) {
        let cores = millicores / 1000;
        if cores == 1 {
            "1 core".to_string()
        } else {
            format!("{cores} cores")
        }
    } else if millicores > 1000 {
        let fraction = format!("{:03}", millicores % 1000);
        format!(
            "{}.{} cores",
            millicores / 1000,
            fraction.trim_end_matches('0')
        )
    } else {
        format!("{millicores}m")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(affinity_cores: usize) -> HostReadings {
        HostReadings {
            affinity_cores,
            ..HostReadings::default()
        }
    }

    fn budget(cores: usize) -> CpuBudget {
        CpuBudget::resolve(&CpuConfig::default(), &host(cores)).expect("detection cannot fail")
    }

    fn quota(affinity_cores: usize, quota_millicores: u64) -> HostReadings {
        HostReadings {
            affinity_cores,
            quota_millicores: Some(quota_millicores),
            ..HostReadings::default()
        }
    }

    /// A burstable pod: a CPU request, no CPU limit.
    fn request_only(affinity_cores: usize, request_millicores: u64) -> HostReadings {
        HostReadings {
            affinity_cores,
            quota_millicores: None,
            request_millicores: Some(request_millicores),
        }
    }

    #[test]
    fn cpu_shares_and_weight_recover_the_request() {
        // v1: 1024 shares is one CPU.
        assert_eq!(cgroup::parse_cpu_shares("4096"), Some(4000));
        assert_eq!(cgroup::parse_cpu_shares(" 512 \n"), Some(500));
        // Kubernetes' floor for "no CPU request" reads back as absent, not as a
        // request of ~2 millicores.
        assert_eq!(cgroup::parse_cpu_shares("2"), None);
        assert_eq!(cgroup::parse_cpu_shares("garbage"), None);

        // v2: the kubelet's share-to-weight mapping, inverted. `requests.cpu: 4`
        // becomes 4096 shares becomes weight 157.
        let four_cores = cgroup::parse_cpu_weight("157").expect("a weight maps back to a request");
        assert!(
            (3900..=4100).contains(&four_cores),
            "weight 157 should recover ~4000 millicores, got {four_cores}"
        );
        assert_eq!(cgroup::parse_cpu_weight("1"), None);
        assert_eq!(cgroup::parse_cpu_weight("garbage"), None);
    }

    #[test]
    fn a_request_far_below_the_effective_cores_warns() {
        // The shape this crate exists for: a request, no limit, so detection
        // falls back to the node and dwarfs what the scheduler guarantees.
        let burstable = CpuBudget::resolve(&CpuConfig::default(), &request_only(64, 4000))
            .expect("detection cannot fail");
        let warning = burstable
            .request_shortfall_warning()
            .expect("4 cores requested against 64 effective cores must warn");
        assert!(warning.contains("4 cores"), "{warning}");
        assert!(warning.contains("64 cores"), "{warning}");
        // No configuration surface and no limit set this one — detection did, and
        // the warning names that rather than a setting the operator never touched,
        // plus the absent limit that sent sizing to the machine in the first place.
        assert!(
            warning.contains("the CPUs available to this process"),
            "{warning}"
        );
        assert!(warning.contains("no CPU limit detected"), "{warning}");
        // Never "the host CPU count": a cpuset can pin this process to a subset of
        // the machine, and never "no CPU limit set": an unreadable quota degrades
        // to the same `None` as an absent one, so neither claim is ours to make.
        assert!(!warning.contains("host CPU count"), "{warning}");
        assert!(!warning.contains("no CPU limit set"), "{warning}");

        // Under an explicit limit the value is capped, so the qualifier must not
        // appear — it would contradict the limit the warning just named.
        let limited = CpuBudget::resolve(
            &CpuConfig::default(),
            &HostReadings {
                affinity_cores: 64,
                quota_millicores: Some(16_000),
                request_millicores: Some(4000),
            },
        )
        .expect("detection cannot fail")
        .request_shortfall_warning()
        .expect("must warn");
        assert!(!limited.contains("no CPU limit detected"), "{limited}");

        // A request under an explicit limit warns the same way.
        let capped = CpuBudget::resolve(
            &CpuConfig::default(),
            &HostReadings {
                affinity_cores: 64,
                quota_millicores: Some(16_000),
                request_millicores: Some(4000),
            },
        )
        .expect("detection cannot fail");
        let capped = capped
            .request_shortfall_warning()
            .expect("a request far under the limit must warn");
        assert!(capped.contains("cgroup CPU limit"), "{capped}");
    }

    /// The warning states the discrepancy and its source, and stops there — no
    /// remedy, since which of the two numbers is wrong is the operator's call.
    #[test]
    fn the_warning_carries_no_guidance() {
        let warning = CpuBudget::resolve(&CpuConfig::default(), &request_only(64, 4000))
            .expect("detection cannot fail")
            .request_shortfall_warning()
            .expect("must warn");

        for advice in ["Lower", "Set ", "instead", "Raise", "raise"] {
            assert!(
                !warning.contains(advice),
                "the warning must not advise ({advice:?}): {warning}"
            );
        }
    }

    /// An over-large *configured* value is as wrong as an over-large inferred
    /// one, so the warning fires there too, naming the surface that set it.
    #[test]
    fn an_oversized_configured_value_warns_against_the_request() {
        let configured = CpuBudget::resolve(
            &CpuConfig::from_sources(None, None, Some("384")),
            &request_only(18, 4000),
        )
        .expect("valid");

        let warning = configured
            .request_shortfall_warning()
            .expect("384 configured cores against a 4-core request must warn");
        assert!(warning.contains(CpuConfig::SPICEPOD_SETTING), "{warning}");
        assert!(warning.contains("384 cores"), "{warning}");

        // The CLI surface names itself rather than the spicepod field.
        let via_cli = CpuBudget::resolve(
            &CpuConfig::from_sources(Some("384"), None, None),
            &request_only(18, 4000),
        )
        .expect("valid")
        .request_shortfall_warning()
        .expect("must warn");
        assert!(via_cli.contains(CpuConfig::CLI_SETTING), "{via_cli}");
    }

    #[test]
    fn a_request_close_to_the_effective_cores_is_quiet() {
        // Matching the effective core count: no warning.
        let matched = CpuBudget::resolve(&CpuConfig::default(), &request_only(4, 4000))
            .expect("detection cannot fail");
        assert_eq!(matched.request_shortfall_warning(), None);

        // cgroup v2 quantizes `requests.cpu: 1` to 974m; that must not warn.
        let quantized = CpuBudget::resolve(&CpuConfig::default(), &request_only(1, 974))
            .expect("detection cannot fail");
        assert_eq!(quantized.request_shortfall_warning(), None);

        // Nothing to compare against.
        assert_eq!(budget(16).request_shortfall_warning(), None);
    }

    /// The threshold is *below* half, so exactly half stays quiet and a hair
    /// under it warns. Pinning both sides keeps a later refactor from drifting
    /// the comparison to `<=` and warning on every evenly-halved request.
    #[test]
    fn the_threshold_is_strictly_below_half_the_effective_cores() {
        // 8 effective cores; a request of exactly 4 cores is half, so quiet.
        let exactly_half = CpuBudget::resolve(&CpuConfig::default(), &request_only(8, 4000))
            .expect("detection cannot fail");
        assert_eq!(
            exactly_half.request_shortfall_warning(),
            None,
            "a request at exactly half the effective cores must stay quiet"
        );

        // One millicore under half must warn.
        let just_under = CpuBudget::resolve(&CpuConfig::default(), &request_only(8, 3999))
            .expect("detection cannot fail");
        assert!(
            just_under.request_shortfall_warning().is_some(),
            "a request just under half the effective cores must warn"
        );
    }

    #[test]
    fn max_concurrent_queries_scales_with_the_budget() {
        assert_eq!(budget(1).max_concurrent_queries(), 4);
        assert_eq!(budget(16).max_concurrent_queries(), 64);
    }

    #[test]
    fn quantities() {
        assert_eq!(parse_cpu_quantity("4"), Some(Some(4000)));
        assert_eq!(parse_cpu_quantity("3.5"), Some(Some(3500)));
        assert_eq!(parse_cpu_quantity("3500m"), Some(Some(3500)));
        assert_eq!(parse_cpu_quantity(" 500m "), Some(Some(500)));
        assert_eq!(parse_cpu_quantity("auto"), Some(None));
        assert_eq!(parse_cpu_quantity("AUTO"), Some(None));
        assert_eq!(parse_cpu_quantity(""), Some(None));

        assert_eq!(parse_cpu_quantity("0"), None);
        assert_eq!(parse_cpu_quantity("0m"), None);
        assert_eq!(parse_cpu_quantity("-1"), None);
        assert_eq!(parse_cpu_quantity("-1m"), None);
        assert_eq!(parse_cpu_quantity("3.5m"), None);
        assert_eq!(parse_cpu_quantity("garbage"), None);
        assert_eq!(parse_cpu_quantity("NaN"), None);
        assert_eq!(parse_cpu_quantity("inf"), None);
    }

    #[test]
    fn invalid_configured_value_is_an_error() {
        let err = CpuBudget::resolve(&CpuConfig::from_sources(None, None, Some("0")), &host(18))
            .expect_err("zero cores must fail startup");
        let message = err.to_string();
        assert!(message.contains("runtime.cpu.cores"), "{message}");
        assert!(message.contains("Invalid value '0'"), "{message}");
        assert!(message.contains(DOCS_URL), "{message}");
    }

    #[test]
    fn precedence_is_cli_then_env_then_spicepod_then_detection() {
        let host = host(18);
        let cli = CpuBudget::resolve(
            &CpuConfig::from_sources(Some("2"), Some("4"), Some("8")),
            &host,
        )
        .expect("valid");
        assert_eq!(cli.cores(), 2);
        assert_eq!(cli.source(), CpuSource::Configured);

        let env = CpuBudget::resolve(&CpuConfig::from_sources(None, Some("4"), Some("8")), &host)
            .expect("valid");
        assert_eq!(env.cores(), 4);

        let spicepod = CpuBudget::resolve(&CpuConfig::from_sources(None, None, Some("8")), &host)
            .expect("valid");
        assert_eq!(spicepod.cores(), 8);

        let detected =
            CpuBudget::resolve(&CpuConfig::from_sources(None, None, None), &host).expect("valid");
        assert_eq!(detected.cores(), 18);
        assert_eq!(detected.source(), CpuSource::Affinity);

        // `auto` on a higher-precedence surface still wins, and detects.
        let auto = CpuBudget::resolve(
            &CpuConfig::from_sources(Some("auto"), None, Some("8")),
            &host,
        )
        .expect("valid");
        assert_eq!(auto.cores(), 18);
        assert_eq!(auto.source(), CpuSource::Affinity);
    }

    #[test]
    fn fractional_entitlements_round_up_to_whole_threads() {
        let budget =
            CpuBudget::resolve(&CpuConfig::from_sources(Some("3.5"), None, None), &host(18))
                .expect("valid");
        assert_eq!(budget.millicores(), 3500);
        assert_eq!(budget.cores(), 4);

        let sub_core = CpuBudget::resolve(
            &CpuConfig::from_sources(Some("500m"), None, None),
            &host(18),
        )
        .expect("valid");
        assert_eq!(sub_core.millicores(), 500);
        assert_eq!(sub_core.cores(), 1);
    }

    /// The bottom of the ladder: nothing detectable at all still yields a usable
    /// one-core budget rather than a zero-thread pool.
    #[test]
    fn quota_beats_affinity_and_is_capped_by_it() {
        // A quota narrower than the host: the quota wins.
        let b = CpuBudget::resolve(&CpuConfig::default(), &quota(18, 4000)).expect("valid");
        assert_eq!(b.source(), CpuSource::CgroupQuota);
        assert_eq!(b.cores(), 4);

        // A quota WIDER than the cores the process may run on is capped, because
        // `available_parallelism` already took the minimum of the two — not
        // capping would size a `--cpus=100` container for 100 cores.
        let wide = CpuBudget::resolve(&CpuConfig::default(), &quota(16, 100_000)).expect("valid");
        assert_eq!(wide.cores(), 16);
    }

    /// The behaviour this crate must NOT change: a pod with `requests.cpu` and
    /// no `limits.cpu` has no quota, and is entitled to burst across every idle
    /// core on its node. Detection must therefore report the node, exactly as
    /// `available_parallelism` did — inferring a ceiling from the request would
    /// silently take that headroom away.
    #[test]
    fn a_request_without_a_limit_still_sizes_for_the_whole_node() {
        // requests.cpu set, limits.cpu unset
        let burstable = request_only(64, 4000);
        let budget = CpuBudget::resolve(&CpuConfig::default(), &burstable).expect("valid");
        assert_eq!(budget.source(), CpuSource::Affinity);
        assert_eq!(budget.cores(), 64);
        assert_eq!(budget.target_partitions(), 64);

        // ...and an operator who wants it sized to the request says so.
        let pinned =
            CpuBudget::resolve(&CpuConfig::from_sources(None, Some("4"), None), &burstable)
                .expect("valid");
        assert_eq!(pinned.source(), CpuSource::Configured);
        assert_eq!(pinned.cores(), 4);
    }

    #[test]
    fn nothing_detectable_falls_back_to_one_core() {
        let budget = CpuBudget::resolve(&CpuConfig::default(), &host(0)).expect("valid");
        assert_eq!(budget.source(), CpuSource::Fallback);
        assert_eq!(budget.cores(), 1);
        assert_eq!(budget.main_runtime_worker_threads(), 1);
        assert_eq!(budget.dedicated_runtime_worker_threads(), 1);
    }

    #[test]
    fn busy_fraction_divides_by_the_entitlement_and_clamps() {
        let four = budget(4);
        // A fully saturated 4-core entitlement.
        assert!((four.cpu_busy_fraction(4.0, 1.0) - 1.0).abs() < f64::EPSILON);
        assert!((four.cpu_busy_fraction(1.0, 1.0) - 0.25).abs() < f64::EPSILON);
        // Configured below the true host count, so the process can exceed it.
        assert!((four.cpu_busy_fraction(18.0, 1.0) - 1.0).abs() < f64::EPSILON);
        // Degenerate windows report no pressure rather than NaN/inf.
        assert!((four.cpu_busy_fraction(1.0, 0.0)).abs() < f64::EPSILON);
        assert!((four.cpu_busy_fraction(f64::NAN, 1.0)).abs() < f64::EPSILON);

        let fractional =
            CpuBudget::resolve(&CpuConfig::from_sources(Some("3.5"), None, None), &host(18))
                .expect("valid");
        assert!((fractional.cpu_busy_fraction(3.5, 1.0) - 1.0).abs() < f64::EPSILON);
    }

    /// The regression guard for every derived value. Each row is what the call
    /// site computed before this crate existed, so a changed body shows up here
    /// rather than in production.
    #[test]
    fn sizing_matrix() {
        let rows: Vec<Vec<usize>> = [1_usize, 2, 4, 16, 64, 384]
            .into_iter()
            .map(|cores| {
                let b = budget(cores);
                vec![
                    b.cores(),
                    b.main_runtime_worker_threads(),
                    b.dedicated_runtime_worker_threads(),
                    b.target_partitions(),
                    b.cayenne_encode_permits(),
                    b.cayenne_write_concurrency_ceiling(),
                    b.metastore_pool_connections(),
                    b.embedding_pool_threads(),
                    b.duckdb_threads(),
                ]
            })
            .collect();

        assert_eq!(
            rows,
            vec![
                //  cores main dedicated partitions encode wc pool embed duckdb
                vec![1, 1, 1, 1, 1, 1, 2, 1, 1],
                vec![2, 2, 1, 2, 1, 2, 2, 2, 2],
                vec![4, 4, 3, 4, 2, 4, 4, 4, 4],
                vec![16, 16, 15, 16, 12, 16, 16, 16, 16],
                vec![64, 64, 63, 64, 48, 64, 32, 64, 64],
                vec![384, 384, 383, 384, 288, 384, 32, 384, 384],
            ]
        );
    }

    #[test]
    fn summary_lines() {
        insta::assert_snapshot!(
            "summary_configured",
            CpuBudget::resolve(&CpuConfig::from_sources(None, None, Some("4")), &host(18))
                .expect("valid")
                .summary_line()
        );

        insta::assert_snapshot!(
            "summary_cgroup_quota",
            CpuBudget::resolve(&CpuConfig::default(), &quota(18, 4000))
                .expect("valid")
                .summary_line()
        );
        insta::assert_snapshot!(
            "summary_affinity",
            CpuBudget::resolve(&CpuConfig::default(), &host(18))
                .expect("valid")
                .summary_line()
        );
    }

    /// The startup log has to name every derived value, so a mis-sized pool is
    /// diagnosable from the log alone. This snapshot is also the guard that a
    /// new sizing method was added to `derived_sizing`.
    #[test]
    fn derived_sizing_is_fully_reported() {
        let budget = CpuBudget::resolve(&CpuConfig::from_sources(None, None, Some("4")), &host(18))
            .expect("valid");

        // Every public sizing method appears in the reported inventory.
        let reported: std::collections::HashSet<_> = budget
            .derived_sizing()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        for name in [
            "main_runtime_worker_threads",
            "dedicated_runtime_worker_threads",
            "target_partitions",
            "max_concurrent_queries",
            "cayenne_encode_permits",
            "cayenne_write_concurrency_ceiling",
            "cayenne_compaction_permits",
            "cayenne_upload_concurrency",
            "cayenne_max_concurrent_file_scans",
            "metastore_pool_connections",
            "embedding_pool_threads",
            "duckdb_threads",
            "cluster_executor_concurrent_tasks",
        ] {
            assert!(reported.contains(name), "{name} is not reported at startup");
        }

        insta::assert_snapshot!("derived_sizing", budget.derived_sizing_line());
    }

    #[test]
    fn millicore_formatting() {
        assert_eq!(format_millicores(1000), "1 core");
        assert_eq!(format_millicores(4000), "4 cores");
        assert_eq!(format_millicores(3500), "3.5 cores");
        assert_eq!(format_millicores(500), "500m");
        assert_eq!(format_millicores(1250), "1.25 cores");
    }
}
