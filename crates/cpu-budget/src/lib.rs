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
//! `std::thread::available_parallelism` and `num_cpus::get` read the cgroup CPU
//! *quota* and otherwise fall back to `sched_getaffinity` — the node's core
//! count. A Kubernetes pod that sets `resources.requests.cpu` with no
//! `resources.limits.cpu` has no quota, so both report the node. Sizing thread
//! pools, query fan-out, and accelerator concurrency from that over-commits the
//! CPU the pod is entitled to, and — worse — feeds a wrong denominator to the
//! Cayenne tuner's CPU busy-fraction, which then concludes CPU is idle while the
//! process is saturated.
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
    /// A Kubernetes cgroup CPU share (v2 `cpu.weight`, v1 `cpu.shares`), which
    /// the kubelet derives from `requests.cpu`.
    CgroupShares,
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
            Self::CgroupShares => "cgroup_shares",
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
    /// Millicores implied by a cgroup CPU quota.
    pub quota_millicores: Option<u64>,
    /// The cgroup CPU share, normalized to v1 units.
    pub shares: Option<cgroup::CpuShares>,
    /// Running under a kubelet-managed cgroup.
    pub in_kubernetes: bool,
    /// Running inside a container of any flavour.
    pub in_container: bool,
}

impl HostReadings {
    /// Read the host. Every field degrades to "unknown" rather than failing.
    #[must_use]
    #[expect(
        clippy::disallowed_methods,
        reason = "the one sanctioned detection site: `clippy.toml` forbids this everywhere else so a call site cannot size itself from the node's cores"
    )]
    pub fn detect() -> Self {
        let in_kubernetes = cgroup::in_kubernetes();
        Self {
            affinity_cores: std::thread::available_parallelism()
                .map_or(1, std::num::NonZeroUsize::get),
            quota_millicores: detect_quota_millicores(),
            shares: detect_shares(),
            in_kubernetes,
            // `in_kubernetes` implies a container; short-circuit the extra reads.
            in_container: in_kubernetes || cgroup::in_container(),
        }
    }
}

fn detect_quota_millicores() -> Option<u64> {
    if let Some(path) = cgroup::v2_file_path("cpu.max")
        && let Ok(contents) = std::fs::read_to_string(&path)
        && let Some(millicores) = cgroup::parse_cpu_max(&contents)
    {
        return Some(millicores);
    }
    let quota = cgroup::v1_file_path("cpu", "cpu.cfs_quota_us")?;
    let period = cgroup::v1_file_path("cpu", "cpu.cfs_period_us")?;
    cgroup::parse_cfs_quota(
        &std::fs::read_to_string(quota).ok()?,
        &std::fs::read_to_string(period).ok()?,
    )
}

fn detect_shares() -> Option<cgroup::CpuShares> {
    if let Some(path) = cgroup::v2_file_path("cpu.weight")
        && let Ok(contents) = std::fs::read_to_string(&path)
        && let Some(shares) = cgroup::parse_weight(&contents)
    {
        return Some(shares);
    }
    let path = cgroup::v1_file_path("cpu", "cpu.shares")?;
    cgroup::parse_shares(&std::fs::read_to_string(path).ok()?)
}

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
    /// The entitlement the cgroup CPU share implies, when that share carries
    /// information. Drives the over-commitment warning.
    shares_implied_millicores: Option<u64>,
}

impl CpuBudget {
    /// Resolve the effective entitlement.
    ///
    /// The ladder, first hit wins:
    ///
    /// 1. an explicit `cores` value -> [`CpuSource::Configured`]
    /// 2. a cgroup CPU quota -> [`CpuSource::CgroupQuota`] (today's effective
    ///    behaviour, so quota'd deployments see no change)
    /// 3. a **Kubernetes** cgroup CPU share -> [`CpuSource::CgroupShares`]
    /// 4. `sched_getaffinity` -> [`CpuSource::Affinity`]
    /// 5. one core -> [`CpuSource::Fallback`]
    ///
    /// Step 3 is gated on Kubernetes and can never raise the value above step 4.
    /// Outside Kubernetes a CPU share is a *relative* scheduling weight with an
    /// ambiguous default — systemd's default `CPUWeight` of 100 inverts to ≈2.5
    /// cores, so trusting it would size an unconstrained bare-metal `spiced` for
    /// 2.5 cores instead of 64.
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

        let shares_implied_millicores = host
            .shares
            .filter(|s| s.is_meaningful())
            .map(cgroup::CpuShares::millicores);

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
            (quota.max(1), CpuSource::CgroupQuota, None)
        } else if let Some(implied) = shares_implied_millicores.filter(|_| host.in_kubernetes) {
            // Never above what the process may actually run on.
            (
                implied.min(affinity_millicores),
                CpuSource::CgroupShares,
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
            shares_implied_millicores: host
                .shares
                .filter(|s| s.is_meaningful() && !s.is_default && host.in_container)
                .map(cgroup::CpuShares::millicores),
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

    /// Log the effective budget, every value derived from it, and — where it
    /// applies — the over-commitment warning. Call once, before anything is
    /// sized.
    ///
    /// The derived values are logged as their own line because the headline
    /// summary names only the three an operator recognizes. Every consumer's
    /// number is here, so a mis-sized pool can be diagnosed from a startup log
    /// alone rather than by correlating `/metrics` gauges — and so that a future
    /// change to a sizing policy is visible in the log diff.
    pub fn log_summary(&self) {
        tracing::info!("{}", self.summary_line());
        tracing::info!("{}", self.derived_sizing_line());
        if let Some(warning) = self.overcommit_warning() {
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

    /// The one-line startup summary: the effective value, where it came from,
    /// and what it sizes.
    #[must_use]
    pub fn summary_line(&self) -> String {
        let origin = match self.source {
            CpuSource::Configured => self.setting.unwrap_or(CpuConfig::SPICEPOD_SETTING),
            CpuSource::CgroupQuota => "cgroup CPU quota",
            CpuSource::CgroupShares => "cgroup CPU share, i.e. the pod's requests.cpu",
            CpuSource::Affinity => "detected CPUs",
            CpuSource::Fallback => "fallback",
        };
        format!(
            "CPU budget: {entitlement} (source: {origin}; host reports {detected}) \u{2192} \
             {main} main worker threads, {dedicated} per dedicated runtime pool, \
             {partitions} target partitions",
            entitlement = format_millicores(self.millicores),
            detected = self.detected_cores,
            main = self.main_runtime_worker_threads(),
            dedicated = self.dedicated_runtime_worker_threads(),
            partitions = self.target_partitions(),
        )
    }

    /// The warning for a container whose CPU share implies a materially smaller
    /// entitlement than the one being sized for — the request-without-limit
    /// deployment outside Kubernetes, where the share is real but cannot be
    /// trusted automatically.
    ///
    /// `None` when the budget already sizes from the share or from explicit
    /// configuration, when the share is the cgroup default (systemd and a plain
    /// `docker run` both leave it there, so it means nothing), or when the two
    /// values agree.
    #[must_use]
    pub fn overcommit_warning(&self) -> Option<String> {
        if matches!(self.source, CpuSource::Configured | CpuSource::CgroupShares) {
            return None;
        }
        let implied = self.shares_implied_millicores?;
        // "Materially below": at most half the entitlement being used, so a
        // rounding difference never warns.
        if implied.saturating_mul(2) > self.millicores {
            return None;
        }
        Some(format!(
            "Detected {detected} CPUs, but this container's cgroup CPU share implies ~{implied} \
             and no CPU quota is set. spiced is sizing its thread pools for {effective}, which \
             over-commits the CPU this container is entitled to. Set `{spicepod}` (or \
             {env}) to the container's CPU request. See: {DOCS_URL}",
            detected = self.detected_cores,
            implied = format_millicores(implied),
            effective = format_millicores(self.millicores),
            spicepod = CpuConfig::SPICEPOD_SETTING,
            env = CpuConfig::ENV_SETTING,
        ))
    }

    // ---- the entitlement ----

    /// The entitlement in whole cores, rounded up, at least 1.
    #[must_use]
    pub const fn cores(&self) -> usize {
        self.cores
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
            shares_implied_millicores: None,
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

    fn kubernetes_shares(affinity_cores: usize, request_millicores: u64) -> HostReadings {
        let shares = (request_millicores * 1024 / 1000).max(cgroup::FLOOR_SHARES);
        HostReadings {
            affinity_cores,
            shares: Some(cgroup::CpuShares {
                shares,
                is_default: shares == cgroup::DEFAULT_SHARES,
                is_floor: shares <= cgroup::FLOOR_SHARES,
            }),
            in_kubernetes: true,
            in_container: true,
            ..HostReadings::default()
        }
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

    #[test]
    fn quota_beats_shares_beats_affinity() {
        let mut readings = kubernetes_shares(18, 4000);
        readings.quota_millicores = Some(2000);
        let quota = CpuBudget::resolve(&CpuConfig::default(), &readings).expect("valid");
        assert_eq!(quota.source(), CpuSource::CgroupQuota);
        assert_eq!(quota.cores(), 2);

        let shares =
            CpuBudget::resolve(&CpuConfig::default(), &kubernetes_shares(18, 4000)).expect("valid");
        assert_eq!(shares.source(), CpuSource::CgroupShares);
        assert_eq!(shares.cores(), 4);
    }

    /// The bottom of the ladder: nothing detectable at all still yields a usable
    /// one-core budget rather than a zero-thread pool.
    #[test]
    fn nothing_detectable_falls_back_to_one_core() {
        let budget = CpuBudget::resolve(&CpuConfig::default(), &host(0)).expect("valid");
        assert_eq!(budget.source(), CpuSource::Fallback);
        assert_eq!(budget.cores(), 1);
        assert_eq!(budget.main_runtime_worker_threads(), 1);
        assert_eq!(budget.dedicated_runtime_worker_threads(), 1);
    }

    #[test]
    fn shares_are_ignored_outside_kubernetes() {
        let mut readings = kubernetes_shares(18, 4000);
        readings.in_kubernetes = false;
        let budget = CpuBudget::resolve(&CpuConfig::default(), &readings).expect("valid");
        assert_eq!(budget.source(), CpuSource::Affinity);
        assert_eq!(budget.cores(), 18);
    }

    #[test]
    fn shares_never_exceed_the_affinity_count() {
        let budget = CpuBudget::resolve(&CpuConfig::default(), &kubernetes_shares(4, 32_000))
            .expect("valid");
        assert_eq!(budget.source(), CpuSource::CgroupShares);
        assert_eq!(budget.cores(), 4);
    }

    #[test]
    fn a_pod_with_no_cpu_request_falls_through_to_affinity() {
        // The kubelet floor (`shares = 2`) is what a best-effort or
        // memory-only-request pod gets; it must keep node-sized behaviour.
        let budget =
            CpuBudget::resolve(&CpuConfig::default(), &kubernetes_shares(18, 0)).expect("valid");
        assert_eq!(budget.source(), CpuSource::Affinity);
        assert_eq!(budget.cores(), 18);
    }

    #[test]
    fn one_core_requests_are_honoured_in_kubernetes() {
        // `shares == 1024` is also cgroup v1's *default*, but under a confirmed
        // kubelet cgroup it unambiguously means `requests.cpu: 1`.
        let budget =
            CpuBudget::resolve(&CpuConfig::default(), &kubernetes_shares(18, 1000)).expect("valid");
        assert_eq!(budget.source(), CpuSource::CgroupShares);
        assert_eq!(budget.cores(), 1);
    }

    #[test]
    fn overcommit_warning_fires_only_for_a_non_default_container_share() {
        // The issue's repro: `docker run --cpu-shares=4096` on an 18-core host,
        // no quota, not Kubernetes.
        let mut docker = kubernetes_shares(18, 4000);
        docker.in_kubernetes = false;
        let budget = CpuBudget::resolve(&CpuConfig::default(), &docker).expect("valid");
        let warning = budget
            .overcommit_warning()
            .expect("a 4-core share against 18 detected cores must warn");
        assert!(warning.contains("18 CPUs"), "{warning}");
        assert!(warning.contains("SPICE_CPU_CORES"), "{warning}");

        // systemd's default CPUWeight, and a plain `docker run`, leave the
        // cgroup default in place — it means nothing, so it must not warn.
        let mut default_share = docker.clone();
        default_share.shares = Some(cgroup::CpuShares {
            shares: cgroup::DEFAULT_SHARES,
            is_default: true,
            is_floor: false,
        });
        assert!(
            CpuBudget::resolve(&CpuConfig::default(), &default_share)
                .expect("valid")
                .overcommit_warning()
                .is_none()
        );

        // Bare metal: a share outside a container is a scheduling weight.
        let mut bare_metal = docker.clone();
        bare_metal.in_container = false;
        assert!(
            CpuBudget::resolve(&CpuConfig::default(), &bare_metal)
                .expect("valid")
                .overcommit_warning()
                .is_none()
        );

        // Already sizing from the share, or from explicit configuration.
        assert!(
            CpuBudget::resolve(&CpuConfig::default(), &kubernetes_shares(18, 4000))
                .expect("valid")
                .overcommit_warning()
                .is_none()
        );
        assert!(
            CpuBudget::resolve(&CpuConfig::from_sources(Some("4"), None, None), &docker)
                .expect("valid")
                .overcommit_warning()
                .is_none()
        );
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
            "summary_cgroup_shares",
            CpuBudget::resolve(&CpuConfig::default(), &kubernetes_shares(18, 4000))
                .expect("valid")
                .summary_line()
        );

        let mut docker = kubernetes_shares(18, 4000);
        docker.in_kubernetes = false;
        let detected = CpuBudget::resolve(&CpuConfig::default(), &docker).expect("valid");
        insta::assert_snapshot!("summary_affinity", detected.summary_line());
        insta::assert_snapshot!(
            "warning_overcommit",
            detected.overcommit_warning().expect("warns")
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
