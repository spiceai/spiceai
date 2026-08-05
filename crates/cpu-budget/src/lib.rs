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
//! With nothing configured and no CPU request declared, this resolves to the same
//! value `available_parallelism` returned (a cgroup quota, capped by
//! `sched_getaffinity`) — so every bare-metal deployment and every benchmark keeps
//! the whole host. What is new is that the value has one owner, a named source, and
//! an explicit override for deployments the host cannot describe.
//!
//! A pod that declares a CPU request and no CPU limit is sized from that request
//! instead: a request is a scheduling floor rather than a ceiling, so sizing *at*
//! it would take away the bursting that is the reason the limit was omitted, and
//! sizing for the node hands the runtime a machine it does not own. The
//! entitlement is a bounded multiple of the request — see
//! [`CPU_REQUEST_BURST_FACTOR`] — and `runtime.cpu.cores: all` opts out for the
//! deployment that packs many mostly-idle instances onto one node.
//!
//! The request must be *declared*, through [`CPU_REQUEST_ENV`]. A cgroup CPU
//! *share* is never an input: every cgroup carries one whether or not a request was
//! expressed, and the conversion back to a request varies by writer, so it is read
//! for reporting and for noticing a resize, never for sizing. See
//! [`HostReadings::cpu_share`].
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
        "Failed to configure the runtime CPU budget ({setting}): Invalid value '{value}'. Expected a positive CPU quantity such as `4`, `3.5`, or `3500m`; `auto` to detect it; or `all` for every available core. See: {DOCS_URL}"
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
    /// `runtime.cpu.cores: all` — every CPU available, with the request rung
    /// deliberately suppressed. A cgroup quota still caps it.
    AllCores,
    /// A bounded multiple of the pod's declared `requests.cpu`.
    RequestBurst,
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
            Self::AllCores => "all_cores",
            Self::RequestBurst => "request_burst",
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

/// A cgroup CPU share, exactly as the kernel reports it.
///
/// Carried raw and never converted into a `requests.cpu`. The kubelet derives
/// the share *from* the request, but the conversion varies by writer — see
/// [`cgroup::parse_cpu_weight`] for measurements two to three times off — so a
/// recovered core count would be a confident wrong answer. What the raw value is
/// good for is comparing against itself: if it moves while the process runs, the
/// pod was resized, whatever the number means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuShare {
    /// cgroup v2 `cpu.weight`, in its 1..=10000 range.
    Weight(u64),
    /// cgroup v1 `cpu.shares`, in its 2..=262144 range.
    Shares(u64),
}

impl std::fmt::Display for CpuShare {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Weight(weight) => write!(f, "weight {weight}"),
            Self::Shares(shares) => write!(f, "{shares} shares"),
        }
    }
}

/// Watches the cgroup CPU share for movement after the budget was installed.
///
/// In-place pod resize is [GA in Kubernetes 1.35](https://kubernetes.io/blog/2025/12/19/kubernetes-v1-35-in-place-pod-resize-ga),
/// so a VPA-managed pod's `requests.cpu` can change underneath a running process.
/// The budget resolves once into a `OnceLock` and every consumer captured its
/// derived quantities at startup, so a fleet silently sized for a request that no
/// longer exists is not diagnosable from a startup log.
///
/// This notices and says so. It cannot act: nothing here re-installs the budget,
/// so [`Error::AlreadyInstalled`] stays unreachable on this path, and the honest
/// way to pick up a new size is a restart.
///
/// The *raw* share is what makes this sound. Comparing a reading against itself
/// needs no conversion, so the writer-specific share-to-request mapping (see
/// [`cgroup::parse_cpu_weight`]) cannot mislead it — and the declared request
/// cannot serve here at all, because `resourceFieldRef` resolves once at
/// container creation and does not change on a resize.
pub struct ShareDriftWatcher {
    /// The entitlement still in force, rendered once.
    entitlement: String,
    /// The last share observed, encoded — see [`encode_share`]. Lock-free because
    /// this is read on a timer and written only when the value moves.
    last: std::sync::atomic::AtomicU64,
}

/// Pack `Option<CpuShare>` into a `u64`: a two-bit tag, then the value.
///
/// A weight is at most 10000 and a share at most 262144, so nothing is truncated.
const fn encode_share(share: Option<CpuShare>) -> u64 {
    match share {
        None => 0,
        Some(CpuShare::Weight(weight)) => (weight << 2) | 1,
        Some(CpuShare::Shares(shares)) => (shares << 2) | 2,
    }
}

/// Render an encoded share the way [`CpuShare`] renders itself.
fn describe_share(encoded: u64) -> String {
    match encoded & 0b11 {
        1 => CpuShare::Weight(encoded >> 2).to_string(),
        2 => CpuShare::Shares(encoded >> 2).to_string(),
        _ => "unset".to_string(),
    }
}

impl ShareDriftWatcher {
    /// The message to log when the share has moved since the last observation.
    ///
    /// Returns `Some` once per change rather than on every poll: an unchanged
    /// reading is silent, and a *further* change is new information and speaks
    /// again.
    #[must_use]
    pub fn observe(&self, current: Option<CpuShare>) -> Option<String> {
        let current = encode_share(current);
        let previous = self
            .last
            .swap(current, std::sync::atomic::Ordering::Relaxed);
        if previous == current {
            return None;
        }
        Some(format!(
            "This pod's CPU share changed from {before} to {after} after startup; spiced is still \
             sized for {entitlement}. Restart to apply the new request. See: {DOCS_URL}",
            before = describe_share(previous),
            after = describe_share(current),
            entitlement = self.entitlement,
        ))
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
    /// The cgroup CPU share, raw. Never an input to sizing: every cgroup carries
    /// a share whether or not a request was expressed — a plain `docker run`
    /// reports `cpu.weight: 100` — so its presence is not evidence of a request
    /// unless something else establishes that the kubelet wrote it. `None` when
    /// no share can be read, or when it sits at the no-request floor.
    pub cpu_share: Option<CpuShare>,
    /// Whether this process is running under Kubernetes, from
    /// `KUBERNETES_SERVICE_HOST` — the same signal `client-go` uses for
    /// in-cluster detection, and the one thing that makes a share interpretable
    /// as evidence that a request was set. A cgroup path gate cannot do this
    /// job: with cgroup namespaces `/proc/self/cgroup` reads `0::/` inside a
    /// container, so the `kubepods` ancestry is not visible from in here.
    pub kubernetes: bool,
    /// The pod's own `requests.cpu`, in millicores, as declared by whatever
    /// wrote the pod spec ([`CPU_REQUEST_ENV`]). Unlike the share this is exact
    /// and unquantized, and its presence is itself evidence that something which
    /// could read a pod spec put it there. `None` when no request is declared,
    /// which is every bare-metal and benchmark deployment.
    pub declared_request_millicores: Option<u64>,
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
            cpu_share: detect_cpu_share(),
            kubernetes: std::env::var_os("KUBERNETES_SERVICE_HOST")
                .is_some_and(|value| !value.is_empty()),
            declared_request_millicores: detect_declared_request_millicores(),
        }
    }
}

/// The environment variable carrying the pod's own `requests.cpu`.
///
/// Written by whatever templates the pod spec — the Helm chart and the
/// Kubernetes operator — from the downward API, never by hand:
///
/// ```yaml
/// - name: SPICE_CPU_REQUEST_MILLICORES
///   valueFrom:
///     resourceFieldRef:
///       resource: requests.cpu
///       divisor: 1m
/// ```
///
/// The `divisor: 1m` is what makes the value millicores, and it is load-bearing:
/// `requests.cpu: 4` arrives as `4000`, not `4`.
///
/// The unit is in the variable's name because it has to be. `SPICE_CPU_CORES`
/// takes the Kubernetes quantity grammar, where a bare `4` means four *cores*,
/// and the two variables sit one concept apart — so a shared name with unstated
/// units would put a 1000x difference behind two indistinguishable values.
/// `divisor: 1` would have avoided that by sending whole cores, but it rounds
/// up, and this value is *reported*: a `100m` request would arrive as `1` and be
/// logged as one core, misstating by 10x the exact number an operator compares
/// against the budget. A core-shaped value is therefore rejected rather than
/// read 1000x too small — see [`parse_declared_request_millicores`].
pub const CPU_REQUEST_ENV: &str = "SPICE_CPU_REQUEST_MILLICORES";

/// The declared `requests.cpu` from [`CPU_REQUEST_ENV`], in millicores.
///
/// A value that is set but unparseable warns and reads as absent, so a
/// malformed variable falls through to the next reading rather than sizing
/// anything from a number nobody can interpret.
fn detect_declared_request_millicores() -> Option<u64> {
    let raw = std::env::var(CPU_REQUEST_ENV).ok()?;
    if raw.trim().is_empty() {
        return None;
    }
    let parsed = parse_declared_request_millicores(&raw);
    if parsed.is_none() {
        tracing::warn!(
            "Ignoring {CPU_REQUEST_ENV}: '{raw}' is not a whole number of millicores. Expected the pod's CPU request in millicores, as written by `resourceFieldRef` with `divisor: 1m` (a `requests.cpu` of 4 arrives as '4000'). CPU sizing will fall back to the cgroup CPU limit or the available CPU count. See: {DOCS_URL}"
        );
    }
    parsed
}

/// Parse [`CPU_REQUEST_ENV`]: a whole number of millicores, with an optional
/// `m` suffix.
///
/// Deliberately *not* [`parse_cpu_quantity`]. That grammar reads a bare number
/// as cores, which is the opposite of what `divisor: 1m` produces, so sharing it
/// would read a correctly-wired `requests.cpu: 4` (`4000`) as 4000 cores. Since
/// the two grammars disagree on every bare integer, anything core-shaped — a
/// decimal like `3.5`, or a sign — is rejected instead of guessed at: a
/// deployment surface that dropped the divisor is a bug to surface, not a value
/// to interpret 1000x too small.
#[must_use]
pub fn parse_declared_request_millicores(value: &str) -> Option<u64> {
    let value = value.trim();
    let digits = value.strip_suffix('m').unwrap_or(value).trim();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let millicores: u64 = digits.parse().ok()?;
    (millicores > 0).then_some(millicores)
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

/// The cgroup CPU share, raw.
///
/// Reporting only. Sizing never consults it — see [`CpuBudget::resolve`]. Public
/// so a later re-read can compare against the value captured at startup and
/// notice that the pod was resized underneath the process.
///
/// Read at the leaf, unlike the quota. A share is a relative weight *among
/// siblings at one level*, not a ceiling inherited down the tree, so the
/// smallest value along the path would mean nothing; the container's own share
/// is the one the kubelet derived from its `requests.cpu`.
pub fn detect_cpu_share() -> Option<CpuShare> {
    if let Some((mountpoint, cgroup_path)) = cgroup::v2_mount_and_path()
        && let Ok(contents) = std::fs::read_to_string(cgroup::cgroup_file_path(
            &mountpoint,
            &cgroup_path,
            "cpu.weight",
        ))
        && let Some(weight) = cgroup::parse_cpu_weight(&contents)
    {
        return Some(CpuShare::Weight(weight));
    }
    let (mountpoint, cgroup_path) = cgroup::v1_mount_and_path("cpu")?;
    let shares = std::fs::read_to_string(cgroup::cgroup_file_path(
        &mountpoint,
        &cgroup_path,
        "cpu.shares",
    ))
    .ok()?;
    cgroup::parse_cpu_shares(&shares).map(CpuShare::Shares)
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

/// How far above its CPU request a burstable pod sizes itself.
///
/// A request is a scheduling floor, not a ceiling, so sizing *at* the request
/// would take away the bursting that is the reason the pod omitted a limit.
/// Sizing for the whole node instead is the bug this exists to fix. 2x leaves
/// real headroom over the floor while keeping the derived pools within reach of
/// what the scheduler guarantees, and it keeps the arithmetic exact in
/// millicores. Nothing in the configuration depends on the value: an operator
/// who disagrees writes a number, and one who wants the node writes `all`.
const CPU_REQUEST_BURST_FACTOR: u64 = 2;

/// The smallest entitlement a request may derive, unless the host itself is
/// smaller.
///
/// A `requests.cpu: 100m` pod would otherwise land on one core, which resolves
/// to one worker thread per runtime and `target_partitions = 1` — a runtime that
/// cannot overlap a scan with anything. Go 1.25 makes the same call for the same
/// reason, deriving `GOMAXPROCS` as `max(2, ceil(limit))`.
const REQUEST_DERIVED_FLOOR_MILLICORES: u64 = 2000;

/// Below this fraction of what the host reports, a request-derived entitlement
/// is worth saying out loud — see [`CpuBudget::sizing_notice`].
///
/// A judgement call rather than a tuned number, and deliberately not a quarter.
/// The recommended shape — a CPU request with no limit — lands at exactly
/// `factor / cores_on_the_node`, so a 4-core request on a 64-core host derives
/// 1/8 of it. Warning there would fire on the configuration this enhancement
/// tells operators to adopt, on every restart, with nothing to fix. An eighth
/// keeps that quiet while still speaking up for the order-of-magnitude case a
/// `100m` request produces: 2 cores of 64 is 1/32, and 2 of 18 is 1/9.
const SIZING_NOTICE_NUM: u64 = 1;
/// Denominator of [`SIZING_NOTICE_NUM`].
const SIZING_NOTICE_DEN: u64 = 8;

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
    /// The cgroup CPU share, raw. Reported only — see [`HostReadings::cpu_share`]
    /// for why it never sizes and is never converted to cores.
    cpu_share: Option<CpuShare>,
    /// Whether this process is running under Kubernetes.
    kubernetes: bool,
    /// The pod's declared `requests.cpu`. Reported, and the only request the
    /// warning below is allowed to compare against.
    declared_request_millicores: Option<u64>,
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
            Some((value, setting)) => Some((
                parse_cpu_quantity(value).context(InvalidCpuQuantitySnafu {
                    setting: (*setting).to_string(),
                    value: value.clone(),
                })?,
                *setting,
            )),
            None => None,
        };

        // `all` is not a rung. It suppresses the request rung and lets the rest of
        // the ladder run, so it resolves exactly as it would on a pod that
        // declared no request — a cgroup limit if one is set, otherwise the
        // available CPUs. Expressing it as "skip one rung" rather than as its own
        // `min(quota, affinity)` keeps one place that knows how to weigh a quota
        // against affinity, and needs no update when a rung is added.
        let all_cores = matches!(configured, Some((CpuSetting::All, _)));
        let explicit = match configured {
            Some((CpuSetting::Cores(millicores), setting)) => Some((millicores, setting)),
            _ => None,
        };

        let (millicores, source, setting) = if let Some((millicores, setting)) = explicit {
            (millicores, CpuSource::Configured, Some(setting))
        } else if let Some(quota) = host.quota_millicores {
            // Capped by affinity: a quota wider than the cores the process may
            // run on cannot be used, and `available_parallelism` already takes
            // the minimum of the two. Without the cap a `--cpus=100` container
            // on a 16-core host would size itself for 100 cores.
            //
            // A quota outranks the request rung below deliberately: it is a hard
            // CFS ceiling, and bursting above it produces throttling rather than
            // CPU. `all` keeps the quota for the same reason.
            let capped = quota.max(1).min(affinity_millicores);
            let source = if all_cores {
                CpuSource::AllCores
            } else {
                CpuSource::CgroupQuota
            };
            (capped, source, None)
        } else if let Some(request) = host.declared_request_millicores.filter(|_| !all_cores) {
            // The burstable pod: a request, no limit. Sized for a bounded
            // multiple of what the scheduler guarantees, never above what the
            // process can actually run on, and never so small that the runtime
            // cannot overlap a scan with anything.
            let burst = request
                .saturating_mul(CPU_REQUEST_BURST_FACTOR)
                .max(REQUEST_DERIVED_FLOOR_MILLICORES)
                .min(affinity_millicores);
            (burst, CpuSource::RequestBurst, None)
        } else if host.affinity_cores > 0 {
            let source = if all_cores {
                CpuSource::AllCores
            } else {
                CpuSource::Affinity
            };
            (affinity_millicores, source, None)
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
            cpu_share: host.cpu_share,
            kubernetes: host.kubernetes,
            declared_request_millicores: host.declared_request_millicores,
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
        // Both notices report a consequence of something the operator chose, so
        // they are INFO: the job is to make the choice apparent to whoever is
        // debugging, not to claim it was a mistake. The warning below is different
        // — a request that exists and did not arrive is a gap, not a decision.
        if let Some(notice) = self.request_shortfall_notice() {
            tracing::info!("{notice}");
        }
        if let Some(notice) = self.sizing_notice() {
            tracing::info!("{notice}");
        }
        if let Some(warning) = self.undeclared_request_warning() {
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
            CpuSource::AllCores => "runtime.cpu.cores: all",
            CpuSource::RequestBurst => {
                const {
                    assert!(
                        CPU_REQUEST_BURST_FACTOR == 2,
                        "the origin string below names the factor"
                    );
                }
                "the declared CPU request (x2)"
            }
            CpuSource::Affinity => "the CPUs available to this process",
            CpuSource::Fallback => "fallback",
        }
    }

    /// The one-line startup summary: the effective value, where it came from,
    /// the readings it sits between, and what it sizes.
    ///
    /// Names the declared request and the cgroup share separately, because they
    /// are different claims: the request is what the pod asked for, the share is
    /// a weight that only means anything under Kubernetes.
    #[must_use]
    pub fn summary_line(&self) -> String {
        let unset = "unset".to_string();
        format!(
            "CPU budget: {entitlement} (source: {origin}; host reports {detected}, \
             declared CPU request {declared}, cgroup share {share}, \
             cgroup limit {limit}) \u{2192} \
             {main} main worker threads, {dedicated} per dedicated runtime pool, \
             {partitions} target partitions",
            entitlement = format_millicores(self.millicores),
            origin = self.origin(),
            detected = self.detected_cores,
            declared = self
                .declared_request_millicores
                .map_or_else(|| unset.clone(), format_millicores),
            share = self
                .cpu_share
                .map_or_else(|| unset.clone(), |share| share.to_string()),
            limit = self.limit_millicores.map_or(unset, format_millicores),
            main = self.main_runtime_worker_threads(),
            dedicated = self.dedicated_runtime_worker_threads(),
            partitions = self.target_partitions(),
        )
    }

    /// An INFO note when the *declared* CPU request sits well below the core
    /// count the runtime sized itself for, i.e. when sizing leans on CPU the
    /// scheduler does not guarantee.
    ///
    /// States the discrepancy and which rung produced the number, and stops
    /// there. Both values were chosen by an operator, and which of them is wrong
    /// is their call, so this exists to make the pairing visible to whoever is
    /// debugging — not to claim a mistake, which is why it is not a warning.
    ///
    /// Fires for every source, and names the one responsible: a value read from
    /// an explicit `limits.cpu`, a value configured by hand, and — the case that
    /// motivates this crate — a request with no limit at all, where detection
    /// falls back to every CPU the process may use. An over-large configured value
    /// is just as wrong as an over-large inferred one, so the check is on the
    /// effective core count rather than on any particular rung of the ladder.
    ///
    /// Compares against [`HostReadings::declared_request_millicores`] and never
    /// the cgroup share. A share is not a request: every cgroup carries one, so
    /// driving this from the share told every bare-metal host with more than ~5
    /// cores that its correct budget was misconfigured.
    ///
    /// Reachable only for [`CpuSource::Configured`] and
    /// [`CpuSource::CgroupQuota`]. A declared request otherwise derives the
    /// entitlement itself, and both derived sources are suppressed below.
    ///
    /// `None` when no request was declared, or when the request is at or above
    /// [`REQUEST_SHORTFALL_NUM`]/[`REQUEST_SHORTFALL_DEN`] of the effective core
    /// count.
    #[must_use]
    pub fn request_shortfall_notice(&self) -> Option<String> {
        // `request_burst` is a multiple of the request by construction, and
        // `all_cores` is an operator who already said they want more than it — so
        // neither is sizing for CPU nobody asked for, which is what this warns
        // about. Left unsuppressed, the floor alone would make it fire on the
        // runtime's own default: a `100m` request derives 2 cores, and 200 is
        // below half of 2000.
        if matches!(self.source, CpuSource::RequestBurst | CpuSource::AllCores) {
            return None;
        }
        let request = self.declared_request_millicores?;
        if request.saturating_mul(REQUEST_SHORTFALL_DEN)
            >= self.millicores.saturating_mul(REQUEST_SHORTFALL_NUM)
        {
            return None;
        }
        Some(format!(
            "This pod's CPU request is {request}, below the {effective} sized for (from {origin}). \
             See: {DOCS_URL}",
            request = format_millicores(request),
            effective = format_millicores(self.millicores),
            origin = self.origin(),
        ))
    }

    /// An INFO note when a request-derived entitlement is far below what the host
    /// reports.
    ///
    /// Using 4 cores of a 64-core node is a *choice*: the pod asked for it. This
    /// exists so the choice is apparent to someone who did not realise they had
    /// made it — a `requests.cpu: 100m` pod goes from 64 target partitions to 2,
    /// and the pod it happens to is the one whose logs have rolled by the time
    /// anyone asks why a query is slow. It states what happened and stops; the
    /// knob is in the docs, and recommending `limits.cpu` would trade this for CFS
    /// throttling and scheduling friction.
    ///
    /// Only fires for [`CpuSource::RequestBurst`], which means any explicit
    /// `runtime.cpu.cores` silences it: reaching this rung at all requires that
    /// nothing was configured.
    #[must_use]
    pub fn sizing_notice(&self) -> Option<String> {
        if !matches!(self.source, CpuSource::RequestBurst) {
            return None;
        }
        let request = self.declared_request_millicores?;
        let detected_millicores = u64::try_from(self.detected_cores)
            .unwrap_or(u64::MAX)
            .saturating_mul(1000);
        if self.millicores.saturating_mul(SIZING_NOTICE_DEN)
            >= detected_millicores.saturating_mul(SIZING_NOTICE_NUM)
        {
            return None;
        }
        Some(format!(
            "Sized for {entitlement} from this pod's CPU request of {request} (x{factor}); the host \
             reports {detected} cores. See: {DOCS_URL}",
            entitlement = format_millicores(self.millicores),
            request = format_millicores(request),
            factor = CPU_REQUEST_BURST_FACTOR,
            detected = self.detected_cores,
        ))
    }

    /// A warning when this is a Kubernetes pod whose CPU request never reached
    /// the process.
    ///
    /// Sizing needs the request declared through [`CPU_REQUEST_ENV`], and only
    /// the surface that wrote the pod spec can supply it. When that surface did
    /// not — a hand-rolled manifest, or a chart predating the passthrough — this
    /// process silently sizes for whatever the next rung reports, which on a
    /// burstable pod is the whole machine.
    ///
    /// The cgroup share is what makes the gap detectable without making it
    /// sizeable. Under Kubernetes a share above the no-request floor means the
    /// kubelet was given a request, so its *presence* is the evidence, while its
    /// value stays unused — the conversion back to cores is not trustworthy (see
    /// [`cgroup::parse_cpu_weight`]).
    ///
    /// `None` outside Kubernetes, where a share is just the cgroup default and
    /// implies nothing, and `None` once the request has been declared.
    #[must_use]
    pub fn undeclared_request_warning(&self) -> Option<String> {
        if !self.kubernetes || self.declared_request_millicores.is_some() {
            return None;
        }
        let share = self.cpu_share?;
        Some(format!(
            "This pod's cgroup carries a CPU request ({share}) that spiced cannot read: \
             {CPU_REQUEST_ENV} is unset, so sizing used {entitlement} (from {origin}). \
             See: {DOCS_URL}",
            entitlement = format_millicores(self.millicores),
            origin = self.origin(),
        ))
    }

    /// A watcher seeded with the cgroup share this budget was resolved against.
    ///
    /// Poll it on a timer with a fresh [`detect_cpu_share`] and log whatever it
    /// returns — see [`ShareDriftWatcher`] for why the share rather than the
    /// declared request.
    #[must_use]
    pub fn share_drift_watcher(&self) -> ShareDriftWatcher {
        ShareDriftWatcher {
            entitlement: format_millicores(self.millicores),
            last: std::sync::atomic::AtomicU64::new(encode_share(self.cpu_share)),
        }
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

    /// The cgroup CPU share in millicores, when one can be read.
    ///
    /// Reported so an operator can see it next to the budget; it is never an
    /// input to the detection ladder, and never compared against the budget —
    /// see [`HostReadings::cpu_share`].
    #[must_use]
    pub const fn cpu_share(&self) -> Option<CpuShare> {
        self.cpu_share
    }

    /// The pod's declared `requests.cpu` in millicores, when one was declared
    /// through [`CPU_REQUEST_ENV`].
    #[must_use]
    pub const fn declared_request_millicores(&self) -> Option<u64> {
        self.declared_request_millicores
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
            cpu_share: None,
            kubernetes: false,
            declared_request_millicores: None,
        })
    })
}

/// What `runtime.cpu.cores` (or `SPICE_CPU_CORES`, or `--cpu-cores`) asked for.
///
/// Three states rather than two, because "detect it" and "all of them" are
/// different intents once detection derives the entitlement from the pod's CPU
/// request. `auto` means run the ladder; `all` means run it as though no request
/// were declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuSetting {
    /// `auto`, or unset: resolve through the whole detection ladder.
    Auto,
    /// `all`: every CPU available, with the request rung suppressed. Not a rung
    /// of its own — a cgroup CPU limit still caps it, because sizing past a quota
    /// does not produce CPU, it produces throttling.
    All,
    /// An explicit entitlement in millicores, which short-circuits detection.
    Cores(u64),
}

/// Parse a Kubernetes-style CPU quantity, `auto`, or `all`.
///
/// Accepts `4`, `3.5`, `3500m`, `auto`, and `all`, case-insensitively for the two
/// words. `None` means the value is not any of those, which
/// [`CpuBudget::resolve`] turns into [`Error::InvalidCpuQuantity`]. Zero and
/// negative values are invalid, not silently clamped — `0` is not a spelling of
/// `all`, so a typo cannot resolve to full-node sizing.
#[must_use]
pub fn parse_cpu_quantity(value: &str) -> Option<CpuSetting> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("auto") {
        return Some(CpuSetting::Auto);
    }
    if value.eq_ignore_ascii_case("all") {
        return Some(CpuSetting::All);
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
    (millicores > 0).then_some(CpuSetting::Cores(millicores))
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

    /// An unconstrained Linux host: no quota, no declared request, and the cgroup
    /// CPU share every cgroup has whether or not anyone asked for one. `2536`
    /// is what cgroup v2's default `cpu.weight: 100` inverts to.
    fn bare_metal(affinity_cores: usize) -> HostReadings {
        HostReadings {
            affinity_cores,
            cpu_share: Some(CpuShare::Weight(100)),
            ..HostReadings::default()
        }
    }

    /// A pod with both a CPU request and a CPU limit: the quota rung wins, and
    /// the request is left to be compared against it.
    fn quota_and_request(
        affinity_cores: usize,
        quota_millicores: u64,
        declared_request_millicores: u64,
    ) -> HostReadings {
        HostReadings {
            affinity_cores,
            quota_millicores: Some(quota_millicores),
            declared_request_millicores: Some(declared_request_millicores),
            ..HostReadings::default()
        }
    }

    /// A burstable pod: a *declared* CPU request, no CPU limit.
    fn request_only(affinity_cores: usize, declared_request_millicores: u64) -> HostReadings {
        HostReadings {
            affinity_cores,
            quota_millicores: None,
            cpu_share: None,
            kubernetes: false,
            declared_request_millicores: Some(declared_request_millicores),
        }
    }

    #[test]
    fn cpu_share_parsing_rejects_the_no_request_floors_and_junk() {
        // Raw values through, both flavours.
        assert_eq!(cgroup::parse_cpu_shares("4096"), Some(4096));
        assert_eq!(cgroup::parse_cpu_shares(" 512 \n"), Some(512));
        assert_eq!(cgroup::parse_cpu_weight("157"), Some(157));

        // The floors Kubernetes writes for "no CPU request" read back as absent,
        // rather than as a share of 2 or a weight of 1.
        assert_eq!(cgroup::parse_cpu_shares("2"), None);
        assert_eq!(cgroup::parse_cpu_shares("1"), None);
        assert_eq!(cgroup::parse_cpu_weight("1"), None);

        assert_eq!(cgroup::parse_cpu_shares("garbage"), None);
        assert_eq!(cgroup::parse_cpu_weight("garbage"), None);
    }

    /// The measured reason a share is not a request, and is not converted.
    ///
    /// `docker run` with no CPU flags produces `cpu.weight: 100` and
    /// `cpu.max: max 100000` — a share with no quota (Docker 29.4, cgroup v2,
    /// cgroupfs driver). Every cgroup carries that weight whether or not anyone
    /// expressed a request.
    ///
    /// The same runtime maps `--cpu-shares` 512/1024/2048/4096 to weights
    /// 59/100/174/303. The kubelet's formula would invert those to roughly
    /// 1486m/2536m/4431m/7734m for requests of 500m/1000m/2000m/4000m, i.e. two
    /// to three times too high, so the weight is reported exactly as read.
    #[test]
    fn a_cgroup_share_is_reported_raw_and_never_converted() {
        // Raw, both flavours, with the no-request floors rejected.
        assert_eq!(cgroup::parse_cpu_weight("100"), Some(100));
        assert_eq!(cgroup::parse_cpu_weight("174"), Some(174));
        assert_eq!(cgroup::parse_cpu_weight("1"), None);
        assert_eq!(cgroup::parse_cpu_shares("2048"), Some(2048));
        assert_eq!(cgroup::parse_cpu_shares("2"), None);

        // Rendered as the reading it is, so nobody reads it as an entitlement.
        assert_eq!(CpuShare::Weight(100).to_string(), "weight 100");
        assert_eq!(CpuShare::Shares(2048).to_string(), "2048 shares");
    }

    /// After the request rung exists, this warns for exactly two sources: an
    /// over-large *configured* value, and a CPU limit far above the request. A
    /// declared request with no limit derives its own entitlement instead, so
    /// there is nothing left to warn about there.
    #[test]
    fn a_request_far_below_the_effective_cores_warns() {
        // A hard limit four times the request: sizing leans on CPU the scheduler
        // does not guarantee, and the limit is what chose it.
        let capped =
            CpuBudget::resolve(&CpuConfig::default(), &quota_and_request(64, 16_000, 4000))
                .expect("detection cannot fail")
                .request_shortfall_notice()
                .expect("a request far under the limit must warn");
        assert!(capped.contains("4 cores"), "{capped}");
        assert!(capped.contains("16 cores"), "{capped}");
        assert!(capped.contains("cgroup CPU limit"), "{capped}");

        // An over-large configured value is as wrong as an over-large inferred
        // one, and names the surface that set it.
        let configured = CpuBudget::resolve(
            &CpuConfig::from_sources(None, None, Some("384")),
            &request_only(384, 4000),
        )
        .expect("valid")
        .request_shortfall_notice()
        .expect("384 configured cores against a 4-core request must warn");
        assert!(
            configured.contains(CpuConfig::SPICEPOD_SETTING),
            "{configured}"
        );
        assert!(configured.contains("384 cores"), "{configured}");

        // Never claims the machine's total: a cpuset can pin this process to a
        // subset of it.
        for warning in [&capped, &configured] {
            assert!(!warning.contains("host CPU count"), "{warning}");
        }
    }

    /// The warning states the discrepancy and its source, and stops there — no
    /// remedy, since which of the two numbers is wrong is the operator's call.
    #[test]
    fn the_warning_carries_no_guidance() {
        let warning =
            CpuBudget::resolve(&CpuConfig::default(), &quota_and_request(64, 16_000, 4000))
                .expect("detection cannot fail")
                .request_shortfall_notice()
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
            .request_shortfall_notice()
            .expect("384 configured cores against a 4-core request must warn");
        assert!(warning.contains(CpuConfig::SPICEPOD_SETTING), "{warning}");
        assert!(warning.contains("384 cores"), "{warning}");

        // The CLI surface names itself rather than the spicepod field.
        let via_cli = CpuBudget::resolve(
            &CpuConfig::from_sources(Some("384"), None, None),
            &request_only(18, 4000),
        )
        .expect("valid")
        .request_shortfall_notice()
        .expect("must warn");
        assert!(via_cli.contains(CpuConfig::CLI_SETTING), "{via_cli}");
    }

    #[test]
    fn a_request_close_to_the_effective_cores_is_quiet() {
        // Matching the effective core count: no warning.
        let matched = CpuBudget::resolve(&CpuConfig::default(), &request_only(4, 4000))
            .expect("detection cannot fail");
        assert_eq!(matched.request_shortfall_notice(), None);

        // cgroup v2 quantizes `requests.cpu: 1` to 974m; that must not warn.
        let quantized = CpuBudget::resolve(&CpuConfig::default(), &request_only(1, 974))
            .expect("detection cannot fail");
        assert_eq!(quantized.request_shortfall_notice(), None);

        // Nothing to compare against.
        assert_eq!(budget(16).request_shortfall_notice(), None);
    }

    /// The threshold is *below* half, so exactly half stays quiet and a hair
    /// under it warns. Pinning both sides keeps a later refactor from drifting
    /// the comparison to `<=` and warning on every evenly-halved request.
    #[test]
    fn the_threshold_is_strictly_below_half_the_effective_cores() {
        // A limit of 8 cores; a request of exactly 4 is half, so quiet. Measured
        // against a quota rather than a request-derived budget, because a request
        // with no limit now derives its own entitlement and is suppressed.
        let exactly_half =
            CpuBudget::resolve(&CpuConfig::default(), &quota_and_request(64, 8000, 4000))
                .expect("detection cannot fail");
        assert_eq!(
            exactly_half.request_shortfall_notice(),
            None,
            "a request at exactly half the effective cores must stay quiet"
        );

        // One millicore under half must warn.
        let just_under =
            CpuBudget::resolve(&CpuConfig::default(), &quota_and_request(64, 8000, 3999))
                .expect("detection cannot fail");
        assert!(
            just_under.request_shortfall_notice().is_some(),
            "a request just under half the effective cores must warn"
        );
    }

    /// The request rung: `min(max(2 cores, R x 2), affinity)`.
    #[test]
    fn a_declared_request_derives_a_bounded_burst() {
        // 4-core request on an 18-core host: 8 cores, not 18.
        let burst =
            CpuBudget::resolve(&CpuConfig::default(), &request_only(18, 4000)).expect("valid");
        assert_eq!(burst.millicores(), 8000);
        assert_eq!(burst.cores(), 8);
        assert_eq!(burst.source(), CpuSource::RequestBurst);

        // Clamped by what the process can actually run on: 2 x 16 > 18.
        let clamped =
            CpuBudget::resolve(&CpuConfig::default(), &request_only(18, 16_000)).expect("valid");
        assert_eq!(clamped.cores(), 18, "never above affinity");

        // Floored, so a tiny request still overlaps a scan with something.
        for request in [1_u64, 100, 500, 999] {
            let floored = CpuBudget::resolve(&CpuConfig::default(), &request_only(64, request))
                .expect("valid");
            assert_eq!(
                floored.cores(),
                2,
                "a {request}m request must floor at 2 cores"
            );
            assert!(floored.target_partitions() >= 2);
        }

        // The floor yields to a genuinely smaller host.
        let one_core =
            CpuBudget::resolve(&CpuConfig::default(), &request_only(1, 100)).expect("valid");
        assert_eq!(one_core.cores(), 1, "the floor never invents CPU");
    }

    /// A hard CFS ceiling outranks the request: bursting past a quota produces
    /// throttling, not CPU.
    #[test]
    fn a_quota_outranks_a_declared_request() {
        let both = CpuBudget::resolve(
            &CpuConfig::default(),
            &HostReadings {
                affinity_cores: 64,
                quota_millicores: Some(4000),
                declared_request_millicores: Some(16_000),
                ..HostReadings::default()
            },
        )
        .expect("valid");
        assert_eq!(both.cores(), 4);
        assert_eq!(both.source(), CpuSource::CgroupQuota);
    }

    /// No request declared is the path every benchmark and bare-metal deployment
    /// takes, and it must keep every core.
    #[test]
    fn no_declared_request_keeps_the_whole_host() {
        let bare = CpuBudget::resolve(&CpuConfig::default(), &bare_metal(64)).expect("valid");
        assert_eq!(bare.cores(), 64);
        assert_eq!(bare.source(), CpuSource::Affinity);
    }

    /// `all` suppresses one rung rather than being one. The assertion that makes
    /// that checkable rather than merely stated is *equality* with what the same
    /// host would resolve to with no request declared at all.
    #[test]
    fn all_suppresses_the_request_rung_but_not_a_quota() {
        let all = CpuConfig::from_sources(None, None, Some("all"));

        // With a request and no quota: ignores the request, takes the host.
        let unlimited = CpuBudget::resolve(&all, &request_only(18, 4000)).expect("valid");
        assert_eq!(unlimited.cores(), 18);
        assert_eq!(unlimited.source(), CpuSource::AllCores);
        assert_eq!(
            unlimited.cores(),
            CpuBudget::resolve(&CpuConfig::default(), &host(18))
                .expect("valid")
                .cores(),
            "`all` must resolve as if no request were declared"
        );

        // With a request *and* a quota: the quota, identical to what an unset
        // `cores` produces on the same host.
        let readings = HostReadings {
            affinity_cores: 64,
            quota_millicores: Some(8000),
            declared_request_millicores: Some(1000),
            ..HostReadings::default()
        };
        let limited = CpuBudget::resolve(&all, &readings).expect("valid");
        assert_eq!(limited.cores(), 8, "`all` respects a hard CPU limit");
        assert_eq!(limited.source(), CpuSource::AllCores);
        assert_eq!(
            limited.cores(),
            CpuBudget::resolve(&CpuConfig::default(), &quota(64, 8000))
                .expect("valid")
                .cores(),
            "`all` under a quota must equal the cgroup_quota rung's value"
        );

        // Every surface spells it, and an explicit number still wins outright.
        for cfg in [
            CpuConfig::from_sources(Some("all"), None, None),
            CpuConfig::from_sources(None, Some("all"), None),
        ] {
            assert_eq!(
                CpuBudget::resolve(&cfg, &request_only(18, 4000))
                    .expect("valid")
                    .source(),
                CpuSource::AllCores
            );
        }
        assert_eq!(
            CpuBudget::resolve(
                &CpuConfig::from_sources(Some("6"), None, Some("all")),
                &request_only(18, 4000)
            )
            .expect("valid")
            .cores(),
            6,
            "a number on a higher-precedence surface beats `all`"
        );
    }

    /// `all` is the escape hatch that preserves the behaviour this rung changes:
    /// a pod with `requests.cpu` and no `limits.cpu` sizing for its whole node.
    ///
    /// Pinned against the exact values the pre-rung default produced, so the
    /// opt-out cannot quietly stop being a faithful one. An operator upgrading
    /// into the new default has this available as a one-line revert.
    #[test]
    fn all_preserves_the_pre_change_whole_node_sizing() {
        let burstable = request_only(64, 4000);

        // What this rung now does by default.
        let derived = CpuBudget::resolve(&CpuConfig::default(), &burstable).expect("valid");
        assert_eq!(derived.source(), CpuSource::RequestBurst);
        assert_eq!(derived.cores(), 8);

        // What every release before the rung did, recovered by one setting.
        let preserved = CpuBudget::resolve(
            &CpuConfig::from_sources(None, None, Some("all")),
            &burstable,
        )
        .expect("valid");
        assert_eq!(preserved.cores(), 64, "the node, exactly as before");
        assert_eq!(preserved.target_partitions(), 64);
        assert_eq!(preserved.main_runtime_worker_threads(), 64);
        assert_eq!(preserved.dedicated_runtime_worker_threads(), 63);
        assert_eq!(preserved.source(), CpuSource::AllCores);

        // Every derived quantity matches what the same host produces with no
        // request declared at all — which is what the old behaviour *was*.
        let as_if_undeclared = CpuBudget::resolve(&CpuConfig::default(), &host(64)).expect("valid");
        assert_eq!(
            preserved.derived_sizing(),
            as_if_undeclared.derived_sizing(),
            "`all` must reproduce the pre-rung sizing in full, not just the core count"
        );

        // And it stays silent: an operator who has stated the intent is not told
        // about it on every restart.
        assert_eq!(preserved.sizing_notice(), None);
        assert_eq!(preserved.request_shortfall_notice(), None);
    }

    /// The sizing notice speaks for a pod sized an order of magnitude below its
    /// host and stays quiet otherwise, and any explicit setting silences it by
    /// never reaching the rung.
    #[test]
    fn the_sizing_notice_fires_only_on_a_steep_downsize() {
        let steep =
            CpuBudget::resolve(&CpuConfig::default(), &request_only(64, 100)).expect("valid");
        let notice = steep
            .sizing_notice()
            .expect("2 cores on a 64-core host must be noticed");
        // The three facts needed to understand what happened: what it sized for,
        // what it derived that from, and what the machine actually has.
        assert!(notice.contains("2 cores"), "{notice}");
        assert!(notice.contains("100m"), "{notice}");
        assert!(notice.contains("64 cores"), "{notice}");
        // ...and nothing else. It reports a choice; it does not argue with it, and
        // it never suggests `limits.cpu`, which would trade sizing for throttling.
        for advice in ["set ", "Set ", "should", "limits.cpu", "silence"] {
            assert!(
                !notice.contains(advice),
                "the notice must not advise ({advice:?}): {notice}"
            );
        }
        assert!(notice.len() < 160, "keep it one readable line: {notice}");

        // 8 of 18 cores is within shouting distance: quiet.
        assert_eq!(
            CpuBudget::resolve(&CpuConfig::default(), &request_only(18, 4000))
                .expect("valid")
                .sizing_notice(),
            None
        );

        // The recommended shape on a large node — a 4-core request, no limit —
        // derives exactly an eighth of it. That is the configuration this change
        // tells operators to adopt, so it must not warn about itself.
        let recommended =
            CpuBudget::resolve(&CpuConfig::default(), &request_only(64, 4000)).expect("valid");
        assert_eq!(recommended.cores(), 8);
        assert_eq!(
            recommended.sizing_notice(),
            None,
            "the recommended configuration must not warn on every restart"
        );
        // One millicore less crosses the line.
        assert!(
            CpuBudget::resolve(&CpuConfig::default(), &request_only(64, 3999))
                .expect("valid")
                .sizing_notice()
                .is_some()
        );

        // Any explicit setting silences it, including `all`.
        for value in ["all", "2", "auto"] {
            let cfg = CpuConfig::from_sources(None, None, Some(value));
            let resolved = CpuBudget::resolve(&cfg, &request_only(64, 100)).expect("valid");
            if value == "auto" {
                // `auto` is "detect", so it still reaches the rung and still speaks.
                assert!(resolved.sizing_notice().is_some(), "auto still derives");
            } else {
                assert_eq!(resolved.sizing_notice(), None, "`{value}` must silence it");
            }
        }

        // Rungs that are not request-derived never emit it.
        assert_eq!(budget(64).sizing_notice(), None);
        assert_eq!(
            CpuBudget::resolve(&CpuConfig::default(), &quota(64, 1000))
                .expect("valid")
                .sizing_notice(),
            None
        );
    }

    /// The shortfall warning must not fire on the runtime's own default. Both
    /// request-derived sources are headroom by construction, and the 2-core floor
    /// would otherwise trip the 1/2 threshold for any request below 1 core.
    #[test]
    fn the_shortfall_warning_is_suppressed_for_derived_sources() {
        let floored =
            CpuBudget::resolve(&CpuConfig::default(), &request_only(64, 100)).expect("valid");
        assert_eq!(floored.source(), CpuSource::RequestBurst);
        assert_eq!(
            floored.request_shortfall_notice(),
            None,
            "a 100m request derives 2 cores; 200 is below half of 2000, so only the \
             source check keeps this quiet"
        );

        let all = CpuBudget::resolve(
            &CpuConfig::from_sources(None, None, Some("all")),
            &request_only(64, 100),
        )
        .expect("valid");
        assert_eq!(all.source(), CpuSource::AllCores);
        assert_eq!(all.request_shortfall_notice(), None);

        // Still fires where sizing genuinely leans on CPU nobody asked for.
        assert!(
            CpuBudget::resolve(
                &CpuConfig::from_sources(None, None, Some("384")),
                &request_only(18, 4000)
            )
            .expect("valid")
            .request_shortfall_notice()
            .is_some(),
            "an over-large configured value must still warn"
        );
    }

    /// Every source has a stable metric label and a human origin.
    #[test]
    fn new_sources_are_labelled() {
        assert_eq!(CpuSource::RequestBurst.as_str(), "request_burst");
        assert_eq!(CpuSource::AllCores.as_str(), "all_cores");
    }

    /// Drift speaks once per change, stays silent otherwise, and never resizes
    /// anything.
    #[test]
    fn share_drift_is_reported_once_per_change() {
        let budget = CpuBudget::resolve(
            &CpuConfig::default(),
            &HostReadings {
                affinity_cores: 64,
                cpu_share: Some(CpuShare::Weight(174)),
                declared_request_millicores: Some(4000),
                kubernetes: true,
                ..HostReadings::default()
            },
        )
        .expect("valid");
        let watcher = budget.share_drift_watcher();

        // The reading it was seeded with is not a change.
        assert_eq!(watcher.observe(Some(CpuShare::Weight(174))), None);
        assert_eq!(watcher.observe(Some(CpuShare::Weight(174))), None);

        // A resize moves it, and the message names both readings plus the size
        // still in force.
        let drift = watcher
            .observe(Some(CpuShare::Weight(303)))
            .expect("a moved share must be reported");
        assert!(drift.contains("weight 174"), "{drift}");
        assert!(drift.contains("weight 303"), "{drift}");
        assert!(
            drift.contains("8 cores"),
            "still sized for the old value: {drift}"
        );
        assert!(drift.contains("Restart"), "{drift}");
        // Never converts the share to cores, and never claims a new entitlement.
        assert!(!drift.contains("2.5"), "{drift}");

        // Once, not on every poll.
        assert_eq!(watcher.observe(Some(CpuShare::Weight(303))), None);
        assert_eq!(watcher.observe(Some(CpuShare::Weight(303))), None);

        // A further change is new information.
        assert!(watcher.observe(Some(CpuShare::Weight(59))).is_some());

        // Disappearing and reappearing are both changes, rendered as "unset".
        let gone = watcher.observe(None).expect("a vanished share is a change");
        assert!(gone.contains("to unset"), "{gone}");
        let back = watcher
            .observe(Some(CpuShare::Shares(2048)))
            .expect("a returning share is a change");
        assert!(back.contains("from unset"), "{back}");
        assert!(back.contains("2048 shares"), "{back}");

        // The budget itself is untouched — the watcher cannot resize anything, so
        // `AlreadyInstalled` is unreachable from here.
        assert_eq!(budget.cores(), 8);
        assert_eq!(budget.source(), CpuSource::RequestBurst);
    }

    /// The encoding is only sound if it round-trips every reading the kernel can
    /// produce, including the extremes of both ranges.
    #[test]
    fn share_encoding_round_trips() {
        for share in [
            None,
            Some(CpuShare::Weight(1)),
            Some(CpuShare::Weight(100)),
            Some(CpuShare::Weight(10_000)),
            Some(CpuShare::Shares(2)),
            Some(CpuShare::Shares(1024)),
            Some(CpuShare::Shares(262_144)),
        ] {
            let described = describe_share(encode_share(share));
            let expected = share.map_or_else(|| "unset".to_string(), |s| s.to_string());
            assert_eq!(described, expected, "{share:?} must survive encoding");
        }

        // A weight and a share of the same number must not collide.
        assert_ne!(
            encode_share(Some(CpuShare::Weight(100))),
            encode_share(Some(CpuShare::Shares(100)))
        );
    }

    #[test]
    fn max_concurrent_queries_scales_with_the_budget() {
        assert_eq!(budget(1).max_concurrent_queries(), 4);
        assert_eq!(budget(16).max_concurrent_queries(), 64);
    }

    #[test]
    fn quantities() {
        use CpuSetting::{All, Auto, Cores};

        assert_eq!(parse_cpu_quantity("4"), Some(Cores(4000)));
        assert_eq!(parse_cpu_quantity("3.5"), Some(Cores(3500)));
        assert_eq!(parse_cpu_quantity("3500m"), Some(Cores(3500)));
        assert_eq!(parse_cpu_quantity(" 500m "), Some(Cores(500)));
        assert_eq!(parse_cpu_quantity("auto"), Some(Auto));
        assert_eq!(parse_cpu_quantity("AUTO"), Some(Auto));
        assert_eq!(parse_cpu_quantity(""), Some(Auto));

        // `all` is a named sentinel like `auto`, and case-insensitive like it.
        assert_eq!(parse_cpu_quantity("all"), Some(All));
        assert_eq!(parse_cpu_quantity("ALL"), Some(All));
        assert_eq!(parse_cpu_quantity(" All "), Some(All));

        // `0` stays an error rather than becoming a second spelling of `all`, so
        // no typo resolves to full-node sizing.
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

    /// A pod with `requests.cpu` and no `limits.cpu` is entitled to burst across
    /// its node, so it is sized for a bounded multiple of the request rather than
    /// pinned to it — and the two ways of asking for the whole node are still
    /// there, one explicit and one a number.
    #[test]
    fn a_request_without_a_limit_derives_a_burst_and_can_opt_out() {
        // requests.cpu set, limits.cpu unset.
        let burstable = request_only(64, 4000);
        let budget = CpuBudget::resolve(&CpuConfig::default(), &burstable).expect("valid");
        assert_eq!(budget.source(), CpuSource::RequestBurst);
        assert_eq!(
            budget.cores(),
            8,
            "bursts above the request, not to the node"
        );
        assert_eq!(budget.target_partitions(), 8);

        // The whole node is still reachable, stated once.
        let everything = CpuBudget::resolve(
            &CpuConfig::from_sources(None, None, Some("all")),
            &burstable,
        )
        .expect("valid");
        assert_eq!(everything.source(), CpuSource::AllCores);
        assert_eq!(everything.cores(), 64);

        // ...and an operator who wants a specific number says so.
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
        // The case that had no fixture: a share is present, no request was
        // declared. The summary must show them as the separate claims they are.
        insta::assert_snapshot!(
            "summary_bare_metal_share",
            CpuBudget::resolve(&CpuConfig::default(), &bare_metal(18))
                .expect("valid")
                .summary_line()
        );
        insta::assert_snapshot!(
            "summary_declared_request",
            CpuBudget::resolve(&CpuConfig::default(), &request_only(18, 4000))
                .expect("valid")
                .summary_line()
        );
    }

    /// The guard that keeps an inferred value out of the reporting path: a cgroup
    /// CPU share is never a request.
    ///
    /// A plain `docker run` reports `cpu.weight: 100`, which the kubelet's
    /// formula inverts to ~2536m. Feeding that to the shortfall warning told
    /// every unconstrained Linux host above ~5 cores that its correct budget was
    /// misconfigured. The second half of this test declares that same 2536m to
    /// prove the warning still fires on a real request, so this fails if the
    /// share is ever wired back in.
    #[test]
    fn a_cgroup_share_is_never_treated_as_a_declared_request() {
        let resolved = CpuBudget::resolve(&CpuConfig::default(), &bare_metal(18)).expect("valid");
        assert_eq!(
            resolved.request_shortfall_notice(),
            None,
            "a cgroup share must never produce a shortfall warning"
        );
        // Dropped as an input, not as output: still reported, still raw.
        assert_eq!(resolved.cpu_share(), Some(CpuShare::Weight(100)));
        assert_eq!(resolved.declared_request_millicores(), None);

        // The share left sizing alone entirely: the whole host, from detection.
        assert_eq!(resolved.cores(), 18);
        assert_eq!(resolved.source(), CpuSource::Affinity);

        // The same number *declared* moves the entitlement, which is the sharpest
        // form of the guard: had the share been wired in, a plain `docker run`
        // would have sized itself for a request nobody made.
        let declared =
            CpuBudget::resolve(&CpuConfig::default(), &request_only(18, 2536)).expect("valid");
        assert_eq!(declared.source(), CpuSource::RequestBurst);
        assert_eq!(declared.cores(), 6, "2536m x2 = 5072m, rounded up");
        assert_ne!(
            declared.cores(),
            resolved.cores(),
            "if these ever agree, this test has stopped proving anything"
        );
    }

    /// Under Kubernetes a share above the no-request floor means the kubelet was
    /// given a request, so a missing [`CPU_REQUEST_ENV`] is a wiring gap worth
    /// naming — the diagnostic a hand-rolled manifest would otherwise never get.
    #[test]
    fn a_kubernetes_pod_whose_request_never_arrived_is_warned_about() {
        let unwired = HostReadings {
            affinity_cores: 18,
            quota_millicores: None,
            cpu_share: Some(CpuShare::Weight(174)),
            declared_request_millicores: None,
            kubernetes: true,
        };
        let warning = CpuBudget::resolve(&CpuConfig::default(), &unwired)
            .expect("valid")
            .undeclared_request_warning()
            .expect("a Kubernetes pod with a share but no declared request must warn");
        assert!(warning.contains(CPU_REQUEST_ENV), "{warning}");
        assert!(warning.contains("weight 174"), "{warning}");
        // It says what happened, and never converts the share to cores.
        assert!(warning.contains("18 cores"), "{warning}");

        // Wired up: nothing to say.
        let wired = HostReadings {
            declared_request_millicores: Some(4000),
            ..unwired
        };
        assert_eq!(
            CpuBudget::resolve(&CpuConfig::default(), &wired)
                .expect("valid")
                .undeclared_request_warning(),
            None
        );

        // Not Kubernetes: the same share is just the cgroup default and implies
        // nothing, so bare metal and plain Docker stay silent.
        assert_eq!(
            CpuBudget::resolve(&CpuConfig::default(), &bare_metal(18))
                .expect("valid")
                .undeclared_request_warning(),
            None,
            "a share outside Kubernetes is not evidence of a request"
        );

        // Kubernetes, but the pod genuinely declared no request: the kubelet
        // writes the floor, which reads back as absent.
        let no_request = HostReadings {
            cpu_share: None,
            ..unwired
        };
        assert_eq!(
            CpuBudget::resolve(&CpuConfig::default(), &no_request)
                .expect("valid")
                .undeclared_request_warning(),
            None
        );
    }

    /// [`CPU_REQUEST_ENV`] is millicores (`divisor: 1m`), not the core-denominated
    /// grammar `runtime.cpu.cores` accepts. The two disagree on every bare
    /// integer, so a core-shaped value must be rejected rather than read 1000x
    /// too small.
    #[test]
    fn declared_request_parses_millicores_and_rejects_core_quantities() {
        // What the downward API actually sends.
        assert_eq!(parse_declared_request_millicores("4000"), Some(4000));
        assert_eq!(parse_declared_request_millicores("100"), Some(100));
        assert_eq!(parse_declared_request_millicores(" 250 "), Some(250));
        // An explicit unit is accepted and means the same thing.
        assert_eq!(parse_declared_request_millicores("3500m"), Some(3500));

        // Core-shaped, and therefore a surface that dropped `divisor: 1m`.
        // Reading `3.5` as 3 millicores would under-size by 1000x.
        assert_eq!(parse_declared_request_millicores("3.5"), None);
        assert_eq!(parse_declared_request_millicores("0.1"), None);

        // Not a quantity at all.
        assert_eq!(parse_declared_request_millicores(""), None);
        assert_eq!(parse_declared_request_millicores("  "), None);
        assert_eq!(parse_declared_request_millicores("abc"), None);
        assert_eq!(parse_declared_request_millicores("-1"), None);
        assert_eq!(parse_declared_request_millicores("1e3"), None);
        // Zero cores is not a request; it reads as absent, not as "all".
        assert_eq!(parse_declared_request_millicores("0"), None);
        assert_eq!(parse_declared_request_millicores("0m"), None);
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
