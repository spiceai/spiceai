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
///
/// `all` is the exception: it says the surface imposes no ceiling of its own, so
/// it defers to a quantity named below it. That is what lets a platform set
/// `SPICE_CPU_CORES=all` on every deployment without silencing an operator who
/// wrote `runtime.cpu.cores: 4`.
#[derive(Debug, Clone, Default)]
pub struct CpuConfig {
    /// Every surface that was set, in precedence order. All of them are kept
    /// rather than just the winner, because `all` defers to a quantity named
    /// below it — see [`CpuBudget::resolve`].
    surfaces: Vec<(String, &'static str)>,
}

impl CpuConfig {
    pub const CLI_SETTING: &str = "--cpu-cores";
    pub const ENV_SETTING: &str = "SPICE_CPU_CORES";
    pub const SPICEPOD_SETTING: &str = "runtime.cpu.cores";

    /// Resolve the three configuration surfaces in precedence order.
    #[must_use]
    pub fn from_sources(cli: Option<&str>, env: Option<&str>, spicepod: Option<&str>) -> Self {
        let surfaces = [
            (cli, Self::CLI_SETTING),
            (env, Self::ENV_SETTING),
            (spicepod, Self::SPICEPOD_SETTING),
        ]
        .into_iter()
        .filter_map(|(value, setting)| value.map(|value| (value.to_string(), setting)))
        .collect();
        Self { surfaces }
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
/// `divisor: 1` would have avoided that by sending whole cores, but it rounds up,
/// and this value is *reported*: a `100m` request would arrive as `1` and be
/// logged as one core, misstating by 10x the exact number an operator compares
/// against the budget.
///
/// The name is the whole guard, because parsing cannot be. `4` is a legal value
/// under both readings — four millicores with the divisor, four cores without it —
/// so [`parse_declared_request_millicores`] rejects only what cannot be a
/// millicore count at all (decimals, signs). A surface that omits the divisor
/// therefore under-states the request by 1000x and is not detectable from the
/// value alone; what bounds the damage is the 2-core floor
/// ([`REQUEST_DERIVED_FLOOR_MILLICORES`]), which is also why the floor is not
/// merely a nicety.
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
/// Deliberately *not* [`parse_cpu_quantity`]. That grammar reads a bare number as
/// cores, which is the opposite of what `divisor: 1m` produces, so sharing it
/// would read a correctly-wired `requests.cpu: 4` (`4000`) as 4000 cores.
///
/// Rejects what cannot be a whole number of millicores — a decimal like `3.5`, a
/// sign, anything non-numeric — rather than everything that *might* have been
/// written in cores. A bare integer cannot be told apart: `4` is four millicores
/// with the divisor and four cores without it, and both are legal. See
/// [`CPU_REQUEST_ENV`] for why that puts the unit in the variable's name.
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

/// Below this, a declared CPU request is more likely a whole-core value that lost
/// its unit than a real request.
///
/// A deployment surface that writes `resourceFieldRef` without `divisor: 1m` sends
/// whole cores, so `requests.cpu: 1` through `9` arrive as `1` through `9` and are
/// read as single-digit *millicores* — the request under-stated by 1000x. Nothing
/// in the value says which was meant (see [`CPU_REQUEST_ENV`]), but Kubernetes'
/// own floor is `1m` and nothing runs `spiced` in single-digit millicores, so the
/// range is worth remarking on. Ten leaves the smallest requests anyone actually
/// writes — `50m`, `100m` — well clear.
const SUSPECT_CORE_SHAPED_MILLICORES: u64 = 10;

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
        let affinity_millicores = cores_to_millicores(detected_cores);

        // Precedence is CLI > env > spicepod, with one exception. `all` says the
        // surface imposes no ceiling of its own, so it defers to an explicit
        // quantity named below it: a platform that sets `SPICE_CPU_CORES=all` on
        // every deployment must not thereby ignore an operator who wrote
        // `runtime.cpu.cores: 4` in their spicepod. It does not defer to `auto`,
        // which is itself an instruction — "detect it" — rather than the absence of
        // one.
        //
        // Only surfaces that are consulted get parsed, so an invalid value below a
        // winning quantity stays ignored exactly as before.
        let mut deferred_all: Option<&'static str> = None;
        let mut configured: Option<(CpuSetting, &'static str)> = None;
        for (value, setting) in &cfg.surfaces {
            let parsed = parse_cpu_quantity(value).context(InvalidCpuQuantitySnafu {
                setting: (*setting).to_string(),
                value: value.clone(),
            })?;
            match parsed {
                CpuSetting::Cores(millicores) => {
                    configured = Some((CpuSetting::Cores(millicores), *setting));
                    break;
                }
                CpuSetting::All => deferred_all = deferred_all.or(Some(*setting)),
                // Blocks the surfaces below it, unless we are already scanning past
                // an `all` for a quantity — "detect it" names no ceiling either.
                CpuSetting::Auto => {
                    if deferred_all.is_none() {
                        configured = Some((CpuSetting::Auto, *setting));
                        break;
                    }
                }
            }
        }
        let configured =
            configured.or_else(|| deferred_all.map(|setting| (CpuSetting::All, setting)));

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
        // Only faults are reported beyond the two lines above. Anything that is a
        // consequence of a choice — which entitlement the request derived, how it
        // compares to the host — is already in the summary, which names the rung, the
        // multiplier, the request, the share and the limit; a notice restating it
        // added a second line and no fact.
        if let Some(warning) = self.undeclared_request_warning() {
            tracing::warn!("{warning}");
        }
        if let Some(warning) = self.core_shaped_request_warning() {
            tracing::warn!("{warning}");
        }
        if let Some(warning) = self.configured_above_ceiling_warning() {
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
            ("scan_split_concurrency", self.scan_split_concurrency()),
            ("vortex_parallelism", self.vortex_parallelism()),
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

    /// A warning when the declared CPU request looks like whole cores that lost
    /// their unit.
    ///
    /// The 1000x failure this cannot rule out by parsing: a surface that omits
    /// `divisor: 1m` sends `4` for a four-core request, which is a legal reading as
    /// four millicores. When the request rung wins, the 2-core floor keeps that from
    /// starving the runtime, but silently: on a small host the summary line reads
    /// unremarkably — two cores of four looks like an ordinary entitlement — so
    /// without this nothing names the cause. Fires whichever rung won, because a
    /// request that lost to a quota today is still wrong, and reports the resolved
    /// entitlement rather than calling it a minimum.
    ///
    /// Phrased as a conditional, because `requests.cpu: 4m` is legal and would land
    /// here too. It names the whole-core reading so an operator who wrote that can
    /// recognise their own value.
    ///
    /// `None` when no request was declared, or when it is large enough to be
    /// unambiguous.
    #[must_use]
    pub fn core_shaped_request_warning(&self) -> Option<String> {
        let request = self.declared_request_millicores?;
        if request >= SUSPECT_CORE_SHAPED_MILLICORES {
            return None;
        }
        Some(format!(
            "Declared CPU request of {request_m} seems small; if this pod requests \
             {request_as_cores} cores, {CPU_REQUEST_ENV} is missing `divisor: 1m` and is 1000x too \
             small. Sized for {entitlement}. See: {DOCS_URL}",
            request_m = format_millicores(request),
            request_as_cores = request,
            // The resolved value, not "the minimum": this fires on the declared
            // request whichever rung won, and a quota or an explicit setting can
            // outrank it. The summary line above names which one did.
            entitlement = format_millicores(self.millicores),
        ))
    }

    /// A warning when an explicit `cores` value exceeds the CPU this process will
    /// actually be given.
    ///
    /// Every other rung resolves through `min(reading, affinity)`, so the explicit
    /// override is the only one that can name a quantity the host will not honour.
    /// It stays unclamped on purpose: an operator may be sizing for a ceiling they
    /// are about to raise, and silently shrinking a value someone wrote is its own
    /// surprise. Both readings are already in the summary line, though, so leaving
    /// the gap between them unremarked means a throttled deployment whose own logs
    /// never name the cause.
    ///
    /// Names the tightest ceiling, since that is the one the sizing has to clear.
    /// Which of the two that is cannot be read off `detected_cores` alone.
    /// `available_parallelism` already returns `min(quota, affinity)` floored to
    /// whole cores, so under a fractional quota it under-reports the real ceiling by
    /// up to a core — a `limits.cpu: 2500m` pod reports 2 — and treating that as an
    /// exact affinity reading would warn an operator who configured exactly their
    /// quota, and send them after an affinity limit that does not exist. A quota can
    /// only be the binding constraint while it is under the next whole core, because
    /// otherwise flooring it would not have produced this reading; above that,
    /// affinity is provably what got floored and is exact, since a CPU mask is always
    /// a whole number of CPUs.
    ///
    /// `None` unless an explicit quantity won this budget — `all` and `auto` name
    /// no quantity of their own, and both already resolve through the clamp — and
    /// `None` while that quantity sits at or below every ceiling.
    #[must_use]
    pub fn configured_above_ceiling_warning(&self) -> Option<String> {
        if !matches!(self.source, CpuSource::Configured) {
            return None;
        }
        let affinity_millicores = cores_to_millicores(self.detected_cores);
        // Under the next whole core, the quota is the wall that is either binding or
        // indistinguishable from it — and exact where the floored reading is not.
        let binding_quota = self
            .limit_millicores
            .filter(|limit| *limit < affinity_millicores.saturating_add(1000));
        let ceiling = binding_quota.unwrap_or(affinity_millicores);
        if self.millicores <= ceiling {
            return None;
        }
        let (ceiling_phrase, consequence, remedy) = match binding_quota {
            Some(limit) => (
                format!(
                    "this container's cgroup CPU limit of {}",
                    format_millicores(limit)
                ),
                "the container is CFS-throttled under load",
                "raise the container's CPU limit",
            ),
            None => (
                format!(
                    "the {} available to this process",
                    format_millicores(affinity_millicores)
                ),
                "its threads contend for those CPUs",
                "widen this process's CPU affinity",
            ),
        };
        Some(format!(
            "`{origin}` is set to {entitlement}, above {ceiling_phrase}, so every pool is sized \
             for {entitlement} and {consequence} instead of being given the extra CPU. \
             Lower it to {ceiling_value} or {remedy}. See: {DOCS_URL}",
            origin = self.origin(),
            entitlement = format_millicores(self.millicores),
            ceiling_value = format_millicores(ceiling),
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
    /// implies nothing; `None` once the request has been declared; and `None`
    /// whenever another rung won, because the request would have been outranked and
    /// wiring it would not have moved the entitlement.
    #[must_use]
    pub fn undeclared_request_warning(&self) -> Option<String> {
        if !self.kubernetes || self.declared_request_millicores.is_some() {
            return None;
        }
        // Only when sizing actually fell through to the machine. Every other rung
        // outranks the request rung, so wiring the request would not have changed
        // the entitlement: a cgroup limit wins outright, an explicit
        // `runtime.cpu.cores` short-circuits detection, and `all` suppresses the
        // request rung on purpose. Warning there tells an operator to fix something
        // that is not broken — and `all` in particular is a deployment stating this
        // intent deliberately, which is how the Spice Cloud platform expresses an
        // unlimited-CPU plan.
        if !matches!(self.source, CpuSource::Affinity) {
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

    /// The cgroup CPU share as the kernel reports it — a `cpu.weight` or a
    /// `cpu.shares`, never converted to cores — when one can be read.
    ///
    /// Reported so an operator can see it next to the budget; it is never an input
    /// to the detection ladder, and its value is never interpreted — see
    /// [`HostReadings::cpu_share`] and [`CpuShare`].
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

    /// Ceiling on the splits ONE Vortex file scan decodes concurrently when the
    /// count is derived rather than configured (`scan_concurrency: auto`).
    ///
    /// The derivation divides the query fan-out across the files a scan plans, so
    /// a table held in few files concentrates the whole fan-out inside a single
    /// file scan. That fan-out follows [`Self::target_partitions`] only while it
    /// is unset — an explicit `runtime.query.target_partitions` above the
    /// entitlement would otherwise carry straight into the number of decodes a
    /// single scan runs at once. Past the entitlement those decodes cannot run in
    /// parallel anyway; they only add resident decoded batches, which the scan
    /// charges to the query memory pool. So the derived count stops here.
    ///
    /// An explicit per-table `scan_concurrency` is an operator override and is NOT
    /// clamped, matching how every other explicitly-set knob outranks its derived
    /// default.
    #[must_use]
    pub const fn scan_split_concurrency(&self) -> usize {
        self.cores
    }

    /// The parallelism spiced declares to Vortex at startup
    /// (`vortex_utils::parallelism::set_available_parallelism`).
    ///
    /// Vortex sizes its remaining concurrency defaults — encode fan-out and
    /// per-worker scan lookahead — from that declaration; undeclared, it reads
    /// the machine's core count instead of the entitlement. This is a default
    /// Vortex derives fan-outs from, not an enforced ceiling.
    #[must_use]
    pub const fn vortex_parallelism(&self) -> usize {
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

/// Whole CPUs as millicores.
///
/// Saturating rather than wrapping: a core count that overflows `u64` millicores
/// is not a machine, and clamping keeps it a ceiling rather than turning it into
/// a very low one.
fn cores_to_millicores(cores: usize) -> u64 {
    u64::try_from(cores)
        .unwrap_or(u64::MAX)
        .saturating_mul(1000)
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
    /// CPU share every cgroup has whether or not anyone asked for one. `100` is
    /// cgroup v2's default `cpu.weight`, measured from a `docker run` with no CPU
    /// flags, and is carried raw — nothing converts it to cores.
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

    /// The 1000x case parsing cannot catch, and the one host size where nothing
    /// else would mention it.
    #[test]
    fn a_core_shaped_request_is_remarked_on() {
        // Four cores requested, divisor dropped, so it arrives as four millicores.
        let mistyped =
            CpuBudget::resolve(&CpuConfig::default(), &request_only(4, 4)).expect("valid");
        let warning = mistyped
            .core_shaped_request_warning()
            .expect("4m on a 4-core host must be remarked on");
        assert!(warning.contains("4m"), "{warning}");
        assert!(
            warning.contains("4 cores"),
            "names the whole-core reading: {warning}"
        );
        assert!(warning.contains(CPU_REQUEST_ENV), "{warning}");
        assert!(
            warning.contains("divisor"),
            "names the likely cause: {warning}"
        );
        assert!(warning.contains("Sized for 2 cores"), "{warning}");

        // A genuinely 1-core host gets 1, because the floor yields to a smaller
        // machine — so the "minimum" wording has to track the value, not the const.
        let tiny = CpuBudget::resolve(&CpuConfig::default(), &request_only(1, 4)).expect("valid");
        assert_eq!(tiny.cores(), 1);
        let on_tiny = tiny
            .core_shaped_request_warning()
            .expect("still remarked on");
        assert!(on_tiny.contains("Sized for 1 core"), "{on_tiny}");

        // This is the host size that made the warning necessary. The floor caught the
        // mistake, but two cores of four is an unremarkable-looking entitlement, so a
        // reader of the summary line has nothing to notice; this names the cause.
        assert_eq!(mistyped.cores(), 2, "the floor caught it");
        assert_eq!(mistyped.detected_cores(), 4);

        // Requests anyone actually writes are unambiguous and silent.
        for request in [10_u64, 50, 100, 500, 1000, 4000] {
            assert_eq!(
                CpuBudget::resolve(&CpuConfig::default(), &request_only(64, request))
                    .expect("valid")
                    .core_shaped_request_warning(),
                None,
                "{request}m must not be remarked on"
            );
        }

        // Nothing declared, nothing to say.
        assert_eq!(budget(64).core_shaped_request_warning(), None);

        // It fires on the value regardless of which rung won, because the declared
        // value is suspect either way — and it reports the resolved entitlement
        // rather than calling it a minimum, which would have described a quota's
        // 8 cores as a floor.
        let under_quota =
            CpuBudget::resolve(&CpuConfig::default(), &quota_and_request(64, 8000, 4))
                .expect("valid")
                .core_shaped_request_warning()
                .expect("a suspect request is worth naming under a quota too");
        assert!(under_quota.contains("Sized for 8 cores"), "{under_quota}");
        assert!(!under_quota.contains("minimum"), "{under_quota}");
    }

    /// The explicit override is the one rung that can name more CPU than a cgroup
    /// quota will hand over, so the mismatch has to be said out loud.
    ///
    /// Regression test for #13275.
    #[test]
    fn a_configured_value_above_the_cgroup_quota_is_remarked_on() {
        let spicepod = |cores: &str| CpuConfig::from_sources(None, None, Some(cores));

        // The reported deployment: `limits.cpu: 2` with `runtime.cpu.cores: 6`. Both
        // readings are 2 here, and the quota is named because it is the wall that
        // throttles.
        let throttled = CpuBudget::resolve(&spicepod("6"), &quota(2, 2000)).expect("valid");
        let warning = throttled
            .configured_above_ceiling_warning()
            .expect("6 configured cores under a 2-core quota must be remarked on");
        assert!(
            warning.contains("`runtime.cpu.cores` is set to 6 cores"),
            "names the setting and its value: {warning}"
        );
        assert!(
            warning.contains("cgroup CPU limit of 2 cores"),
            "names the ceiling it exceeds: {warning}"
        );
        assert!(
            warning.contains("CFS-throttled"),
            "names what the operator will observe: {warning}"
        );
        assert!(
            warning.contains("Lower it to 2 cores"),
            "gives an actionable fix: {warning}"
        );
        assert!(
            warning.contains("instead of being given the extra CPU"),
            "names what the extra configured CPU does not buy: {warning}"
        );
        assert!(warning.contains(DOCS_URL), "{warning}");

        // Warn-only: the configured value is still honoured, so an operator sizing
        // for a limit they are about to raise is not silently shrunk.
        assert_eq!(throttled.cores(), 6, "the value must not be clamped");
        assert_eq!(throttled.source(), CpuSource::Configured);

        // The ordinary Kubernetes shape: the quota is narrow, affinity is the whole
        // node, so the quota is what the sizing has to clear.
        let on_a_big_node = CpuBudget::resolve(&spicepod("6"), &quota(64, 2000))
            .expect("valid")
            .configured_above_ceiling_warning()
            .expect("a narrow quota on a wide node must still be named");
        assert!(
            on_a_big_node.contains("cgroup CPU limit of 2 cores"),
            "{on_a_big_node}"
        );
    }

    /// The second door, which no quota reading covers: more configured cores than
    /// the process may run on at all.
    ///
    /// Regression test for #13275.
    #[test]
    fn a_configured_value_above_the_available_cpus_is_remarked_on() {
        let spicepod = |cores: &str| CpuConfig::from_sources(None, None, Some(cores));

        // A bare-metal or `taskset` deployment: no quota exists to compare against.
        let oversubscribed = CpuBudget::resolve(&spicepod("6"), &bare_metal(2))
            .expect("valid")
            .configured_above_ceiling_warning()
            .expect("6 configured cores on a 2-CPU host must be remarked on");
        assert!(
            oversubscribed.contains("the 2 cores available to this process"),
            "names affinity rather than a quota nothing set: {oversubscribed}"
        );
        assert!(
            oversubscribed.contains("threads contend"),
            "names the contention, not throttling: {oversubscribed}"
        );
        assert!(
            !oversubscribed.contains("cgroup"),
            "no quota to name here: {oversubscribed}"
        );

        // A quota wider than affinity cannot be used, so affinity is the tightest
        // ceiling and the one worth naming — `docker run --cpus=100` on 16 CPUs.
        let wide_quota = CpuBudget::resolve(&spicepod("32"), &quota(16, 100_000))
            .expect("valid")
            .configured_above_ceiling_warning()
            .expect("affinity is the real ceiling when the quota exceeds it");
        assert!(
            wide_quota.contains("the 16 cores available to this process"),
            "{wide_quota}"
        );
    }

    /// Nothing to say while the sizing fits, so an ordinary deployment gains no
    /// warning it would learn to ignore.
    #[test]
    fn a_configured_value_within_every_ceiling_stays_silent() {
        let spicepod = |cores: &str| CpuConfig::from_sources(None, None, Some(cores));

        for (cores, host) in [
            ("2", quota(2, 2000)),
            ("1", quota(2, 2000)),
            ("2", quota(64, 2000)),
            ("16", bare_metal(16)),
            ("3.5", quota(64, 4000)),
        ] {
            assert_eq!(
                CpuBudget::resolve(&spicepod(cores), &host)
                    .expect("valid")
                    .configured_above_ceiling_warning(),
                None,
                "{cores} within the ceiling must stay silent"
            );
        }

        // The settings that name no quantity resolve through the clamp already, so
        // their value is the ceiling by construction.
        for setting in ["all", "auto"] {
            let resolved = CpuBudget::resolve(&spicepod(setting), &quota(64, 2000)).expect("valid");
            assert_eq!(resolved.cores(), 2, "{setting} must clamp to the quota");
            assert_eq!(
                resolved.configured_above_ceiling_warning(),
                None,
                "{setting} names no quantity to exceed a ceiling"
            );
        }

        // And nothing to say when no override was written at all.
        assert_eq!(budget(64).configured_above_ceiling_warning(), None);
        assert_eq!(
            CpuBudget::resolve(&CpuConfig::default(), &quota(64, 2000))
                .expect("valid")
                .configured_above_ceiling_warning(),
            None
        );
    }

    /// A fractional quota is the reading `detected_cores` cannot express, so the
    /// ceiling has to come from the quota itself.
    ///
    /// `available_parallelism` floors the quota it has already applied, so a
    /// `limits.cpu: 2500m` pod on a wide node reports 2 available cores. Comparing
    /// against that floored value warns an operator who configured exactly their
    /// quota, and sends them after an affinity limit that does not exist.
    ///
    /// Regression test for #13275.
    #[test]
    fn a_fractional_quota_is_named_exactly_rather_than_floored() {
        let spicepod = |cores: &str| CpuConfig::from_sources(None, None, Some(cores));
        let fractional = quota(2, 2500);
        assert_eq!(
            CpuBudget::resolve(&CpuConfig::default(), &fractional)
                .expect("valid")
                .detected_cores(),
            2,
            "the premise: 2500m floors to a 2-core reading"
        );

        // Anything up to the real quota is within the entitlement, including the
        // 500m the floored reading cannot see.
        for cores in ["2", "2.1", "2.5", "2500m"] {
            assert_eq!(
                CpuBudget::resolve(&spicepod(cores), &fractional)
                    .expect("valid")
                    .configured_above_ceiling_warning(),
                None,
                "{cores} is within a 2500m quota and must not be warned about"
            );
        }

        // Above it, the quota is named exactly rather than as the floored count.
        let over = CpuBudget::resolve(&spicepod("3"), &fractional)
            .expect("valid")
            .configured_above_ceiling_warning()
            .expect("3 cores above a 2500m quota must be remarked on");
        assert!(
            over.contains("cgroup CPU limit of 2.5 cores"),
            "names the exact quota, not the floored core count: {over}"
        );
        assert!(
            !over.contains("affinity"),
            "a fractional quota is not an affinity problem: {over}"
        );
    }

    /// `all` is the weakest setting: it says "no ceiling of my own", so a quantity
    /// named on a lower-precedence surface wins.
    ///
    /// The case this exists for is a platform setting `SPICE_CPU_CORES=all` on
    /// every deployment to keep pods on the whole machine. Without this, an
    /// operator writing `runtime.cpu.cores: 4` in their spicepod would be silently
    /// ignored, and their only way to narrow would be `limits.cpu` — a CFS quota,
    /// which is the throttling this whole knob exists to avoid.
    #[test]
    fn all_defers_to_a_quantity_below_it() {
        // (cli, env, spicepod) -> resolved cores on a 64-core host with a 100m
        // declared request.
        let cases = [
            // The case this is for: the platform says `all`, the user narrows.
            (None, Some("all"), Some("4"), 4, CpuSource::Configured),
            // Nothing below to defer to, so `all` stands.
            (None, Some("all"), None, 64, CpuSource::AllCores),
            // `auto` is an instruction, not the absence of one, so `all` outranks it.
            (None, Some("all"), Some("auto"), 64, CpuSource::AllCores),
            // A quantity above `all` still wins outright — ordinary precedence.
            (
                Some("16"),
                Some("all"),
                Some("4"),
                16,
                CpuSource::Configured,
            ),
            // `all` on the lowest surface behaves as it always did.
            (None, None, Some("all"), 64, CpuSource::AllCores),
            // Two `all`s and a quantity: the quantity is the only ceiling named.
            (
                Some("all"),
                Some("all"),
                Some("4"),
                4,
                CpuSource::Configured,
            ),
            // Scanning past `all`, an intervening `auto` names no ceiling either.
            (
                Some("all"),
                Some("auto"),
                Some("4"),
                4,
                CpuSource::Configured,
            ),
            // Unchanged: ordinary precedence between quantities.
            (None, Some("8"), Some("4"), 8, CpuSource::Configured),
            // Unchanged: `auto` above a quantity still blocks it, which is its own
            // sharp edge but not one `all` introduces.
            (None, Some("auto"), Some("4"), 2, CpuSource::RequestBurst),
            // Unchanged: nothing configured at all.
            (None, None, None, 2, CpuSource::RequestBurst),
        ];

        for (cli, env, spicepod, expected_cores, expected_source) in cases {
            let cfg = CpuConfig::from_sources(cli, env, spicepod);
            let resolved = CpuBudget::resolve(&cfg, &request_only(64, 100)).expect("valid");
            assert_eq!(
                (resolved.cores(), resolved.source()),
                (expected_cores, expected_source),
                "cli={cli:?} env={env:?} spicepod={spicepod:?}"
            );
        }
    }

    /// A value below a winning quantity is never consulted, so it is never parsed
    /// — but one below an `all` is, because `all` defers to it.
    #[test]
    fn only_consulted_surfaces_are_validated() {
        // The CLI quantity wins immediately; the garbage below is not reached.
        let unreached = CpuBudget::resolve(
            &CpuConfig::from_sources(Some("4"), Some("garbage"), None),
            &host(18),
        )
        .expect("a value below the winning quantity is never parsed");
        assert_eq!(unreached.cores(), 4);

        // `all` defers downward, so the value it defers to has to be valid.
        let err = CpuBudget::resolve(
            &CpuConfig::from_sources(None, Some("all"), Some("garbage")),
            &host(18),
        )
        .expect_err("a consulted value must be validated");
        assert!(
            err.to_string().contains(CpuConfig::SPICEPOD_SETTING),
            "{err}"
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

        // Every rung that outranks the request rung: wiring the request would not
        // have changed the entitlement, so there is nothing to report. `all` is the
        // case that matters in practice — a platform expressing an unlimited-CPU
        // plan sets it on every pod, and must not be told to wire a request it is
        // deliberately ignoring.
        for cfg in [
            CpuConfig::from_sources(None, None, Some("all")),
            CpuConfig::from_sources(None, None, Some("6")),
        ] {
            let resolved = CpuBudget::resolve(&cfg, &unwired).expect("valid");
            assert_eq!(
                resolved.undeclared_request_warning(),
                None,
                "{:?} must not be told to wire a request it would ignore",
                resolved.source()
            );
        }
        let under_quota = HostReadings {
            quota_millicores: Some(8000),
            ..unwired
        };
        assert_eq!(
            CpuBudget::resolve(&CpuConfig::default(), &under_quota)
                .expect("valid")
                .undeclared_request_warning(),
            None,
            "a limit outranks the request, so nothing would change"
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
    /// grammar `runtime.cpu.cores` accepts, so this parses whole millicores and
    /// rejects everything that cannot be one.
    ///
    /// It cannot reject "looks like cores": `4` is four millicores with the divisor
    /// and four cores without it, and both are legal. That ambiguity is why the
    /// unit is in the variable's name, and the assertion below pins the accepting
    /// half of it so the limit of the guard stays visible rather than being read as
    /// a bug.
    #[test]
    fn declared_request_parses_whole_millicores_only() {
        // What the downward API actually sends.
        assert_eq!(parse_declared_request_millicores("4000"), Some(4000));
        assert_eq!(parse_declared_request_millicores("100"), Some(100));
        assert_eq!(parse_declared_request_millicores(" 250 "), Some(250));
        // An explicit unit is accepted and means the same thing.
        assert_eq!(parse_declared_request_millicores("3500m"), Some(3500));

        // A bare integer is accepted whatever it was meant to be: this is four
        // millicores, and a surface that dropped the divisor meaning four cores is
        // indistinguishable. The 2-core floor is what bounds that, not parsing.
        assert_eq!(parse_declared_request_millicores("4"), Some(4));

        // Not a whole number of millicores, which only a core-denominated value
        // would be — reading `3.5` as 3 millicores would under-size by 1000x.
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
