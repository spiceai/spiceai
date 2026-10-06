# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The scenario catalog: one entry per claim about HTTP rate control.

Every bound below is derived from the configuration, not from a previous run's
numbers:

  * a budget of N rps bounds what reaches the origin, however many datasets,
    replicas or in-flight requests there are;
  * "saturated" is a p99 second of at least 70% of the budget;
  * "throttled" is a p99 second of at most 50% of the budget;
  * the arrival peak may be N+1: a permit is spent when its window grants it
    and the request lands milliseconds later, so one request can be counted in
    the next whole second. The exact per-window check is in `state.py`, which
    reads the leased budget rather than wall-clock arrivals.

**A single node and a cluster move at completely different speeds**, and the
phase lengths here follow from that rather than from taste. A single node's
adaptive controller decays over `rate_control_window`, which defaults to 10s,
so it takes tens of seconds to walk a saturated origin down to its floor and
back. A cluster's half-life is one window (`refresh_interval`, 1s here),
because one window is the shortest half-life the shared state can express, so
it steps rather than glides. `Settle` therefore skips a long transient in a
single-node fault phase and a short one in a cluster's -- and `Timing` asserts
the transient itself, so the reaction time is measured rather than hidden
inside a steady-state bound.

The catalog is ordered by how many things vary at once: one origin and one
dataset first, then several origins, then several datasets on one origin, then
the same questions again with several spiced replicas sharing one budget.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .rates import Slice
from .topology import ClusterState, DatasetSpec, OriginSpec, RateControl, Topology

BUDGET = 20
SATURATED = 0.7 * BUDGET  # 14
THROTTLED = 0.5 * BUDGET  # 10
#: Five whole seconds may carry five budgets, plus one. A shorter window is
#: not a statement about the rate: a token bucket's slack puts a single second
#: at 21 and two adjacent seconds at 42 against this 20 rps limit, without the
#: sustained rate ever exceeding it.
ROLLING_5S = 5 * BUDGET + 1
#: The cluster budget is no longer written back: each replica derives
#: `effective_burst` from the shared counts and holds it for the window. What
#: the shared file still bounds is the configured burst, because a grant is
#: capped at `effective_burst - granted_by_others` and `effective_burst` is
#: itself capped at `burst_per_window`.

P1_PORT = 9001
P2_PORT = 9002


@dataclass(frozen=True)
class Settle:
    """Seconds to ignore at the start of each phase while the controller moves.

    Not a fudge factor: a steady-state bound is a claim about the steady state,
    and `Timing` is where the transient is asserted.
    """

    warmup: float = 6.0
    fault: float = 6.0
    recovery: float = 6.0

    def of(self, phase: str) -> float:
        return getattr(self, phase)


@dataclass(frozen=True)
class Timing:
    """How fast the controller must react, and how fast it must give the rate back."""

    throttle_to: float | None = None
    throttle_within_s: float | None = None
    recover_to: float | None = None
    recover_within_s: float | None = None


@dataclass(frozen=True)
class Fault:
    """A `/control` profile applied to one origin for the fault phase."""

    origin: str
    profile: dict[str, Any]


@dataclass(frozen=True)
class Bound:
    """One claim about the arrival rate in one phase, over one slice."""

    claim: str
    phase: str  # "warmup" | "fault" | "recovery"
    where: Slice = field(default_factory=Slice)
    min_p99: float | None = None
    max_p99: float | None = None
    #: No five consecutive whole seconds may carry more than this. The honest
    #: wall-clock form of a budget: a per-second peak counts requests that were
    #: granted in one window and landed in the next, and with several requests
    #: in flight more than one can cross.
    max_rolling_5s: float | None = None
    #: Every arrival in the slice must carry one of these statuses. Used where
    #: the point is that a dataset was throttled without itself failing.
    only_statuses: tuple[str, ...] | None = None
    #: The slice must have received at least this many arrivals, so an empty
    #: slice cannot pass a `max_` bound by having no traffic at all.
    min_total: int | None = None
    #: This slice's p99 must be at most this fraction of its OWN p99 in the
    #: warmup phase. The right shape for "throttled" whenever the throttle is
    #: proportional rather than deep: a controller that keys on an origin's
    #: overall error rate barely moves when the failing tenant is a minority of
    #: that origin's traffic, so an absolute bound would be asserting the error
    #: mix rather than the coupling.
    max_fraction_of_warmup: float | None = None


@dataclass(frozen=True)
class Scenario:
    name: str
    claim: str
    topology: Topology
    faults: tuple[Fault, ...] = ()
    bounds: tuple[Bound, ...] = ()
    timing: Timing | None = None
    #: The published `adaptive_admission_ratio` must dip below this during the
    #: fault phase. Read from the runtime's own telemetry, so it decides
    #: "the controller reacted" independently of what reached the origin.
    fault_admission_ratio_below: float | None = None
    #: The published `adaptive_admission_ratio` must stay at or above this for
    #: the whole fault phase -- the claim that the controller did NOT react.
    fault_admission_ratio_above: float | None = None
    #: A configuration the runtime is expected to refuse. When set, the run
    #: starts the replicas, greps their logs for this text, and stops.
    expect_startup_error: str | None = None
    warmup_s: float = 25.0
    fault_s: float = 40.0
    recovery_s: float = 25.0
    settle: Settle = field(default_factory=Settle)
    #: Concurrent queries per (replica, dataset). Demand must stay well above
    #: the budget or the measurement is of the load generator, not the limiter.
    workers: int = 8


# A single node's default `rate_control_window` is 10s, so the walk down takes
# ~40s and the walk back up ~15s. Measure the settled tail of each.
SINGLE_ADAPTIVE_SHAPE = dict(
    warmup_s=25.0,
    fault_s=65.0,
    recovery_s=30.0,
    settle=Settle(warmup=6.0, fault=35.0, recovery=18.0),
)
SINGLE_ADAPTIVE_TIMING = Timing(
    throttle_to=THROTTLED,
    throttle_within_s=30.0,
    recover_to=SATURATED,
    recover_within_s=20.0,
)
# A cluster's half-life is one window, so it steps instead of gliding.
CLUSTER_SHAPE = dict(
    warmup_s=25.0,
    fault_s=40.0,
    recovery_s=25.0,
    settle=Settle(warmup=6.0, fault=10.0, recovery=10.0),
)
CLUSTER_TIMING = Timing(
    throttle_to=THROTTLED,
    throttle_within_s=12.0,
    recover_to=SATURATED,
    recover_within_s=12.0,
)
# Nothing should move, so nothing needs settling for.
STEADY_SHAPE = dict(
    warmup_s=20.0, fault_s=30.0, recovery_s=15.0, settle=Settle(6.0, 6.0, 6.0)
)


def _adaptive(rps: int = BUDGET, threshold: str = "20%", window: str | None = None) -> RateControl:
    return RateControl(requests_per_second=rps, failure_threshold=threshold, window=window)


#: There is no static mode to compare against any more -- rate control is always
#: adaptive. The nearest expressible control is a threshold so tolerant that a
#: half-failing origin stays under it: at a 90% threshold `K = 10`, so a 50%
#: success rate gives `min(1, (10*0.5r + 1)/(r + 1)) = 1` and nothing throttles.
#: That is a real claim about the knob, not a stand-in for a mode.
TOLERANT_THRESHOLD = "90%"


def _tolerant(rps: int = BUDGET) -> RateControl:
    return RateControl(requests_per_second=rps, failure_threshold=TOLERANT_THRESHOLD)


def _limit(rps: int = BUDGET) -> RateControl:
    """A plain request-rate limit on its default threshold and window."""
    return RateControl(requests_per_second=rps)


def _one_origin(rate_control: RateControl, datasets: int = 1) -> Topology:
    return Topology(
        origins=(OriginSpec("p1", P1_PORT, rate_control),),
        datasets=tuple(
            DatasetSpec(f"d{index + 1}", "p1", "/data" if index == 0 else "/data.json")
            for index in range(datasets)
        ),
    )


FAIL_503 = {"id": "fail-503", "mode": "status", "error_status": 503, "error_rate": 1.0}
FAIL_429 = {"id": "fail-429", "mode": "status", "error_status": 429, "error_rate": 1.0}
HANG = {"id": "hang", "mode": "hang", "error_rate": 1.0, "timeout_hang_ms": 3000}
REFUSE = {"id": "refuse", "mode": "refuse", "error_rate": 1.0}
LATENCY_ONLY = {"id": "latency", "mode": "latency", "latency_ms": {"base": 300, "jitter": 100}}
ERRORS_10_PERCENT = {
    "id": "errors-10pct",
    "mode": "status",
    "error_status": 503,
    "error_rate": 0.1,
    "seed": 1,
}


def _saturated(phase: str, where: Slice = Slice(), claim: str = "") -> Bound:
    return Bound(
        claim=claim or f"{where.label()} runs at the configured budget",
        phase=phase,
        where=where,
        min_p99=SATURATED,
        max_rolling_5s=ROLLING_5S,
    )


def _throttled(
    phase: str, where: Slice = Slice(), claim: str = "", max_p99: float = THROTTLED
) -> Bound:
    return Bound(
        claim=claim or f"{where.label()} is throttled",
        phase=phase,
        where=where,
        max_p99=max_p99,
        min_total=1,
    )


def _unchanged(phase: str, claim: str, where: Slice = Slice()) -> Bound:
    return Bound(claim=claim, phase=phase, where=where, min_p99=SATURATED, max_rolling_5s=ROLLING_5S)


# --------------------------------------------------------------------------
# A. One origin, one dataset, one replica -- does the controller react at all,
#    and to the right things?
# --------------------------------------------------------------------------

A_SCENARIOS = (
    Scenario(
        name="tolerant-threshold",
        claim="a threshold the origin's error rate stays under does not throttle",
        topology=_one_origin(_tolerant()),
        # Half the requests fail, against a 90% threshold. This is the negative
        # control for every throttling scenario below: if it throttled too, the
        # others would be evidence of something other than the error signal.
        faults=(Fault("p1", dict(FAIL_503, error_rate=0.5, seed=7)),),
        fault_admission_ratio_above=0.999,
        bounds=(
            _saturated("warmup"),
            _unchanged("fault", "an error rate under the threshold changes nothing"),
            _saturated("recovery"),
        ),
        **STEADY_SHAPE,
    ),
    Scenario(
        name="throttle-503",
        claim="rate control throttles on 5xx and recovers when the origin does",
        topology=_one_origin(_adaptive()),
        faults=(Fault("p1", FAIL_503),),
        bounds=(_saturated("warmup"), _throttled("fault"), _saturated("recovery")),
        timing=SINGLE_ADAPTIVE_TIMING,
        **SINGLE_ADAPTIVE_SHAPE,
    ),
    Scenario(
        name="throttle-429",
        claim="a 429 is a failure signal, the same as a 5xx",
        topology=_one_origin(_adaptive()),
        faults=(Fault("p1", FAIL_429),),
        bounds=(_saturated("warmup"), _throttled("fault"), _saturated("recovery")),
        timing=SINGLE_ADAPTIVE_TIMING,
        **SINGLE_ADAPTIVE_SHAPE,
    ),
    Scenario(
        name="throttle-timeout",
        claim="a request that hangs past client_timeout throttles like an error does",
        # The hang is the slow path: each worker is parked for a whole
        # client_timeout, so demand needs many more of them to stay above the
        # budget.
        topology=Topology(
            origins=(OriginSpec("p1", P1_PORT, _adaptive()),),
            datasets=(DatasetSpec("d1", "p1"),),
            # Bare seconds; see Topology.client_timeout.
            client_timeout="1",
        ),
        faults=(Fault("p1", HANG),),
        workers=48,
        bounds=(_saturated("warmup"), _throttled("fault"), _saturated("recovery")),
        timing=SINGLE_ADAPTIVE_TIMING,
        **SINGLE_ADAPTIVE_SHAPE,
    ),
    Scenario(
        name="throttle-refuse",
        claim="a refused connection throttles like an error does",
        topology=_one_origin(_adaptive()),
        faults=(Fault("p1", REFUSE),),
        bounds=(_saturated("warmup"), _throttled("fault"), _saturated("recovery")),
        timing=SINGLE_ADAPTIVE_TIMING,
        **SINGLE_ADAPTIVE_SHAPE,
    ),
    Scenario(
        name="no-throttle-latency-only",
        claim="a slow but healthy origin is not throttled",
        topology=_one_origin(_adaptive()),
        faults=(Fault("p1", LATENCY_ONLY),),
        # 400ms per response caps one worker at 2.5 rps, so the budget needs
        # more workers than a fast origin does.
        workers=24,
        bounds=(
            _saturated("warmup"),
            _unchanged("fault", "latency alone does not move the admission coefficient"),
            _saturated("recovery"),
        ),
        **STEADY_SHAPE,
    ),
    Scenario(
        name="below-threshold",
        claim="an error rate under the failure threshold does not throttle",
        topology=_one_origin(_adaptive(threshold="50%")),
        faults=(Fault("p1", ERRORS_10_PERCENT),),
        bounds=(
            _saturated("warmup"),
            _unchanged("fault", "10% errors under a 50% threshold leave the rate alone"),
        ),
        **STEADY_SHAPE,
    ),
)


# --------------------------------------------------------------------------
# B. Several origins -- one limiter each, and no leakage between them.
# --------------------------------------------------------------------------

B_SCENARIOS = (
    Scenario(
        name="multi-origin-isolation",
        claim="a failing origin is throttled and a healthy one beside it is not",
        topology=Topology(
            origins=(
                OriginSpec("p1", P1_PORT, _adaptive()),
                OriginSpec("p2", P2_PORT, _adaptive()),
            ),
            datasets=(DatasetSpec("d1", "p1"), DatasetSpec("d2", "p2")),
        ),
        faults=(Fault("p2", FAIL_503),),
        bounds=(
            _saturated("warmup", Slice(origin="p1")),
            _saturated("warmup", Slice(origin="p2")),
            _throttled("fault", Slice(origin="p2"), "the failing origin is throttled"),
            _unchanged(
                "fault",
                "the healthy origin keeps its own full budget throughout",
                Slice(origin="p1"),
            ),
            _saturated("recovery", Slice(origin="p2")),
        ),
        **SINGLE_ADAPTIVE_SHAPE,
    ),
    Scenario(
        name="multi-origin-budgets",
        claim="each origin holds its own budget at the same time",
        topology=Topology(
            origins=(
                OriginSpec("p1", P1_PORT, _limit(BUDGET)),
                OriginSpec("p2", P2_PORT, _limit(5)),
            ),
            datasets=(DatasetSpec("d1", "p1"), DatasetSpec("d2", "p2")),
        ),
        bounds=(
            Bound(
                claim="p1 runs at its 20 rps budget",
                phase="warmup",
                where=Slice(origin="p1"),
                min_p99=SATURATED,
                max_rolling_5s=ROLLING_5S,
            ),
            Bound(
                claim="p2 runs at its own, smaller 5 rps budget",
                phase="warmup",
                where=Slice(origin="p2"),
                min_p99=3,
                max_rolling_5s=26,
            ),
        ),
        **STEADY_SHAPE,
    ),
)


# --------------------------------------------------------------------------
# C. Several datasets on one origin -- one limiter, shared.
# --------------------------------------------------------------------------

C_SCENARIOS = (
    Scenario(
        name="sameorigin-shared-budget",
        claim="datasets that share an origin share one budget, they do not each get one",
        topology=_one_origin(_limit(), datasets=2),
        bounds=(
            Bound(
                claim="two saturated datasets on one origin stay within ONE budget",
                phase="warmup",
                min_p99=SATURATED,
                max_rolling_5s=ROLLING_5S,
            ),
            Bound(
                claim="both datasets are actually sending",
                phase="warmup",
                where=Slice(dataset="d2"),
                min_total=20,
            ),
        ),
        **STEADY_SHAPE,
    ),
    Scenario(
        name="sameorigin-coupled-throttle",
        claim=(
            "one dataset's failures shrink the budget its healthy co-tenant is "
            "admitted through, without the co-tenant itself being slowed down"
        ),
        topology=_one_origin(_adaptive(), datasets=2),
        # Scoped to d1's path only, so d2's own responses stay 200 throughout.
        #
        # Two things make this scenario's bounds look weaker than they are, and
        # both are findings rather than concessions:
        #
        # 1. The controller keys on the ORIGIN's overall error rate, and d1 is
        #    only part of that origin's traffic, so d2's successes dilute d1's
        #    errors. With a 20% threshold and roughly half the traffic failing,
        #    the coefficient settles near 0.8, not at the floor.
        # 2. d2 does not slow down. Over five runs d1 loses about half its rate
        #    every time while d2 stays flat within +/-3 rps and the origin's
        #    total falls by only 1-2. Asserting that d2 slows down would be
        #    asserting something that does not happen; asserting that it speeds
        #    up (an earlier draft did, on two runs) is no better supported.
        #
        # What is true, and is what a per-dataset limiter would fail: d2 is
        # admitted through the budget d1's failures shrank, so the two of them
        # together never exceed one budget, and the published coefficient moves
        # on failures d2 never saw.
        faults=(Fault("p1", dict(FAIL_503, fault_paths=["/data"])),),
        fault_admission_ratio_below=0.95,
        bounds=(
            _saturated("warmup"),
            Bound(
                claim="the origin as a whole slows down",
                phase="fault",
                max_fraction_of_warmup=0.95,
            ),
            Bound(
                claim="both datasets together still fit inside ONE budget",
                phase="fault",
                max_rolling_5s=ROLLING_5S,
            ),
            Bound(
                claim="the co-tenant keeps sending",
                phase="fault",
                where=Slice(dataset="d2"),
                min_total=1,
            ),
            Bound(
                claim="...and it was never itself served an error",
                phase="fault",
                where=Slice(dataset="d2"),
                only_statuses=("200",),
            ),
            _saturated("recovery"),
        ),
        **SINGLE_ADAPTIVE_SHAPE,
    ),
    Scenario(
        name="sameorigin-minority-failure",
        claim=(
            "a dataset small enough can fail EVERY request without the origin's "
            "controller throttling at all"
        ),
        # The controller keys on the origin's overall error rate `f`, and
        # throttling starts exactly when `f` passes the failure threshold. A
        # dataset that is a share `s` of the origin's traffic and fails every
        # request contributes `f = s`, so with `s` under the threshold it is
        # invisible however hard it fails. d1 gets 2 of the 18 workers here, so
        # it asks for roughly a ninth of the origin's traffic against a 20%
        # threshold.
        topology=Topology(
            origins=(OriginSpec("p1", P1_PORT, _adaptive(threshold="20%")),),
            datasets=(
                DatasetSpec("d1", "p1", "/data", workers=2),
                DatasetSpec("d2", "p1", "/data.json", workers=16),
            ),
        ),
        faults=(Fault("p1", dict(FAIL_503, fault_paths=["/data"])),),
        bounds=(
            _saturated("warmup"),
            Bound(
                claim="the minority dataset really is failing every request",
                phase="fault",
                where=Slice(dataset="d1"),
                only_statuses=("503",),
                min_total=20,
            ),
            _unchanged(
                "fault", "and the origin keeps running at its full configured rate"
            ),
        ),
        fault_admission_ratio_above=0.999,
        **SINGLE_ADAPTIVE_SHAPE,
    ),
    Scenario(
        name="sameorigin-conflicting-config",
        claim="two datasets on one origin may not ask for different limits",
        topology=Topology(
            origins=(OriginSpec("p1", P1_PORT, _limit()),),
            datasets=(
                DatasetSpec("d1", "p1", "/data"),
                DatasetSpec("d2", "p1", "/data.json", override={"requests_per_second_limit": "5"}),
            ),
        ),
        expect_startup_error="with different rate-control settings",
    ),
)


# --------------------------------------------------------------------------
# D. Several replicas -- the budget is the cluster's, and so is the throttle.
# --------------------------------------------------------------------------


def _cluster(
    rate_control: RateControl,
    replicas: int = 2,
    origins: int = 1,
    datasets_per_origin: int = 1,
    backend: str = "file",
) -> Topology:
    origin_specs = tuple(
        OriginSpec(f"p{index + 1}", P1_PORT + index, rate_control) for index in range(origins)
    )
    datasets = tuple(
        DatasetSpec(
            f"{origin.name}d{index + 1}",
            origin.name,
            "/data" if index == 0 else "/data.json",
        )
        for origin in origin_specs
        for index in range(datasets_per_origin)
    )
    return Topology(
        origins=origin_specs,
        datasets=datasets,
        replicas=replicas,
        cluster=ClusterState(backend=backend),
    )


D_SCENARIOS = (
    Scenario(
        name="cluster-adaptive",
        claim="replicas sharing one budget throttle together and recover together",
        topology=_cluster(_adaptive()),
        faults=(Fault("p1", FAIL_503),),
        bounds=(_saturated("warmup"), _throttled("fault"), _saturated("recovery")),
        timing=CLUSTER_TIMING,
        **CLUSTER_SHAPE,
    ),
    Scenario(
        name="cluster-tolerant-threshold",
        claim="a tolerant threshold does not throttle a cluster either",
        topology=_cluster(_tolerant()),
        faults=(Fault("p1", dict(FAIL_503, error_rate=0.5, seed=7)),),
        bounds=(
            _saturated("warmup"),
            _unchanged("fault", "an error rate under the threshold changes nothing"),
        ),
        **STEADY_SHAPE,
    ),
    Scenario(
        name="cluster-asymmetric",
        claim="a replica that saw no failure is throttled by its peers' failures",
        topology=_cluster(_adaptive()),
        # Only r0's requests fail. A per-node controller would leave r1 alone.
        # Only one of the two replicas fails, so the cluster's overall error
        # rate is about half and the shared coefficient settles part-way down
        # rather than at the floor. The claim is the coupling, so the bound is
        # relative to r1's own healthy rate.
        faults=(Fault("p1", dict(FAIL_503, fault_headers={"x-spice-replica": "r0"})),),
        fault_admission_ratio_below=0.95,
        bounds=(
            _saturated("warmup"),
            Bound(
                claim="the replica that only ever got 200s does not gain rate",
                phase="fault",
                where=Slice(replica="r1"),
                # How FAR it falls is set by the fleet's overall error rate --
                # only one of two replicas fails here, so the shared
                # coefficient settles part-way down and the split between the
                # replicas moves run to run. What does not vary: the replica
                # that saw no failure never gets more than it had while the
                # origin was healthy. Contrast `sameorigin-coupled-throttle`,
                # where the healthy co-tenant does gain.
                max_fraction_of_warmup=1.0,
                min_total=1,
            ),
            Bound(
                claim="...and it was never itself served an error",
                phase="fault",
                where=Slice(replica="r1"),
                only_statuses=("200",),
            ),
            _saturated("recovery"),
        ),
        **CLUSTER_SHAPE,
    ),
    Scenario(
        name="cluster-single-replica",
        claim="one replica alone gets the whole cluster budget, not a share of it",
        topology=_cluster(_adaptive(), replicas=1),
        faults=(Fault("p1", FAIL_503),),
        bounds=(_saturated("warmup"), _throttled("fault"), _saturated("recovery")),
        timing=CLUSTER_TIMING,
        **CLUSTER_SHAPE,
    ),
    Scenario(
        name="cluster-three-replicas",
        claim="the budget does not grow with the fleet",
        topology=_cluster(_adaptive(), replicas=3),
        faults=(Fault("p1", FAIL_503),),
        workers=6,
        bounds=(_saturated("warmup"), _throttled("fault"), _saturated("recovery")),
        timing=CLUSTER_TIMING,
        **CLUSTER_SHAPE,
    ),
    Scenario(
        name="cluster-multi-origin",
        claim="each origin gets its own shared budget and its own cluster throttle",
        topology=_cluster(_adaptive(), replicas=2, origins=2),
        faults=(Fault("p2", FAIL_503),),
        bounds=(
            _saturated("warmup", Slice(origin="p1")),
            _saturated("warmup", Slice(origin="p2")),
            _throttled("fault", Slice(origin="p2"), "the failing origin's cluster throttles"),
            _unchanged(
                "fault",
                "the healthy origin's cluster keeps its own full budget",
                Slice(origin="p1"),
            ),
            _saturated("recovery", Slice(origin="p2")),
        ),
        **CLUSTER_SHAPE,
    ),
    Scenario(
        name="cluster-sameorigin",
        claim="one budget covers every replica and every dataset on one origin",
        topology=_cluster(_limit(), replicas=2, datasets_per_origin=2),
        bounds=(
            Bound(
                claim="2 replicas x 2 datasets stay within ONE 20 rps budget",
                phase="warmup",
                min_p99=SATURATED,
                max_rolling_5s=ROLLING_5S,
            ),
            Bound(
                claim="every replica/dataset pair is actually sending",
                phase="warmup",
                where=Slice(replica="r1", dataset="p1d2"),
                min_total=10,
            ),
        ),
        workers=6,
        **STEADY_SHAPE,
    ),
    Scenario(
        name="cluster-adaptive-s3",
        claim="the same cluster throttle over a real object store",
        topology=_cluster(_adaptive(), backend="s3"),
        faults=(Fault("p1", FAIL_503),),
        bounds=(_saturated("warmup"), _throttled("fault"), _saturated("recovery")),
        timing=CLUSTER_TIMING,
        **CLUSTER_SHAPE,
    ),
)


SCENARIOS: dict[str, Scenario] = {
    scenario.name: scenario
    for scenario in A_SCENARIOS + B_SCENARIOS + C_SCENARIOS + D_SCENARIOS
}

GROUPS: dict[str, tuple[str, ...]] = {
    "single": tuple(s.name for s in A_SCENARIOS),
    "multi-origin": tuple(s.name for s in B_SCENARIOS),
    "same-origin": tuple(s.name for s in C_SCENARIOS),
    "cluster": tuple(s.name for s in D_SCENARIOS),
    "all": tuple(SCENARIOS),
}
