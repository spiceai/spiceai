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

"""The shape of a rate-control scenario: origins, datasets, replicas, and the
spicepod that expresses them.

Rate control is keyed on the origin (`rate_control_key` = scheme://host:port,
path-independent), so the three dimensions a scenario can vary are:

  * how many origins there are            -> how many independent limiters
  * how many datasets share an origin     -> how many users of one limiter
  * how many spiced replicas there are    -> whether the limiter is local to a
                                             process or leased from shared state

`RateControl` therefore hangs off `OriginSpec`, not `DatasetSpec`: the runtime
refuses to start when two datasets on one origin ask for different limits
(`resolve_existing_controller` -> `conflicting_config_error`), so a per-dataset
limit is not a thing a working configuration can express. `DatasetSpec.override`
exists only to build the configuration that is supposed to be refused.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Mapping

REPLICA_HEADER = "X-Spice-Replica"


@dataclass(frozen=True)
class RateControl:
    """The rate-control parameters every dataset on one origin must agree on.

    There is no mode. HTTP rate control is always adaptive: the limits scale
    down while the origin fails and back up as it recovers, and the only way to
    make it tolerant is a high `failure_threshold`. A dataset with no limit at
    all has nothing to scale, which is not an error.
    """

    requests_per_second: int | None = None
    requests_per_minute: int | None = None
    max_concurrent_requests: int | None = None
    #: The error rate above which throttling starts. Default 10%.
    failure_threshold: str | None = None  # e.g. "20%"
    #: The decay half-life. Default 10s locally, `refresh_interval` in cluster
    #: mode -- one window is the shortest half-life the shared state expresses.
    window: str | None = None
    acquire_timeout: str = "2s"

    def as_params(self) -> dict[str, str]:
        """The dataset parameters these settings become."""
        params: dict[str, str] = {
            "rate_control_acquire_timeout": self.acquire_timeout,
        }
        if self.requests_per_second is not None:
            params["requests_per_second_limit"] = str(self.requests_per_second)
        if self.requests_per_minute is not None:
            params["requests_per_minute_limit"] = str(self.requests_per_minute)
        if self.max_concurrent_requests is not None:
            params["max_concurrent_requests"] = str(self.max_concurrent_requests)
        if self.failure_threshold is not None:
            params["rate_control_failure_threshold"] = self.failure_threshold
        if self.window is not None:
            params["rate_control_window"] = self.window
        return params


@dataclass(frozen=True)
class OriginSpec:
    """One upstream origin: one host:port, and therefore one rate limiter."""

    name: str
    port: int
    rate_control: RateControl = field(default_factory=RateControl)

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def control_url(self) -> str:
        return f"{self.base}/control"

    @property
    def health_url(self) -> str:
        return f"{self.base}/healthz"


@dataclass(frozen=True)
class DatasetSpec:
    """One dataset. Several may share an origin; they then share its limiter."""

    name: str
    origin: str
    path: str = "/data"
    #: Concurrent queries for this dataset, overriding the scenario's default.
    #: Sets how large a share of its origin's traffic this dataset asks for,
    #: which is what decides how much of the origin's overall error rate a
    #: failure here accounts for.
    workers: int | None = None
    # Only for the configuration that is supposed to be refused: parameters that
    # disagree with the origin's own `RateControl`.
    override: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ClusterState:
    """Where replicas lease a shared budget from, if they do.

    The location is `runtime.state.location`, which is the runtime's one shared
    object store -- also used for results-cache warmup and distributed query
    state. Setting it turns cluster rate control on for every origin that has a
    request-rate limit; there is no rate-control-specific location any more.
    `runtime.source_rate_control.state_location` and `.params` were removed, and
    `source_rate_control` rejects unknown fields, so the old spelling fails to
    load rather than being ignored.
    """

    backend: str = "file"  # "file" | "s3"
    #: `runtime.source_rate_control.refresh_interval`: the lease window, and in
    #: cluster mode the default adaptive half-life.
    refresh_interval: str = "1s"
    bucket: str = "rate-control-state"
    prefix: str = "state/"


@dataclass(frozen=True)
class Topology:
    origins: tuple[OriginSpec, ...]
    datasets: tuple[DatasetSpec, ...]
    replicas: int = 1
    # None: each replica keeps its limiter in process memory.
    cluster: ClusterState | None = None
    # Bare seconds, NOT a duration string. The https connector parses these two
    # with `t.parse::<u64>()` and falls back to its default (30s / 10s) when the
    # parse fails, silently -- so "2s" here means 30s in the runtime, and a
    # scenario that depends on a short client timeout would quietly not run.
    # Every other duration parameter in the same block does take a unit.
    client_timeout: str = "2"
    connect_timeout: str = "1"

    def origin(self, name: str) -> OriginSpec:
        for origin in self.origins:
            if origin.name == name:
                return origin
        raise KeyError(f"no origin named {name!r}")

    def datasets_for(self, origin: str) -> tuple[DatasetSpec, ...]:
        return tuple(d for d in self.datasets if d.origin == origin)

    def replica_names(self) -> tuple[str, ...]:
        return tuple(f"r{index}" for index in range(self.replicas))

    def with_replicas(self, replicas: int) -> "Topology":
        return replace(self, replicas=replicas)


def render_spicepod(
    topology: Topology,
    spicepod_name: str,
    replica: str,
    state_location: str | None,
    state_params: str = "",
) -> str:
    """The spicepod one replica runs.

    Every replica renders the SAME `name`: the shared state object key is
    `{spicepod_name}/{host}_{port}-{hash(origin)}`, so a replica with a
    different name writes a different file and never meets its peers.
    """
    lines = [
        "version: v1",
        "kind: Spicepod",
        f"name: {spicepod_name}",
        "",
        "runtime:",
        "  telemetry:",
        "    enabled: true",
        "  caching:",
        "    # The results cache would answer a repeated query without reaching",
        "    # the connector, so demand would never arrive at the limiter.",
        "    sql_results:",
        "      enabled: false",
    ]
    if state_location is not None:
        assert topology.cluster is not None
        lines += [
            "  state:",
            f"    location: {state_location}",
        ]
        if state_params:
            lines += state_params.rstrip("\n").split("\n")
        lines += [
            "  source_rate_control:",
            f'    refresh_interval: "{topology.cluster.refresh_interval}"',
        ]
    lines += ["", "datasets:"]

    for dataset in topology.datasets:
        origin = topology.origin(dataset.origin)
        params: dict[str, str] = {
            "file_format": "json",
            # Selects the dynamic JSON API provider. `file_format: json` alone
            # routes to the object-store listing connector, which rejects every
            # rate-control parameter.
            "allowed_request_paths": dataset.path,
            "client_timeout": topology.client_timeout,
            "connect_timeout": topology.connect_timeout,
            # One query is one intended origin request: no retry, and no
            # response cache in front of the limiter.
            "max_retries": "0",
            "response_cache_max_size_bytes": "0",
            "http_headers": f"{REPLICA_HEADER}: {replica}",
        }
        params.update(origin.rate_control.as_params())
        params.update(dataset.override)

        lines += [
            f"  - from: {origin.base}{dataset.path}",
            f"    name: {dataset.name}",
            "    params:",
        ]
        lines += [f'      {key}: "{value}"' for key, value in params.items()]
    return "\n".join(lines) + "\n"
