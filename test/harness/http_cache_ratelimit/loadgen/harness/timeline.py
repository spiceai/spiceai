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

"""Background fault timeline stepped on the shared harness clock.

A ``Timeline`` POSTs a sequence of fault profiles to an origin ``/control``
endpoint at wall-clock offsets from a shared ``t0``, and records one driver
event per step so the run can be correlated with the per-response samples and
the origin arrival log (all keyed on ``t_rel = now - t0``).

``profile`` (on both ``Step`` and ``Event``) stays a plain dict: it is the
JSON body POSTed to ``/control`` and its shape varies per scenario (a fault
profile may carry ``mode``, ``error_status``, ``latency_ms``, ``headers``,
``seed``, ...), so a fixed dataclass would not fit it.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any

from . import http


@dataclass
class Step:
    """One scheduled ``/control`` POST: apply ``profile`` at ``t0 + at_s``."""

    at_s: float
    profile: dict[str, Any]
    note: str = ""


@dataclass
class Event:
    """The recorded outcome of one ``Step`` actually being applied."""

    t_rel_s: float
    at_s: float
    action: str
    profile_id: str
    mode: str
    error: str | None = None


class Timeline:
    """Steps an origin fault profile on the shared clock, one event per step."""

    def __init__(self, t0: float, control_url: str, steps: list[Step]):
        self.t0 = t0
        self.control_url = control_url
        self.steps = steps
        self.events: list[Event] = []
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        for step in self.steps:
            target = self.t0 + step.at_s
            while time.time() < target:
                time.sleep(min(0.05, max(0.0, target - time.time())))
            profile = step.profile
            err: str | None = None
            try:
                http.post_json(self.control_url, profile)
            except Exception as e:  # noqa: BLE001 - record, do not abort the run
                err = f"{type(e).__name__}: {e}"
            self.events.append(
                Event(
                    t_rel_s=round(time.time() - self.t0, 4),
                    at_s=step.at_s,
                    action=step.note or profile.get("id", ""),
                    profile_id=profile.get("id", ""),
                    mode=profile.get("mode", ""),
                    error=err,
                )
            )
