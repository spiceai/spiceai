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

"""The warmup -> fault -> recovery (-> burst) window model.

A run is divided into phases by wall-clock offset from the shared ``t0``.
``phase_at`` maps a relative time to its phase name so every sample can be
tagged consistently. The optional burst window (used by the overshoot
scenario) is disabled by leaving ``burst_start`` at its default of infinity.

Scenarios that add their own sub-window (e.g. Phase 1's stale-if-error window,
which starts partway through the fault window) layer that classification on top
of these base phases locally.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Phases:
    """Phase-window boundaries in seconds relative to the shared ``t0``."""

    fault_start: float
    fault_end: float
    run_end: float
    burst_start: float = float("inf")  # >= run_end disables the burst window
    burst_qps: float = 0.0  # 0 disables the burst ramp


def phase_at(t_rel: float, p: Phases) -> str:
    """Return the phase name (``warmup`` | ``fault`` | ``recovery`` | ``burst``)."""
    if t_rel >= p.burst_start:
        return "burst"
    if t_rel >= p.fault_end:
        return "recovery"
    if t_rel >= p.fault_start:
        return "fault"
    return "warmup"
