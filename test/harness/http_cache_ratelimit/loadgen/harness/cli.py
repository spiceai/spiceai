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

"""Environment-variable argparse default helpers.

The load generators take every knob from either a CLI flag or an environment
variable, so their ``argparse`` defaults are all of the form
``float(os.environ.get("NAME", "3"))``. These helpers collapse that to
``env_float("NAME", 3.0)`` — same behavior, far less noise, and the default's
type is written once rather than round-tripped through a string.

Argument *definitions* stay in each phase script: the phases genuinely differ
in which knobs they expose (single vs. dual origin, cache windows vs. rate
limits), so a shared argparse group would couple them for little gain.
"""

from __future__ import annotations

import os


def env_str(name: str, default: str) -> str:
    """The value of env var ``name``, or ``default`` if unset."""
    return os.environ.get(name, default)


def env_float(name: str, default: float) -> float:
    """The value of env var ``name`` parsed as float, or ``default`` if unset."""
    value = os.environ.get(name)
    return default if value is None else float(value)


def env_int(name: str, default: int) -> int:
    """The value of env var ``name`` parsed as int, or ``default`` if unset."""
    value = os.environ.get(name)
    return default if value is None else int(value)
