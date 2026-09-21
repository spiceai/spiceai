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

"""Shared building blocks for the HTTP cache / rate-limit load generators.

Each ``run_phaseN.py`` script keeps only what is phase-specific — its
scenario table, its ``Sample`` shape, its assertions and its console report.
Everything the phases share (the HTTP client, the fault timeline, the origin
arrival-log schema, the phase-window math, and the assertion / verdict
plumbing) lives here so there is a single source of truth for it.

Modules:
  http      - tiny urllib JSON/SQL client used to talk to spiced and origins.
  timeline  - background thread that steps an origin fault profile on the
              shared clock and records one driver event per step.
  arrivals  - parse and window the origin ``/data`` arrival log.
  phases    - the warmup/fault/recovery(/burst) window model.
  oracle    - assertion collection, the verdict ladder, and artifact writers.
  cli       - environment-variable argparse default helpers.
"""
