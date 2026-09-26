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

"""Shared query-string key set and SQL helpers for the HTTP cache /
rate-limit harness load generators (Phase 0, 1 and 2).

Every load generator drives its dataset with ``WHERE request_query = '<key>'``
so distinct query strings become distinct upstream requests / cache entries
against the same origin. Keeping the key set and the SQL builder here means
the three generators share one definition instead of duplicating it.

Each key is its own upstream request / cache key, so the harness exercises
many keys against one origin rather than a single key. The set is large
(1000) and deliberately not fully primed by anything -- most keys are cold
at any given moment, which is the realistic case for a cache in front of a
long-tail workload.
"""

from __future__ import annotations

import random
from collections.abc import Callable

# Fixed set of query-string keys. Each becomes its own upstream request and
# cache entry. `request_query` values omit the leading '?'.
QUERY_KEYS: list[str] = [f"q={i}" for i in range(1, 1001)]

# Default seed so a run replays deterministically. The harness convention
# elsewhere uses 12345.
DEFAULT_SEED = 12345


def make_key_picker(seed: int = DEFAULT_SEED) -> Callable[[], str]:
    """Return a deterministic, seeded picker over ``QUERY_KEYS``.

    The picker owns a single ``random.Random`` stream, so a generator that
    wants reproducible key selection creates exactly one picker and calls it
    per query rather than seeding a second independent RNG.
    """
    rng = random.Random(seed)

    def pick() -> str:
        return rng.choice(QUERY_KEYS)

    return pick


def with_request_query(base_sql: str, query_key: str) -> str:
    """Return ``base_sql`` with an ``AND request_query = '<key>'`` predicate.

    ``base_sql`` must already carry its own ``WHERE`` clause (every generator
    filters on ``origin`` first). The key is single-quote escaped for SQL.
    """
    escaped = query_key.replace("'", "''")
    return f"{base_sql} AND request_query = '{escaped}'"
