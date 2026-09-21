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

"""Prometheus /metrics scraper for the rate-control harness (Phase 2).

Polls spiced's Prometheus endpoint on a fixed cadence, parses the text
exposition format with a small stdlib line parser (no prometheus_client
dependency), and keeps only the harness-relevant series keyed by the
``origin`` label. Metric names are read from ``metrics_config.json`` so a
rename on the branch under test is a one-line config change, not a code
edit (harness plan, open question 7).

Matching is by PREFIX: a scraped series is kept when its name starts with
one of the configured base names. That absorbs any suffix the
OpenTelemetry -> Prometheus exporter appends (``_total``, a unit suffix,
...), so the config stays correct whether or not the exporter renames the
instrument.

Every kept sample is one row in ``metrics.csv``:
    scrape_epoch_ms, t_rel_s, metric_name, origin, value

The oracle diffs counters over a window and reads the latest gauge in a
window, both off the in-memory samples this module keeps.
"""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.request
from dataclasses import dataclass
from typing import Optional

# name{labels} value [timestamp]   OR   name value [timestamp]
_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)"
    r"(?:\{(?P<labels>.*)\})?"
    r"\s+(?P<value>[^\s]+)"
    r"(?:\s+[0-9.eE+\-]+)?\s*$"
)
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def _parse_value(raw: str) -> Optional[float]:
    """Parse a Prometheus sample value; drop non-finite values."""
    try:
        v = float(raw)
    except ValueError:
        return None
    if v != v or v in (float("inf"), float("-inf")):  # NaN / +Inf / -Inf
        return None
    return v


def _parse_labels(raw: Optional[str]) -> dict[str, str]:
    if not raw:
        return {}
    out: dict[str, str] = {}
    for m in _LABEL_RE.finditer(raw):
        key, val = m.group(1), m.group(2)
        out[key] = val.replace('\\"', '"').replace("\\\\", "\\").replace("\\n", "\n")
    return out


@dataclass
class MetricSample:
    scrape_epoch_ms: int
    t_rel_s: float
    metric_name: str
    origin: str
    value: float


class MetricsScraper:
    """Background poller for a Prometheus text endpoint."""

    def __init__(
        self,
        config: dict,
        t0: float,
        interval_s: float = 1.0,
        endpoint: Optional[str] = None,
    ):
        self.endpoint = endpoint or config["metrics_endpoint"]
        self.origin_label = config.get("origin_label", "origin")
        self.t0 = t0
        self.interval_s = interval_s
        # Flat set of base names to match by prefix.
        self.base_names: list[str] = []
        for group in ("adaptive", "cooldown", "static_limiter"):
            self.base_names.extend(config.get(group, {}).values())
        self.samples: list[MetricSample] = []
        self.discovered_names: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    # -- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self.interval_s + 2.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.time()
            self.scrape_once()
            elapsed = time.time() - started
            self._stop.wait(max(0.0, self.interval_s - elapsed))

    # -- scraping ---------------------------------------------------------

    def scrape_once(self) -> int:
        """Scrape once, keep matching series, return count kept."""
        try:
            with urllib.request.urlopen(self.endpoint, timeout=5.0) as resp:
                text = resp.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001 - a scrape miss is not fatal
            return 0
        scrape_ms = int(time.time() * 1000)
        t_rel = scrape_ms / 1000.0 - self.t0
        kept = 0
        rows: list[MetricSample] = []
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            m = _LINE_RE.match(line)
            if not m:
                continue
            name = m.group("name")
            base = self._match_base(name)
            if base is None:
                continue
            value = _parse_value(m.group("value"))
            if value is None:
                continue
            labels = _parse_labels(m.group("labels"))
            origin = labels.get(self.origin_label, "")
            rows.append(MetricSample(scrape_ms, round(t_rel, 4), base, origin, value))
            kept += 1
            self.discovered_names.add(name)
        with self._lock:
            self.samples.extend(rows)
        return kept

    def _match_base(self, scraped_name: str) -> Optional[str]:
        """Return the configured base name this scraped series maps to, by
        prefix, or None. Longest base wins so `_total` variants are not
        shadowed by a shorter prefix."""
        best: Optional[str] = None
        for base in self.base_names:
            if scraped_name == base or scraped_name.startswith(base):
                if best is None or len(base) > len(best):
                    best = base
        return best

    # -- queries over the collected samples -------------------------------

    def series(
        self, base: str, origin: Optional[str] = None
    ) -> list[MetricSample]:
        with self._lock:
            out = [s for s in self.samples if s.metric_name == base]
        if origin is not None:
            out = [s for s in out if s.origin == origin]
        return sorted(out, key=lambda s: s.scrape_epoch_ms)

    def origins(self, base: str) -> set[str]:
        with self._lock:
            return {s.origin for s in self.samples if s.metric_name == base}

    def latest_in_window(
        self, base: str, origin: str, t_lo: float, t_hi: float
    ) -> Optional[float]:
        pts = [
            s
            for s in self.series(base, origin)
            if t_lo <= s.t_rel_s <= t_hi
        ]
        return pts[-1].value if pts else None

    def min_in_window(
        self, base: str, origin: str, t_lo: float, t_hi: float
    ) -> Optional[float]:
        pts = [
            s.value
            for s in self.series(base, origin)
            if t_lo <= s.t_rel_s <= t_hi
        ]
        return min(pts) if pts else None

    def max_in_window(
        self, base: str, origin: str, t_lo: float, t_hi: float
    ) -> Optional[float]:
        pts = [
            s.value
            for s in self.series(base, origin)
            if t_lo <= s.t_rel_s <= t_hi
        ]
        return max(pts) if pts else None

    def counter_delta(
        self, base: str, origin: str, t_lo: float, t_hi: float
    ) -> Optional[float]:
        """last - first for a monotonic counter over [t_lo, t_hi]."""
        pts = [
            s
            for s in self.series(base, origin)
            if t_lo <= s.t_rel_s <= t_hi
        ]
        if len(pts) < 1:
            return None
        return pts[-1].value - pts[0].value

    # -- persistence ------------------------------------------------------

    def write_csv(self, path: str) -> None:
        import csv

        with self._lock:
            rows = list(self.samples)
        rows.sort(key=lambda s: (s.scrape_epoch_ms, s.metric_name, s.origin))
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["scrape_epoch_ms", "t_rel_s", "metric_name", "origin", "value"])
            for s in rows:
                w.writerow([s.scrape_epoch_ms, s.t_rel_s, s.metric_name, s.origin, s.value])

    def write_discovered(self, path: str) -> None:
        with open(path, "w") as f:
            for name in sorted(self.discovered_names):
                f.write(name + "\n")


def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


if __name__ == "__main__":
    # Standalone probe: scrape once and print the harness-relevant series and
    # the origin label values, so the exact names / label form can be pinned
    # from a live endpoint before a scored run.
    import argparse
    import os

    p = argparse.ArgumentParser(description="probe the /metrics endpoint once")
    here = os.path.dirname(os.path.abspath(__file__))
    p.add_argument("--config", default=os.path.join(here, "metrics_config.json"))
    p.add_argument("--endpoint", default=None)
    args = p.parse_args()

    cfg = load_config(args.config)
    sc = MetricsScraper(cfg, t0=time.time(), endpoint=args.endpoint)
    kept = sc.scrape_once()
    print(f"endpoint: {sc.endpoint}")
    print(f"kept {kept} harness-relevant samples")
    print("discovered series names:")
    for name in sorted(sc.discovered_names):
        print(f"  {name}")
    print("per-base latest value by origin:")
    for base in sc.base_names:
        for origin in sorted(sc.origins(base)):
            pts = sc.series(base, origin)
            if pts:
                print(f"  {base}{{origin={origin!r}}} = {pts[-1].value}")
