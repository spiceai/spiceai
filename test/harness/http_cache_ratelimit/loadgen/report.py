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

"""Render a run directory (any phase) into one static, self-contained HTML report.

Reads only what a run already writes -- ``samples.csv``, ``metrics.csv`` (Phase
2 only), ``driver_events.csv``, and ``assertions.json`` -- so it works on any
past run without re-running anything. No third-party dependency: charts are
hand-drawn inline SVG, so the output opens in a browser with no server and no
network access.

Usage:
  python loadgen/report.py --run-dir /tmp/http_cache_phase2_run/ratecontrol-sre
  # writes <run-dir>/report.html by default; --out to change the path.

The phase is auto-detected from which files/columns are present:
  Phase 0 - samples.csv has no ``phase``/``fault_id`` column.
  Phase 1 - samples.csv has ``fault_id`` but no ``origin_name``/``dataset``.
  Phase 2 - metrics.csv exists (dual-origin, rate-control time series).
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
from html import escape
from typing import Any, Optional


# --------------------------------------------------------------------------
# Data loading
# --------------------------------------------------------------------------


def _read_csv(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_json(path: str) -> Optional[dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _num(row: dict[str, str], key: str) -> Optional[float]:
    v = row.get(key)
    if v is None or v == "" or v == "None":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _read_origin_logs(run_dir: str) -> list[dict[str, Any]]:
    """Read every ``origin_p*.jsonl`` arrival log in ``run_dir`` (one per
    origin for Phase 1/2), tagged with that origin's label. Falls back to
    Phase 0's hardcoded default log path if none is found in ``run_dir`` --
    Phase 0 predates writing the origin log into the run directory.

    Only ``GET /data*`` arrivals are kept (``/control`` POSTs and HEAD
    probes are dropped) since those are the only rows that represent an
    actual upstream fetch.
    """
    paths = sorted(glob.glob(os.path.join(run_dir, "origin_p*.jsonl")))
    if not paths:
        fallback = "/tmp/origin_p1.jsonl"
        if os.path.exists(fallback):
            paths = [fallback]

    rows: list[dict[str, Any]] = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("method") != "GET" or not str(rec.get("path", "")).startswith("/data"):
                    continue
                rows.append(rec)
    return rows


def _read_spicepod(run_dir: str) -> tuple[Optional[str], Optional[str]]:
    """Return ``(label, yaml_text)`` for the spicepod the run actually used.

    Phase 1/2 copy the resolved pod into the run directory
    (``spicepod.resolved.yaml``), which is authoritative -- it reflects
    whatever the orchestration script actually substituted. Phase 0 predates
    that copy, so fall back to its known, hardcoded source pod relative to
    this script (``loadgen/`` -> harness root -> ``spicepod/spicepod.swr.yaml``).
    """
    resolved = os.path.join(run_dir, "spicepod.resolved.yaml")
    if os.path.exists(resolved):
        with open(resolved, encoding="utf-8") as f:
            return "spicepod.resolved.yaml", f.read()
    harness_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    fallback = os.path.join(harness_root, "spicepod", "spicepod.swr.yaml")
    if os.path.exists(fallback):
        with open(fallback, encoding="utf-8") as f:
            return "spicepod/spicepod.swr.yaml (Phase 0 default; not copied per-run)", f.read()
    return None, None


_STATUS_NAMES = {
    429: "Too Many Requests, a retryable failure",
    500: "Internal Server Error, a retryable failure",
    502: "Bad Gateway, a retryable failure",
    503: "Service Unavailable, a retryable failure",
    504: "Gateway Timeout, a retryable failure",
}

_MODE_DESCRIPTIONS = {
    "status": "return an HTTP status instead of the real response",
    "hang": "sleep before responding, to force a client-side timeout",
    "refuse": "abort the TCP connection with a RST — the client sees a "
    "connection error, not an HTTP status",
    "latency": "no error at all, just added latency on every response",
    "healthy": "no fault — steady state",
}


def describe_fault(config: dict[str, Any], phase: int) -> Optional[str]:
    """A plain-English "what we simulated" bullet list from a run's
    ``assertions.json`` config, or ``None`` when the phase injects no fault
    (Phase 0)."""
    profile = config.get("fault_profile")
    if not profile:
        return None

    mode = profile.get("mode", "healthy")
    bullets: list[str] = [
        f"<code>mode: {escape(str(mode))}</code> — {escape(_MODE_DESCRIPTIONS.get(mode, mode))}."
    ]

    if mode == "status":
        status = profile.get("error_status")
        name = _STATUS_NAMES.get(status, "")
        suffix = f" ({name})" if name else ""
        bullets.append(f"<code>error_status: {escape(str(status))}</code>{escape(suffix)}.")

    if mode == "hang":
        hang_ms = profile.get("timeout_hang_ms")
        bullets.append(
            f"<code>timeout_hang_ms: {escape(str(hang_ms))}</code> — the origin waits "
            f"{escape(str(hang_ms))}ms before responding; the client's "
            "<code>client_timeout</code> decides whether that becomes a timeout error."
        )

    if mode in ("status", "hang", "refuse") and profile.get("error_rate") is not None:
        rate = float(profile["error_rate"])
        pct = round(rate * 100)
        bullets.append(
            f"<code>error_rate: {rate:g}</code> — {pct}% of requests get the fault; "
            f"the other {100 - pct}% succeed normally."
        )

    lat = profile.get("latency_ms")
    if lat and (lat.get("base") or lat.get("jitter")):
        base, jitter = lat.get("base", 0), lat.get("jitter", 0)
        if mode == "latency":
            bullets.append(
                f"<code>latency_ms: {{base: {base}, jitter: {jitter}}}</code> — every "
                f"response is slowed by ~{base}-{base + jitter}ms, no errors regardless "
                "of <code>error_rate</code>."
            )
        else:
            bullets.append(
                f"<code>latency_ms: {{base: {base}, jitter: {jitter}}}</code> — every "
                f"response (faulted or healthy) also carries a ~{base}-{base + jitter}ms "
                "slowdown, just to keep it realistic."
            )

    headers = profile.get("headers")
    if headers:
        pairs = ", ".join(f"{k}: {v}" for k, v in headers.items())
        bullets.append(
            f"<code>headers: {{{escape(pairs)}}}</code> — advertised on every faulted "
            "response, so cooldown/advertised-quota behavior can be exercised."
        )

    # Fault window + seed, phase-specific field names.
    if "fault_window_s" in config:
        start, end = config["fault_window_s"]
    elif "warmup_s" in config and "fault_s" in config:
        start, end = config["warmup_s"], config["warmup_s"] + config["fault_s"]
    else:
        start = end = None
    if start is not None:
        seed = profile.get("seed")
        seed_note = (
            f", seeded RNG (<code>seed: {escape(str(seed))}</code>) so the exact "
            "pattern replays deterministically"
            if seed is not None
            else ""
        )
        bullets.append(
            f"Fault window: <code>t={start:g}s</code> to <code>t={end:g}s</code> "
            f"({end - start:g}s){seed_note}."
        )

    if phase == 2:
        bullets.append(
            "Injected at origin <code>p2</code> only — <code>p1</code> stays healthy the "
            "entire run, which is how per-origin isolation is proven."
        )
    elif "origin_name" in config:
        bullets.append(f"Injected at origin <code>{escape(str(config['origin_name']))}</code>.")

    return '<ul class="fault-desc">' + "".join(f"<li>{b}</li>" for b in bullets) + "</ul>"


def detect_phase(samples: list[dict[str, str]], has_metrics: bool) -> int:
    if has_metrics:
        return 2
    if samples and "fault_id" in samples[0]:
        return 1
    return 0


# --------------------------------------------------------------------------
# Tiny inline-SVG chart primitives (no JS, no external libraries)
# --------------------------------------------------------------------------

_COLORS = ["#2563eb", "#dc2626", "#16a34a", "#d97706", "#7c3aed", "#0891b2"]


def _fmt_tick(v: float) -> str:
    """Format an axis tick without ever switching to scientific notation
    (``{:.3g}`` does, e.g. ``1.14e+04`` for a millisecond latency value)."""
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    if abs(v) >= 1:
        return f"{v:.1f}".rstrip("0").rstrip(".")
    return f"{v:.3f}"


def _percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile (0-100) of ``values``. ``values`` need not be sorted."""
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, min(len(s) - 1, math.ceil(pct / 100.0 * len(s)) - 1))
    return s[idx]


def _scale(value: float, lo: float, hi: float, out_lo: float, out_hi: float) -> float:
    if hi <= lo:
        return out_lo
    return out_lo + (value - lo) / (hi - lo) * (out_hi - out_lo)


def line_chart(
    series: dict[str, list[tuple[float, float]]],
    title: str,
    y_label: str,
    width: int = 860,
    height: int = 260,
    shaded_windows: Optional[list[tuple[float, float, str]]] = None,
    mode: str = "line",
) -> str:
    """A minimal multi-series chart as a standalone inline SVG.

    ``series`` maps a legend label to a list of ``(x, y)`` points, already
    sorted by x. ``shaded_windows`` draws translucent vertical bands (e.g. a
    fault window) as ``(x_start, x_end, color)``. ``mode="line"`` connects
    each series with a polyline (for a bucketed/aggregated time series);
    ``mode="scatter"`` draws one small circle per point with no connecting
    line (for raw per-response points, where a line would imply a
    continuity between unrelated individual responses that isn't there).
    """
    pad_l, pad_r, pad_t, pad_b = 56, 16, 28, 32
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    all_x = [x for pts in series.values() for x, _ in pts]
    all_y = [y for pts in series.values() for _, y in pts]
    if not all_x:
        return f'<div class="chart-empty">{escape(title)}: no data</div>'
    x_lo, x_hi = min(all_x), max(all_x)
    y_lo, y_hi = min(0.0, min(all_y)), max(all_y)
    if y_hi == y_lo:
        y_hi = y_lo + 1.0

    def px(x: float) -> float:
        return pad_l + _scale(x, x_lo, x_hi, 0, plot_w)

    def py(y: float) -> float:
        return pad_t + _scale(y, y_lo, y_hi, plot_h, 0)

    parts: list[str] = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" '
        f'class="chart" role="img" aria-label="{escape(title)}">'
    ]
    parts.append(f'<text x="{pad_l}" y="16" class="chart-title">{escape(title)}</text>')

    # Shaded windows (drawn first, under the axes/lines).
    for x0, x1, color in shaded_windows or []:
        x0c, x1c = max(x0, x_lo), min(x1, x_hi)
        if x1c <= x0c:
            continue
        parts.append(
            f'<rect x="{px(x0c):.1f}" y="{pad_t}" width="{px(x1c) - px(x0c):.1f}" '
            f'height="{plot_h}" fill="{color}" opacity="0.10" />'
        )

    # Axes.
    parts.append(
        f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{pad_l + plot_w}" '
        f'y2="{pad_t + plot_h}" class="axis" />'
    )
    parts.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + plot_h}" class="axis" />')
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y_val = y_lo + frac * (y_hi - y_lo)
        y_px = py(y_val)
        parts.append(
            f'<line x1="{pad_l}" y1="{y_px:.1f}" x2="{pad_l + plot_w}" y2="{y_px:.1f}" class="grid" />'
        )
        parts.append(f'<text x="{pad_l - 6}" y="{y_px + 3:.1f}" class="tick tick-y">{_fmt_tick(y_val)}</text>')
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        x_val = x_lo + frac * (x_hi - x_lo)
        parts.append(
            f'<text x="{px(x_val):.1f}" y="{pad_t + plot_h + 16}" class="tick tick-x">{x_val:.0f}s</text>'
        )
    parts.append(
        f'<text x="{pad_l - 40}" y="{pad_t - 10}" class="tick">{escape(y_label)}</text>'
    )

    # Series lines.
    legend_parts: list[str] = []
    for i, (label, pts) in enumerate(series.items()):
        if not pts:
            continue
        color = _COLORS[i % len(_COLORS)]
        if mode == "scatter":
            circles = "".join(
                f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="2.2" fill="{color}" opacity="0.5" />'
                for x, y in pts
            )
            parts.append(circles)
        else:
            path_d = " ".join(
                f"{'M' if j == 0 else 'L'}{px(x):.1f},{py(y):.1f}" for j, (x, y) in enumerate(pts)
            )
            parts.append(f'<path d="{path_d}" fill="none" stroke="{color}" stroke-width="1.6" />')
        legend_parts.append(
            f'<span class="legend-item"><span class="swatch" style="background:{color}"></span>{escape(label)}</span>'
        )

    parts.append("</svg>")
    parts.append(f'<div class="legend">{"".join(legend_parts)}</div>')
    return "".join(parts)


def bucketed_counts(
    rows: list[dict[str, str]],
    t_key: str,
    category_key: str,
    bucket_s: float = 1.0,
) -> dict[str, list[tuple[float, float]]]:
    """Count rows per (time bucket, category) -> one series per category."""
    buckets: dict[tuple[float, str], int] = {}
    for r in rows:
        t = _num(r, t_key)
        if t is None:
            continue
        b = math.floor(t / bucket_s) * bucket_s
        raw_cat = r.get(category_key)
        cat = str(raw_cat) if raw_cat is not None else "unknown"
        buckets[(b, cat)] = buckets.get((b, cat), 0) + 1
    series: dict[str, list[tuple[float, float]]] = {}
    for (b, cat), count in buckets.items():
        series.setdefault(cat, []).append((b, float(count) / bucket_s))
    for pts in series.values():
        pts.sort(key=lambda p: p[0])
    return series


def numeric_series(
    rows: list[dict[str, str]], t_key: str, y_key: str, group_key: Optional[str] = None
) -> dict[str, list[tuple[float, float]]]:
    series: dict[str, list[tuple[float, float]]] = {}
    for r in rows:
        t, y = _num(r, t_key), _num(r, y_key)
        if t is None or y is None:
            continue
        label = r.get(group_key, y_key) if group_key else y_key
        series.setdefault(label or y_key, []).append((t, y))
    for pts in series.values():
        pts.sort(key=lambda p: p[0])
    return series


def bucketed_percentiles(
    rows: list[dict[str, Any]],
    t_key: str,
    y_key: str,
    percentiles: list[float],
    group_key: Optional[str] = None,
    bucket_s: float = 1.0,
) -> dict[str, list[tuple[float, float]]]:
    """One series per (group, percentile): a mean hides exactly the bimodal
    fast (cache hit) / slow (origin fetch) split that matters here, so this
    reports percentiles per time bucket instead -- p99 in particular, per
    this repo's rule against averages/p50 as evidence."""
    buckets: dict[tuple[float, Optional[str]], list[float]] = {}
    for r in rows:
        t, y = _num(r, t_key), _num(r, y_key)
        if t is None or y is None:
            continue
        b = math.floor(t / bucket_s) * bucket_s
        g = str(r.get(group_key)) if group_key and r.get(group_key) is not None else None
        buckets.setdefault((b, g), []).append(y)

    series: dict[str, list[tuple[float, float]]] = {}
    for (b, g), vals in buckets.items():
        for p in percentiles:
            p_label = "max" if p >= 100 else f"p{p:g}"
            label = f"{g} {p_label}" if g else p_label
            series.setdefault(label, []).append((b, _percentile(vals, p)))
    for pts in series.values():
        pts.sort(key=lambda x: x[0])
    return series


# --------------------------------------------------------------------------
# Report assembly
# --------------------------------------------------------------------------

_CSS = """
body { font: 14px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       margin: 0; padding: 24px; background: #f8fafc; color: #0f172a; }
h1 { font-size: 20px; margin: 0 0 4px; }
h2 { font-size: 15px; margin: 28px 0 8px; color: #334155; }
.verdict { display: inline-block; padding: 3px 10px; border-radius: 4px;
           font-weight: 600; font-size: 13px; }
.verdict.PASS { background: #dcfce7; color: #166534; }
.verdict.FAIL { background: #fee2e2; color: #991b1b; }
.verdict.BLOCKED { background: #fef9c3; color: #854d0e; }
.meta { color: #64748b; font-size: 12px; margin-bottom: 16px; }
.card { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px;
        padding: 12px 16px; margin-bottom: 14px; }
.pod-details summary { cursor: pointer; font-weight: 600; font-size: 13px;
        color: #334155; }
.pod-yaml { margin: 10px 0 0; padding: 10px 12px; background: #0f172a; color: #e2e8f0;
        border-radius: 6px; font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
        overflow-x: auto; white-space: pre; }
.fault-desc { margin: 0; padding-left: 20px; font-size: 13px; }
.fault-desc li { margin-bottom: 6px; }
.fault-desc code { background: #eef2f6; border-radius: 3px; padding: 1px 4px;
        font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; }
table.assertions { width: 100%; border-collapse: collapse; font-size: 13px; }
table.assertions td { padding: 4px 6px; border-bottom: 1px solid #eef2f6; vertical-align: top; }
.pass-badge { font-weight: 700; }
.pass-badge.ok { color: #16a34a; }
.pass-badge.fail { color: #dc2626; }
.chart { display: block; }
.chart-title { font-size: 12px; fill: #334155; font-weight: 600; }
.axis { stroke: #94a3b8; stroke-width: 1; }
.grid { stroke: #eef2f6; stroke-width: 1; }
.tick { font-size: 10px; fill: #64748b; }
.tick-x { text-anchor: middle; }
.tick-y { text-anchor: end; }
.legend { font-size: 11px; color: #475569; padding: 2px 0 8px; }
.legend-item { margin-right: 14px; }
.swatch { display: inline-block; width: 9px; height: 9px; border-radius: 2px;
          margin-right: 4px; position: relative; top: 1px; }
.chart-empty { color: #94a3b8; font-size: 12px; padding: 8px 0; }
"""

# Fault/driver-event colors reused for shading windows across every chart.
_FAULT_WINDOW_COLOR = "#dc2626"


def _driver_windows(driver_events: list[dict[str, str]]) -> list[tuple[float, float, str]]:
    """Best-effort fault window(s) from consecutive driver events: a window
    opens on any non-``healthy`` mode and closes on the next event."""
    windows: list[tuple[float, float, str]] = []
    open_at: Optional[float] = None
    for ev in sorted(driver_events, key=lambda e: _num(e, "t_rel_s") or 0.0):
        t = _num(ev, "t_rel_s")
        mode = ev.get("mode", "")
        if t is None:
            continue
        if mode and mode != "healthy" and open_at is None:
            open_at = t
        elif mode == "healthy" and open_at is not None:
            windows.append((open_at, t, _FAULT_WINDOW_COLOR))
            open_at = None
    return windows


def render(run_dir: str) -> str:
    samples = _read_csv(os.path.join(run_dir, "samples.csv"))
    metrics = _read_csv(os.path.join(run_dir, "metrics.csv"))
    driver_events = _read_csv(os.path.join(run_dir, "driver_events.csv"))
    verdict_obj = _read_json(os.path.join(run_dir, "assertions.json"))

    phase = detect_phase(samples, has_metrics=bool(metrics))
    windows = _driver_windows(driver_events)

    sections: list[str] = []

    scenario = (verdict_obj or {}).get("scenario", os.path.basename(run_dir.rstrip("/")))
    # Phase 0 predates the shared harness.oracle verdict shape and only ever
    # carries a boolean "pass"; Phase 1/2 carry an explicit "verdict" string.
    if verdict_obj is not None and "verdict" in verdict_obj:
        verdict = verdict_obj["verdict"]
    elif verdict_obj is not None and "pass" in verdict_obj:
        verdict = "PASS" if verdict_obj["pass"] else "FAIL"
    else:
        verdict = "UNKNOWN"
    sections.append(
        f'<h1>{escape(str(scenario))} '
        f'<span class="verdict {escape(verdict)}">{escape(verdict)}</span></h1>'
        f'<div class="meta">phase {phase} &middot; run dir: {escape(run_dir)} &middot; '
        f'{len(samples)} samples'
        + (f' &middot; {len(metrics)} metric points' if metrics else "")
        + "</div>"
    )

    fault_desc = describe_fault((verdict_obj or {}).get("config", {}), phase)
    sections.append(
        '<div class="card"><h2>What we simulated</h2>'
        + (fault_desc or "<p>No fault injected — steady-state freshness/absorption test only.</p>")
        + "</div>"
    )

    pod_label, pod_text = _read_spicepod(run_dir)
    if pod_text is not None:
        sections.append(
            '<details class="card pod-details"><summary>Spicepod: '
            f'{escape(pod_label or "")}</summary>'
            f'<pre class="pod-yaml">{escape(pod_text)}</pre></details>'
        )

    origin_rows = _read_origin_logs(run_dir)
    qps_group_key = "origin_name" if samples and "origin_name" in samples[0] else None

    # 1. QPS at the HTTP origin(s) -- true upstream arrival rate, grouped by
    # origin label (one series per origin; a single series if there is only
    # one). This is ground truth for "did the request actually reach
    # upstream", independent of anything spiced did with it.
    origin_qps_series = bucketed_counts(origin_rows, "t_rel_s", "origin", bucket_s=0.1)
    sections.append(
        '<div class="card"><h2>QPS at HTTP origin (0.1s buckets)</h2>'
        + line_chart(origin_qps_series, "origin arrivals/sec", "qps", shaded_windows=windows)
        + "</div>"
    )

    # 2. QPS at spiced -- the load generator's own send rate, i.e. what the
    # client observed, grouped by dataset/origin when Phase 2 runs two.
    if qps_group_key:
        qps_series = bucketed_counts(samples, "t_send_rel_s", qps_group_key, bucket_s=0.1)
    else:
        qps_series = bucketed_counts(
            [{**r, "_all": "queries"} for r in samples], "t_send_rel_s", "_all", bucket_s=0.1
        )
    sections.append(
        '<div class="card"><h2>QPS at spiced (client-observed, 0.1s buckets)</h2>'
        + line_chart(qps_series, "queries/sec", "qps", shaded_windows=windows)
        + "</div>"
    )

    # 3. QPS per status at the HTTP origin(s) -- one chart per origin (mixing
    # origins in one chart would mix unrelated status vocabularies), each
    # with one series per ``applied_status`` value (200, 503, "hang", ...).
    origin_groups: dict[str, list[dict[str, Any]]] = {}
    for r in origin_rows:
        origin_groups.setdefault(str(r.get("origin") or "unknown"), []).append(r)
    for origin_label, grp in sorted(origin_groups.items()):
        status_series = bucketed_counts(grp, "t_rel_s", "applied_status", bucket_s=0.1)
        sections.append(
            f'<div class="card"><h2>QPS per status at HTTP origin {escape(origin_label)}</h2>'
            + line_chart(status_series, "arrivals/sec by status", "qps", shaded_windows=windows)
            + "</div>"
        )

    # 4. QPS per status at spiced -- one chart per origin/dataset (Phase 2)
    # or one chart overall (Phase 0/1), each with one series per
    # ``http_status`` value.
    if qps_group_key:
        spiced_groups: dict[str, list[dict[str, str]]] = {}
        for r in samples:
            spiced_groups.setdefault(str(r.get(qps_group_key) or "unknown"), []).append(r)
    else:
        spiced_groups = {"spiced": samples}
    for label, grp in sorted(spiced_groups.items()):
        status_series = bucketed_counts(grp, "t_send_rel_s", "http_status", bucket_s=0.1)
        sections.append(
            f'<div class="card"><h2>QPS per status at spiced ({escape(label)})</h2>'
            + line_chart(status_series, "queries/sec by status", "qps", shaded_windows=windows)
            + "</div>"
        )

    # Latency at spiced: p50/p99/max per 1s bucket, split the same way as
    # (2). Never a mean/average -- a mean hides exactly the bimodal
    # fast-cache-hit / slow-origin-fetch split that matters for reading
    # caching behavior off this chart (see the scatter chart below for the
    # per-response view of that split).
    lat_group_key = qps_group_key
    lat_series = bucketed_percentiles(samples, "t_send_rel_s", "latency_ms", [50, 99, 100], group_key=lat_group_key)
    sections.append(
        '<div class="card"><h2>Latency at spiced: p50 / p99 / max per 1s bucket</h2>'
        + line_chart(lat_series, "latency_ms", "ms", shaded_windows=windows)
        + "</div>"
    )

    # Staleness (Phase 0/1 only): lag_versions over time.
    if samples and "lag_versions" in samples[0]:
        lag_series = numeric_series(samples, "t_send_rel_s", "lag_versions")
        sections.append(
            '<div class="card"><h2>Staleness (lag_versions = origin hwm - version served)</h2>'
            + line_chart(lag_series, "lag_versions", "versions", shaded_windows=windows)
            + "</div>"
        )
        # Freshness band counts per bucket (FRESH/STALE/EMPTY/ERROR).
        band_series = bucketed_counts(samples, "t_send_rel_s", "freshness")
        sections.append(
            '<div class="card"><h2>Response freshness band (count/s)</h2>'
            + line_chart(band_series, "freshness", "count/s", shaded_windows=windows)
            + "</div>"
        )

        # Per-response latency scatter, colored by freshness band -- this is
        # the "caching done by spiced" view: a FRESH response served from
        # the local cache should cluster near zero latency, while a STALE
        # response served during a synchronous refresh, or one that had to
        # wait on the origin, shows up as the high-latency tail. Bucketed
        # percentiles above can hide this if a band is a small fraction of
        # a given second's traffic; the raw scatter cannot.
        latency_by_band = numeric_series(samples, "t_send_rel_s", "latency_ms", group_key="freshness")
        sections.append(
            '<div class="card"><h2>Per-response latency by freshness band (caching behavior)</h2>'
            + line_chart(latency_by_band, "latency_ms", "ms", shaded_windows=windows, mode="scatter")
            + "</div>"
        )

    # Rate-control metrics (Phase 2 only): admission + effective limit per origin.
    if metrics:
        for metric_suffix, y_label in (
            ("adaptive_rate_control_admission_coefficient_permille", "admission permille"),
            ("adaptive_rate_control_effective_limit", "effective limit"),
        ):
            rows = [r for r in metrics if r.get("metric_name", "").endswith(metric_suffix)]
            series = numeric_series(rows, "t_rel_s", "value", group_key="origin")
            sections.append(
                f'<div class="card"><h2>{escape(metric_suffix)}</h2>'
                + line_chart(series, metric_suffix, y_label, shaded_windows=windows)
                + "</div>"
            )

    # Assertions table.
    assertions = (verdict_obj or {}).get("assertions", [])
    if assertions:
        rows_html = "".join(
            f'<tr><td class="pass-badge {"ok" if a.get("pass") else "fail"}">'
            f'{"PASS" if a.get("pass") else "FAIL"}</td>'
            f'<td>{escape(str(a.get("name", "")))}</td>'
            f'<td>{escape(str(a.get("detail", "")))}</td></tr>'
            for a in assertions
        )
        sections.append(
            '<div class="card"><h2>Assertions</h2>'
            f'<table class="assertions">{rows_html}</table></div>'
        )

    body = "\n".join(sections)
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{escape(str(scenario))} report</title><style>{_CSS}</style>"
        f"</head><body>{body}</body></html>"
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Render a harness run directory to a static HTML report")
    p.add_argument("--run-dir", required=True, help="Run directory (contains samples.csv, assertions.json, ...)")
    p.add_argument("--out", default=None, help="Output HTML path (default: <run-dir>/report.html)")
    args = p.parse_args()

    out_path = args.out or os.path.join(args.run_dir, "report.html")
    html = render(args.run_dir)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
