# HTTP rate control — catalog results

`./run_ratecontrol.sh all`, 20/20 PASS, against `spiced
v2.4.0-unstable-build.41116b5ae9+models.metal` built from
`2edb8ab231` (branch `14575-cluster-adaptive-rate-control`) with the
`rate-control` cargo feature. macOS, 10 cores, everything on localhost,
2026-10-01.

Every scenario: a 20 rps budget per origin, `refresh_interval: 1s` where a
cluster is involved, `rate_control_failure_threshold: 20%` unless stated, both
caches off, `max_retries: 0`. Rates are p50 / p99 / peak of per-second arrivals
at the origin, over each phase minus its settling window.

## One origin, one dataset, one replica

| scenario | warmup | fault | recovery | throttle / recover |
|---|---|---|---|---|
| `static-limit` (control) | 20/21 | **20/21** — unchanged | 20/20 | — |
| `adaptive-503` | 20/20 | **3/5** | 20/21 | 14s / 5s |
| `adaptive-429` | 20/20 | **3/5** | 20/20 | 13s / 5s |
| `adaptive-timeout` (hang > `client_timeout`) | 20/21 | **3/5** | 20/20 | 15s / 6s |
| `adaptive-refuse` (TCP RST) | 20/20 | **3/5** | 20/20 | 13s / 5s |
| `adaptive-latency-only` (400ms, healthy) | 20/20 | **20/21** — unchanged | 20/20 | — |
| `below-threshold` (10% errors, 50% threshold) | 20/21 | **20/20** — unchanged | 20/20 | — |

A 429, a refused connection and a timeout are each the same failure signal as a
5xx. Latency alone is not, and an error rate under the threshold is not. Static
mode never throttles, which is what makes the other rows mean something.

## Several origins

| scenario | phase | p1 | p2 |
|---|---|---|---|
| `multi-origin-isolation` (p2 fails) | warmup | 20 | 20 |
| | fault | **20 — untouched** | **5 — throttled** |
| | recovery | 20 | 20 |
| `multi-origin-budgets` (p1 20 rps, p2 5 rps) | warmup | 20 | 5 |

One limiter per origin, one adaptive controller per origin, and no leakage
between them: the healthy origin holds its full budget through its neighbour's
outage, and two origins hold two different budgets at the same time.

## Several datasets on one origin

| scenario | phase | combined | d1 | d2 |
|---|---|---|---|---|
| `sameorigin-shared-budget` | warmup | **20/21** | 19 | 19 |
| `sameorigin-coupled-throttle` (d1's path fails) | warmup | 20/21 | 17 | 20 |
| | fault | **18** | **11** | **17** |
| | recovery | 20/20 | 17 | 16 |

Two saturated datasets on one origin stay inside **one** budget, not two.

When only d1's path fails, the origin's published admission coefficient falls to
**0.82** — on failures d2 never saw — but the effect on the co-tenant is small.
Over five runs d1 loses about half its rate every time (14→10, 19→9, 17→9,
21→9, 17→11) while d2 stays flat (18→16, 16→16, 14→16, 18→17, 20→17) and the
origin's total falls by only 1–2 rps. "Dataset B is throttled by dataset A's
failures" is not what happens; B is admitted through a budget A's failures
shrank, which is still more than a per-dataset limiter would do, but a failing
tenant on a busy origin barely moves the origin's load.

`sameorigin-conflicting-config`: two datasets on one origin asking for
different limits is refused at start-up — *"Multiple HTTP-based components
target http://127.0.0.1:9001 with different rate-control settings."*

## Several replicas sharing one budget

| scenario | warmup | fault | recovery | notes |
|---|---|---|---|---|
| `cluster-adaptive` (2 replicas) | 18/20 | **6/7** | 18/20 | throttles in 2s, recovers in 2s |
| `cluster-static` (control) | 18/20 | **18/20** — unchanged | 17/21 | |
| `cluster-asymmetric` (only r0 fails) | 19/21 | **14/16** (r0 7, r1 9) | 19/20 | coefficient 0.40; r1 saw only 200s |
| `cluster-single-replica` | 19/20 | **5/6** | 19/19 | one replica, the whole budget |
| `cluster-three-replicas` | 18/19 | **6/9** | 18/19 | the budget does not grow with the fleet |
| `cluster-multi-origin` (p2 fails) | p1 20, p2 20 | **p1 21, p2 6** | p1 19, p2 19 | one shared budget per origin |
| `cluster-sameorigin` (2 replicas x 2 datasets) | **18/19** | — | — | one budget over the whole cross-product |
| `cluster-adaptive-s3` (rustfs) | 20/20 | **6/10** | 19/19 | same behaviour over an object store |

Beyond the rates, from the shared state object and each replica's own
`/metrics`, across every cluster scenario:

- `sum(granted) <= effective_burst` in **every window**.
- Published `failed` equals the origin's own non-200 count **per window** —
  62/62 windows in most runs — while hundreds of queries in the same runs were
  refused a permit. A permit-acquire timeout never reaches the origin and never
  reaches the failure counter.
- `effective_burst` re-derived from the published formula agrees in 54–62 of 62
  windows, and the best-fitting source-window offset is 2 in every run where
  the coefficient moved.
- Every replica publishes the same `cluster_effective_burst` and
  `adaptive_admission_ratio` in **every settled second** (55/55 with three
  replicas), and all three walked the same 14 distinct budgets, having
  exchanged no traffic with each other.
- Zero lease-refresh errors and zero fail-closed requests. On the S3 backend
  the optimistic-concurrency path is exercised for real: ~10 conflicts over 90s,
  each retried into a successful write.

## Not covered

- Clock skew. Every replica here shares one machine clock, so the design's
  claim that window-identifier decay removes the dependence on a clock reading
  is untested against real skew.
- `requests_per_minute_limit` — only the per-second limit is exercised.
- Half-lives longer than one window in cluster mode (`rate_control_window` >
  `refresh_interval`); only the rounding message is checked.
- IETF `RateLimit` / `RateLimit-Policy` advertised-quota headers, and the
  `Retry-After` cooldown path — `run_phase2.sh` covers those for a single node.
