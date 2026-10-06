# HTTP rate control — catalog results

`./run_ratecontrol.sh all`, **21/21 PASS**, against `spiced
v2.4.0-unstable-build.c7460de8c6+models.metal` — the tip of
`14136-adaptive-rate-control` (PR #14143), built with the `rate-control` cargo
feature. macOS, 10 cores, everything on localhost, 2026-10-06.

Every scenario: a 20 rps budget per origin, `runtime.source_rate_control.refresh_interval: 1s`
where a cluster is involved, `rate_control_failure_threshold: 20%` unless
stated, both caches off, `max_retries: 0`. Rates are p50 / p99 / peak of
per-second arrivals at the origin, over each phase minus its settling window.

There is no static mode: rate control is always adaptive. The negative controls
are a tolerant threshold, latency without errors, and an error rate under the
threshold.

## One origin, one dataset, one replica

| scenario | warmup | fault | recovery | throttle / recover |
|---|---|---|---|---|
| `tolerant-threshold` (50% errors, 90% threshold) | 20/20 | **20/20** — unchanged, coefficient 1.000 | 20/20 | — |
| `throttle-503` | 20/21 | **3/5** | 20/21 | 13s / 5s |
| `throttle-429` | 20/21 | **3/5** | 20/21 | 14s / 5s |
| `throttle-timeout` (hang > `client_timeout`) | 20/21 | **3/5** | 20/21 | 14s / 5s |
| `throttle-refuse` (TCP RST) | 20/20 | **3/5** | 20/20 | 14s / 6s |
| `no-throttle-latency-only` (400ms, healthy) | 20/21 | **20/20** — unchanged | 20/20 | — |
| `below-threshold` (10% errors, 50% threshold) | 20/20 | **20/21** — unchanged | 20/20 | — |

A 429, a refused connection and a timeout are each the same failure signal as a
5xx. Latency alone is not, an error rate under the threshold is not, and a
threshold the error rate stays under is not.

## Several origins

| scenario | phase | p1 | p2 |
|---|---|---|---|
| `multi-origin-isolation` (p2 fails) | warmup | 20 | 20 |
| | fault | **21 — untouched** | **5 — throttled** |
| | recovery | 21 | 21 |
| `multi-origin-budgets` (p1 20 rps, p2 5 rps) | throughout | 20 | 5 |

One limiter per origin and no leakage between them: the healthy origin holds
its full budget through its neighbour's outage, and two origins hold two
different budgets at the same time.

## Several datasets on one origin

| scenario | phase | combined | d1 | d2 |
|---|---|---|---|---|
| `sameorigin-shared-budget` | warmup | **20/20** | 16 | 16 |
| `sameorigin-coupled-throttle` (d1's path fails) | warmup | 20/20 | 17 | 16 |
| | fault | **18/19** | **11** | **17** |
| | recovery | 20/20 | 14 | 16 |

Two saturated datasets on one origin stay inside **one** budget, not two.

When only d1's path fails, the origin's published admission coefficient falls to
**0.849** — on failures d2 never saw — but the effect on the co-tenant is small:
d1 loses about a third of its rate while d2 is unchanged and the origin's total
falls by 2 rps. "Dataset B is throttled by dataset A's failures" is not what
happens; B is admitted through a budget A's failures shrank.

`sameorigin-minority-failure` is the sharp edge of that: d1 gets 2 of 18 workers
and fails **every** request, is about 10% of the origin's traffic, and the
origin-wide error rate therefore stays under the 20% threshold. The coefficient
never moves and the origin keeps taking its full 20 rps.

`sameorigin-conflicting-config`: two datasets on one origin asking for different
limits is refused at start-up. The message now names all seven parameters that
must agree.

## Several replicas sharing one budget

| scenario | warmup | fault | recovery | implied vs published coefficient |
|---|---|---|---|---|
| `cluster-adaptive` (2 replicas) | 18/21 | **5/7** | 18/20 | 0.308 vs **0.302** |
| `cluster-tolerant-threshold` (control) | 18/19 | **18/19** — unchanged | 18/19 | both 1.000 |
| `cluster-asymmetric` (only r0 fails) | 19/21 | **12/13** (r0 7, r1 7) | 19/20 | 0.673 vs **0.672** |
| `cluster-single-replica` | 18/20 | **4/8** | 18/20 | 0.198 vs 0.289 |
| `cluster-three-replicas` | 19/20 | **6/9** | 18/18 | 0.317 vs **0.317** |
| `cluster-multi-origin` (p2 fails) | p1 20, p2 20 | **p1 20, p2 7** | p1 21, p2 20 | p2: 0.308 vs **0.296** |
| `cluster-sameorigin` (2 replicas x 2 datasets) | **19/20** | 19/20 | 19/20 | no fault |
| `cluster-adaptive-s3` (rustfs) | 20/20 | **6/10** | 19/19 | — |

Beyond the rates, from the shared state object and each replica's own
`/metrics`, across every cluster scenario:

- `sum(granted) <= burst_per_window` in **every window**. The throttled budget
  is no longer persisted — each replica derives it and holds it for the window —
  so this is the bound the design guarantees: a grant is capped at
  `effective_burst - granted_by_others`, and `effective_burst` at the
  configured burst.
- Published `failed` equals the origin's own non-200 count **exactly in total**
  in every run (176/176, 282/282, 217/217, 153/153, 179/179, 175/175), with no
  single window off by more than one, while hundreds of queries in the same runs
  were refused a permit. A permit-acquire timeout never reaches the origin and
  never reaches the failure counter.
- The coefficient recomputed from the shared counts matches the
  `adaptive_admission_ratio` the replicas published, to within 0.01 at two and
  three replicas. At one replica it is 0.09 apart, which is the limit of the
  method rather than a disagreement: the source-window offset is not cleanly
  identifiable with a single writer.
- Every replica publishes the same coefficient in every settled second and
  sweeps the same range of them, having exchanged no traffic with its peers.
- Zero lease-refresh errors and zero fail-closed requests, including over S3.

## Not covered

- Clock skew. Every replica here shares one machine clock.
- `requests_per_minute_limit` — only the per-second limit is exercised.
- Cluster half-lives longer than one window.
- IETF `RateLimit` / `RateLimit-Policy` headers and the `Retry-After` cooldown
  path — `run_phase2.sh` covers those for a single node.
- Hot reload of `runtime.state` (deferred to #14780).
