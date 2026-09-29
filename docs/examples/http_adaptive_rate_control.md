# Adaptive HTTP rate control on slow responses

For a single-node HTTPS or GraphQL dataset, `rate_control_mode: adaptive` reduces
requests to an origin when errors or slow responses exceed
`rate_control_failure_threshold` (default `10%`). It scales configured limits
without exceeding them. At least one of `requests_per_second_limit`,
`requests_per_minute_limit`, or `max_concurrent_requests` must be set.

```yaml
datasets:
  - from: https://api.example.com/v1/items
    name: items
    params:
      requests_per_second_limit: 10
      rate_control_mode: adaptive
      client_timeout: 30
      rate_control_slow_response_threshold: 2s
```

A successful response taking **strictly longer** than `2s` still returns the same
rows. It counts as `slow` for adaptive control, with the same effect on admission
as an error, but does not cause a retry or query error. A response at the threshold
counts as `success`. Requests exceeding `client_timeout` still fail normally.

The threshold accepts durations such as `2s` or `500ms`; unset or `0` disables it.
Static mode ignores it, including invalid values. There is no runtime-wide default.
The threshold must be less than the effective HTTPS `client_timeout` (in whole
seconds), or the GraphQL connector's fixed `30s` request timeout. Validation messages
render durations in seconds, including fractional values.

## Timing and outcomes

Each actual attempt is timed separately, immediately before sending through
complete body consumption. Concurrency admission, request quotas, jitter,
`Retry-After` waits and retry backoff are excluded. A body-read failure counts as
`failure`, not as an earlier header success.

| Complete attempt | Adaptive outcome | Query behavior |
| --- | --- | --- |
| `2xx`, at or below the threshold | `success` | Returns normally |
| `2xx`, above the threshold | `slow` | Returns normally, without a latency retry |
| Timeout, transport or body-read error | `failure` | Existing errors and retries |
| `408`, `429`, `5xx` | `failure` | Existing errors and retries; HTTPS does not retry `408` |
| Other `4xx` | Not recorded | Existing behavior |

Datasets on the same origin share a controller but may have different slow-response
thresholds. Every other rate-control setting must agree, including the acquire
timeout. Each dataset's latency classification feeds that shared controller.
For example, use `1s` for `/items` and `20s` for `/search`, with the same origin limits.
If their request timeouts differ, explicitly set the same
`rate_control_acquire_timeout` so its connector-derived defaults do not conflict.

## Choose a threshold from observed latency

Start with the threshold unset. Inspect `http_client_request_duration_ms` during
normal load, including representative response sizes, and choose a threshold above
normal high-percentile latency but below the request timeout. The histogram includes
body download time: a large healthy response may need a higher threshold.

The histogram records every sent attempt in every rate-control mode, with `origin`
and `http.response.status_code` attributes. The status attribute is absent when no
response headers arrived. Prometheus renders the status attribute as
`http_response_status_code` and exposes histogram `_bucket`, `_sum`, and `_count`
series. The origin is `scheme://host:port`, without paths, query strings or credentials.

For example, a five-minute origin P99 in milliseconds:

```promql
histogram_quantile(0.99,
  sum by (origin, le) (rate(http_client_request_duration_ms_bucket[5m])))
```

Use `0.999` for P99.9. Origin aggregation combines datasets on the same origin;
measure an endpoint independently when those datasets have different normal latency.

`rate_control_adaptive_outcomes_total` has `origin` and
`outcome=success|slow|failure` attributes. All three outcomes start at zero in adaptive
mode; the metric is absent in static mode. Like the other component rate-control
metrics, its exported name has the owning connector's `dataset_http_` or
`dataset_graphql_` prefix. Each origin has one metric owner, so datasets sharing a
controller do not double-count its outcomes.

Compare the proportion of `slow` and `failure` outcomes with
`rate_control_adaptive_admission_ratio`. A ratio of `1` admits the configured limits;
a smaller ratio applies backpressure. `rate_control_window` is the decay half-life
(default `10s`), controlling reaction and recovery. A slow response uses the existing
failure threshold; there is no separate slow-call percentage setting.

## Warnings and limits

When slow responses contribute to throttling, the warning names both causes:

```text
WARN Upstream 'https://api.example.com' is failing, or responding slower than its `rate_control_slow_response_threshold`, on more than the 10% `rate_control_failure_threshold`, so adaptive rate control is reducing requests to it below the configured limits until it recovers. See: https://spiceai.org/docs/components/data-connectors/https/deployment#rate-control
```

If the origin remains at its floor for a full `rate_control_window` and a dataset
still returns slow responses, the following warning is emitted once for that dataset:

```text
WARN Responses from 'https://api.example.com' for dataset 'items' still take longer than its 1s `rate_control_slow_response_threshold` at the minimum request rate, so the threshold may be below this API's normal response time. Check `http_client_request_duration_ms` and raise `rate_control_slow_response_threshold` for dataset 'items'.
```

Increase the threshold when the histogram shows that it is below normal response
time. The floor retains recovery probes; once the origin responds quickly again,
the admission ratio recovers toward `1`.

Support is limited to dynamic HTTPS API and GraphQL datasets on a single node.
Structured HTTP file datasets use the listing connector and do not support these
controls. Databricks does not declare or read this parameter. Adaptive mode is
rejected with cluster rate control (`runtime.source_rate_control.state_location`).
Low-traffic origins remain sensitive to individual outcomes; choose the failure
threshold and window accordingly. The threshold can only lower load to its own
origin; `rate_control_acquire_timeout` bounds queued requests' admission wait.
