---
name: verify-request-metric-labels
description: Live-check that HTTP request metrics (http_requests, http_requests_total, http_requests_duration_ms) never take label values from caller-chosen headers, and that 401 responses carry no client dimension. Use after touching track_metrics in crates/runtime/src/http/routes.rs, RequestContext dimensions in crates/runtime-request-context, or the auth layer order.
---

# Verify request metric labels against a live runtime

The in-process regression tests are `request_header_values_do_not_add_metric_series` and
`unauthenticated_requests_carry_no_client_dimension` in `crates/runtime/tests/metrics.rs`.
This skill checks the same properties end to end against a built `spiced`.

## Setup

In a scratch directory (not the repo), create a spicepod with API-key auth and no datasets:

```yaml
version: v1
kind: Spicepod
name: verify-metric-labels
runtime:
  auth:
    api-key:
      enabled: true
      keys: [verify-key]
```

Run `spiced --http 127.0.0.1:18090 --metrics 127.0.0.1:19090`.

## Checks

1. Record the baseline series count: `curl -s 127.0.0.1:19090/metrics | grep -c '^http_requests'`.
2. Using any HTTP client, send a few hundred `GET /v1/datasets` requests **without** an API key,
   each with a different `User-Agent` value. Every one must get `401`.
3. Send the same number **with** `X-API-Key: verify-key`, each with a different `User-Agent` value.
4. Scrape `/metrics` again:
   - The `http_requests*` series count has grown by a small constant (one series per
     method/path/status/client), not by the number of requests.
   - `http_requests_total{...,path="/v1/datasets",status="401"}` has `method`, `path`, `status`
     and `protocol` labels, and **no** `client`, `runtime`, `runtime_version` or `runtime_system`.
   - The authenticated series has `client="other"`, plus the known client name for a real
     Spice SDK `User-Agent` (for example `spicepy/3.0`).
   - No line contains `otel_metric_overflow`.
5. A further authenticated request increments the `status="200"` series for `/v1/datasets`.

Stop `spiced` and delete the scratch directory afterwards.
