# Caching TTL

The `caching_ttl` parameter controls how long data is considered fresh when using the `caching` refresh mode. Rows older than this TTL are treated as stale and are refreshed from the source.

## Default Value

When not explicitly configured, `caching_ttl` defaults to **30 seconds**.

This default is applied in two places internally:
- During stale row refresh loops (`refresh_task.rs`)
- During automatic cache retention setup at startup (`datafusion/mod.rs`)

## Configuration

`caching_ttl` is set as a parameter under the `acceleration` block of a dataset definition. It accepts a duration string compatible with Spice's duration parser (e.g., `30s`, `5m`, `1h`, `1d`).

```yaml
datasets:
  - from: postgres:my_table
    name: my_table
    acceleration:
      enabled: true
      refresh_mode: caching
      params:
        caching_ttl: 5m
```

## Retention Eviction at Startup

When a dataset uses `caching` refresh mode and `caching_stale_if_error` is **not enabled** (the default), the runtime automatically configures a retention policy at startup based on `caching_ttl` and `caching_stale_while_revalidate_ttl`:

```
retention_period = caching_ttl + caching_stale_while_revalidate_ttl
```

This retention policy periodically evicts rows whose `cache_refreshed_at` timestamp is older than the combined TTL. The check interval is the maximum of the retention period and 30 seconds.

If the dataset already has a user-specified `retention_period`, a warning is emitted and the user-specified retention is overridden by the automatic cache retention in caching mode.

## Related Parameters

- **`caching_stale_while_revalidate_ttl`** — Allows serving stale (cached) data while a refresh is in progress, for this duration beyond `caching_ttl`. Defaults to no grace period.
- **`caching_stale_if_error`** — When enabled, allows serving stale data if the source returns an error. When disabled (default), the automatic retention described above is active.

## How It Works

1. The `caching_ttl` is parsed from dataset acceleration params at initialization (`acceleration.rs`).
2. It is passed through to the accelerated table builder and the refresh task.
3. The stale row refresh loop (`refresh_stale_cached_rows`) identifies rows that have not been refreshed within the TTL window and re-fetches them from the federated source.
4. At startup, if automatic retention is active, the retention check task periodically evicts rows older than `caching_ttl + caching_stale_while_revalidate_ttl`.
