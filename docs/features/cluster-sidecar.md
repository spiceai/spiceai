# Application sidecars

Set `runtime.mode: sidecar` when Spice serves an application's queries from the
same pod or host. The application keeps using its existing local SQL endpoint.
The sidecar uses the SQL results cache and configured accelerations before
reading a remote Spice source according to the dataset's existing policies.

```yaml
version: v1
kind: Spicepod
name: orders-sidecar
runtime:
  mode: sidecar
datasets:
  - from: spice.ai:orders
    name: orders
    params:
      spiceai_endpoint: http://spice-cluster:50051
```

`spiceai:orders` is also supported. Spice Cloud datasets use their existing
organization, application, region, and authentication parameters. Use TLS and
the connector's existing authentication settings when connecting across hosts.

In sidecar mode, Spice plans queries locally and sends supported remote
fragments as Substrait over Arrow Flight SQL. The upstream Spice runtime creates
physical plans and executes those fragments. Unsupported operators execute in
the sidecar when the query can be decomposed without changing its meaning.
An upstream that cannot execute the required Substrait plan returns an error;
upgrade it or select `runtime.mode: server` to use SQL federation.

For a local working set, add the existing acceleration configuration:

```yaml
    acceleration:
      enabled: true
      engine: cayenne
      mode: memory
```

Use Cayenne's existing file mode for persistent local storage. Sidecar mode does
not automatically replicate datasets, change refresh schedules, or replace a
dataset's configured fallback policy. An empty accelerated result does not
automatically trigger a remote query. Cache TTL, stale serving, invalidation,
and explicit request cache controls keep their existing meanings.

Sidecars default local queries to one target partition so independent queries
can share their CPU budget. Override this with `runtime.query.target_partitions`
when appropriate for your workload. Query admission and memory limits still
apply. Explicit resource and cache settings take precedence over defaults.

The upstream results cache can key Substrait requests by a hash of their
serialized plan when `runtime.caching.sql_results.cache_key_type: plan` is set.
Identical plan bytes use the same key within the caller's cache namespace.
Normal cache eligibility and freshness rules still apply. Substrait requests
bypass a cache configured for SQL keys because they contain no SQL text.

Omitting `runtime.mode` selects `server`. The mode takes effect at startup;
restart Spice to change it. A sidecar cannot also be a scheduler or executor.
Ports, bindings, authentication, and readiness settings are configured as usual.
For rollback, select `server` and restart; configured acceleration files remain.

The [Enhancement](https://github.com/spiceai/spiceai/issues/15707) records the
contract. [The native qualification driver](../../test/scripts/sidecar.py)
compares SQLite answers with both federation paths, exercises local serving
during an upstream outage, and drives the full client → sidecar → standalone
upstream path with `testoperator` in interleaved SQL/Substrait arms. Example:

```bash
python3 test/scripts/sidecar.py \
  --spiced target/release/spiced \
  --testoperator target/release/testoperator \
  --out /tmp/sidecar-qualification \
  --concurrency 2 8 32 128 --duration 30
```

The driver holds CPU, memory, admission, and local partitions constant and
measures remote cache misses by default. Use `--cache-states hit` for a separate
measurement of sidecar-cache hits. It keeps
commands, provenance, expected/returned rows, plans, metrics, and benchmark
outputs in the run directory. Add `--executors 2` for separate distributed
cluster qualification.
