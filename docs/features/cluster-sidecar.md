# Application sidecars

Start Spice with `--role sidecar` when it serves an application's queries from
the same pod or host. The application keeps using its existing local SQL
endpoint. The sidecar uses the SQL results cache and configured accelerations
before reading a remote Spice source according to the dataset's existing
policies.

```bash
spiced --role sidecar
```

```yaml
version: v1
kind: Spicepod
name: orders-sidecar
datasets:
  - from: spice.ai:orders
    name: orders
    params:
      spiceai_endpoint: http://spice-cluster:50051
```

`spiceai:orders` is also supported. Spice Cloud datasets use their existing
organization, application, region, and authentication parameters. Use TLS and
the connector's existing authentication settings when connecting across hosts.

As a sidecar, Spice plans queries locally and sends supported remote
fragments as Substrait over Arrow Flight SQL. The upstream Spice runtime creates
physical plans and executes those fragments. Unsupported operators execute in
the sidecar when the query can be decomposed without changing its meaning.
An upstream that cannot execute the required Substrait plan returns an error;
upgrade it, or restart the sidecar with `--role server` to use SQL federation.

Models and embeddings delegate to the cluster the same way. A `spiceai:` (or
`spice.ai:`) model or embedding names a model the upstream runtime serves, and
`spiceai_endpoint` is that runtime's HTTP endpoint, which serves the
OpenAI-compatible API:

```yaml
models:
  - from: spiceai:assistant
    name: assistant
    params:
      spiceai_endpoint: http://spice-cluster:8090
embeddings:
  - from: spiceai:text_embed
    name: text_embed
    params:
      spiceai_endpoint: http://spice-cluster:8090
```

The application calls the sidecar's `/v1/chat/completions`, `/v1/responses`, and
`/v1/embeddings`, and SQL functions such as `ai()` and `embed()`, with the names
the sidecar declares. These functions run in the sidecar, never in a remote
fragment, so a name always refers to the sidecar's own model. The embeddings
cache (`runtime.caching.embeddings`) answers repeated inputs locally.

Search runs where the indexes are. When a sidecar's Spice dataset is not
accelerated and has no local embeddings or full-text index, `vector_search`
and `text_search` run on the upstream runtime, against its embeddings and
indexes, with no configuration in the sidecar:

```sql
SELECT id, label, _score FROM vector_search(docs, 'snow') WHERE value > 0 LIMIT 5;
SELECT * FROM rrf(vector_search(docs, 'snow'), text_search(docs, 'snow', label));
```

The sidecar sends each search to the upstream as one Substrait plan, with the
filters and limit the upstream can evaluate, and receives the dataset's
columns plus `_score`. A filter with a subquery is evaluated in the sidecar,
against the sidecar's own catalog. An accelerated dataset is searched only
through a local index, since a search on the upstream would return rows the
working set does not hold. The query is embedded upstream, so the sidecar needs no
embedding model. `rrf` combines delegated searches in the sidecar. `/v1/search`
delegates vector search over every column the upstream embeds; full-text
search through `/v1/search` needs a local full-text index, so use `text_search`
in SQL for a delegated dataset. Because of that, `/v1/search` through a sidecar
does not fuse vector and full-text results as the upstream's own `/v1/search`
does, and its results carry key columns in `data` rather than in
`primary_key`. The upstream must be a release that accepts
delegated search; an older one returns an error naming the dataset.

Each remote read names only the columns the query uses. A dataset whose
upstream schema has an embedding column (`<column>_embedding`, a fixed-size
list) is read without it unless the query selects it; selecting an embedding
column from a sidecar is not supported yet, because Substrait has no fixed-size
list type. Search results include embedding columns.

For a local working set, add the existing acceleration configuration:

```yaml
    acceleration:
      enabled: true
      engine: cayenne
      mode: memory
```

A sidecar serves the hot and warm tiers: memory (`mode: memory`) and local
disk (`mode: file`), which may be the pod's ephemeral disk. The cluster serves
those and the cold tier on object storage. A sidecar refuses, at load, a
dataset that keeps data or a search index on object storage:
`cayenne_datalake_location`, a `cayenne_file_path` on object storage,
`cayenne_s3_zone_ids`, or `vectors: { engine: s3_vectors }`. Search over data
with no local index runs on the cluster, as above. A sidecar does
not automatically replicate datasets, change refresh schedules, or replace a
dataset's configured fallback policy. An empty accelerated result does not
automatically trigger a remote query. Cache TTL, stale serving, invalidation,
and explicit request cache controls keep their existing meanings.

A sidecar shares its pod or host with the application, so it runs on at most
4 cores. The CPU entitlement Spice detects (a cgroup CPU limit, a declared CPU
request, or the CPUs available) is capped at 4, and the startup log reports
`source: the sidecar maximum of 4 cores` when the cap applies. The cap is hard:
a `runtime.cpu.cores`, `SPICE_CPU_CORES`, or `--cpu-cores` value above 4,
including `all` on a larger host, is capped at 4 with a warning that names the
setting. A value of 4 or less applies as configured. Sidecars also default local
queries to one target partition, so independent queries share the CPU budget;
override this with `runtime.query.target_partitions` when appropriate for your
workload. Because those queries mostly wait on the upstream, a sidecar admits
16 concurrent queries per core (64 on 4 cores) when
`runtime.query.max_concurrent_queries` is unset, where a server admits 4.
Memory limits still apply. Explicit resource and cache settings take
precedence over defaults.

The upstream results cache can key Substrait requests by a hash of their
serialized plan when `runtime.caching.sql_results.cache_key_type: plan` is set.
Identical plan bytes use the same key within the caller's cache namespace.
Normal cache eligibility and freshness rules still apply. Substrait requests
bypass a cache configured for SQL keys because they contain no SQL text.

A sidecar guards its application's access to the cluster, so it is read-only
by default and runs no code of its own. Datasets and catalogs default to
`access: read`; writes reach the cluster only where the sidecar's Spicepod
grants them. HTTP and WebAssembly `functions:` are not registered on a sidecar,
and each one is logged as an error at startup; SQL functions are registered as
usual.

A sidecar does not run DDL. `CREATE` and `DROP` statements for tables, views,
and schemas are refused with `DDL is not allowed on a sidecar`, even on a
catalog configured with `access: read_write_create`, because the sidecar's
catalog is its application's view of data the cluster owns. Run DDL on the
cluster. Reads, and writes to datasets and catalogs configured for them, are
unaffected.

`--role` accepts `server`, `sidecar`, `scheduler`, and `executor`. Omitting it
selects `server`, or `executor` when `--scheduler-address` is set; a sidecar
does not join a cluster, so `--role sidecar` with `--scheduler-address` is
rejected at startup. The same Spicepod runs as a server or a sidecar; restart
Spice to change its role. Ports, bindings, authentication, and readiness
settings are configured as usual. For rollback, restart with `--role server`;
configured acceleration files remain.

The [Enhancement](https://github.com/spiceai/spiceai/issues/15707) records the
contract. `test/spicescript/results-cache.yaml` checks results-cache serving in
both roles, and `crates/runtime/tests/flight/sidecar.rs` compares a sidecar's
answers through Substrait federation with an independent DataFusion session
over the same rows.
