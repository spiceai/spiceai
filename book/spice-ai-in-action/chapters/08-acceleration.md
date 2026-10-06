# 8. Choosing and operating an accelerator

Northstar's dashboard repeatedly asks questions about the same orders. Federation gives it access, but each request can depend on the source. Acceleration introduces a serving representation that can be maintained separately. The decision is not simply which engine is fastest. It is which representation fits the data, query patterns, refresh mechanism, and recovery requirements.

## 8.1 Begin with the serving contract

Write down the working set size, common predicates, aggregation shapes, concurrency, update rate, and acceptable lag. Include the rebuild time that the application can tolerate after storage loss. A small dataset with frequent full refreshes has different needs from a large CDC-fed relation receiving updates and deletes throughout the day.

Acceleration changes capacity accounting. You now have source reads or replication traffic, ingestion buffers, accelerated storage, query memory, result caches, and maintenance work. A small storage file does not imply a small peak process footprint. A refresh and several concurrent joins can consume memory at the same time.

## 8.2 The engine families

Arrow acceleration keeps a representation designed around Arrow data in memory. It is a useful starting point for bounded datasets and tests where losing the representation on restart is acceptable because it can be reloaded. Its memory requirement must fit alongside queries and the rest of the runtime.

DuckDB provides an embedded analytical database option, with memory and file-backed configurations. It is useful to evaluate for analytical working sets and SQL shapes supported by the integration. SQLite provides another embedded option with different storage and query characteristics. PostgreSQL acceleration uses a PostgreSQL-backed representation and introduces the operations of that server.

Cayenne is Spice's native accelerator built around Vortex data and managed metadata. Its read, write, and maintenance design deserves a separate chapter. It should be evaluated with the actual refresh path and workload, especially for mutable data and larger serving sets.

| Choice | State to account for | Useful evaluation focus |
|---|---|---|
| Arrow | Resident accelerated batches | Fit, reload time, query memory |
| DuckDB | Engine state and optional database files | Analytical plans, refresh, persistence |
| SQLite | Database files or memory state | Supported types, indexes, update behavior |
| PostgreSQL | External database state | Server capacity and operational ownership |
| Cayenne | Data files, metadata, mutable state | CDC, scan behavior, maintenance, recovery |

This table is a way to organize experiments. It does not assign universal winners or imply identical feature support.

## 8.3 Run the same contract on another engine

**Local lab.** Stop the starter runtime before replacing its configuration. Create the parent directory for the file-backed variant:

```bash
mkdir -p .spice/duckdb
spiced spicepod.duckdb.yaml \
  --http 127.0.0.1:8090 --flight 127.0.0.1:50051
```

The companion variant gives each dataset its own database path. Its representative fragment is:

```yaml
acceleration:
  enabled: true
  engine: duckdb
  mode: file
  refresh_mode: full
  params:
    duckdb_file: .spice/duckdb/orders.db
```

Run `verify.py` again and retain a separate output file. Compare row values before comparing plans. The common table names and views let the application SQL remain the same while the physical path changes.

The authoring run initially attempted a path whose parent directory did not exist. The captured DuckDB error reported that the file could not be opened because the directory was missing. Creating the directory is therefore part of this lab's setup, not an assumed side effect of the connector. Appendix A records the final coverage of each variant.

## 8.4 Memory and file modes express lifecycle

A memory mode means the accelerated representation is ephemeral. A file mode means there is persistent state to manage. Persistence by itself does not guarantee that the stored state is current, compatible with a new binary, or a complete backup of everything needed to resume replication.

Some releases and engines expose additional modes such as `file_create` and `file_update`. Their lifecycle can include recreating storage. Do not select a destructive lifecycle mode as a casual response to a startup error. Read the documented semantics, determine whether the source can rebuild the table, and practice that rebuild in a disposable environment.

Give a persistent dataset an explicit path and a single owner. Do not point unrelated running instances at the same embedded database file unless the engine and Spice deployment explicitly support that sharing pattern. Shared storage is not automatically a shared database protocol.

## 8.5 Primary keys and conflicts

A primary key identifies which logical row an update or delete affects. In an acceleration configuration, it also informs conflict handling and some search or storage behavior. The declared key must reflect the source's real uniqueness scope.

```yaml
# Acceleration fragment for a supported mutable source
primary_key: order_id
on_conflict:
  order_id: upsert
```

For tenant-local order identifiers, use the documented composite-key syntax and test it with two tenants sharing the same order number. The fixture uses global order IDs for readability; that is not permission to omit tenant scope in a different source.

An upsert policy is not a substitute for event ordering. Replaying an old row after a newer row can restore stale values unless the ingestion path applies the appropriate ordering and checkpoint rules. Replication chapters focus on the whole path.

## 8.6 Indexes, sort order, and coverage

Indexes and physical sort order can help selected workloads, but each introduces write or maintenance work and supports particular predicates. Begin with query evidence. If a lookup pattern dominates, investigate the index support of the chosen engine. If a columnar scan repeatedly filters a date range, investigate layout and pruning.

A refresh query can narrow the accelerated data to a working set or projection. That changes what the table contains. If only seven days are stored, an all-time sum over the accelerated table is not an all-time sum. Name and document the coverage, and test queries at the boundary.

Avoid loading unnecessary sensitive columns merely because `SELECT *` is convenient. A smaller data contract can reduce both operational complexity and the amount of information available to downstream tools.

## 8.7 A restart is part of the test

After a successful load, stop the runtime gracefully and restart it with the same version, configuration, and storage. Record readiness and the query contract. Then, in a copied disposable environment, rehearse loss of the accelerated representation and measure rebuild behavior using runtime metrics and a run log.

Do not delete the only persistent state of a live integration to test recovery. A recovery rehearsal needs an isolated copy, a known source history, and an explicit cleanup procedure. For replicated datasets, state includes positions and checkpoints as well as visible rows.

**Exercise.** Compare federated, Arrow, and one file-backed variant using the same data and SQL. Record row equality and plans. Which additional artifacts would you need before making a latency or memory claim? Design that measurement without using this eight-order fixture as a benchmark.

**Further reading.** Consult the [accelerator reference](https://spiceai.org/docs/components/data-accelerators), cookbook `arrow/`, `sqlite/accelerator/`, `cayenne/`, and `acceleration/indexes/`. The inspected configuration types live under `crates/spicepod/src/acceleration`.
