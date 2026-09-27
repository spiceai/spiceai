# 9. Refresh, caching, and the age of an answer

A sales dashboard is allowed to be slightly behind the source. An order-cancellation decision may not be. Both can return a correct answer for the state they read, while only one meets its application's freshness contract. This chapter gives you a way to describe and measure that contract.

## 9.1 Define the clocks

An event has a business time, a source commit time, an ingestion observation time, an accelerator visibility time, and a response time. These timestamps answer different questions. An order created yesterday but corrected now has an old business time and a new commit time.

Freshness should normally be tied to the change the application expects to observe, not simply the maximum event timestamp in the table. An idle source can have old event timestamps and be fully caught up. A busy source can contain a very recent row while an older partition is missing updates.

For Northstar, define the dashboard promise as “committed order and refund changes become visible within the agreed bound under normal operating conditions.” Specify what happens when the bound is exceeded: display a stale-data state, refuse a critical operation, or route an explicitly designed request to the source.

## 9.2 Full refresh

A full refresh reloads the configured representation. It is a natural first choice for small tables, periodic exports, and data where a complete snapshot is readily available. Its operational questions are snapshot duration, source load, publication behavior, and overlap with serving queries.

```yaml
# Acceleration fragment
refresh_mode: full
refresh_check_interval: 1m
```

An interval of one minute is a scheduling parameter, not a proof that every response is at most one minute old. A refresh can take time, fail, or wait for resources. Result caching can add another interval. The source's own export may already be stale before Spice reads it.

Check the exact engine's refresh visibility behavior. Do not assume a partial load is visible or hidden merely from the mode's name. Test concurrent reads during a refresh with a fixture that makes mixed versions detectable.

## 9.3 Append refresh

Append refresh is appropriate only when the source and configured extraction logic fit an append model. A time column and overlap window can help capture late-arriving records, but the overlap must be reconciled according to supported key and conflict behavior.

The hard questions are updates and deletes. A source row corrected in place may not look like a new append. A deleted source row does not arrive as another ordinary row. If the business requires those changes, use a mechanism that represents them or a reconciliation process that explicitly detects them.

Define the watermark semantics: whether the boundary is inclusive, which timestamp advances it, and how equal timestamps are handled. Test records exactly on the boundary and records arriving late with older timestamps. Wall clocks alone do not provide a unique event ordering.

## 9.4 Change-driven refresh

`refresh_mode: changes` selects a supported change stream. It brings inserts, updates, and deletes through the connector's replication or streaming mechanism. This removes the need to repeatedly scan unchanged rows, but it adds source retention, checkpoint, identity, and restart responsibilities.

Changes can be visible before all related tables have reached an application-consistent point. A join between orders and refunds needs an explicit consistency expectation. Do not infer a distributed transaction snapshot from the fact that both tables use CDC. Chapter 10 develops the recovery model.

## 9.5 Result caching is another stateful layer

A cache configuration can make its intended lifetime explicit:

```yaml
# Runtime fragment
runtime:
  caching:
    sql_results:
      enabled: true
      max_size: 128MiB
      item_ttl: 5s
```

This is a policy example, not a recommendation that five seconds fits every request. A cached answer may already reflect an accelerated representation that trails the source. A useful conservative budget accounts for source publication lag, refresh or replication lag, result-cache age, and any application or browser cache age. Overlapping mechanisms and invalidation can make the actual behavior more nuanced; measure end to end.

For diagnostic SQL requests, the local verifier sends `Cache-Control: no-cache`. Inspect the release's documented response headers to distinguish hits, misses, and bypass behavior. A response served quickly does not by itself identify a hit.

A cache key must respect the authorization context and query parameters. The application should include the tenant restriction in its fixed SQL and bind the tenant value. Do not depend on a result-cache implementation to repair an interface that lets a caller submit another tenant's query.

## 9.6 Empty results and fallback

The `on_zero_results` policy distinguishes returning an empty accelerated result from using the source for the documented fallback path. The latter can help selected lookup workloads whose records are outside a local working set. It also restores dependence on the source.

```yaml
# Acceleration fragment; choose deliberately
on_zero_results: return_empty
```

An empty result is a legitimate fact: an order may not exist, a tenant may have no sales, or a filter may match nothing. Conversely, a partial result is not necessarily empty. A recent-only aggregate can produce a nonempty answer while omitting older rows. Zero-result fallback must not be treated as a general completeness guarantee for arbitrary analytical queries.

Test the exact query shapes the application uses: a point lookup, a filtered list, an aggregate over no rows, and an aggregate over a partially covered interval. Record the plan and source activity so you know which path served each response.

## 9.7 Measure freshness with a sentinel

Use a disposable source record or a dedicated probe table. Write a unique marker and a source timestamp, then poll the application query until that exact marker and value appear. Record source commit observation, each poll result, runtime lag metrics, and the first matching response. Use a deadline and preserve the last observed state on failure.

A sentinel measures one path. Spread probes across partitions or tenants when they can lag independently. Supplement them with connector lag metrics, refresh success timestamps, and backlog observations. Do not use a single recent row as evidence that every table is current.

![Figure 9.1. An answer can age at several layers before it reaches the application.](figures/freshness.png)

## 9.8 Make staleness visible to users

Northstar's dashboard can show the last successfully observed data version or a freshness status derived from an application-owned check. It should not label a request timestamp as “data updated at.” The assistant can say that order data is temporarily unavailable or outside the promised freshness bound while still retrieving a static policy, if the product explicitly supports that split.

**Exercise.** Allocate a 30-second freshness budget across a periodic source export, acceleration, and result caching. Now suppose the export alone runs every five minutes. Explain why changing Spice's cache TTL cannot satisfy the requirement. Design the source or product change required.

**Further reading.** See [data acceleration](https://spiceai.org/docs/features/data-acceleration), [caching](https://spiceai.org/docs/features/caching), and cookbook `acceleration/data-refresh/`, `retention/`, and `acceleration/dual-dataset-registration/`.
