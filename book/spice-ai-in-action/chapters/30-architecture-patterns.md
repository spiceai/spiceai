# 27. Architecture patterns from real deployments

The blog archive adds a useful perspective to the local project: why teams introduce a runtime boundary in an existing system and how that boundary changes as an application grows. This chapter draws on the analytics-replica, cluster-sidecar, multi-tenancy, DynamoDB, and customer-case-study posts. Their architectural lessons inform the designs below; their reported performance outcomes are not reproduced benchmarks for this book.

## 27.1 An analytics replica is a serving role

A conventional read replica often preserves the source database's storage and execution characteristics. An analytics replica can consume operational changes into a representation chosen for scans, joins, and aggregations. Its purpose is to give analytical callers a separate serving environment while the existing database remains responsible for transactions.

The July 29, 2026 [analytics-replica article](https://spice.ai/blog/the-analytics-replica-pattern-shortening-the-path-to-data-based-ai) presents this as an incremental adoption pattern. Start with one useful table and a measured application question. You do not need to move an entire business system before evaluating the benefit.

For Northstar, replicate orders for sales summaries while leaving order creation and cancellation in the transactional service. Add returns only when the net-sales contract is defined. Add historical data through an explicit coverage boundary. Each addition has a clear owner and acceptance test.

Replication still has costs: initial snapshots, log decoding, network transfer, retained history, and source connections. The architectural advantage is separation of analytical query execution, not the disappearance of every source-side resource. Measure those costs alongside the serving workload.

## 27.2 Distinguish isolation dimensions

Isolation can mean several things: a different query compute pool, a different process, a different set of accessible rows, a different credential, or a different failure domain. A design can provide one without all the others.

The April 15, 2026 [multi-tenancy article](https://spice.ai/blog/multi-tenancy-for-ai-agents-without-pipelines) organizes deployments into shared query filtering, configuration-separated datasets, dedicated runtimes, and a hybrid placement model. Use those as distinct choices:

| Pattern | What becomes separate | What remains shared |
|---|---|---|
| Fixed tenant-filtered operations | Request scope | Runtime, storage, resource pools |
| Separate dataset declarations | Named source boundaries | Runtime and often credentials or capacity |
| Dedicated runtime | Process, configuration, local state | Any shared source or infrastructure |
| Hybrid placement | Selected tenant environments | Shared tier for remaining tenants |

A filtered view is useful only when the caller cannot bypass it. A dedicated process still needs correct routing. A distinct API key still needs an authorization policy. Write down which property the deployment actually establishes and test it at the relevant boundary.

## 27.3 Cluster-sidecar architecture controls ingestion fanout

A sidecar per application replica can multiply source ingestion. The April 21, 2026 [cluster-sidecar article](https://spice.ai/blog/cluster-sidecar-architecture) addresses this by separating a central ingestion and broad-query tier from application-local working sets. Applications talk to their sidecars; sidecars obtain data or delegate supported queries to the central tier; source credentials remain concentrated in that tier.

This pattern has three potentially useful reuse layers: centrally maintained data, a sidecar's bounded working set, and result caching. They should not be confused. A result-cache hit reuses an answer. A local accelerated scan computes a new answer from retained data. Delegation asks another runtime to perform work.

For Northstar, a sidecar might retain a tenant's current policies and recent summaries, while an annual report goes to the shared cluster. The product needs a routing and coverage contract for each request. Do not assume that an arbitrary query over a partially accelerated table will automatically recover every missing row from the central tier.

The article's “local endpoint” describes the application's connection point. Delegated requests still cross a network. Measure the local-hit, local-scan, and delegated paths separately. A single aggregate latency distribution can hide a very different experience on a cold sidecar.

## 27.4 Snapshot distribution is a publication protocol

The cluster-sidecar post describes a single producer publishing compatible acceleration snapshots for many consumers to bootstrap. The important idea is ownership: one producer creates the serving artifact, and readers consume an identified published version.

Do not copy a Cayenne representation into a DuckDB file and expect format compatibility. Snapshot support, source engine, consumer engine, storage mode, and version must match the documented mechanism. Early blog examples discuss DuckDB and SQLite file snapshots; use the installed release's snapshot reference for current support.

A snapshot publication needs an identity, completeness, access control, and retention. A sidecar needs to know which version it loaded and whether it has caught up with later changes. Download completion alone does not prove that the artifact matches the configured schema or application definition.

Test a new sidecar starting while a newer snapshot is being published. Test an unavailable snapshot store. Test a snapshot older than the allowed freshness bound. Decide whether the sidecar waits, serves a visible stale state, or uses a supported alternate path.

## 27.5 Control-plane data is a distinct workload

The January 22, 2026 [DynamoDB Streams article](https://spice.ai/blog/real-time-acceleration-with-dynamodb-streams) describes a control-plane/data-plane separation: configuration changes originate centrally, while processing nodes need local access to the configuration they execute.

This is different from an ad hoc dashboard. A processing node may need a complete small configuration dataset, predictable local reads, and a well-defined update boundary. A cache miss that reaches the source can reintroduce the operational dependency the design intended to remove.

For Northstar, imagine per-tenant routing rules or product eligibility rules used on every request. If the full authorized rule set is small, a complete local representation can be easier to reason about than an opportunistic result cache. The application still needs to know which rule version is active and what to do when updates stop.

The post's bootstrap discussion highlights a general principle: establish recoverable stream progress using the source's supported protocol, then reconcile initial state and later changes. Iterator lifetimes and retention are source-specific. Do not replace that protocol with an application clock or a sleep before beginning consumption.

## 27.6 Customer stories are workload studies

The [Barracuda case study](https://spice.ai/blog/barracuda-networks-100x-faster-query-responses-with-spice) describes data-intensive security and archival access with strict serving requirements. Its useful design questions are which data stays in object storage, which representation serves the hot path, and how several data operations are consolidated. Its headline performance and cost figures belong to the reported customer deployment, not to Northstar or a universal Spice specification.

The [Basis Set Ventures case study](https://spice.ai/blog/basis-set-ventures-deploys-spice-ai) describes natural-language discovery over continuously refreshed people and company information. That workload emphasizes entity identity, changing facts, retrieval, and reducing unsupported answers. It is a useful contrast to the deterministic sales endpoint: freshness and schema descriptions are central even when the user begins with prose rather than SQL.

Read a case study by extracting the workload, original constraints, chosen boundary, and acceptance evidence. Do not begin by copying its engine settings. Different data distribution, concurrency, and source access can make the same settings inappropriate.

## 27.7 Build an architecture decision record

A strong decision record for Northstar can fit on two pages. State the request classes, authoritative sources, freshness and availability behavior, tenant boundary, selected topology, state ownership, and measured evidence. List the conditions that would trigger reconsideration: growing sidecar fanout, a larger working set, new regulatory boundaries, or a shift from interactive queries to batch exports.

Attach a diagram and the query contract. Record which claims came from your run and which came from published examples. The record should make it possible for a future engineer to change the topology without accidentally changing the business meaning.

**Workshop.** Design three versions of Northstar: a single internal application, a shared SaaS service with many small tenants, and a service with a few large dedicated tenants. Keep the `/sales` and `/search` contracts identical. Compare ingestion fanout, routing, state ownership, and outage behavior. The best design is the one whose costs and guarantees you can explain and verify.
