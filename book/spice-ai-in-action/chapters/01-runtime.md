# 1. An application data runtime

Northstar's first dashboard is straightforward: show paid sales by day and tenant. An engineer can query the orders database directly, add an index, and ship. The second screen joins orders to product attributes exported by a merchandising system. A third screen searches support policies. The assistant that follows needs both the order facts and the policy text. Each screen introduces a connection, a data transformation, an authentication decision, and another source of latency.

A common response is to put more integration code in the application. That works until every feature carries its own retry behavior, connection management, schema assumptions, and caching rules. Another response is to move everything into a central warehouse. That may be appropriate for reporting, but it creates a separate ingestion and serving system whose latency and ownership must match application needs.

Spice offers another building block: place a data runtime beside the application or behind an internal service endpoint. The application asks SQL questions, retrieves documents, or calls a configured model. The runtime connects to sources and can materialize selected datasets into accelerators. The business database remains the system of record unless you deliberately design a supported write path.

## 1.1 The five objects to understand

A **Spicepod** is the declarative description of an application data environment. It names datasets, views, models, embeddings, tools, and runtime settings. It belongs in version control because it determines which data and capabilities the application sees.

A **dataset** is a named relation backed by a connector. `orders` might refer to a PostgreSQL table today and a local file in a test. Its SQL name need not reveal its physical location. That indirection is useful, but it does not erase differences in type support, permissions, transactions, or source capabilities.

A **connector** supplies access to a kind of source. Some connectors can translate filters and projections into remote work. Others expose files or APIs. A connector is a behavioral boundary, not merely a connection string.

An **accelerator** maintains a representation of source data for serving. Choosing it means deciding where bytes live, how updates arrive, and what happens after a restart. An accelerated table still has a relationship with its source; the refresh and fallback policies define that relationship.

A **model or embedding component** adds inference capabilities behind a named interface. A remote provider and a locally loaded model have different capacity and failure models even when the HTTP request looks similar.

![Figure 1.1. The runtime sits between application interfaces and data or model services.](figures/runtime.png)

## 1.2 What happens to a query

Consider a request for paid orders. The SQL parser creates a logical representation. Planning resolves table names and types, rewrites expressions, and chooses an execution strategy. DataFusion provides the central SQL planning and execution foundation. Connectors and acceleration layers determine which scans and operations are available. Arrow record batches provide a columnar representation for data flowing through the engine.

If `orders` is federated, some work may execute at the source and the remaining work in Spice. If it is accelerated, a scan may read the accelerated representation. A result cache can answer an eligible repeated request without doing that same work again. These are different paths. A short response time alone cannot tell you which occurred; query plans, response headers, logs, and metrics can.

Do not assume that every function executes inside a selected accelerator. A deployment can combine work done by the source, the accelerator, and DataFusion. The practical question is where the expensive and selective operators execute. Chapter 5 teaches you to answer it with a plan.

## 1.3 Federation, acceleration, and caching

Federation provides a common query surface over separately managed data. Its attraction is incremental adoption: register a source and begin asking questions. Its cost is continued dependence on source latency, source availability, transfer volume, and the connector's capabilities.

Acceleration moves a chosen data representation closer to the serving path. It introduces storage, refresh work, and lag. It is useful when a bounded working set is repeatedly queried or when analytical work should be separated from operational request traffic. Reading a change log or taking a snapshot still consumes source resources; “separate analytics” does not mean free replication.

Result caching retains the answer to an eligible query for a policy-defined interval. It is valuable when requests repeat, but it does not create a generally queryable replica of a table. A dashboard that issues thousands of unique filters can have little result reuse while benefiting from acceleration.

| Mechanism | Reuses | Principal dependency | Question to ask |
|---|---|---|---|
| Federation | Connections and source access | Source query execution | How much work is remote? |
| Acceleration | Dataset representation | Refresh or replication | How current is the visible copy? |
| Result cache | A query's result | Cache eligibility and lifetime | Can this response be reused for this caller? |

Several mechanisms can coexist. That makes observability more valuable, not less. A freshness budget must include every layer that can retain an older answer.

## 1.4 Choosing a deployment boundary

In a sidecar arrangement, each application instance has a nearby Spice runtime. The network path is simple and ownership is local to the application. Replicas may each maintain storage and source connections, so scale-out can multiply ingestion and memory requirements.

A shared service centralizes those resources. It also creates a shared capacity pool and a service that needs its own availability and admission controls. A distributed cluster can spread eligible query work across nodes, but it introduces scheduling, shuffle, state, identity, and recovery concerns. Distribution is a capacity decision, not an automatic next step after a successful local query.

Begin by writing down the boundary Northstar needs. Which requests must continue when the source is unavailable? How much lag can a sales card display? Must the assistant enforce tenant isolation? How large is a normal result? Which team responds when a refresh stops? These answers select a topology more reliably than a feature checklist.

## 1.5 The first architecture decision

For this book, Northstar starts with a local runtime and files. That choice isolates the learning problem: we can inspect every row and reproduce every business result without a cloud account. We then replace files with production-shaped sources, add acceleration, and expose bounded application operations.

A production decision record might read: “The sales summary may trail committed orders by 30 seconds. The operational order status endpoint remains on the source database. Spice serves read-oriented analytics. The application owns tenant authorization and exposes no unrestricted SQL endpoint to end users.” This statement is specific enough to test and modest enough to implement.

**Exercise.** Choose an application you know. List three requests and, for each, the authoritative source, maximum acceptable staleness, maximum result size, and failure behavior. Identify one request that should remain on its transactional source. Revisit the list after Chapter 9.

**Further reading.** The [OSS overview](https://spiceai.org/docs) introduces the product surface. Source entry points include `README.md`, `crates/runtime`, and the workspace members in `Cargo.toml`. Treat published benchmark results as reports about their stated rigs, not specifications for your application.
