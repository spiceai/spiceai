# DR: Use Apache Ballista as Spice's distributed query framework

## Context

Spice aims to be a best-in-class query engine used by enterprises to quickly enable search and inference workloads. To tackle the "big data" warehouses frequently found in enterprise organizations, Spice should be able to scale its query execution capability past a single process.

## Assumptions

1. Spice will need to process queries that exceed the memory and compute capacity of a single process.
2. Users will run analytical queries and inference workloads that may be long-running and require fault tolerance.
3. The distributed query framework should integrate well with DataFusion, Spice's existing query engine.
4. The distributed framework will need to support custom Spice features like acceleration, search, UDFs, metrics, and telemetry.
5. Users may deploy Spice in multi-node clusters where network reliability and data persistence are important.
6. Executor nodes will have access to the same data sources (e.g., shared object storage, databases) or data will be pushed via shuffle.
7. Sufficient local disk space will be available on executor nodes for shuffle intermediate data.
8. Network bandwidth between scheduler and executors is adequate for shuffle traffic.
9. Single-node Spice deployments must remain fully functional without requiring distributed infrastructure.
10. Distributed execution targets batch/analytical workloads (seconds to minutes); low-latency real-time queries (sub-second) will continue to use single-node acceleration.

## Options

### Primary Candidates

- [Apache Ballista](https://github.com/apache/datafusion-ballista): Native distributed DataFusion framework with scheduler-executor architecture, shuffle service, and Arrow Flight RPC.
- [datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed): Lightweight Rust library providing distributed capabilities with minimal opinions about networking. Uses Arrow Flight for worker communication; any node can act as coordinator or worker.
- **Custom in-house solution**: Build a bespoke distributed execution layer using [DataFusion Substrait](https://docs.rs/datafusion-substrait/latest/datafusion_substrait/) for cross-language plan serialization, Arrow Flight for data transport, and custom scheduler/executor components tailored to Spice's needs.

#### Maturity & Ecosystem Comparison

| Metric                   | Apache Ballista           | datafusion-distributed         |
| ------------------------ | ------------------------- | ------------------------------ |
| **GitHub Stars**         | ~1,900                    | ~50                            |
| **Contributors**         | 346                       | 14                             |
| **Commits**              | 4,488                     | ~140                           |
| **Tagged Releases**      | 34                        | 0                              |
| **Project Age**          | 4+ years (ASF since 2021) | ~5 months                      |
| **Apache Project**       | ✅ Yes (ASF governance)    | ❌ No (datafusion-contrib)      |
| **Production Readiness** | Production-used           | "Not yet ready for production" |

#### Company Backing & Production Usage

**Apache Ballista:**

- **Apple**: Andy Grove (Ballista creator, Apache Arrow/DataFusion PMC member) is an Apple employee
- **InfluxData**: Andrew Lamb (major DataFusion contributor) is an InfluxData employee
- **Coralogix**: Maintains [production fork](https://github.com/coralogix/arrow-ballista) with 65+ releases and custom features (shuffle metrics, task groups, order-aware coalesce)
- **440+ dependent repositories** on GitHub including arkflow-rs, SDF Labs

**datafusion-distributed:**

- **Datadog**: Primary backer—top contributors (@gabotechs, @ahmed-mez) are Datadog employees; maintains internal fork
- Currently appears to be internal tooling being open-sourced

**Custom In-House:**

- Full control over architecture, features, and roadmap
- No external dependencies or upstream coordination required
- Significant upfront engineering investment (estimated 6-12+ months for production-ready shuffle, fault tolerance, metrics)

#### Performance

**Apache Ballista:**

- Published TPC-H SF100 (100GB) benchmarks show **2.9x overall speedup** vs single-node DataFusion
- Memory usage 5-10x lower than Apache Spark in some cases (due to Rust + Arrow)
- Benchmarks available in repository with per-query comparison charts

**datafusion-distributed:**

- TPC-H and ClickBench benchmarks recently added
- Published comparison chart vs single-node DataFusion and Trino
- Quantitative speedup numbers not yet published
- In-memory execution may offer lower latency for small-to-medium datasets

**Custom In-House:**

- Performance would depend entirely on implementation choices
- Could optimize specifically for Spice's workload patterns
- Risk of under-performing mature solutions due to lack of optimization iterations

### Evaluated and Excluded

- **Apache Spark / Comet / Gluten**: JVM-based frameworks requiring significant infrastructure overhead. While [DataFusion Comet](https://github.com/apache/datafusion-comet) accelerates Spark with DataFusion, it inverts the desired relationship (Spark → DataFusion rather than DataFusion-first).
- **Ray Data / Dask**: Python-centric distributed computing frameworks. Poor fit for Rust-first architecture; would require FFI bridging or separate process coordination.
- **Meta Velox**: High-performance C++ execution engine, Arrow-compatible. Not a distributed framework itself—would require building distribution layer. Different language ecosystem.
- **Polars**: Rust-based DataFrame engine with commercial distributed offering (Polars Cloud). Different query model (LazyFrame vs SQL), not designed as embeddable library.

### Notable Mention

[DataFusion Substrait](https://docs.rs/datafusion-substrait/latest/datafusion_substrait/): Cross-language plan serialization format. Frameworks other than DataFusion can also send and receive Substrait plans, e.g. [Apache Calcite](https://calcite.apache.org/). This would be a long-term "nice to have" for either choice, possibly providing richer federation support to Substrait-enabled engines.

## First-Principles

- **Align to industry standards**: The chosen framework should build on Apache Arrow, Flight RPC, and DataFusion—industry-standard technologies Spice already uses.
- **Developer experience first**: Minimal code changes for users to scale from single-node to distributed execution. PySpark-like client APIs are a plus.
- **Secure by default**: Distributed communication between scheduler and executors must support TLS encryption. The framework should not require users to opt-in to security.
- **API first**: Distributed query execution should be accessible through Spice's existing APIs (HTTP, Arrow Flight) without requiring separate cluster management interfaces.
- **First-class extensibility**: Must support custom Spice components (acceleration, search, UDFs, telemetry) without forking core distribution logic.
- **Simplicity**: Prefer batteries-included solutions that reduce time-to-production over DIY approaches requiring significant custom infrastructure.

## Decision

Spice will use Apache Ballista as its distributed query framework.

**Why**:

Two high-level reasons:

- Reliability
  - Ballista includes a shuffle service: intermediate data is spilled to disk and provided as input to the next stage (where they are re-read from disk). In contrast, `datafusion-distributed` is completely in-memory, meaning a failed stage requires re-executing from the beginning.
    - Failed stages can be retried from intermediate data without starting over.
      - A good retry system for a system like this can look like an easy ask but take week or more to get right. Why spend the time?
    - Users may want long-running 'analytical' queries.
    - Users may have slow UDF workloads (e.g. inference).
    - The latency benefits of fully in-memory stages are only apparent if the leaf data source execs are not the bottleneck (most data lakes are not low-latency data sources).
    - Spice is already great at low-latency "edge duties" in single process mode with acceleration.
- "Batteries included"
  - Ballista has mature concepts for:
    - Execution model: scheduler (query planning, task management, exec coordination), executor (workers)
      - Scheduler's work queue model also allows dynamic resizing of executor pool while a query is running
    - Metrics collection
    - Shuffle service
    - Arrow/RPC services around the above
    - PySpark-like client support (important for DX/UX)
  - `datafusion-distributed` has a much simpler model: any machine is a worker, a basic flight service consumes execution plans, and it is up to you to implement discovery, the concept of participating in a cluster, etc. It is more of a "box of tools" than something ready-to-use. Building the basics is time-consuming.
- Performance tradeoffs
  - Distributed execution is inherently higher-latency than single-node due to scheduling overhead, network serialization, and shuffle I/O.
  - Ballista's disk-based shuffle prioritizes reliability over latency—appropriate for batch/analytical workloads.
  - Real-time queries (sub-second latency requirements) should use single-node Spice with acceleration; distributed mode is for throughput and scale, not latency.
  - An async (batch) query API will be required for distributed workloads to avoid blocking client connections during long-running queries.

**Why not**:

- Spice already has some runtime concepts that would be duplicated with Ballista (e.g., metrics)
  - Mitigation: We don't have to use Ballista's metrics collection, and can propagate distributed metrics into our `task_history` and existing telemetry.
- Fork maintenance burden (related to previous): we likely need to customize certain components (e.g., metrics, or even just scheduler/executor TLS comms). This is going to be another item to check off during DataFusion upgrades.
  - Mitigation: Realistically nothing; but we can try to make as few customizations as are necessary for our use-case.
- HA scheduler support: Ballista does not natively support high-availability scheduler nodes. Implementing HA (e.g., discovery, cluster participation, leader election) would require significant customization.
  - Mitigation: Start with single-node scheduler deployments. Evaluate whether HA customizations can be upstreamed; if not, consider whether `datafusion-distributed`'s flexibility would be more appropriate for HA use-cases. This should be a separate decision record.
- Latency argument (datafusion-distributed's in-memory model)
  - We can always implement in-memory shuffle stages or [custom responses to shuffle RPC fetch](https://github.com/apache/datafusion-ballista/blob/main/ballista/executor/src/flight_service.rs#L92)
- Custom in-house solution: Building from scratch would require significant engineering investment (estimated 6-12+ months) to reach production quality.
  - Shuffle service, fault tolerance, metrics, and scheduler logic are non-trivial distributed systems problems.
  - Substrait provides plan serialization but not execution infrastructure—we'd still need to build scheduling, task distribution, shuffle, and failure recovery.
  - Risk of under-investing in edge cases (network partitions, partial failures, backpressure) that mature frameworks have already solved.
  - Team bandwidth better spent on Spice's differentiating features (acceleration, search, inference) rather than reimplementing distributed query execution.

## Consequences

- Spice will integrate Apache Ballista's scheduler and executor components to enable distributed query execution across multiple nodes.
- A new deployment mode will be introduced where Spice can run as a Ballista scheduler, executor, or both.
- Distributed metrics will be propagated into Spice's existing `task_history` and telemetry systems rather than using Ballista's built-in metrics collection.
- The Ballista codebase will need to be maintained as a dependency, with potential fork requirements for customizations (e.g., TLS communications, custom metrics integration).
- DataFusion upgrades will require coordinating with Ballista compatibility, adding an additional validation step to the upgrade process.
- Users will be able to scale query execution horizontally for large analytical workloads and long-running inference queries.
- Intermediate query results will be persisted to disk via Ballista's shuffle service, enabling fault-tolerant query execution with stage retry capabilities.
- The development team will need to become familiar with Ballista's architecture, including its scheduler-executor model and Arrow Flight RPC services.
- Documentation and deployment guides will need to be created for multi-node Spice cluster setup and configuration.
- Client SDK support for distributed queries will benefit from Ballista's existing PySpark-like client capabilities.
- Full fault tolerance (including scheduler-layer resilience) will require an async query submission API, as current APIs are synchronous. This is a broader concern that warrants a separate decision record.
- HA scheduler deployments are out of scope for this decision; initial implementation targets single-node scheduler with multiple executors.
- Executor nodes will require local disk space for shuffle data; capacity planning guidance will be needed.
- Network topology will impact performance; co-located deployments (same datacenter/region) are recommended initially.
- Spicepod configuration schema will need extensions to support distributed deployment parameters (scheduler address, executor count, shuffle settings).
- Integration test infrastructure will need to support multi-node test scenarios.
- Distributed tracing (OpenTelemetry) will need to propagate trace context across scheduler and executor boundaries for end-to-end observability.

## Links

### Primary References

- [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista)
- [Ballista User Guide](https://datafusion.apache.org/ballista/)
- [Ballista Architecture](https://datafusion.apache.org/ballista/contributors-guide/architecture.html)
- [DataFusion Distributed](https://github.com/datafusion-contrib/datafusion-distributed)
- [DataFusion Distributed Documentation](https://datafusion-contrib.github.io/datafusion-distributed/)

### Related Technologies

- [DataFusion Substrait](https://docs.rs/datafusion-substrait/latest/datafusion_substrait/) - Cross-language plan serialization
- [Substrait Specification](https://substrait.io/) - Cross-language serialization for relational algebra
- [Apache Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html) - High-performance data transport

### Background Research

- [Uber's Highly Scalable and Distributed Shuffle as a Service](https://www.uber.com/blog/ubers-highly-scalable-and-distributed-shuffle-as-a-service/) - Shuffle optimization reference
- [DataFusion Thread Pools](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/thread_pools.rs) - Runtime architecture patterns
- [Ballista Extending Components](https://datafusion.apache.org/ballista/user-guide/extending-components.html) - Extensibility guide

### Production Forks & Usage

- [Coralogix Ballista Fork](https://github.com/coralogix/arrow-ballista) - Production fork with 65+ releases
- [Datadog datafusion-distributed Fork](https://github.com/DataDog/datafusion-distributed) - Internal fork
- [Ballista Dependents](https://github.com/apache/datafusion-ballista/network/dependents) - 440+ repositories using Ballista
