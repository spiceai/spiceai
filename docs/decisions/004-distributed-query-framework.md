# DR: Use Apache Ballista as Spice's distributed query framework

## Context

Spice aims to be a best-in-class query engine used by enterprises to quickly enable search and inference workloads. To tackle the "big data" warehouses frequently found in enterprise organizations, Spice should be able to scale its query execution capability past a single process.

## Assumptions

1. Spice will need to process queries that exceed the memory and compute capacity of a single process.
2. Users will run analytical queries and inference workloads that may be long-running and require fault tolerance.
3. The distributed query framework should integrate well with DataFusion, Spice's existing query engine.
4. The distributed framework will need to support custom Spice features like acceleration, search, UDFs, metrics, and telemetry.
5. Users may deploy Spice in multi-node clusters where network reliability and data persistence are important.

## Options

- [datafusion-ballista](https://github.com/apache/datafusion-ballista), original distributed DataFusion framework
- [datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed), new library with common components (exec nodes, stage planner, custom network traits) for "DIY" distributed execution

Notable mention: [DataFusion Substrait](https://docs.rs/datafusion-substrait/latest/datafusion_substrait/). Frameworks other than DataFusion can also send and receive Substrait plans, e.g. [Apache Calcite](https://calcite.apache.org/). This would be a long-term "nice to have" for either choice, possibly providing richer federation support to Substrait enabled engines.

## First-Principles

- **Align to industry standards**
- **Developer experience first**
- **Simplicity**

## Decision

Spice will use Apache Ballista as its distributed query framework.

**Why**:

Two high-level reasons:

- Reliability
  - Ballista includes a shuffle service: intermediate data is spilled to disk and provided as input to the next stage (where they are re-read from disk). `datafusion-distributed` is completely in-memory.
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

**Why not**:

- Spice already has some runtime concepts that would be duplicated with Ballista (e.g., metrics)
  - Mitigation: We don't have to use Ballista's metrics collection, and can propagate distributed metrics into our `task_history` and existing telemetry.
- Fork maintenance burden (related to previous): we likely need to customize certain components (e.g., metrics, or even just scheduler/executor TLS comms). This is going to be another item to check off during DataFusion upgrades.
  - Mitigation: Realistically nothing; but we can try to make as few customizations as are necessary for our use-case.
- Latency argument (datafusion-distributed's in-memory model)
  - We can always implement in-memory shuffle stages or [custom responses to shuffle RPC fetch](https://github.com/apache/datafusion-ballista/blob/main/ballista/executor/src/flight_service.rs#L92)

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

## Links

- [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista)
- [DataFusion Distributed](https://github.com/datafusion-contrib/datafusion-distributed)
- [Uber's Highly Scalable and Distributed Shuffle as a Service](https://www.uber.com/blog/ubers-highly-scalable-and-distributed-shuffle-as-a-service/)
  - Useful as a future reference for shuffle optimizations
