# Apache Ballista at Spice AI: Distributed Query Execution

> How we use Apache Ballista to scale SQL queries across multiple nodes with fault-tolerant shuffle and mTLS security

---

## 📚 Engineering at Spice AI Series

This article is part of our **Engineering at Spice AI** series, where we share technical deep-dives into the technologies and practices that power our SQL query, search, and inference engine.

- [Rust at Spice AI](rust-at-spiceai.md) — Our systems programming foundation
- [Apache Arrow at Spice AI](apache-arrow-at-spiceai.md) — Arrow as our core data format
- [Apache DataFusion at Spice AI](apache-datafusion-at-spiceai.md) — Our SQL query engine foundation
- [DuckDB at Spice AI](duckdb-at-spiceai.md) — Embedded analytics acceleration
- [Apache Iceberg at Spice AI](apache-iceberg-at-spiceai.md) — Open table format integration
- [Vortex at Spice AI](vortex-at-spiceai.md) — Columnar compression for Cayenne
- **Apache Ballista at Spice AI** *(You are here)*

---

## Table of Contents

- [What is Apache Ballista?](#what-is-apache-ballista)
- [Why Ballista?](#why-ballista)
- [Scheduler-Executor Architecture](#scheduler-executor-architecture)
- [Integrating Ballista into Spice](#integrating-ballista-into-spice)
- [Distributed Query Planning](#distributed-query-planning)
- [File Scan Distribution](#file-scan-distribution)
- [Custom Codec Serialization](#custom-codec-serialization)
- [mTLS Cluster Security](#mtls-cluster-security)
- [Remote Catalog and UDF Sync](#remote-catalog-and-udf-sync)
- [High Availability Design](#high-availability-design)
- [Our Fork and Extensions](#our-fork-and-extensions)
- [Lessons Learned](#lessons-learned)

---

Spice is built for speed on a single node—local acceleration with DuckDB and Cayenne delivers sub-millisecond query performance. But enterprise data warehouses can exceed the memory and compute capacity of a single process. When queries need to scan terabytes of Parquet files across object storage, or when long-running inference workloads tie up a single machine, you need to distribute work across a cluster.

Apache Ballista is a distributed query framework built natively on DataFusion and Arrow. It provides a scheduler-executor architecture, a disk-based shuffle service for fault tolerance, and Arrow Flight RPC for high-performance data transport. We chose it as Spice's distributed query layer because it integrates directly with our existing DataFusion-based query engine—no format conversions, no impedance mismatch.

## What is Apache Ballista?

[Apache Ballista](https://github.com/apache/datafusion-ballista) extends DataFusion with distributed execution capabilities:

1. **Scheduler-Executor Model** — A scheduler accepts queries, plans execution, and distributes tasks to executor workers.
2. **Stage-Based Execution** — Queries are broken into stages separated by shuffle boundaries (repartitions, aggregations). Each stage runs as independent tasks on executors.
3. **Disk-Based Shuffle** — Intermediate data between stages is spilled to disk, enabling fault tolerance. Failed stages can retry from intermediate data without re-executing from scratch.
4. **Arrow Flight RPC** — Executors exchange Arrow data over Flight, maintaining zero-copy semantics across the network boundary.
5. **DataFusion Native** — Uses DataFusion's `LogicalPlan`, `PhysicalPlan`, and `SessionState` directly. Custom `TableProvider` implementations, optimizer rules, and UDFs work without modification.

```text
SQL Query
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│                       Scheduler                          │
│  Parse → Plan → Optimize → Distribute Stages → Track    │
└──────────────────────────────────────────────────────────┘
         │              │              │
         ▼              ▼              ▼
   ┌───────────┐  ┌───────────┐  ┌───────────┐
   │ Executor 1│  │ Executor 2│  │ Executor 3│
   │  Stage A  │  │  Stage A  │  │  Stage B  │
   │  Task 1   │  │  Task 2   │  │  Task 1   │
   └───────────┘  └───────────┘  └───────────┘
         │              │              │
         └──── Shuffle (disk) ─────────┘
                        │
                        ▼
                  Arrow Results
```

## Why Ballista?

We evaluated several distributed query frameworks before choosing Ballista. The decision came down to two qualities: reliability and batteries-included maturity.

### Reliability Through Shuffle

The key differentiator is Ballista's disk-based shuffle service. Intermediate data between query stages is persisted to disk, so a failed stage can be retried without re-executing the entire query from scratch. This matters for Spice because:

- **Long-running inference queries**: UDF workloads that call LLM endpoints can take minutes. Losing progress on a network hiccup is unacceptable.
- **Large analytical scans**: TPC-H SF100 benchmarks show 2.9× speedup vs. single-node DataFusion. At this scale, fault tolerance is essential.
- **Data lake sources**: Most data lakes are not low-latency sources. Re-reading terabytes from S3 because a late stage failed wastes time and money.

Alternatives like `datafusion-distributed` use a fully in-memory model—faster for small datasets, but a single failure means starting over.

### Batteries Included

Ballista provides production infrastructure that would take months to build from scratch:

| Component            | What Ballista Provides                                  |
| -------------------- | ------------------------------------------------------- |
| **Scheduler**        | Query planning, task management, execution coordination |
| **Executor**         | Worker pool management, resource tracking               |
| **Shuffle Service**  | Disk-based intermediate data, stage retry               |
| **Arrow Flight RPC** | Efficient data transport between nodes                  |
| **Work Queue**       | Dynamic executor pool resizing during query execution   |
| **Metrics**          | Task-level execution metrics                            |
| **PySpark Client**   | Python client compatibility for data science workflows  |

Building equivalent infrastructure in-house was estimated at 6-12+ months of engineering effort—time better spent on Spice's differentiating features like acceleration, search, and inference.

### DataFusion Native

Because Ballista is built on DataFusion, integration is natural. Our 20+ custom `TableProvider` implementations, optimizer rules, and UDFs like `ai()`, `embed()`, and `vector_search()` work in distributed mode without modification. The query flows through the same DataFusion pipeline—Ballista adds distribution, not a different query engine.

## Scheduler-Executor Architecture

In Spice, cluster mode is configured entirely through CLI flags. A single `spiced` binary can run as a scheduler, an executor, or a standalone (non-distributed) instance:

```bash
# Start a scheduler
spiced --role scheduler \
  --node-advertise-address scheduler.cluster.local \
  --node-mtls-ca-certificate-file ca.pem \
  --node-mtls-certificate-file scheduler.pem \
  --node-mtls-key-file scheduler-key.pem

# Start an executor (--role executor implied by --scheduler-address)
spiced --scheduler-address scheduler.cluster.local:50052 \
  --node-advertise-address executor1.cluster.local \
  --node-mtls-ca-certificate-file ca.pem \
  --node-mtls-certificate-file executor1.pem \
  --node-mtls-key-file executor1-key.pem
```

Spice uses a two-port architecture to separate public and internal traffic:

| Port  | Visibility | Services                                               | Security          |
| ----- | ---------- | ------------------------------------------------------ | ----------------- |
| 50051 | Public     | Arrow Flight (user queries), OpenTelemetry             | Optional user TLS |
| 8090  | Public     | HTTP REST API (queries, health, status)                | Optional user TLS |
| 50052 | Internal   | Ballista `SchedulerGrpcServer`, Spice `ClusterService` | **mTLS required** |
| 9090  | Public     | Prometheus metrics                                     | None              |

Users submit queries through the public ports (50051, 8090) exactly as they would with a single-node Spice deployment. The scheduler transparently distributes execution across executors.

### Role Configuration

The `ClusterConfig` struct maps CLI arguments to runtime behavior:

```rust
#[derive(Debug, Clone, clap::Parser)]
pub struct ClusterConfig {
    #[arg(long = "role")]
    pub role: Option<ClusterRole>,

    #[arg(long = "node-bind-address", default_value = "0.0.0.0:50052")]
    pub node_bind_address: SocketAddr,

    #[arg(long = "scheduler-address")]
    pub scheduler_address: Option<String>,

    #[arg(long = "node-advertise-address")]
    pub node_advertise_address: Option<String>,

    // mTLS certificate flags...
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ClusterRole {
    Scheduler,
    Executor,
}
```

A convenience: setting `--scheduler-address` without `--role` implicitly selects executor mode. This reduces boilerplate for the common case where you're adding workers to an existing cluster.

## Integrating Ballista into Spice

The integration lives primarily in the `runtime` crate under `crates/runtime/src/cluster/`. Here's how the major components fit together:

```text
crates/runtime/src/cluster/
├── mod.rs                      # Scheduler/executor init, poll loops
├── servers.rs                  # Internal gRPC server (mTLS)
├── service.rs                  # ClusterServiceImpl (GetAppDefinition, ExpandSecret)
├── scheduler_registry.rs       # HA scheduler discovery
└── datafusion/
    ├── mod.rs                  # Physical optimizer rule registration
    ├── datafusion_scheduler_ext.rs  # Scheduler DataFusion extensions
    └── codec/
        ├── spice_logical_codec.rs   # Serialization for Spice plan nodes
        └── spice_physical_codec.rs  # Serialization for Spice execution plans
```

### Scheduler Initialization

When Spice starts in scheduler mode, it creates a Ballista `SchedulerServer` and a Spice `ClusterServiceImpl`:

```rust
let scheduler = SchedulerServer::new(
    scheduler_name,
    config,
    ballista_cluster,
    codec,
    Arc::new(config_producer),
    Arc::new(runtime_producer),
);
```

The `ClusterServiceImpl` handles Spice-specific RPCs that aren't part of stock Ballista:

```rust
pub struct ClusterServiceImpl {
    app: Arc<RwLock<Option<Arc<App>>>>,
    secrets: Arc<RwLock<Secrets>>,
    advertise_address: String,
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
}

#[tonic::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn get_app_definition(&self, request: Request<GetAppDefinitionRequest>)
        -> Result<Response<GetAppDefinitionResponse>, Status> { ... }

    async fn expand_secret(&self, request: Request<ExpandSecretRequest>)
        -> Result<Response<ExpandSecretResponse>, Status> { ... }
}
```

Both services are multiplexed on the internal port (50052) behind mTLS.

### Executor Poll Loop

Executors connect to the scheduler and enter a poll loop, requesting tasks and reporting results. The connection uses exponential backoff with Fibonacci intervals:

```rust
fn spawn_scheduler_poll_loop(
    scheduler_address: String,
    client_tls_config: Option<ClientTlsConfig>,
    executor: Arc<Executor>,
    codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode>,
    readiness_sender: Arc<Mutex<Option<oneshot::Sender<String>>>>,
) -> SchedulerPollHandle {
    let cancel = CancellationToken::new();
    let task = tokio::spawn(async move {
        let mut backoff = FibonacciBackoffBuilder::new()
            .max_duration(Some(Duration::from_secs(5)))
            .build();
        // Connection state machine: NeedsEndpoint → ReadyToConnect → connected
        // Avoids redundant work when only later stages fail
        loop {
            // ... connect, register, execute tasks
        }
    });
    SchedulerPollHandle { cancel, task }
}
```

The state machine pattern (`SchedulerConnectionState`) avoids redundant work—if TLS handshake succeeds but the scheduler is temporarily unreachable, the executor retries the connection step without recreating the endpoint.

## Distributed Query Planning

When a user submits a query in cluster mode, Spice decides whether to distribute it. Not every query benefits from distribution—`DESCRIBE TABLE` or queries against the `spice_sys` schema run locally:

```rust
fn should_distribute_plan(plan: &LogicalPlan) -> datafusion::common::Result<bool> {
    let mut should_distribute = true;
    let _ = plan.apply(|p| {
        if let LogicalPlan::DescribeTable(_) = p {
            should_distribute = false;
        } else if let LogicalPlan::TableScan(scan) = p
            && matches!(scan.table_name.schema(), Some(SPICE_RUNTIME_SCHEMA))
        {
            should_distribute = false;
        }
        if should_distribute {
            Ok(TreeNodeRecursion::Continue)
        } else {
            Ok(TreeNodeRecursion::Stop)
        }
    })?;
    Ok(should_distribute)
}
```

For distributable queries, the `BallistaQueryPlanner` replaces DataFusion's default physical planner. It serializes the logical plan, sends it to the scheduler, and the scheduler breaks it into stages across executors:

```rust
let query_planner: BallistaQueryPlanner<LogicalPlanNode> =
    BallistaQueryPlanner::with_local_planner(
        scheduler_url.to_string(),
        cfg.ballista_config(),
        SpiceLogicalCodec::new_codec(),
        DefaultPhysicalPlanner::with_extension_planners(default_extension_planners()),
    );

SessionStateBuilder::new_from_existing(self.df.ctx.state())
    .with_config(
        cfg.with_ballista_query_planner(Arc::new(query_planner))
            .with_option_extension(SpiceClusterConfig::default()),
    )
    .build()
    .upgrade_for_ballista(scheduler_url.to_string())
```

The gRPC message size is configured to 100MB to match other Flight configurations in the codebase—Ballista's default 16MB is too small for queries returning large record batches.

## File Scan Distribution

One of our most important physical optimizer rules is `DistributeFileScanOptimizer`. Stock DataFusion produces a single `DataSourceExec` that scans all file groups on one node. For distributed execution, we break that into individual scans that Ballista can distribute across executors:

```text
Before (single node):
  DataSourceExec: file_groups={20 groups ...}, file_type=parquet

After (distributed):
  UnionExec
    ├── CoalescePartitionsExec          ← Stage boundary for Ballista
    │     └── DataSourceExec: file_groups={1 group: [wiki_a.parquet:0..43MB]}
    ├── CoalescePartitionsExec
    │     └── DataSourceExec: file_groups={1 group: [wiki_a.parquet:43MB..87MB]}
    └── CoalescePartitionsExec
          └── DataSourceExec: file_groups={1 group: [wiki_b.parquet:0..50MB]}
```

Each `CoalescePartitionsExec` node signals to Ballista's `DistributedPlanner` that it can break the plan into a new stage. The scheduler assigns each stage as a task to an available executor. This enables parallel scanning of large Parquet datasets across the cluster.

The optimizer is registered alongside DataFusion's default physical optimization rules:

```rust
pub fn datafusion_and_cluster_physical_optimizers()
    -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>
{
    let mut rules = PhysicalOptimizer::new().rules;
    rules.extend(vec![
        EnsureSupportedFileScan::new(),
        DistributeFileScanOptimizer::new(),
        UnionProjectionPushdownOptimizer::new(),
    ]);
    rules
}
```

## Custom Codec Serialization

Ballista serializes query plans as protobuf to send them between scheduler and executors. Spice has custom plan nodes—accelerated tables, search operators, inference UDFs—that stock Ballista doesn't know about. We extend Ballista's codec system with `SpiceLogicalCodec` and `SpicePhysicalCodec`:

```text
crates/runtime/src/cluster/datafusion/codec/
├── spice_logical_codec.rs    # Encodes/decodes Spice logical plan nodes
└── spice_physical_codec.rs   # Encodes/decodes Spice physical execution plans
```

These codecs handle serialization of Spice-specific nodes (e.g., `AcceleratedTable` references, custom `ExecutionPlan` implementations) so that the scheduler can plan queries involving Spice features and executors can execute them.

The codec is wired into the `SessionConfig` when building the distributed session:

```rust
let cfg = self.df.ctx.copied_config()
    .with_ballista_logical_extension_codec(SpiceLogicalCodec::new_codec())
    .with_ballista_use_tls(tls_enabled)
    .with_ballista_grpc_client_max_message_size(100 * 1024 * 1024);
```

## mTLS Cluster Security

Internal cluster communication carries highly privileged operations:

- **`GetAppDefinition`**: Executors fetch the complete Spicepod configuration, including dataset definitions.
- **`ExpandSecret`**: Executors request secret values from the scheduler's secret store.
- **Task Dispatch**: The scheduler sends query fragments containing connection strings and credentials.

We require mTLS (mutual TLS) for all internal cluster communication. Both the scheduler and executor must present valid certificates signed by a shared CA:

```rust
fn server_with_cluster_mtls(
    server: Server,
    tls_config: &ClusterTlsConfig,
) -> Result<Server, tonic::transport::Error> {
    let server_tls_config = ServerTlsConfig::new()
        .identity(tls_config.server_identity.clone())
        .client_ca_root(tls_config.ca_certificate.clone());
    server.tls_config(server_tls_config)
}
```

If mTLS certificates aren't provided, Spice refuses to start in cluster mode unless `--allow-insecure-connections` is explicitly set (intended for development only):

```rust
if let Some(tls_config) = tls_config {
    server = server_with_cluster_mtls(server, tls_config)?;
    tracing::info!("Cluster mTLS enabled for internal cluster server");
} else if !rt.df.cluster_config.allow_insecure_connections() {
    return Err(Error::InsecureConfiguration {
        message: "Cluster mode without mTLS requires --allow-insecure-connections".to_string(),
    });
}
```

CLI tooling simplifies certificate management:

```bash
# Initialize PKI for development
spice cluster tls init

# Generate node certificates
spice cluster tls add scheduler1
spice cluster tls add executor1 --host executor1.cluster.local
```

## Remote Catalog and UDF Sync

Stock Ballista requires clients to manually register tables and UDFs. In Spice, the scheduler is the source of truth for the catalog—datasets, models, and UDFs are defined in the Spicepod and loaded by the scheduler. Clients and executors need access to this metadata without manual duplication.

We extended Ballista with new RPC endpoints in our fork:

- **`GetCatalog`**: Returns catalog, schema, and table metadata with Arrow schemas.
- **`GetFunctions`**: Returns UDF signatures, return types, and documentation.

On the client side, `RemoteTableProvider` and `RemoteScalarUDF` act as stubs that participate in query planning but defer execution to the scheduler. This means users can run `SHOW TABLES`, `DESCRIBE TABLE`, and build queries in notebooks without pre-registering anything.

This architecture is phased:

| Phase   | Capability               | How                                         |
| ------- | ------------------------ | ------------------------------------------- |
| Phase 1 | SQL (current)            | Scheduler does all planning                 |
| Phase 2 | DataFrame API, Substrait | Remote catalog enables client-side planning |

## High Availability Design

Stock Ballista uses a single scheduler—a single point of failure. For production deployments, Spice implements an active/active multi-scheduler architecture:

```text
                ┌─────────────────────┐
                │   Load Balancer     │
                └─────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌────────────┐   ┌────────────┐   ┌────────────┐
   │ Scheduler  │   │ Scheduler  │   │ Scheduler  │◄──► Object Store (S3)
   └────────────┘   └────────────┘   └────────────┘
          ▲                ▲                ▲
          │     (executor-initiated)        │
   ┌────────────┐   ┌────────────┐   ┌────────────┐
   │  Executor  │   │  Executor  │   │  Executor  │───► Object Store (shuffle)
   └────────────┘   └────────────┘   └────────────┘
```

Key design decisions:

- **Object-store shared state**: Schedulers coordinate through S3 (or compatible) with conditional writes—no etcd, ZooKeeper, or Redis required.
- **Executor-initiated connections**: Executors connect to all schedulers. Network topology only requires executor→scheduler connectivity.
- **Exactly-once job execution**: Jobs execute exactly once; individual stages and tasks use at-least-once semantics with retry.
- **Stateless schedulers**: Any scheduler can accept queries. Failed schedulers don't affect in-progress queries because state lives in the object store.

The `SchedulerPeers` registry tracks sibling schedulers through shared state:

```rust
pub struct ClusterServiceImpl {
    app: Arc<RwLock<Option<Arc<App>>>>,
    secrets: Arc<RwLock<Secrets>>,
    advertise_address: String,
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
}
```

## Our Fork and Extensions

Spice maintains a fork of Apache Ballista at [`github.com/spiceai/datafusion-ballista`](https://github.com/spiceai/datafusion-ballista) on the `spiceai-50` branch (tracking DataFusion 50.x). The fork is referenced in our workspace `Cargo.toml`:

```toml
[patch.crates-io]
ballista-core = { git = "https://github.com/spiceai/datafusion-ballista", ... }
ballista-executor = { git = "https://github.com/spiceai/datafusion-ballista", ... }
ballista-scheduler = { git = "https://github.com/spiceai/datafusion-ballista", ... }
```

### Fork Extensions

Our fork adds four categories of extensions:

1. **Remote Catalog Sync** — `GetCatalog` RPC with `RemoteTableProvider` stubs for schema discovery without pre-registration.
2. **UDF Sync** — `GetFunctions` RPC with `RemoteScalarUDF` stubs for function discovery.
3. **Cluster Security** — `BallistaConfigGrpcEndpoint` for TLS endpoint configuration, `BallistaGrpcMetadataInterceptor` for header injection.
4. **Executor Readiness** — Scheduler-side health checking of executors, compatible with HA multi-scheduler designs.

### Upstream Contributions

We aim to minimize fork divergence. TLS/mTLS extensions and executor readiness detection are candidates for upstreaming to the Apache project. Extensions that are Spice-specific (custom codecs, catalog sync tied to Spicepod) will remain in the fork.

## Lessons Learned

### 1. Single-Node Must Stay Fast

Distributed execution adds latency—scheduling overhead, network serialization, shuffle I/O. We keep single-node Spice as the default and only distribute queries that benefit from parallelism. Metadata queries, system table scans, and `DESCRIBE` operations always run locally.

### 2. Not Every Query Should Be Distributed

The `should_distribute_plan` check prevents distributing queries where the overhead exceeds the benefit. System catalog queries, describe operations, and queries against internal Spice schemas stay local.

### 3. mTLS Is Non-Negotiable

The `ExpandSecret` RPC sends raw secret values over the wire. Any cluster communication without mutual authentication is a security vulnerability. Making mTLS required-by-default (with an explicit opt-out for development) prevents accidental insecure deployments.

### 4. Fork Maintenance Is Real Work

Every DataFusion upgrade requires validating Ballista compatibility and rebasing our fork extensions. We minimize this by keeping extensions isolated and well-tested. The fork is pinned to specific revisions in `Cargo.toml` so upgrades are deliberate.

### 5. State Machines Beat Retry Loops

The executor poll loop uses a `SchedulerConnectionState` enum to track progress through endpoint creation, TLS configuration, and connection. When a later step fails, the state machine retries from the right point—not from scratch. This avoids redundant TLS handshakes and DNS resolutions.

### 6. Object Store as Coordination Layer

For HA, we chose object store (S3) with conditional writes over external coordination services. This eliminates additional infrastructure dependencies—Spice clusters need only an S3-compatible store that most deployments already have.

---

## Conclusion

Apache Ballista gives Spice horizontal scalability without abandoning the DataFusion ecosystem we've built on. Our integration is pragmatic: distribute file scans for large analytical workloads, but keep single-node acceleration for latency-sensitive queries. The mTLS security model, HA scheduler design, and remote catalog sync extend stock Ballista into a production-ready distributed query layer for enterprise deployments.

The key insight: distributed query execution should be transparent. Users submit queries through the same APIs regardless of whether Spice runs as a single process or a multi-node cluster. The scheduler handles distribution, the shuffle service handles fault tolerance, and mTLS handles security—all behind the same `spiced` binary.

---

## References

- [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista)
- [Ballista Architecture Guide](https://datafusion.apache.org/ballista/contributors-guide/architecture.html)
- [Ballista User Guide](https://datafusion.apache.org/ballista/)
- [Spice DataFusion Ballista Fork](https://github.com/spiceai/datafusion-ballista)
- [Apache Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html)
- [Coralogix Ballista Fork](https://github.com/coralogix/arrow-ballista) — Production fork with 65+ releases
- [DataFusion Thread Pools](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/thread_pools.rs) — Runtime architecture patterns
- [gRPC TLS Authentication](https://grpc.io/docs/guides/auth/)
