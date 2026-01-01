# DR-006: High Availability Distributed Query with Stateless Schedulers

## Status

Accepted

## Context

Following the decisions to adopt Apache Ballista as Spice's distributed query framework (DR-004) and extend it with catalog sync, UDF sync, and security features (DR-005), Spice needs to support production-grade high availability for distributed query execution.

Stock Apache Ballista uses a single-scheduler model where the scheduler is a single point of failure. If the scheduler process dies:

1. All in-flight queries are lost with no recovery mechanism.
2. Clients must manually reconnect and resubmit queries.
3. Executor state and task progress are orphaned.

For production deployments, Spice requires:

* **Active-active schedulers**: Multiple scheduler nodes behind a load balancer/virtual IP with no leader election or failover delays.
* **Stateless schedulers**: Any scheduler can handle any request; scheduler pods may disappear/reappear at any time.
* **Resilient query execution**: In-flight queries should fail gracefully when a scheduler dies, with clients able to resubmit to any scheduler.
* **Elastic scaling**: Ability to resize the cluster (add/remove executors) while queries are running.

Related decisions:

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md)

## Assumptions

1. Schedulers are deployed behind a load balancer/virtual IP; clients connect to a single scheduler endpoint.
2. The cluster runs on Kubernetes; executor discovery leverages K8s EndpointSlices or DNS.
3. Spicepod configuration is static within a deployment; dynamic spicepod updates are out of scope.
4. Spice is object-store native/first. Object stores like S3 with conditional writes are the preferred mechanism for shared state and shuffle data persistence.
5. When shared scheduler state is required (e.g., for query resumption), S3 with conditional writes (PutObject with `If-None-Match`) provides distributed coordination without external infrastructure.
6. Network partitions between schedulers and executors should cause affected queries to fail rather than hang indefinitely.
7. Queries are submitted via an async API; synchronous query APIs are not suitable for HA (see DR-004 consequences).
8. Initial scope targets graceful failure and resubmission; automatic query resumption by a different scheduler is a future enhancement.
9. Executors are stateless workers; all durable/intermediate result state lives in object storage.

## Options

### Scheduler State Management

1. **Stateless schedulers with lease-based coordination**: Schedulers hold ephemeral leases on executor slots; leases expire if scheduler dies. No shared state store required for basic operation.
2. **S3 with conditional writes**: Job/task state stored in S3 using conditional PutObject (`If-None-Match`, `If-Match`). Provides distributed coordination without Redis/etcd. Object-store native approach.
3. **External state store (Redis/etcd)**: All job/task state stored in Redis, etcd, or a database. Any scheduler can read/write state. Adds infrastructure dependency.
4. **Leader-follower schedulers**: Single active scheduler with standby replicas; failover on leader death. Simpler state management but failover latency.

### Executor Discovery

1. **Kubernetes EndpointSlices**: Direct K8s API integration for real-time executor discovery. Tight K8s coupling.
2. **DNS-based discovery**: Resolve headless Service to executor IPs. Works outside K8s; eventual consistency (DNS TTL).
3. **Explicit registration**: Executors register with schedulers on startup. Requires scheduler endpoint knowledge; incompatible with stateless schedulers.
4. **Service mesh discovery**: Leverage Istio/Linkerd for service discovery. Adds infrastructure dependency.

### Executor Capacity Management

1. **Slot leasing**: Executors own their capacity; schedulers lease slots with TTL. Executor-centric; graceful degradation on scheduler failure.
2. **Scheduler-tracked slots**: Schedulers maintain executor slot counts. Requires state synchronization between schedulers.
3. **Work stealing**: Executors pull work from a shared queue. Decouples scheduling from execution; requires shared queue infrastructure.
4. **Static partitioning**: Each scheduler owns a subset of executors. Simple but limits flexibility and fault tolerance.

### Query Failure Handling

1. **Fail-fast with client retry**: Queries fail immediately when scheduler dies; clients resubmit to any scheduler.
2. **Automatic query resumption via S3**: Job state persisted to S3; new scheduler reads orphaned job state and continues execution. Object-store native approach.
3. **Stage-level checkpointing**: Shuffle outputs persisted to S3; new scheduler resumes from last completed stage. Minimizes recomputation.
4. **Query timeout with cleanup**: Queries expire after TTL; executors clean up associated work. Prevents resource leaks.

## First-Principles

* **Secure by default**: mTLS between all cluster components (scheduler-executor, executor-executor). No plaintext cluster traffic.
* **Developer experience first**: HA should be transparent to users; no special client configuration beyond async API usage.
* **Object-store native/first**: Prefer S3-compatible object stores with conditional writes for shared state over external databases or coordination services.
* **Simplicity**: Prefer stateless designs that avoid distributed consensus complexity. Lease-based coordination over Raft/Paxos.
* **First-class extensibility**: HA mechanisms should integrate with existing Ballista extension points, not require forking core scheduling logic.
* **Align to industry standards**: Use standard K8s patterns (Services, EndpointSlices, health checks) rather than custom discovery mechanisms.
* **Any machine can go offline without customer impact**: Core design principle for production readiness.

## Decision

Spice will implement high availability distributed query using **stateless schedulers with lease-based executor coordination** and **S3-based persistence for shuffle data and future job state**:

1. **Stateless Schedulers**: Schedulers maintain only ephemeral, soft-state caches. No external coordination service required for basic operation.

2. **Object-Store Native Shuffle**: Shuffle intermediate data persists to S3-compatible object storage, enabling stage recovery and executor failure tolerance.

3. **DNS-Based Executor Discovery**: Schedulers discover executors via DNS resolution of a headless Kubernetes Service. Discovery is cached and periodically refreshed.

4. **Executor-Owned Slot Leasing**: Executors expose gRPC APIs for slot reservation:
   * `ReserveSlots { scheduler_id, request_id, slots, ttl_ms } -> { lease_id, granted_slots, expires_at }`
   * `RenewLease { lease_id, ttl_ms } -> { expires_at }`
   * `ReleaseLease { lease_id } -> {}`
   * Leases are idempotent (via `request_id`) and expire automatically if not renewed.

5. **Executor Details RPC**: Schedulers call `DescribeExecutor` on discovered endpoints to learn `executor_id`, `slots_total`, and capabilities.

6. **Lease-Bound Execution**: Tasks include a `lease_id`; executors validate leases before accepting work. Expired leases cause associated work to be cancelled.

7. **Fail-Fast Query Semantics**: If a scheduler dies:
   * Leases stop renewing and expire after TTL.
   * Executors cancel/abort work associated with expired leases.
   * Queries fail; clients resubmit to any available scheduler via load balancer.

8. **Async Query API**: All distributed queries use an async submission API with job IDs, enabling clients to poll for status and handle scheduler failures gracefully.

9. **Future: S3-Based Job State**: When automatic query resumption is implemented, job state will be persisted to S3 using conditional writes, enabling any scheduler to resume orphaned queries.

### Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                           Clients                                │
│                    (SQL, DataFrame, Substrait)                   │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    Load Balancer    │
                    │  spice-schedulers   │
                    └─────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
       ┌────────────┐   ┌────────────┐   ┌────────────┐
       │ Scheduler 1│   │ Scheduler 2│   │ Scheduler N│
       │ (stateless)│   │ (stateless)│   │ (stateless)│
       └────────────┘   └────────────┘   └────────────┘
              │                │                │
              │    DNS Discovery (headless Service)
              │                │                │
              └────────────────┼────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
       ┌────────────┐   ┌────────────┐   ┌────────────┐
       │ Executor 1 │   │ Executor 2 │   │ Executor M │
       │ (slot mgr) │   │ (slot mgr) │   │ (slot mgr) │
       └────────────┘   └────────────┘   └────────────┘
              │                │                │
              └────────────────┼────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Shared Storage     │
                    │ (S3/GCS for shuffle)│
                    └─────────────────────┘
```

### Why

#### Stateless Schedulers with Lease-Based Coordination

* **No external coordination infrastructure**: Avoids Redis, etcd, or database dependencies for scheduler coordination. Reduces operational complexity.
* **Object-store native for persistence**: When state persistence is needed (shuffle, future job state), S3 with conditional writes provides coordination without additional services.
* **Natural fault isolation**: Scheduler death only affects queries it was coordinating; other schedulers and executors are unaffected.
* **Horizontal scaling**: Add/remove schedulers freely; no rebalancing or leader election required.
* **Lease TTL provides bounded failure detection**: Executors automatically reclaim slots after lease expiry, preventing resource leaks.

#### DNS-Based Executor Discovery

* **Works in any K8s environment**: Headless Services are standard K8s primitives; no custom operators required initially.
* **Graceful degradation outside K8s**: DNS works in non-K8s environments with appropriate configuration.
* **Eventual consistency is acceptable**: Executor churn is infrequent; stale discovery is handled by RPC failures and retry.

#### Executor-Owned Slot Management

* **Executors are the source of truth for their capacity**: Eliminates need for schedulers to synchronize slot state.
* **Idempotent lease operations**: Safe for scheduler retries after transient failures.
* **Lease expiry handles scheduler failures**: No orphaned reservations; executors self-heal.

#### Fail-Fast with Client Retry

* **Simplest initial implementation**: Avoids complexity of query resumption or state transfer.
* **Async API enables graceful handling**: Clients poll for status; failure is explicit and actionable.
* **Foundation for future enhancements**: Stage checkpointing and query resumption can be added incrementally.

### Why not

#### External State Store (Redis/etcd)

* Adds infrastructure dependency and operational burden.
* Single point of failure unless itself made HA.
* Latency overhead for state reads/writes on hot path.
* Not aligned with Spice's object-store-first principle.
* **When it makes sense**: For sub-millisecond state access patterns not suitable for S3 latency.

#### Leader-Follower Schedulers

* Failover latency during leader election impacts availability.
* Standby replicas are underutilized.
* Doesn't support horizontal scaling of query throughput.

#### Work Stealing / Shared Queue

* Requires highly available queue infrastructure (Kafka, SQS, etc.).
* Adds latency for queue operations.
* Complicates task priority and affinity handling.
* **When it makes sense**: For very high query volumes requiring work distribution.

#### Automatic Query Resumption (deferred)

* Requires persistent job state (shared store).
* Complex coordination for stage-level resume.
* Initial scope prioritizes simplicity and graceful failure.
* **Future enhancement**: Can be added once basic HA is proven in production.

## Consequences

### Scheduler Changes

* Schedulers become stateless; all job state is ephemeral within scheduler process.
* Scheduler startup requires only DNS/K8s access; no state recovery from persistent store.
* Scheduler metrics and task history must be aggregated externally (Prometheus, OpenTelemetry).

### Executor Changes

* Executors implement new gRPC APIs: `DescribeExecutor`, `ReserveSlots`, `RenewLease`, `ReleaseLease`.
* Executors maintain a slot manager tracking active leases and their TTLs.
* Executors cancel work when associated leases expire.
* Executor health checks must reflect slot availability.

### Client API Changes

* Async query API required: `SubmitQuery -> job_id`, `GetQueryStatus(job_id)`, `GetQueryResults(job_id)`.
* Synchronous query APIs remain for non-distributed (single-node) workloads only.
* Client SDKs need retry logic for scheduler failures during async polling.

### Deployment Changes

* Kubernetes deployment manifests for scheduler StatefulSet/Deployment with Service.
* Headless Service for executor discovery.
* Load balancer configuration for scheduler endpoint.
* Documentation for mTLS certificate provisioning across cluster components.

### Operational Impact

* No single point of failure for schedulers; rolling updates are safe.
* Executor scaling (add/remove pods) is dynamic; schedulers discover changes via DNS.
* Query failure on scheduler death requires client-side retry.
* Monitoring dashboards need to aggregate metrics from all scheduler instances.

### Testing Requirements

* Chaos testing: scheduler pod kills during query execution.
* Lease expiry testing: verify executor cleanup after TTL.
* Discovery testing: executor add/remove during active queries.
* Load testing: multiple schedulers with concurrent queries.

### Future Work

* **Automatic query resumption via S3**: Persist job state to S3 with conditional writes; new scheduler reads and resumes orphaned queries.
* **Stage-level checkpointing**: Resume from last completed stage using S3-persisted shuffle outputs.
* **Hybrid deployments**: Hosted schedulers with self-managed executors.
* **Autoscaling**: Executor scaling based on query load and slot utilization.
* **Strongly consistent job state**: For scenarios requiring exactly-once execution semantics.

## Links

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md)
* [HA Scheduler Design Issue](https://github.com/spiceai/spiceai/issues/8559)
* [Spice DataFusion Ballista Fork](https://github.com/spiceai/datafusion-ballista)
* [Kubernetes Headless Services](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services)
* [Kubernetes EndpointSlices](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/)
* [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md)
