# DR-006: High Availability Distributed Query with Active/Active Schedulers

## Status

Accepted

## Context

Stock Apache Ballista uses a single-scheduler model where the scheduler is a single point of failure. For production deployments, Spice requires multiple active schedulers with no single point of failure, graceful query failure handling, and elastic cluster scaling. Clients submit query jobs and expect those jobs to be completed reliably. In a clustered deployment, the system must maintain shared state about node topology (which executors and schedulers exist), job execution (running jobs/queries and task-level status), and results (shuffle/output data).

Related decisions:

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md)

## Assumptions

1. Network topology:
  a. allows executors to connect to schedulers.
  b. may or may not allow executors to connect to other executors.
  c. allows schedulers to connect to other schedulers.
2. Orchestration system is responsible for maintaining minimum host availability.
3. Object store supports conditional writes.

## First-Principles

* **Secure by default**: mTLS between all cluster components (scheduler-executor, executor-executor). No plaintext cluster traffic.
* **Developer experience first**: HA should be transparent to users; no special client configuration beyond async API usage.
* **Object-store native/first**: Prefer S3-compatible object stores with conditional writes for shared state over external databases or coordination services.
* **Simplicity**: Prefer stateless designs that avoid distributed consensus complexity.
* **First-class extensibility**: HA mechanisms should integrate with existing Ballista extension points, not require forking core scheduling logic.
* **Align to industry standards**: Use standard deployment patterns and protocols rather than custom discovery mechanisms.

## HA Principles

* **Nodes going offline does not impact query availability**: Core design principle for production readiness.
* **No external dependent services**: Avoid additional infrastructure requirements (e.g. Redis, etcd) beyond object store.
* **Client submitted jobs to schedulers are highly available once accepted**: Clients can submit jobs to any scheduler; jobs are resilient to scheduler failure after acceptance.

## Options

### Shared state and conflict resolution

How schedulers store and coordinate shared cluster state, and how conflicting updates are resolved. Cluster state includes scheduler/executor membership and in-flight job/task metadata.

* Object store shared state with conditional writes.
* External coordination services (e.g., etcd, ZooKeeper, Redis) for shared state and leader election.
* SQL database or distributed KV store for job and scheduler state.
* Object store with separate lock service instead of conditional writes.

### Discovery and connectivity

How schedulers and executors find each other and which side initiates and maintains network connections.

* Scheduler discovery via shared state registration; executor discovery via logical connections; executor-initiated scheduler connections.
* Static configuration (predefined scheduler and executor lists).
* Service discovery via DNS/SRV or platform-native registry.
* Bidirectional scheduler-executor connections with scheduler-initiated calls.

### Cluster topology

The topology of the cluster and how schedulers and executors are arranged and shared across the cluster.

* Multiple active schedulers with executors connected to all schedulers; executors may be shared.
* Active/passive scheduler with failover.
* Dedicated executors per scheduler (no sharing).
* Executors connected to a subset of schedulers instead of all.

### Execution and shuffle behavior

Execution guarantees and where shuffle/intermediate data is stored during query execution.

* Exactly-once job execution (stages/tasks at-least once); shuffle in object store or executor local storage.
* At-least-once job execution with client-level deduplication.
* Shuffle state always in object store (no local executor shuffle).
* External shuffle service separate from executors.

## Decision

* **Shared state and conflict resolution**
  * Schedulers share state and that state is stored in object store.
  * Job state is part of the shared state.
  * Object store conditional writes is the mechanism for distributed conflict resolution.
* **Discovery and connectivity**
  * Scheduler discovery is via scheduler shared state registration.
  * Executor discovery is based on logical executor connections.
  * Network connections between executors and schedulers are one-way executor to scheduler.
* **Cluster composition**
  * An HA cluster is defined by:
    * one or more schedulers that are siblings (i.e. can talk to each other).
    * one or more executors that are connected to all schedulers.
  * Executors can be shared across schedulers.
* **Execution and shuffle behavior**
  * Jobs should be executed exactly once (stages/tasks are at-least once).
  * Shuffle state is stored either in object store or on executor local storage.

### Architecture

```
                    ┌─────────────────────┐
                    │    Load Balancer    │
                    └─────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
       ┌────────────┐   ┌────────────┐   ┌────────────┐
       │ Scheduler  │   │ Scheduler  │   │ Scheduler  │◄──►  Object Store
       │            │   │            │   │            │      (S3)
       └────────────┘   └────────────┘   └────────────┘
              ▲                ▲                ▲
              │                │                │
              │    (executor-initiated)         │
              │                │                │
       ┌────────────┐   ┌────────────┐   ┌────────────┐
       │  Executor  │   │  Executor  │   │  Executor  │────► Object Store
       └────────────┘   └────────────┘   └────────────┘      (shuffle)
```

## Consequences

* Async query API required; synchronous queries cannot survive scheduler death.
* Network topology decides if shuffle state must be stored in object store.
* Object store will be in the hot path for query execution.
* Polling is required for object store based updates, which increases eventual consistency latency.
* Shared state is eventually consistent.

## Links

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md)
* [HA Scheduler Design Issue](https://github.com/spiceai/spiceai/issues/8559)
