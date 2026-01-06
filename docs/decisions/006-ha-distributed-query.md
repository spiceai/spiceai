# DR-006: High Availability Distributed Query with Active/Active Schedulers

## Status

Draft

## Context

Stock Apache Ballista uses a single-scheduler model where the scheduler is a single point of failure. For production deployments, Spice requires multiple active schedulers with no single point of failure, graceful query failure handling, and elastic cluster scaling.

Related decisions:

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md)

## Assumptions

1. Network topology:
  a. allows executors to connect to schedulers.
  b. may or may not allow executors to connect to other executors.
  c. allows schedulers to connect to other schedulers.
2. Orchestration system is responsible for maintaining minimum cluster requirements.
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

### Cluster Topology

### State

1. Node topology - scheduler shared state stored in object store
  - Which executors exist (logical connection based)
  - Which schedulers exist (object store)
2. Job state
  - Running jobs/queries and their task level status (object store)
3. Results
  - Shuffle/results data (object store)

## Decision

* Schedulers share state that is stored in object store.
* Executors do not share state, all state comes from schedulers.
* Scheduler discovery is via scheduler shared state registration.
* Network connections between executors and schedulers are one-way executor to scheduler.
* Executor discovery is based on logical executor connections.
* Job state is stored in shared state.
* An HA cluster is defined by:
  * one or more schedulers that are siblings (i.e. can talk to each other).
  * one or more executors that are connected to all schedulers.
* Executors can be shared across schedulers.
* Shuffle state is stored either in object store or on executor local storage.
* Jobs should be executed exactly once (stages/tasks are at-least once).
* Object store conditional writes is the mechanism for distributed conflict resolution.

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
