# DR-008: Distributed Query Job Execution

## Status

Draft

## Context

Following DR-004 (Ballista) and DR-006 (HA schedulers), this decision defines how query jobs and tasks are distributed, executed, and recovered across the cluster.

Ballista offers two models: executors pull work by polling schedulers, or schedulers push work over a stream. Both preserve executor-initiated connections, but they differ in complexity, scheduler visibility into executor load, failure recovery behavior, and dispatch latency.

In a multi-scheduler HA environment, schedulers must avoid duplicate assignment, task state must be shared for recovery, and executors connected to multiple schedulers need a consistent view of available work. This decision focuses on work distribution, executor load management, and failure recovery at the architecture level.

Related decisions:

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md)
* [DR-006: High Availability Distributed Query with Active/Active Schedulers](./006-ha-distributed-query.md)

## Assumptions

1. Executors initiate connections to schedulers.
2. Multiple schedulers may be active concurrently.
3. Job execution state is shared across active schedulers to support reassignment and recovery.
4. Client interaction is async and tolerates eventual consistency for job state.

## First-Principles

* **Developer experience first**: Job execution should remain stable and predictable under scheduler or executor failures.
* **Simplicity**: Favor the approach with the least architectural complexity consistent with HA requirements.
* **Resilience**: Job scheduling must tolerate scheduler and executor failures without losing accepted jobs.

## Options

### Work distribution model

How tasks are assigned to executors.

* Pull: executors poll schedulers for work.
* Push: schedulers stream work to executors over gRPC.
* Hybrid: executors poll, but schedulers can push when certain conditions are met.

### Executor load and pacing

How scheduler decisions account for executor busyness and capacity.

* Executor-regulated pacing (poll frequency reflects load).
* Scheduler-tracked executor load and capacity.
* Token or lease-based capacity signaling between schedulers and executors.

### Failure recovery and reassignment

How tasks are recovered when schedulers or executors fail mid-execution.

* Scheduler-local task ownership with limited cross-scheduler recovery.
* Shared task state that allows any scheduler to reassign work.
* External coordination service for task ownership and recovery.

## Decision

* **Work distribution model**
  * Use pull-based scheduling where executors poll schedulers for work.
  * Allow scheduler-initiated notifications that prompt executors to poll sooner, without pushing tasks directly.
* **Executor load and pacing**
  * Rely on executor-regulated pacing through poll frequency rather than centralized scheduler load tracking.
* **Failure recovery and reassignment**
  * Maintain task assignment and progress in shared state so another scheduler can reassign work if a scheduler or executor fails.

## Consequences

* Pull-based scheduling minimizes changes to Ballista while remaining compatible with executor-initiated connections.
* Scheduler load tracking is not required for basic scheduling decisions; executors self-regulate via polling behavior.
* Recovery depends on shared task state being accurate enough for reassignment under eventual consistency.

## Links

* [DR-006: High Availability Distributed Query with Active/Active Schedulers](./006-ha-distributed-query.md)
