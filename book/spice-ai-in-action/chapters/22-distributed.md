# 22. Distributed query and asynchronous jobs

Northstar's interactive sales cards fit comfortably in a bounded serving set. A yearly report over a large lakehouse may require more aggregate CPU, memory, and I/O than one node should supply. Distributed query introduces schedulers and executors to coordinate eligible work across machines. It also introduces overhead and new failure states, so begin with a workload that justifies it.

## 22.1 The scheduler and executor boundary

A scheduler receives or coordinates query work, plans stages, and tracks execution. Executors perform assigned tasks and exchange or publish intermediate results according to the configured architecture. Spice builds its distributed-query integration on Apache Ballista with additional runtime, catalog, and security integration.

Not every query benefits from distribution. A tiny lookup can spend more effort on scheduling and communication than on computation. A query with a serial bottleneck or a heavily skewed key can remain constrained after adding nodes. Measure the critical path rather than treating executor count as a speed multiplier.

![Figure 22.1. Distributed execution separates coordination, task execution, and shared state.](figures/cluster.png)

## 22.2 Rehearse on isolated instances

The cookbook `distributed/` recipe provides a scheduler-and-executor lab with development mTLS certificates. Use separate instance directories and distinct listener ports for each process. The development binary used while writing this book enforces single-instance ownership of its runtime directory; a second process should not share identity and state accidentally.

The CLI recipe generates a development CA and node certificates with `spice cluster tls`. These are for a lab. Production certificates need a managed issuance, trust, rotation, and revocation process appropriate to the cluster.

A scheduler is started with `--role scheduler`, an internal node bind and advertise address, its public listeners, and the cluster mTLS files. An executor uses `--role executor`, a scheduler address, its own node address, and its own certificate. Keep the full commands in the versioned deployment manifest; do not reuse a scheduler certificate for every executor.

## 22.3 State location is an architectural choice

The local recipe configures:

```yaml
runtime:
  scheduler:
    state_location: file:///tmp/northstar-cluster-state
```

A local path is suitable for understanding one machine's lab. It is not a shared state service for schedulers on different hosts. A highly available deployment needs the supported shared-state backend and the storage semantics required by the release, including conditional operations where the design relies on them.

The source architecture decisions describe coordination through shared state and object storage. An accepted design document is architectural context, not proof that every failure mode is supported by an installed binary. Test the specific release and deployment's scheduler-loss behavior.

## 22.4 Data must be reachable where tasks execute

A file path readable by the scheduler is not necessarily readable by an executor on another host. Object-store credentials, connector configuration, extensions, and relevant model or function capabilities must be available through the supported distribution mechanism.

Use a source accessible to the intended executors and verify it from the actual workload. A query that happens to run locally on the scheduler does not prove executor access. Capture task assignments and executor logs or metrics so you can show where work ran.

Catalog and object-store registration can involve wrappers and deferred initialization. For a connector or runtime change, integration tests must exercise the remote task path. A locally passing scan cannot validate those boundaries.

## 22.5 Shuffle, skew, and intermediate size

A distributed join or aggregation can repartition rows by key. The shuffle volume depends on the rows and columns that survive earlier operators. Push selective filters and projections into earlier stages where the plan can do so correctly.

Skew occurs when a few keys account for a disproportionate share of work. One executor can become the tail while others finish. Capture per-task rows, bytes, duration, and spills. A cluster-wide average can hide the overloaded partition.

Intermediate state also needs storage and retention. Decide where shuffle data lives, how failed tasks are retried, and when abandoned results are reclaimed. Disk exhaustion in a temporary directory can be as operationally significant as exhaustion of the primary accelerator volume.

## 22.6 Asynchronous queries are owned resources

The inspected cookbook's async-query interface requires a compatible cluster deployment. The runtime source exposes `/v1/queries` submission and job-specific status, results, and cancellation routes. Consult the release's OpenAPI schema for request fields and state names.

An asynchronous application submits a job, records its identifier, polls with a deadline or resumes later, fetches bounded result pages, and handles terminal failures. The submitting identity must own or be authorized to read the job and its result objects. Do not expose a job ID as a universal bearer capability unless that is an intentional, protected design.

Poll at a bounded cadence with backoff appropriate to job duration. A long report does not need dozens of status requests per second. Preserve the last observed state on timeout and distinguish a client that stopped waiting from a server-side cancellation.

## 22.7 Acceptance tests for a cluster

Run a query whose plan and task artifacts demonstrate executor work. Verify returned rows against a single-node or source reference. Then, in a disposable cluster, interrupt an executor during a sufficiently long job and observe task recovery or the documented failure. Repeat for scheduler loss only within the supported high-availability configuration.

Keep the query ID, task IDs, node identities, timestamps, logs, and result validation. A load balancer returning HTTP 200 from another scheduler proves that a listener is available, not that an accepted job survived.

Test cancellation and abandoned result cleanup. Test credential rotation with executor access to object storage. Test scale-down while tasks are active. The operating model needs these answers before an overnight report becomes a production dependency.

## 22.8 Decide whether to distribute

Compare a tuned single-node baseline with the cluster on the same data and workload definition. Include scheduling overhead, transfer, storage requests, and operational cost. Report concurrent throughput and tail behavior, not only the fastest isolated query.

Northstar can keep interactive summaries on a local or shared accelerated service and route broad exports to asynchronous cluster jobs. That product split makes the latency and consistency expectations explicit rather than forcing every request through the largest available topology.

**Exercise.** Choose a large report and draw its likely stages. Identify the largest intermediate relation, the key most likely to be skewed, and the data each executor must access. Define the artifact that will prove distribution actually occurred.

**Further reading.** See cookbook `distributed/` and `async-queries/`, the [distributed-query documentation](https://spiceai.org/docs/features/distributed-query), and source decisions `004-distributed-query-framework.md`, `005-ballista-extensions.md`, `006-ha-distributed-query.md`, and `007-cluster-mtls.md`.
