# 33. Distributed Enterprise query and acceleration

A distributed Enterprise deployment separates the work of planning a query, storing an accelerated working set, executing tasks, and maintaining shared coordination state. That separation creates room to scale, but it also creates new boundaries where ownership, identity, and recovery must remain clear.

Chapter 22 introduced distributed query concepts. This chapter applies them to the Enterprise configuration and the operator's `SpicepodCluster` resource. All cluster launch and failure procedures here are integration labs; the book's offline manifest checks do not establish a running distributed deployment.

## 33.1 Define scheduler and executor pools

A v2 `SpicepodCluster` contains `schedulerSpec` and `executorSpec`. The scheduler specification carries the Spicepod. Executors obtain their application definition through the cluster bootstrap path, so the executor specification does not contain a second Spicepod to maintain.

```yaml
apiVersion: spice.ai/v2
kind: SpicepodCluster
metadata:
  name: northstar-cluster
  namespace: spice-book
spec:
  schedulerSpec:
    replicas: 2
    spicepod:
      version: v1
      kind: Spicepod
      name: northstar-enterprise
      runtime:
        scheduler:
          state_location: s3://REPLACE_BUCKET/northstar/cluster/
          params:
            region: us-west-2
            auth: iam_role
  executorSpec:
    replicas: 3
```

The complete companion template adds images, workload identity, credentials, resource requests, and executor PVCs. Two schedulers and three executors are illustrative pool sizes for a resilience exercise. They are not an availability proof or a throughput estimate.

The operator creates child SpicepodSets and manages cluster certificate resources. Treat the parent cluster specification as the desired-state input. Editing generated child resources by hand can conflict with reconciliation and obscure which configuration should survive the next operator pass.

## 33.2 Understand shared state before testing failover

The Enterprise design uses an object store for shared cluster state, including membership and partition ownership. Conditional writes support optimistic concurrency: a writer updates the state only if it still has the expected version, otherwise it must re-read and retry. The runtime source separately models scheduler instances and job ownership.

This makes the object store an active dependency. “The bucket is durable” does not establish that its conditional-write behavior, credentials, connectivity, and latency satisfy the cluster's coordination needs. Verify the chosen backend and preserve errors from conditional operations during failure testing. A plain writable folder or storage endpoint is not automatically equivalent to the supported shared-state contract.

Keep separate storage prefixes for separate clusters and environments. Do not point a staging cluster at production membership and job state. A shared bucket can be a container for several isolated prefixes, but identity policy and operational tooling must preserve that separation.

The supplied documentation describes multi-active schedulers and shared executors. Interpret this as a design whose recovery paths must be accepted for your release. A scheduler restart, lost executor, unavailable object store, stale credential, and interrupted result stream are different experiments. One successful scheduler restart does not establish all of them.

## 33.3 Keep internal mTLS distinct from client authentication

The internal cluster connection normally uses port `50052` with mutual TLS. The operator's cluster path provisions a root CA and node certificates. Runtime HTTP, Flight, and metrics are separate interfaces with their own exposure and authentication decisions.

Do not expose the internal cluster service as a public application endpoint. It participates in application bootstrap, secret resolution, task coordination, and partition control. The fact that a client-facing API uses HTTPS does not secure a different internal listener. Conversely, automatic cluster mTLS does not authenticate a browser user.

For a non-Kubernetes deployment, the supplied CLI documentation describes generating a cluster PKI and supplying CA, certificate, key, and advertised node addresses. Verify certificate names against the addresses peers actually use. A certificate can be valid in time and still fail hostname verification because an advertised address changed.

The insecure-development flag exists for controlled tests. Omit it from the Enterprise templates. Certificate expiry and rotation belong in the operational record alongside data-source credential rotation; test how existing and new connections behave during the change.

## 33.4 Partition an accelerated table by a business-compatible key

Distributed acceleration assigns partitions of a logical table to executors. This differs from running several complete independent copies of the table. The inspected cluster registration path requires partition keys for accelerated components participating in assignment. Use the supported accelerator and refresh combination for your release; the supplied Enterprise reference identifies Arrow and Cayenne as the distributed acceleration choices, with write-through requiring Cayenne.

A read-oriented integration fragment is:

```yaml
acceleration:
  enabled: true
  engine: cayenne
  mode: file
  refresh_mode: full
  partition_by:
    - "bucket(8, order_id)"
```

This is a fragment to add to a real supported dataset, not a complete cluster configuration. The eight buckets are a small lab choice. The partition expression is evaluated over the source schema. Test missing keys, NULLs, type changes, and the exact row placement before depending on it.

Static `bucket(N, column)` partitioning gives a bounded set of bucket identifiers without needing to enumerate distinct business values from the source. Other partition expressions can require source discovery. High-cardinality expressions may produce many partitions; several partition keys can multiply the number of combinations. Count the resulting partitions before treating the declaration as a scaling strategy.

Partition by access and maintenance behavior, not by a convenient field name alone. A tenant key can localize some tenant queries but leave one large tenant disproportionately hot. A bucketed order key can spread rows while making tenant requests touch many buckets. A date key can align with retention while concentrating recent writes. The right choice depends on observed plans, skew, and lifecycle requirements.

## 33.5 Read assignment and pruning evidence

The scheduler configuration exposes assignment cadence, a per-interval assignment cap, a per-executor soft partition cap, and a discovery timeout. In the inspected schema these include `partition_assignment_interval`, `max_partition_assignments_per_interval`, `max_partitions_per_executor`, and `partition_discovery_timeout`.

These controls affect how work is admitted and distributed. They do not mean that all partitions are immediately available after increasing the executor count. Observe discovery, committed ownership, executor readiness, and the time at which the expected rows become query-visible.

For Northstar, run a full key-set reconciliation and the paid-sales aggregation after initial assignment. Then run a tenant-filtered query and capture its plan. Only claim partition pruning if the plan and operator observations show which partitions were visited. A query returning the right four northern paid orders does not by itself prove that it avoided reading southern partitions.

A missing owner must not become an incomplete successful aggregate. The release acceptance should distinguish a query error from a partial result and verify the intended failure behavior while a required executor is unavailable. Preserve returned rows and status together. Do not weaken a correctness check merely to obtain a green availability indicator.

## 33.6 Understand write-through separately from replication lag

The Enterprise reference describes write-through as committing a supported write to the federated source, with acceleration catching up through its configured refresh mechanism. That is a different contract from treating the accelerator as the sole source of truth. Cluster write routing additionally needs partition ownership and a connector/engine combination that supports the operation.

Test one write at a time before testing concurrency. Record the source commit, response status, partition key, accelerator-visible value, and eventual reconciliation. Then test an update that changes a partition key, a duplicate request, and an interrupted response. Define whether the application requires immediate read-your-writes behavior and how it obtains it.

Do not enable write-through for a source merely because the accelerator supports it. The file and object-store labs in the earlier book are read-oriented. They do not become transactional operational databases by adding a write-mode field. Write-back has a separate durability and propagation contract and must not be assumed to be supported in cluster mode.

## 33.7 Test recovery at the query boundary

Use a bounded, repeatable workload that records request IDs, complete results, and errors while introducing one failure. Begin with a scheduler restart. Check whether new requests can use another scheduler, what happens to a query whose owner disappeared, and whether an asynchronous job can be discovered from a different scheduler.

Next, interrupt an executor holding known partitions. Record which requests fail, which remain valid, and how the partitions recover. Finally, exercise temporary object-store unavailability in an isolated environment. Do not mutate the shared state document manually as a substitute for a supported recovery procedure.

Measure request success, result correctness, recovery duration, and backlog on the same deployment. Keep the full run directory. Replica counts and architecture diagrams explain the intended mechanism; only the run establishes the recovery behavior you can present to users.

**Exercise.** Design a partitioning experiment with one tenant containing most orders. Compare tenant partitioning with bucketed order IDs using the same data and query set. Record the expected tradeoffs first, then preserve plans and per-executor measurements without inventing results in advance.

**Further reading.** The supplied `enterprise/features/distributed-query.md`, `distributed-accelerations.md`, and `mtls.md` describe the product interfaces. Source grounding includes the runtime cluster modules, `runtime-cluster`, scheduler configuration, and the operator's v2 cluster types. Chapter 34 continues with snapshot and rollout state.
