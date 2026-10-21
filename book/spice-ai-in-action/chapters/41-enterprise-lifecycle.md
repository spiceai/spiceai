# 34. Enterprise snapshots, storage, and safe rollouts

An accelerator's local files, an object-store snapshot, and an operator's retained standby generation solve different problems. Local files preserve state on a particular storage attachment. Snapshots publish a supported representation for later bootstrap. A standby generation preserves running workloads from a previous specification. A recovery design must say which one it will use and what data version that choice represents.

![Figure 34.1. Snapshot bootstrap restores a version that must be reconciled with the source contract.](figures/enterprise-snapshot.png)

## 34.1 Define a snapshot as a publication contract

The supplied Enterprise snapshot feature supports object-store-backed bootstrap and creation for selected file-based accelerators. The documented engine set includes DuckDB, SQLite, and Cayenne; Arrow's in-memory acceleration should be treated as reloaded from its source rather than assigned a fictitious file-snapshot path.

A useful snapshot record includes dataset identity, schema, accelerator format, configuration compatibility, partition identity where applicable, creation time, and integrity metadata. An object-store listing alone does not tell a consumer whether a particular artifact is the right state for its application version.

The runtime has a global snapshot configuration and a per-dataset opt-in. Configure both. The following is an integration fragment for an entitled build and a separately provisioned S3 location:

```yaml
snapshots:
  enabled: true
  location: s3://REPLACE_BUCKET/northstar/snapshots/
  bootstrap_on_failure_behavior: fallback
  params:
    region: us-west-2
    s3_auth: iam_role
```

The snapshot store and scheduler store are separate configuration concepts even when they use the same cloud account or bucket. Keep their prefixes and permissions explicit. The parameter spelling for one object-store integration should not be copied blindly into another component.

## 34.2 Select producer and consumer behavior

The per-acceleration modes distinguish bootstrap from publication:

| Mode | Loads an existing snapshot | Creates snapshots | Typical role |
|---|---|---|---|
| `disabled` | No | No | Source-loaded acceleration without snapshot participation |
| `enabled` | Yes | Yes | A runtime that both recovers and publishes its owned state |
| `bootstrap_only` | Yes | No | A consumer of published snapshots |
| `create_only` | No | Yes | A designated producer that starts from its source |

For an owning Cayenne acceleration, an example fragment is:

```yaml
acceleration:
  enabled: true
  engine: cayenne
  mode: file
  snapshots: enabled
  snapshots_trigger: time_interval
  snapshots_trigger_threshold: "10m"
  snapshots_creation_policy: on_change
  snapshots_compaction: enabled
```

The ten-minute interval is a teaching value. A snapshot trigger is not a promise that a new complete artifact exists exactly every ten minutes. Creation duration, failures, source change, and the `on_change` policy affect publication. Alert on observed publication and recovery behavior, not just the configured interval.

In a partitioned cluster, relate the snapshot to the partition assignment and the supported ownership model. Do not create an ad hoc second writer by pointing unrelated runtimes at the same snapshot prefix. A read replica's `bootstrap_only` setting prevents it from publishing, but it does not independently establish who the canonical producer is.

## 34.3 Choose bootstrap failure behavior for the product

The documented bootstrap choices are `warn`, `retry`, and `fallback`. They represent different ways to proceed when the newest snapshot cannot be loaded: continue toward source-based recovery, keep attempting bootstrap, or try older generations. The exact startup, readiness, and refresh behavior must be accepted with the selected engine and release.

For Northstar, an older valid snapshot can be useful for a dashboard if the response exposes its age. The same data may be unacceptable for a time-sensitive eligibility decision. An empty working set must never be presented as zero business activity simply because startup continued.

Design the test around an isolated copy of the snapshot namespace. Make the newest generation unavailable, attempt bootstrap, and capture the selected generation, logs, readiness transitions, and returned rows. Then restore access and observe how the system reaches the current state. Do not corrupt the only production recovery artifact to perform this experiment.

If a consumer uses `snapshots_reset_expiry_on_load`, understand the resulting retention-clock semantics. Loading data later does not change when the underlying business event happened. Keep event time, publication time, load time, and retention policy separate in the application contract.

## 34.4 Connect snapshot age to CDC recovery

A snapshot can be internally consistent yet older than changes committed at the source. A CDC-backed acceleration therefore needs a compatible restart position and enough retained source history to catch up. The source's retention window, replication checkpoint, accelerator state, and snapshot generation form one recovery design.

Reconcile final keys and values after bootstrap and replay. A row count alone misses an update that changes an amount without changing cardinality. Include deletion, a repeated key update, and a transaction committed while the consumer was unavailable. Preserve the source-side evidence together with the recovered query result.

Do not describe a snapshot as a complete backup of the application unless the recovery record also covers configuration, policies, model artifacts, external dependencies, and the data ownership model. A snapshot of an analytics replica is not a backup of the operational source's entire transactional history.

## 34.5 Budget storage for transitions

Storage must cover more than the steady-state compressed dataset. Add accelerator metadata, active writes, temporary query work, compaction inputs and outputs, snapshot staging, and the overlap between generations. A large rebuild can also increase source traffic and network transfer while consuming local storage.

Use the storage class's actual attachment and zone constraints. A persistent volume may survive a pod restart while remaining unavailable on a replacement node in another zone. Local NVMe may offer useful bandwidth while requiring reconstruction after node loss. Provisioning a PVC is the start of the storage contract, not the end.

Record the observed high-water marks from a refresh, compaction, snapshot, and restore rehearsal. Keep headroom for the largest expected transition. Do not treat a generic fraction of dataset size as a measured safety margin for every accelerator and query workload.

## 34.6 Match rollout strategy to state ownership

The current operator exposes `RollingOrdered`, `RollingParallel`, and `BlueGreen`. Ordered rolling updates replace replicas in sequence. Parallel rolling updates can bound concurrent disruption with `maxUnavailable`. Blue-green creates a new generation and changes the Service's selected version after the configured readiness condition.

The storage distinction is central: the inspected blue-green design creates new PVCs for a new generation. It does not reuse the old generation's local contents as an in-place update would. Plan source reload or supported snapshot bootstrap before selecting this strategy for a large accelerated working set.

```yaml
spec:
  updateStrategy:
    type: BlueGreen
  standbyVersion:
    enabled: true
    retentionPeriodSeconds: 900
```

This is a `SpicepodSet` fragment. The retained standby feature belongs to that resource's supported interface; do not assume the same field is accepted directly under a cluster node specification. The operator's content-based version identity allows a previous retained specification to be recognized for a traffic switch. Record the full previous specification rather than trying to synthesize an internal version label.

Retaining a standby costs capacity. It also retains an older application and potentially older data. After cutover, verify both the active version and the state of the retained generation. A fast traffic switch is valuable only if the selected generation can satisfy the current product contract.

## 34.7 Upgrade the control plane and data plane as related releases

Plan operator, CRD, runtime, and configuration upgrades independently but test them together. Read the matching release and migration instructions. Render the new chart, inspect schema changes, validate representative custom resources, and only then rehearse the runtime change.

Helm rollback is not a universal state-format rollback. A previous operator may not understand a newly stored CRD field; an older runtime may not accept a newer accelerator format or snapshot. Keep the old artifacts and test the actual downgrade or restoration route before making it the incident plan.

A disruption budget addresses supported voluntary evictions. It does not force a controller's own rollout algorithm to preserve every application invariant, nor does it restore a failed storage zone. Check generated pod labels before selecting a budget's targets, and measure availability through real requests during maintenance. The upstream [Kubernetes disruption-budget guide](https://kubernetes.io/docs/tasks/run-application/configure-pdb/) explains the eviction boundary.

## 34.8 Rehearse a complete restore

Start in an isolated namespace and storage prefix. Restore the reviewed configuration and identities, select a known snapshot, bring the runtime to readiness, replay or refresh to the required source boundary, and execute the business checks. Then run the tenant-policy and application-service checks.

Record the elapsed recovery stages and the exact data version available at each. Distinguish “process running,” “snapshot loaded,” “dataset queryable,” and “application contract restored.” The final one is the recovery objective the user actually experiences.

**Exercise.** Compare an ordered rollout with a blue-green rollout for a Northstar acceleration that takes significant time to rebuild. Account for peak storage, source load, snapshot age, standby retention, and the response behavior during a failed bootstrap. State the acceptance evidence required before choosing either.

**Further reading.** Consult `enterprise/features/acceleration-snapshots.md`, `enterprise/production/storage.md`, and `enterprise/production/upgrades.md`, together with the pinned operator's current update-strategy and standby documentation. The snapshot source under `runtime-acceleration` establishes the build and behavior boundaries used here.
