# 21. Packaging and deployment

A successful local runtime becomes a service only when its configuration, storage, identity, and lifecycle are reproducible. Northstar's first deployment should make those decisions visible rather than burying them in a container command copied from a demo.

## 21.1 Package the application state

A deployable unit includes the binary or image identity, Spicepod, referenced SQL files, required connector features, model artifacts or provider configuration, and secret references. Persistent accelerator state is usually a separate volume or managed storage resource with its own lifecycle.

Pin the runtime image by an approved tag and preferably a digest in the deployment record. The existence of a newer release does not establish that its connectors, model backends, and persisted formats are compatible with your application. Rehearse upgrades against the query contract and copied state.

Do not include `.env` files, database passwords, local evidence logs, or a developer's cached credentials in an image build context. Use a minimal context and inspect the resulting image contents when creating a production packaging process.

## 21.2 A local container pattern

The following is a deployment template. Replace `SPICE_IMAGE` with the image reference that passed your acceptance checks:

```bash
export SPICE_IMAGE='spiceai/spiceai:REPLACE_WITH_TESTED_TAG'
docker run --rm \
  -p 127.0.0.1:8090:8090 \
  -p 127.0.0.1:50051:50051 \
  -v "$PWD/spicepod.yaml:/app/spicepod.yaml:ro" \
  -v "$PWD/data:/app/data:ro" \
  "$SPICE_IMAGE"
```

The placeholder deliberately prevents the listing from silently selecting a moving release. Check the image's working directory and entrypoint in its published deployment instructions. If you pass explicit listener flags, ensure they bind appropriately inside the container; a container's loopback interface is not the host's.

For file-backed acceleration, mount the configured writable data path separately and create its parent directory with permissions appropriate to the container user. Verify that a restart with the same volume restores the intended state. An ephemeral container filesystem is not persistent acceleration storage.

## 21.3 Health, readiness, and startup

Use `/health` to establish that the process can respond and `/v1/ready` for the runtime's readiness contract. Add application-specific probes when the product depends on a particular view or freshness bound beyond generic readiness.

A large initial load may legitimately take longer than ordinary request deadlines. Give startup a bounded but realistic budget. Avoid a liveness policy that repeatedly kills the process while it is successfully bootstrapping. Conversely, do not keep a process in service merely because it responds to health while essential data is unavailable.

Kubernetes separates startup, readiness, and liveness probes. A representative probe fragment is:

```yaml
startupProbe:
  httpGet:
    path: /health
    port: 8090
  periodSeconds: 5
  failureThreshold: 60
readinessProbe:
  httpGet:
    path: /v1/ready
    port: 8090
  periodSeconds: 5
livenessProbe:
  httpGet:
    path: /health
    port: 8090
  periodSeconds: 10
```

These values are illustrative. Tune them from observed startup and recovery behavior, and check the deployment's response to readiness failures. A probe schedule is not a substitute for a data-freshness monitor.

## 21.4 Resource budgets must include overlap

Budget CPU and memory for queries, refresh or CDC, model inference, caches, and maintenance. A node that fits a loaded dataset while idle may fail during a full refresh or a concurrent join. Container memory limits apply to the process and its allocations, not only the query pool.

The inspected runtime supports a query-memory setting:

```yaml
runtime:
  query:
    memory_limit: 2GiB
```

This is an illustrative query budget. It is not a promise that process RSS remains below 2 GiB. Leave capacity for other components and observe the combined workload. On the source branch, CPU sizing is centralized in the CPU-budget subsystem; specify explicit entitlement where the deployment requires it and verify the resulting behavior.

For local disks, account for current data, refresh overlap, compaction temporary space, retained snapshots, and logs. A storage volume sized only to the current table files can run out during ordinary maintenance.

## 21.5 Sidecar or shared service

A sidecar aligns the runtime lifecycle with the application replica and can simplify local communication. It also multiplies storage, connections, and ingestion across replicas. Before scaling the application from three to thirty replicas, calculate the effect on the source and model downloads.

A shared service can consolidate those resources but needs explicit concurrency, fairness, and availability policies. Separate heavy report traffic from latency-sensitive requests if they interfere. A shared service's failure now affects multiple applications, so ownership and rollout practices must reflect that scope.

A hybrid arrangement can keep a small local serving set while using a larger shared service for broader work. Be explicit about which requests use which path and how their freshness and failure behavior differ.

## 21.6 Rollouts and state compatibility

For stateless configuration changes, a rolling replacement may be straightforward. For persistent accelerators and replication, the old and new versions can have different state expectations. Test an upgrade on a copy and establish whether rollback can reuse the migrated state or requires restoring a backup or rebuilding.

Do not run two independently writing embedded-engine instances against the same volume merely to achieve a rolling update. Use the supported ownership pattern. A deployment controller's ability to start a second pod does not establish that the storage protocol supports two writers.

Drain requests during shutdown according to the runtime and orchestration settings. Verify how long queries, refresh tasks, and replication checkpoints behave when termination begins. Keep the termination grace period aligned with the observed and documented shutdown behavior.

## 21.7 Configuration is part of the release

Save a release manifest containing image digest, Spicepod digest, referenced SQL digests, model identity, source schema version, secret versions or references, storage locations, and acceptance evidence. A rollback instruction should name the artifacts to restore, not merely say “use the previous container.”

**Exercise.** Design a deployment for two application replicas with persistent acceleration. Decide whether each replica owns its own data, whether a shared service is better, and what happens during a rolling upgrade. Include the source connection and replication-slot implications.

**Further reading.** See the [Docker deployment guide](https://spiceai.org/docs/deployment/docker), cookbook `docker/`, and the OSS deployment documentation matching your topology. Verify Kubernetes and cloud-provider behavior against their current primary documentation before applying manifests.
