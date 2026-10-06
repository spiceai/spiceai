# 30. Choosing and introducing Spice.ai Enterprise

Northstar's local application has a clear contract: the correct tenant receives the correct facts and policy evidence. An enterprise deployment adds a different set of questions. Who can install a runtime? Which identity can read the source? How do operators roll out a new policy? What happens when a node disappears? Who owns the recovery procedure? Spice.ai Enterprise supplies additional runtime and deployment capabilities for these concerns, while the organization retains responsibility for its own infrastructure and application behavior.

This section extends the earlier chapters with the Enterprise distribution, Cedar authorization, the Kubernetes operator, distributed accelerations, snapshots, and advanced function and inference facilities. It is grounded in the supplied Enterprise checkout, operator checkout, and Cloud/Enterprise documentation. Those repositories have different commit identities and release schedules. The examples therefore identify the interface they target instead of treating “Enterprise” as a single version number.

## 30.1 Separate product capability from operating responsibility

Three deployment models recur in the documentation. OSS gives you the open-source runtime and its available build capabilities. Cloud provides a managed service. Enterprise provides a self-hosted distribution and associated enterprise capabilities and support arrangements. A self-hosted Enterprise runtime still needs storage, networking, identity, backup, monitoring, and an upgrade owner.

The practical comparison is an ownership table:

| Concern | Decision for a self-hosted Enterprise deployment |
|---|---|
| Runtime distribution | Select an image containing the connectors, accelerator, and model support required by the application. |
| Infrastructure | Allocate nodes, durable storage, network access, and failure domains. |
| Data identity | Give each runtime role the source and object-store privileges it actually needs. |
| Application identity | Configure API keys or the supported OIDC integration and test identity propagation. |
| Authorization | Define allowed endpoints, datasets, models, and tools; verify row and column restrictions. |
| Lifecycle | Pin and promote the operator, CRDs, runtime image, Spicepod, and policies together as a reviewed release. |
| Recovery | Demonstrate that the chosen topology can restore the business contract after the specified failures. |

Commercial entitlement, support windows, and service commitments belong to the current agreement. This book does not turn a documentation comparison table into a contractual promise. Equally, a support agreement does not establish that your database's credentials, storage class, or tenant policy are correct.

## 30.2 Choose a distribution by the work it must perform

The supplied Enterprise distribution documentation describes general data-and-AI images, data-only builds, NAS connectivity, GPU-oriented builds, allocator variants, and prebuilt optional integrations such as ODBC and advanced function tiers. The runtime is 64-bit. Select the architecture and CPU/GPU capabilities supported by the actual image and nodes.

Start with a capability inventory. Northstar's analytics service needs its source connectors, a supported accelerator, SQL APIs, and observability. The support assistant additionally needs search, its embedding implementation, and access to a configured generator. A deployment using only remote model inference has a different local memory and device requirement from one loading model weights into a GPU.

Keep image selection explicit in the deployment record. A tag that includes models does not establish that every desired provider is configured. A CUDA image does not provision a device plugin, allocate GPU resources, or guarantee that a model fits. An ODBC-enabled image does not supply every database vendor's driver and connection policy. A NAS connector does not grant access to a share.

Allocator selection belongs to workload measurement. Run the same query set, data, concurrency, and memory limits before comparing allocator images. Preserve the output and memory evidence. Do not transfer a percentage improvement from a distribution description to an unrelated application.

## 30.3 Record five independent versions

An enterprise release has at least five identities: runtime image, operator image/chart, Kubernetes CRD API and schema, embedded Spicepod document, and application/policy bundle. Add the data and model artifacts where relevant. An image digest identifies bytes; a Spicepod format version identifies a document contract; `spice.ai/v2` identifies a Kubernetes API. These are different uses of the word *version*.

For this section, the inspected operator's chart declares version `1.1.0`, while its workload image default refers to a separate runtime line. That is a concrete reason to maintain a compatibility record rather than infer that all numbers must match. The book's Kubernetes examples use `spice.ai/v2`, with an object-valued `spicepod` field. They do not mix those fields with legacy snake_case manifests.

A useful release record includes the repository commit, immutable image identity, chart values, CRD storage version, configuration hash, policy revision, required source privileges, and acceptance artifacts. Keep the previous accepted record alongside it. “Roll back to yesterday” is ambiguous if a mutable tag, policy service, or model locator has changed since yesterday.

## 30.4 Choose a topology before choosing a replica count

A `SpicepodSet` manages replicas of a Spice application. Two replicas can serve the same logical application while each has its own local accelerated state. Their freshness may differ, and each can impose ingestion work on the source. Increasing the replica count does not automatically create a sharded shared database.

A `SpicepodCluster` defines scheduler and executor pools for distributed execution. The scheduler coordinates work and partition ownership; executors perform distributed tasks and hold assigned accelerated partitions. Shared object-store state participates in the cluster's coordination and recovery model. Chapter 33 develops this architecture.

An application sidecar places a runtime next to the application container. This can simplify local connectivity, but it couples scheduling and resource contention to the application pod. Snapshot-based bootstrap can change how a sidecar receives accelerated data; it does not eliminate the need to identify who produces that data and how current it is.

Choose a topology for a stated reason: bounded local access, independent application replicas, a working set that needs partitioning, centralized administration, or a required recovery model. Adding scheduler and executor pools to a tiny fixture is an architecture exercise, not evidence that the fixture needs a cluster.

## 30.5 Establish three identity planes

The operator identity talks to Kubernetes. It creates and reconciles the resources its permissions allow. The workload identity talks to data sources, secret stores, model providers, and cluster storage. The request identity represents the user or service asking Spice to perform an operation.

Keep those identities separate in both diagrams and deployment manifests. Annotating the operator's ServiceAccount for cloud access does not necessarily annotate the ServiceAccount used by a managed runtime. Granting a pod access to an S3 bucket does not authorize a particular end user to query every dataset in that bucket. A valid OIDC token does not grant a runtime permission to read a PostgreSQL replication slot.

For Northstar, the workload can have a restricted data-reader identity, while a user token carries the tenant and analyst role. Cedar policy then constrains what the user can do through the runtime. The application service continues to expose bounded product operations instead of assuming that policy makes every arbitrary query an appropriate product API.

## 30.6 Introduce Enterprise through an integration sequence

Begin with a runtime whose only application query is `SELECT 1`. Verify image access, startup, HTTP authentication, and the selected Service path. Then introduce one dataset with a known key set and total. Only after that baseline passes should you add OIDC, policy, acceleration, distributed placement, and recovery automation.

This sequence makes failures distinguishable. An image-pull failure is not a Cedar decision. A valid token rejected at a dataset boundary is not a missing Service endpoint. A source connection failure is not evidence that the partition assignment logic is wrong. Each stage adds a new dependency while preserving the prior checks.

The companion Enterprise directory contains two Kubernetes templates and a renderer. The templates are structurally checked against the CRDs rendered from the pinned operator chart. They still require a licensed or otherwise entitled runtime image, registry credentials, a Kubernetes cluster, secrets, and—in the cluster case—storage and workload identity. Offline validation is not a deployment transcript.

**Exercise.** Write Northstar's ownership table for your organization. Assign a named team to runtime releases, source access, user identity, policy changes, snapshot retention, and recovery. Identify one failure that crosses two teams and write the handoff evidence each needs.

**Further reading.** Use the supplied documentation under `enterprise/getting-started/`, `enterprise/deployment/`, and `enterprise/production/`. The public entry point is [Spice.ai Enterprise](https://docs.spice.ai/docs/enterprise). Appendix F records the three additional repository commits used for this section.
