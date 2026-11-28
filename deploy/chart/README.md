# Spice.ai Helm Chart

Deploy the Spice.ai runtime on Kubernetes.

## Install

```bash
helm install spiceai deploy/chart \
  --set spicepod.name=my-app
```

## Authentication

The runtime requires an API key on HTTP (8090) and Flight (50051). On first install the chart generates a read-write key into the Secret `<release>-spiceai-api-key`, keeps it across upgrades and uninstalls, and the runtime reads it through `runtime.auth`. Read it with:

```bash
kubectl get secret spiceai-spiceai-api-key -o jsonpath='{.data.api-key}' | base64 -d
```

Send it as `X-API-Key: <key>` over HTTP, or as the password of a Flight handshake. The `/health` and `/v1/ready` probes need no key.

The runtime reads the key from the `SPICE_API_KEY` environment variable. When `spicepod.secrets` declares secret stores, the chart reads it through the first `from: env` store without `params`. If there is none, the chart adds a `from: env` store named `spiceai_chart_env` before your stores, so your stores keep their order and are searched first for `${ secrets:KEY }`.

- **Your own key**: create a Secret with an `api-key` entry and set `auth.apiKey.existingSecret` to its name.
- **Your own `runtime.auth`**: when `spicepod.runtime.auth` is set, the chart adds no key and uses yours as written. Behind a `NodePort` or `LoadBalancer` Service it must enable an API key, unless the runtime requires client certificates; see [Services outside the cluster](#services-outside-the-cluster).
- **No API key**: `auth.mode: none`. The chart refuses it with a `NodePort` or `LoadBalancer` Service unless the runtime requires client certificates; see [Services outside the cluster](#services-outside-the-cluster). Anonymous callers then get the runtime's rules for unauthenticated callers on a non-loopback listener, and the chart adds the Service's names to the Spicepod's allowed hosts so in-cluster clients can address it by name: to `runtime.allowed_hosts`, or to the deprecated `runtime.mcp.allowed_hosts` when the Spicepod sets only that list.

### Services outside the cluster

A `NodePort` or `LoadBalancer` Service serves callers outside the cluster, so the chart renders one only for a runtime that authenticates every caller, and refuses the render otherwise. The runtime authenticates every caller when either:

- `runtime.auth` has an `api-key` entry (or its alias `api_key`) whose `enabled` is not `false`. `auth.mode: api_key`, the default, writes one; a `spicepod.runtime.auth` is read as written.
- TLS is on and requires a client certificate: `spicepod.runtime.tls.enabled: true` and `spicepod.runtime.tls.client_auth_mode: required`. This holds with any `auth.mode`, `none` included. `client_auth_mode: request` admits callers without a certificate, and `client_auth_mode` has no effect while TLS is off, so neither counts.

So the chart refuses `auth.mode: none` without required client certificates, and a `spicepod.runtime.auth` that is null or empty, has no `api-key` entry, has a null `api-key`, or sets `enabled: false`. Each refusal names the setting and its fix.

The chart also reads the flags in a custom `command` that change this: `--tls-enabled true` turns TLS on, and `--tls-client-auth-mode` takes precedence over `spicepod.runtime.tls.client_auth_mode`. `--set-runtime` for `auth` or `tls` changes the Spicepod after the chart has read it, so the chart refuses it unless the setting it does not change authenticates callers; set those in `spicepod.runtime` instead. The chart cannot read flags that a script in `command` adds.

### GitOps (Argo CD, `helm template`)

A render that cannot read the cluster, such as `helm template`, `helm install --dry-run` without `=server`, or Argo CD, cannot reuse a generated key, so it fails unless `auth.apiKey.existingSecret` is set. Create the Secret (for example with External Secrets or Sealed Secrets) and set `auth.apiKey.existingSecret`.

## Configuration

### Values

| Parameter                          | Description                                                                  | Default                           |
| ---------------------------------- | ---------------------------------------------------------------------------- | --------------------------------- |
| `image.repository`                 | Container image repository                                                   | `ghcr.io/spiceai/spiceai-nightly` |
| `image.tag`                        | Container image tag                                                          | `latest-models`                   |
| `replicaCount`                     | Number of replicas                                                           | `1`                               |
| `role`                             | `spiced --role`: `server`, `sidecar`, `scheduler`, or `executor`             | `server`                          |
| `cluster.schedulerAddress`         | Scheduler's cluster address; required for `role: executor`                   | `''`                              |
| `cluster.advertiseAddress`         | Host or IP other cluster nodes reach this node at                            | the pod IP                        |
| `cluster.port`                     | Port of the cluster's internal gRPC service                                  | `50052`                           |
| `cluster.tls.secretName`           | Secret with `ca.crt`, `tls.crt`, and `tls.key` for cluster mTLS              | `''`                              |
| `cluster.allowInsecureConnections` | Run the cluster without mTLS (development and testing only)                  | `false`                           |
| `serviceAccount.create`            | Create a ServiceAccount                                                      | `false`                           |
| `serviceAccount.name`              | ServiceAccount name (auto-generated if empty and `create` is true)           | `null`                            |
| `serviceAccount.annotations`       | Annotations for the ServiceAccount                                           | `{}`                              |
| `service.type`                     | Kubernetes Service type (`ClusterIP`, `NodePort`, `LoadBalancer`, or `null`) | `null`                            |
| `service.additionalAnnotations`    | Additional annotations on the Service                                        | `{}`                              |
| `service.ports`                    | The Service's ports                                                          | `http` and `flight`               |
| `auth.mode`                        | `api_key`: require an API key; `none`: no API key (see Authentication)       | `api_key`                         |
| `auth.apiKey.existingSecret`       | Secret with the API key under `api-key`; empty: the chart generates one      | `''`                              |
| `livenessProbe`                    | Kubernetes liveness probe configuration                                      | see `values.yaml`                 |
| `readinessProbe`                   | Kubernetes readiness probe configuration                                     | see `values.yaml`                 |
| `startupProbe`                     | Kubernetes startup probe configuration                                       | see `values.yaml`                 |
| `stateful.enabled`                 | Use a StatefulSet with PVC                                                   | `false`                           |
| `stateful.storageClass`            | StorageClass for StatefulSet PVC                                             | `standard`                        |
| `stateful.size`                    | PVC size                                                                     | `1Gi`                             |
| `stateful.mountPath`               | Mount path in container                                                      | `/data`                           |
| `monitoring.podMonitor.enabled`    | Create a PodMonitor for Prometheus                                           | `false`                           |
| `monitoring.podMonitor.tlsConfig`  | Prometheus Operator `SafeTLSConfig` for the scrape; set: scrape over HTTPS   | `{}`                              |
| `additionalLabels`                 | Labels added to all resources                                                | `{}`                              |
| `additionalEnv`                    | Extra environment variables                                                  | `[]`                              |
| `resources`                        | CPU/memory requests and limits                                               | `{}`                              |
| `volumes`                          | Additional volumes                                                           | `[]`                              |
| `volumeMounts`                     | Additional volume mounts                                                     | `[]`                              |
| `spicepod`                         | Spicepod configuration (inlined into a ConfigMap)                            | see `values.yaml`                 |

### Roles

`role` selects how the runtime runs, as `spiced --role` does. Omitted, it is `server`, which serves queries over its own datasets.

`role: sidecar` serves an application's queries beside it, answering from its results cache and accelerations and sending reads from a remote Spice runtime as Substrait plans created locally. See [Application sidecars](../../docs/features/cluster-sidecar.md). The chart runs the sidecar as its own workload; to share a pod with your application, add a `spiced --role sidecar` container to the application's pod spec.

A distributed cluster is two releases: one with `role: scheduler` and one with `role: executor` whose `cluster.schedulerAddress` points at the scheduler's Service. Both expose the cluster port, `50052` by default. Each release runs as a StatefulSet behind its own headless Service, and each pod advertises its stable per-pod DNS name `<pod>.<release>-spiceai-headless.<namespace>.svc.cluster.local`, which is where the other cluster nodes connect to it; `cluster.advertiseAddress` overrides it. A StatefulSet name is at most 52 characters, so that its pods, `<statefulset>-0`, `<statefulset>-1`, and so on, have names Kubernetes accepts: `<release>-spiceai` when that fits, and otherwise that name cut to 45 characters plus a 6-character suffix from a hash of the whole name, so two releases whose names differ only past the cut still get different StatefulSets. The headless Service follows the same rule: `<release>-spiceai-headless` when `<release>-spiceai` is at most 54 characters, and otherwise that name cut to 47 characters, a 6-character suffix from a hash of the whole name, and `-headless`. Name that Service in each node's certificate; `kubectl get service` shows it. Clients and `cluster.schedulerAddress` keep using the scheduler's Service. The scheduler's `spicepod` needs `runtime.state.location`:

```bash
kubectl create secret generic spice-api-key --from-literal=api-key="$(openssl rand -hex 16)"

helm install spice-scheduler deploy/chart \
  --set role=scheduler \
  --set cluster.tls.secretName=spice-scheduler-tls \
  --set auth.apiKey.existingSecret=spice-api-key \
  --set spicepod.runtime.state.location=s3://my-bucket/spice-state

helm install spice-executor deploy/chart \
  --set role=executor \
  --set replicaCount=3 \
  --set cluster.schedulerAddress=https://spice-scheduler-spiceai:50052 \
  --set cluster.tls.secretName=spice-executor-tls \
  --set auth.apiKey.existingSecret=spice-api-key
```

Both releases name the same `auth.apiKey.existingSecret`, so every node accepts the key that clients send to the scheduler.

Cluster nodes authenticate each other with mTLS. Each release's Secret holds `ca.crt`, `tls.crt`, and `tls.key`, the layout cert-manager writes, and **each certificate must name the address its node is reached at**: for the schedulers, the scheduler Service host, which executors dial first, and each scheduler pod's per-pod DNS name; for the executors, each executor pod's per-pod DNS name. A node whose certificate does not name its advertise address refuses to start, and the scheduler refuses to register such an executor, because peers could not reach it there ([distributed query](https://spiceai.org/docs/features/distributed-query)).

**Give schedulers and executors separate certificates.** A node is trusted as a scheduler by the identity of the scheduler's certificate, so a certificate shared by both roles would let any executor act as the scheduler. Issue two cert-manager `Certificate`s from the same issuer, each with its own Secret, and set `cluster.tls.secretName` per release. One wildcard DNS name per role covers every pod of that release:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: spice-scheduler-tls
spec:
  secretName: spice-scheduler-tls       # cluster.tls.secretName of the scheduler release
  issuerRef:
    name: spice-cluster-ca              # a cert-manager Issuer backed by the cluster CA
    kind: Issuer
  commonName: spice-scheduler
  usages: [digital signature, key encipherment, server auth, client auth]
  dnsNames:
    - spice-scheduler-spiceai           # the scheduler Service host
    - "*.spice-scheduler-spiceai-headless.default.svc.cluster.local"  # every scheduler pod
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: spice-executor-tls
spec:
  secretName: spice-executor-tls        # cluster.tls.secretName of the executor release
  issuerRef:
    name: spice-cluster-ca
    kind: Issuer
  commonName: spice-executor
  usages: [digital signature, key encipherment, server auth, client auth]
  dnsNames:
    - "*.spice-executor-spiceai-headless.default.svc.cluster.local"   # every executor pod
```

Set `cluster.advertiseAddress` only to override the defaults above; the value must be a name the certificate covers. For development and testing on a trusted network, `cluster.allowInsecureConnections: true` runs the cluster without mTLS. The chart refuses to render a scheduler or executor with neither. An executor serves only `/health`, so its readiness probe checks `/health` in place of the default `/v1/ready`.

**Upgrading from a release before per-pod DNS:** issue the two certificates above first, then upgrade the executors, and the schedulers last. Both releases move from a Deployment to a StatefulSet, so their pods are replaced; with more than one scheduler, failover keeps the cluster available while they roll.

Probe values use the standard Kubernetes probe configuration shape. They can be omitted entirely to use the built-in defaults, or partially overridden while omitted fields keep the defaults from `values.yaml`. If `exec`, `tcpSocket`, or `grpc` is configured, the chart omits the default `httpGet` handler from the rendered probe.

### ServiceAccount Annotations for Cloud IAM

Use `serviceAccount.annotations` to bind cloud IAM roles to Spice.ai pods, granting access to cloud resources (S3, DynamoDB, GCS, etc.) without static credentials. This works with any Kubernetes distribution that supports annotation-based identity federation.

#### AWS IRSA (EKS)

[IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html) works on any EKS cluster (including those running on EC2, Fargate, or Outposts) with an OIDC provider.

```yaml
serviceAccount:
  create: true
  name: spiceai
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/my-spice-role
```

#### AWS EKS Pod Identity

[EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html) is a newer alternative to IRSA. It uses the EKS Pod Identity Agent add-on instead of OIDC, so no ServiceAccount annotation is required — the association is configured via the EKS API. You still need a named ServiceAccount; either set `serviceAccount.create: true` to have the chart create one, or set `serviceAccount.name` to reference a pre-existing ServiceAccount.

#### GKE Workload Identity

```yaml
serviceAccount:
  create: true
  name: spiceai
  annotations:
    iam.gke.io/gcp-service-account: my-gcp-sa@my-project.iam.gserviceaccount.com
```

#### Azure Workload Identity (AKS)

```yaml
serviceAccount:
  create: true
  name: spiceai
  annotations:
    azure.workload.identity/client-id: <AZURE_CLIENT_ID>
```

A complete AWS IRSA example is available at [deploy/chart/examples/aws-irsa-values.yaml](examples/aws-irsa-values.yaml).
