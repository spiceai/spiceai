# Spice.ai Helm Chart

Deploy the Spice.ai runtime on Kubernetes.

## Install

```bash
helm install spiceai deploy/chart \
  --set spicepod.name=my-app
```

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
| `livenessProbe`                    | Kubernetes liveness probe configuration                                      | see `values.yaml`                 |
| `readinessProbe`                   | Kubernetes readiness probe configuration                                     | see `values.yaml`                 |
| `startupProbe`                     | Kubernetes startup probe configuration                                       | see `values.yaml`                 |
| `stateful.enabled`                 | Use a StatefulSet with PVC                                                   | `false`                           |
| `stateful.storageClass`            | StorageClass for StatefulSet PVC                                             | `standard`                        |
| `stateful.size`                    | PVC size                                                                     | `1Gi`                             |
| `stateful.mountPath`               | Mount path in container                                                      | `/data`                           |
| `monitoring.podMonitor.enabled`    | Create a PodMonitor for Prometheus                                           | `false`                           |
| `additionalLabels`                 | Labels added to all resources                                                | `{}`                              |
| `additionalEnv`                    | Extra environment variables                                                  | `[]`                              |
| `resources`                        | CPU/memory requests and limits                                               | `{}`                              |
| `volumes`                          | Additional volumes                                                           | `[]`                              |
| `volumeMounts`                     | Additional volume mounts                                                     | `[]`                              |
| `spicepod`                         | Spicepod configuration (inlined into a ConfigMap)                            | see `values.yaml`                 |

### Roles

`role` selects how the runtime runs, as `spiced --role` does. Omitted, it is `server`, which serves queries over its own datasets.

`role: sidecar` serves an application's queries beside it, answering from its results cache and accelerations and sending reads from a remote Spice runtime as Substrait plans created locally. See [Application sidecars](../../docs/features/cluster-sidecar.md). The chart runs the sidecar as its own workload; to share a pod with your application, add a `spiced --role sidecar` container to the application's pod spec.

A distributed cluster is two releases: one with `role: scheduler` and one with `role: executor` whose `cluster.schedulerAddress` points at the scheduler's Service. Both expose the cluster port, `50052` by default, and advertise their pod IP unless `cluster.advertiseAddress` is set. The scheduler's `spicepod` needs `runtime.state.location`:

```bash
helm install spice-scheduler deploy/chart \
  --set role=scheduler \
  --set cluster.tls.secretName=spice-cluster-tls \
  --set spicepod.runtime.state.location=s3://my-bucket/spice-state

helm install spice-executor deploy/chart \
  --set role=executor \
  --set replicaCount=3 \
  --set cluster.schedulerAddress=https://spice-scheduler-spiceai:50052 \
  --set cluster.tls.secretName=spice-cluster-tls
```

Cluster nodes authenticate each other with mTLS. The Secret holds `ca.crt`, `tls.crt`, and `tls.key`, the layout cert-manager writes, and each certificate must name the address its node is reached at: the scheduler Service host for the scheduler, and each node's advertise address. For development and testing on a trusted network, `cluster.allowInsecureConnections: true` runs the cluster without mTLS. The chart refuses to render a scheduler or executor with neither. An executor serves only `/health`, so its readiness probe checks `/health` in place of the default `/v1/ready`.

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
