# Spice.ai Helm Chart

Deploy the Spice.ai runtime on Kubernetes.

## Install

```bash
helm install spiceai deploy/chart \
  --set spicepod.name=my-app
```

## Configuration

### Values

| Parameter                       | Description                                                                  | Default                           |
| ------------------------------- | ---------------------------------------------------------------------------- | --------------------------------- |
| `image.repository`              | Container image repository                                                   | `ghcr.io/spiceai/spiceai-nightly` |
| `image.tag`                     | Container image tag                                                          | `latest-models`                   |
| `replicaCount`                  | Number of replicas                                                           | `1`                               |
| `serviceAccount.create`         | Create a ServiceAccount                                                      | `false`                           |
| `serviceAccount.name`           | ServiceAccount name (auto-generated if empty and `create` is true)           | `null`                            |
| `serviceAccount.annotations`    | Annotations for the ServiceAccount                                           | `{}`                              |
| `service.type`                  | Kubernetes Service type (`ClusterIP`, `NodePort`, `LoadBalancer`, or `null`) | `null`                            |
| `service.additionalAnnotations` | Additional annotations on the Service                                        | `{}`                              |
| `livenessProbe`                 | Kubernetes liveness probe configuration                                      | see `values.yaml`                 |
| `readinessProbe`                | Kubernetes readiness probe configuration                                     | see `values.yaml`                 |
| `startupProbe`                  | Kubernetes startup probe configuration                                       | see `values.yaml`                 |
| `stateful.enabled`              | Use a StatefulSet with PVC                                                   | `false`                           |
| `stateful.storageClass`         | StorageClass for StatefulSet PVC                                             | `standard`                        |
| `stateful.size`                 | PVC size                                                                     | `1Gi`                             |
| `stateful.mountPath`            | Mount path in container                                                      | `/data`                           |
| `monitoring.podMonitor.enabled` | Create a PodMonitor for Prometheus                                           | `false`                           |
| `additionalLabels`              | Labels added to all resources                                                | `{}`                              |
| `additionalEnv`                 | Extra environment variables                                                  | `[]`                              |
| `resources`                     | CPU/memory requests and limits                                               | `{}`                              |
| `volumes`                       | Additional volumes                                                           | `[]`                              |
| `volumeMounts`                  | Additional volume mounts                                                     | `[]`                              |
| `spicepod`                      | Spicepod configuration (inlined into a ConfigMap)                            | see `values.yaml`                 |

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
