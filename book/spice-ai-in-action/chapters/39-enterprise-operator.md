# 32. Deploying Enterprise with the Kubernetes operator

A Kubernetes operator turns an application specification into a managed set of resources and continuously reconciles the difference between desired and observed state. The Spice operator does that for `SpicepodSet` and `SpicepodCluster`. The application team supplies a reviewed specification; the controller manages the resources described by its installed version.

This chapter uses the `spice.ai/v2` interface from the pinned operator checkout. The companion templates passed offline structural validation against its rendered CRDs. The Kubernetes commands are an integration lab: they require your own cluster, image entitlement, registry credentials, and deployment permissions.

![Figure 32.1. The operator reconciles reviewed specifications into managed Kubernetes resources.](figures/enterprise-operator.png)

## 32.1 Understand the two nested documents

A `SpicepodSet` is a Kubernetes resource. Its `spec.spicepod` is a Spicepod object interpreted by the runtime. The outer document uses Kubernetes names such as `apiVersion`, `metadata`, and camelCase operator fields. The inner document uses Spice's own component schema.

```yaml
apiVersion: spice.ai/v2
kind: SpicepodSet
metadata:
  name: northstar
  namespace: spice-book
spec:
  replicas: 2
  image:
    repository: REPLACE_ENTERPRISE_REPOSITORY
    tag: REPLACE_TESTED_RUNTIME_TAG
    pullSecret: northstar-registry
    pullPolicy: IfNotPresent
  updateStrategy:
    type: RollingOrdered
  spicepod:
    version: v1
    kind: Spicepod
    name: northstar-enterprise
```

The image values are explicit placeholders. The complete companion template adds resource settings and a runtime API key obtained from a Kubernetes Secret. The shortened listing shows the nesting without disguising it as a production configuration.

Do not paste a legacy `spicepod: |` string or snake_case image fields into this v2 example. The inspected v2 schema uses an object-valued Spicepod and an `image` object. Legacy CRD versions may be served through conversion webhooks, but that is a migration path, not permission to combine fields from different versions in one manifest.

## 32.2 Render and inspect the operator chart

Before installing, render the exact chart version with the exact values intended for the environment. The local authoring check used the supplied chart directory:

```bash
helm template spice-book ~/dev/spice-k8s-operator/deploy/chart \
  --namespace spice-system --kube-version 1.33.0 \
  > operator-rendered.yaml
helm lint ~/dev/spice-k8s-operator/deploy/chart \
  --kube-version 1.33.0
```

Observed lint output included `1 chart(s) linted, 0 chart(s) failed`. The render contained nine Kubernetes documents, including the two CRDs. This verifies chart rendering for those inputs. It does not contact an API server, pull images, or exercise admission.

The supplied chart has `crds.enabled` and `crds.keep` settings. Keeping CRDs on chart removal is an operational choice with consequences for the custom resources they describe. Read the rendered annotations and the upgrade guide before changing lifecycle behavior. Removing a CRD is not a routine way to restart an operator.

For an actual installation, obtain the supported chart through the distribution channel for your Enterprise arrangement, pin its version, and keep the rendered output. Do not infer runtime image compatibility from the chart's own version number. The operator process and the workloads it manages use separate image settings.

## 32.3 Supply prerequisites without embedding credentials

The companion set template references `northstar-registry` for private image pulls and `northstar-runtime` for its runtime API key. Provision these through your organization's secret workflow in the `spice-book` namespace. The book does not include real secrets or execute that provisioning.

The runtime environment entry uses Kubernetes `valueFrom.secretKeyRef`:

```yaml
spec:
  env:
    - name: BOOK_API_KEY
      valueFrom:
        secretKeyRef:
          name: northstar-runtime
          key: api-key
  spicepod:
    version: v1
    kind: Spicepod
    name: northstar-enterprise
    runtime:
      auth:
        api_key:
          enabled: true
          keys:
            - ${ env:BOOK_API_KEY }
```

This crosses two parsers: Kubernetes places the Secret value in the process environment, and Spice expands the reference. The `${ env:... }` expression is not a Helm template variable. Inspect each layer's final configuration without printing the secret value.

The companion renderer uses Python's JSON encoder rather than shell interpolation. Set `BOOK_ENTERPRISE_REPOSITORY` and `BOOK_ENTERPRISE_TAG` to the reviewed values, then run:

```bash
python3 enterprise/render.py spicepodset \
  --output enterprise/rendered/spicepodset.json
```

JSON is a supported Kubernetes manifest format. The renderer does not connect to a cluster or check that an image tag exists. Inspect the resulting file and associate it with the tested image digest in your release record.

## 32.4 Apply to the intended context and inspect reconciliation

In the integration environment, confirm your current Kubernetes context and namespace before applying the rendered resource. Use server-side validation to exercise the installed API schema and admission path:

```bash
kubectl config current-context
kubectl apply --dry-run=server \
  -f enterprise/rendered/spicepodset.json
kubectl apply -f enterprise/rendered/spicepodset.json
kubectl get spicepodset northstar -n spice-book -o yaml
kubectl get pods -n spice-book -l spice.ai/spicepod=northstar
kubectl get svc spicepod-northstar -n spice-book -o yaml
```

These commands have not been executed against a cluster for the book. Their acceptance criteria are a stored desired specification, reconciled workloads, ready replicas, and a Service selecting the intended pods. A successful `kubectl apply` reports API acceptance, not application readiness.

The inspected operator creates StatefulSets. For its simple single-replica, no-volume, non-cluster case, it can create a hash-suffixed StatefulSet for a new specification before removing the previous workload. Volume-backed, clustered, or multi-replica configurations use per-replica StatefulSets. Inspect ownership references and generated names rather than assuming a single Deployment named after the application.

## 32.5 Follow Service ports to container ports

The current operator's generated standalone Service exposes HTTP on port `8080`, targeting the configured runtime HTTP port, normally `8090`. Flight and metrics have their own mappings. This distinction matters when port-forwarding a Service instead of a pod.

After inspecting the actual Service, the integration query path is:

```bash
kubectl port-forward -n spice-book svc/spicepod-northstar \
  8090:8080
```

In another terminal, with the lab key set securely in your shell:

```bash
curl --fail-with-body -sS http://127.0.0.1:8090/v1/sql \
  -H "X-API-Key: $BOOK_API_KEY" \
  -H 'Content-Type: text/plain' \
  --data 'SELECT 1 AS ok'
```

Use the inspected Service's ports if your operator version differs. Do not use a remembered container port as proof of the Service contract. Likewise, use the SQL API's actual request format; a JSON property named by analogy is not interchangeable with the documented `sql` field.

## 32.6 Attach persistent storage deliberately

The v2 `volumeClaimTemplates` field is an array of Kubernetes-style PVC templates. A template named `data` is automatically mounted at `/data`; other templates require matching `volumeMounts` entries. The complete cluster template in the companion uses this shape:

```yaml
volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes:
        - ReadWriteOnce
      storageClassName: REPLACE_STORAGE_CLASS
      resources:
        requests:
          storage: 40Gi
```

Forty GiB is a lab allocation placeholder, not a capacity recommendation. Accelerator data, metadata, temporary query work, logs, snapshots in transit, and maintenance headroom all compete for storage. Size from the actual dataset and maintenance behavior. Keep the accelerator's configured file paths on the intended mount.

The operator supports expansion requests for volume templates, while actual expansion depends on the storage class and CSI driver. Shrinking storage is a different operation. Blue-green generations also have different storage lifecycles from an in-place rolling update; Chapter 34 examines that distinction.

## 32.7 Configure workload identity and network access

A workload ServiceAccount belongs under the resource's `serviceAccount` configuration. An existing account can be selected with `enabled: true`, `create: false`, and its name. This is separate from the Helm values controlling the operator's own ServiceAccount.

For EKS, the supplied references describe IRSA: a federated service-account identity assumes an IAM role with the required permissions. AKS and GKE have their own workload-identity setup. Verify the actual pod's identity and provider integration. A ServiceAccount annotation alone is not proof that a federated credential, admission mutation, or cloud role binding exists.

The operator's `network.ingress` and `network.egress` use Kubernetes NetworkPolicy rule shapes. Include DNS, source endpoints, identity discovery, model services, and cluster peers according to the chosen topology. Check the CNI's enforcement behavior. An admission warning or a syntactically valid policy does not establish successful data access.

## 32.8 Treat status as evidence, not decoration

Read desired replicas, ready replicas, conditions, observed generation, and pause information together. The controller can protect a workload from repeated crash loops by pausing it. Restoring replicas without understanding the recorded cause can repeat the failure. Likewise, a stale ready count needs its generation context before it can support a rollout decision.

The operator's monitoring and optional status API describe controller behavior. The runtime's health, readiness, SQL results, and metrics describe application behavior. Use both. Chapter 36 combines them into a deployment acceptance record.

**Exercise.** Draw the request path from a laptop port-forward to the Service, selected pod, runtime HTTP listener, authentication layer, and SQL result. Annotate every port and identity. Then explain how that path differs from an Arrow Flight client and from an executor's internal cluster connection.

**Further reading.** Consult the pinned operator `README.md`, `docs/user-guide.md`, `UPGRADING.md`, `deploy/chart/values.yaml`, and generated v2 CRDs. The supplied Cloud documentation's `enterprise/kubernetes/` pages provide the product-facing context; use the installed CRD as the field-level deployment contract.
