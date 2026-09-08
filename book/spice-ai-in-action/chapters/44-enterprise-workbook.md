# Appendix G. Enterprise deployment workbook

This workbook is a reader-owned integration record. Populate it from your deployment and attach actual artifacts. The authoring checks described below are limited to local runtime examples, chart rendering, lint, and offline CRD validation.

## G.1 Artifact inventory

| Artifact | Record for the deployment |
|---|---|
| Runtime | Distribution, architecture, image digest, reported runtime version |
| Operator | Chart version, chart digest or source commit, operator image, values hash |
| Kubernetes API | Cluster version, `spice.ai/v2` CRD schema and stored versions |
| Application | Spicepod hash, source locators, schema, query definitions |
| Policy | Provider, bundle revision, principal/action/resource acceptance matrix |
| Data | Fixture or source version, keys, row counts, monetary invariants |
| Models and functions | Artifact revisions, tokenizer, signatures, prompt and evaluation versions |
| State | PVC/storage class, object-store prefixes, snapshot and source replay contract |
| Recovery | Previous accepted release, restore procedure, observed recovery artifacts |

## G.2 Companion files

`enterprise/spicepodset.template.yaml` and `.json` describe a two-replica set with explicit image placeholders and a Secret-backed runtime API key. `enterprise/spicepodcluster.template.yaml` and `.json` describe separate scheduler and executor pools, shared state, workload identity, and executor PVCs. The `.json` files are the renderer's inputs; the YAML files are readable equivalents.

The templates reference an existing namespace `spice-book`, image-pull Secret `northstar-registry`, and runtime Secret `northstar-runtime` with key `api-key`. The cluster additionally references the existing ServiceAccount `northstar-data`, a reviewed storage class, and a provisioned shared-state bucket. Provision those prerequisites through the environment's normal platform workflow.

Set the non-secret deployment selectors in your shell:

```bash
export BOOK_ENTERPRISE_REPOSITORY='your-entitled-image-repository'
export BOOK_ENTERPRISE_TAG='your-tested-runtime-tag'
export BOOK_STATE_BUCKET='your-isolated-cluster-state-bucket'
export BOOK_STORAGE_CLASS='your-tested-storage-class'
```

The quoted values above are descriptive placeholders. Replace them with actual reviewed values. The renderer preserves Spice secret-reference syntax and uses JSON serialization, so shell metacharacters are not evaluated as part of manifest construction.

```bash
python3 enterprise/render.py spicepodset \
  --output enterprise/rendered/spicepodset.json
python3 enterprise/render.py spicepodcluster \
  --output enterprise/rendered/spicepodcluster.json
```

Choose one topology for an integration run. Applying both templates creates two independent workloads and is not a migration procedure by itself. The renderer writes files only; it performs no deployment or image-access check.

## G.3 Acceptance matrix

| Stage | Required observation |
|---|---|
| API admission | Server-side dry-run and apply results for the installed CRDs/webhooks |
| Reconciliation | Desired and observed generation, child resources, ready replicas, Service endpoints |
| Runtime identity | Reported binary version and resolved image digest |
| Authentication | Valid credential succeeds; invalid, expired, wrong-issuer and wrong-audience cases are handled as designed |
| Tenant mapping | Northern and southern identities resolve to their intended scope |
| Policy | Allowed operations succeed; forbidden datasets, tools, models and mutations are denied |
| SQL | Correct complete rows, key sets, counts and amounts; NULL and empty-set behavior |
| Search | Eligible candidate IDs, evidence text, source versions, and no wrong-tenant evidence |
| State | Snapshot/PVC identity, bootstrap selection, source catch-up and full reconciliation |
| Rollout | Active generation, request behavior, state compatibility and tested rollback route |
| Recovery | Actual outputs and timings for each failure in the stated recovery model |

The matrix records outcomes, including failures. It does not imply that every row was executed during authoring. The Enterprise deployment, OIDC, Cedar, cluster, snapshot, remote function, WASM, and multi-node inference procedures require the reader's environment.

## G.4 Authoring checks

The pinned operator chart rendered nine Kubernetes documents, including `SpicepodSet` and `SpicepodCluster` CRDs. Helm lint reported:

```text
1 chart(s) linted, 0 chart(s) failed
```

Both companion Kubernetes templates passed offline structural validation against the rendered v2 schemas. The embedded Spicepod is represented using a preserve-unknown-fields schema at that layer; this check does not validate its entire runtime semantics. Admission and the selected runtime remain additional checks.

The SQL-function lab returned the following actual values on the recorded development binary:

| Expression | Observed value |
|---|---|
| `net_cents(22200,11200)` | `11000` |
| `net_cents(CAST(NULL AS BIGINT),0)` | `NULL` |
| `net_cents(0,0)` | `0` |

The complete records are in `evidence/enterprise/`. The companion's base SQL, search, vector, and service records remain in the main evidence directory and `final-config/`.

## G.5 Operational handover questions

Which team can change policy, and which team can change workload identity? Which source owns the business truth? Which query defines each published metric? Which object-store prefix belongs to this cluster? Which snapshot can the current image load? What does a user see when that snapshot is old or unavailable? Which failure has actually been rehearsed, and where are its returned rows and recovery timings?

These questions are answered by the deployment record, not by the brand name of the distribution. Keep the record alongside the service so the next engineer can operate what you built.
