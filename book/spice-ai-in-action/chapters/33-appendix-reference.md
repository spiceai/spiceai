# Appendix D. Configuration and operations reference

This is a navigation aid, not a replacement for the versioned schema. The examples use settings verified against the inspected source or labeled integration patterns. Engine and connector support must be checked together.

## D.1 Configuration ownership

| Configuration area | Owns | Common review question |
|---|---|---|
| `datasets[].from` | Source locator | Is this the intended source and namespace? |
| `datasets[].name` | SQL-facing identity | Does changing it break callers? |
| `datasets[].params` | Connector behavior | Are credentials and source options correct? |
| `datasets[].acceleration` | Serving representation and refresh | What is stored, and how current is it? |
| `acceleration.params` | Engine-specific storage behavior | Does the selected engine recognize these options? |
| `views` | Named business queries | Are grain, types, and definitions stable? |
| `embeddings` | Text-to-vector components | Is the model space versioned? |
| `models` | Application-facing inference aliases | Is the provider capability tested? |
| `tools` | External or built-in operations | What can the model call? |
| `secrets` | Credential lookup | Which store supplies each value? |
| `runtime.auth` | Runtime caller authentication | Is every exposed protocol tested? |
| `runtime.caching` | Result and embedding reuse | What age and scope are acceptable? |
| `runtime.query` | Query execution settings | What does the limit actually account for? |
| `runtime.scheduler` | Cluster coordination settings | Is state shared and recoverable as required? |

## D.2 Refresh decision guide

Use full refresh when a complete source snapshot is affordable and periodic replacement meets the contract. Use append only when the source and extraction policy truly represent new records and handle overlap deliberately. Use changes when a supported change stream represents required inserts, updates, and deletes. Consider caching-mode acceleration only for a workload whose cache-miss and completeness behavior is understood in the selected release.

Snapshot-based startup is a lifecycle mechanism, not a generic replacement for every refresh mode. A snapshot can accelerate bootstrap while later refresh or replication maintains current state. The consumer must load a compatible, complete artifact and establish the correct continuation behavior.

## D.3 Listener checklist

Record the bind address, advertise address where applicable, network policy, authentication, TLS, and monitoring owner for HTTP, Flight, metrics, and internal cluster services. Check from the actual client network. A localhost test validates only that path.

Public-facing application clients should use the bounded application API. Internal analytical clients may use SQL interfaces under their own authorization and resource policies. Model tools need an explicitly selected subset of those capabilities.

## D.4 Query acceptance card

A query acceptance card contains the business question, input grain, output grain, key scope, allowed source versions, NULL policy, currency and timezone, expected fixture rows, production result bounds, and freshness requirements. Add a plan when execution placement matters.

For Northstar's gross-sales query, the expected rows are `north: (4, 22200)` and `south: (2, 24900)`. For net sales attributed to order, expected net values are 11,000 and 21,600 cents. A change that alters those values must explain why the business definition changed or be treated as a failed contract.

## D.5 Incident triage card

Begin with the request identifier, affected operation, tenant scope, observed status, last successful result, and current binary/configuration identities. Check whether the failure is authentication, source access, registration, refresh, query execution, retrieval, generation, or response delivery.

Preserve the first useful artifact before restarting or clearing state. For an unavailable dataset, keep its log and source error. For wrong results, keep SQL and rows. For a hang, obtain the appropriate stack evidence. For memory, keep a process trace or profile. For latency, keep workload and operator measurements.

Avoid changing several settings at once. A temporary workaround should have an owner, expiry condition, and verification that it preserves the data contract.

## D.6 CDC recovery card

Record source system and version, dataset identity, key, snapshot policy, accelerator mode, storage location, consumer progress identity, retention window, and last accepted reconciliation. State which failures require resume and which require a full resnapshot.

The runbook should answer what happens if the accelerator is empty but the source remembers a consumer position. It should also identify who can retire old slots, publications, topics, or checkpoints. Those resources may outlive a runtime process.

## D.7 Search acceptance card

Record corpus identity and version, eligible tenant scope, text columns, document and chunk IDs, embedding model identity, metric, index configuration, candidate depth, fusion/reranker settings, and the judgment set. Include update, deletion, no-answer, and cross-tenant cases.

Keep intermediate candidates and final evidence IDs for failures. A final answer with a valid-looking citation is not enough to locate the problem. The evidence must show whether the correct passage was available and used.

## D.8 Release manifest template

```yaml
# Application-owned release metadata; not a Spicepod schema
release_id: northstar-example
runtime:
  binary_version: record-the-actual-version
  image_digest: record-if-containerized
configuration:
  spicepod_sha256: record-digest
  sql_revision: record-revision
sources:
  schema_revision: record-schema
  snapshot_or_fixture: record-identity
models:
  generator: record-if-used
  embedding_artifact: record-digest-if-used
acceptance:
  sql_results: path-to-artifact
  service_results: path-to-artifact
  recovery_results: path-to-artifact-or-not-run
  workload_results: path-to-artifact-or-not-run
```

This manifest is deliberately separate from the runtime configuration. It records what was accepted, including checks not run. Replace placeholders with actual identities before using it as a release record.

## D.9 Production-readiness questions

Can the team identify every authoritative and derived copy? Can it explain the age of a response? Can it reproduce a metric from raw fixture rows? Can it deny a wrong-tenant request before revealing data? Can it restore serving state from the documented artifacts? Can it explain which workloads were measured and which remain untested?

These questions are practical acceptance criteria. A sophisticated topology that cannot answer them is harder to operate than a small deployment with clear contracts.
