# 36. Capstone: operating Northstar as an Enterprise service

The Enterprise capstone preserves the two application contracts from Chapters 25 and 26 while moving their runtime into a governed deployment. The sales service still returns tenant-specific paid totals. The support service still returns evidence with stable source identities. Enterprise deployment is successful when those contracts remain understandable, correct, and recoverable under the new identity and lifecycle model.

This is an integration capstone. The supplied templates have offline schema evidence, and the underlying local business checks have real runtime evidence. The book does not present an unexecuted Kubernetes deployment as a completed production launch.

## 36.1 Write the deployment contract first

Northstar's minimum Enterprise contract has four parts. The data contract defines the five source relations, schema, keys, timestamps, and metric definitions. The identity contract maps users to tenants and roles. The service contract defines bounded SQL and search operations. The recovery contract defines which data version can be served after each supported failure.

Choose one topology for the first deployment. A SpicepodSet is sufficient for learning image access, runtime authentication, workload identity, and policy. A SpicepodCluster adds partition assignment, shared state, executor storage, and distributed recovery. Keep the second as a separate release candidate until the first has an accepted business baseline.

Use a dedicated namespace and source dataset. The fictional fixture is suitable because its complete key set and amounts are known. Do not begin an authorization experiment on a production dataset with unknown exposure. Preserve the same adversarial NULL, zero-value, duplicate-title, and cross-tenant conditions used in the local labs.

## 36.2 Move the fixture to a reachable source

The local `file://data/...` paths refer to files on the authoring host. Kubernetes pods do not automatically inherit that directory. Choose an explicit delivery mechanism: an application image containing fixture files, a supported mounted volume arrangement, or a separately provisioned object-store dataset reachable by the workload identity.

For an object-store lab, publish the five CSV files under a versioned fixture prefix and record their hashes. Replace each source locator while preserving dataset names and business views. Keep the schema interpretation explicit, including quoted connector parameters. Verify the exact files returned by the source path before enabling acceleration.

Do not use a mutable “latest fixture” prefix for a reproducibility claim. A test result without a data identity can silently change when another engineer replaces a file. Record the fixture version alongside the Spicepod and runtime image.

If you choose distributed acceleration, declare the supported partition keys and accelerator configuration for each accelerated component. Run the correctness suite across the whole logical table and capture the per-executor assignment evidence. The Cayenne discrepancy recorded in Appendix A remains an unresolved acceptance item for the tested builds; a production promotion needs an accepted result on the selected release, not an assumption that Enterprise changes SQL semantics.

## 36.3 Deploy identity before exposing product traffic

Provision registry access and workload secrets through the approved platform workflow. Render the set or cluster manifest using the companion renderer. Check the target context, then use server-side dry-run and the installed admission path before applying it to the integration namespace.

First test runtime API-key authentication with a harmless query. Then configure the OIDC issuer and run the identity query from Chapter 31. Preserve the user ID, tenant ID, and role outcome for the fictional principals, with tokens excluded from artifacts. Only after identity mapping is correct should you enable the reviewed default-deny policy bundle.

Keep a controlled administrative path available for diagnosing a denied deployment, but do not give the application the administrator's credential. The application should use the identity and permissions designed for its product operations. A support assistant must not inherit broad database access merely because it shares a runtime with an analytics service.

## 36.4 Re-run the business contract through the deployed route

Use the Service or ingress path that the application will actually call. Run the paid-order query for each tenant, including expected counts and amounts. Inspect NULL behavior, join grain, and the anti-join checks. Save the returned rows and the actual query plan where execution placement matters.

Next, run the two shipping questions through the application service with the corresponding tenant identities. The northern evidence must identify article 3, and the southern evidence must identify article 6 for the lexical fixture pipeline. If you change the pipeline to semantic or hybrid retrieval, update the judged candidate contract deliberately while preserving tenant eligibility and source identity.

Test a user without the required role, a request for an unapproved dataset, a cross-tenant lookup, and a missing identity claim. A denied request must not become a successful empty report. An unavailable search service must not become an answer generated without evidence.

The direct Spice API and the product API serve different purposes. You may use direct SQL for operational verification while keeping the user-facing service bounded. Record which credentials can reach each route and keep their tests separate.

## 36.5 Observe the controller and the runtime separately

The operator reports reconciliation, Kubernetes API interactions, resource status, and certificate lifecycle. The runtime reports query execution, dataset loading or refresh, accelerator behavior, model operations, and application readiness. The platform also needs storage, network, source, and identity-provider observations.

The supplied operator supports Prometheus scraping and optional OTLP export. A small chart-values fragment for an existing collector is:

```yaml
telemetry:
  otlp:
    enabled: true
    endpoint: otel-collector.observability:4317
```

This configures the operator's telemetry. It does not automatically configure every workload's runtime telemetry. Verify the rendered environment variables and the collector's received resource identity. Keep scheduler, executor, application, and operator streams distinguishable in dashboards.

Changing a metric prefix can rename series used by existing alerts. Aggregation temporality also matters for interpreting counters. Treat telemetry changes as a release with a validation query in the backend. A rendered exporter setting is not proof that the collector received or correctly interpreted a metric.

For Northstar, correlate an application request ID with the runtime operation and the deployment version. During a rollout, add the active generation and pod identity. During a snapshot bootstrap, add the selected data version. This lets an operator distinguish a wrong policy, stale data, failed source, and incomplete rollout without guessing from a single error rate.

## 36.6 Perform one controlled failure at a time

Start with an application pod restart while its runtime remains available. Then restart one runtime replica and inspect readiness and state recovery. In a cluster, rehearse the scheduler and executor scenarios from Chapter 33. Add snapshot bootstrap and source catch-up only after normal operation has a stable baseline.

For each experiment, define the expected application response during the failure. Some requests may retry within a bounded deadline; others may return a clear unavailable status. Record incorrect successes separately from errors. A successful HTTP status carrying an incomplete total is more serious than an explicit refusal to answer.

Measure recovery from the user's perspective. The controller may finish reconciliation before every dataset meets its freshness contract. A pod may be Ready while an optional model is still unavailable. Tie the launch gate to the actual operations Northstar exposes, rather than selecting whichever infrastructure timestamp is earliest.

## 36.7 Promote the exact accepted artifacts

Promotion should identify the image digest, chart and values, CRD version, rendered custom resources, Spicepod, policy bundle, source fixture or dataset version, model artifacts, and acceptance record. Apply the same accepted artifacts to the next environment with only the reviewed environment-specific identities and addresses changed.

Separate infrastructure changes from business-definition changes where possible. If you change tenant mapping, query semantics, accelerator, and topology in one release, a changed total becomes harder to locate. The release record should explain every intentional difference and preserve the baseline needed to diagnose an unintentional one.

Rollback must be concrete. Name the prior configuration and image, the available standby or restore route, and the state compatibility assumptions already tested. A Git revert can restore declarative input; it cannot recreate a deleted source log or reverse an external side effect. Keep those responsibilities visible in the runbook.

## 36.8 Finish with a handover another engineer can run

The handover includes the bounded product operations, ownership table, identity and policy matrix, data invariants, deployment templates, monitoring links, and recovery procedure. Include the negative results and unresolved acceptance items. Do not replace them with a blanket “all checks passed.”

The engineer receiving the service should be able to reproduce a northern sales answer, explain why a southern policy is excluded, locate the deployed data version, and restore the contract after a rehearsed failure. That is the practical result of bringing SQL, search, inference, and Enterprise operations into one application design.

**Final exercise.** Use Appendix G's workbook to conduct a launch review. Have a second engineer follow the evidence from authenticated request to query or search result and then through the chosen recovery route. Resolve any missing artifact before promoting the deployment.

**Further reading.** The supplied `enterprise/production/` guides, operator `docs/metrics-otlp.md`, and runtime metrics documentation provide the operating references. The companion Enterprise directory and Appendix A state exactly which checks were executed for this edition.
