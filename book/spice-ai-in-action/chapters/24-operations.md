# 24. Recovery, upgrades, and extension boundaries

A system is not operationally complete until someone can recover it without guessing which state matters. Northstar's runbooks should name the source of truth, the persistent artifacts, the last accepted data contract, and the procedure for restoring service. This chapter also explains when an extension belongs in application code and when it needs a runtime contribution.

## 24.1 Classify state before backing it up

The Spicepod and referenced SQL are configuration state. Accelerator files and metadata are serving state. Replication positions and checkpoints are recovery state. Models and indexes are derived artifacts with potentially expensive rebuilds. Secrets and certificates are identity state. Query results and logs have their own retention and access policies.

For each item, decide whether it is authoritative, reproducible, or disposable. A local accelerator may be reproducible from the source, but only if the source still contains the required rows or history. A model index is reproducible only if the corpus and model artifact are preserved.

A backup is useful when its restore procedure has been tested. “The volume is snapshotted” does not establish that a live embedded engine, its metadata, and its source checkpoint form a consistent recoverable set. Use the engine's supported procedure and test it on an isolated restore target.

## 24.2 A recovery sequence

First, identify the incident boundary: process failure, node failure, storage loss, source outage, expired history, or schema incompatibility. Preserve logs and state before making destructive changes. Record the binary and configuration actually running.

Second, choose the supported recovery path. A process restart with intact storage differs from a full rebuild. A CDC source with expired history may require resnapshotting. A schema migration may require a new representation rather than reopening old storage.

Third, validate the restored service with key sets, representative values, freshness checks, and the application query contract. Readiness is necessary but does not prove that the recovered historical state is complete.

Finally, restore traffic gradually where the deployment supports it and retain the evidence. Do not discard the prior state until the recovery is accepted and the retention policy permits cleanup.

## 24.3 Upgrade in a copy before upgrading in place

Take a representative copy of configuration and persistent state using the supported backup method. Run the target binary against it in an isolated environment. Execute the SQL contract, source mutation tests, search update/deletion checks, and restart procedure relevant to the deployment.

Determine whether the new version migrates state and whether the previous version can read it. If rollback requires a restore or rebuild, state that in the release plan. A rollback that only changes the image tag can fail when persistent formats have already changed.

Model and embedding upgrades need their own versioning. A new generator can be switched behind an alias after evaluation, while a new embedding space generally requires a new index generation. Keep old and new artifacts isolated during comparison.

## 24.4 Troubleshoot in the order of dependency

For an unavailable table, check the intended configuration, secret resolution, network and source authentication, source permissions, schema interpretation, accelerator initialization, and readiness. Use the named dataset in logs to follow the failure. Do not respond to every startup problem by deleting storage.

For stale results, identify which source version is expected, whether the connector has observed it, whether it is applied and visible, and whether a cache is serving an older response. Bypass the relevant cache through the documented mechanism for diagnosis. Keep source and runtime observations together.

For search failures, inspect corpus membership, index readiness, candidate retrieval, filters, ranking, and generation in that order. A plausible final answer does not prove retrieval worked.

## 24.5 When to extend outside the runtime

If the need is a business-specific report, a tenant-aware tool, or an application response shape, implement it at the application boundary using fixed SQL and supported APIs. This keeps business policy close to the product and avoids introducing a new engine extension unnecessarily.

If the need is a reusable source protocol, a new storage capability, or a model integration that belongs across applications, a runtime extension may be appropriate. Begin with the repository's extension interfaces and crate layering. Do not pull high-level orchestration into a low-level utility crate merely to reuse one type.

A custom connector must do more than return rows. It needs a schema contract, error behavior, credential handling, connection management, cancellation, pushdown semantics, and lifecycle tests. Its user-facing errors should name the dataset and explain what the user can do.

## 24.6 Wrapper delegation is part of correctness

The runtime often wraps providers to add acceleration, federation, search, or deferred behavior. A new trait method with a default implementation can compile while a wrapper silently inherits the default instead of forwarding a meaningful inner implementation.

When changing a trait, find every wrapper and deliberately forward the method or document why it is not forwarded. This is especially important for capabilities, object-store registration, statistics, and methods used by distributed execution. Prefer an interface that makes missing implementation visible to the compiler where practical.

An integration test should exercise the wrapped path. A direct unit test of the connector can pass while the application path fails because a wrapper never called it. For distributed sources, the test must reach executor-side access, not only local planning.

## 24.7 Statistics and errors are public contracts

A provider's statistics can influence optimization and, when marked exact, potentially the result of a query. Preserve exactness only when justified by the state actually visible to the scan. Mutable overlays and independent source changes can make a previously exact fact inexact.

Error propagation is equally important. A connector that silently skips a failed partition may return a plausible partial result. Return a structured error when completeness cannot be established. A user-facing warning on a degrade-and-continue path must explain the observable consequence.

## 24.8 Contribute with evidence

For a suspected runtime defect, read the relevant path and reproduce it through a real runtime, integration test, or targeted external experiment. Preserve the command, actual output, and configuration. Label unreproduced concerns as hypotheses.

A regression test should fail before the change and pass after it, but a unit test alone is not sufficient evidence for a data-loss or correctness claim. The artifact should match the claim: wrong rows for wrong results, a plan for pushdown, a profile for memory, and a backtrace for a crash.

Scope builds and tests to the affected crates and keep feature flags consistent. Follow repository review and signoff requirements when contributing code. The book's companion artifacts are application examples; they do not modify the runtime or claim to repair the Cayenne discrepancy recorded during authoring.

**Exercise.** Write a restore runbook for a file-backed CDC dataset whose node disk has been lost but whose source is healthy. Identify which state is unavailable, whether the source retains enough history, and which acceptance queries establish a complete rebuilt table.

**Further reading.** See `docs/EXTENSIBILITY.md`, `docs/dev/crate_layering.md`, `docs/dev/error_handling.md`, and the source's agent instructions for contribution standards. Deployment-specific backup and recovery procedures must match the selected engine and release.
