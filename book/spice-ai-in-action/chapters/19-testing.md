# 19. Testing data applications and AI behavior

Northstar now has SQL, acceleration, retrieval, and a model-facing workflow. Each layer can fail while adjacent layers appear healthy. A test strategy should preserve the contracts between them and produce artifacts that explain a failure rather than merely a red status.

## 19.1 Organize tests by the claim they support

A unit test is useful for a pure calculation, a request validator, or error formatting. An integration test establishes that the real connector, wrapper, runtime, and transport work together. A workload run measures performance under stated conditions. A retrieval evaluation judges ranking against relevant documents. An answer evaluation checks support, citations, and task behavior.

These tests are complementary. A mocked connector cannot prove that a real source's NULLs and timestamp types survive ingestion. A passing end-to-end answer cannot prove that the assistant always enforces tenant scope. A benchmark without result validation can reward an incorrect query.

| Claim | Required observation |
|---|---|
| The query computes net sales correctly | Returned rows reconciled with a known fixture |
| A filter executes before a large transfer | Plan plus observed rows or bytes |
| CDC recovers after interruption | Source event history, restart state, final keys and values |
| Retrieval finds the right policy | Ranked IDs compared with relevance judgments |
| The answer is grounded | Claims mapped to retrieved supporting passages |
| A deployment meets a latency objective | Workload metrics from the stated rig and concurrency |

![Figure 19.1. Acceptance evidence connects business and deployment contracts.](figures/testing.png)

## 19.2 Keep a small immutable fixture

Northstar's fixture is intentionally small enough to inspect by hand. Version it with the SQL and store a digest in the test record. Add new cases when a business contract grows, but keep the original invariants easy to derive.

Do not let test setup fetch a changing public dataset when exact expected results matter. A public sample is useful for exploration; a test needs a fixed artifact or a recorded snapshot identity. If the fixture generator is randomized, record the seed and verify generated invariants before using the data as an oracle.

Use several complementary checks: row count, key set, selected values, and aggregates. A sum alone can hide offsetting errors. A count alone can hide wrong identities. A hash alone is hard to diagnose unless the compared rows are also retained or recoverable.

## 19.3 Compare equivalent execution paths

The companion verifier submits the same SQL to federated and accelerated variants. This is a differential test of observable behavior. It is particularly valuable where execution moves between DataFusion, a remote system, and an embedded accelerator.

Do not assume the majority result is necessarily correct. Establish the expected business or SQL semantics independently. When a variant differs, retain the actual query and rows and investigate the difference. The book's Cayenne acceptance record does exactly that for a NULL-sensitive expression; it does not turn a partially successful run into a clean bill of health.

For numeric comparisons, use exact equality when the contract is integer or fixed decimal. For floating-point computations, choose a justified tolerance and handle NaN, infinities, and ordering explicitly. A broad tolerance can hide a real regression.

## 19.4 Test lifecycle, not just startup

A production test suite needs initial load, source mutation, refresh, restart, credential rotation, and recovery from a bounded outage. Add schema changes where the application expects them. For every test, specify the acceptable intermediate states as well as the final state.

A CDC test should identify each mutation and poll for the expected value under a deadline. If it times out, save the last observed rows, connector state, runtime log, and source progress. Fixed sleeps create tests whose success depends on machine timing instead of the condition under test.

Use disposable source namespaces and storage paths. The fixture's table names are not a reason to delete similarly named production resources. Cleanup should target resources created by that test run and record their identities.

## 19.5 Evaluate retrieval separately

A retrieval case contains a question, tenant scope, eligible corpus version, relevant IDs, and optional graded relevance. Recall at K asks whether relevant items entered the candidate set. Reciprocal rank emphasizes how early the first relevant item appears. A ranking metric cannot replace a hard isolation check.

Keep categories: exact identifiers, paraphrases, conflicting policies, no-answer cases, recently updated documents, and deleted documents. Report category results alongside an aggregate. A high average can conceal failure on the class that matters most to the product.

Avoid evaluating only questions written after seeing the current results. That can overfit the test set to the implementation. Hold out a set reviewed independently from model and prompt tuning, and periodically add real anonymized failure cases where the data policy permits them.

## 19.6 Evaluate generated answers

An answer evaluation should inspect the evidence and the trace. Check whether each factual claim is supported, whether citations resolve to supplied evidence, whether amounts and dates match deterministic results, and whether the model abstains when the source is insufficient.

A rubric can use explicit categories rather than a single “quality” score. For example: incorrect tenant is an automatic failure; unsupported policy claim is a factual failure; a missing citation is an attribution failure; excessive verbosity is a presentation issue. These should not cancel each other out in an average.

Record the generator model, prompt version, tool definitions, retrieval settings, and corpus version. An answer that changes after a model migration is not reproducible from the user question alone.

## 19.7 Build a release gate

Northstar's release gate can start with schema validation, fixture SQL, application response tests, and local retrieval checks. Connector or acceleration changes add the relevant source and restart tests. Model changes add answer evaluations. Deployment changes add readiness, authentication, and resource-bound checks.

Keep expensive checks scoped to the risk of the change, but do not substitute a cheap unrelated test. A style-only manuscript correction needs no database benchmark. A replication recovery change needs more than a parser unit test.

**Exercise.** Write a test manifest for an upgrade from one Spice binary to another. Identify which tests run against a copied persistent accelerator and which rebuild from a clean fixture. Define the artifact that would block release even if all performance measurements improve.

**Further reading.** See cookbook `evals/`, `crates/runtime/tests`, `test/spicepods`, and the repository's `testoperator` tooling. The book's verifier and evidence register provide a small example of artifact-producing integration checks.
