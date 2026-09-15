# 35. Enterprise functions and distributed model inference

An enterprise application often needs domain logic that is more specific than a built-in SQL function and model serving that is more demanding than a remote API alias. Spice's function interfaces and distributed inference facilities extend those boundaries. They also introduce executable code, network calls, model artifacts, and additional failure modes into the query path.

The supplied documentation marks user-defined functions as alpha. Inline SQL functions are available beyond Enterprise; the HTTP and WebAssembly tiers are supplied in the documented Enterprise and Cloud distributions and depend on the selected build elsewhere. Multi-node inference is a separate Enterprise build capability. Keep those distinctions when selecting an image.

## 35.1 Start with a typed SQL function

The companion contains a small function lab that can be run independently of Kubernetes and Enterprise authorization. It enables the function subsystem, declares a scalar signature, and keeps the function out of the model tool registry:

```yaml
version: v1
kind: Spicepod
name: northstar-functions
runtime:
  functions:
    enabled: true
functions:
  - name: net_cents
    from: sql
    description: Subtract a reconciled refund from gross cents.
    volatility: immutable
    as_tool: false
    signature:
      args:
        - { name: gross, type: int64 }
        - { name: refund, type: int64 }
      returns: int64
    body: gross - refund
```

Run the configuration from `companion/enterprise/` with a compatible runtime, then query it through the usual HTTP SQL interface:

```sql
SELECT net_cents(22200, 11200) AS net_cents;
SELECT net_cents(CAST(NULL AS BIGINT), 0) AS net_cents;
SELECT net_cents(0, 0) AS net_cents;
```

The authoring development binary returned `11000`, `NULL`, and `0`, respectively. These are actual local results recorded in `evidence/enterprise/functions-sql.json`; they are not an Enterprise-cluster execution claim. The function performs arithmetic on already reconciled amounts. It does not discover returns, decide a refund policy, or make an invalid join correct.

Keep integer bounds in the contract. The three examples do not establish overflow handling for every possible pair of `int64` inputs. Add boundary cases appropriate to the domain and inspect the actual error or value before using a function for amounts that approach the type's limits.

## 35.2 Treat volatility as a semantic promise

An immutable function promises the same result for the same inputs. A stable function has query-scoped stability. A volatile function can change between calls. These declarations affect how a query engine may simplify, reuse, or schedule expressions.

Do not mark a network lookup immutable merely because the current implementation usually returns the same value. The remote service may change its rules, data, identity context, or model. Likewise, a function that reads current time or request identity has a different contract from a pure arithmetic expression.

Use a clear name and version for business semantics. If Northstar changes how refunds are recognized, it may need a different function or metric definition rather than silently changing a function's body under an existing report. Preserve function declarations with the application release and inspect registration through the supported function introspection interfaces.

## 35.3 Understand the HTTP batch protocol

The remote function tier invokes an HTTP endpoint with a typed contract. For a scalar function without table arguments, the documented shape is a batch of argument objects:

```json
{"rows":[{"code":"BOOT"},{"code":"LIGHT"}]}
```

The corresponding response has one value for each input row in the same order:

```json
{"values":["Footwear","Lighting"]}
```

The endpoint must implement that protocol; an arbitrary JSON API is not automatically a Spice function. Scalar functions with table inputs and table functions use different request/response shapes. Choose the correct signature and test the complete call path before embedding the function in a report.

Batch size, concurrent batches, timeout, and maximum response size are operational controls. A query over a large relation can create substantial work outside Spice. Filtering the input relation, bounding the operation, and measuring remote call counts are as important as tuning the endpoint itself.

A retry can repeat an external side effect. The documented remote tier does not supply a universal transactional retry or exactly-once contract. Prefer read-oriented enrichment for a SQL function. Route business mutations through an explicit application operation with an idempotency and transaction design.

## 35.4 Make the remote endpoint a deliberate trust boundary

Remote function configuration includes bearer authentication and an endpoint-range allowlist for explicitly permitted internal ranges. Keep credentials in the configured secret mechanism. The function's network identity is a deployment concern; the requesting user's identity is a separate authorization concern.

Specify exactly which columns and values can leave the runtime. Apply policy and minimization before the remote call, and verify the actual payload in an isolated acceptance environment. A remote model or enrichment service cannot unreceive a sensitive value after the final result is masked.

The function implementation belongs to the query's dependency graph. An unavailable endpoint should surface as a failed operation, not a fabricated enrichment value. Test a non-success response, malformed JSON, a value-count mismatch, NULL input, and a response exceeding the configured size. Preserve the status and error details without exposing credentials or unnecessary row content in user-facing messages.

## 35.5 Use WebAssembly as a typed execution boundary

The documented WASM tier exchanges Arrow IPC data with a sandboxed module and supports configured fuel and memory limits. A module path, entrypoint, signature, and resource budget form part of the deployed artifact. The module must implement Spice's host/guest ABI; an arbitrary `.wasm` file is not sufficient.

Keep the compiled module, source revision, compiler/toolchain record, and content hash with the release. Validate the input and output Arrow schemas. For a scalar transformation, preserve row alignment. For a table function, define how output cardinality relates to its table and scalar arguments.

Fuel and linear-memory limits bound parts of guest execution. They do not make a poor algorithm efficient or guarantee that host-side materialization is small. Test representative batches and deliberately exhausted budgets. An interrupted module should produce an observable failure rather than partially accepted business output.

SQL, HTTP, and WASM are execution choices for a function contract. Choose the simplest tier that satisfies that contract. A pure expression does not need a network hop; a centrally governed enrichment service may justify one; a portable specialized transformation may justify a compiled module.

## 35.6 Control the bridge between SQL and model tools

Scalar functions can be exposed as model tools, while table functions remain a distinct SQL surface in the documented interface. The inverse direction lets a configured tool become a SQL-callable operation when it has an explicit supported signature. The two directions must be reviewed separately.

Northstar's arithmetic example sets `as_tool: false` to keep its initial surface small. If you later expose an order lookup or policy calculation to a model, define the authenticated scope, argument types, returned fields, timeout, and error behavior. A generated argument must not be allowed to select arbitrary SQL structure or an unrelated dataset.

Distributed query adds another requirement: executors must have the same function definitions and required artifacts as the planner expects. The documented bootstrap carries application definitions, but a local module path still needs the corresponding file in the execution environment. A successful scheduler-side registration does not prove every executor can load the module or reach the endpoint.

## 35.7 Distinguish distributed inference from distributed SQL

Distributed query divides query work across executors. Tensor-parallel inference divides one model's computation across participating nodes. These are different topologies. Increasing `executorSpec.replicas` does not automatically shard a language model.

The supplied Enterprise inference reference describes a ring backend configured through model parameters. Its documented example uses a rank-ordered node list, an identical model on each node, and a distinct `node_rank`. Rank zero is the head serving the API. The documented ring support is limited to two nodes; the world size must also satisfy model head-count constraints.

```yaml
# Integration fragment; requires the supported distributed build
models:
  - name: enterprise_chat
    from: huggingface:huggingface.co/THUDM/glm-4-9b-chat
    params:
      model_type: glm4
      distributed_backend: ring
      nodes: "10.0.4.21,10.0.4.22"
      node_rank: "0"
```

This is a version-sensitive configuration example, not a model recommendation or an executed lab. Replace the addresses with a reviewed network topology and use a model artifact whose license, revision, tokenizer, memory needs, and supported implementation have been checked. The second node uses its own rank while preserving the same ordered node list.

Model fit is only one acceptance criterion. Measure time to load, time to first token, decode behavior, network transfer, and output quality on the intended hardware. Test participant failure and head-node restart. Tensor parallelism is not itself a high-availability mechanism: a model computation that depends on both participants may fail when either disappears.

**Exercise.** Classify three Northstar operations: arithmetic on already reconciled cents, a centrally managed product-category lookup, and a large local model. Choose an execution boundary for each and write the evidence needed to justify it. Include NULLs, unavailable dependencies, artifact versions, and which user data crosses each boundary.

**Further reading.** Use the supplied `enterprise/features/functions.md`, `enterprise/features/distributed-inference.md`, and distribution reference, together with function registration and distributed-model configuration source. The local arithmetic transcript demonstrates only the SQL tier on the recorded development binary.
