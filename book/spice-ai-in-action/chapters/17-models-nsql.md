# 17. Model gateways and natural-language SQL

A model gateway gives an application a named place to call inference while centralizing provider configuration. Natural-language SQL adds another capability: translate a user's question into a query over the available schema. Both are useful when their boundaries remain explicit. Neither removes the need to validate what the application is asking the data system to do.

## 17.1 Name a model for the application

A Spice model component separates an application-facing name from a provider's model identifier. A representative provider-backed fragment is:

```yaml
# Integration fragment: requires a provider account and model access
models:
  - from: openai:gpt-4o-mini
    name: support_chat
    params:
      openai_api_key: ${ env:OPENAI_API_KEY }
```

The provider identifier is an example from the inspected recipe family, not a claim of permanent availability or a recommendation about current model pricing. Select an available model for the deployed provider and verify its capabilities and limits. Record the selection with the application release.

An application calls the configured Spice model name:

```bash
curl --fail-with-body -sS http://127.0.0.1:8090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  --data '{"model":"support_chat","messages":[{"role":"user","content":"Explain the supplied return policy."}]}'
```

This is a protocol example. The authoring run did not make paid generation requests, and no generated response is fabricated here. A production request also includes the runtime's configured authentication and the actual evidence messages from Chapter 16.

## 17.2 Compatibility is a surface, not universal equivalence

An OpenAI-compatible endpoint allows clients to use a familiar request shape. Provider capabilities can differ in tool calling, structured output, streaming, context limits, image input, and error behavior. A gateway may translate some fields and reject or constrain others.

Create a compatibility contract around the fields your application actually uses. Test a normal request, a streamed request if needed, a tool call, a timeout, an invalid model name, and the provider's rate-limit response. Do not assume that every option accepted by a client library is honored by every configured provider.

A model change can preserve the API shape while altering instruction following or retrieval use. Treat model migration as an application behavior change with evaluation and rollback. A local alias simplifies routing; it does not certify behavioral interchangeability.

## 17.3 Local inference changes ownership

Locally served models can keep inference traffic within the chosen environment and avoid dependence on a hosted provider for each request. They introduce model distribution, hardware capacity, memory, scheduling, and update responsibilities.

Check the build's local-inference support, model format, hardware backend, and available memory. Loading a model successfully is only the first acceptance check. Test concurrent inference alongside the actual query and ingestion workload. A runtime that combines capabilities still shares finite physical resources.

Keep model artifacts versioned and verify them before use. Avoid downloading an unspecified default revision at every production startup. For loaders without revision pinning, prefetch a chosen artifact and reference a controlled local path, with a digest in the deployment manifest.

## 17.4 Natural language is an ambiguous query language

“Show our best customers last month” leaves several decisions unspecified: revenue or order count, gross or net, purchase or refund date, tenant scope, timezone, and whether pending orders count. A text-to-SQL model can produce syntactically valid SQL before any of those ambiguities are resolved.

Northstar can improve the task by exposing business views such as `paid_orders` and a documented net-revenue view, with descriptions that explain their grain. It should also constrain the available tables and operations. A model that sees raw operational tables with unexplained status codes has a harder and riskier task.

For high-impact ambiguity, ask the user for the missing definition or use an explicit product default shown in the response. Do not silently invent a finance metric and present it as the organization's definition.

## 17.5 Separate generation, validation, and execution

A robust text-to-SQL workflow produces a candidate query, validates it, checks its resource and authorization boundaries, executes it, and presents the result with its interpretation. The Spice NSQL interface can participate in this workflow; consult its release-specific request and response schema to determine which steps it performs.

Validation should parse the SQL using a real parser or a trusted query construction layer. A regex that looks for `SELECT` is not a complete policy. Queries can contain multiple statements, nested expressions, functions, and references whose consequences are not visible to a keyword check.

Allow only approved relations and operations. Enforce tenant scope independently of the model's compliance with a prompt. Bound date ranges, result size, and runtime. Where the business question maps to a small set of known report templates, selecting a template and binding parameters can be more reliable than free-form SQL generation.

## 17.6 Test the generated query's meaning

The Chapter 4 fixture is useful for NSQL evaluation. Ask for paid sales, customers with no orders, and net sales after returns. A model that uses the wrong join grain can produce a plausible total of 74,600 cents. A model that uses `NOT IN` without handling NULL may miss the customer with no orders.

Judge the resulting rows and metric definition, not only whether the SQL executes. Multiple SQL expressions can be equivalent. Comparing generated strings to one preferred query can reject a valid alternative while accepting a query that resembles the template but changes a predicate.

Retain the original question, exposed schema version, generated SQL, validation decisions, returned rows, and answer. This is the artifact that lets you distinguish a language-model failure from a query-execution or data-freshness problem.

## 17.7 Tools amplify capabilities and responsibility

Model configuration can expose tools, including data and search operations. Broad automatic tool exposure is convenient for exploration, but a production support assistant should receive only the operations it needs. A tool description should state its input, output, tenant scope, and side effects.

A read-only analysis tool and a refund-creation tool belong in different trust and retry categories. Even a read tool can expose sensitive data or consume substantial resources. Use bounded operations with server-side authorization instead of relying on the model to infer those constraints.

**Exercise.** Write five ambiguous business questions and their clarified definitions. For each, decide whether the product should ask a follow-up question, use a named default, or select a fixed report template. Test the final SQL against the fixture and retain the actual rows.

**Further reading.** See cookbook `text-to-sql/`, `models/openai/`, `models/filesystem/`, and the [model documentation](https://spiceai.org/docs/features/large-language-models). Inspect `crates/runtime/src/http/v1/nsql.rs` and the generated OpenAPI schema for the installed interface.
