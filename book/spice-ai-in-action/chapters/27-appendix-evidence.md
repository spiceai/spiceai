# Appendix A. Reproducibility and verification record

This appendix separates executed observations from integration procedures. It is part of the manuscript's technical contract. The presence of a configuration listing is not a claim that its external database, cloud account, model provider, or cluster was provisioned during authoring.

## A.1 Environment and source identities

| Artifact | Recorded identity |
|---|---|
| Local preparation date | September 7, 2026, America/Los_Angeles |
| Platform | macOS 26.6.2, arm64 |
| Runtime source checkout | `16c436f7b8a76ce161a07c0288277ced0ada4a07` |
| Source workspace version | `2.3.0-unstable` |
| Installed stable binary | `v2.1.1+models.metal` |
| Existing development binary | `v2.3.0-unstable-build.b286009302+models` |
| OSS documentation checkout | `e30ef3d3dd84f7ba32c1eb32fc7f7f5d5dc6f375` |
| Cookbook checkout | `a76a26add6545c7edf986791e23966eafcb09a7b` |

The development binary's embedded build identity differs from the source checkout identity. This manuscript does not claim that the development binary was rebuilt from the inspected checkout. UTC timestamps in the logs fall on September 8 because the local session crossed that UTC date while remaining September 7 in the recorded timezone.

## A.2 Executed local coverage

| Path | Observation | Limit |
|---|---|---|
| Federated CSV and views, stable binary | SQL contract completed | Eight-order fixture only |
| HTTP SQL parameter binding | Northern sum returned 22,200 cents | Fixed report query |
| Arrow acceleration, development binary | SQL contract completed | Full-refresh fixture |
| DuckDB file acceleration, development binary | SQL contract completed | Full-refresh fixture; parent directories created |
| SQLite file acceleration, development binary | SQL contract completed | Full-refresh fixture; parent directories created |
| Cayenne file acceleration | Several checks passed; `NOT IN` check differed | Variant is not fully validated |
| Full-text SQL | Northern shipping query returned article 3 | Specific query and configuration |
| Model2Vec vector SQL | Article 1 ranked first for the boot-return query | Public default model revision used in authoring |
| Hybrid SQL | Article 1 ranked first | Small corpus, not a relevance benchmark |
| Runtime API-key authentication | Missing key: 401; valid key: 200 | HTTP SQL acceptance check |
| Northstar application service | Eight service cases passed | Local teaching server and fixture |
| Metrics and system-table discovery | Scrape and table inventory captured | No performance benchmark performed |

The SQL suite contains thirteen named query cases and a parameter-binding check. Some cases capture plans or schema rather than asserting their exact text. A completed suite therefore means its asserted row contracts passed and the observation cases executed; it is not exhaustive SQL certification.

## A.3 The baseline command and output

From the companion directory, with the stable runtime bound to the authoring port:

```bash
python3 verify.py --url http://127.0.0.1:18090 \
  --output ../evidence/stable-sql.json
```

Selected actual output:

```text
first-query: [{"tenant_id": "north", "paid_orders": 4,
  "gross_cents": 22200}, {"tenant_id": "south",
  "paid_orders": 2, "gross_cents": 24900}]
null-counts: [{"rows": 8, "known_customers": 7,
  "distinct_customers": 4}]
empty-aggregate: [{"n": 0, "total": null}]
wrong-join: [{"gross_cents": 74600}]
fixed-join: [{"gross_cents": 47100, "line_cents": 47100}]
customers-without-orders: [{"customer_id": 5}]
wrong-not-in: []
parameters: [{"gross_cents": 22200}]
```

Line wrapping has been added for the printed page; the JSON values are unchanged. Complete query strings, rows, schema, and plans are retained in `stable-sql.json` and the text transcript.

The wrong join and corrected join are the before-and-after demonstration of an application SQL error. No runtime code was changed. The correction changes the query grain and produces the independently reconciled paid-order total.

## A.4 An acceptance discrepancy retained

The Cayenne full-refresh variant returned customer 5 for this expression:

```sql
SELECT customer_id
FROM customers
WHERE customer_id NOT IN (
  SELECT customer_id FROM orders
);
```

Its captured assertion record contains:

```text
actual:   [{"customer_id": 5}]
expected: []
```

The federated baseline returned `[]` for the same expression, whose subquery includes a NULL customer ID. The correctly formulated `NOT EXISTS` business query returned customer 5 in both observed paths. The surrounding checks also recorded eight orders and seven non-NULL customer IDs.

This difference was observed with the installed stable Cayenne run and the existing development binary. The manuscript does not assign an engine root cause, claim that the inspected HEAD reproduces it, or claim a fix. Root-cause diagnosis is unverified; the integration artifact establishes the output difference. The variant's full acceptance suite remains failed at that check, and later checks in that run were not executed after the assertion stopped it.

This is why the book reports acceptance coverage explicitly. A series of earlier green queries cannot erase a later differing result.

## A.5 Search compatibility observations

The inspected source defines `_score` and `_fused_score` as search output names. A local query ordering by `score` returned a schema error naming `_score` among the available fields. Ordering by `_score` returned the intended row. Chapters 14 and 15 use the observed names.

A parameterized query-text argument to `text_search` produced HTTP 400 with the following relevant error:

```text
Second argument must be a query string, but got
Some(Placeholder(Placeholder { id: "$1", field: None })).
```

The exact request and response are in `search-placeholder.json`. The application capstone uses `/v1/search` with a structured JSON `text` field and server-controlled tenant predicates. It does not interpolate the question into SQL.

The initial full-text-only corpus indexed bodies. The southern shipping policy's body does not contain the word “shipping”; its title does. A service check expecting article 6 therefore initially received an empty evidence list. Adding a title full-text index made the same service test return article 6. The final search configuration includes both title and body indexes, while the chapter's explicit `text_search(..., body)` query continues to demonstrate the body-specific northern result.

## A.6 Captured vector and hybrid results

For the northern query text `return unused hiking boots`, the vector run returned:

| Article ID | Title | Observed vector score |
|---|---|---:|
| 1 | Returning a trail boot | 0.8230662556490832 |
| 2 | Waterproof care | 0.5973872008227763 |
| 3 | Shipping delays | 0.5702680000114665 |

The corresponding hybrid query returned IDs 1, 2, and 3 with fused scores 0.03278688524590164, 0.016129032258064516, and 0.015873015873015872. These are captured output values, not stable API constants. Model artifacts, numerical libraries, or corpus changes can alter scores.

The authoring run downloaded the public model's default revision. A production reproduction should pin downloaded artifacts by digest or a controlled local model path. The book's evidence records the model locator but does not claim an immutable upstream revision pin.

## A.7 Application-service checks

The service verifier made real HTTP requests to the local application, which in turn called Spice. The final outcomes were:

| Case | HTTP status | Relevant result |
|---|---:|---|
| No authorization | 401 | `unauthorized` |
| Northern sales | 200 | 22,200 cents |
| Southern sales | 200 | 24,900 cents |
| Tenant override in query string | 404 | `not_found` |
| Northern shipping search | 200 | article 3 |
| Southern shipping search | 200 | article 6 |
| Tenant override in search body | 400 | `invalid_request` |
| Empty question | 400 | `invalid_request` |

The test command is `python3 verify_service.py --url http://127.0.0.1:8088`. Authoring used port 18088 to avoid occupying the conventional lab port. The response transcripts are in `service-verification.json` and `.txt`.

## A.8 What was not executed

External PostgreSQL, MySQL, MongoDB, DynamoDB, Kafka, lakehouse catalog writes, cloud deployment, cluster failover, ADBC transport, paid model generation, reranker providers, and production-scale performance tests were not executed for this manuscript. Their chapters provide configuration patterns, prerequisites, and acceptance procedures grounded in the inspected source and references. Expected integration results are labeled as acceptance criteria.

No production latency, throughput, memory reduction, recovery-time objective, or freshness bound is claimed from the fixture. No runtime source fixes or dependency changes were made. Readers should extend the acceptance record with their own binary, data, credentials, deployment, and artifacts before promoting an integration.

## A.9 Final configuration checks

The final listings quote connector parameter values such as `csv_has_header: "true"` and use array notation for search row identities, `row_id: [article_id]`. The source checkout's Draft 2020-12 schema accepts the seven non-authentication companion Spicepods. The authentication file uses `api_key` and a string secret reference. Its real runtime checks returned HTTP 401 without the key and HTTP 200 with it.

The generated schema rejects that string API-key entry because it describes the enum's object representation. The inspected `ApiKey` implementation has custom string deserialization. This is an example of why schema validation and runtime acceptance are separate observations; the book retains the runtime-accepted authentication syntax. `config-validation.json` records the schema result, while `final-config/` retains the final configuration runs. No runtime behavior was changed to obtain these results.

## A.10 Enterprise additions

The operator chart rendered nine Kubernetes documents, including both CRDs, and Helm lint reported zero failed charts. The two companion v2 templates passed offline structural validation against those rendered CRDs. No Kubernetes API server, admission webhook, registry, or cluster was contacted for those checks. The nested Spicepod remains a separate runtime validation surface.

The local SQL-function lab ran on the recorded development binary and returned 11,000 cents, NULL, and zero for its normal, NULL-input, and zero-input cases. The transcript is `enterprise/functions-sql.json`. It demonstrates the SQL function tier, which is not exclusive to Enterprise.

Enterprise OIDC and Cedar enforcement, policy-provider updates, cluster mTLS and failover, partitioned acceleration, snapshot recovery, remote/WASM function execution, and tensor-parallel inference were not executed. Chapters 30–36 give source-grounded configuration and acceptance procedures for those integrations. Appendix G consolidates their required deployment record.

A final search experiment also illustrates why retrieval expectations must name the pipeline. Against the vector-enabled configuration, the structured Search API returned northern article IDs 3, 1, and 2 for “shipping”; the lexical-only service contract expects only article 3. The exact lexical assertion therefore failed for that different pipeline. The final lexical configuration passed all eight service checks. Both observations are retained under `final-config/`; no unauthorized tenant result was observed in that recorded request.
