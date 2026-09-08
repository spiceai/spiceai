# Appendix C. Worked exercises and design answers

A design exercise can have several valid answers. The solutions below emphasize the contract and evidence that distinguish a defensible choice from an unsupported assumption. SQL solutions use the supplied fixture unless otherwise stated.

## C.1 Selecting a deployment boundary

For a sales dashboard, a plausible contract is read-oriented access to paid orders, a bounded lag, and a visible stale-data state during an outage. For an order-cancellation action, the authoritative source should decide whether the order can still be changed under its transaction rules. For policy search, the authoritative document version and tenant eligibility matter more than a generic “latest request” timestamp.

The resulting architecture can use Spice for analytics and retrieval while keeping cancellation on the transactional service. That is not an incomplete adoption; it assigns each operation to the system whose guarantees it needs. The decision should be revisited only when a supported write path and acceptance tests justify moving the boundary.

## C.2 Inferred schema versus a data contract

The local `orders` schema reports integer IDs and amounts, UTF-8 tenant and status fields, and a timestamp with second precision. Those are observations of this file and runtime. The business contract additionally requires key uniqueness, tenant presence, currency semantics, and a definition of the timestamp.

A later CSV containing a malformed amount can change inference or fail parsing. A schema contract should not depend on the first few rows remaining representative forever. Prefer typed source tables or explicit normalization with failure on unrepresentable values, and test the observed boundary schema during upgrades.

## C.3 Article identity and policy currency

A production article contract needs a stable document key, tenant scope, content version, effective interval, and deletion semantics. The fixture's `article_id` is sufficient for a static exercise. A production policy amended in place needs a version if citations must reproduce the wording used for an earlier decision.

“Current policy” could mean the latest published version or the version effective on a purchase date. Decide before indexing. Retaining only the newest text cannot answer a historical-policy question without another authoritative history source.

## C.4 Customers with zero orders

```sql
SELECT c.customer_id, c.customer_name,
       COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id
 AND c.tenant_id = o.tenant_id
GROUP BY c.customer_id, c.customer_name
ORDER BY c.customer_id;
```

The expected counts are 2, 2, 2, 1, and 0 for customers 1 through 5. Count the joined order key, not `COUNT(*)`, so the unmatched outer-join row contributes zero orders. Order 1007 has no customer and therefore belongs to none of these customer counts.

The sum of customer order counts is seven while the source contains eight orders. That is correct for this definition. A reconciliation report should identify the unmatched order rather than forcing the customer counts to equal the source count by assigning an invented customer.

## C.5 A second return does not duplicate gross sales

In a copied fixture, add return 2004 for order 1001 with 500 refund cents. The `refunds` CTE still creates one row per order, so gross sales remain 47,100 cents. Northern refunds become 11,700 and northern net becomes 10,500.

A raw join of orders to return rows would repeat order 1001's gross amount. Aggregating the many side before the join preserves the order grain. Verify the key count and gross sum before considering the modified net result.

## C.6 Recognize refunds on their own dates

```sql
WITH events AS (
  SELECT tenant_id,
         CAST(ordered_at AS DATE) AS event_date,
         total_cents AS signed_cents
  FROM paid_orders
  UNION ALL
  SELECT o.tenant_id,
         CAST(r.returned_at AS DATE) AS event_date,
         -r.refund_cents AS signed_cents
  FROM returns r
  JOIN orders o ON r.order_id = o.order_id
  WHERE o.status = 'paid'
)
SELECT tenant_id, event_date,
       SUM(signed_cents) AS net_event_cents
FROM events
GROUP BY tenant_id, event_date
ORDER BY tenant_id, event_date;
```

This is an event-date definition. Northern sales occur on August 1 and 3, with refunds of 11,200 cents on August 5. Southern sales occur on August 2 and 4, with a 3,300-cent refund on August 6. The all-time total reconciles to 32,600 cents, but the daily pattern differs from attributing refunds to purchase dates.

In production, return records need tenant-safe order identity, and a refund can have its own state such as requested or settled. Filter according to the business definition. The fixture assumes all three refunds are recognized.

## C.7 Projection and pushdown evidence

A narrow projection can remove columns from the scan, but predicate columns may remain necessary even when they are absent from the final result. In the Chapter 5 plan, the scan reads order ID, status, and total because two fields are needed for filtering.

After replacing the file source with PostgreSQL, compare the source scan and any residual local filter. Do not expect identical operator names. The evidence that matters is which work and transfer remain at each boundary. A source-side plan or query log can complement Spice's plan where needed.

## C.8 Connector acceptance

A database acceptance card should include source version, typed schema, reader identity, connection budget, TLS behavior, representative values, and an outage test. A document-source card should add pagination completeness, stable IDs, updates, deletions, and provenance.

Reject an integration that silently omits rows or fields required by the contract, even if common queries return useful-looking data. Reject a write integration whose retry behavior cannot distinguish a committed request from an uncommitted one. Record the failing artifact rather than describing the integration as merely “flaky.”

## C.9 Historical coverage and late corrections

For a seven-day serving window, use an explicit publication boundary between historical and recent data. A ten-day-old correction needs a route into the historical representation: republish the affected partition, apply a table-format update, or record an adjustment event according to the reporting model.

A query over only the current seven-day accelerator cannot discover that correction unless the system explicitly brings it into the queried representation. Zero-result fallback does not solve a nonempty monthly aggregate that lacks one corrected historical row.

## C.10 Comparing accelerators

The same fixture and SQL establish a row contract across physical paths. Plans show different work placement. Neither proves a general speed or memory ranking. To compare performance, add representative data, a fixed rig, workload concurrency, cache state, run duration, result validation, and preserved metrics.

A failed row contract blocks the performance conclusion for that variant. Faster incorrect results are not a successful optimization. The book's acceptance table deliberately keeps the Cayenne discrepancy visible for this reason.

## C.11 A freshness budget that cannot fit

If the source publishes an export every five minutes, a 30-second freshness promise cannot be met by reducing the result-cache TTL alone. The newest state is not yet available to Spice. Options include changing the source publication mechanism, consuming a supported change stream, querying the authoritative source for the relevant operation, or changing the product's promise.

The budget should include processing and failure behavior, not only nominal intervals. Measure a uniquely identifiable change through the complete application path and retain the source and response observations.

## C.12 CDC interruption after commit

Create a unique source mutation and record its committed state. Interrupt the consumer at a controlled point, retain its durable state and source progress, then restart it through the supported procedure. Poll until the exact final key and value appear, and verify that no extra logical row remains.

A count can stay constant across an update or an insert/delete pair. Therefore, compare keys and values as well as aggregate totals. If the source history needed for replay has expired, the test should exercise a documented resnapshot rather than retrying forever.

## C.13 Current state versus an event log

A current-state table uses the entity key and applies later changes to that entity, including deletions. An event log uses an event identity and retains the sequence of actions. Deleting an entity from current state does not usually mean deleting every historical event about it.

Retention also differs. Current-state retention can make the table incomplete for old entities; event retention bounds the history available for analysis or replay. Define the business meaning before applying a generic upsert policy to either representation.

## C.14 Cayenne evaluation design

Initial-load evidence includes source snapshot identity, loaded keys and values, readiness, and storage state. Steady-CDC evidence adds mutation history, visible values, lag, and checkpoints. Concurrent analytics adds validated query results, per-query metrics, RSS, and maintenance observations. Recovery adds the failure boundary and restored state.

The files and metadata needed for a coherent table must be restored through the supported procedure. Derived caches may be rebuildable. Source history determines whether lost mutable or checkpoint state can be safely reconstructed.

## C.15 Application parameterization

A trusted context object supplies the tenant; the request supplies only permitted report parameters. Bind values into a fixed query and validate response shape. A string such as `north' OR 1=1 --` remains a value when correctly bound, but it should still fail the application's authorized-tenant selection because it is not a valid identity.

An unavailable runtime must produce an error. An empty successful aggregate may produce zero according to the metric definition. Parameterization addresses SQL injection through values; it does not establish authorization or availability semantics.

## C.16 Retrieval judgments

A useful ten-question set includes return-policy paraphrases, exact product terminology, shipping questions for each tenant, battery care, an unrelated warranty question, and a request for information that exists only in the other tenant. Label supporting article IDs and no-answer cases before tuning.

For the boot-return paraphrase, article 1 is relevant to `north`. For southern bicycle-light returns, article 4 is relevant. For warranty coverage not stated in the fixture, the expected answer should acknowledge missing evidence instead of guessing.

## C.17 Hybrid search tradeoffs

Keep each component ranking. A product code may benefit more from an exact-match path than from semantic similarity. A paraphrase may benefit from vectors. RRF can combine the ranked evidence without equating raw score scales, but it can also bring irrelevant candidates from a weaker signal.

Evaluate at the candidate depth used by the generator or reranker. If the relevant policy is ranked fourth and the pipeline only passes three passages, a later excellent generator still lacks the needed evidence.

## C.18 Evidence envelopes and abstention

The supported return envelope includes policy 1 and its exact conditions. The warranty envelope may include care instructions but no warranty rule; the answer must distinguish those. The wrong-tenant order lookup should be denied before any order facts enter an envelope.

A citation validator checks membership in supplied evidence, but semantic support requires more: the cited passage must actually support the claim. A correct source ID attached to an invented exception remains an unsupported answer.

## C.19 Natural-language report definitions

“Best customer” should be clarified or mapped to a named metric such as net paid sales during a UTC calendar month. “Recent orders” needs a date window and status definition. “Returns” needs requested versus settled refunds and the date attribution rule.

Where the product offers a small report menu, a model can select a template and extract parameters. This constrains query structure while retaining a natural-language interface. Free-form SQL should be reserved for a surface whose validation and resource policy can support it.

## C.20 Three bounded tools

`lookup_order` accepts an order ID, derives tenant and customer scope, and returns a bounded set of fields. `search_policies` accepts a short question, derives corpus scope, and returns up to a fixed number of cited passages. `sales_summary` accepts an allowed interval and metric name and uses fixed SQL templates.

An unrestricted shell tool is excluded because these tasks do not require arbitrary filesystem or process access. A general write-SQL tool is excluded because the assistant has no need to mutate source state to explain a policy.

## C.21 An upgrade gate

Run schema and row checks against a clean fixture and against a supported copy of persistent state. For CDC, test resume and source mutations. For search, test updated and deleted documents. For model changes, run answer evaluations against a fixed evidence set.

A wrong row, an unauthorized result, or an unrecoverable state transition blocks release regardless of performance improvement. Preserve enough artifacts to determine whether the difference comes from the source, runtime, configuration, or application.

## C.22 Tenant tracing

The user's identity is validated by the application, which derives the tenant. The order tool applies tenant and ownership constraints. Retrieval selects an authorized corpus. Candidate text is checked before external model use. The final citations resolve through an authorized route.

The expected behavior for a northern user requesting southern order 1004 is a denial or a product-defined not-found response that reveals no unauthorized facts. The model should never receive the southern order merely to decide whether to hide it later.

## C.23 Replica and storage ownership

Two sidecars can each own separate storage and source-consumer state. That is simple to reason about but doubles ingestion and storage. A shared service consolidates state but needs its own availability and fairness controls. Two embedded-engine processes sharing the same files is not a valid default rolling-upgrade strategy.

The choice should include source connection and replication-state capacity. Application autoscaling can multiply these resources even when each pod's local memory looks modest.

## C.24 Distributed-report evidence

A large grouped report may scan partitions, apply filters, perform partial aggregates, shuffle by group key, and finalize results. The largest intermediate relation may occur before the partial aggregate or in a join. A hot tenant or product category can create skew.

Task assignments and executor metrics demonstrate distributed work. A final result or scheduler log alone may not. Validate the rows against a reference and inspect per-task work before attributing a speed change to added nodes.

## C.25 Performance-report integrity

Leave unmeasured fields blank or explicitly marked unmeasured. Do not fill a memory field with the query-memory limit, a freshness field with the refresh interval, or a latency field with one stopwatch observation. These are different quantities.

A useful report links to the exact run directory and includes failures. If ten queries time out and ninety finish, the report must retain all hundred outcomes. Reporting only the successful subset misstates the workload.

## C.26 Restoring a lost CDC node

Determine whether the source remains authoritative for all required rows and whether a fresh snapshot is possible. If the node's accelerator and checkpoint state are lost, do not assume an existing source position can safely resume into an empty table. Use the documented resnapshot and new or reconciled consumer-state procedure.

After rebuilding, compare keys, representative values, aggregates, and freshness, then apply a new mutation to verify continued streaming. Retire obsolete source resources only after the replacement consumer is accepted and their ownership is established.

## C.27 Architecture patterns: separate placement from isolation

A sidecar, a shared analytics service, and a tenant-specific runtime make different choices about placement and ownership. Choose a sidecar when the application needs local access and can operate its lifecycle; account for duplicated loading and resource competition. Choose a shared service when centralized ingestion and governance outweigh the additional network boundary. Choose a tenant-specific runtime when the isolation or lifecycle requirement justifies the operational cost. These choices can coexist in one product.

For Northstar, two tiny policy sets do not justify a separate runtime by size alone. A contractual requirement for tenant-specific keys, deletion, or upgrade schedules could justify it. Write that requirement down, then test the controls that enforce it. Putting the same unsafe SQL endpoint in two containers does not by itself create a sound identity model.

A cluster-sidecar design needs an explicit snapshot compatibility and publication contract. A consumer should know which dataset version it loaded and whether that version is permitted for the application release. If publication fails, the previous accepted snapshot may remain available, but its age must be observable. A filename existing in object storage is not an acceptance record.

## C.28 Cloud and BI: identify every stored copy

For an imported BI model, the source-to-Spice freshness and Spice-to-BI refresh are separate intervals. If Spice sees an update within the desired bound but the BI model refreshes only hourly, the user still sees an hourly copy. Verify the desktop report, published service, refresh identity, and report-cache behavior separately. A successful desktop query does not establish the hosted refresh path.

A migration from an OSS runtime to a managed service should compare version, connector access, authentication, data residency requirements, durable-state lifecycle, and observable failure behavior. Preserve the same business queries and expected results, then add the service-specific deployment checks. Do not substitute a Cloud release announcement for a feature-availability check in the actual tenant and region.

For the AWS workshop design, assign S3 Tables to tabular storage, S3 Vectors to the supported vector service, and Bedrock to supported model inference. Give each integration its own identity and acceptance test. The shared AWS label does not imply shared permissions, compatible embedding dimensions, or atomic visibility across services.

## C.29 Engineering foundations: inspect the contract at a boundary

Suppose a filtered query returns unexpected rows after a connector change. Begin with the actual input, SQL, output, and plan. Determine whether the planner retained a residual filter and what the connector claimed about pushdown. Then follow the provider and wrapper implementations. A guessed fix in the optimizer is premature until the evidence identifies which contract was violated.

For statistics, ask whether the values describe the visible logical table, including updates and deletes, and whether exactness is justified. For columnar batches, inspect schema and nullability before treating buffers as business values. For distributed execution, verify that an executor can resolve the same source and credentials as the coordinator. These are different boundaries; success at one does not prove the others.

A coding assistant can help find all implementations of a trait or draft a configuration. The engineer still owns the reproduction and acceptance record. The useful output is a change whose behavior can be explained and run against the real path, with artifacts another person can inspect.

## C.30 Enterprise ownership

The application team owns the metric and evidence contracts. The platform team owns the installed operator, CRDs, runtime distribution, and deployment route. Data owners approve source access and retention. Identity administrators own issuer and group membership. Policy changes cross these boundaries and need a clear review owner. The incident handoff should carry the affected request, principal scope, resource, release identity, and observed failure, without raw credentials.

A commercial support relationship can help diagnose a runtime problem. It does not replace the local team's knowledge of which dataset or policy the application intended to use. Preserve that knowledge in the deployment record.

## C.31 Policy coverage

A sales-only application needs its SQL endpoint and the exact relations used by its report definition. If the query touches orders, returns, and customers, reviewing only an orders permit is incomplete. Test direct table access and the view path under both tenants, and verify that mutation attempts do not succeed. The support assistant needs a different resource set: policy articles, its approved model, and narrowly scoped tools. Sharing a runtime is not a reason to share every permission.

A missing tenant claim should not match all rows. Alternating principals through the same client connection is a useful identity-reuse test. Preserve the returned tenant IDs and rows for each request.

## C.32 Kubernetes request routing

A Service port maps to a pod target port. In the inspected standalone operator Service, port 8080 maps to the configured HTTP listener, normally 8090. A Service port-forward therefore uses the Service's port on its right-hand side. A pod port-forward addresses the pod's listener instead. The API key or bearer token is checked at the runtime layer; network reachability alone does not authenticate a request.

Flight uses a separate transport and client authentication path. Internal cluster traffic uses the cluster listener and node mTLS. Label all three paths explicitly so a public ingress change does not accidentally expose a coordination endpoint.

## C.33 Partitioning and skew

Tenant partitioning can make tenant-local queries easier to route, but a large tenant can dominate one partition's work. Hash bucketing an order key can spread rows while increasing the number of partitions touched by a tenant query. A useful experiment records each partition's size, executor assignment, query plan, scan work, and resulting rows. Compare both layouts on the same source snapshot and hardware.

Neither a balanced row count nor a correct final total proves that the workload is balanced. Wide rows, selective predicates, expensive expressions, and join structure can distribute work differently from row counts.

## C.34 Rollout state

An ordered rollout can reuse a replica's storage according to the workload's supported lifecycle. A blue-green rollout creates a new generation with new PVCs in the inspected operator design, so it needs a bootstrap or reload plan and overlapping capacity. A retained standby can shorten a traffic rollback while still containing older data and software.

The decision should include peak capacity, source load, snapshot compatibility, freshness at cutover, and a failed-bootstrap experiment. A Service selector switch is a routing event; application readiness and data acceptance must be established before depending on it.

## C.35 Execution tiers

Reconciled integer arithmetic belongs naturally in a SQL function. A centrally managed category service can justify an HTTP function if the batch protocol, timeout, identity, privacy, and failure behavior are explicit. A large model may justify tensor parallelism only after its supported architecture, memory, network cost, and failure behavior are measured.

NULL and overflow tests belong to the arithmetic contract. Ordered response cardinality and unavailable-endpoint behavior belong to the HTTP contract. Model artifact identity, participant rank, quality, and participant failure belong to the inference contract. One successful example cannot stand in for all three.

## C.36 Enterprise launch review

A complete handover lets another engineer reproduce an allowed answer, a denied request, and a recovery scenario using the recorded artifacts. It identifies the deployed image, configuration, policy, source data, and observed route. It also carries failures and integration gaps without converting them into assumed successes.

The launch decision follows that record. If the selected accelerator still fails a required SQL invariant, or the identity matrix has not been executed, more replicas do not resolve the missing acceptance. Complete the relevant integration and preserve its evidence before treating the service as ready.
