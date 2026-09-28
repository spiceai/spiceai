# 25. Capstone: Northstar operational analytics

This chapter assembles the book's data path into a small working service and then defines the steps needed to replace the fixture with operational sources. The local result is deliberately narrow: authenticated tenant-specific sales summaries. Its narrowness makes the ownership, metric definition, and failure behavior easy to inspect.

![Figure 25.1. The capstones share one bounded service boundary.](figures/northstar.png)

## 25.1 The product contract

The endpoint returns paid-order count and gross sales in USD cents for the authenticated tenant. It includes a definition identifier, `paid-gross-v1`. Pending and cancelled orders are excluded. A zero-value paid order is counted. Refunds are not subtracted by this endpoint; the net-sales query from Chapter 4 is a separate metric definition.

The local tokens stand in for a production identity provider. The server maps each token to a tenant. A request body or query string cannot select another tenant. The upstream Spice URL and optional runtime API key are server configuration.

This is a teaching HTTP service, not a replacement for a production web framework, identity provider, ingress, or admission-control layer. Its purpose is to make the data and authorization boundary executable. Before deployment, add the production controls described in Chapters 20 and 21.

## 25.2 Start the runtime and service

Start the search variant so both capstones can share one runtime:

```bash
spiced spicepod.search.yaml \
  --http 127.0.0.1:8090 --flight 127.0.0.1:50051
```

In another terminal, configure two distinct local demonstration tokens and start the service:

```bash
export NORTHSTAR_NORTH_TOKEN='local-north-token-1234'
export NORTHSTAR_SOUTH_TOKEN='local-south-token-5678'
export SPICE_URL='http://127.0.0.1:8090'
python3 service.py
```

These published values are test credentials and must not be used in a deployed environment. The service binds to `127.0.0.1:8088` by default. Its token map requires two distinct strings of at least 16 characters for the demonstration.

The standard-library implementation keeps dependencies small. A production version should put the same business functions behind a mature HTTP stack with connection timeouts, bounded workers, TLS, rate limiting, access logging, and real identity validation.

## 25.3 Call the summary

```bash
curl --fail-with-body -sS http://127.0.0.1:8088/sales \
  -H "Authorization: Bearer $NORTHSTAR_NORTH_TOKEN"
```

The application client's observed northern result is:

```json
{
  "tenant_id":"north",
  "paid_orders":4,
  "gross_cents":22200,
  "currency":"USD",
  "definition":"paid-gross-v1"
}
```

The southern contract is two paid orders and 24,900 cents. The service does not accept `?tenant_id=south` as an alternate selector; it rejects unsupported query strings. The tenant comes from the token mapping and is bound as a SQL parameter.

Try the request without an authorization header and verify HTTP 401. Try a token that belongs to neither tenant. Then query each tenant and compare the values. Save the HTTP status and body, not only a screenshot of the successful response.

## 25.4 Trace the execution

The handler establishes tenant context, calls `sales`, and returns a JSON object. `sales` executes fixed SQL with a bound value, verifies the result's tenant and numeric types, and attaches the metric definition. Upstream errors become `data_unavailable` with HTTP 502 in the teaching service.

The service does not convert a query failure into zero sales. It returns zeros only when the successful grouped query produces no row for the authorized tenant. This distinguishes an empty business result from an unavailable data system.

In a production service, attach a request identifier and correlate it with runtime diagnostics. Preserve the internal error safely for operators while keeping the public error stable. Do not log authentication tokens or arbitrary request bodies.

## 25.5 Add net revenue as a deliberate version

Use the refunds-at-order-grain query from Chapter 4 to create a separate net-sales operation. Give it a definition such as `paid-net-by-order-v1`, because refunds are attributed to their original orders. Return gross, refund, and net amounts so the client can explain the calculation.

The fixture's northern values are 22,200 gross, 11,200 refunds, and 11,000 net. Southern values are 24,900, 3,300, and 21,600. Reconcile all three fields. If the product instead needs refunds recognized on their return dates, use the event-ledger solution from Appendix C and name that metric differently.

This is how a data API evolves safely: a named new definition, explicit tests, and a documented migration for callers. Quietly changing `/sales` from gross to net would preserve its JSON field type while changing its meaning.

## 25.6 Replace files with PostgreSQL

Create the source tables and load the fixture into a disposable PostgreSQL database. Replace the `orders` dataset with the connector declaration from Chapter 6 while preserving its name and schema contract. Run the same application requests and SQL verifier.

Then introduce CDC using Chapter 10's procedure. Keep the source mutation test and expected final values. The application should continue to use the same fixed SQL while the data path changes. Validate the plan and freshness because unchanged SQL does not imply unchanged execution or timing.

Add returns through its own supported source and replication path. Do not assume that orders and returns now form a globally synchronized snapshot. Define how temporary cross-table lag affects the net-sales metric and whether the application gates it on a shared freshness condition.

## 25.7 Add a coverage and freshness response

The local fixture is a fixed historical dataset, so a current request timestamp would be misleading as its “last updated” value. In production, derive freshness from the source and replication or refresh observations relevant to the metric.

A response can include a data-version identifier, coverage interval, and status such as current, stale, or loading, using a product-defined enum. The status should come from a real check, not from whether the last HTTP query happened to succeed.

If the freshness contract is exceeded, choose the product behavior before an incident. A dashboard may display the last known result with a visible stale state. An operational eligibility decision may refuse to proceed. Do not silently route all requests to the source unless source capacity and authorization have been designed for that fallback.

## 25.8 Acceptance before launch

The launch record should include the SQL fixture contract, authenticated service responses, source and accelerator acceptance, restart recovery, freshness observations, and a workload measurement on the intended deployment. Include wrong-tenant and unavailable-runtime cases.

A local demonstration does not establish production concurrency or availability. It establishes the small contract that the production system must preserve while adding capacity and operations. Keep that contract in the release gate as data volumes and topology grow.

**Design review.** Explain where tenant identity originates, which system owns order truth, how refunds are defined, which state survives a restart, and what the user sees when freshness exceeds its bound. If two team members give different answers, resolve that disagreement before changing infrastructure.

**Companion files.** `service.py`, `app_client.py`, `spicepod.search.yaml`, `verify.py`, and the `data/` directory contain the executable local path. Appendix A distinguishes its observed results from the external PostgreSQL acceptance procedure.
