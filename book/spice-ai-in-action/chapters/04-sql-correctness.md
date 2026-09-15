# 4. SQL that preserves the business facts

A query can parse, run quickly, and return a plausible answer while being wrong for the business question. Northstar's small fixture lets us make these errors visible. The principles apply equally to federated and accelerated queries: identify the grain, preserve keys, handle missing values deliberately, and reconcile results with independent arithmetic.

## 4.1 State the grain before writing the join

The grain of `orders` is one row per order. The grain of `order_items` is one row per line within an order. Joining them creates one row per matching line. An order amount repeated on those rows is no longer safe to sum as though each order still appears once.

**Local lab: the wrong aggregation.**

```sql
SELECT SUM(o.total_cents) AS gross_cents
FROM paid_orders o
JOIN order_items i ON o.order_id = i.order_id;
```

Observed result:

```json
[{"gross_cents":74600}]
```

The known paid-order total is 47,100 cents. The extra 27,500 is the repeated 12,500-cent order and the repeated 15,000-cent order, each of which has two lines. No engine malfunction is needed: the SQL faithfully sums the joined rows.

Aggregate the line side to the order grain before joining:

```sql
WITH line_totals AS (
  SELECT order_id,
         SUM(quantity * unit_price_cents) AS line_cents
  FROM order_items
  GROUP BY order_id
)
SELECT SUM(o.total_cents) AS gross_cents,
       SUM(i.line_cents) AS line_cents
FROM paid_orders o
JOIN line_totals i ON o.order_id = i.order_id;
```

Observed result:

```json
[{"gross_cents":47100,"line_cents":47100}]
```

`SUM(DISTINCT total_cents)` is not a general repair. Two different orders may legitimately have the same amount. De-duplication must use the entity's key, not a coincidentally repeated measure.

## 4.2 NULL is neither zero nor an empty string

Run:

```sql
SELECT COUNT(*) AS rows,
       COUNT(customer_id) AS known_customers,
       COUNT(DISTINCT customer_id) AS distinct_customers
FROM orders;
```

Observed output is `rows = 8`, `known_customers = 7`, and `distinct_customers = 4`. `COUNT(*)` counts rows. `COUNT(expression)` counts non-NULL values. A NULL customer does not become a fifth known customer.

This distinction changes outer-join reports. To count orders per customer, count a nonnullable order key from the joined order relation, not `COUNT(*)`; an unmatched customer still contributes an outer-join row. Also include the tenant relationship in the join when identifiers are scoped by tenant.

An empty aggregate is another common boundary:

```sql
SELECT COUNT(*) AS n, SUM(total_cents) AS total
FROM orders
WHERE status = 'missing';
```

Observed result is `[{"n":0,"total":null}]`. Replacing NULL with zero is a business decision. “There were no matching sales” can reasonably display zero. “The upstream amount is unknown” should not silently become zero. Use `COALESCE` after deciding which condition it represents.

## 4.3 Anti-joins and missing keys

To find customers without orders, prefer an explicit existence test:

```sql
SELECT c.customer_id
FROM customers c
WHERE NOT EXISTS (
  SELECT 1 FROM orders o
  WHERE o.customer_id = c.customer_id
    AND o.tenant_id = c.tenant_id
)
ORDER BY c.customer_id;
```

The federated local run returns customer 5. Compare this with:

```sql
SELECT customer_id
FROM customers
WHERE customer_id NOT IN (
  SELECT customer_id FROM orders
);
```

The same federated run returns `[]`. The subquery contains NULL, so the expression does not establish a TRUE nonmembership predicate for the otherwise unmatched customer. The verifier preserves both outputs. Use the `NOT EXISTS` expression for the business question and retain NULL-focused checks when changing execution engines. Appendix A records the precise coverage and any variant discrepancy; the portable lesson is to verify semantics at the deployed boundary.

## 4.4 Refunds require a second grain decision

An order may have several returns. Aggregate them before joining to orders:

```sql
WITH refunds AS (
  SELECT order_id, SUM(refund_cents) AS refund_cents
  FROM returns
  GROUP BY order_id
)
SELECT o.tenant_id,
       SUM(o.total_cents) AS gross_cents,
       SUM(COALESCE(r.refund_cents, 0)) AS refund_cents,
       SUM(o.total_cents - COALESCE(r.refund_cents, 0))
         AS net_cents
FROM paid_orders o
LEFT JOIN refunds r ON o.order_id = r.order_id
GROUP BY o.tenant_id
ORDER BY o.tenant_id;
```

Observed results are:

| Tenant | Gross cents | Refund cents | Net cents |
|---|---:|---:|---:|
| north | 22,200 | 11,200 | 11,000 |
| south | 24,900 | 3,300 | 21,600 |

This definition attributes refunds to their original orders. A finance report that recognizes refunds on the return date needs a different query, usually an event ledger with positive sales and negative refunds. Neither definition is inherently universal. Name the metric so a reader knows which one they are seeing.

The fixture has one currency. Production must either keep currencies separate or perform a documented conversion using a rate, rate date, and rounding policy. Summing cents from unrelated currencies produces an invalid metric regardless of numeric precision.

## 4.5 Money, time, and precision

Store money in a representation that preserves the required unit. Integer minor units work for the fixture. Decimal types are appropriate when the domain requires fixed precision and scale. Converting to floating point for display should not become the intermediate representation used to reconcile balances.

The local expression `CAST(SUM(total_cents) AS DECIMAL(18,2)) / 100` returned `471.0` in JSON. JSON formatting does not establish the server-side decimal type or a universal number of displayed places. Format currency at the application boundary and retain integer or decimal values for calculations.

Timestamp casts also deserve attention. The fixture's file schema inferred `Timestamp(s)`. Casting it in a view gives a stable SQL-facing expression, but production values may have finer precision or timezone offsets. Decide whether a daily bucket means UTC day or a local business day before writing `CAST(timestamp AS DATE)`. Daylight-saving transitions make “one day” and “24 hours” different concepts in local time.

## 4.6 Windows and deterministic ordering

A running sales total needs both a partition and an explicit frame:

```sql
SELECT order_id,
       SUM(total_cents) OVER (
         PARTITION BY tenant_id
         ORDER BY ordered_at, order_id
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS running_cents
FROM paid_orders
WHERE tenant_id = 'north'
ORDER BY order_id;
```

Observed running values are 12,500, 19,700, 22,200, and 22,200 for orders 1001, 1002, 1005, and 1007. The order key breaks timestamp ties. The `ROWS` frame makes row-wise accumulation explicit. A default range frame can treat peers differently from the intended sequence.

Pagination has the same requirement. An `ORDER BY` on a nonunique timestamp alone does not identify a stable boundary. Use a deterministic tie-breaker and decide what happens when data changes between pages.

## 4.7 A correctness checklist with teeth

Before accepting a query, write down its input grain, output grain, key scope, and missing-value policy. Add at least one duplicate key candidate, one unmatched join row, one NULL, and an empty input to the validation data where the domain permits them. Reconcile the result with an independently computed small example.

A successful query is evidence of execution, not of meaning. The companion verifier establishes a compact set of meanings for Northstar. It is intentionally more valuable than a screenshot of a populated dashboard.

**Exercises.** Write a customer report that includes customer 5 with zero orders. Then add a second return to a copy of the fixture and verify that the net-revenue query does not duplicate gross sales. Finally, design a daily event ledger that recognizes the three refunds on August 5 and 6 rather than on the original order dates. Solutions appear in Appendix C.

**Further reading.** The [Spice SQL reference](https://spiceai.org/docs/reference/sql) documents the available dialect. The exact demonstrations in this chapter are preserved in `evidence/stable-sql.json`; they are real SQL sessions, not simulated output.
