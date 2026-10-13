# 2. Your first working application

The goal of this chapter is a running environment with an answer you can verify independently. You will load five files, query a view, and use an automated checker. There are no external data services, API keys, or model downloads in this lab.

## 2.1 Prepare the tools

Install a Spice CLI and runtime using the instructions for your operating system. The official installation page is the reference for supported distributions and release packages. Record the versions before beginning:

```bash
spice version
spiced --version
python3 --version
```

If `spiced` is not on your shell path, use the runtime path installed by the CLI. Keep the binary location in your run record. Installing a CLI does not establish which runtime binary an unrelated shell command will execute.

The starter SQL in this edition was run with `v2.1.1+models.metal`. You can run it on another compatible version, but preserve that version's output rather than assuming byte-identical plans. Do not use an unpinned `latest` image for a reproducibility claim.

Open the supplied `companion/` directory. Its initial structure is:

```text
companion/
  data/
    customers.csv
    orders.csv
    order_items.csv
    returns.csv
    articles.csv
  sql/
    01-first-query.sql
  spicepod.yaml
  verify.py
```

The fixture contains five customers, eight orders, ten order lines, three returns, and six articles. Customer 5 has no orders. Order 1007 has a missing customer and a zero amount. These conditions remain in the fixture throughout the book.

## 2.2 Read the configuration

The core dataset declaration is intentionally small:

```yaml
version: v1
kind: Spicepod
name: northstar

datasets:
  - from: file://data/orders.csv
    name: orders
    params:
      file_format: csv
      csv_has_header: "true"
```

The complete companion configuration registers all five files. Relative file paths are resolved in the application context; start the runtime from the companion directory so the intended files are unambiguous. The dataset name becomes the SQL name. The file format and header declaration make the input interpretation explicit.

Connector `params` use string values in the schema, so `csv_has_header` is quoted. Other typed fields, such as acceleration `enabled`, use booleans where the schema requires them. That does not imply that every runtime setting accepts a boolean, nor that strings such as `enabled` can replace `true` in arbitrary fields. Configuration is typed.

Two views define the first application vocabulary. `paid_orders` filters the source to paid orders and gives the time column an explicit SQL type. `daily_sales` groups those rows by tenant and date:

```yaml
views:
  - name: paid_orders
    sql: |
      SELECT order_id, tenant_id, customer_id,
             CAST(ordered_at AS TIMESTAMP) AS ordered_at,
             total_cents
      FROM orders
      WHERE status = 'paid'
  - name: daily_sales
    sql: |
      SELECT tenant_id,
             CAST(ordered_at AS DATE) AS sales_date,
             COUNT(*) AS order_count,
             SUM(total_cents) AS gross_cents
      FROM paid_orders
      GROUP BY tenant_id, CAST(ordered_at AS DATE)
```

A view is a named query. Do not infer that it stores its results: materialization requires an explicit supported acceleration configuration. The view is valuable even without materialization because it gives the application a stable business concept.

## 2.3 Start and check readiness

In the first terminal, run:

```bash
spiced --http 127.0.0.1:8090 \
  --flight 127.0.0.1:50051 \
  --metrics 127.0.0.1:9090
```

The HTTP endpoint handles the lab's requests. Flight is a separate interface used by Arrow clients. The metrics listener is explicitly enabled for later chapters. Loopback bindings keep this local exercise on the host.

In another terminal, distinguish process health from readiness:

```bash
curl --fail-with-body http://127.0.0.1:8090/health
curl --fail-with-body http://127.0.0.1:8090/v1/ready
```

The captured local responses were `ok` and `ready`, respectively. A healthy process can still be loading its datasets. The verifier polls readiness with a deadline and then checks the dependent view. It does not assume that waiting an arbitrary number of seconds makes a system ready.

If a listener cannot bind, choose unused ports and update the client URL. If a dataset fails to load, read the log for its name and source path. Verify that the file exists relative to the directory you launched from. A successful health response is not evidence that a particular table is queryable.

## 2.4 Ask the first question

Run the supplied query:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:8090/v1/sql \
  -H 'Content-Type: text/plain' \
  -H 'Accept: application/json' \
  --data-binary @sql/01-first-query.sql
```

Its SQL is:

```sql
SELECT tenant_id, COUNT(*) AS paid_orders,
       SUM(total_cents) AS gross_cents
FROM paid_orders
GROUP BY tenant_id
ORDER BY tenant_id;
```

Observed output, formatted for reading:

```json
[
  {"tenant_id":"north","paid_orders":4,"gross_cents":22200},
  {"tenant_id":"south","paid_orders":2,"gross_cents":24900}
]
```

You can verify the northern total from the file: 12,500 + 7,200 + 2,500 + 0 = 22,200 cents. The pending order is excluded. The zero-value order is counted because “paid” is the business criterion and zero is a valid amount in this fixture.

This establishes more than connectivity. It verifies parsing, registration, the view predicate, grouping, aggregation, and result serialization on a real running engine.

## 2.5 Run the contract

```bash
python3 verify.py --url http://127.0.0.1:8090 \
  --output verification.json
```

The script submits SQL over HTTP and records the actual rows. It checks the first result, NULL handling, empty aggregates, join grain, net revenue, customers without orders, and a running total. It also captures schema and plan output. A deliberately wrong query has a deliberately wrong expected business result; this is how the book demonstrates the mistake, not a recommendation to use that query.

The script bypasses result caching for its observations. It compares row values directly for the small fixture. Plans are captured rather than compared byte-for-byte because operator formatting and partition counts can vary with the runtime and machine.

Keep `verification.json` with the binary version and Spicepod used. A future upgrade that changes a result deserves investigation even if the SQL still returns HTTP 200.

## 2.6 Make one controlled change

Change only the first query's tenant filter to select `north`. The result should contain four rows if you select individual paid orders, and one row if you aggregate them without grouping. Explain that difference before running it. Then add `ORDER BY order_id` and inspect the zero-value order.

Do not edit the fixture yet. Later chapters rely on its known totals, and changing data before learning the checks makes failures harder to interpret. For experiments, copy the companion directory or create a separate dataset name.

**Exercise.** Query `information_schema.columns` for `orders`. Record the inferred type and nullability of each column. Which types are guarantees of your data contract, and which merely reflect the eight rows sampled in the fixture? Chapter 3 turns that distinction into a configuration practice.

**Further reading.** See the [installation guide](https://spiceai.org/docs/getting-started), [File connector](https://spiceai.org/docs/components/data-connectors/file), and cookbook recipe `file/`. The actual local transcripts are in `evidence/stable-sql.txt` and `evidence/stable-sql.json`.
