# Appendix B. Complete local project reference

This appendix contains the complete fixture and core runtime configuration so the first project can be reconstructed from the manuscript alone. The companion archive additionally contains the verifier, service, client, alternate Spicepods, and captured results. All fixture records are fictional.

## B.1 Reconstruct the directory

Create a new directory named `northstar` with `data/` and `sql/` subdirectories. Save each listing under its indicated filename. Start the runtime from `northstar`. Use a separate copy for data mutations so the original contract remains reproducible.

```bash
mkdir -p northstar/data northstar/sql
cd northstar
```

The CSV files use a header row, comma separators, UTF-8 text, and an empty customer field on order 1007. Do not replace that empty field with a literal zero or the string `NULL`: it is part of the missing-value exercise.

## B.2 data/customers.csv

```csv
customer_id,tenant_id,customer_name,region
1,north,Ada Outfitters,west
2,north,Birch Books,east
3,south,Cedar Cycles,west
4,south,Dune Design,east
5,north,Elm Studio,west
```

## B.3 data/orders.csv

```csv
order_id,tenant_id,customer_id,ordered_at,status,total_cents
1001,north,1,2026-08-01T09:00:00Z,paid,12500
1002,north,2,2026-08-01T10:00:00Z,paid,7200
1003,north,1,2026-08-02T11:00:00Z,pending,5000
1004,south,3,2026-08-02T12:00:00Z,paid,9900
1005,north,2,2026-08-03T13:00:00Z,paid,2500
1006,south,4,2026-08-03T14:00:00Z,cancelled,8000
1007,north,,2026-08-03T15:00:00Z,paid,0
1008,south,3,2026-08-04T16:00:00Z,paid,15000
```

## B.4 data/order_items.csv

```csv
order_id,line_id,sku,quantity,unit_price_cents
1001,1,TRAIL-BOOT,2,4000
1001,2,RAIN-SHELL,1,4500
1002,1,DAY-PACK,1,7200
1003,1,WOOL-SOCK,2,2500
1004,1,BIKE-LIGHT,3,3300
1005,1,WOOL-SOCK,1,2500
1006,1,TRAIL-BOOT,2,4000
1007,1,GIFT-CARD,1,0
1008,1,TRAVEL-BAG,2,5000
1008,2,CAMP-KIT,1,5000
```

## B.5 data/returns.csv

```csv
return_id,order_id,refund_cents,returned_at
2001,1001,4000,2026-08-05T09:00:00Z
2002,1002,7200,2026-08-05T10:00:00Z
2003,1004,3300,2026-08-06T11:00:00Z
```

## B.6 data/articles.csv

```csv
article_id,tenant_id,title,body
1,north,Returning a trail boot,Unused trail boots may be returned within 30 days. Keep the receipt and original packaging.
2,north,Waterproof care,Brush off dirt and air dry waterproof shells. Do not use a tumble dryer.
3,north,Shipping delays,Check the tracking number before opening a shipping delay ticket. Contact support after five business days.
4,south,Returning a bicycle light,Bicycle lights may be returned within 14 days. Include every mounting bracket.
5,south,Battery care,Charge bicycle light batteries indoors. Stop using a damaged battery and contact support.
6,south,Shipping delays,Contact the account manager after three business days without a tracking update.
```

## B.7 spicepod.yaml

```yaml
version: v1
kind: Spicepod
name: northstar

datasets:
  - from: file://data/customers.csv
    name: customers
    params:
      file_format: csv
      csv_has_header: "true"
  - from: file://data/orders.csv
    name: orders
    params:
      file_format: csv
      csv_has_header: "true"
  - from: file://data/order_items.csv
    name: order_items
    params:
      file_format: csv
      csv_has_header: "true"
  - from: file://data/returns.csv
    name: returns
    params:
      file_format: csv
      csv_has_header: "true"
  - from: file://data/articles.csv
    name: articles
    params:
      file_format: csv
      csv_has_header: "true"
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
      SELECT tenant_id, CAST(ordered_at AS DATE) AS sales_date,
             COUNT(*) AS order_count, SUM(total_cents) AS gross_cents
      FROM paid_orders
      GROUP BY tenant_id, CAST(ordered_at AS DATE)
```

## B.8 Search additions

The final search variant adds Arrow acceleration and indexes both body and title on `articles`. The explicit SQL examples specify `body` when demonstrating that index alone. The structured Search API can use the available indexed text through its supported search pipeline.

```yaml
    acceleration:
      enabled: true
      engine: arrow
      refresh_mode: full
    columns:
      - name: body
        full_text_search:
          enabled: true
          row_id: [article_id]
      - name: title
        full_text_search:
          enabled: true
          row_id: [article_id]
```

For the vector variant, add the `policy_embed` component from Chapter 14 and the embedding declaration to the body column. Keep the title full-text index. Use `spicepod.vectors.yaml` from the companion package for the complete merged file.

## B.9 Run the local checks

With a runtime listening at the default HTTP port:

```bash
python3 verify.py --url http://127.0.0.1:8090 \
  --output verification.json
python3 app_client.py north
python3 app_client.py north --search shipping
```

Start `service.py` with the environment from Chapter 25, then run:

```bash
python3 verify_service.py --url http://127.0.0.1:8088 \
  --output service-verification.json
```

The service verifier's published default tokens are for this disposable local demonstration only. Pass different tokens through its arguments when testing another isolated setup. The runtime's own API key, when enabled, is supplied to the application client through `SPICE_API_KEY`.

## B.10 File-backed variants

Create `.spice/duckdb` and `.spice/sqlite` before using their file-backed variants. Each dataset receives a distinct engine file in the supplied configurations. Do not run two variants concurrently against the same state paths.

```bash
mkdir -p .spice/duckdb .spice/sqlite
spiced spicepod.duckdb.yaml \
  --http 127.0.0.1:8090 --flight 127.0.0.1:50051
```

Stop that runtime before starting another variant. The book's authoring harness used separate instance-state directories and unused ports to avoid collisions; a reader can use sequential runs for a simpler lab.

## B.11 Cleanup and persistence

Stop the teaching service and runtime with their normal termination mechanism. The source CSV files remain unchanged by the read-only checks. File-backed variants leave derived engine state under `.spice/`; model runs can also leave downloaded assets in the runtime's configured cache location.

To discard a lab, remove only the isolated directory you created after confirming no process uses it. Do not apply a recursive cleanup command to a shared runtime directory, production volume, or source replication state. The external CDC labs have additional source-side resources, including slots or publications, that must be decommissioned by their owner.

## B.12 Companion manifest

| File or directory | Purpose |
|---|---|
| `data/` | Five immutable fictional CSV fixtures |
| `spicepod.yaml` | Federated starter and business views |
| `spicepod.arrow.yaml` | Full-refresh Arrow variant |
| `spicepod.duckdb.yaml` | File-backed DuckDB variant |
| `spicepod.sqlite.yaml` | File-backed SQLite variant |
| `spicepod.cayenne.yaml` | Cayenne experiment; acceptance limitation in Appendix A |
| `spicepod.search.yaml` | Full-text body and title indexes |
| `spicepod.vectors.yaml` | Local model, embeddings, and full-text indexes |
| `spicepod.auth.yaml` | Runtime API-key experiment |
| `verify.py` | Real HTTP SQL checks and plan capture |
| `app_client.py` | Parameterized sales and structured search client |
| `service.py` | Bounded local teaching HTTP service |
| `verify_service.py` | Service integration requests and checks |
| `sql/` | Named SQL examples and exercise solutions |
| `evidence/` in the archive | Selected transcripts and environment record |
