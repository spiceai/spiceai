# Text to SQL Benchmark

Instructions to run the benchmark:

1. Run Spice
1. Run `tpch_nsql` eval dataset

```bash
curl -XPOST "http://localhost:8090/v1/evals/tpch_nsql" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o-mini"
  }'
[{"id":"d846dcc0a0dd9df94b8d4f72c874aa90","created_at":"2025-02-04T23:10:29","dataset":"tpch_nsql","model":"gpt-4o-mini","status":"Completed","scorers":["json_match"],"metrics":{"json_match/mean":0.6363636}}]
```

To review benchmark detailed result:

```bash
curl -XPOST http://localhost:8090/v1/sql \
  --data "SELECT
    input,
    output,
    actual,
    value
  FROM eval.results
  WHERE run_id='74d3c352a21966af0f1a5a56fc85f4b8'" | jq
```

## Prompt to generate sample questions

```shell
Use the following context for TPC-H Database:
Schema Overview:
- PART: Information about products, including identifiers, names, types, and retail prices  
- SUPPLIER: Details about product suppliers, including location and account balance  
- PARTSUPP: Part-supplier relationships with supply costs and availability  
- CUSTOMER: Information about customers, such as their market segment and account balance  
- ORDERS: Records of customer orders with total price and order priority  
- LINEITEM: Detailed information for each order line, including ship dates, discounts, and taxes  
- NATION: Countries and their relationships to regions  
- REGION: Definitions of geographical regions  
Table Relationships:
- PARTSUPP references PART and SUPPLIER  
- LINEITEM references ORDERS, PART, and SUPPLIER  
- CUSTOMER connects to ORDERS  
- NATION references REGION and connects to CUSTOMER and SUPPLIER

"partsupp": [
  { "name": "ps_partkey", "data_type": "Int32", "nullable": true },
  { "name": "ps_suppkey", "data_type": "Int32", "nullable": true },
  { "name": "ps_availqty", "data_type": "Int32", "nullable": true },
  { "name": "ps_supplycost", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "ps_comment", "data_type": "LargeUtf8", "nullable": true }
],
"orders": [
  { "name": "o_orderkey", "data_type": "Int32", "nullable": true },
  { "name": "o_custkey", "data_type": "Int32", "nullable": true },
  { "name": "o_orderstatus", "data_type": "LargeUtf8", "nullable": true },
  { "name": "o_totalprice", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "o_orderdate", "data_type": "Date32", "nullable": true },
  { "name": "o_orderpriority", "data_type": "LargeUtf8", "nullable": true },
  { "name": "o_clerk", "data_type": "LargeUtf8", "nullable": true },
  { "name": "o_shippriority", "data_type": "Int32", "nullable": true },
  { "name": "o_comment", "data_type": "LargeUtf8", "nullable": true }
],
"customer": [
  { "name": "c_custkey", "data_type": "Int32", "nullable": true },
  { "name": "c_name", "data_type": "LargeUtf8", "nullable": true },
  { "name": "c_address", "data_type": "LargeUtf8", "nullable": true },
  { "name": "c_nationkey", "data_type": "Int32", "nullable": true },
  { "name": "c_phone", "data_type": "LargeUtf8", "nullable": true },
  { "name": "c_acctbal", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "c_mktsegment", "data_type": "LargeUtf8", "nullable": true },
  { "name": "c_comment", "data_type": "LargeUtf8", "nullable": true }
],
"part": [
  { "name": "p_partkey", "data_type": "Int32", "nullable": true },
  { "name": "p_name", "data_type": "LargeUtf8", "nullable": true },
  { "name": "p_mfgr", "data_type": "LargeUtf8", "nullable": true },
  { "name": "p_brand", "data_type": "LargeUtf8", "nullable": true },
  { "name": "p_type", "data_type": "LargeUtf8", "nullable": true },
  { "name": "p_size", "data_type": "Int32", "nullable": true },
  { "name": "p_container", "data_type": "LargeUtf8", "nullable": true },
  { "name": "p_retailprice", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "p_comment", "data_type": "LargeUtf8", "nullable": true }
],
"region": [
  { "name": "r_regionkey", "data_type": "Int32", "nullable": true },
  { "name": "r_name", "data_type": "LargeUtf8", "nullable": true },
  { "name": "r_comment", "data_type": "LargeUtf8", "nullable": true }
],
"nation": [
  { "name": "n_nationkey", "data_type": "Int32", "nullable": true },
  { "name": "n_name", "data_type": "LargeUtf8", "nullable": true },
  { "name": "n_regionkey", "data_type": "Int32", "nullable": true },
  { "name": "n_comment", "data_type": "LargeUtf8", "nullable": true }
],
"supplier": [
  { "name": "s_suppkey", "data_type": "Int32", "nullable": true },
  { "name": "s_name", "data_type": "LargeUtf8", "nullable": true },
  { "name": "s_address", "data_type": "LargeUtf8", "nullable": true },
  { "name": "s_nationkey", "data_type": "Int32", "nullable": true },
  { "name": "s_phone", "data_type": "LargeUtf8", "nullable": true },
  { "name": "s_acctbal", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "s_comment", "data_type": "LargeUtf8", "nullable": true }
],
"lineitem": [
  { "name": "l_orderkey", "data_type": "Int32", "nullable": true },
  { "name": "l_partkey", "data_type": "Int32", "nullable": true },
  { "name": "l_suppkey", "data_type": "Int32", "nullable": true },
  { "name": "l_linenumber", "data_type": "Int32", "nullable": true },
  { "name": "l_quantity", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "l_extendedprice", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "l_discount", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "l_tax", "data_type": {"Decimal128": [15, 2]}, "nullable": true },
  { "name": "l_returnflag", "data_type": "LargeUtf8", "nullable": true },
  { "name": "l_linestatus", "data_type": "LargeUtf8", "nullable": true },
  { "name": "l_shipdate", "data_type": "Date32", "nullable": true },
  { "name": "l_commitdate", "data_type": "Date32", "nullable": true },
  { "name": "l_receiptdate", "data_type": "Date32", "nullable": true },
  { "name": "l_shipinstruct", "data_type": "LargeUtf8", "nullable": true },
  { "name": "l_shipmode", "data_type": "LargeUtf8", "nullable": true },
  { "name": "l_comment", "data_type": "LargeUtf8", "nullable": true }
],

Task:
Generate 10 natural language questions that a user might ask when querying this database for the following areas:  
- Simple select  
- Simple aggregation involved single table
- Filtering  
- Advanced aggregatioins and multi-table joins

Example:  
- find part brand for part key 3
- what is total number of orders
- what is customer Customer#000000001 nation
- how many customers reside in each nation. Return top 3 ordered by nation name

Return the response as a jsonl structured as follows:  
{"category": "select", "level": "basic", "input": "find part brand for part with key 3.  Fields: part_brand", "ideal": "{\"part_brand\": \"Brand#42\"}", "sql": "select p_brand from part where p_partkey = 3;"}
{"category": "aggregation", "level": "basic", "input": "what is total number of orders. Fields: total_orders", "ideal": "{\"total_orders\": 1500000}", "sql": "select count(*) as total_orders from orders;"}
{"category": "join", "level": "basic", "input": "what is customer Customer#000000001 nation. Fields: nation", "ideal": "{\"nation\": \"MOROCCO\"}", "sql": "select n_name as nation from customer c, nation n where c.c_name = 'Customer#000000001' and c.c_nationkey = n.n_nationkey;"}
```
