# Spice.ai OSS Benchmarks

## TPC-DS (Decision Support Benchmark)

### Intervals like `date + 30 days` or `date + 5` are not supported

**Limitation**: Queries using direct date arithmetic (e.g., `date + 30 days` or `date + 5`) are not supported.

**Solution**: Use the _INTERVAL_ data type for date arithmetic.

```sql
# fail
SELECT (now() + 30 days);

# fail
SELECT (now() + 30);

# success
SELECT (now() + INTERVAL '30 days');
```

| **Affected queries**     |                          |                          |
| ------------------------ | ------------------------ | ------------------------ |
| [q5.sql](tpcds/q5.sql)   | [q77.sql](tpcds/q77.sql) | [q16.sql](tpcds/q16.sql) |
| [q12.sql](tpcds/q12.sql) | [q80.sql](tpcds/q80.sql) | [q20.sql](tpcds/q20.sql) |
| [q21.sql](tpcds/q21.sql) | [q82.sql](tpcds/q82.sql) | [q32.sql](tpcds/q32.sql) |
| [q37.sql](tpcds/q37.sql) | [q92.sql](tpcds/q92.sql) | [q40.sql](tpcds/q40.sql) |
| [q94.sql](tpcds/q94.sql) | [q95.sql](tpcds/q95.sql) | [q98.sql](tpcds/q98.sql) |
| [q72.sql](tpcds/q72.sql) |                          |                          |

### `EXCEPT` and `INTERSECT` keywords are not supported

**Solution**: Use `DISTINCT` and `IN`/`NOT IN` instead

```sql
# fail
SELECT ws_item_sk FROM web_sales
INTERSECT
SELECT ss_item_sk FROM store_sales;

# success
SELECT DISTINCT ws_item_sk FROM web_sales
WHERE ws_item_sk IN (
    SELECT DISTINCT ss_item_sk FROM store_sales
);

# fail
SELECT ws_item_sk FROM web_sales
EXCEPT
SELECT ss_item_sk FROM store_sales;

# success
SELECT DISTINCT ws_item_sk FROM web_sales
WHERE ws_item_sk NOT IN (
    SELECT DISTINCT ss_item_sk FROM store_sales
);
```
| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q8.sql](tpcds/q8.sql)   | [q38.sql](tpcds/q38.sql) |
| [q14.sql](tpcds/q14.sql) | [q87.sql](tpcds/q87.sql) |

### Projections require unique expression names

**Limitation**: When performing multiple operations on the same column, each result must have a unique name. If multiple expressions produce identical names in the SELECT clause, the query will fail
**Solution**: Use aliases for duplicate duplicate expression names

```sql
# fail
SELECT
  cd_gender,
  cd_dep_count,
  STDDEV_SAMP(cd_dep_count),
  STDDEV_SAMP(cd_dep_count)
FROM
  customer_demographics
GROUP BY
  cd_gender,
  cd_marital_status,
  cd_dep_count
LIMIT 100;

# success
SELECT
  cd_gender,
  cd_dep_count,
  STDDEV_SAMP(cd_dep_count) AS stddev_dep_count_1,
  STDDEV_SAMP(cd_dep_count) AS stddev_dep_count_2
FROM
  customer_demographics
GROUP BY
  cd_gender,
  cd_marital_status,
  cd_dep_count
LIMIT 100;
```

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q35.sql](tpcds/q35.sql) | |


### DataFusion Supports Only Single SQL Statement per Query

**Limitation**: DataFusion does not support multiple SQL statements within a single query.

**Solution**: Ensure each query contains only one SQL statement.

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q14.sql](tpcds/q14.sql) | [q23.sql](tpcds/q23.sql) |
| [q24.sql](tpcds/q24.sql) | [q39.sql](tpcds/q39.sql) |
