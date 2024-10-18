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

**Limitation**: There is an error `syntax error at or near "ANTI"` when `EXCEPT` is used, and a `syntax error at or near "SEMI"` in the case of `INTERSECT`
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

### Runtime worker has overflowed its stack

**Limitation**: On some platforms (e.g. Linux kernel 6.9.3), the Runtime will encounter a stack overflow when running certain queries.

**Solution**: Increase the stack size when running `spiced`, with `RUST_MIN_STACK=8388608 spiced` to set an 8MB minimum stack size.

Some platforms default to a lower minimum stack size, like 2MB, which is too small when running certain queries.

**Example Error**:

```bash
thread 'tokio-runtime-worker' has overflowed its stack
fatal runtime error: stack overflow
[1]    77809 IOT instruction (core dumped)
```

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q25.sql](tpcds/q25.sql) | [q29.sql](tpcds/q29.sql) |
| [q30.sql](tpcds/q30.sql) | [q31.sql](tpcds/q31.sql) |
| [q33.sql](tpcds/q33.sql) | [q34.sql](tpcds/q34.sql) |
| [q41.sql](tpcds/q41.sql) | [q44.sql](tpcds/q44.sql) |
| [q49.sql](tpcds/q49.sql) | [q49.sql](tpcds/q49.sql) |

### PostgreSQL does not support using a column alias in a CASE statement

**Limitation**: PostgreSQL does not allow a column alias to be referenced in a `CASE` statement. For example, `CASE WHEN lochierarchy = 0 THEN i_category END`, where `lochierarchy` is defined as `SELECT GROUPING(i_category) + GROUPING(i_class) AS lochierarchy`.
**Solution**: Replace the alias with the actual column name or expression from the `SELECT` statement

```sql
# fail
select
  i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
 from
    web_sales
   ,date_dim       d1
   ,item
 where
    d1.d_month_seq between 1205 and 1205+11
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc,
   case when lochierarchy = 0 then i_category end
  LIMIT 100;
```

```sql
# success
select
  i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
 from
    web_sales
   ,date_dim       d1
   ,item
 where
    d1.d_month_seq between 1205 and 1205+11
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc,
   case when grouping(i_category)+grouping(i_class) = 0 then i_category end
  LIMIT 100;
```

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q36.sql](tpcds/q36.sql)   | [q86.sql](tpcds/q86.sql) |

## MySQL does not support FULL JOIN

**Limitation**: The MySQL connector does not support `FULL JOIN` or `FULL OUTER JOIN` statements.

**Solution**: Rewrite your query to use `UNION` or `UNION ALL`, for example:

```sql
SELECT * FROM t1
LEFT JOIN t2 ON t1.id = t2.id
UNION
SELECT * FROM t1
RIGHT JOIN t2 ON t1.id = t2.id
```

`UNION` removes duplicate records, so if you require duplicate records to remain after your union, use `UNION ALL` like:

```sql
SELECT * FROM t1
LEFT JOIN t2 ON t1.id = t2.id
UNION ALL
SELECT * FROM t1
RIGHT JOIN t2 ON t1.id = t2.id
WHERE t1.id IS NULL
```

**Example Error**:

```bash
Query Error Execution error: Unable to query arrow: Server error: `ERROR 42000 (1064): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'FULL JOIN (SELECT `catalog_sales`.`cs_bill_customer_sk` AS `customer_sk`, `catal' at line 1
```

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q97.sql](tpcds/q97.sql) | |

## MySQL returns NULL on division by zero

**Limitation**: The MySQL connector does not support queries that divide by zero.

**Solution**: Rewrite your query to handle division by zero:

```sql
SELECT
  CASE 
    WHEN count(t1_id) / count(t2_id) = 0 THEN 0
    ELSE count(t1_id) / count(t2_id)
FROM t1, t2
```

MySQL does not return a syntax error when dividing by zero, instead returning `NULL`.

**Example Error**:

```bash
Query Error Unable to convert record batch: Invalid argument error: Column 'am_pm_ratio' is declared as non-nullable but contains null values
```

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q90.sql](tpcds/q90.sql) | |
