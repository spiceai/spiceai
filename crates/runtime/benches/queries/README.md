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

| **Affected queries**     |     |
| ------------------------ | --- |
| [q35.sql](tpcds/q35.sql) |     |

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
| [q49.sql](tpcds/q49.sql) |                          |

### PostgreSQL does not support using column aliases in expressions with ORDER BY

**Limitation**: PostgreSQL does not allow the use of column aliases in expressions in the `ORDER BY` clause.
**Solution**: Use only the alias in an `ORDER BY` without an expression or other column name. Alternatively, replace the contents of the alias with the actual expression/column.

```sql
-- fail
SELECT a AS b, c FROM tbl ORDER BY b + c;
```

```sql
-- success
SELECT a, c FROM tbl ORDER BY a + c;
-- success
SELECT a AS b, c FROM tbl ORDER BY b;
```

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q36.sql](tpcds/q36.sql) | [q86.sql](tpcds/q86.sql) |
| [q70.sql](tpcds/q70.sql) |                          |

### MySQL does not support FULL JOIN

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
| [q97.sql](tpcds/q97.sql) | [q51.sql](tpcds/q51.sql) |

### MySQL returns NULL on division by zero

**Limitation**: The MySQL connector does not support queries that divide by zero.

**Solution**: Rewrite your query to handle division by zero:

```sql
SELECT
  CASE
    WHEN count(t1_id) / count(t2_id) IS NULL THEN 0
    ELSE count(t1_id) / count(t2_id)
FROM t1, t2
```

MySQL does not return a syntax error when dividing by zero, instead returning `NULL`.

**Example Error**:

```bash
Query Error Unable to convert record batch: Invalid argument error: Column 'am_pm_ratio' is declared as non-nullable but contains null values
```

| **Affected queries**     |     |
| ------------------------ | --- |
| [q90.sql](tpcds/q90.sql) |     |

### SQLite does not support `ROLLUP` and `GROUPING`

**Limitation**: SQLite Data Acccelerator does not support advanced grouping features such as `ROLLUP` and `GROUPING`

**Solution**: To achieve similar functionality in SQLite

- Use manual aggregation with `UNION ALL`: write separate queries for each level of aggregation and combine them with `UNION ALL`.
- Use `CASE` within a `GROUP BY`: simulate `ROLLUP` behavior by applying `CASE` statements within a `GROUP BY` clause.

Example for TPC-DS Q27. Orignal query:

```sql
select  i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'U' and
       cd_education_status = 'Secondary' and
       d_year = 2000 and
       s_state in ('TN','TN', 'TN', 'TN', 'TN', 'TN')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
  LIMIT 100;
```

Rewritten query:

```sql
SELECT i_item_id,
       s_state,
       0 AS g_state,
       AVG(ss_quantity) AS agg1,
       AVG(ss_list_price) AS agg2,
       AVG(ss_coupon_amt) AS agg3,
       AVG(ss_sales_price) AS agg4
FROM store_sales
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE cd_gender = 'M'
  AND cd_marital_status = 'U'
  AND cd_education_status = 'Secondary'
  AND d_year = 2000
  AND s_state IN ('TN')
GROUP BY i_item_id, s_state

UNION ALL

-- Subtotals by i_item_id
SELECT i_item_id,
       NULL AS s_state,
       1 AS g_state,
       AVG(ss_quantity) AS agg1,
       AVG(ss_list_price) AS agg2,
       AVG(ss_coupon_amt) AS agg3,
       AVG(ss_sales_price) AS agg4
FROM store_sales
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE cd_gender = 'M'
  AND cd_marital_status = 'U'
  AND cd_education_status = 'Secondary'
  AND d_year = 2000
  AND s_state IN ('TN')
GROUP BY i_item_id

ORDER BY i_item_id, s_state
LIMIT 100;
```

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q5.sql](tpcds/q5.sql)   | [q14.sql](tpcds/q14.sql) |
| [q18.sql](tpcds/q18.sql) | [q22.sql](tpcds/q22.sql) |
| [q27.sql](tpcds/q27.sql) | [q36.sql](tpcds/q36.sql) |
| [q67.sql](tpcds/q67.sql) | [q70.sql](tpcds/q70.sql) |
| [q77.sql](tpcds/q77.sql) | [q80.sql](tpcds/q80.sql) |
| [q86.sql](tpcds/q86.sql) |                          |

### SQLite does not support `stddev`

**Limitation**: SQLite Data Accelerator does not support the `stddev` (standard deviation) function. There is an error `no such function: stddev` when running the following TPC-DS queries:

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q17.sql](tpcds/q17.sql) | [q29.sql](tpcds/q29.sql) |
| [q35.sql](tpcds/q35.sql) | [q74.sql](tpcds/q74.sql) |

### SQLite DECIMAL Casting and Division

**Limitation**: In SQLite, `CAST(value AS DECIMAL)` does not convert an integer to a floating-point value if the casted value is an integer. Mathematical operations like `CAST(1 AS DECIMAL) / CAST(2 AS DECIMAL)` will be treated as integer division, resulting in `0` instead of the expected `0.5`.

**Solution**: Use `FLOAT` to ensure conversion to a floating-point value: `CAST(1 AS FLOAT) / CAST(2 AS FLOAT)`

Example for TPC-DS Q90. Orignal query:

```sql
select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 9 and 9+1
         and household_demographics.hd_dep_count = 2
         and web_page.wp_char_count between 2500 and 5200) at,
      ( select count(*) pmc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 15 and 15+1
         and household_demographics.hd_dep_count = 2
         and web_page.wp_char_count between 2500 and 5200) pt
 order by am_pm_ratio
  LIMIT 100;
```

Rewritten query:

```sql
-- Updated TPC-DS Q90 with `CAST(.. AS DECIMAL(15,4)` replaced with `FLOAT` to match SQLite's type system

select  cast(amc as FLOAT)/cast(pmc as FLOAT) am_pm_ratio
 from ( select count(*) amc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 9 and 9+1
         and household_demographics.hd_dep_count = 2
         and web_page.wp_char_count between 2500 and 5200) at,
      ( select count(*) pmc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 15 and 15+1
         and household_demographics.hd_dep_count = 2
         and web_page.wp_char_count between 2500 and 5200) pt
 order by am_pm_ratio
  LIMIT 100;
```

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q49.sql](tpcds/q49.sql) | [q75.sql](tpcds/q75.sql) |
| [q90.sql](tpcds/q90.sql) |                          |

### Datafusion Physical Plan does not support logical expression `Exists`, `InSubquery` / not yet implemented for `GROUPING` aggregate function

**Limitation**: Datafusion Physical Plan does not support `Exists`, `InSubquery`, `GROUPING` aggregate function, there will be errors when running the following queries in s3 connector and in-memory arrow accelerator

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q10.sql](tpcds/q10.sql) | [q27.sql](tpcds/q27.sql) |
| [q35.sql](tpcds/q35.sql) | [q36.sql](tpcds/q36.sql) |
| [q45.sql](tpcds/q45.sql) | [q70.sql](tpcds/q70.sql) |
| [q86.sql](tpcds/q86.sql) |                          |

### Datafusion Error during planning: Correlated column is not allowed in predicate

**Limitation**: Datafusion doesn't support planning for queries containing correlated columns in predicate, for example, predicate `item.i_manufact = outer_ref(i1.i_manufact)`, there will be errors when running the following queries in s3 connector and in-memory arrow accelerator

| **Affected queries**     |     |
| ------------------------ | --- |
| [q41.sql](tpcds/q41.sql) |     |
