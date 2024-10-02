# Spice.ai OSS Benchmarks

## TPC-DS (Decision Support Benchmark)

### Intervals like 'date + 30 days' are not supported

To fix: use *INTERVAL* data type.

```sql
# fail
SELECT (now() + 30 days);

# success
SELECT (now() + INTERVAL '30 days');
```

| **Affected queries** |  |  |
| --- | --- | --- |
| [q5.sql](/tpcds/q5.sql)   | [q77.sql](/tpcds/q77.sql) | [q16.sql](/tpcds/q16.sql) |
| [q12.sql](/tpcds/q12.sql) | [q80.sql](/tpcds/q80.sql) | [q20.sql](/tpcds/q20.sql) |
| [q21.sql](/tpcds/q21.sql) | [q82.sql](/tpcds/q82.sql) | [q32.sql](/tpcds/q32.sql) |
| [q37.sql](/tpcds/q37.sql) | [q92.sql](/tpcds/q92.sql) | [q40.sql](/tpcds/q40.sql) |
| [q94.sql](/tpcds/q94.sql) | [q95.sql](/tpcds/q95.sql) | [q98.sql](/tpcds/q98.sql) |
