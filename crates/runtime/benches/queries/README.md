# Spice.ai OSS Benchmarks

## TPC-DS (Decision Support Benchmark) Limitations

### Interval Arithmetic (date + 30 days) Not Supported

**Limitation**: Queries using direct date arithmetic (e.g., date + 30 days) are not supported.

**Solution**: Use the _INTERVAL_ data type for date arithmetic.

```sql
# fail
SELECT (now() + 30 days);

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

### DataFusion Supports Only Single SQL Statement per Query

**Limitation**: DataFusion does not support multiple SQL statements within a single query.

**Solution**: Ensure each query contains only one SQL statement.

| **Affected queries**     |                          |
| ------------------------ | ------------------------ |
| [q14.sql](tpcds/q14.sql) | [q23.sql](tpcds/q23.sql) |
| [q24.sql](tpcds/q24.sql) | [q39.sql](tpcds/q39.sql) |
