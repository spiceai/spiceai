# Testing Instructions for Prepared Statements Fix

## Quick Test

Once spiced is built, run the Python ADBC tests:

```bash
cd /Users/lukim/dev/spice2/test/adbc/python
./run_test.sh --start-spiced
```

## Expected Results

### Before the fix:
```
test_basic_query PASSED
test_null_parameter PASSED  
test_multiple_params FAILED
test_string_param FAILED
test_mixed_types FAILED
test_reuse_statement FAILED
test_update_statement FAILED
test_error_handling PASSED (probably)
```

### After the fix:
Most or all tests should pass, especially:
- test_multiple_params (SELECT $1 + $2)
- test_mixed_types (SELECT $1 * $2)  
- test_string_param
- test_reuse_statement

## Manual Testing

You can also test manually with Python:

```python
import adbc_driver_flightsql.dbapi as flight_sql

# Connect to spiced
conn = flight_sql.connect("grpc://localhost:50051")
cursor = conn.cursor()

# Test 1: Simple arithmetic with parameters
cursor.execute("SELECT ? + ?", parameters=[10, 32])
result = cursor.fetchone()
print(f"10 + 32 = {result[0]}")  # Should print 42

# Test 2: Multiplication
cursor.execute("SELECT ? * ?", parameters=[6, 7])
result = cursor.fetchone()
print(f"6 * 7 = {result[0]}")  # Should print 42

# Test 3: String concatenation
cursor.execute("SELECT ? || ' ' || ?", parameters=["Hello", "World"])
result = cursor.fetchone()
print(f"Result: {result[0]}")  # Should print "Hello World"

cursor.close()
conn.close()
```

## What the Fix Does

The fix ensures that when you bind parameters in ADBC:
1. The parameter VALUES are sent via DoPut
2. The parameter SCHEMA (types) is also captured and stored
3. When DoGet executes, it uses the schema to create a typed logical plan
4. DataFusion can then properly infer the result types

This solves errors like:
- "Cannot get result type for arithmetic operation Null + Null"
- Type inference failures for queries without context

## Debugging

If tests still fail, check the spiced logs for:
```
do_put_query: Storing parameter schema
do_get: Using parameter schema to create typed logical plan
```

These log messages confirm the schema is being stored and reused correctly.
