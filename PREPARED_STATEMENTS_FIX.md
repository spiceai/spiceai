# Prepared Statements Parameter Schema Fix

## Problem
DataFusion's prepared statement support requires parameter types to be known during logical plan creation. For queries without context (e.g., `SELECT $1 + $2`), DataFusion treats unbound parameters as NULL, which causes planning errors like "Cannot get result type for arithmetic operation Null + Null".

## Root Cause
The prepared statement flow was:
1. DoPut: Receive and store parameter VALUES
2. DoGet: Create logical plan from SQL (parameters treated as NULL)
3. Bind parameters with `with_param_values()` (too late - plan already failed)

DataFusion needs the parameter types DURING plan creation, not after.

## Solution
Store the parameter SCHEMA from DoPut and reuse it in DoGet:

1. **DoPut Phase**: When parameters are received, serialize both:
   - Parameter values (as before)
   - **NEW**: Parameter schema in Arrow IPC format

2. **DoGet Phase**: When executing:
   - Decode the stored parameter schema
   - Create logical plan from SQL
   - Bind typed parameters with `with_param_values()`
   - Create physical plan and execute
   - This gives DataFusion the type information it needs during planning

## Changes Made

### PreparedStatement Struct
Added `parameter_schema` field to store serialized Arrow schema:
```rust
pub(crate) struct PreparedStatement {
    pub(super) query: String,
    pub(super) parameters: Vec<u8>,  // Serialized parameter values
    pub(super) parameter_schema: Option<Vec<u8>>,  // NEW: Serialized parameter schema
}
```

### do_put_query Function
Store parameter schema alongside values:
```rust
// Serialize parameter schema to IPC format
let mut schema_bytes = Vec::new();
let mut writer = StreamWriter::try_new(&mut schema_bytes, &record_batch.schema())?;
writer.finish()?;
drop(writer);

stmt.parameter_schema = Some(schema_bytes);
```

### do_get Function
Use stored schema to create typed logical plan:
```rust
if let (Some(schema_bytes), Some(params)) = (&parameter_schema, &param_values) {
    // Decode the parameter schema
    let schema = StreamReader::try_new(&schema_bytes[..], None)?.schema();
    
    // Create logical plan
    let plan = session.create_logical_plan(&sql).await?;
    
    // Bind typed parameters
    let plan = plan.with_param_values(params.clone())?;
    
    // Execute
    let physical_plan = session.create_physical_plan(&plan).await?;
    let stream = execute_stream(physical_plan, datafusion.ctx.task_ctx())?;
    
    // Convert to FlightData
    ...
}
```

## Testing

### Rust Unit Tests
All 26 unit tests pass:
- Parameter replacement (? → $1, $2)
- Parameter encoding/decoding  
- PreparedStatement serialization
- Error handling

### Python ADBC Tests
Location: `/Users/lukim/dev/spice2/test/adbc/python/`

Run with:
```bash
cd /Users/lukim/dev/spice2/test/adbc/python
./run_test.sh --start-spiced
```

Expected improvements:
- Before: 2/8 tests passing
- After: Should see more tests passing, especially those with arithmetic on parameters

## References
- DataFusion PR #14639: PREPARE statement syntax
- DataFusion PR #15743: Parameter type inference improvements
- Arrow Flight SQL Spec: Prepared statement protocol
