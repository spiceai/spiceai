# S3 Vectors Metadata Filtering

A crate to handle parsing the S3 vectors metadata filtering syntax and provide interopability with Apache DataFusion expressions.

## Features

- **JSON-based filter language** with MongoDB-style operators
- **Type-safe parsing and validation** of filter expressions
- **DataFusion integration** for SQL-compatible query execution
- **Comprehensive operator support** including comparison, array, and logical operations
- **Robust error handling** with detailed error messages


## DataFusion Integration

The crate seamlessly converts metadata filters to DataFusion expressions:

```rust
use vector_metadata_filter::*;
use datafusion::logical_expr::Expr;

let filter = MetadataFilter::from_json(r#"{"year": {"$gt": 2019}}"#)?;
let expr: Expr = convert_to_datafusion_expr(&filter)?;
```
