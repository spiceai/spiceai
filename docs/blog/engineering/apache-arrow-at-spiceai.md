# Apache Arrow at Spice AI: The Universal Data Format

> How we use Apache Arrow as our in-memory columnar format for zero-copy data flow

---

## 📚 Engineering at Spice AI Series

This article is part of our **Engineering at Spice AI** series, where we share technical deep-dives into the technologies and practices that power our SQL query, search, and inference engine.

- [Rust at Spice AI](rust-at-spiceai.md) — Our systems programming foundation
- **Apache Arrow at Spice AI** *(You are here)*
- [Apache DataFusion at Spice AI](apache-datafusion-at-spiceai.md) — Our SQL query engine foundation
- [DuckDB at Spice AI](duckdb-at-spiceai.md) — Embedded analytics acceleration
- [Apache Iceberg at Spice AI](apache-iceberg-at-spiceai.md) — Open table format integration
- [Vortex at Spice AI](vortex-at-spiceai.md) — Columnar compression for Cayenne
- [Apache Ballista at Spice AI](apache-ballista-at-spiceai.md) — Distributed query execution

---

## Table of Contents

- [What is Apache Arrow?](#what-is-apache-arrow)
- [Why Arrow as Our Core Format?](#why-arrow-as-our-core-format)
- [RecordBatch: The Universal Data Unit](#recordbatch-the-universal-data-unit)
- [Zero-Copy Patterns](#zero-copy-patterns)
- [Arrow Flight for Data Transfer](#arrow-flight-for-data-transfer)
- [Schema Evolution and Casting](#schema-evolution-and-casting)
- [Change Data Capture with Arrow](#change-data-capture-with-arrow)
- [Arrow Compute Kernels](#arrow-compute-kernels)
- [Integration Points](#integration-points)
- [Lessons Learned](#lessons-learned)

---

Apache Arrow is the lingua franca of modern data systems. It defines a language-independent columnar memory format for flat and hierarchical data, enabling zero-copy reads for analytical operations without serialization overhead.

At Spice, Arrow isn't just a library we use—it's the foundation of our entire data architecture. Every byte of data flowing through our system is represented as Arrow arrays, enabling seamless integration with DataFusion, DuckDB, and dozens of data sources.

## What is Apache Arrow?

Apache Arrow provides:

1. **Columnar Memory Format** — Data is laid out by column, not by row. This enables SIMD vectorization and cache-efficient analytical processing.

2. **Language-Agnostic Specification** — The same memory layout works in Rust, Python, Java, C++, Go, and more. No serialization when crossing language boundaries.

3. **Zero-Copy Sharing** — Multiple processes can share Arrow data without copying. Memory-mapped files, IPC, and Flight all leverage this.

4. **Compute Kernels** — Optimized implementations of common operations (filter, cast, aggregate) that automatically use SIMD.

5. **Arrow Flight** — A high-performance RPC framework for exchanging Arrow data over the network.

## Why Arrow as Our Core Format?

Before Arrow, data systems faced a tradeoff:

- **Row-oriented formats** (JSON, CSV, protocol buffers) are easy to work with but slow for analytics
- **Custom columnar formats** are fast but require serialization at system boundaries

Arrow eliminates this tradeoff. We chose it for Spice because:

### Unified Representation

Data from Postgres, Snowflake, S3, and MongoDB all becomes Arrow `RecordBatch` objects. Our query engine doesn't need format-specific code paths—everything is Arrow.

```rust
// All data sources produce the same type
async fn query_postgres() -> SendableRecordBatchStream { ... }
async fn query_snowflake() -> SendableRecordBatchStream { ... }
async fn query_s3_parquet() -> SendableRecordBatchStream { ... }

// DataFusion processes them identically
```

### Zero Serialization Overhead

When data moves between components—from a connector to the query engine to an accelerator—it stays as Arrow. No JSON encoding, no protobuf marshaling.

```rust
// Data flows without transformation
Connector → RecordBatch → DataFusion → RecordBatch → Accelerator
                    └──────────────────────────────────┘
                    // Same bytes, different owners
```

### Ecosystem Integration

Apache DataFusion, DuckDB, Polars, and most modern data tools speak Arrow natively. This lets us integrate deeply without impedance mismatch.

## RecordBatch: The Universal Data Unit

All data in Spice flows as Arrow `RecordBatch` objects. A `RecordBatch` is:

- A schema (field names and types)
- A collection of equal-length column arrays
- Immutable once created

### Our DataUpdate Abstraction

We wrap `RecordBatch` in domain-specific types:

```rust
// From crates/runtime/src/dataupdate.rs
#[derive(Debug, Clone)]
pub struct DataUpdate {
    pub schema: SchemaRef,
    pub data: Vec<RecordBatch>,
    pub update_type: UpdateType,  // Append, Overwrite, or Changes
}
```

For streaming scenarios, we use `StreamingDataUpdate`:

```rust
pub struct StreamingDataUpdate {
    pub data: SendableRecordBatchStream,
    pub update_type: UpdateType,
}
```

This abstraction lets us handle:

- **Full refreshes** — Replace all data (`Overwrite`)
- **Incremental updates** — Append new rows (`Append`)
- **CDC changes** — Insert, update, delete operations (`Changes`)

### Schema Sharing with SchemaRef

Arrow schemas are wrapped in `Arc` for cheap sharing:

```rust
// SchemaRef is Arc<Schema>
let schema: SchemaRef = batch.schema();

// Shared across all batches in a stream
let adapter = RecordBatchStreamAdapter::new(
    Arc::clone(&schema),  // Just refcount++
    stream
);
```

## Zero-Copy Patterns

The key to Arrow's performance is avoiding unnecessary copies. Here's how we leverage this:

### Arc for Column Sharing

Columns are `Arc<dyn Array>`, making "copies" just reference count increments:

```rust
// This is O(1), not O(n)
let shared_column: ArrayRef = Arc::clone(batch.column(0));

// Multiple batches can share the same underlying buffer
let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::clone(&column)])?;
let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::clone(&column)])?;
// batch1 and batch2 share the same memory
```

### Slicing Without Copying

`RecordBatch::slice()` creates a view into existing data:

```rust
// These share the underlying buffer
let full_batch = get_batch();           // 10,000 rows
let first_half = full_batch.slice(0, 5000);     // No copy
let second_half = full_batch.slice(5000, 5000); // No copy
```

### Schema-Compatible Sharing

When the schema matches, we can reuse columns directly:

```rust
pub fn try_cast_to(record_batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let existing_schema = record_batch.schema();

    // If schemas are compatible, just update the schema metadata
    if schema.contains(&existing_schema) {
        return record_batch.with_schema(schema);  // No data copy
    }

    // Only cast columns that actually need it
    let cols = schema.fields().iter().map(|field| {
        if let Some(column) = record_batch.column_by_name(field.name()) {
            if types_match(column.data_type(), field.data_type()) {
                Ok(Arc::clone(column))  // Zero-copy
            } else {
                cast(column, field.data_type())  // Only when necessary
            }
        } else {
            Ok(new_null_array(field.data_type(), record_batch.num_rows()))
        }
    }).collect()?;

    RecordBatch::try_new(schema, cols)
}
```

### Downcast Instead of Convert

When you need a specific array type, downcast rather than convert:

```rust
// Good: Zero-copy downcast
let int_array = array
    .as_any()
    .downcast_ref::<Int32Array>()
    .context(TypeMismatchSnafu)?;

// Bad: Unnecessary conversion
let values: Vec<i32> = array.values().iter().copied().collect();
```

## Arrow Flight for Data Transfer

[Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) is a high-performance RPC framework built on gRPC that uses Arrow's IPC format. We use it extensively for:

1. **Federated queries** — Pushing queries to remote Arrow Flight SQL servers
2. **Data ingestion** — Receiving streaming data via DoPut
3. **Subscriptions** — Bidirectional streaming for CDC with DoExchange

### Flight Client

Our `FlightClient` enables zero-copy data transfer:

```rust
#[derive(Debug, Clone)]
pub struct FlightClient {
    client: FlightServiceClient<Channel>,
    credentials: Credentials,
    url: Arc<str>,
    metadata: Option<tonic::metadata::MetadataMap>,
}

impl FlightClient {
    pub async fn query(&self, sql: &str) -> Result<FlightRecordBatchStream> {
        // Execute SQL on remote server
        // Returns streaming Arrow batches without serialization
        let ticket = self.get_flight_info(sql).await?;
        let stream = self.do_get(ticket).await?;
        Ok(FlightRecordBatchStream::new(stream))
    }
}
```

### Flight Server for Query Results

When serving query results, we convert `RecordBatch` streams to Flight:

```rust
pub fn record_batches_to_flight_stream(
    record_batches: Vec<RecordBatch>,
) -> impl Stream<Item = Result<FlightData, Status>> {
    FlightDataEncoderBuilder::new()
        .build(stream::iter(record_batches.into_iter().map(Ok)))
        .map_err(to_tonic_err)
}
```

### DoPut for Data Ingestion

External systems can push data to Spice via Flight DoPut:

```rust
// Decode incoming Flight data to Arrow
let batch = arrow_flight::utils::flight_data_to_arrow_batch(
    message,
    Arc::clone(&schema),
    &dictionaries,
)?;

// Stream to accelerator for storage
let write_stream: SendableRecordBatchStream = Box::pin(
    RecordBatchStreamAdapter::new(schema, batch_receiver)
);
accelerator.write(write_stream).await?;
```

### DoExchange for CDC Subscriptions

Bidirectional streaming enables real-time data subscriptions:

```rust
// Client subscribes to changes
let (tx, rx) = client.do_exchange(subscription_request).await?;

// Server streams CDC events as Arrow batches
loop {
    let batch = rx.next().await?;
    process_changes(batch)?;
}
```

## Schema Evolution and Casting

Real-world data sources have schema evolution. Our `arrow_tools` crate handles this:

### Schema Validation

```rust
pub fn verify_schema(
    expected: &arrow::datatypes::Fields,
    actual: &arrow::datatypes::Fields,
) -> Result<()> {
    if expected.len() != actual.len() {
        return SchemaMismatchNumFieldsSnafu {
            expected: expected.len(),
            actual: actual.len(),
        }.fail();
    }

    for (expected_field, actual_field) in expected.iter().zip(actual) {
        if !DFSchema::datatype_is_semantically_equal(
            expected_field.data_type(),
            actual_field.data_type()
        ) {
            return SchemaMismatchDataTypeSnafu {
                field: expected_field.name(),
                expected: expected_field.data_type(),
                actual: actual_field.data_type(),
            }.fail();
        }
    }
    Ok(())
}
```

### Automatic Casting

When schemas differ but are compatible, we cast automatically:

```rust
// Int32 → Int64: Safe widening cast
// Utf8 → LargeUtf8: Safe string cast
// Int64 → Int32: Checked narrowing (may fail)

let casted_batch = try_cast_to(batch, target_schema)?;
```

## Change Data Capture with Arrow

CDC events are represented as Arrow batches with a standard schema:

```rust
pub fn changes_schema(table_schema: &Schema) -> Schema {
    Schema::new(vec![
        // Operation: c=create, u=update, d=delete, r=read, t=truncate
        Field::new("op", DataType::Utf8, false),

        // Primary key columns that changed
        Field::new(
            "primary_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),

        // The actual row data as a nested struct
        Field::new(
            "data",
            DataType::Struct(table_schema.fields().clone()),
            true,
        ),
    ])
}
```

This representation enables:

- **Streaming changes** as Arrow batches
- **Efficient storage** in accelerators
- **Type-safe processing** with Arrow's type system

## Arrow Compute Kernels

Arrow provides SIMD-optimized kernels for common operations. We use these instead of manual loops:

### Filtering

```rust
use arrow::compute::filter;

// SIMD-optimized filtering
let mask = BooleanArray::from(vec![true, false, true, false]);
let filtered = filter(&array, &mask)?;
```

### Casting

```rust
use arrow::compute::cast;

// Type conversion with overflow checking
let int64_array = cast(&int32_array, &DataType::Int64)?;
```

### Aggregation

```rust
use arrow::compute::{sum, min, max};

let total = sum(&int_array);
let minimum = min(&int_array);
let maximum = max(&int_array);
```

### Comparison

```rust
use arrow::compute::{eq, lt, gt};

// Returns BooleanArray for filtering
let mask = gt(&array, &scalar)?;
```

## Integration Points

Arrow is our integration layer with external systems:

### Connector Conversions

Each data source converts to Arrow:

```rust
// ClickHouse
pub fn block_to_arrow<T: ColumnType>(block: &Block<T>) -> Result<RecordBatch> {
    let fields = block.columns()
        .map(|col| map_clickhouse_to_arrow_type(col.sql_type()))
        .collect();
    let arrays = block.columns()
        .map(|col| convert_column_to_array(col))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}

// OpenTelemetry Metrics
pub fn metrics_to_arrow(metrics: &[Metric]) -> Result<RecordBatch> {
    let mut builder = OtelToArrowConverter::new();
    for metric in metrics {
        builder.append(metric)?;
    }
    builder.finish()
}
```

### DuckDB Integration

DuckDB has native Arrow support:

```rust
// Query returns Arrow directly
let batches: Vec<RecordBatch> = connection
    .execute("SELECT * FROM table")?
    .collect()?;

// Insert from Arrow
connection.register_arrow_table("temp", batches)?;
```

### Parquet I/O

Parquet files serialize Arrow batches:

```rust
// Write
let writer = ArrowWriter::try_new(file, schema, None)?;
for batch in batches {
    writer.write(&batch)?;
}
writer.close()?;

// Read
let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
    .with_batch_size(8192)
    .build()?;
for batch in reader {
    process(batch?);
}
```

## Lessons Learned

After building our entire system on Arrow, here are our key takeaways:

### 1. Design for Arrow from the Start

Retrofitting Arrow into an existing system is painful. We designed every data flow around `RecordBatch` from day one.

### 2. Trust the Reference Counting

`Arc::clone()` is nearly free. Don't pre-optimize by trying to pass references everywhere—let Arrow's reference counting handle sharing.

### 3. Use Arrow Compute Kernels

The kernels are SIMD-optimized and handle edge cases (nulls, type coercion) correctly. Rolling your own loops is slower and buggier.

### 4. Streaming is Key

Always work with `SendableRecordBatchStream`, not `Vec<RecordBatch>`. Collecting entire datasets into memory defeats Arrow's streaming benefits.

```rust
// Good: Streaming
let stream = query_result.execute()?;
while let Some(batch) = stream.next().await {
    process(batch?)?;
}

// Bad: Materializing everything
let all_batches: Vec<RecordBatch> = stream.try_collect().await?;
```

### 5. Schema Metadata Matters

Arrow schema metadata is powerful for passing information through the query engine:

```rust
let mut metadata = HashMap::new();
metadata.insert("spice.source".to_string(), "postgresql".to_string());
metadata.insert("spice.accelerated".to_string(), "true".to_string());

let schema = Schema::new_with_metadata(fields, metadata);
```

### 6. Watch for Dictionary Encoding

Dictionary-encoded arrays (for low-cardinality strings) require special handling in IPC:

```rust
let encoder = FlightDataEncoderBuilder::new()
    .with_dictionary_handling(DictionaryHandling::Hydrate)  // or Resend
    .build(stream);
```

---

## Conclusion

Apache Arrow is more than a library—it's an architectural decision. By building Spice on Arrow, we get zero-copy data sharing, seamless integration with the data ecosystem, and SIMD-optimized compute kernels.

The key insight: Arrow eliminates the serialization tax at every system boundary. Data flows through connectors, query engines, and accelerators without transformation, enabling the performance that modern data applications demand.

---

## References

- [Apache Arrow Specification](https://arrow.apache.org/docs/format/Columnar.html)
- [Apache Arrow Rust Implementation](https://arrow.apache.org/rust/)
- [Arrow Flight Protocol](https://arrow.apache.org/docs/format/Flight.html)
- [arrow-rs GitHub](https://github.com/apache/arrow-rs)
- [DataFusion Query Engine](https://datafusion.apache.org/)

