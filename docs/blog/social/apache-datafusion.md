# Apache DataFusion

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

**Apache DataFusion: Building Query Engines Through Extension**

A complete SQL query engine requires: parser, logical planner, optimizer, physical planner, and execution engine. Building this from scratch takes years.

Apache DataFusion provides all of these components—and makes every one extensible. It's not a query engine you use; it's a query engine you extend.

```
┌─────────────────────────────────────────────────────────────────┐
│              DATAFUSION: THE EXTENSION ARCHITECTURE              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   THE FULL QUERY PIPELINE:                                       │
│                                                                  │
│   SQL Query                                                      │
│       │                                                          │
│       ▼                                                          │
│   ┌───────────────┐                                              │
│   │    Parser     │ ← Built-in: PostgreSQL dialect              │
│   └───────────────┘   Handles SQL syntax, operator precedence   │
│           │                                                      │
│           ▼                                                      │
│   ┌───────────────┐                                              │
│   │Logical Planner│ ← Extension Point: TableProvider            │
│   └───────────────┘   Your data sources become queryable tables │
│           │           Implement: schema(), scan()                │
│           ▼                                                      │
│   ┌───────────────┐                                              │
│   │   Optimizer   │ ← Extension Point: OptimizerRule            │
│   └───────────────┘   Add custom optimization passes            │
│           │           Examples: federation pushdown, caching    │
│           ▼                                                      │
│   ┌───────────────┐                                              │
│   │Physical Planner│ ← Extension Point: PhysicalOptimizerRule   │
│   └───────────────┘   Custom execution strategies                │
│           │           Examples: index scans, specialized joins   │
│           ▼                                                      │
│   ┌───────────────┐                                              │
│   │   Execution   │ ← Extension Point: ExecutionPlan            │
│   └───────────────┘   Custom operators that produce Arrow       │
│           │                                                      │
│           ▼                                                      │
│   Arrow RecordBatches                                            │
│                                                                  │
│   ┌───────────────┐                                              │
│   │   Functions   │ ← Extension Point: ScalarUDF, AggregateUDF  │
│   └───────────────┘   Custom SQL functions                       │
│                       Examples: ai_inference(), geo_distance()   │
│                                                                  │
│   EVERY BOX IS A TRAIT. EVERY TRAIT CAN BE IMPLEMENTED.         │
└─────────────────────────────────────────────────────────────────┘
```

**Key extension points explained:**

**TableProvider**: The interface between DataFusion and your data. Implement this trait, and any data source becomes a SQL table. The contract: given filters and projections, return a stream of Arrow RecordBatches.

**OptimizerRule**: Add passes to the query optimizer. Use cases: detect patterns and rewrite them (e.g., push entire subtrees to remote sources), enforce policies (e.g., row-level security), add caching hints.

**PhysicalOptimizerRule**: Optimize the physical execution plan. Use cases: replace table scans with index scans when indexes exist, add parallelism hints, inject custom operators.

**ExecutionPlan**: Custom operators in the execution tree. Use cases: specialized join algorithms, external system calls, custom I/O patterns.

**ScalarUDF / AggregateUDF / TableFunction**: Custom SQL functions. `SELECT my_function(column) FROM table`. The function is just Rust code that operates on Arrow arrays.

**What DataFusion handles for you:**

- SQL parsing with PostgreSQL dialect
- Type checking and validation
- Cost-based optimization (join reordering, predicate pushdown)
- Parallel execution across CPU cores
- Memory management and spilling
- Arrow-native processing (vectorized, cache-efficient)

**Typical extension patterns:**

1. **Data connector**: Implement TableProvider for your data source (database, API, file format)
2. **Query federation**: Add OptimizerRule to push computation to remote sources
3. **AI integration**: Register UDFs that call ML models
4. **Custom acceleration**: Add PhysicalOptimizerRule to use your indexes/caches

From experience: we estimated 18 months to build a query engine from scratch. With DataFusion, we shipped in 3 months—implementing TableProviders for 15+ sources, custom optimizer rules for federation, and AI inference UDFs. The engine's foundation came from DataFusion; we focused on what made our system unique.

---

## X

DataFusion: the query engine you extend, not replace

Full pipeline:
SQL → Parser → Logical Plan → Optimizer → Physical Plan → Execution → Arrow

Every stage extensible:

| Stage             | Extension Point             |
| ----------------- | --------------------------- |
| Logical Planning  | TableProvider, AnalyzerRule |
| Optimization      | OptimizerRule               |
| Physical Planning | ExtensionPlanner            |
| Execution         | ExecutionPlan               |
| Functions         | ScalarUDF, TableFunction    |

Key implementation patterns:

```rust
// AcceleratedTable → FederatedTable → Connector
// Wraps source with local cache + refresh + fallback
```

Performance built-in:
- Vectorized Arrow processing
- Push-based streaming execution
- Partition-aware parallelism
- Filter/projection pushdown

PostgreSQL dialect for familiar SQL.
Case-sensitive identifiers preserved.
Separate IO runtime to avoid blocking handlers.

Write the domain logic. DataFusion handles execution.
