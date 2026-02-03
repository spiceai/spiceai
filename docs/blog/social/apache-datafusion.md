# Apache DataFusion

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

Apache DataFusion: Building Query Engines Through Extension

A complete SQL query engine requires: parser, logical planner, optimizer, physical planner, and execution engine. Building this from scratch takes years.

Apache DataFusion provides all of these components—and makes every one extensible. It's not a query engine you use; it's a query engine you extend.

The query pipeline flows from SQL through Parser, Logical Planner, Optimizer, Physical Planner, and Execution—outputting Arrow RecordBatches. Every stage is extensible.

Key extension points:

→ TableProvider: The interface between DataFusion and your data. Implement this trait, and any data source becomes a SQL table. The contract: given filters and projections, return a stream of Arrow RecordBatches.

→ OptimizerRule: Add passes to the query optimizer. Use cases: detect patterns and rewrite them (e.g., push entire subtrees to remote sources), enforce policies (e.g., row-level security), add caching hints.

→ PhysicalOptimizerRule: Optimize the physical execution plan. Use cases: replace table scans with index scans when indexes exist, add parallelism hints, inject custom operators.

→ ExecutionPlan: Custom operators in the execution tree. Use cases: specialized join algorithms, external system calls, custom I/O patterns.

→ ScalarUDF / AggregateUDF / TableFunction: Custom SQL functions. SELECT my_function(column) FROM table. The function is just Rust code that operates on Arrow arrays.

What DataFusion handles for you:
• SQL parsing with PostgreSQL dialect
• Type checking and validation
• Cost-based optimization (join reordering, predicate pushdown)
• Parallel execution across CPU cores
• Memory management and spilling
• Arrow-native processing (vectorized, cache-efficient)

Typical extension patterns:
1. Data connector: Implement TableProvider for your data source (database, API, file format)
2. Query federation: Add OptimizerRule to push computation to remote sources
3. AI integration: Register UDFs that call ML models
4. Custom acceleration: Add PhysicalOptimizerRule to use your indexes/caches

From experience: we estimated 18 months to build a query engine from scratch. With DataFusion, we shipped in 3 months—implementing TableProviders for 15+ sources, custom optimizer rules for federation, and AI inference UDFs. The engine's foundation came from DataFusion; we focused on what made our system unique.

---

## X (5 posts, 280 characters each)

Post 1:
DataFusion: the query engine you extend, not replace. Full SQL pipeline built-in: parser, planner, optimizer, execution. Every stage is a trait you can implement. Build a query engine in months, not years.

Post 2:
Extension points in DataFusion: TableProvider (your data sources become SQL tables), OptimizerRule (custom optimization passes), ExecutionPlan (custom operators). The engine handles parsing, type checking, parallelism.

Post 3:
TableProvider is the key abstraction. Implement schema() and scan(). Return Arrow RecordBatches. Any data source becomes queryable: databases, APIs, file formats. DataFusion handles the SQL layer.

Post 4:
Performance comes free: vectorized Arrow processing, push-based streaming, partition-aware parallelism, filter/projection pushdown. PostgreSQL dialect. You write domain logic, DataFusion handles execution.

Post 5:
We estimated 18 months to build a query engine from scratch. With DataFusion: 3 months. 15+ data sources, federation rules, AI inference UDFs. The foundation was there; we focused on what made us unique.
