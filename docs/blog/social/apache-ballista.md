# Apache Ballista

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

Apache Ballista: Distributed SQL Execution Built on DataFusion and Arrow

Single-node query engines have a ceiling. When your data lives in thousands of Parquet files across object storage, or when analytical queries need to scan hundreds of gigabytes, one process isn't enough.

Apache Ballista extends Apache DataFusion into a distributed query engine. Same SQL. Same Arrow format. Same extensibility. But now across a cluster of machines.

Here's the architecture:

```
        SQL Query
            │
            ▼
    ┌──────────────┐
    │   Scheduler   │   Plans query, breaks into stages, assigns tasks
    └──────────────┘
      │       │       │
      ▼       ▼       ▼
   ┌─────┐ ┌─────┐ ┌─────┐
   │Exec1│ │Exec2│ │Exec3│  Execute stages, exchange shuffle data
   └─────┘ └─────┘ └─────┘
      │       │       │
      └───────┼───────┘
              ▼
       Arrow Results
```

The scheduler accepts a SQL query, runs it through DataFusion's planner and optimizer, then breaks the physical plan into stages separated by shuffle boundaries—repartitions, aggregations, joins. Each stage becomes a set of tasks distributed across executor workers.

What makes Ballista different from writing your own distributed layer on top of DataFusion:

→ Disk-based shuffle. Intermediate data between stages is persisted to disk. If a late stage fails, you retry from the intermediate data—not from scratch. For queries scanning terabytes, this is the difference between a 30-second retry and re-reading everything from S3.

→ Stage-based execution. Queries decompose into a DAG of stages. Each stage runs as independent tasks on executors. The scheduler tracks task completion and triggers downstream stages when dependencies are met.

→ Arrow Flight data transport. Executors exchange data using Arrow Flight RPC—the same wire format as the in-memory format. No serialization between stages. Just Arrow RecordBatches flowing over gRPC.

→ DataFusion native. Ballista uses DataFusion's LogicalPlan, PhysicalPlan, and SessionState directly. Your custom TableProviders, optimizer rules, and UDFs work in distributed mode without modification.

→ Dynamic executor pool. The scheduler's work queue model allows executors to join or leave during query execution. Scale up for a heavy query, scale down when idle.

Published TPC-H SF100 (100GB) benchmarks show a 2.9x overall speedup versus single-node DataFusion, with memory usage 5-10x lower than Apache Spark.

The key design decision: Ballista chose reliability over minimal latency. The disk-based shuffle adds overhead compared to fully in-memory approaches, but it means failed stages don't restart the entire query. For analytical workloads (seconds to minutes), this tradeoff is correct. For sub-second queries, stay single-node.

When to consider Ballista:

• Data exceeds single-node memory (100GB+ Parquet scans)
• Long-running analytical queries that need fault tolerance
• Batch inference workloads (LLM/ML over large datasets)
• You already use DataFusion and need horizontal scaling

When to stay single-node:

• Sub-second latency requirements
• Data fits in memory with acceleration (DuckDB, local caching)
• Simple queries that don't benefit from parallelism

Ballista is an Apache Software Foundation project with 346 contributors, production-used at companies including Apple and Coralogix (which maintains a fork with 65+ releases). It's the natural scale-out path for any system built on DataFusion.

The broader trend: query engines are becoming composable. Arrow for the format. DataFusion for single-node execution. Ballista for distribution. Flight for transport. Each layer is a library you compose, not a monolith you adopt.

### First Reply (with links)

References:

• Apache DataFusion Ballista: https://github.com/apache/datafusion-ballista
• Ballista Architecture: https://datafusion.apache.org/ballista/contributors-guide/architecture.html
• Ballista User Guide: https://datafusion.apache.org/ballista/
• Apache DataFusion: https://datafusion.apache.org/
• Apache Arrow Flight: https://arrow.apache.org/docs/format/Flight.html
• Coralogix Ballista Fork: https://github.com/coralogix/arrow-ballista

---

## X (5 posts, 280 characters each)

Post 1:
Apache Ballista: distributed SQL execution built on DataFusion + Arrow. Same query engine, same format, but across a cluster. Scheduler plans queries, breaks them into stages, distributes tasks to executor workers. TPC-H SF100: 2.9x speedup vs single-node.

Post 2:
Ballista's key design: disk-based shuffle between stages. If stage 5 of 6 fails, retry from intermediate data—not from scratch. For 100GB+ scans, this is the difference between 30 seconds and re-reading everything from S3.

Post 3:
How Ballista distributes a query: SQL → DataFusion planner → physical plan → break at shuffle boundaries → assign stages to executors → exchange data via Arrow Flight. Your custom TableProviders and UDFs work without changes.

Post 4:
When to use Ballista: data exceeds single-node memory, long-running analytical/inference queries, you already use DataFusion. When to stay single-node: sub-second latency, data fits in memory, simple queries that don't benefit from parallelism.

Post 5:
Query engines are becoming composable: Arrow for format. DataFusion for single-node SQL. Ballista for distribution. Flight for transport. Each is a library you compose, not a monolith you adopt. The unbundled data stack is here.
