# How We Built Real-Time Control Plane Acceleration with DynamoDB Streams

At Spice AI, we build infrastructure that accelerates data access for AI applications. Recently, a customer came to us with a challenging architecture problem: they needed to synchronize control plane configuration data to thousands of data plane nodes with sub-second latency. This post walks through the problem they faced and how we built DynamoDB Streams support to solve it.

> **"It was too easy."** — First feedback from our customer after deploying DynamoDB Streams with Spice

## The Problem: Decoupling Data Plane from OLTP

Our customer was building a new data processing platform. Their architecture had two distinct layers:

**Control plane**: An OLTP application backed by DynamoDB where customers configure how their data pipelines behave. This is a single-table DynamoDB design holding all configuration data.

**Data plane**: Thousands of processing nodes that need access to this configuration data with single-digit millisecond latency. When a customer updates their configuration, it needs to reflect in the data pipeline within a couple of seconds.

Their initial approach used a multi-tiered caching setup: each data plane node ran a daemon with an in-memory LRU cache backed by DAX and DynamoDB. This had several problems:

- **Cold start penalty**: On cache miss or node startup, requests had to traverse the network to DAX or DynamoDB, adding latency
- **Coupling**: Data plane nodes were directly coupled to the OLTP database—cache misses meant queries hitting DynamoDB
- **TTL tuning hell**: They had to carefully balance cache TTLs between keeping hot data local and propagating configuration changes quickly

What they really wanted was to **decouple the data plane entirely** from DynamoDB. Instead of falling back to the source database on cache miss, they wanted the complete dataset accelerated locally on each node.

## The Target Architecture

We designed a two-tiered Spice architecture:

```
┌─────────────────┐     ┌─────────────────┐
│  OLTP App       │────▶│   DynamoDB      │
└─────────────────┘     └────────┬────────┘
                                 │ DynamoDB Streams
                                 ▼
                        ┌─────────────────┐
                        │  Central Spice  │ ◀── Consumes CDC, builds
                        │     Layer       │     accelerated dataset
                        └─────────────────┘
                                 ▲
                                 │ Pull (periodic refresh)
                    ┌────────────┼────────────┐
                    │            │            │
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │  Spice   │ │  Spice   │ │  Spice   │
              │  Daemon  │ │  Daemon  │ │  Daemon  │
              │(SQLite/  │ │(SQLite/  │ │(SQLite/  │
              │ DuckDB)  │ │ DuckDB)  │ │ DuckDB)  │
              └──────────┘ └──────────┘ └──────────┘
                  │            │            │
              Data Plane   Data Plane   Data Plane
                Node 1       Node 2       Node N
```

The central Spice layer consumes DynamoDB Streams and maintains a near-real-time accelerated dataset. Each data plane node runs a local Spice daemon with SQLite or DuckDB that syncs from the central layer. Data plane processes read from localhost—no network egress, no coupling to DynamoDB.

The key requirements:
- Scale to thousands of data plane nodes
- Single-digit millisecond read latency from local storage
- Sub-second replication from DynamoDB to accelerated datasets
- Fast cold start—new nodes need data within seconds

This meant we needed robust DynamoDB Streams support with reliable bootstrapping and checkpointing.

## DynamoDB Streams vs Kinesis

DynamoDB offers two change capture options. We evaluated both:

**DynamoDB Streams** provides exactly-once delivery with strict ordering within each shard. Records arrive in write order with no duplicates.

**Kinesis Data Streams** can deliver duplicates and doesn't guarantee ordering, requiring deduplication logic on every message.

For keeping accelerated tables in sync, exactly-once delivery was decisive. We didn't want deduplication overhead, and the 24-hour retention is sufficient since we checkpoint continuously. The trade-offs (shorter retention, fewer consumers) were acceptable for this use case.

## Bootstrapping: The Checkpoint-First Approach

When connecting a DynamoDB table to Spice, we need to load current state before consuming changes. This is trickier than it sounds.

### The Problem with LATEST Iterators

The naive approach: get a `LATEST` iterator for each shard, scan the table, start consuming. But DynamoDB Streams iterators expire after 15 minutes. If your table takes longer to scan, your iterators are gone.

Buffering changes during scan has problems too. For high-throughput tables, you could exhaust memory. For idle streams, you might never receive a message to establish position.

### Our Solution: Checkpoint First, Scan Second

1. **Create a checkpoint at the current stream position** — Walk all shards and record their sequence numbers
2. **Scan the entire table** — Load all existing rows
3. **Subscribe using the checkpoint from step 1** — Start consuming from the recorded position

```rust
let (should_bootstrap, checkpoint) =
    load_or_initialize_checkpoint(&dynamodb, &dataset_name).await?;

if should_bootstrap {
    let bootstrap_stream = Arc::clone(&dynamodb)
        .bootstrap_stream()
        .await
        .map(move |msg| {
            msg.map(|change_batch| {
                ChangeEnvelope::new(Box::new(NoOpCommitter), change_batch, false)
            })
        });
```

After bootstrap completes, we commit the checkpoint and start the changes stream:

```rust
bootstrap_stream
    .chain(
        stream::once(async move {
            let committer = DynamoDBStreamCommitter::new(checkpoint_cloned);
            if let Err(err) = committer.commit() {
                tracing::error!("Failed to commit bootstrap checkpoint: {:?}", err);
            }
            stream::empty()
        })
        .flatten()
    )
    .chain(changes_stream_from_checkpoint(&dynamodb, &checkpoint))
```

### The Time Travel Trade-off

The checkpoint points to a moment *before* the scan completes. Some changes during the scan will replay afterward. The table can briefly "go back in time"—a row might update to an older value before catching up.

We mitigate this by not marking the dataset "ready" until stream lag drops below a threshold (default 2 seconds). Downstream consumers only see the dataset once it's caught up.

This approach works for any table regardless of size or throughput. No dependence on receiving messages within a window, no unbounded memory buffering.

## Cold Start and Snapshotting

For the customer's use case, cold start performance was critical. New data plane nodes need to spin up with data ready in seconds, not minutes.

Our solution: snapshot the accelerated dataset to object storage with the checkpoint embedded.

```
         │ Periodic snapshot (including checkpoint)
         ▼
    ┌─────────┐
    │   S3    │
    └─────────┘
         ▲
         │ Download on startup
┌─────────────────┐
│  New Spice      │ ──▶ Resume CDC from checkpoint
│  Daemon Node    │
└─────────────────┘
```

When a new node starts:
1. Download the latest snapshot from S3
2. Read the embedded watermark
3. Resume the CDC stream from that position

This gets nodes operational in seconds rather than re-scanning the entire source table. For a dataset of a few gigabytes, startup time drops from minutes to single-digit seconds.

## Shard Management with a Pure State Machine

DynamoDB Streams organizes data into shards with parent-child relationships. You must fully process a parent before reading children to maintain ordering.

We modeled this as a state machine:

```rust
pub struct StreamState {
    // Ready to poll, have iterators, participate in checkpoints
    active: HashMap<String, ActiveShard>,
    // Have checkpoint but no iterator (expired or initializing)
    initializing: HashMap<String, InitializingShard>,
    // Blocked by unfinished parent
    blocked: HashMap<String, BlockedShard>,
    // Historical record of all seen shards
    historical: HashMap<String, HistoricalShard>,
}
```

The key insight: keep state transitions pure. All transitions happen through methods that take input and return results without external API calls:

```rust
pub fn handle_poll_result(
    &mut self,
    shard_id: &str,
    new_iterator: Option<String>,
    records: Vec<Record>,
) -> Result<ShardPollResult> {
    // Process records, update checkpoint and watermark
    
    if let Some(iter) = new_iterator {
        self.active.get_mut(shard_id)?.update_iterator(iter);
    } else {
        // Shard exhausted
        self.active.remove(shard_id);
        self.promote_children(shard_id);
    }
}
```

When a shard exhausts, we promote its children from blocked to initializing. This separation means we can test every state transition without mocking AWS.

## Schema Handling for Single-Table Designs

The customer uses a single-table DynamoDB design—a common NoSQL pattern where different entity types share one table, distinguished by partition and sort key patterns. The schema evolves over time as new entity types are added.

Our initial connector inferred schema from the first 10 items scanned. This doesn't work for single-table designs where different items have different attributes.

The solution: let users define a minimal relational schema with explicit columns for partition key, sort key, and any fields needed for filtering or indexing. Everything else becomes a JSON blob:

```
┌──────────────┬──────────────┬─────────────────────┐
│ partition_key│ sort_key     │ data (JSON)         │
├──────────────┼──────────────┼─────────────────────┤
│ USER#123     │ PROFILE      │ {"name": "...", ...}│
│ USER#123     │ CONFIG#abc   │ {"setting": ...}    │
│ PIPELINE#456 │ META         │ {"status": ...}     │
└──────────────┴──────────────┴─────────────────────┘
```

This gives users:
- Predicate pushdown on partition and sort keys
- Ability to add indexed columns for frequently filtered fields
- Application-level control over JSON schema versioning
- Stable accelerated schema even as DynamoDB items evolve

The application handles unmarshalling the JSON blob, including any schema version handling needed.

## Error Handling: Transient vs Fatal

Errors fall into two categories:

```rust
pub enum Error {
    // Permanent - require intervention
    TableNotFound,
    StreamNotFound,
    StreamBeyondRetention,

    // Retriable - resolve with retry
    Timeout,
    ConnectionFailure,
    Throttled,

    // Special handling
    IteratorExpired,
}
```

Iterator expiration needs special treatment. DynamoDB Streams iterators expire after 15 minutes of inactivity. You can't retry with the same iterator—you need a new one from your last checkpoint:

```rust
pub fn handle_poll_error(&mut self, shard_id: &str, error: Error) -> Result<ShardPollResult> {
    if error.is_retriable() {
        tracing::warn!("Poll error for shard {}, will retry: {}", shard_id, error);
    } else if matches!(error, Error::IteratorExpired) {
        tracing::warn!("Iterator expired for shard {}, reinitializing", shard_id);
        self.reinitialize_shard_with_checkpoint(shard_id);
    } else {
        return Err(error);
    }
}
```

For transient errors, exponential backoff with a 60-second cap prevents thundering herds while recovering quickly from brief network issues.

## Watermarks and Dataset Readiness

To track how far behind real-time we are, we use watermarks based on each record's `approximate_creation_date_time`. The minimum watermark across active shards indicates global progress:

```rust
fn combine_shard_batches(poll_results: &[ShardPollResult]) -> DynamoDBStreamBatch {
    let mut shard_watermarks = Vec::new();

    for shard_result in poll_results {
        let is_watermark_eligible = match &shard_result.outcome {
            PollOutcome::Records { .. } => true,
            PollOutcome::Failed => true,  // Failed shards represent unprocessed lag
            PollOutcome::Empty => false,  // Empty shards are caught up
        };

        if is_watermark_eligible {
            if let Some(watermark) = shard_result.current_watermark {
                shard_watermarks.push(watermark);
            }
        }
    }

    let watermark = shard_watermarks.into_iter().min()
        .unwrap_or_else(SystemTime::now);
}
```

This watermark drives dataset readiness. A dataset is marked ready when lag drops below the threshold:

```rust
ChangeEnvelope::new(
    Box::new(committer),
    change_batch,
    lag.is_some_and(|l| l < acceptable_lag),  // Ready signal
)
```

For the customer's use case, this means data plane processes don't see the local dataset until it's within 2 seconds of real-time—no stale reads during catch-up.

## Checkpointing for Reliability

Checkpoints capture sequence number positions for each shard:

```rust
pub struct ShardCheckpoint {
    pub sequence_number: String,
    pub parent_id: Option<String>,
    pub updated_at: SystemTime,
    pub position: CheckpointPosition,
}

pub enum CheckpointPosition {
    At,    // Resume AT this sequence (inclusive) - not yet processed
    After, // Resume AFTER this sequence (exclusive) - already processed
}
```

On recovery, we resume from leaf shards only—those with no children in the checkpoint. Parents are already exhausted:

```rust
pub fn leaf_shards(&self) -> Vec<(&String, &ShardCheckpoint)> {
    let parent_ids: HashSet<&str> = self.shards.values()
        .filter_map(|sc| sc.parent_id.as_deref())
        .collect();

    self.shards.iter()
        .filter(|(shard_id, _)| !parent_ids.contains(shard_id.as_str()))
        .collect()
}
```

Checkpoints serialize as JSON to Spice's file-accelerated storage, enabling reliable resume after restarts.

## Scaling to Thousands of Nodes

With thousands of data plane nodes, having each consume directly from DynamoDB Streams isn't realistic. The central Spice layer acts as a fan-out point.

Edge nodes poll the central layer on a configurable interval using our append refresh strategy. This scales well—each edge node independently pulls updates without coordinating with others. Nodes can also filter to pull only relevant partitions, reducing data transfer for deployments where different node pools need different data subsets.

For the customer's use case, different teams had different requirements: one had fewer nodes but larger datasets, while the other had smaller datasets across more nodes. The pull-based architecture handles both patterns efficiently.

## Metrics and Monitoring

DynamoDB Streams lacks built-in lag metrics. We built our own:

```rust
pub struct MetricsCollector {
    pub active_shards_number: RwLock<usize>,
    pub records: AtomicUsize,
    pub transient_errors: AtomicUsize,
    pub watermark: RwLock<Option<SystemTime>>,
}
```

Exposed through OpenTelemetry:
- `shards_active` — current active shards being polled
- `records_consumed_total` — total records since startup
- `lag_ms` — current lag from watermark to wall clock
- `errors_transient_total` — recoverable error count

The lag metric is especially important for the customer's SLA—they need to verify configuration changes propagate within seconds.

## Configuration: A Complete Example

One of our design principles is making everything as easy as possible for developers. Here's a complete Spicepod configuration that implements the architecture described above:

```yaml
version: v1
kind: Spicepod
name: dynamodb-streams-demo

snapshots:
  enabled: true
  location: s3://<path>
  bootstrap_on_failure_behavior: fallback 
  params:
    s3_auth: key
    s3_key: ${secrets:AWS_ACCESS_KEY_ID}
    s3_secret: ${secrets:AWS_SECRET_ACCESS_KEY}
    s3_region: us-east-2

datasets:
  - from: dynamodb:<table>
    name: <table>
    params:
      dynamodb_aws_region: ap-northeast-2
      dynamodb_aws_auth: iam_role
    acceleration:
      enabled: true
      refresh_mode: changes
      engine: duckdb
      mode: file
      snapshots: enabled
      snapshots_trigger: time_interval
      snapshots_trigger_threshold: 2m
    metrics:
      - name: shards_active
      - name: records_consumed_total
      - name: lag_ms
      - name: errors_transient_total
```

This configuration:
- Points Spice at a DynamoDB table with IAM role authentication
- Enables change data capture via `refresh_mode: changes`
- Accelerates to a file-backed DuckDB for persistence across restarts
- Snapshots every 2 minutes to S3 for fast cold start on new nodes
- Falls back to bootstrap from source if snapshot loading fails
- Exposes all four key metrics for monitoring

That's it. No custom CDC consumers to build, no checkpoint management code to write, no shard tracking logic to maintain. Point it at your table and start querying.

When our customer first deployed this configuration, their feedback was immediate: **"It was too easy."** They had expected weeks of integration work. Instead, they had real-time DynamoDB synchronization running in an afternoon.

## Lessons Learned

Building this taught us several lessons:

1. **Choose abstractions that match your guarantees.** DynamoDB Streams' exactly-once delivery saved us from deduplication complexity. The "simpler" option with fewer features was actually less work.

2. **Bootstrap carefully.** The checkpoint-first approach handles edge cases that naive strategies miss—large tables, idle streams, memory constraints. Temporary "time travel" during catch-up is an acceptable trade-off.

3. **Pure state machines pay off immediately.** Separating state transitions from I/O made shard management testable and easy to reason about.

4. **Build observability from day one.** Without AWS-provided lag metrics, we built our own. Having watermarks and lag tracking from the start made debugging and operations much easier.

5. **Design for the scaling requirements.** The two-tier architecture with push/pull flexibility handles both the "few nodes, large data" and "many nodes, small data" patterns the customer needed.

The architecture is extensible. If we need Kinesis Data Streams support for longer retention, the core state machine and checkpointing logic can be reused with a deduplication layer on top.
