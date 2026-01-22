# Spice Distributed Nodes Grafana Dashboard Plan

## Overview

This document specifies a comprehensive Grafana dashboard for monitoring Spice's Ballista-based distributed query execution. The dashboard provides visibility into distributed node health, scheduler performance, executor utilization, shuffle efficiency, and error tracking.

**Data Source**: Prometheus (scraping `/metrics?scope=cluster` endpoint)

**Refresh Rate**: 10s (recommended for operational monitoring)

---

## Dashboard Variables

Define these template variables for flexible filtering:

| Variable | Label | Query | Multi | Include All |
|----------|-------|-------|-------|-------------|
| `$node` | Node | `label_values(node_status, node_id)` | Yes | Yes |
| `$role` | Role | `scheduler,executor` (custom) | Yes | Yes |
| `$interval` | Interval | `$__auto_interval_interval` (auto) | No | No |

---

## Dashboard Layout

### Row 1: Distributed Overview

**Collapsed by default**: No

#### Panel 1.1: Distributed Health Status
- **Type**: Stat
- **Description**: Overall distributed system health indicator
- **Query**:
  ```promql
  min(node_status{node_id=~"$node"})
  ```
- **Value mappings**:
  - 0 → "Unknown" (gray)
  - 1 → "Healthy" (green)
  - 2 → "Unhealthy" (red)
  - 3 → "Draining" (yellow)
- **Thresholds**: 1=green, 2=red, 3=yellow

#### Panel 1.2: Active Nodes
- **Type**: Stat (2 values side-by-side)
- **Description**: Count of schedulers and executors
- **Queries**:
  ```promql
  # Schedulers
  count(node_status{role="scheduler", node_id=~"$node"} == 1)
  
  # Executors
  sum(scheduler_active_executors_count{node_id=~"$node"})
  ```

#### Panel 1.3: Task Throughput (Distributed)
- **Type**: Stat
- **Description**: Tasks completed per second across distributed nodes
- **Query**:
  ```promql
  sum(rate(node_tasks_total{status="completed", node_id=~"$node"}[$interval]))
  ```
- **Unit**: tasks/s

#### Panel 1.4: Task Success Rate
- **Type**: Gauge
- **Description**: Percentage of tasks completing successfully
- **Query**:
  ```promql
  100 * (
    sum(rate(node_tasks_total{status="completed", node_id=~"$node"}[$interval])) /
    clamp_min(sum(rate(node_tasks_total{node_id=~"$node"}[$interval])), 0.001)
  )
  ```
- **Unit**: percent (0-100)
- **Thresholds**: 0-90=red, 90-99=yellow, 99-100=green

#### Panel 1.5: Shuffle Locality Ratio
- **Type**: Gauge
- **Description**: Percentage of shuffle reads served locally (higher is better)
- **Query**:
  ```promql
  100 * (
    sum(rate(executor_shuffle_read_local_bytes{node_id=~"$node"}[$interval])) /
    clamp_min(
      sum(rate(executor_shuffle_read_local_bytes{node_id=~"$node"}[$interval])) +
      sum(rate(executor_shuffle_read_remote_bytes{node_id=~"$node"}[$interval])),
      1
    )
  )
  ```
- **Unit**: percent (0-100)
- **Thresholds**: 0-50=red, 50-80=yellow, 80-100=green

---

### Row 2: Scheduler Performance

**Collapsed by default**: No

#### Panel 2.1: Task Queue Depth
- **Type**: Time series
- **Description**: Number of tasks waiting to be scheduled
- **Query**:
  ```promql
  scheduler_task_queue_depth{node_id=~"$node"}
  ```
- **Legend**: `{{node_id}}`
- **Alert threshold**: Consider alerting if > 1000 for > 5m

#### Panel 2.2: Job Queue Depth
- **Type**: Time series
- **Description**: Number of jobs waiting in queue
- **Query**:
  ```promql
  scheduler_job_queue_depth{node_id=~"$node"}
  ```
- **Legend**: `{{node_id}}`

#### Panel 2.3: Task Scheduling Latency
- **Type**: Time series (heatmap or percentiles)
- **Description**: Time from task becoming schedulable to being assigned
- **Queries**:
  ```promql
  # p50
  histogram_quantile(0.50, sum(rate(scheduler_task_scheduling_latency_ms_bucket{node_id=~"$node"}[$interval])) by (le, node_id))
  
  # p95
  histogram_quantile(0.95, sum(rate(scheduler_task_scheduling_latency_ms_bucket{node_id=~"$node"}[$interval])) by (le, node_id))
  
  # p99
  histogram_quantile(0.99, sum(rate(scheduler_task_scheduling_latency_ms_bucket{node_id=~"$node"}[$interval])) by (le, node_id))
  ```
- **Unit**: milliseconds
- **Legend**: `p50 {{node_id}}`, `p95 {{node_id}}`, `p99 {{node_id}}`

#### Panel 2.4: Query Planning Duration
- **Type**: Time series
- **Description**: Time spent planning queries (distributed plan generation)
- **Queries**:
  ```promql
  # p50
  histogram_quantile(0.50, sum(rate(scheduler_planning_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le, node_id))
  
  # p95
  histogram_quantile(0.95, sum(rate(scheduler_planning_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le, node_id))
  ```
- **Unit**: milliseconds

#### Panel 2.5: Executor Assignments Rate
- **Type**: Time series
- **Description**: Rate of task-to-executor assignments
- **Query**:
  ```promql
  sum(rate(scheduler_executor_assignments{node_id=~"$node"}[$interval])) by (node_id)
  ```
- **Unit**: assignments/s
- **Legend**: `{{node_id}}`

---

### Row 3: Stage Execution

**Collapsed by default**: Yes

#### Panel 3.1: Stage Throughput by Status
- **Type**: Time series (stacked)
- **Description**: Rate of stage completions, failures, and cancellations
- **Query**:
  ```promql
  sum(rate(scheduler_stages_total{node_id=~"$node"}[$interval])) by (status)
  ```
- **Legend**: `{{status}}`
- **Colors**: completed=green, failed=red, cancelled=yellow

#### Panel 3.2: Stage Duration
- **Type**: Time series
- **Description**: Time to complete stages
- **Queries**:
  ```promql
  # p50
  histogram_quantile(0.50, sum(rate(scheduler_stage_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le))
  
  # p95
  histogram_quantile(0.95, sum(rate(scheduler_stage_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le))
  
  # p99
  histogram_quantile(0.99, sum(rate(scheduler_stage_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le))
  ```
- **Unit**: milliseconds

#### Panel 3.3: Tasks per Stage Distribution
- **Type**: Heatmap
- **Description**: Distribution of task counts per stage
- **Query**:
  ```promql
  sum(rate(scheduler_tasks_per_stage_bucket{node_id=~"$node"}[$interval])) by (le)
  ```
- **Unit**: tasks

#### Panel 3.4: Stage Retries
- **Type**: Time series
- **Description**: Rate of stage retries (indicates instability)
- **Query**:
  ```promql
  sum(rate(scheduler_stage_retries{node_id=~"$node"}[$interval])) by (node_id)
  ```
- **Unit**: retries/s

---

### Row 4: Executor Performance

**Collapsed by default**: No

#### Panel 4.1: Active Tasks per Executor
- **Type**: Time series
- **Description**: Number of tasks currently executing on each executor
- **Query**:
  ```promql
  executor_tasks_active{node_id=~"$node"}
  ```
- **Legend**: `{{node_id}}`

#### Panel 4.2: Task Execution Duration
- **Type**: Time series
- **Description**: Time to execute individual tasks
- **Queries**:
  ```promql
  # p50
  histogram_quantile(0.50, sum(rate(executor_task_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le, node_id))
  
  # p95
  histogram_quantile(0.95, sum(rate(executor_task_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le, node_id))
  ```
- **Unit**: milliseconds
- **Legend**: `p50 {{node_id}}`, `p95 {{node_id}}`

#### Panel 4.3: Executor Task Throughput
- **Type**: Time series
- **Description**: Tasks completed per second by each executor
- **Query**:
  ```promql
  sum(rate(executor_tasks_total{status="completed", node_id=~"$node"}[$interval])) by (node_id)
  ```
- **Unit**: tasks/s
- **Legend**: `{{node_id}}`

#### Panel 4.4: Executor Memory Available
- **Type**: Time series
- **Description**: Available memory on each executor
- **Query**:
  ```promql
  executor_memory_available_bytes{node_id=~"$node"}
  ```
- **Unit**: bytes (with IEC formatting: GiB, MiB)
- **Legend**: `{{node_id}}`

#### Panel 4.5: Executor Utilization Heatmap
- **Type**: Heatmap or Bar gauge
- **Description**: Relative utilization across executors
- **Query**:
  ```promql
  executor_tasks_active{node_id=~"$node"}
  ```
- **Note**: If max slots per executor is configurable, can show `active / max_slots * 100`

---

### Row 5: Shuffle Data Flow

**Collapsed by default**: No

#### Panel 5.1: Shuffle Write Throughput
- **Type**: Time series
- **Description**: Rate of shuffle data being written
- **Query**:
  ```promql
  sum(rate(executor_shuffle_write_bytes{node_id=~"$node"}[$interval])) by (node_id)
  ```
- **Unit**: bytes/s (with formatting: MB/s, GB/s)
- **Legend**: `{{node_id}}`

#### Panel 5.2: Shuffle Read Throughput (Local vs Remote)
- **Type**: Time series (stacked)
- **Description**: Shuffle read rate, split by local and remote
- **Queries**:
  ```promql
  # Local reads (same executor)
  sum(rate(executor_shuffle_read_local_bytes{node_id=~"$node"}[$interval]))
  
  # Remote reads (network)
  sum(rate(executor_shuffle_read_remote_bytes{node_id=~"$node"}[$interval]))
  ```
- **Unit**: bytes/s
- **Legend**: `Local`, `Remote`
- **Colors**: Local=green, Remote=orange

#### Panel 5.3: Shuffle Locality by Executor
- **Type**: Bar gauge
- **Description**: Local read percentage per executor
- **Query**:
  ```promql
  100 * (
    rate(executor_shuffle_read_local_bytes{node_id=~"$node"}[$interval]) /
    clamp_min(
      rate(executor_shuffle_read_local_bytes{node_id=~"$node"}[$interval]) +
      rate(executor_shuffle_read_remote_bytes{node_id=~"$node"}[$interval]),
      1
    )
  )
  ```
- **Unit**: percent
- **Thresholds**: 0-50=red, 50-80=yellow, 80-100=green

#### Panel 5.4: Shuffle Read Latency (Local vs Remote)
- **Type**: Time series
- **Description**: Compare latency of local vs remote shuffle reads
- **Queries**:
  ```promql
  # Local p95
  histogram_quantile(0.95, sum(rate(executor_shuffle_read_local_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le))
  
  # Remote p95
  histogram_quantile(0.95, sum(rate(executor_shuffle_read_remote_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le))
  ```
- **Unit**: milliseconds
- **Legend**: `Local p95`, `Remote p95`

#### Panel 5.5: Shuffle Rows Processed
- **Type**: Time series
- **Description**: Rate of rows being shuffled (written and read)
- **Queries**:
  ```promql
  sum(rate(executor_shuffle_write_rows{node_id=~"$node"}[$interval]))
  sum(rate(executor_shuffle_read_local_rows{node_id=~"$node"}[$interval])) + sum(rate(executor_shuffle_read_remote_rows{node_id=~"$node"}[$interval]))
  ```
- **Unit**: rows/s
- **Legend**: `Written`, `Read`

---

### Row 6: Result Collection

**Collapsed by default**: Yes

#### Panel 6.1: Result Fetch Throughput
- **Type**: Time series
- **Description**: Rate of final result data being fetched by scheduler
- **Query**:
  ```promql
  sum(rate(scheduler_result_fetch_bytes{node_id=~"$node"}[$interval])) by (node_id)
  ```
- **Unit**: bytes/s
- **Legend**: `{{node_id}}`

#### Panel 6.2: Result Fetch Latency
- **Type**: Time series
- **Description**: Time to fetch final results from executors
- **Queries**:
  ```promql
  # p50
  histogram_quantile(0.50, sum(rate(scheduler_result_fetch_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le))
  
  # p95
  histogram_quantile(0.95, sum(rate(scheduler_result_fetch_duration_ms_bucket{node_id=~"$node"}[$interval])) by (le))
  ```
- **Unit**: milliseconds

#### Panel 6.3: Result Fetch Operations
- **Type**: Stat
- **Description**: Total result fetch operations
- **Query**:
  ```promql
  sum(rate(scheduler_result_fetch_count{node_id=~"$node"}[$interval]))
  ```
- **Unit**: ops/s

---

### Row 7: Errors & Reliability

**Collapsed by default**: No

#### Panel 7.1: Task Failures by Error Type
- **Type**: Time series (stacked)
- **Description**: Rate of task failures categorized by error type
- **Query**:
  ```promql
  sum(rate(node_task_failures{node_id=~"$node"}[$interval])) by (error_type)
  ```
- **Legend**: `{{error_type}}`
- **Colors**: All red variants

#### Panel 7.2: Task Failures by Node
- **Type**: Time series
- **Description**: Task failure rate per node
- **Query**:
  ```promql
  sum(rate(node_task_failures{node_id=~"$node"}[$interval])) by (node_id, role)
  ```
- **Legend**: `{{role}}: {{node_id}}`

#### Panel 7.3: Task Retries
- **Type**: Time series
- **Description**: Rate of task retries
- **Query**:
  ```promql
  sum(rate(node_task_retries{node_id=~"$node"}[$interval])) by (node_id, role)
  ```
- **Unit**: retries/s
- **Legend**: `{{role}}: {{node_id}}`

#### Panel 7.4: Stage Failures by Error Type
- **Type**: Time series (stacked)
- **Description**: Rate of stage failures by error type
- **Query**:
  ```promql
  sum(rate(scheduler_stage_failures{node_id=~"$node"}[$interval])) by (error_type)
  ```
- **Legend**: `{{error_type}}`

#### Panel 7.5: Executor Task Failures
- **Type**: Bar gauge (last value)
- **Description**: Total executor task failures in time range
- **Query**:
  ```promql
  sum(increase(executor_task_failures{node_id=~"$node"}[$__range])) by (node_id, error_type)
  ```
- **Legend**: `{{node_id}} - {{error_type}}`

---

### Row 8: Node Details (Drill-down)

**Collapsed by default**: Yes

#### Panel 8.1: Node Status Table
- **Type**: Table
- **Description**: Status of all nodes with key metrics
- **Queries** (merge into table):
  ```promql
  # Status
  node_status{node_id=~"$node"}
  
  # Active tasks
  node_tasks_active{node_id=~"$node"}
  
  # Memory (executors only)
  executor_memory_available_bytes{node_id=~"$node"}
  ```
- **Columns**: Node ID, Role, Status, Active Tasks, Memory Available

#### Panel 8.2: Per-Node Task Completion Rate
- **Type**: Bar gauge
- **Description**: Task completion rate by node
- **Query**:
  ```promql
  sum(rate(node_tasks_total{status="completed", node_id=~"$node"}[$interval])) by (node_id)
  ```
- **Unit**: tasks/s

#### Panel 8.3: Per-Executor Shuffle Write
- **Type**: Bar gauge
- **Description**: Shuffle write rate per executor
- **Query**:
  ```promql
  rate(executor_shuffle_write_bytes{node_id=~"$node"}[$interval])
  ```
- **Unit**: bytes/s

---

## Alert Rules

### Critical Alerts

| Alert | Expression | For | Severity |
|-------|------------|-----|----------|
| DistributedUnhealthy | `min(node_status) != 1` | 2m | critical |
| NoActiveExecutors | `sum(scheduler_active_executors_count) == 0` | 1m | critical |
| HighTaskFailureRate | `sum(rate(node_task_failures[5m])) / sum(rate(node_tasks_total[5m])) > 0.1` | 5m | critical |

### Warning Alerts

| Alert | Expression | For | Severity |
|-------|------------|-----|----------|
| TaskQueueBacklog | `scheduler_task_queue_depth > 1000` | 5m | warning |
| HighSchedulingLatency | `histogram_quantile(0.95, sum(rate(scheduler_task_scheduling_latency_ms_bucket[5m])) by (le)) > 1000` | 5m | warning |
| LowShuffleLocality | `sum(rate(executor_shuffle_read_local_bytes[5m])) / (sum(rate(executor_shuffle_read_local_bytes[5m])) + sum(rate(executor_shuffle_read_remote_bytes[5m]))) < 0.5` | 10m | warning |
| ExecutorMemoryLow | `executor_memory_available_bytes < 1073741824` | 5m | warning |
| HighTaskRetryRate | `sum(rate(node_task_retries[5m])) > 10` | 5m | warning |
| ExecutorOffline | `count(node_status{role="executor"} == 1) < sum(scheduler_active_executors_count offset 5m)` | 2m | warning |

### Info Alerts

| Alert | Expression | For | Severity |
|-------|------------|-----|----------|
| StageRetries | `sum(rate(scheduler_stage_retries[5m])) > 0` | 1m | info |
| HighRemoteShuffleRatio | `sum(rate(executor_shuffle_read_remote_bytes[5m])) > sum(rate(executor_shuffle_read_local_bytes[5m]))` | 10m | info |

---

## Dashboard JSON Export

**Output file**: `monitoring/grafana-distributed-dashboard.json`

The dashboard can be exported from Grafana and stored in version control. Key settings:

```json
{
  "title": "Spice Distributed Overview",
  "uid": "spice-distributed",
  "tags": ["spice", "distributed", "ballista"],
  "timezone": "browser",
  "refresh": "10s",
  "time": {
    "from": "now-1h",
    "to": "now"
  }
}
```

---

## Key Derived Metrics Summary

| Metric | Formula | Purpose |
|--------|---------|---------|
| Shuffle Locality % | `local_bytes / (local_bytes + remote_bytes) * 100` | Data placement efficiency |
| Task Success Rate % | `completed / total * 100` | Overall reliability |
| Executor Utilization % | `active_tasks / max_slots * 100` | Resource efficiency |
| Avg Scheduling Latency | `histogram_quantile(0.50, ...)` | Scheduler responsiveness |
| Network Shuffle Ratio | `remote_bytes / total_shuffle_bytes` | Network overhead |

---

## Best Practices

### Panel Organization
1. **Top row**: High-level distributed health (glanceable)
2. **Middle rows**: Operational metrics (scheduler, executor, shuffle)
3. **Bottom rows**: Error tracking and drill-down details

### Color Conventions
- **Green**: Healthy, completed, local
- **Yellow**: Warning, draining, retrying
- **Red**: Failed, unhealthy, errors
- **Blue**: Informational counters
- **Orange**: Remote operations, network

### Time Ranges
- **Default**: 1 hour (operational monitoring)
- **Debugging**: 15 minutes (fine-grained analysis)
- **Trend analysis**: 24 hours or 7 days

### Variable Usage
- Always filter by `$node` for drill-down capability
- Use `$interval` for rate calculations (auto-adjusts to time range)
- Enable "All" option for distributed-wide views

---

## Implementation Checklist

- [ ] Create dashboard in Grafana
- [ ] Add template variables
- [ ] Implement Row 1: Distributed Overview
- [ ] Implement Row 2: Scheduler Performance
- [ ] Implement Row 3: Stage Execution
- [ ] Implement Row 4: Executor Performance
- [ ] Implement Row 5: Shuffle Data Flow
- [ ] Implement Row 6: Result Collection
- [ ] Implement Row 7: Errors & Reliability
- [ ] Implement Row 8: Node Details
- [ ] Configure alert rules
- [ ] Export and commit dashboard JSON
- [ ] Test with live cluster data
