# Executor Utilization Metric Plan

## Overview

The current `Executor Utilization` panel uses `executor_tasks_active` directly, which only shows active task counts. To display a true utilization percentage, we need a per-executor capacity metric that indicates the maximum concurrent tasks (task slots). This plan adds a capacity gauge and updates the Grafana panel to compute `active / capacity * 100`.

## Goals

1. Emit a per-executor capacity metric with a stable value at runtime.
2. Compute a percent utilization in Grafana (0-100%).
3. Keep cardinality low and align with the existing cluster metrics naming conventions.

## Proposed Metric

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `executor_task_slots` | Gauge | `node_id` | Maximum concurrent task slots on the executor |

Notes:
- Gauge is stable at executor start and can be updated if capacity changes (e.g., dynamic config).
- Label only `node_id` to avoid high cardinality.

## Implementation Steps

### 1) Ballista Fork (Instrumentation)

Identify the executor capacity source (likely in the executor config/options):
- Executor startup config that defines max concurrent tasks or task slots.
- If not explicitly defined, derive from worker thread pool size or configured concurrency.

Add a new method to the executor metrics collector trait:
- `set_task_slots(&self, slots: u64)`

Call it once during executor startup (or when capacity changes):
- In `ballista/executor/src/executor.rs` (or wherever executor config is finalized).

### 2) Spice Metrics Definitions

Add the gauge definition in the cluster metrics module:

- File: `crates/runtime/src/metrics/cluster/mod.rs`
- Metric: `executor_task_slots` (Gauge<u64>)
- Description: "Maximum concurrent task slots on the executor"

### 3) Spice Metrics Collector

Update the OpenTelemetry executor metrics collector to record capacity:

- File: `crates/runtime/src/cluster/metrics_collector.rs`
- Implement `set_task_slots` to record `executor_task_slots` with `node_id`.

### 4) Wire Capacity into Executor Startup

When the executor starts, capture the configured capacity and record it once:

- File: `crates/runtime/src/cluster/mod.rs`
- Use the same `node_advertise_address` label as other executor metrics.

### 5) Grafana Update

Update the Executor Utilization bar gauge to compute percentage:

```
100 * (
  executor_tasks_active{node_id=~"$node"} /
  clamp_min(executor_task_slots{node_id=~"$node"}, 1)
)
```

- Unit: percent
- Thresholds: 0-50 red, 50-80 yellow, 80-100 green (adjust if needed)

## Testing Plan

1. **Unit tests**: Validate `set_task_slots` records the gauge with correct labels.
2. **Integration**: Run a cluster with known task slot settings and confirm:
   - `executor_task_slots` appears in `/metrics?scope=cluster`.
   - Utilization percent matches expected values for running tasks.
3. **Grafana**: Verify bar gauge shows % utilization and updates with load.

## Open Questions

1. **Capacity source**: What is the canonical executor capacity setting in Ballista?
2. **Dynamic updates**: Does capacity change at runtime (e.g., autoscaling)?
3. **Executor naming**: Ensure `node_id` matches existing executor labels consistently.

## Success Criteria

- `executor_task_slots` is emitted for each executor.
- Executor Utilization panel shows percent utilization.
- No high-cardinality labels added.
