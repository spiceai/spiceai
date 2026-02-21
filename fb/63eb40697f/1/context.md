# Session Context

## User Prompts

### Prompt 1

`get_app_metrics` in `crates/spice-cloud-client/src/client.rs` now returns another key

```json
  "ingestion": {
    "rows_ingested": 1000000,
    "bytes_ingested": 524288000,
  },
  "metrics: {... unchanged}
```

Update the crate accordingly. Update bin/spice/src/commands/cloud/client.rs accordingly. Update tools/spidapter/src/stdio_server.rs accordingly

