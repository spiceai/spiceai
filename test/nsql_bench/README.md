# Text to SQL Benchmark

Instructions to run the benchmark:

1. Run Spice
1. Run `tpch_nsql` eval dataset

```bash
curl -XPOST "http://localhost:8090/v1/evals/tpch_nsql" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o-mini"
  }'
[{"id":"62470b97daaf3105c321d743ae4a7201","created_at":"2025-01-28T22:50:17","dataset":"tpch_nsql","model":"gpt-4o-mini","status":"Completed","scorers":["match"],"metrics":{"match/mean":1.0}}]% 
```