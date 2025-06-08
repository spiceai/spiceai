# FinanceBench

Instructions to run the [FinanceBench](https://github.com/patronus-ai/financebench) tests:

1. Run Spice
1. Run `financebench` eval

```bash
curl -XPOST "http://localhost:8090/v1/evals/financebench" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4.1"
  }'
[{"id":"a5a4ee4d40dfcfc81a050bc5e23135c9","created_at":"2025-03-24T22:28:14","completed_at":"2025-03-24T22:29:19","dataset":"financebench.evals","model":"gpt-4o","status":"Completed","scorers":["match"],"metrics":{"match/mean":0.0}}]%  
```

To review model answers and score details:

```bash
curl -XPOST http://localhost:8090/v1/sql --data "
WITH latest_run AS (
  SELECT id FROM spice.eval.runs ORDER BY created_at DESC LIMIT 1
)
SELECT run_id, input, expected, actual, value
FROM eval.results
WHERE run_id = (SELECT id FROM latest_run)
" | jq
```

To review overall eval workflow:

```bash
 spice trace eval_run --include-input --truncate 250
```
