# Run structured output eval

```bash
curl -XPOST http://localhost:8090/v1/evals/structured_output \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "test_model"
  }'
```
