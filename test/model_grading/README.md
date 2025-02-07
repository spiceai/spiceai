# Spice Model Grading Kit

This repository include the eval datasets, test scripts, and sample spicepod config for grading models.

## Evaluate model's ability to follow structured output

Follow the [structured output eval sample spicepod](./structured_output/structured_output.yaml), replace model provider with the model to be tested. Run evals against the model using the following command, which will run the structured output eval using the [structured_output.jsonl](./structured_output/structured_output.jsonl) dataset.

```bash
curl -XPOST http://localhost:8090/v1/evals/structured_output \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "test_model"
  }'
```

## Evaluate whether model's enters a recursion loop through any means (tool use, chat completions, etc)

Follow the [recursion test sample spicepod](./test_recursion/test_recursion.yaml), replace model provider with the model to be tested. Run the [recursion test shell script](./test_recursion/test_recursion.bash) to evaluate whether the model would enter a recursion loop through any means. The shell script consists simple user request each indicating no more than 3 tool calls per request. Summary stats on sum of tool calls and chat completion per user request will be calculated when all user requests have finished.

## Evaluate model's ability to produce valid and correct sql queries

Follow the [nsql eval sample spicepod](../nsql_bench/spicepod.yaml), replace model provider with the model to be tested. Run evals against the model using the following command, which will run the structured output eval using the [tpch_nsql.jsonl](../nsql_bench/tpch_nsql.jsonl) dataset.

```bash
curl -XPOST http://localhost:8090/v1/evals/tpch_nsql \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "test_model"
  }'
```

For details about model grading criteria, refer to the [grading criteria](../../docs/criteria/models/grading.md) docs.
