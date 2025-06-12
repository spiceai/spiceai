# Spice Model Grading Kit

This repository include the eval datasets, test scripts, and sample spicepod config for grading models.

## Evaluate model's ability to follow structured output
Use the [testoperator](tools/testoperator/README.md) to evaluate a model's performance at returning structured output.
```bash
./testoperator run evals \
   --data-dir test/model_grading/ \
   -p ./test/spicepods/models/structured_output.yaml \
   --model o3-mini \
   --metrics
```
 - To test a **different model**: Use (or define) a new model component in the spicepod at `./test/spicepods/models/structured_output.yaml`.
 - To use **different test data**: Set the `--data-dir` to a directory where a `structured_output.jsonl` is present.


## Evaluate whether model's enters a recursion loop through any means (tool use, chat completions, etc)

Follow the [recursion test sample spicepod](./test_recursion/test_recursion.yaml), replace model provider with the model to be tested. Run the [recursion test shell script](./test_recursion/test_recursion.bash) to evaluate whether the model would enter a recursion loop through any means. The shell script consists simple user request each indicating no more than 3 tool calls per request. Summary stats on sum of tool calls and chat completion per user request will be calculated when all user requests have finished.

## Evaluate model's ability to produce valid and correct sql queries

See [nsql_bench](../nsql_bench/README.md).

---

For details about model grading criteria, refer to the [grading criteria](../../docs/criteria/models/grading.md) docs.
