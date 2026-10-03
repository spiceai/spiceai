# Evaluate API (`POST /v1/evaluate`)

`POST /v1/evaluate` takes unstructured `state` plus a map of typed questions — `noul` (yes/no probability), `choice` (one of a closed set of options), `score` (a rubric of 2–10 levels) — and returns a typed answer for each.

`model` names any model in the Spicepod:

| Model | Example `from` | Probabilities |
| --- | --- | --- |
| System One model | `typesafe:jev` | Calibrated by the model. See [TypeSafe models](typesafe.md). |
| Any chat model | `openai:gpt-4o-mini`, `anthropic:claude-haiku-4-5`, `bedrock:…`, `huggingface:…` | The model's own estimates. They are not calibrated: a chat model's `0.9` is not a 90% likelihood the way a System One model's is. |

## Evaluating with a chat model

Every chat model answers `/v1/evaluate` with no extra configuration:

```yaml
models:
  - from: openai:gpt-4o-mini
    name: judge
    params:
      openai_api_key: ${secrets:OPENAI_API_KEY}
```

```bash
curl -X POST http://localhost:8090/v1/evaluate \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "judge",
    "state": "Help! My payouts have been failing for 3 days.",
    "questions": {
      "is_urgent": { "type": "noul", "instructions": "Does this convey urgency?" },
      "team": {
        "type": "choice",
        "instructions": "Which team should handle this?",
        "criteria": { "billing": "Payments and payouts", "technical": "Bugs and outages" }
      }
    }
  }'
```

The response has the same shape as a System One model's:

```json
{
  "model": "judge",
  "answers": {
    "is_urgent": { "type": "noul", "noul": 0.9 },
    "team": {
      "type": "choice",
      "choice": "billing",
      "probabilities": { "billing": 0.8, "technical": 0.2 },
      "confidence": 0.6
    }
  },
  "usage": { "input_tokens": 612, "output_tokens": 31 }
}
```

How a chat model answers:

- The questions become a JSON schema that pins each answer to its own options or rubric levels, and the schema is written into the system prompt. `state` is sent as an untrusted document, and the prompt tells the model never to follow instructions found in it.
- The model is called without the Spice runtime tools its `tools` param enables, so text in `state` cannot trigger a tool call. Its `system_prompt` and parameter defaults still apply.
- Every reply is validated: each question answered, nothing else present, and every value inside its question's domain. The reply's JSON object may be wrapped in a Markdown fence or surrounded by prose, but a reply with two objects, a repeated key, or an unclosed object is rejected rather than guessed at. A malformed reply is sent back to the model once with its problems. If the second reply is still malformed, the request fails with HTTP 500 rather than returning a partial or guessed answer.
- A probability distribution whose sum is within rounding of 1 is rescaled to sum to exactly 1. A sum further from 1 counts as a malformed reply.
- A `choice` answer is the most probable option (an exact tie goes to the first option in alphabetical order). A `score` is the probability-weighted average level. `confidence` measures how concentrated the distribution is: 0 for uniform, 1 for certain.
- `usage` totals the tokens of every call the evaluation made, including a corrective retry, and is omitted when any call did not report its usage.
- Each call to the model is recorded in `runtime.task_history` under the evaluation's `ai_evaluate` span and in the chat model's `llm_*` metrics.

How reliably a chat model answers depends on the model. Small local models often return malformed JSON or copy parts of the schema back instead of answering, and follow instructions planted in `state` despite the prompt telling them not to; use a model that follows JSON instructions well.

The prompts and answer conversion follow TypeSafe's [system-one-adapter-python](https://github.com/typesafe-ai/system-one-adapter-python) (probabilities mode, schema in the prompt), so results can be compared with that adapter's.
