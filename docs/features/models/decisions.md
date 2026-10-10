# Decisions

Ask typed questions about your data in SQL, or over an HTTP API compatible with OpenAI's Decisions API. Every model in the Spicepod can answer:

| Model | Example `from` | Probabilities |
| --- | --- | --- |
| TypeSafe decision model | `typesafe:jev` | Calibrated by the model. See [TypeSafe models](typesafe.md). |
| OpenAI decision model | `openai:gpt-6-luna` | The model's estimates. OpenAI recommends setting thresholds from your own labeled examples. |
| Any chat model | `openai:gpt-4o-mini`, `anthropic:claude-haiku-4-5`, `bedrock:…` | The model's own estimates. A chat model's `0.9` is not a 90% likelihood the way a calibrated model's is. |

A decision model answers decisions only; `/v1/chat/completions` returns 400 for it. An OpenAI decision model takes the same params as other OpenAI models:

```yaml
models:
  - from: openai:gpt-6-luna
    name: luna
    params:
      openai_api_key: ${secrets:OPENAI_API_KEY}
```

## SQL functions

| Function | Returns |
| --- | --- |
| `ai_if(input, condition)` | `BOOLEAN`: true when the probability that `condition` holds is above 0.5 |
| `ai_probability(input, condition)` | `DOUBLE` in [0, 1]: the probability that `condition` holds |
| `ai_classify(input, labels)` | `VARCHAR`: the label that best fits, always one of `labels` |
| `ai_score(input, instructions, levels)` | `DOUBLE` in [0, n−1]: the probability-weighted 0-based index of `levels` |
| `ai_decide(input, questions)` | `STRUCT`: every answer to a set of questions, with probabilities and confidence |

```sql
-- Filter: the other predicates run first, so the model only sees open tickets.
SELECT id, subject FROM tickets
WHERE status = 'open' AND ai_if(body, 'The customer is asking for a refund');

-- Route and rank: both decisions on `body` share one request per row.
SELECT id,
       ai_classify(body, ['billing', 'technical', 'account']) AS team,
       ai_score(body, 'How frustrated is the customer?', ['calm', 'annoyed', 'furious']) AS frustration
FROM tickets
ORDER BY frustration DESC
LIMIT 20;

-- Tune a threshold, or count the expected matches.
SELECT count(*) FILTER (WHERE ai_probability(body, 'The customer threatens to cancel') >= 0.8) AS at_risk,
       sum(ai_probability(body, 'The customer threatens to cancel')) AS expected_cancellations
FROM tickets;
```

Arguments:

- `input`: text, or a struct, map or list, which is sent as JSON — for example `named_struct('subject', subject, 'body', body)`. Other types are sent as text, except binary, which is refused: cast it to text first, with `encode(col, 'hex')`, or `CAST(col AS VARCHAR)` for UTF-8 bytes. A NULL `input` returns NULL without a model call.
- `condition`, `instructions`: constant text.
- `labels`: a constant list of 2 to 255 labels, such as `['billing', 'technical']`, or a JSON object of label to description, such as `'{"billing": "Payments and refunds", "technical": null}'`. `ai_classify` also takes `instructions => '...'`. Include a fallback label such as `'other'` when no label may fit.
- `levels`: a constant list of 2 to 10 level descriptions, lowest first.
- `questions`: a constant JSON object of question id to question, the same grammar as TypeSafe's API and Databricks' `ai_decide`. A `choice` maps 1 to 255 non-empty labels to descriptions (`null` when the label says it all), a `score` lists 2 to 10 levels, lowest first, and no question id or label may repeat:

  ```sql
  SELECT ai_decide(body, '{
    "team":   {"type": "choice", "instructions": "Which team should handle this?",
               "criteria": {"billing": "Payments and payouts", "technical": "Bugs and outages"}},
    "urgent": {"type": "noul", "instructions": "Does this convey urgency?"},
    "tone":   {"type": "score", "instructions": "How upset is the customer?",
               "criteria": ["calm", "annoyed", "furious"]}
  }') AS d
  FROM tickets;
  ```

  Each field of the result is one answer: `noul` → `{probability}`, `choice` → `{choice, probabilities: [{value, probability}], confidence}`, `score` → `{score, probabilities: [{value, label, probability}], confidence}`. Read one with `d['team']['choice']`.
- `model => 'name'`: the model that answers. When omitted, the only model that can answer is used, or else the only decision model among several; otherwise the error lists the models to choose from.
- `on_error => 'fail' | 'null'`: what a row the model cannot answer does. `'fail'` (the default) stops the query and names the model and the cause, after retrying rate limits and transient failures. `'null'` returns NULL for that row. A NULL in `WHERE` drops the row like a no, which is why it is not the default.

Constants are checked when the query is planned, before any model is called.

How the engine runs them:

- In one `SELECT` list or `WHERE` clause, `ai_if`, `ai_probability`, `ai_classify` and `ai_score` calls on the same `input`, `model` and `on_error` share one request per row, and identical `ai_decide` calls share one request and one answer. Each distinct `ai_decide` call is its own request, asking all of its questions at once. Identical inputs are asked once within each slice of up to 1,024 rows.
- In `WHERE`, every other predicate runs first; the model only sees rows that pass them.
- A `LIMIT` stops requests only between input batches: each batch (up to 8,192 rows per partition) is answered whole before its rows reach the `LIMIT`. Narrow the rows with other predicates to bound what a query sends.
- The functions work in `SELECT`, `WHERE`, `HAVING`, `ORDER BY`, `GROUP BY`, window functions, aggregate arguments (including `FILTER (WHERE ...)`), and inner-join conditions. An outer-join condition is refused with the rewrite to use.
- They are never pushed down to a federated source or another engine.
- Each batch of calls is recorded in `runtime.task_history` as an `ai_decide` task, and every model call in the model's `llm_*` metrics. The model's `max_concurrency` and `requests_per_minute_limit` apply.

## HTTP: `POST /v1/decisions`

Compatible with [OpenAI's Decisions API](https://developers.openai.com/api/reference/resources/decisions/methods/create): point an OpenAI SDK's base URL at Spice and call `client.decisions.create(...)`.

```bash
curl -X POST http://localhost:8090/v1/decisions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "jev",
    "input": "Help! My payouts have been failing for 3 days.",
    "questions": [
      {"type": "predicate", "name": "urgent", "instructions": "Does this convey urgency?"},
      {"type": "choice", "name": "team", "instructions": "Which team should handle this?",
       "choices": [{"value": "billing", "description": "Payments and payouts"}, {"value": "technical", "description": "Bugs and outages"}]},
      {"type": "score", "name": "tone", "instructions": "How upset is the customer?",
       "levels": [{"label": "calm"}, {"label": "annoyed"}, {"label": "furious"}]}
    ]
  }'
```

```json
{
  "model": "jev-1.13.0",
  "answers": [
    {"type": "predicate", "name": "urgent", "probability": 0.9},
    {"type": "choice", "name": "team", "choice": "billing",
     "probabilities": [{"value": "billing", "probability": 0.8}, {"value": "technical", "probability": 0.2}], "confidence": 0.6},
    {"type": "score", "name": "tone", "score": 1.3,
     "probabilities": [{"value": 0, "label": "calm", "probability": 0.1}, {"value": 1, "label": "annoyed", "probability": 0.5}, {"value": 2, "label": "furious", "probability": 0.4}], "confidence": 0.3}
  ],
  "usage": {"input_tokens": 612, "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens": 0, "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 612}
}
```

- `questions`: 1 to 200 `predicate`, `choice` (2 to 255 `choices`) or `score` (2 to 10 `levels`, lowest first) questions. Answers come back in question order with each question's `name`, or `null` when it has none.
- `input`: a string, or user messages with `input_text` parts. Image inputs return 400.
- `reasoning_effort` (optional): `none`, `minimal`, `low`, `medium`, `high` or `xhigh`, passed to a chat model's completion request. Omitted keeps the model's setting. This is a Spice extension; OpenAI's Decisions API has no such field. A decision model returns 400 with `code` `unsupported_parameter`.
- Any other field outside OpenAI's schema returns 400. Errors use OpenAI's envelope: `{"error": {"message", "type", "param", "code"}}`.
- `usage` is omitted when the model did not report it.
- Each request is recorded in `runtime.task_history` as an `ai_decision` task.

## How a chat model answers

- The questions become a JSON schema that pins each answer to its own options or rubric levels, and the schema is written into the system prompt. The input is sent as an untrusted document, and the prompt tells the model never to follow instructions found in it.
- The model is called without the Spice runtime tools its `tools` param enables, so text in the input cannot trigger a tool call. Its `system_prompt` and parameter defaults still apply.
- Every reply is validated: each question answered, nothing else present, and every value inside its question's domain. A malformed reply is sent back to the model once with its problems; if the second reply is still malformed, the call fails rather than returning a guessed answer.
- A probability distribution whose sum is within rounding of 1 is rescaled to sum to exactly 1. A `choice` answer is the most probable option (an exact tie goes to the first option in alphabetical order), a `score` is the probability-weighted average level, and `confidence` measures how concentrated the distribution is: 0 for uniform, 1 for certain.

How reliably a chat model answers depends on the model. Small local models often return malformed JSON or follow instructions planted in the input despite the prompt; use a model that follows JSON instructions well, or a decision model.

The prompts and answer conversion follow TypeSafe's [system-one-adapter-python](https://github.com/typesafe-ai/system-one-adapter-python) (probabilities mode, schema in the prompt), so results can be compared with that adapter's.
