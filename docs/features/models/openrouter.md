# OpenRouter decision models

[OpenRouter](https://openrouter.ai) serves decision models, such as Baseten's `baseten/elephant-1`, through its [Decisions API](https://openrouter.ai/docs/api/api-reference/alpha-decisions/create-api-alpha-decisions). A decision model takes `state` and typed questions (noul / choice / score) and returns typed answers with probabilities.

Spice supports only OpenRouter **decision** models. Chat completions (`POST /v1/chat/completions`) return 400 for them. Ask decisions in SQL (`ai_if`, `ai_classify`, `ai_score`, `ai_decide`, ...) or with `POST /v1/decisions`.

## Spicepod

```yaml
models:
  - from: openrouter:baseten/elephant-1
    name: elephant
    params:
      openrouter_api_key: ${secrets:OPENROUTER_API_KEY}
```

The model id after `openrouter:` (or `openrouter/`) is the OpenRouter model id, with its `<provider>/` part. To find decision models, see [OpenRouter models with the decisions output](https://openrouter.ai/models?output_modalities=decisions).

| Param | Default | Description |
| --- | --- | --- |
| `openrouter_api_key` | Autoloads `OPENROUTER_API_KEY` | OpenRouter API key. Required. |
| `openrouter_endpoint` | `https://openrouter.ai/api` | API base URL. Decisions go to `{endpoint}/alpha/decisions`. |
| `max_concurrency`, `requests_per_minute_limit` | 20, 1200 | Runtime rate controls. |

At load, Spice reads `GET {endpoint}/v1/models/{model_id}/endpoints`. The model does not load if:

- OpenRouter has no model with that id.
- The model does not output `decisions`. For example, `openrouter:openai/gpt-4o` is a chat model, so Spice does not load it.
- No provider serves the model at that time.

That route needs no key. Thus Spice reports a bad key on the first decision request, not at load.

## Decisions

```sql
SELECT id FROM tickets WHERE ai_if(body, 'Does this convey urgency?', model => 'elephant');
```

```bash
curl -X POST http://localhost:8090/v1/decisions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "elephant",
    "input": "Help! My payouts have been failing for 3 days.",
    "questions": [{"type": "predicate", "name": "urgent", "instructions": "Does this convey urgency?"}]
  }'
```

OpenRouter's `box` questions, audio and video inputs, and the `assets` map are not supported.
