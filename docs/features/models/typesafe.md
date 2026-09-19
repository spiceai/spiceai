# TypeSafe (Jev) System One evaluation models

[TypeSafe Jev](https://typesafe.ai/blog/introducing-system-one-models-and-jev) is a **System One evaluation model**, not a chat LLM. It takes unstructured `state` plus typed questions and returns structured answers (noul / choice / score) with calibrated probabilities.

Chat completions (`POST /v1/chat/completions`) are **not supported**. Use `POST /v1/evaluate`.

## Spicepod

```yaml
models:
  - from: typesafe:jev
    name: jev
    params:
      typesafe_api_key: ${secrets:TYPESAFE_API_KEY}
```

Aliases:

| `from` | Upstream model id |
| --- | --- |
| `typesafe:jev` | `jev-latest` |
| `typesafe:jev-latest` | `jev-latest` |
| `typesafe:jev-preview` | `jev-preview` |
| `typesafe:jev-1.13.0` | `jev-1.13.0` (pin) |

Secrets: prefer `TYPESAFE_API_KEY` (autoload as `typesafe_api_key`). Alias `typesafe_ai_api_key` / `TYPESAFE_AI_API_KEY` is also accepted.

Optional `typesafe_endpoint` overrides the base URL (default `https://api.typesafe.ai` — direct API, not the Vercel gateway).

## Evaluate API

```bash
curl -X POST http://localhost:8090/v1/evaluate \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "jev",
    "state": "Help! My payouts have been failing for 3 days.",
    "questions": {
      "is_urgent": {
        "type": "noul",
        "instructions": "Does this convey urgency?"
      }
    }
  }'
```

See the [TypeSafe API reference](https://docs.typesafe.ai/api) for question types and answer shapes.
