# Darkbloom models

[Darkbloom](https://darkbloom.ai) is an inference network that runs open-weight models on Apple Silicon machines and serves them through an `OpenAI`-compatible API. A `from: darkbloom:<model_id>` model calls that API.

## Spicepod

```yaml
models:
  - from: darkbloom:gpt-oss-20b
    name: darkbloom
    params:
      darkbloom_api_key: ${secrets:DARKBLOOM_API_KEY}
```

`<model_id>` is a model id that Darkbloom serves, as its `GET /v1/models` lists it. An id can contain a `/`: `darkbloom:EigenLabs/Qwen3.8-27B-4bit-mtp`.

| Param | Default | Description |
| --- | --- | --- |
| `darkbloom_api_key` | The `darkbloom_api_key` secret | The Darkbloom API key, from the [Darkbloom console](https://console.darkbloom.dev). When it is not set, Spice reads the `darkbloom_api_key` secret from the secret stores, such as the `DARKBLOOM_API_KEY` environment variable. |
| `darkbloom_endpoint` | `https://api.darkbloom.dev/v1` | The base URL of the Darkbloom API. |

The parameters all model providers accept also apply, prefixed with `darkbloom_` where they are request defaults: for example `darkbloom_temperature`, `tools`, `system_prompt` and `max_concurrency`.

## What Spice serves

- `POST /v1/chat/completions`, streamed and not streamed, and Spice tools, through Darkbloom's chat completions.
- `POST /v1/responses`, streamed and not streamed, through Darkbloom's own Responses API.
- `POST /v1/messages`, which Spice translates into chat completions.
- `GET /v1/models`, with the context window and output cap that Darkbloom's model listing reports (`context_length`, `max_output_length`).
- Decisions (`POST /v1/decisions`, `ai_if`, `ai_classify`, and the other decision functions) through chat.
- The SQL `ai` functions.

When the model loads, Spice sends one short chat completion and one short Responses API request to check the key and the model. A model id Darkbloom does not serve fails to load, and the log lists the models Darkbloom does serve. If only the Responses API check fails, the model still serves chat completions, and the log says it is not available on `/v1/responses`.

## Limitations

- Darkbloom describes its network as an experimental research prototype. Check its terms before you send production data.
- Darkbloom decrypts each request inside its coordinator, and the machine that serves the model reads the prompt in plaintext. It is not end-to-end encrypted.
- Darkbloom serves no embeddings, so there is no `darkbloom` embeddings provider.
- Darkbloom publishes no rate limits. Spice allows 10 concurrent requests and 500 requests per minute for each model by default; set `max_concurrency` and `requests_per_minute_limit` to change them.
- Darkbloom accepts `n: 1` only.
- Darkbloom's provider attestation (the `x-provider-*` response headers and the `se_signature` field) is not checked or returned by Spice.
