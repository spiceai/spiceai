# Spice.ai OSS Models - Alpha Release Criteria

This document defines the set of criteria that is required before a model is considered to be of Alpha quality.

All criteria must be met for the model to be considered Alpha. As Alpha signifies the lowest release quality, criteria exceptions are not permitted.

## Alpha Quality Models

| Model Type              | Alpha Quality | DRI Sign-off    |
| ----------------------- | ------------- | --------------- |
| File                    | ✅            | @Jeadie         |
| Hugging Face            | ✅            | @Jeadie         |
| Spice.ai Cloud Platform | ➖            |                 |
| OpenAI                  | ✅            | @ewgenius       |
| Azure Openai            | ➖            |                 |
| Anthropic               | ➖            |                 |
| Nvidia NIM              | ✅            | @phillipleblanc |
| xAI (Grok)              | ✅            | @Sevenannn      |

## Alpha Release Criteria

- [ ] Supports `v1/chat/completion` for roles: `user`, `assistant`, `system` with `stream=false`.
- [ ] Supports `v1/chat/completion` for roles: `user`, `assistant`, `system` with `stream=true`.

### Documentation

- [ ] Documentation includes all steps to set  up the model.
- [ ] Documentation includes known limitations or issues for the model.
- [ ] The model is listed in the table of Model Providers in [spiceai/docs](https://github.com/spiceai/docs).
