# Spice.ai OSS Embeddings - Alpha Release Criteria

This document defines the set of criteria that is required before an embedding component is considered to be of Alpha quality.

All criteria must be met for the embedding component to be considered Alpha. As Alpha signifies the lowest release quality, criteria exceptions are not permitted.

## Alpha Quality Embeddings

|     Embedding Type      | Alpha Quality | DRI Sign-off |
| ----------------------- | ------------- | ------------ |
| File                    | ✅            | @Jeadie      |
| Hugging Face            | ✅            | @Jeadie      |
| Spice.ai Cloud Platform | ➖            |              |
| OpenAI                  | ✅            | @ewgenius    |
| Azure Openai            | ➖            |              |
| xAI (Grok)              | ➖            |              |

## Alpha Release Criteria

- [ ] Functional `v1/embeddings` endpoint, with support for both float and base64 inputs.

### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] The embedding component is listed in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
