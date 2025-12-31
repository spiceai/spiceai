# Spice.ai OSS Embeddings - Beta Release Criteria

This document defines the set of criteria that is required before an embedding component is considered to be of Beta quality.

All criteria must be met for the embedding component to be considered Beta, with exceptions permitted only in some cases.

## Beta Quality Embeddings

|     Embedding Type      | Beta Quality | DRI Sign-off |
| ----------------------- | ------------ | ------------ |
| File                    | ✅           | @Jeadie      |
| Hugging Face            | ✅           | @Jeadie      |
| Spice.ai Cloud Platform | ➖           |              |
| OpenAI                  | ✅           | @ewgenius    |
| Azure Openai            | ➖           |              |
| xAI (Grok)              | ➖           |              |

## Beta Release Criteria

- [ ] All [Alpha release criteria](./alpha.md) pass.
- [ ] `.usage` field from the `v1/embeddings` response is non-empty.
- [ ] Embedding requests emit runtime metrics
- [ ] Embedding requests emit runtime tracing, including linkage to parent tasks when used internally.

### UX

- [ ] All of the embedding model's error messages follow the [error handling guidelines](../../dev/error_handling.md)

### Documentation

- [ ] All documentation meets alpha criteria.
- [ ] Documentation includes any exceptions made for Beta quality.
