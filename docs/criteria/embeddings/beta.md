# Spice.ai OSS Embeddings - Beta Release Criteria

This document defines the set of criteria that is required before an embedding component is considered to be of Beta quality.

All criteria must be met for the embedding component to be considered Beta, with exceptions permitted only in some cases.

## Beta Quality Embeddings

|     Embedding Type      | Beta Quality | DRI Sign-off |
| ----------------------- | ------------ | ------------ |
| File                    | ➖           |              |
| Hugging Face            | ➖           |              |
| Spice.ai Cloud Platform | ➖           |              |
| OpenAI                  | ➖           |              |
| Azure Openai            | ➖           |              |
| xAI (Grok)              | ➖           |              |

## Beta Release Criteria

- All [Alpha release criteria](./alpha.md) pass.
- `.usage` field from the `v1/embeddings` response is non-empty.

### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] Documentation includes any exceptions made for Beta quality.
- [ ] The embedding component has an easy-to-follow cookbook recipe.
- [ ] The embedding component is listed in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
