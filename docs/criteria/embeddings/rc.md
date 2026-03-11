# Spice.ai OSS Embeddings - RC Release Criteria

This document defines the set of criteria that is required before an embedding component is considered to be of RC quality.

All criteria must be met for the embedding component to be considered RC.

## RC Quality Embeddings

|     Embedding Type      | RC Quality | DRI Sign-off |
| ----------------------- | ---------- | ------------ |
| File                    | ✅         | @Jeadie      |
| Hugging Face            | ✅         | @Jeadie      |
| Spice.ai Cloud Platform | ➖         |              |
| OpenAI                  | ✅         | @ewgenius    |
| Azure Openai            | ➖         |              |
| xAI (Grok)              | ➖         |              |

## RC Release Criteria

- [ ] All [Beta release criteria](./beta.md) pass.

### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] The embedding component has an easy-to-follow cookbook recipe.
- [ ] The embedding component status is updated in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
