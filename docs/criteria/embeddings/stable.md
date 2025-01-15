# Spice.ai OSS Embeddings - Stable Release Criteria

This document defines the set of criteria that is required before an embedding component is considered to be of Stable quality.

All criteria must be met for the embedding component to be considered Stable.

## Stable Quality Embeddings

|     Embedding Type      | Stable Quality | DRI Sign-off |
| ----------------------- | -------------- | ------------ |
| File                    | ➖             |              |
| Hugging Face            | ➖             |              |
| Spice.ai Cloud Platform | ➖             |              |
| OpenAI                  | ➖             |              |
| Azure Openai            | ➖             |              |
| xAI (Grok)              | ➖             |              |

## Stable Release Criteria
- [ ] All [RC release criteria](./rc.md) pass.

### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] Documentation includes any exceptions made for Stable quality.
- [ ] The embedding component has an easy-to-follow cookbook.
- [ ] The embedding component status is updated in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
