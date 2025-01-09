# Spice.ai OSS Embeddings - RC Release Criteria

This document defines the set of criteria that is required before an embedding component is considered to be of RC quality.

All criteria must be met for the embedding component to be considered RC.

## RC Quality Embeddings

| Embedding Type | RC Quality | DRI Sign-off |
| -------------- | ---------- | ------------ |
| File           | ➖         |              |
| Hugging Face   | ➖         |              |
| Spice.ai       | ➖         |              |
| OpenAI         | ➖         |              |
| Azure          | ➖         |              |
| Grok           | ➖         |              |

## RC Release Criteria

For embedding components run in the Spice runtime:
- Can handle X TPS (Transactions Per Second) and Y tokens per second successfully and without increasing latency for 1 minute.

### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] The embedding component has an easy-to-follow cookbook recipe.
- [ ] The embedding component status is updated in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
