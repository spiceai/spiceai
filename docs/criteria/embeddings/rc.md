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
- [ ] An embedding model running in the Spice runtime (as opposed to network-attached models), can handle consistent requests from several clients without an adverse impact on latency.
  - 8 clients consistently sending requests (i.e. sending another request upon receipt of prior request)
  - A duration of 60 minutes.
  - The body must have at least 128 tokens.
  - An increase in latency is defined as a 10% increase in both the 50th & 95th percentile between the first and last minute.

### Conditional Criteria

- For hosted models in the Spice runtime:
- [ ] Does not excessively increase the latency of the underlying hosted model.
  - An increase in latency is defined as a 15% increase in either the 50th & 95th percentile above the underlying model.

### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] The embedding component has an easy-to-follow cookbook recipe.
- [ ] The embedding component status is updated in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
