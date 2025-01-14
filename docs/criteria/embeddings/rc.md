# Spice.ai OSS Embeddings - RC Release Criteria

This document defines the set of criteria that is required before an embedding component is considered to be of RC quality.

All criteria must be met for the embedding component to be considered RC.

## RC Quality Embeddings

|     Embedding Type      | RC Quality | DRI Sign-off |
| ----------------------- | ---------- | ------------ |
| File                    | ➖         |              |
| Hugging Face            | ➖         |              |
| Spice.ai Cloud Platform | ➖         |              |
| OpenAI                  | ➖         |              |
| Azure Openai            | ➖         |              |
| xAI (Grok)              | ➖         |              |

## RC Release Criteria

- All [Beta release criteria](./beta.md) pass.

### Conditional Criteria
- For embedding models running in the Spice runtime (as opposed to network-attached models).
  - For a period of 1 minute, can handle N clients consistently sending embedding requests (i.e. instantly send another request upon receipt of prior request), each with Y tokens, without increasing round trip (to client) latency for 5 minute. An increase in latency is defined as a 10% increase in both the 50th & 95th percentile between the first and last minute.


### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] The embedding component has an easy-to-follow cookbook recipe.
- [ ] The embedding component status is updated in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
