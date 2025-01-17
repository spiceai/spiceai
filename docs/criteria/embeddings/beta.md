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

- [ ] All [Alpha release criteria](./alpha.md) pass.
- [ ] `.usage` field from the `v1/embeddings` response is non-empty.
- [ ] Embedding requests emit runtime metrics
- [ ] Embedding requests emit runtime tracing, including linkage to parent tasks when used internally.
- [ ] An embedding model running in the Spice runtime (as opposed to network-attached models), can handle consistent requests from several clients without an adverse impact on latency.
  - 8 clients consistently sending requests (i.e. sending another request upon receipt of prior request)
  - A duration of 5 minutes.
  - The body must have at least 128 tokens.
  - An increase in latency is defined as a 10% increase in both the 50th & 95th percentile between the first and last minute.


### UX
- [ ] All of the embedding model's error messages follow the [error handling guidelines](../../dev/error_handling.md)


### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] Documentation includes any exceptions made for Beta quality.
- [ ] The embedding component has an easy-to-follow cookbook recipe.
- [ ] The embedding component is listed in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
