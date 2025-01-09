# Spice.ai OSS Embeddings - Beta Release Criteria

This document defines the set of criteria that is required before an embedding component is considered to be of Beta quality.

All criteria must be met for the embedding component to be considered Beta, with exceptions permitted only in some cases.

## Beta Quality Embeddings

| Embedding Type | Beta Quality | DRI Sign-off |
| -------------- | ------------ | ------------ |
| File                    | ➖           |              |
| Hugging Face            | ➖           |              |
| Spice.ai Cloud Platform | ➖           |              |
| OpenAI                  | ➖           |              |
| Azure                   | ➖           |              |
| xAI (Grok)              | ➖           |              |

## Beta Release Criteria

- Usage of the `v1/embeddings` endpoint is reported back to the user.

### Documentation

- [ ] Documentation includes all steps to set up the embedding component.
- [ ] Documentation includes known limitations or issues for the embedding component.
- [ ] Documentation includes any exceptions made for Beta quality.
- [ ] The embedding component has an easy-to-follow cookbook recipe.
- [ ] The embedding component is listed in the table of components in [spiceai/docs](https://github.com/spiceai/docs).
