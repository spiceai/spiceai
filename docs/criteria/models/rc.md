# Spice.ai OSS Models - RC Release Criteria

This document defines the set of criteria that is required before a model is considered to be of RC quality.

All criteria must be met for the model to be considered RC.

## RC Quality Models

| Model Type              | RC Quality | DRI Sign-off |
| ----------------------- | ---------- | ------------ |
| File                    | ✅         | @sgrebnov    |
| Hugging Face            | ✅         | @Sevenannn   |
| Spice.ai Cloud Platform | ➖         |              |
| OpenAI                  | ✅         | @ewgenius    |
| Azure Openai            | ➖         |              |
| Anthropic               | ➖         |              |
| xAI (Grok)              | ➖         |              |

## RC Release Criteria

- [ ] All [Beta release criteria](./beta.md) pass.
- [ ] Can handle consistent requests from several clients without an adverse impact on latency. Resource efficiency (memory, CPU, and I/O usage) is measured.
  - 8 clients consistently sending requests (i.e. sending another request upon receipt of prior request)
  - A duration of 60 minutes.
  - The body must have at least 128 tokens (number of prompt tokens in the templated input string).
  - An increase in latency is defined as a 10% increase in both the 50th & 95th percentile between the first and last minute.

### Conditional Criteria

- For hosted models in the Spice runtime:
- [ ] Does not excessively increase the latency of the underlying hosted model.
  - An increase in latency is defined as a 15% increase in either the 50th & 95th percentile above the underlying model.

### Documentation

- [ ] All documentation meets beta criteria.
- [ ] Documentation includes any exceptions made for RC quality.
