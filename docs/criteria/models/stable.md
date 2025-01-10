# Spice.ai OSS Models - Stable Release Criteria

This document defines the set of criteria that is required before a model is considered to be of Stable quality.

All criteria must be met for the model to be considered Stable.

## Stable Quality Models

|     Model Type          | Stable Quality | DRI Sign-off |
| ----------------------- | -------------- | ------------ |
| File                    | ➖             |              |
| Hugging Face            | ➖             |              |
| Spice.ai Cloud Platform | ➖             |              |
| OpenAI                  | ➖             |              |
| Azure                   | ➖             |              |
| Anthropic               | ➖             |              |
| xAI (Grok)              | ➖             |              |

## Stable Release Criteria
- All [RC release criteria](./rc.md) pass.

### Conditional Criteria
- A language model running in the Spice runtime (as opposed to network-attached models), can handle consistent requests from several clients without an adverse impact on latency.
  - N clients consistently sending requests (i.e. sending another request upon reciept of prior request)
  - A duration of 60 minutes.
  - The body must have at least Y tokens (number of prompt tokens in the templated input string).
  - An increase in latency is defined as a 10% increase in both the 50th & 95th percentile between the first and last minute.

### Documentation

- [ ] All documentation meets RC criteria.
- [ ] Documentation includes any exceptions made for Stable quality.
