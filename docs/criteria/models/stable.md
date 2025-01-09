# Spice.ai OSS Models - Stable Release Criteria

This document defines the set of criteria that is required before a model is considered to be of Stable quality.

All criteria must be met for the model to be considered Stable.

## Stable Quality Models

| Model Type     | Stable Quality | DRI Sign-off |
| -------------- | -------------- | ------------ |
| File           | ➖             |              |
| Hugging Face   | ➖             |              |
| Spice.ai       | ➖             |              |
| OpenAI         | ➖             |              |
| Azure          | ➖             |              |
| Anthropic      | ➖             |              |
| xAI (Grok)     | ➖             |              |

## Stable Release Criteria
- All [RC release criteria](./rc.md) pass.

### Conditional Criteria
- For models running in the Spice runtime (as opposed to network-attached models).
  - Can handle X TPS and Y tokens per second for 1 hour without increasing latency.

### Documentation

- [ ] All documentation meets RC criteria.
- [ ] Documentation includes any exceptions made for Stable quality.
