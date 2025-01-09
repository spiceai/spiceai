# Spice.ai OSS Models - RC Release Criteria

This document defines the set of criteria that is required before a model is considered to be of RC quality.

All criteria must be met for the model to be considered RC.

## RC Quality Models

| Model Type     | RC Quality | DRI Sign-off |
| -------------- | ---------- | ------------ |
| File           | ➖         |              |
| Hugging Face   | ➖         |              |
| Spice.ai       | ➖         |              |
| OpenAI         | ➖         |              |
| Azure          | ➖         |              |
| Anthropic      | ➖         |              |
| xAI (Grok)     | ➖         |              |

## RC Release Criteria

- Complete and passing integration testing in [`runtime`](https://github.com/spiceai/spiceai/blob/trunk/crates/runtime/tests/models) crate, including:
  - NSQL integration tests.
- For both synchronous and streaming APIs, usage numbers are reported.
- W3 [TraceContext](https://www.w3.org/TR/trace-context/) headers are supported, and when used, the internal tool recursion is maintained.

### Conditional Criteria
- For models running in the Spice runtime (as opposed to network-attached models).
  - Can handle X TPS and Y tokens per second successfully for 1 minute without increasing latency.

### Documentation

- [ ] All documentation meets beta criteria.
- [ ] Documentation includes any exceptions made for RC quality.
