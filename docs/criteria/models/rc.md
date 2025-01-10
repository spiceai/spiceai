# Spice.ai OSS Models - RC Release Criteria

This document defines the set of criteria that is required before a model is considered to be of RC quality.

All criteria must be met for the model to be considered RC.

## RC Quality Models

|     Model Type          | RC Quality | DRI Sign-off |
| ----------------------- | ---------- | ------------ |
| File                    | ➖         |              |
| Hugging Face            | ➖         |              |
| Spice.ai Cloud Platform | ➖         |              |
| OpenAI                  | ➖         |              |
| Azure                   | ➖         |              |
| Anthropic               | ➖         |              |
| xAI (Grok)              | ➖         |              |

## RC Release Criteria

- All [Beta release criteria](./beta.md) pass.
- Complete and passing integration testing in [`runtime`](https://github.com/spiceai/spiceai/blob/trunk/crates/runtime/tests/models) crate, including:
  - NSQL integration tests.
- For both synchronous and streaming APIs, usage numbers are reported.
- W3 [TraceContext](https://www.w3.org/TR/trace-context/) headers are supported, and when used, the internal tool recursion is maintained.

### Conditional Criteria
- A language model running in the Spice runtime (as opposed to network-attached models), can handle consistent requests from several clients without an adverse impact on latency.
  - N clients consistently sending requests (i.e. sending another request upon reciept of prior request)
  - A duration of 5 minutes.
  - The body must have at least Y tokens (number of prompt tokens in the templated input string).
  - An increase in latency is defined as a 10% increase in both the 50th & 95th percentile between the first and last minute.


### Documentation

- [ ] All documentation meets beta criteria.
- [ ] Documentation includes any exceptions made for RC quality.
