# Spice.ai OSS Models - Beta Release Criteria

This document defines the set of criteria that is required before a model is considered to be of Beta quality.

All criteria must be met for the model to be considered Beta, with exceptions permitted only in some cases.

## Beta Quality Models

|     Model Type          | Beta Quality | DRI Sign-off |
| ----------------------- | ------------ | ------------ |
| File                    | ✅           | @Jeadie      |
| Hugging Face            | ✅           | @Jeadie      |
| Spice.ai Cloud Platform | ➖           |              |
| OpenAI                  | ✅            | @ewgenius   |
| Azure Openai            | ➖           |              |
| Anthropic               | ➖           |              |
| xAI (Grok)              | ➖           |              |

## Beta Release Criteria

- [ ] All [Alpha release criteria](./alpha.md) pass.
- [ ] Supports `v1/chat/completion` with `"roles"="tool"` or `.messages[*].tool_calls` for `"roles"="assistant"` and `stream=true`.
- [ ] Loads and runs `params.tools: auto` tools.
- [ ] Completion requests emit runtime metrics
- [ ] Completion requests emit runtime tracing, including linkage to parent tasks when used internally.
- [ ] For both synchronous and streaming APIs, usage numbers are reported.
- [ ] Can handle consistent requests from several clients without an adverse impact on latency. Resource efficiency (memory, CPU, and I/O usage) is measured.
  - 8 clients consistently sending requests (i.e. sending another request upon receipt of prior request)
  - A duration of 5 minutes.
  - The body must have at least 128 tokens (number of prompt tokens in the templated input string).
  - An increase in latency is defined as a 10% increase in both the 50th & 95th percentile between the first and last minute.

### UX

- [ ] All of the model's error messages follow the [error handling guidelines](../../dev/error_handling.md)

### Documentation

- [ ] All documentation meets alpha criteria.
- [ ] Documentation includes any exceptions made for Beta quality.
