# Spice.ai OSS Models - Beta Release Criteria

This document defines the set of criteria that is required before a model is considered to be of Beta quality.

All criteria must be met for the model to be considered Beta, with exceptions permitted only in some cases.

## Beta Quality Models

|     Model Type          | Beta Quality | DRI Sign-off |
| ----------------------- | ------------ | ------------ |
| File                    | ➖           |              |
| Hugging Face            | ➖           |              |
| Spice.ai Cloud Platform | ➖           |              |
| OpenAI                  | ➖           |              |
| Azure                   | ➖           |              |
| Anthropic               | ➖           |              |
| xAI (Grok)              | ➖           |              |

## Beta Release Criteria
- All [Alpha release criteria](./alpha.md) pass.
- Supports `v1/chat/completion` with `"roles"="tool"` or `.messages[*].tool_calls` for `"roles"="assistant"` and `stream=true`.
- Complete and passing integration testing in:
  - [`llms`](https://github.com/spiceai/spiceai/tree/trunk/crates/llms/tests) crate.
  - [`runtime`](https://github.com/spiceai/spiceai/blob/trunk/crates/runtime/tests/models) crate, including:
    - Chat completion integration tests (model defined in SpicePod runtime).
    - Tool usage integration tests.

### UX
- [ ] All of the connector's error messages follow the [error handling guidelines](../../dev/error_handling.md)

### Documentation

- [ ] All documentation meets alpha criteria.
- [ ] Documentation includes any exceptions made for Beta quality.
