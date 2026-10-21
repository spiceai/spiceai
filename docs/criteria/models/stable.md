# Spice.ai OSS Models - Stable Release Criteria

This document defines the set of criteria that is required before a model is considered to be of Stable quality.

All criteria must be met for the model to be considered Stable.

## Stable Quality Models

|     Model Type          | Stable Quality | DRI Sign-off |
| ----------------------- | -------------- | ------------ |
| Anthropic               | ➖             |              |
| Amazon Bedrock          | ➖             |              |
| Azure OpenAI            | ➖             |              |
| Databricks              | ➖             |              |
| File                    | ➖             |              |
| Google (Gemini)         | ➖             |              |
| Hugging Face            | ➖             |              |
| Nvidia NIM              | ➖             |              |
| OpenAI                  | ➖             |              |
| Spice.ai Cloud Platform | ➖             |              |
| xAI (Grok)              | ➖             |              |

## Stable Release Criteria
- [ ] All [RC release criteria](./rc.md) pass.
- [ ] Deployed and test in production at scale.

### Documentation

- [ ] All documentation meets RC criteria.
- [ ] Documentation includes any exceptions made for Stable quality.
- [ ] The model has a Deployment Guide in [spiceai/docs](https://github.com/spiceai/docs) covering production deployment, configuration, authentication, rate limits, and operational considerations (e.g. [Databricks Deployment Guide](https://spiceai.org/docs/next/components/data-connectors/databricks/deployment)).
