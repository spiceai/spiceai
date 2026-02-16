## Release Stages

| Stage | Summary | API/Config Stability | Production Use |
|-------|---------|---------------------|----------------|
| **Alpha** | It's working | Breaking changes expected | Not recommended |
| **Beta** | It's working well | Breaking changes unlikely | Early adopters only |
| **RC** | It's ready for release | Frozen (no breaking changes) | Suitable for production |
| **Stable** | It's running in production | Backward compatible only | Fully supported |

## Promotion Checklist

When promoting a feature to a new release stage:

1. **Complete self-assessment**: Verify all criteria for the target stage are met
2. **Document exceptions**: Any unmet criteria must be documented with justification
3. **Obtain sign-off**: DRI (Directly Responsible Individual) must approve the promotion
4. **Update documentation**: Change release stage labels in all documentation
5. **Announce**: Include promotion in release notes

## Quick Reference Matrix

| Criterion                          | Alpha | Beta |      RC       | Stable |
|------------------------------------|:-----:|:----:|:-------------:|:------:|
| Basic functionality works          | ✓ | ✓ |       ✓       | ✓ |
| Edge cases handled                 | | ✓ |       ✓       | ✓ |
| Unit tests                         | ✓ | ✓ |       ✓       | ✓ |
| Integration tests                  | Basic | Comprehensive | Comprehensive | Comprehensive |
| Covered by E2E test infrastructure | | |       ✓       | ✓ |
| Running in Production deployments | | |       ✓       | ✓ |
| Verified in production <br/> across multiple enterprises at scale      | | |               | ✓ |
| No known major bugs                | | ✓ |       ✓       | ✓ |
| Basic documentation                | ✓ | ✓ |       ✓       | ✓ |
| Complete documentation             | | ✓ |       ✓       | ✓ |
| Error handling guidelines          | | ✓ |       ✓       | ✓ |
| Example dashboard coverage         | |  |       ✓       | ✓ |
| Metrics & tracing                  | Basic | ✓ |       ✓       | ✓ |
| Anonymous Telemetry                |  |  |       ✓       | ✓ |
| Config breaking changes            | Expected | Unlikely |    Frozen     | Backward compatible only |
| Production use                     | Not recommended | Early adopters |   Suitable    | Fully supported |