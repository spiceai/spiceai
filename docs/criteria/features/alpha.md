# Feature Release Criteria

This document defines the set of criteria that is required before a feature is considered to be of Alpha quality.

All criteria must be met for the feature to be considered Alpha. As Alpha signifies the lowest release quality, criteria exceptions are not permitted.

---

## Definitions

### Release Stages

| Stage | Summary | API/Config Stability | Production Use |
|-------|---------|---------------------|----------------|
| **Alpha** | It's working | Breaking changes expected | Not recommended |
| **Beta** | It's working well | Breaking changes unlikely | Early adopters only |
| **RC** | It's ready for release | Frozen (no breaking changes) | Suitable for production |
| **Stable** | It's running in production | Backward compatible only | Fully supported |

---

## Alpha Release Criteria

> *"It's working"* — The feature functions in basic scenarios. Configuration and API are subject to change.

### Functionality

- [ ] Core functionality works in basic/happy-path scenarios
- [ ] Feature can be enabled and configured (even if configuration is minimal)
- [ ] No crashes or panics under normal operation
- [ ] Errors are returned (not swallowed) and include actionable information

### Testing

- [ ] Unit tests cover core functionality
- [ ] At least one integration test demonstrates the feature working end-to-end
- [ ] Manual testing has been performed by the development team

### Configuration & API

- [ ] Configuration parameters are documented (inline or in docs)
- [ ] **Breaking changes are expected** and should be communicated in release notes
- [ ] Default values are set for all optional parameters

### Documentation

- [ ] Basic usage documentation exists (README or docs page)
- [ ] Known limitations are documented
- [ ] Feature is marked as "Alpha" in all public-facing documentation

### Observability

- [ ] Errors are logged with sufficient context for debugging
- [ ] Basic metrics are emitted (if applicable to the feature type)
