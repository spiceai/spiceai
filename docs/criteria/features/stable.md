# Feature Release Criteria

This document defines the set of criteria that is required before a feature is considered to be of Stable quality.

All criteria must be met for the feature to be considered Stable, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug).

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

## Stable Release Criteria

> *"It's running in production"* — The feature is fully supported and proven in production environments. Only backward-compatible changes are permitted.

### Functionality

- [ ] All [RC criteria](#release-candidate-rc-criteria) continue to pass
- [ ] Feature has been running in production for a defined stabilization period

### Testing

- [ ] All RC testing criteria continue to pass

### Configuration & API

- [ ] **Only backward-compatible changes are permitted**
- [ ] Any future deprecations must go through a defined deprecation process:
    - Deprecation warning in release N
    - Continued functionality through release N+1 (minimum)
    - Removal no earlier than release N+2

### Documentation

- [ ] All RC documentation criteria continue to pass
- [ ] Feature is marked as "Stable" in all public-facing documentation

### Operational Readiness

- [ ] Feature has been validated in multiple production environments

