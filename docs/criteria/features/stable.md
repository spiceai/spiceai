# Feature Release Criteria

This document defines the set of criteria that is required before a feature is considered to be of Stable quality.

All criteria must be met for the feature to be considered Stable, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug).

---

## Stable Release Criteria

> *"It's running in production"* — The feature is fully supported and proven in production environments. Only backward-compatible changes are permitted.

### Functionality

- [ ] All [RC criteria](./rc.md#release-candidate-rc-criteria) continue to pass
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

- [ ] Feature has been validated in production environments across multiple enterprises at scale

