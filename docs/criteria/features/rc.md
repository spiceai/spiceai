# Feature Release Criteria

This document defines the set of criteria that is required before a feature is considered to be of [RC](../definitions.md) quality.

All criteria must be met for the feature to be considered [RC](../definitions.md), with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug).

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

## Release Candidate (RC) Criteria

> *"It's ready for release"* — The feature has passed comprehensive testing and is ready for production use. Configuration is frozen.

### Functionality

- [ ] All [Beta criteria](#beta-release-criteria) continue to pass
- [ ] No known major bugs
- [ ] No known minor bugs that significantly impact user experience
- [ ] Performance meets documented requirements/SLAs

### Testing

- [ ] All Beta testing criteria continue to pass
- [ ] Feature included in E2E test infrastructure
- [ ] Chaos/failure testing has been conducted (if applicable)

### Configuration & API

- [ ] Configuration schema is **frozen** — no breaking changes permitted
- [ ] All configuration parameters have been reviewed for naming consistency

### Documentation

- [ ] All Beta documentation criteria continue to pass

### Observability

- [ ] All Beta observability criteria continue to pass
- [ ] Relevant metrics and queries are added to example dashboards

### Compatibility

- [ ] Backward compatibility with Beta configuration verified
- [ ] Interoperability with related features tested
