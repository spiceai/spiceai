# Feature Release Criteria

This document defines the set of criteria that is required before a feature is considered to be of Beta quality.

All criteria must be met for the feature to be considered Beta, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

---

## Beta Release Criteria

> *"It's working well"* — The feature handles edge cases reliably. Configuration changes are unlikely.

### Functionality

- [ ] All [Alpha criteria](./alpha.md#alpha-release-criteria) continue to pass
- [ ] Edge cases are handled gracefully (invalid input, boundary conditions, concurrent access)
- [ ] No known major bugs
- [ ] All minor bugs are documented with workarounds (if applicable)

### Testing

- [ ] All Alpha testing criteria continue to pass
- [ ] Comprehensive integration tests cover primary use cases, edge cases, and error paths
- [ ] Regression tests exist for all fixed bugs

### Configuration & API

- [ ] Configuration schema is considered stable
- [ ] **Breaking changes are unlikely** — any planned changes are documented
- [ ] Configuration validation provides clear error messages for invalid values
- [ ] Migration path documented for any breaking changes from Alpha

### Documentation

- [ ] All Alpha documentation criteria continue to pass
- [ ] Comprehensive documentation including all configuration options
- [ ] Feature has an easy to follow cookbook recipe
  - [ ] Cookbook recipe is added to our list of [endgame release](https://github.com/spiceai/spiceai/blob/trunk/.github/ISSUE_TEMPLATE/end_game.md) testing cookbooks.
- [ ] Any exceptions to Beta quality are explicitly documented

### Observability

- [ ] All Alpha observability criteria continue to pass
- [ ] Metrics follow established naming conventions
- [ ] Tracing is implemented for key operations
- [ ] Error messages follow [error handling guidelines](../../dev/error_handling.md)

### UX

- [ ] User-facing error messages are clear, actionable, and non-technical where appropriate
- [ ] Configuration experience is consistent with other features
