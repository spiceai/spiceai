---
name: Hotfix
about: Ship a hotfix release for Spice.ai Open Source!
title: 'v1.x.x Hotfix'
labels: 'kind/hotfix'
assignees: ''
---

## DRIs

| Role           | DRI          |
|----------------|--------------|
| Primary        |              |
| QA + Secondary |              |

**Note**: Until this issue is closed, whenever Primary DRI goes offline, he/she should clearly handoff control of the release to the Secondary DRI in the
release coordination channel. Secondary DRI should positively acknowledge the handoff.

## Hotfix Branch Creation

- [ ] Create `release/X.Y.Z-prep` from `release/X.Y`
- [ ] Cherry-pick hotfix commits onto release branch
- [ ] Update version (patch)
- [ ] Prepare and finalize release notes

## Pre-Release Testing & Validation

1. **Build Validations**
   - [ ] Ensure all builds (including CUDA) pass on **Linux and Windows**.
   - [ ] Verify all CI workflows complete without warnings or errors.

1. **Unit/Integration Tests**
   - [ ] Confirm local and CI tests pass without major failures.
     - [ ] Verify [integration](https://github.com/spiceai/spiceai/actions/workflows/integration.yml) tests (which include the `run_all_tests` flag) is green on the release branch.

## Final Updates

- [ ] Verify `version.txt` and version in `Cargo.toml` using [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md#version-update).
- [ ] **QA DRI sign-off** confirming readiness and completeness.

## Release Publication Steps

- [ ] Cherry-pick release notes onto the release branch.
- [ ] Create a **pre-release** [GitHub Release](https://github.com/spiceai/spiceai/releases/new) with a tag (e.g. `v1.0.1`). Leave the body empty so automation can populate it from the checked-in notes.
- [ ] Perform a final test pass on the released binaries and Docker images.
- [ ] Notify that the OSS release is complete and ready for communications in the release coordination channel.

## Post-Release Housekeeping

- [ ] Update the supported version in `SECURITY.md` if necessary.
