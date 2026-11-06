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

Release lines cut before `publish_release` existed (2.4 and earlier) still publish from a pushed tag: for those, follow the steps in that release branch's copy of this template.

- [ ] Cherry-pick release notes onto the release branch.
- [ ] Start [build_release](https://github.com/spiceai/spiceai/actions/workflows/build_release.yml) on the release branch with `release_version` set to the version (e.g. `1.0.1`), once the branch head is the commit that passed testing. `version.txt` at that commit must match, or the build fails. Do not push the tag: `publish_release` creates it.
- [ ] Check that the build_release run is green. It builds every binary, CUDA included, and publishes nothing.
- [ ] Create a **draft** [GitHub Release](https://github.com/spiceai/spiceai/releases/new) for tag `v1.0.1` (a new tag, created on publish) targeting the release branch. Leave the body empty to use the checked-in notes, or write the notes in the draft.
- [ ] Start [publish_release](https://github.com/spiceai/spiceai/actions/workflows/publish_release.yml) on the release branch with the same `release_version`. It publishes the build of the branch head at the moment it starts, so start it before anything else merges to the release branch.
- [ ] Approve its `publish` job, then the two Docker push jobs when they wait for approval, as a release approver other than the person who started the run. `publish` attaches the binaries and publishes the release, which creates the tag at the built commit; the push jobs push the Docker images. A stable release that is the highest version becomes the latest release and moves the `latest` Docker tags; a pre-release never does. To retry a failed step, re-run the failed jobs of the same run.
- [ ] Check the published release assets and the `spiceai/spiceai` and `ghcr.io/spiceai/spiceai` image tags.
- [ ] Perform a final test pass on the released binaries and Docker images.
- [ ] Notify that the OSS release is complete and ready for communications in the release coordination channel.

## Post-Release Housekeeping

- [ ] Update `SECURITY.md`: the latest open-source release, and the versions Spice.ai Enterprise currently supports (pre-release, latest, previous minor release line, LTS).
