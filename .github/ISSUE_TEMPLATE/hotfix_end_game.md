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

For release lines 2.4 and earlier, follow the release steps in that branch's copy of this template instead.

In these steps, `{version}` is the version in `version.txt` on the release branch, without a leading `v` (for example `1.0.1`).

- [ ] Cherry-pick the release notes (`docs/release_notes/v{version}.md`) onto the release branch. Without them, `publish_release` publishes a stable version as a pre-release.
- [ ] Run [build_release](https://github.com/spiceai/spiceai/actions/workflows/build_release.yml) on the release branch with `release_version` set to `{version}`. Don't create or push the release tag: `publish_release` creates it.
  - [ ] Wait for the [build_release run](https://github.com/spiceai/spiceai/actions/workflows/build_release.yml) to finish successfully.
- [ ] Create a [draft GitHub Release](https://github.com/spiceai/spiceai/releases/new) with the new tag `v{version}`, targeting the release branch. Leave the description empty to use the checked-in release notes, or paste the notes. Don't set the pre-release or latest options: `publish_release` sets them when it publishes.
- [ ] Run [publish_release](https://github.com/spiceai/spiceai/actions/workflows/publish_release.yml) on the release branch with `release_version` set to `{version}`. If commits that don't change the release were merged to the release branch after the build, set `commit` to the full SHA that `build_release` built.
  - [ ] Have a release approver other than the person who started the run approve the `publish` job. It publishes the release and marks it automatically: an RC as a pre-release, and a stable version as the latest release when it's the highest stable version released.
  - [ ] Check the [GitHub Release](https://github.com/spiceai/spiceai/releases) has its binaries and is marked as described above. There is nothing to mark by hand.
  - [ ] Monitor the [publish_release run](https://github.com/spiceai/spiceai/actions/workflows/publish_release.yml) for the Docker image builds. When they finish, approve the two Docker image publish jobs. They push the `{version}` and `{version}-cuda` image tags, and move `latest` and `latest-cuda` automatically when the release is the latest release.
  - [ ] Check the [Docker Hub](https://hub.docker.com/r/spiceai/spiceai/tags) and [GHCR](https://github.com/spiceai/spiceai/pkgs/container/spiceai) image tags.
  - If a job fails, re-run the failed jobs of the same run.
- [ ] **Latest release only:** Pin `v{version}` on install.spiceai.org. Follow "Updating the scripts" in `install/README.md` of spicehq/ai-platform-cloudflare, and merge the pull request once its `install-scripts` check passes. Until then, the `Check install.spiceai.org` job of the publish_release run warns with the pin to make.
- [ ] Perform a final test pass on the released binaries and Docker images.
- [ ] Run [E2E Test Release Installation](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_release_install.yml) with no inputs. Its `install.spiceai.org serves the latest release` job checks the release, commit and sha256 the site serves against GitHub, and fails until the pin above is live.
- [ ] Notify that the OSS release is complete and ready for communications in the release coordination channel.

## Post-Release Housekeeping

- [ ] Update `SECURITY.md`: the latest open-source release, and the versions Spice.ai Enterprise currently supports (pre-release, latest, previous minor release line, LTS).
