---
name: Enterprise Milestone Endgame
about: Ship a milestone release for Spice.ai Enterprise!
title: 'v1.x.x Endgame'
labels: 'kind/endgame'
assignees: ''
---

## DRIs

| Role    | DRI |
| ------- | --- |
| Endgame |     |
| QA      |     |

## Milestone Release Timeline

| Date            | Description  |
| --------------- | ------------ |
| Planning        |              |
| Branch Creation |              |
| Release         |              |
| Announcement    |              |


## Associated Changes
- OSS Endgame (if applicable): `https://github.com/spiceai/spiceai/issues/<issue-number>`
  - If no associated open source endgame has been completed, an [`end-game.md`](https://github.com/spicehq/spiceai/blob/trunk/.github/ISSUE_TEMPLATE/end_game.md) should be opened, to track additional required steps (mentioned below)
- PR upstreaming `trunk` from `spiceai/spiceai` into `spicehq/spiceai`: `#pr-num`

## Release Branch Creation

- [ ] **Minor & Major Release (vX.Y or vX.0)**: Create `release/X.Y` from `trunk` one day before release.
  - Refer to [docs/RELEASE.md](https://github.com/spicehq/spiceai/blob/trunk/docs/RELEASE.md).
- [ ] Lock the branch to critical fixes only and notify the team.

## Pre-Release Testing & Validation
1. **OSS Endgame validation**
  - [ ] Ensure all pre-release and post-release testing and verification is complete without non-acceptable failures.

1. **Build Validations**

   - [ ] Ensure [builds pass](https://github.com/spicehq/spiceai/actions/workflows/build_and_release.yml) on **Linux and Windows**.
   - [ ] Verify all CI workflows complete without warnings or errors.

1. **Unit/Integration Tests**

   - [ ] Confirm local and CI tests pass without major failures.
     - [ ] Verify [integration](https://github.com/spicehq/spiceai/actions/workflows/integration.yml) tests (which include the `run_all_tests` flag) is green on the release branch.

1. **E2E Tests**
   - [ ] Verify [E2E Test CI (core)](https://github.com/spicehq/spiceai/actions/workflows/e2e_test_ci.yml) is green on `trunk` and the release branch.
   - [ ] Verify [E2E Test CI (models)](https://github.com/spicehq/spiceai/actions/workflows/e2e_test_ci_models.yml) is green on `trunk` and the release branch.
   - [ ] Verify [Test Operator Benchmarks](https://github.com/spicehq/spiceai/actions/workflows/testoperator_run_bench.yml) is green on `trunk` and the release branch.
     - Use the [Test Operator Dispatch](https://github.com/spicehq/spiceai/actions/workflows/testoperator_dispatch.yml) workflow to execute a new benchmark run. Specify `trunk` as the branch source, with the following parameters:
       - Workflow to execute: `bench`
       - All other values left empty.
   - [ ] Verify [E2E Test CLI](https://github.com/spicehq/spiceai/actions/workflows/e2e_test_spice_cli.yml) is green on `trunk` and the release branch.
     - Parameters: Branch: `trunk`
     - Build CLI: `true`
   - [ ] Verify [Throughput Tests](https://github.com/spicehq/spiceai/actions/workflows/testoperator_run_throughput.yml) is green on `trunk` and the release branch.
     - Use the [Test Operator Dispatch](https://github.com/spicehq/spiceai/actions/workflows/testoperator_dispatch.yml) workflow to execute a new throughput run. Specify `trunk` as the branch source, with the following parameters:
       - Workflow to execute: `throughput`
       - All other values left empty.

1. **Cookbook Recipes**

- [ ] Cookbook recipes were tested in associated OSS end game, OR
- [ ] Cookbooks tested in this endgame, tracked in this associated [`end-game.md`](https://github.com/spicehq/spiceai/blob/trunk/.github/ISSUE_TEMPLATE/end_game.md) issue: `<endgame-tracking-cookbook-testing>`

## Final Updates

- [ ] Prepare and finalize enterprise release notes:

  - [ ] Duplicate OSS release notes as the base version for enterprise notes
  - [ ] Acknowledge external and new contributors.
  - [ ] List notable dependency updates (e.g. `datafusion`, `datafusion-table-providers`) under `## Dependencies`.
  - [ ] Summarize any cookbook changes under `## Cookbook`.
  - [ ] Run [Generate Changelog](https://github.com/spicehq/spiceai/actions/workflows/generate_changelog.yml) to update the release notes.
    - Use parameters:
      - Previous Release Tag: the previous release tag (e.g. `v1.4.0`). This is the tag of the previous release we want to compare against.
      - Release Branch: the release branch (e.g. `release/1.5`). This is the branch that contains the new changes that are based on the release branch. If this is a prep branch, use that branch here.
      - The script will generate the changes it believes are in the release. It also generates a list of changes that are only in trunk but not in the release branch, but this is not always accurate. Please review the changes and copy any changes that are missing into the release notes changelog.

- [ ] [Generate Spicepod JSON schema](https://github.com/spicehq/spiceai/actions/workflows/generate_json_schema.yml) and cherry-pick schema update PR onto the release branch.

- [ ] Verify `version.txt` and version in `Cargo.toml` using [docs/RELEASE.md](https://github.com/spicehq/spiceai/blob/trunk/docs/RELEASE.md#version-update).

- [ ] Verify or update the [Helm chart](https://github.com/spicehq/spiceai/blob/trunk/deploy/chart) (chart version & image.tag) in the release branch (not in trunk).
  - [ ] If this is a **minor** release, replace the `ghcr.io/spicehq/spiceai-nightly` repository in `values.yaml` with `spicehq/spiceai` and change the tag to the release version (e.g. `1.0.0-enterprise`).

- [ ] **QA DRI sign-off** and **Docs DRI sign-off** confirming readiness and completeness.

## Release Publication Steps
- [ ] Cherry-pick release notes onto the release branch.
- [ ] Create a **pre-release** [GitHub Release](https://github.com/spicehq/spiceai/releases/new) with a tag (e.g. `v1.0.0-rc.1-enterprise`). Leave the body empty so automation can populate it from the checked-in notes.

### Post-Docker builds
Upon the completion of the [spiced_docker](https://github.com/spicehq/spiceai/actions/workflows/spiced_docker.yml) run associated with the above create Github release:
- [ ] Deploy the new docker image to several apps in dev [SCP](https://dev.spice.ai/). Confirm upgrade and functionality.
- [ ] Deploy the new docker image to demo and public apps [SCP](https://spice.ai/). Confirm upgrade and functionality.
  - spicehq:
    - https://spice.ai/spicehq/s3-vectors-demo
    - https://spice.ai/spicehq/s3-ai-database
    - https://spice.ai/spicehq/team-app
    - https://spice.ai/spicehq/marketing
    - https://spice.ai/spicehq/nginx-demo
    - https://spice.ai/spicehq/databricks-demo
    - https://spice.ai/spicehq/iceberg-ai-demo
    - https://spice.ai/spicehq/ai-platform
    - https://spice.ai/spicehq/embedding-server
  - spiceai:
    - https://spice.ai/spiceai/react
    - https://spice.ai/spiceai/tpch
    - https://spice.ai/spiceai/spiceai
    - https://spice.ai/spiceai/vercel-ai-sdk
    - https://spice.ai/spiceai/nginx
    - https://spice.ai/spiceai/nextjs
    - https://spice.ai/spiceai/dremio
    - https://spice.ai/spiceai/fed-demo
    - https://spice.ai/spiceai/docs
    - https://spice.ai/spiceai/quickstart
    - https://spice.ai/spiceai/tailwindcss
- [ ] Run [Publish to AWS Marketplace](https://github.com/spicehq/spiceai/actions/workflows/aws_marketplace_publish.yml) with both configurations (these will require manual approval by either @lukekim or @phillipleblanc):
 - Standard
   - Docker image: `X.Y.Z-models`
   - Target ECR repository: `spice-ai/spiceai-enterprise`
   - Platforms to publish: `linux/amd64,linux/arm64`
 - BYOL
   - Docker image: `X.Y.Z-models`
   - Target ECR repository: `spice-ai/spiceai-enterprise-byol`
   - Platforms to publish: `linux/amd64,linux/arm64`

- [ ] Mark the [release](https://github.com/spicehq/spiceai/releases) as official once all binaries and Docker images finish building.

- [ ] Perform a final test pass on the released binaries and Docker images.

## Post-Release Validation and Housekeeping
- [ ] Notify the team to verify [customer fixes](https://github.com/orgs/spicehq/projects/53)(if any):
  ```md
  :rocket: **Enterprise release complete!** Please confirm enterprise customer fixes have been merged and resolved.
  1. [Constraint violation check is improved to control behavior when violations occur within a batch](https://github.com/spicehq/customer-twilio/issues/77). DRI: @phillipleblanc
  2. ..
```
- [ ] Bump `version.txt` and `Cargo.toml` in `trunk` to the next planned **minor** release (if required).
- [ ] Update the supported version in `SECURITY.md` if necessary.
