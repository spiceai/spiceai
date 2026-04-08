---
name: Enterprise Milestone Endgame
about: Ship a milestone release for Spice.ai Enterprise!
title: 'v1.x.x-enterprise Endgame'
labels: 'kind/endgame'
assignees: ''
---

## DRIs

| Role    | DRI |
| ------- | --- |
| Endgame |     |
| QA      |     |

## Milestone Release Timeline

| Date            | Description |
| --------------- | ----------- |
| Planning        |             |
| Branch Creation |             |
| Release         |             |
| Announcement    |             |

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
   - [ ] Ensure [builds pass](https://github.com/spicehq/spiceai/actions/workflows/ent_build_and_release.yml) on **Linux and Windows**.
   - [ ] Verify all CI workflows complete without warnings or errors.

1. **Unit/Integration Tests**
   - [ ] Confirm local and CI tests pass without major failures.
     - [ ] Verify [integration](https://github.com/spicehq/spiceai/actions/workflows/ent_integration.yml) tests (which include the `run_all_tests` flag) is green on the release branch.

1. **E2E Tests**
   - [ ] Verify [E2E Test CI (core)](https://github.com/spicehq/spiceai/actions/workflows/ent_e2e_test_ci.yml) is green on `trunk` and the release branch.
   - [ ] Verify [E2E Test CI (models)](https://github.com/spicehq/spiceai/actions/workflows/e2e_test_ci_models.yml) is green on `trunk` and the release branch.
   - [ ] Verify [Test Operator Benchmarks](https://github.com/spicehq/spiceai/actions/workflows/testoperator_run_bench.yml) is green on `trunk` and the release branch.
     - Use the [Test Operator Dispatch](https://github.com/spicehq/spiceai/actions/workflows/ent_testoperator_dispatch.yml) workflow to execute a new benchmark run. Specify `trunk` as the branch source, with the following parameters:
       - Workflow to execute: `bench`
       - All other values left empty.
   - [ ] Verify [Throughput Tests](https://github.com/spicehq/spiceai/actions/workflows/ent_testoperator_run_throughput.yml) is green on `trunk` and the release branch.
     - Use the [Test Operator Dispatch](https://github.com/spicehq/spiceai/actions/workflows/ent_testoperator_dispatch.yml) workflow to execute a new throughput run. Specify `trunk` as the branch source, with the following parameters:
       - Workflow to execute: `throughput`
       - All other values left empty.
   - [ ] Verify [Customer Datasets Benchmarks](https://github.com/spicehq/spiceai/actions/workflows/testoperator_customers.yml) are green on the release branch.
     - Trigger and verify `bench` workflow
     - Trigger and verify `load` workflow

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

- [ ] Verify `version.txt` and version in `Cargo.toml` using [docs/RELEASE.md](https://github.com/spicehq/spiceai/blob/trunk/docs/RELEASE.md#version-update). It should be updated to the release version (e.g. `1.9.0-enterprise` or `1.10.0-rc.1-enterprise`).

- [ ] Verify or update the [Helm chart](https://github.com/spicehq/spiceai/blob/trunk/deploy/chart) (chart version & image.tag) in the release branch (not in trunk).
  - [ ] Ensure that the `image.repository` in `values.yaml` is `ghcr.io/spicehq/spiceai-enterprise` and change the tag to the release version (e.g. `1.9.0-enterprise-models` or `1.10.0-rc.1-enterprise-models`).
  - [ ] Ensure that the `version` in `Chart.yaml` is updated to the release version with `-helm` suffix (e.g. `1.9.0-enterprise-helm` or `1.10.0-rc.1-enterprise-helm`).

- [ ] **QA DRI sign-off** and **Docs DRI sign-off** confirming readiness and completeness.

## Release Publication Steps

- [ ] Cherry-pick release notes onto the release branch.
- [ ] Create a **pre-release** [GitHub Release](https://github.com/spicehq/spiceai/releases/new) with a tag (e.g. `v1.0.0-rc.1-enterprise`). Leave the body empty so automation can populate it from the checked-in notes.
- [ ] After both [build_and_release](https://github.com/spicehq/spiceai/actions/workflows/ent_build_and_release.yml) and [build_and_release_cuda](https://github.com/spicehq/spiceai/actions/workflows/ent_build_and_release_cuda.yml) workflows complete successfully, trigger the [spiced_docker_enterprise](https://github.com/spicehq/spiceai/actions/workflows/spiced_docker_enterprise.yml) workflow.

### Post-Docker Build Steps

- [ ] Ensure the [spiced_docker_enterprise](https://github.com/spicehq/spiceai/actions/workflows/spiced_docker_enterprise.yml) is completed and the image [was promoted](https://github.com/spicehq/ai-platform/actions/workflows/publish-spiceai-enterprise.yaml) to all stamps and environments
- [ ] Ask @lukekim, @phillipleblanc, or @ewgenius to add released tag to the `versions` and `default_tag` in [SCP Config in Vercel](https://vercel.com/spice/~/stores/edge-config/ecfg_pbglo8zol8rij7koia8ijd6t2fwe/items) for **dev**.
- [ ] Deploy the new docker image to several apps in dev [SCP](https://dev.spice.ai/). Confirm upgrade and functionality.
  - [ ] SQL Query
  - [ ] AI Chat
  - [ ] Search
  - spicehq:
    - https://dev.spice.ai/spicehq/scp-test-app-us-east-1-dev-aws
    - https://dev.spice.ai/spicehq/team-app
- [ ] Ask @lukekim, @phillipleblanc, or @ewgenius to add released tag to the `versions` and `default_tag` in [SCP Config in Vercel](https://vercel.com/spice/~/stores/edge-config/ecfg_pbglo8zol8rij7koia8ijd6t2fwe/items) for **prod**.
- [ ] Deploy the new docker image to demo and public apps [SCP](https://spice.ai/). Confirm upgrade and functionality:
  - [ ] SQL Query
  - [ ] AI Chat
  - [ ] Search
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
    - https://spice.ai/spicehq/scp-synthetics-test-app
    - https://spice.ai/spicehq/scp-synthetics-test-app-stateful
    - https://spice.ai/spicehq/scp-synthetics-test-app-aws
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
- [ ] Run [Publish to AWS Marketplace](https://github.com/spicehq/spiceai/actions/workflows/aws_marketplace_publish.yml) using the following configuration:
  - Docker image: `X.Y.Z-enterprise-models` (i.e. `1.10.0-rc.1-enterprise-models`)
  - Target ECR repositories: `spice-ai/spiceai-enterprise-byol,spice-ai/spiceai-enterprise-plan` (both)
  - Platforms to publish: `linux/amd64,linux/arm64`
- [ ] Mark the [release](https://github.com/spicehq/spiceai/releases) as official once all binaries and Docker images finish building.

- [ ] Perform a final test pass on the released binaries and Docker images.

- [ ] Trigger [Platform OpenAPI Spec generation workflow](https://github.com/spicehq/ai-platform/actions/workflows/generate-openapi.yml) to include Spice OSS OpenAPI spec updates, if any exist.
  - [ ] Update HTTP API Reference in https://docs.spice.ai if needed.

- [ ] Run [(production) Build AMI from Enterprise release](https://github.com/spicehq/spiceai-ami/actions/workflows/build-ami-enterprise-release-production.yml)
  - Release tag: `vX.Y.Z-enterprise` (i.e. `v1.10.0-rc.1-enterprise`)
  - Asset name to download: `spiced_models_linux_x86_64`

- [ ] Update the AWS Marketplace listings to point to both the new AMI and Docker image.
  - [ ] Activate `Marketplace Admins` JIT access in the [Azure PIM portal](https://portal.azure.com/#view/Microsoft_Azure_PIMCommon/ActivationMenuBlade/~/aadgroup)
  - [ ] Log into the [AWS Console](https://spiceai.awsapps.com/start/#/) using the Marketplace Admins SSO role for the Production account (903332016402).
  - [ ] Navigate to the [AWS Marketplace Management Portal](https://aws.amazon.com/marketplace/management/products/server?region=us-east-1)
  - [ ] For the [Spice.ai Enterprise (BYOL)](https://aws.amazon.com/marketplace/management/products/prod-pkjfmapef2dlu/overview) listing, add a new version:
    - Load the most recent request to "Add new version"
    - In a separate window on the product page, click "Request changes" > "Update versions" > "Add new version".
    - Fill in the Version title and Release notes - you may need to tweak the release notes to remove emojis and the changelog.
    - Add the Helm and Container Deployment delivery options.
    - Refer to the previous release for guidance on filling out the rest of the form.
    - **IMPORTANT**: Copy the usage instructions from the previous version and update any reference to the release tag to the current version. By default when you create a new version, the usage instructions are reset to a super old version.
    - Submit the changes for review.
  - [ ] For the [Spice.ai Enterprise for AWS](https://aws.amazon.com/marketplace/management/products/prod-qfzcrwtpve3ho/overview) listing, add a new version:
    - Load the most recent request to "Add new version"
    - In a separate window on the product page, click "Request changes" > "Update versions" > "Add new version".
    - Fill in the Version title and Release notes - use the same release notes as the BYOL listing.
    - Add the AMI with CloudFormation delivery option.
    - Use the AMI ID generated from running the [(production) Build AMI from Enterprise release](https://github.com/spicehq/spiceai-ami/actions/workflows/build-ami-enterprise-release-production.yml) workflow.
    - Refer to the previous release for guidance on filling out the rest of the form.
    - **IMPORTANT**: Copy the usage instructions from the previous version and update any reference to the AMI ID to the current version. By default when you create a new version, the usage instructions are reset to a super old version.
    - Submit the changes for review.
  - [ ] Validate that the AWS Marketplace listings are updated - they can take an hour to update.

## Post-Release Validation and Housekeeping

- [ ] Bump `version.txt` and `Cargo.toml` in `trunk` to the next planned **minor** release (if required).
- [ ] Update the supported version in `SECURITY.md` if necessary.
- [ ] Notify that the Enterprise and Cloud releases are complete and ready for communications in the release coordination channel.
