---
name: DataFusion Upgrade
about: Checklist for upgrading to a new major version of DataFusion.
title: 'Upgrade DataFusion to version X.Y.Z'
labels: enhancement
assignees: ''

---

This issue tracks the process of upgrading Spice OSS to a new major version of DataFusion to maintain a version that is one major release version behind the [latest](https://github.com/apache/datafusion/tags). Because many internal crates and forked dependencies rely on DataFusion, they all need to be upgraded in lockstep.

## Pre-upgrade Tasks

- [ ]  Read the DataFusion [changelog](https://github.com/apache/datafusion/tree/branch-49/dev/changelog) of the new version to identify breaking changes and new features.
- [ ]  Read the DataFusion [upgrade guides](https://datafusion.apache.org/library-user-guide/upgrading.html ).
- [ ]  Read the DataFusion [blog](https://datafusion.apache.org/blog/) for the latest release.

## Forked Dependency Upgrades

The following forked dependencies use DataFusion and need to be upgraded in lockstep. This typically involves pulling the latest changes from the upstream repository, resolving conflicts, and updating the commit hash in `Cargo.toml` (See [Core Dependency Upgrade](#core-dependency-upgrade)).

- [ ]  **datafusion**: Update the fork of the datafusion repo.
  - [ ] Branch from the lastest `spice-<X-1>` branch and name it `spice-<X>`.
  - [ ] Merge from the upstream release tag `vX.Y.Z`
- [ ]  **datafusion-federation**: Update the fork to be compatible with the new DataFusion version.
- [ ]  **datafusion-table-providers**: Update the fork to be compatible with the new DataFusion version.
  - Do not merge the into the `spiceai` branch until the main Spice OSS PR is ready to be merged. Merging sooner can block other PRs.
- [ ]  **iceberg-rust**: The `iceberg-datafusion` crate within this forked repository needs to be updated.

## Arrow Updates (if necessary)
Spice should use the same version of Arrow that DataFusion uses. If DataFusion upgraded Arrow, then the following steps should be performed.

- [ ]  **arrow-rs**: 
- [ ]  **snowflake-rs**: 
- [ ]  **delta-kernel-rs**: 
- [ ]  **duckdb-rs**: 

## Core Dependency Upgrade

- [ ]  Create a new branch for the upgrade process.
- [ ]  Update the `datafusion` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  If Arrow needs updating, update the `arrow-rs` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  Update the `datafusion-federation` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  Update the `datafusion-table-providers` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  Update the `datafusion-federation` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  Run `make build` to ensure the entire project compiles without errors.
  - [ ]  Address any compilation errors or test failures. This may involve fixing code that is incompatible with the new DataFusion version
- [ ]  Run all tests using `make build-cli nextest` to verify that all functionality is working as expected and snapshots have not changed.
- [ ]  Create a pull request with the changes.
- [ ]  Ensure all CI checks pass.
- [ ]  Build the branch version and test with test operator, updating snapshots if needed.
