---
name: Apache DataFusion Upgrade
about: Checklist for upgrading to a new major version of Apache DataFusion.
title: 'Upgrade Apache DataFusion to version X.Y.Z'
labels: enhancement
assignees: ''

---

This issue tracks the process of upgrading Spice OSS to a new major version of Apache DataFusion. We endeavor to maintain a version that is one major release version behind the [latest](https://github.com/apache/datafusion/tags). This is a complex process because many of our internal crates and forked dependencies rely on DataFusion. Ensuring they all use the same version is critical for stability and performance.

## Pre-upgrade Tasks

- [ ]  Read the Apache DataFusion [changelog](https://github.com/apache/datafusion/tree/branch-49/dev/changelog) of the new version to identify breaking changes and new features.
- [ ]  Create a new branch for the upgrade process.

## Forked Dependency Upgrades

The following forked dependencies use DataFusion and need to be upgraded to maintain compatibility. This typically involves pulling the latest changes from the upstream repository, resolving conflicts, and updating the commit hash in our `Cargo.toml` (See [Core Dependency Upgrade](#core-dependency-upgrade)).

- [ ]  **datafusion**: Update our fork of the datafusion repo.
  - [ ] Branch from the lastest `spice-<X-1>` branch and name it `spice-<X>`. 
  - [ ] Merge from the upstream release tag `vX.Y.Z`
- [ ]  **datafusion-federation**: Update our fork to be compatible with the new DataFusion version.
- [ ]  **datafusion-table-providers**: Update our fork to be compatible with the new DataFusion version.
- [ ]  **iceberg-rust**: The `iceberg-datafusion` crate within this forked repository needs to be updated.

## Core Dependency Upgrade

- [ ]  Update the `datafusion` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  Update the `arrow-rs` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  Update the `datafusion-federation` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  Update the `datafusion-table-providers` dependency in the root `Cargo.toml` to the new patched commit.
- [ ]  Update the `datafusion-federation` dependency in the root `Cargo.toml` to the new patched commit.


## Build and Test

- [ ]  Run `make build` to ensure the entire project compiles without errors.
- [ ]  Run all tests using `make build-cli nextest` to verify that all functionality is working as expected and snapshots have not changed.
- [ ]  Address any compilation errors or test failures. This may involve fixing code that is incompatible with the new DataFusion version.

## Post-upgrade Tasks

- [ ]  Create a pull request with the changes.
- [ ]  Ensure all CI checks pass.
- [ ]  Build the branch version and test with test operator, updating snapshots if needed.
