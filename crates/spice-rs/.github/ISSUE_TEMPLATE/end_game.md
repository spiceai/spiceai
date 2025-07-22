---
name: Milestone Endgame
about: Ship a milestone!
title: 'v0.x.x endgame'
labels: 'kind/endgame'
assignees: ''
---

## DRIs

|         | DRI |
| ------- | --- |
| Endgame |     |
| QA      |     |
| Docs    |     |

## Planning Checklist

- [ ] Review the specific [GitHub Milestone](https://github.com/spiceai/spice-rs/milestones)

## Release Checklist

- [ ] All features/bugfixes to be included in the release have been merged to trunk
- [ ] Full test pass and update if necessary over [README.md](https://github.com/spiceai/spice-rs/blob/trunk/README.md)
- [ ] Full test pass and update if necessary over Docs
  - [ ] [docs.spiceai.org](https://docs.spiceai.org/sdks/rust)
  - [ ] [docs.spice.ai](https://github.com/spicehq/docs/tree/trunk/sdks/rust-sdk)
- [ ] Test the [`spice-rs` sample](https://github.com/spiceai/samples/tree/trunk/client-sdk/spice-rs-sdk-sample) using the latest `trunk` SDK version.
- [ ] Update [release notes](https://github.com/spiceai/spice-rs/blob/trunk/docs/release_notes)
  - [ ] Ensure all contributors have been acknowledged.
- [ ] Verify the version in `Cargo.toml` is correct and match the milestone version.
- [ ] Run [Test CI](https://github.com/spiceai/spice-rs/actions/workflows/build.yml) and ensure it is green on the trunk branch.
- [ ] QA DRI sign-off
- [ ] Docs DRI sign-off
- [ ] Create a new branch `release-v[semver]` for the release from trunk. E.g. `release-v3.0.0`
- [ ] Release the new version by creating and publishing a latest [GitHub Release](https://github.com/spiceai/spice-rs/releases/new) with the tag from the release branch. E.g. `v3.0.0`.
- [ ] Ensure the [publish](https://github.com/spiceai/spice-rs/actions/workflows/publish.yml) workflow has triggered, and successfully published the package.
- [ ] Run a test pass using the [`spice-rs` sample](https://github.com/spiceai/samples/tree/trunk/client-sdk/spice-rs-sdk-sample) using the latest published version.
- [ ] Update the version in `Cargo.toml` to the next release version.
- [ ] The SDK release is added to the next [Spice release notes](https://github.com/spiceai/spiceai/tree/trunk/docs/release_notes)
