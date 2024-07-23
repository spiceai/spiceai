---
name: Milestone Endgame
about: Ship a milestone!
title: 'v0.x.x-alpha endgame'
labels: 'endgame'
assignees: ''
---

## DRIs

|         | DRI               |
| ------- | ----------------- |
| Endgame | <GitHub Username> |
| QA      | <GitHub Username> |
| Docs    | <GitHub Username> |
| Comms   | <GitHub Username> |

## Milestone Release Timeline

| Date         | Description            |
| ------------ | ---------------------- |
| Planning     | TBD (E.g. Mon, Mar 11) |
| Release      | TBD (E.g. Mon, Mar 11) |
| Announcement | TBD (E.g. Mon, Mar 11) |

## Planning Checklist

- [ ] Review the specific [GitHub Milestone](https://github.com/spiceai/spiceai/milestones)

## Release Checklist

- [ ] All features/bugfixes to be included in the release have been merged to the release branch (e.g. `release-v0.14.0-alpha`) following the [Branching Policy]
  - [ ] Verify all commits that should be included are in by viewing the GitHub compare view: i.e. https://github.com/spiceai/spiceai/compare/trunk...release-v0.13.2-alpha
- [ ] Full test pass and update if necessary over README.md
- [ ] Full test pass and update if necessary over Docs
- [ ] Full test pass and update if necessary over existing and new Samples
- [ ] Full test pass and update if necessary over existing and new Quickstarts
- [ ] Merge [Docs PRs](https://github.com/spiceai/docs/pulls)
- [ ] Merge Registry PRs
- [ ] Merge [Samples PRs](https://github.com/spiceai/samples/pulls)
- [ ] Merge [Quickstarts PRs](https://github.com/spiceai/quickstarts/pulls)
- [ ] Update release notes
  - [ ] Ensure any external contributors have been acknowledged.
- [ ] Update acknowledgements by triggering [Generate Acknowledgements](https://github.com/spiceai/spiceai/actions/workflows/generate_acknowledgements.yml) workflow
  - [ ] Update acknowledgements in [docs](https://github.com/spiceai/docs/blob/trunk/spiceaidocs/docs/acknowledgements/index.md)
- [ ] Verify `version.txt` and version in `Cargo.toml` are correct using [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md#version-update)
- [ ] Ensure [E2E Test CI](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_ci.yml) is green on the release branch.
- [ ] QA DRI sign-off
- [ ] Docs DRI sign-off
- [ ] Release the new version by creating a `pre-release` [GitHub Release](https://github.com/spiceai/spiceai/releases/new) with the tag from the release branch. E.g. `v0.9.1-alpha`
- [ ] Release any docs updates by creating a `v[semver]` tag.
- [ ] Trigger algolia search crawler [workflow](https://github.com/spiceai/docs/actions/workflows/trigger_search_reindex.yml), to reindex updated docs.
- [ ] Update the [Helm chart](https://github.com/spiceai/spiceai/blob/trunk/deploy/chart) version (image.tag version & chart version). Ensure [docker build](https://github.com/spiceai/spiceai/actions/workflows/spiced_docker.yml) for the tag from the release branch completed (~2 hours) and trigger the [Release Chart](https://github.com/spiceai/helm-charts/actions/workflows/release.yml) workflow.
- [ ] Final test pass on released binaries
- [ ] Run [E2E Test Release Installation](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_release_install.yml)
- [ ] Update `version.txt` and version in `Cargo.toml` to the next release version.
- [ ] Update versions in [brew taps](https://github.com/spiceai/homebrew-spiceai)
- [ ] Remove the released version from the [ROADMAP](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md)
- [ ] Create a new branch `release-v[semver]` for the next release version from the current release branch. E.g. `release-v0.14.0-alpha`

## Announcement Checklist

- [ ] X: [@spice_ai](https://twitter.com/spice_ai)
- [ ] Reddit: [reddit.com/r/spiceai](https://reddit.com/r/spiceai)
- [ ] Discord: [#announcements](https://discord.gg/zv8ahzZVpf)
- [ ] Telegram: [spiceai](https://t.me/spiceai)
- [ ] Blog: [blog.spiceai.org](https://blog.spiceai.org)

## PR reference

- PR 1.
- PR 2.
- etc.

[Branching Policy]: https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md
