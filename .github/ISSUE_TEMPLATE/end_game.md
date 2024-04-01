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

- [ ] All outstanding `spiceai` feature PRs are merged
- [ ] Full test pass and update if necessary over README.md (please get screenshots!)
- [ ] Full test pass and update if necessary over Docs (please get screenshots!)
- [ ] Full test pass and update if necessary over existing and new Samples (please get screenshots/videos!)
- [ ] Full test pass and update if necessary over existing and new Quickstarts (please get screenshots/videos!)
- [ ] Merge [Docs PRs](https://github.com/spiceai/docs/pulls)
- [ ] Merge Registry PRs
- [ ] Merge [Samples PRs](https://github.com/spiceai/samples/pulls)
- [ ] Merge [Quickstarts PRs](https://github.com/spiceai/samples/pulls)
- [ ] Update release notes
- [ ] Update acknowledgements by triggering [Generate Acknowledgements](https://github.com/spiceai/spiceai/actions/workflows/generate_acknowledgements.yml) workflow
- [ ] Verify `version.txt` and version in `Cargo.toml` is correct using [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md#version-update)
- [ ] Ensure [E2E Test CI](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_ci.yml) is green on trunk branch
- [ ] QA DRI sign-off
- [ ] Docs DRI sign-off
- [ ] Create a new branch `release-v[semver]` and merge all relevant changes into it. E.g. `release-v0.9.1-alpha`
- [ ] Release the new version by creating a `pre-release` [GitHub Release](https://github.com/spiceai/spiceai/releases/new) with the tag from the release branch. E.g. `v0.9.1-alpha`
- [ ] Release any docs updates by creating a `v[semver]` tag.
- [ ] Final test pass on released binaries
- [ ] Run [E2E Test Release Installation](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_release_install.yml)
- [ ] Update `version.txt` and version in `Cargo.toml` to the next release version.
- [ ] Update versions in [brew taps](https://github.com/spiceai/homebrew-spiceai)
- [ ] Remove the released version from the [ROADMAP](docs/ROADMAP.md)

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
