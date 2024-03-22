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

## Milestone Release Timeline

| Date         | Description |
| ------------ | ----------- |
| Planning     | Mon, Mar 11 |
| Release      | Thu, Mar 14 |
| Announcement | Thu, Mar 14 |

## Checklist

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
- [ ] Update acknowledgements
- [ ] Verify `version.txt` and version in `Cargo.toml` is correct using [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md#version-update)
- [ ] QA DRI sign-off
- [ ] Docs DRI sign-off
- [ ] Create a new branch `release-v[semver]` and merge all relevant changes into it.
- [ ] Release the new version by creating a draft release with the tag from the release branch.
- [ ] Release any docs updates by creating a `v[semver]` tag.
- [ ] Final test pass on released binaries
- [ ] Reddit announcement to [reddit.com/r/spiceai](https://reddit.com/r/spiceai)
- [ ] Discord announcement to [#announcements](https://discord.gg/zv8ahzZVpf)
- [ ] For major releases, [blog.spiceai.org](https://blog.spiceai.org) announcement
- [ ] Update `version.txt` and version in `Cargo.toml` to the next release version.
- [ ] Update versions in [brew taps](https://github.com/spiceai/homebrew-spiceai)

## PR reference

- PR 1.
- PR 2.
- etc.
