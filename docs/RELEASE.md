# Spice.ai Release Process

## Endgame issue

Create an endgame issue with the following content:

```
- [ ] Ensure all outstanding `spiceai` feature PRs are merged
- [ ] Full test pass and update if necessary over README.md (please get screenshots!)
- [ ] Full test pass and update if necessary over Docs (please get screenshots!)
- [ ] Full test pass and update if necessary over new Samples (please get screenshots/videos!)
- [ ] Full test pass and update if necessary over new Quickstarts (please get screenshots/videos!)
- [ ] Merge Docs PRs
- [ ] Merge Registry PRs
- [ ] Merge Samples PRs
- [ ] Merge Quickstarts PRs
- [ ] Merge version rev, docs and release notes
- [ ] Final test pass on released binaries
- [ ] Discord announcement
- [ ] Email announcement

PR reference:
```

## Version update

- Create a PR updating version.txt to remove the `-rc` flag.
  - i.e. `0.1.0-alpha.4-rc` -> `0.1.0-alpha.4`
- Ensure the release notes at `docs/release_notes/v{version}.md` already exists (or add to the version bump PR)
- Merge the PR created in step 1. Verify the releases are created appropropriately with the right version tag.
- Create a new PR updating version.txt to bump to the next version with the `-rc` flag.
  - i.e. `0.1.0-alpha.4` -> `0.1.0-alpha.5-rc`
- Merge that PR and verify the new RC release is created.
