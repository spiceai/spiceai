# Spice.ai Release Process

## Release environments

|                     | Production                     | Development                 | Local                         |
| ------------------- | ------------------------------ | --------------------------- | ----------------------------- |
| Short name          | prod                           | dev                         | local                         |
| spiced Docker image | ghcr.io/spiceai/spiceai:latest | ghcr.io/spiceai/spiceai:dev | ghcr.io/spiceai/spiceai:local |
| Spice Rack URL      | https://api.spicerack.org      | https://dev.spicerack.org   | http://localhost:80           |

## Endgame issue

Create an endgame issue with the following content:

```markdown
- [ ] Ensure all outstanding `spiceai` feature PRs are merged
- [ ] Full test pass and update if necessary over README.md (please get screenshots!)
- [ ] Full test pass and update if necessary over Docs (please get screenshots!)
- [ ] Full test pass and update if necessary over new Samples (please get screenshots/videos!)
- [ ] Full test pass and update if necessary over new Quickstarts (please get screenshots/videos!)
- [ ] Merge Docs PRs
- [ ] Merge Registry PRs
- [ ] Merge Samples PRs
- [ ] Merge Quickstarts PRs
- [ ] Merge release notes
- [ ] Update acknowledgements
- [ ] Update data-components-contrib repo with latest tag and reference it in spiceai. See [Components contrib version update](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md#components-contrib-version-update)
- [ ] Update version using documented process at [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md#version-update)
- [ ] Final test pass on released binaries
- [ ] Reddit announcement to [reddit.com/r/spiceai](https://reddit.com/r/spiceai)
- [ ] Discord announcement to [#announcements](https://discord.gg/zv8ahzZVpf)
- [ ] For major releases, [blog.spiceai.org](https://blog.spiceai.org) announcement
- [ ] For major releases, email announcement

## PR reference:
```

## Version update

- Major and minor updates can drop the patch. i.e. `v0.3` not `v0.3.0`
- Create a PR updating version.txt to the next planned version number.
  - i.e. `0.2.1-alpha` -> `0.3-alpha`
- Ensure the release notes at `docs/release_notes/v{version}.md` exist
- Create a new pre-release [GitHub release](https://github.com/spiceai/spiceai/releases/new) with placeholder information. Create a tag that matches the version to be released. The details of the release will be filled in by the automation.
  - Alternatively push a new tag via the git command line to the GitHub repository, and the release automation will be triggered.

## Acknowledgements update

- Run the `generate-acknowledgements` target and commit the result for each in:
  - spiceai
  - quickstarts
  - samples
  - data-components-contrib
