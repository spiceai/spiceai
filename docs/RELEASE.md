# Spice.ai Release Process

Spice.ai releases once per week.

## Endgame issue

Create a [Milestone Endgame](https://github.com/spiceai/spiceai/issues/new?assignees=&labels=endgame&projects=&template=end_game.md&title=v0.x.x-alpha+Endgame) issue to track the checklist needed for a specific milestone release.

## Version update

- Major and minor updates can drop the patch in documentation and release notes. i.e. `v0.3` not `v0.3.0`. The actual version number in version.txt and Cargo.toml needs to be the full semver.
- Create a PR updating version.txt to the next planned version number.
  - i.e. `0.2.1-alpha` -> `0.3.0-alpha`
- Ensure the release notes at `docs/release_notes/v{version}.md` exist
- Create a new pre-release [GitHub release](https://github.com/spiceai/spiceai/releases/new) with placeholder information. Create a tag that matches the version to be released. The details of the release will be filled in by the automation.
  - Alternatively push a new tag via the git command line to the GitHub repository, and the release automation will be triggered.

## Acknowledgements update

- Run the `generate-acknowledgements` target and commit the result
