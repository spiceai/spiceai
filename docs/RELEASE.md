# Spice.ai Release Process

Spice.ai releases once per week.

## Branching Policy

- At the beginning of the milestone for a target release a release branch is created, i.e. `release-v0.14.0-alpha`, in the main spiceai repository (not in a fork).
- PRs are merged to the trunk branch incrementally as work is completed on individual features/bugs.
- Once a feature/bugfix is considered "done" and ready to be included in the release, a new branch is created from the current release branch, all commits related to the feature/bugfix are cherry-picked into the new branch. And then a PR is created from the new branch into the release branch. That PR will be a regular merge, not a squash merge (IMPORTANT).
- Any commits made directly to a release branch should be merged to the trunk branch.
- If the decision to re-number the release is made by the release DRI, a new release branch with the correct version number is made from the existing release branch. The existing release branch is then deleted.
- The engineer responsible for a given feature/bugfix is responsible for ensuring that the feature/bugfix is included in the release branch.

At release time, we ensure all features/bug fixes we want from trunk have been included via the policy outlined above. Commits that should be not part of the release are not brought into the release branch. Each feature will be a single commit in the release branch, regardless of how many commits/PRs were made to the trunk branch.

## Endgame issue

Create a [Milestone Endgame](https://github.com/spiceai/spiceai/issues/new?assignees=&labels=endgame&projects=&template=end_game.md&title=v0.x.x-alpha+Endgame) issue to track the checklist needed for a specific milestone release.

## Version update

- Create a PR updating version.txt to the next planned version number.
  - i.e. `0.2.1-alpha` -> `0.3-alpha`
- Ensure the release notes at `docs/release_notes/v{version}.md` exist
- Create a new pre-release [GitHub release](https://github.com/spiceai/spiceai/releases/new) with placeholder information. Create a tag that matches the version to be released. The details of the release will be filled in by the automation.
  - Alternatively push a new tag via the git command line to the GitHub repository, and the release automation will be triggered.

## Acknowledgements update

- Run the `generate-acknowledgements` target and commit the result
