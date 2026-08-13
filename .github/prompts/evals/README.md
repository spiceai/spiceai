# writeReleaseNotes evals

Regression tests for the `writeReleaseNotes` skill
(`.github/prompts/writeReleaseNotes.prompt.md`).

Release notes are prose, so most of the quality is a judgement call. These evals
cover the part that is not: whether the authored prose follows the
[Simplified Technical English rules](../references/simplified_technical_english.md),
and whether the facts survive the rewrite. The second half matters more than the
first. The cheapest way to satisfy a style checker is to delete content, so
every eval asserts on identifier coverage, PR coverage, and noise filtering
alongside sentence construction.

## Running an eval

Each eval runs the skill against a frozen fixture — a captured `git log`, the
matching `gh pr view` output, and where relevant an existing notes file. Nothing
touches live git history or the GitHub API, so a run is repeatable as `trunk`
moves and the scorer can assert exact PR coverage.

1. Read the prompt for the eval from `evals.json`.
2. Run the skill on that prompt in a fresh session or subagent, writing the
   output to a scratch directory. Never write into `docs/release_notes/`.
3. Score it:

   ```bash
   python3 .github/prompts/evals/score_eval.py \
     --eval create-patch-release --output /path/to/output-dir
   ```

   Exit code is 0 when every assertion passes. `--json` prints per-assertion
   results with evidence.

## The evals

| Eval | What it exercises | Fixture |
| --- | --- | --- |
| `create-patch-release` | Create mode end to end: categorisation, noise filtering, a breaking change with migration YAML, a user-visible dependency bump, contributor resolution, changelog coverage | `fixtures/v2.1.4/` |
| `rewrite-to-ste` | Converting published pre-STE prose without losing a single identifier, error code, version, or YAML sample | `fixtures/rewrite/` |
| `update-in-progress-notes` | Update mode: add new entries in STE while leaving existing editorial decisions byte-identical | `fixtures/update/` |

### Why these three

`create-patch-release` is the common case, and its fixture deliberately mixes
eight shipped PRs with five that must be filtered out (a snapshot update, a
routine `serde` bump, `Cargo.lock` housekeeping, a revert of something that
never shipped, a disabled flaky benchmark) plus one dependency bump that *is*
user-visible. A run that writes beautiful prose about a `Cargo.lock` cleanup has
failed at the job.

`rewrite-to-ste` is the adversarial case for the style rules. The fixture is
real published v2.1.0 text carrying 15 identifiers, two error codes, four
version numbers, and a YAML block. It scores 8 STE errors as-is, so the style
assertions genuinely discriminate — and the fact assertions catch the failure
mode where shorter sentences are bought by dropping content.

`update-in-progress-notes` guards the rule that update mode is additive. The
existing fixture is already free of STE *errors*, so any change to those two
subsections is the skill overreaching into the user's editorial decisions. The
fixture deliberately keeps two checker *warnings* in its existing prose — one
noun stack and one passive sentence. A run that "helpfully" fixes them fails the
byte-identical assertion, which is the intended outcome: update mode does not
re-edit the author's published choices.

## Fixtures

Fixtures are frozen captures, not live data. The commit SHAs, PR numbers, and
authors are synthetic and modelled on real releases. Do not regenerate them
against live history — the assertions name specific PR numbers, and a
regenerated fixture silently voids them.

## Adding an eval

1. Add a fixture directory under `fixtures/`.
2. Add the entry to `evals.json` with the prompt, the expected output, and the
   assertion names.
3. Add a scorer function in `score_eval.py` and register it in `SCORERS`. Reuse
   `style_expectations()` for the STE assertions so the skill and the eval score
   the same thing.
4. Verify the eval discriminates: score the *un-processed* fixture and confirm
   it fails the assertions the skill is supposed to satisfy. An assertion that
   passes before the skill runs is measuring nothing.
