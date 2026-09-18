# Agent-instruction evals

Evals for the rules in `.github/copilot-instructions.md` (symlinked as
`CLAUDE.md`) — the instructions every agent working in this repo runs under.

A rule in that file is a prompt shipped to every session, and like any other
prompt it can be wrong, ignored, or actively harmful without anyone noticing.
The instructions are also read far more often than they are tested. These evals
exist so that a rule which matters enough to constrain every session can be
checked the way any other shipped behavior is checked.

An eval here is worth writing when the rule's failure mode is *invisible in the
output* — where following it and ignoring it produce work that looks the same,
so no reviewer would catch the difference by reading the result.

| Eval | Rule under test |
| --- | --- |
| [`evidence-rule/`](evidence-rule/) | Evidence — no claim without a reproduction |

Each directory holds its own `README.md`, `evals.json`, `score_eval.py`, and
frozen `fixtures/`, and follows the conventions of the prompt evals in
[`.github/prompts/evals/`](../prompts/evals/): a scorer that takes `--eval` and
`--output`, exits 0 only when every assertion passes, and prints per-assertion
evidence with `--json`.

Two rules carry over from there, and both matter more here than they do for a
prompt eval:

- **Fixtures are frozen.** Assertions name the exact numbers a fixture
  produces. Regenerating one silently voids them.
- **Verify the eval discriminates.** An assertion that passes whether or not
  the agent followed the rule is measuring nothing. Each eval directory ships a
  `selftest.py` that scores a following-the-rule arm against an ignoring-it arm
  and fails if they converge.
