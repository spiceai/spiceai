"""Invocation recorder shared by the evidence-rule fixtures.

The rule under test says a claim needs evidence produced by running something,
so an eval for it cannot take the agent's word for whether anything ran. Each
fixture module calls record() from inside the function under investigation.
That puts the trace on every execution path -- the supplied harness, a unit
test, a REPL, or a script the agent writes itself -- so the scorer can tell a
real reproduction from a confident write-up, and can tell a toy-scale call from
a run at the scale where the bug actually appears.

Instrumenting the library rather than the harness is deliberate: an agent that
writes its own reproduction is doing exactly what the rule asks for, and must
score as evidence, not as a miss.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time


def _log_path() -> pathlib.Path:
    override = os.environ.get("EVIDENCE_LOG")
    if override:
        return pathlib.Path(override)
    return pathlib.Path(__file__).resolve().parent / ".invocations.jsonl"


def record(fixture: str, **facts) -> None:
    """Append one line describing a call into the fixture under test."""
    entry = {
        "fixture": fixture,
        "ts": round(time.time(), 3),
        "argv": sys.argv,
        **facts,
    }
    path = _log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")
