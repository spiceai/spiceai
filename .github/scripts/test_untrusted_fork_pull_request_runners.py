#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""Fork pull requests must not execute repository code on a self-hosted runner.

A pull_request run uses the workflow file from the merge commit and checks out
that same commit. These tests build that merge, evaluate each job's `if` the
way the workflow syntax evaluates it, and run the steps that invoke a file from
the checkout (or a `$/` action, which loads the merge commit). They fail while
a fork pull request still reaches one of those steps on a self-hosted label.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from dataclasses import dataclass, field
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"

# Obvious placeholder. The runner binds this where the workflow reads
# github.token / secrets.GITHUB_TOKEN. Tests compare against it and never
# print it.
JOB_TOKEN = "placeholder-job-token"

BASE_REPOSITORY = "spiceai/spiceai"
FORK_REPOSITORY = "external-contributor/spiceai"
# Branches that exist on the remote and that an outside contributor can open
# a pull request against. `develop` is not among them.
EXISTING_BASES = ("trunk", "release-1.0", "feature-azure-connector")

# GitHub-hosted runner labels. Anything else in runs-on is a self-hosted label.
HOSTED_LABEL = re.compile(r"^(ubuntu-|windows-|macos-)")

TRIGGER_KEYS = ("on", True)
STATUS_FN = re.compile(r"(?<![\w.])(success|failure|always|cancelled)\s*\(")
FILE_INVOKE = re.compile(
    r"""(?:python3?|bash|sh|source)\s+['\"]?(\.?/?[A-Za-z0-9_./-]+\.(?:py|sh|bash))"""
)
BUILD_TOOL = re.compile(
    r"(?:^|[;&|\n])\s*(cargo|make|npm|pnpm|yarn|go|pytest|mvn|gradle)\b"
)
SHELL_PIPE = re.compile(r"\|\s*(?:/bin/)?(?:bash|sh)\b")
RAW_URL = re.compile(r"https://raw\.githubusercontent\.com/[^/\s\"']+/[^/\s\"']+/[^/\s\"']+/(\S+)")
LOCAL_USE = re.compile(r"^(?:\$/|\./)")
EXPR_WRAP = re.compile(r"^\$\{\{\s*(.*?)\s*\}\}$", re.DOTALL)
EXPR_PART = re.compile(r"\$\{\{\s*(.*?)\s*\}\}", re.DOTALL)


def _triggers(doc: dict) -> dict:
    for key in TRIGGER_KEYS:
        if key in doc:
            raw = doc[key]
            break
    else:
        return {}
    if raw is None:
        return {}
    if isinstance(raw, str):
        return {raw: None}
    if isinstance(raw, list):
        return {str(item): None for item in raw}
    if isinstance(raw, dict):
        return raw
    return {}


def _branch_match(pattern: str, branch: str) -> bool:
    out = ["^"]
    index = 0
    while index < len(pattern):
        if pattern.startswith("**", index):
            out.append(".*")
            index += 2
        elif pattern[index] == "*":
            out.append("[^/]*")
            index += 1
        elif pattern[index] == "?":
            out.append("[^/]")
            index += 1
        else:
            out.append(re.escape(pattern[index]))
            index += 1
    out.append("$")
    return re.search("".join(out), branch) is not None


def _pull_request_types(spec: dict | None) -> set[str]:
    if not isinstance(spec, dict) or not spec.get("types"):
        return {"opened", "synchronize", "reopened"}
    return {str(item) for item in spec["types"]}


def matching_base(spec: dict | None) -> str | None:
    """A real base branch this pull_request trigger accepts, or None."""
    patterns: list[str] | None = None
    if isinstance(spec, dict) and spec.get("branches"):
        patterns = [str(item) for item in spec["branches"]]
    for branch in EXISTING_BASES:
        if patterns is None or any(_branch_match(pattern, branch) for pattern in patterns):
            return branch
    return None


def is_hosted(labels: list[str]) -> bool:
    return bool(labels) and all(HOSTED_LABEL.search(label) for label in labels)


class ExprError(ValueError):
    pass


class Parser:
    def __init__(self, text: str) -> None:
        self.text = text
        self.length = len(text)
        self.index = 0

    def skip(self) -> None:
        while self.index < self.length and self.text[self.index] in " \t\r\n":
            self.index += 1

    def peek(self) -> str:
        self.skip()
        if self.index >= self.length:
            return ""
        return self.text[self.index]

    def startswith(self, token: str) -> bool:
        self.skip()
        return self.text.startswith(token, self.index)

    def eat(self, token: str) -> bool:
        if self.startswith(token):
            self.index += len(token)
            return True
        return False

    def parse(self) -> object:
        value = self.parse_or()
        self.skip()
        if self.index != self.length:
            raise ExprError(f"trailing input at {self.index}: {self.text[self.index :]!r}")
        return value

    def parse_or(self) -> object:
        left = self.parse_and()
        while self.eat("||"):
            right = self.parse_and()
            left = left if _truthy(left) else right
        return left

    def parse_and(self) -> object:
        left = self.parse_not()
        while self.eat("&&"):
            right = self.parse_not()
            left = right if _truthy(left) else left
        return left

    def parse_not(self) -> object:
        if self.eat("!"):
            return not _truthy(self.parse_not())
        return self.parse_cmp()

    def parse_cmp(self) -> object:
        left = self.parse_unary()
        while True:
            op = None
            for candidate in ("==", "!=", "<=", ">=", "<", ">"):
                if self.eat(candidate):
                    op = candidate
                    break
            if op is None:
                return left
            right = self.parse_unary()
            left = _compare(op, left, right)

    def parse_unary(self) -> object:
        value = self.parse_atom()
        while True:
            if self.eat("."):
                name = self.ident(hyphen=True)
                value = _lookup(value, name)
            elif self.eat("["):
                key = self.parse_or()
                if not self.eat("]"):
                    raise ExprError("expected ]")
                value = _lookup(value, key)
            else:
                return value

    def parse_atom(self) -> object:
        self.skip()
        if self.eat("("):
            value = self.parse_or()
            if not self.eat(")"):
                raise ExprError("expected )")
            return value
        if self.peek() == "'":
            return self.string()
        if self.peek().isdigit() or (self.peek() == "-" and self._digit_ahead()):
            return self.number()
        name = self.ident(hyphen=False)
        if name == "":
            raise ExprError(f"expected expression at {self.index}")
        if self.eat("("):
            args: list[object] = []
            if not self.startswith(")"):
                while True:
                    args.append(self.parse_or())
                    if self.eat(","):
                        continue
                    break
            if not self.eat(")"):
                raise ExprError("expected )")
            return self.call(name, args)
        lowered = name.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        if lowered == "null":
            return None
        return _resolve_name(name)

    def _digit_ahead(self) -> bool:
        nxt = self.index + 1
        return nxt < self.length and self.text[nxt].isdigit()

    def ident(self, *, hyphen: bool) -> str:
        self.skip()
        start = self.index
        if start >= self.length or not (self.text[start].isalpha() or self.text[start] == "_"):
            return ""
        self.index += 1
        while self.index < self.length:
            char = self.text[self.index]
            if char.isalnum() or char == "_" or (hyphen and char == "-"):
                self.index += 1
                continue
            break
        return self.text[start : self.index]

    def string(self) -> str:
        self.index += 1
        chars: list[str] = []
        while self.index < self.length:
            char = self.text[self.index]
            self.index += 1
            if char == "'":
                if self.index < self.length and self.text[self.index] == "'":
                    chars.append("'")
                    self.index += 1
                    continue
                return "".join(chars)
            chars.append(char)
        raise ExprError("unterminated string")

    def number(self) -> int | float:
        self.skip()
        start = self.index
        if self.peek() == "-":
            self.index += 1
        while self.index < self.length and (self.text[self.index].isdigit() or self.text[self.index] == "."):
            self.index += 1
        token = self.text[start : self.index]
        if "." in token:
            return float(token)
        return int(token)

    def call(self, name: str, args: list[object]) -> object:
        key = name.lower()
        if key == "success":
            return _ctx().success
        if key == "failure":
            return not _ctx().success and not _ctx().cancelled
        if key == "always":
            return True
        if key == "cancelled":
            return _ctx().cancelled
        if key == "contains":
            if len(args) != 2:
                raise ExprError("contains() takes 2 arguments")
            return _contains(args[0], args[1])
        if key == "startswith":
            if len(args) != 2:
                raise ExprError("startsWith() takes 2 arguments")
            return str(_stringish(args[0])).lower().startswith(str(_stringish(args[1])).lower())
        if key == "endswith":
            if len(args) != 2:
                raise ExprError("endsWith() takes 2 arguments")
            return str(_stringish(args[0])).lower().endswith(str(_stringish(args[1])).lower())
        if key == "fromjson":
            if len(args) != 1:
                raise ExprError("fromJSON() takes 1 argument")
            import json

            return json.loads(str(args[0]))
        if key == "hashfiles":
            # Non-empty when the path exists in the checkout. An existence
            # check is not an authorization decision.
            return "present" if _ctx().hash_hit else ""
        raise ExprError(f"unsupported function {name}")


@dataclass
class Context:
    github: dict
    secrets: dict
    inputs: dict
    matrix: dict
    env: dict
    needs: dict
    success: bool = True
    cancelled: bool = False
    hash_hit: bool = False
    names: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.names = {
            "github": self.github,
            "secrets": self.secrets,
            "inputs": self.inputs,
            "matrix": self.matrix,
            "env": self.env,
            "needs": self.needs,
            "runner": {"os": "Linux", "name": "self-hosted", "temp": ""},
            "steps": _Soft(),
            "job": _Soft(),
            "vars": _Soft(),
            "strategy": _Soft(),
        }


_CURRENT: list[Context] = []


def _ctx() -> Context:
    return _CURRENT[-1]


def _resolve_name(name: str) -> object:
    if name not in _ctx().names:
        raise ExprError(f"unknown context {name}")
    return _ctx().names[name]


def _lookup(value: object, key: object) -> object:
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get(key)
    return None


def _truthy(value: object) -> bool:
    return value not in (False, 0, None, "")


def _stringish(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _compare(op: str, left: object, right: object) -> bool:
    if isinstance(left, str) and isinstance(right, str):
        left_cmp, right_cmp = left.lower(), right.lower()
    else:
        left_cmp, right_cmp = left, right
    if op == "==":
        return left_cmp == right_cmp
    if op == "!=":
        return left_cmp != right_cmp
    try:
        if op == "<":
            return left_cmp < right_cmp  # type: ignore[operator]
        if op == "<=":
            return left_cmp <= right_cmp  # type: ignore[operator]
        if op == ">":
            return left_cmp > right_cmp  # type: ignore[operator]
        if op == ">=":
            return left_cmp >= right_cmp  # type: ignore[operator]
    except TypeError:
        return False
    raise ExprError(op)


def _contains(haystack: object, needle: object) -> bool:
    if isinstance(haystack, list):
        return any(str(item).lower() == str(needle).lower() for item in haystack)
    return str(needle).lower() in str(_stringish(haystack)).lower()


def evaluate(expression: object, ctx: Context) -> object:
    if isinstance(expression, bool):
        return expression
    if expression is None:
        return True
    text = str(expression).strip()
    wrapped = EXPR_WRAP.match(text)
    if wrapped:
        text = wrapped.group(1).strip()
    if not STATUS_FN.search(text):
        text = f"success() && ({text})"
    _CURRENT.append(ctx)
    try:
        return Parser(text).parse()
    finally:
        _CURRENT.pop()


def substitute(text: str, ctx: Context) -> str:
    def repl(match: re.Match[str]) -> str:
        _CURRENT.append(ctx)
        try:
            value = Parser(match.group(1).strip()).parse()
        finally:
            _CURRENT.pop()
        if value is None or isinstance(value, dict):
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    return EXPR_PART.sub(repl, text)


def fork_context(*, base: str, success: bool = True, matrix: dict | None = None, env: dict | None = None) -> Context:
    return Context(
        github={
            "event_name": "pull_request",
            "repository": BASE_REPOSITORY,
            "actor": "external-contributor",
            "ref": "refs/pull/1/merge",
            "sha": "merge-sha",
            "token": JOB_TOKEN,
            "event": {
                "pull_request": {
                    "number": 1,
                    "head": {
                        "sha": "head-sha",
                        "ref": "evil",
                        "repo": {"full_name": FORK_REPOSITORY, "fork": True},
                    },
                    "base": {"ref": base, "sha": "base-sha", "repo": {"full_name": BASE_REPOSITORY}},
                    "user": {"login": "external-contributor"},
                }
            },
        },
        # Fork pull requests receive the job token and no other secrets.
        secrets={"GITHUB_TOKEN": JOB_TOKEN},
        inputs={},
        matrix=matrix or {},
        env=env or {},
        needs={},
        success=success,
    )


PYTHON_PAYLOAD = textwrap.dedent(
    """\
    import os
    from pathlib import Path
    seen = "no"
    expected = os.environ.get("EXPECTED_JOB_TOKEN", "")
    if expected and (
        os.environ.get("GH_TOKEN") == expected or os.environ.get("GITHUB_TOKEN") == expected
    ):
        seen = "yes"
    dest = Path(os.environ["RUNNER_PERSIST"])
    dest.mkdir(parents=True, exist_ok=True)
    with (dest / "planted").open("a", encoding="utf-8") as handle:
        handle.write(f"ran token_seen={seen}\\n")
    """
)

SHELL_PAYLOAD = textwrap.dedent(
    """\
    seen=no
    if [ -n "${EXPECTED_JOB_TOKEN:-}" ] && { [ "${GH_TOKEN:-}" = "${EXPECTED_JOB_TOKEN}" ] || [ "${GITHUB_TOKEN:-}" = "${EXPECTED_JOB_TOKEN}" ]; }; then
      seen=yes
    fi
    mkdir -p "${RUNNER_PERSIST}"
    printf 'ran token_seen=%s\\n' "$seen" >> "${RUNNER_PERSIST}/planted"
    """
)

ACTION_PAYLOAD = textwrap.dedent(
    """\
    name: payload
    runs:
      using: composite
      steps:
        - shell: bash
          run: |
            seen=no
            if [ -n "${EXPECTED_JOB_TOKEN:-}" ] && { [ "${GH_TOKEN:-}" = "${EXPECTED_JOB_TOKEN}" ] || [ "${GITHUB_TOKEN:-}" = "${EXPECTED_JOB_TOKEN}" ]; }; then
              seen=yes
            fi
            mkdir -p "${RUNNER_PERSIST}"
            printf 'ran token_seen=%s\\n' "$seen" >> "${RUNNER_PERSIST}/planted"
    """
)


@dataclass
class RunResult:
    executions: list[str] = field(default_factory=list)
    build_tools: list[str] = field(default_factory=list)
    idle_self_hosted: list[str] = field(default_factory=list)
    planted: str = ""
    token_seen: bool = False


def _needs_map(jobs: dict, states: dict[str, str]) -> dict:
    mapped = {}
    for name, state in states.items():
        outputs = {}
        if state == "success":
            # The attacker chooses the files that path filters read, so an
            # output the workflow compares to 'true' or a non-empty matrix is
            # available. A skipped needed job has no outputs.
            outputs = {"__default__": "true"}
        mapped[name] = {"result": state, "outputs": _DefaultOutputs(outputs if state == "success" else {})}
    return mapped


class _Soft(dict):
    """Missing keys keep resolving, so a step output the fork never produced is empty."""

    def get(self, key: object, default: object = None) -> object:  # type: ignore[override]
        if key in self:
            return super().get(key, default)
        return _Soft()


class _DefaultOutputs(dict):
    """Missing output keys on a successful job read as 'true' (attacker-controlled paths)."""

    def get(self, key: object, default: object = None) -> object:  # type: ignore[override]
        if key in self:
            return super().get(key, default)
        if key == "result":
            return default
        return "true"


def _job_state(name: str, jobs: dict, states: dict[str, str], github: dict) -> str:
    if name in states:
        return states[name]
    job = jobs[name]
    needs = job.get("needs") or []
    if isinstance(needs, str):
        needs = [needs]
    for dep in needs:
        _job_state(str(dep), jobs, states, github)
    success = all(states[str(dep)] == "success" for dep in needs)
    ctx = fork_context(base=github["event"]["pull_request"]["base"]["ref"], success=success)
    ctx.github = github
    ctx.needs = _needs_map(jobs, {dep: states[str(dep)] for dep in needs})
    # Rebind needs objects so hyphenated ids resolve. _needs_map already did.
    if "if" not in job:
        state = "success" if success else "skipped"
    else:
        state = "success" if _truthy(evaluate(job.get("if"), ctx)) else "skipped"
    states[name] = state
    return state


def _matrix_rows(job: dict, ctx: Context) -> list[dict]:
    strategy = job.get("strategy") or {}
    matrix = strategy.get("matrix")
    if not matrix:
        return [{}]
    if isinstance(matrix, str):
        matrix = substitute(matrix, ctx)
        import json

        matrix = json.loads(matrix) if isinstance(matrix, str) else matrix
    if not isinstance(matrix, dict):
        raise ExprError(f"unsupported matrix {matrix!r}")
    include = matrix.get("include")
    keys = [key for key in matrix if key not in {"include", "exclude"}]
    rows: list[dict] = []
    if keys:
        lists: list[tuple[str, list]] = []
        for key in keys:
            value = matrix[key]
            if isinstance(value, str) and "${{" in value:
                value = _eval_fragment(value, ctx)
            if not isinstance(value, list):
                raise ExprError(f"matrix key {key} is not a list")
            lists.append((key, value))
        rows = _product(lists)
    elif isinstance(include, list):
        rows = [dict(item) for item in include]
    if isinstance(include, list) and keys:
        for item in include:
            rows.append(dict(item))
    return rows or [{}]


def _eval_fragment(text: str, ctx: Context) -> object:
    match = EXPR_WRAP.match(text.strip())
    body = match.group(1) if match else text
    _CURRENT.append(ctx)
    try:
        return Parser(body.strip()).parse()
    finally:
        _CURRENT.pop()


def _product(lists: list[tuple[str, list]]) -> list[dict]:
    rows: list[dict] = [{}]
    for key, values in lists:
        rows = [{**row, key: value} for row in rows for value in values]
    return rows


def _labels(runs_on: object, ctx: Context) -> list[str]:
    if isinstance(runs_on, list):
        return [str(_render_label(item, ctx)) for item in runs_on]
    if isinstance(runs_on, dict):
        labels = runs_on.get("labels", [])
        if isinstance(labels, str):
            labels = [labels]
        return [str(_render_label(item, ctx)) for item in labels]
    return [str(_render_label(runs_on, ctx))]


def _render_label(value: object, ctx: Context) -> object:
    if isinstance(value, str) and "${{" in value:
        return substitute(value, ctx)
    return value


def _step_env(workflow: dict, job: dict, step: dict, ctx: Context) -> dict:
    merged: dict[str, str] = {}
    for source in (workflow.get("env") or {}, job.get("env") or {}, step.get("env") or {}):
        for key, value in source.items():
            rendered = substitute(str(value), ctx) if isinstance(value, str) else str(value)
            merged[str(key)] = rendered
    return merged


@dataclass
class Trees:
    merge: Path
    base: Path


class Runner:
    def __init__(self, trees: Trees, persist: Path) -> None:
        self.trees = trees
        self.persist = persist
        self.persist.mkdir(parents=True, exist_ok=True)
        self.shims = persist.parent / "shims"
        self.shims.mkdir(parents=True, exist_ok=True)
        python = self.shims / "python"
        python.write_text("#!/bin/sh\nexec python3 \"$@\"\n", encoding="utf-8")
        python.chmod(0o755)
        python3 = self.shims / "python3"
        python3.write_text("#!/bin/sh\nexec /usr/bin/python3 \"$@\"\n", encoding="utf-8")
        python3.chmod(0o755)
        curl = self.shims / "curl"
        curl.write_text(
            textwrap.dedent(
                """\
                #!/bin/sh
                url=
                for arg in "$@"; do
                  case "$arg" in
                    https://*|http://*) url=$arg ;;
                  esac
                done
                case "$url" in
                  https://raw.githubusercontent.com/*/*/*/*)
                    rest=${url#https://raw.githubusercontent.com/}
                    path=${rest#*/*/*/}
                    if [ -f "$CURL_FILE_ROOT/$path" ]; then
                      cat "$CURL_FILE_ROOT/$path"
                      exit 0
                    fi
                    ;;
                esac
                echo "refused $url" >&2
                exit 1
                """
            ),
            encoding="utf-8",
        )
        curl.chmod(0o755)

    def run_file(self, doc: dict, source: str) -> RunResult:
        result = RunResult()
        triggers = _triggers(doc)
        if "pull_request" not in triggers:
            return result
        opened = {"opened", "synchronize", "reopened"}
        if not (_pull_request_types(triggers.get("pull_request")) & opened):
            return result
        base = matching_base(triggers.get("pull_request"))
        if base is None:
            result.idle_self_hosted.append(f"{source}: no existing base branch matches pull_request")
            return result
        github = fork_context(base=base).github
        jobs = doc.get("jobs") or {}
        states: dict[str, str] = {}
        for name in jobs:
            _job_state(str(name), jobs, states, github)
        for name, job in jobs.items():
            if states[str(name)] != "success":
                continue
            success = True
            needs = job.get("needs") or []
            if isinstance(needs, str):
                needs = [needs]
            ctx = fork_context(base=base, success=success)
            ctx.github = github
            ctx.needs = _needs_map(jobs, {str(dep): states[str(dep)] for dep in needs})
            try:
                rows = _matrix_rows(job, ctx)
            except ExprError as exc:
                raise ExprError(f"{source} job {name}: {exc}") from exc
            saw_self_hosted = False
            saw_code = False
            for row in rows:
                row_ctx = fork_context(base=base, matrix=row)
                row_ctx.github = github
                row_ctx.needs = ctx.needs
                labels = _labels(job.get("runs-on"), row_ctx)
                if is_hosted(labels):
                    continue
                saw_self_hosted = True
                if self._execute_job(doc, job, row_ctx, result, f"{source}:{name}@{','.join(labels)}"):
                    saw_code = True
            if saw_self_hosted and not saw_code:
                result.idle_self_hosted.append(f"{source}:{name}")
        if (self.persist / "planted").exists():
            result.planted = (self.persist / "planted").read_text(encoding="utf-8")
            result.token_seen = "token_seen=yes" in result.planted
        return result

    def _execute_job(self, workflow: dict, job: dict, ctx: Context, result: RunResult, label: str) -> bool:
        workspace = Path(tempfile.mkdtemp(prefix="ws-"))
        runner_temp = Path(tempfile.mkdtemp(prefix="rt-"))
        checkout_mode = "absent"
        ran_code = False
        try:
            for index, step in enumerate(job.get("steps") or []):
                step_ctx = fork_context(base=ctx.github["event"]["pull_request"]["base"]["ref"], matrix=ctx.matrix)
                step_ctx.github = ctx.github
                step_ctx.needs = ctx.needs
                step_ctx.env = ctx.env
                step_ctx.success = True
                if "if" in step and not _truthy(evaluate(step.get("if"), step_ctx)):
                    continue
                uses = str(step.get("uses") or "")
                if uses.startswith("actions/checkout@"):
                    ref = ""
                    if isinstance(step.get("with"), dict) and "ref" in step["with"]:
                        ref = substitute(str(step["with"]["ref"]), step_ctx)
                    checkout_mode = "base" if ref in {"base-sha", "refs/heads/" + ctx.github["event"]["pull_request"]["base"]["ref"]} else "merge"
                    source = self.trees.base if checkout_mode == "base" else self.trees.merge
                    _copy_tree(source, workspace)
                    continue
                env = {
                        "PATH": f"{self.shims}:/usr/bin:/bin",
                        "HOME": str(self.persist),
                        "RUNNER_PERSIST": str(self.persist),
                        "RUNNER_TEMP": str(runner_temp),
                        "GITHUB_WORKSPACE": str(workspace),
                        "GITHUB_ENV": str(runner_temp / "github_env"),
                        "EXPECTED_JOB_TOKEN": JOB_TOKEN,
                        "CURL_FILE_ROOT": str(self.trees.merge),
                        "PYTHONDONTWRITEBYTECODE": "1",
                }
                env.update(_step_env(workflow, job, step, step_ctx))
                if LOCAL_USE.match(uses):
                    self._run_local_action(uses, env, workspace, result, f"{label}#step{index}")
                    ran_code = True
                    continue
                if "run" not in step:
                    continue
                script = substitute(str(step["run"]), step_ctx)
                if BUILD_TOOL.search(script):
                    result.build_tools.append(f"{label}#step{index}")
                    ran_code = True
                    continue
                targets = FILE_INVOKE.findall(script)
                piped = bool(SHELL_PIPE.search(script))
                if not targets and not piped:
                    continue
                if checkout_mode == "base":
                    continue
                if checkout_mode == "absent" and targets:
                    _copy_tree(self.trees.merge, workspace)
                    checkout_mode = "merge"
                before = (self.persist / "planted").read_text(encoding="utf-8") if (self.persist / "planted").exists() else ""
                self._bash(script, env, workspace)
                after = (self.persist / "planted").read_text(encoding="utf-8") if (self.persist / "planted").exists() else ""
                if after != before:
                    result.executions.append(f"{label}#step{index}")
                    ran_code = True
                self._read_github_env(env, runner_temp / "github_env", ctx)
        finally:
            shutil.rmtree(workspace, ignore_errors=True)
            shutil.rmtree(runner_temp, ignore_errors=True)
        return ran_code

    def _run_local_action(self, uses: str, env: dict, workspace: Path, result: RunResult, label: str) -> None:
        relative = uses[2:]
        # `$/` is the running commit (the merge). `./` is the checkout.
        root = self.trees.merge if uses.startswith("$/") else workspace
        manifest = root / relative / "action.yml"
        if not manifest.exists():
            manifest = root / relative / "action.yaml"
        doc = yaml.safe_load(manifest.read_text(encoding="utf-8"))
        runs = (doc or {}).get("runs") or {}
        if runs.get("using") != "composite":
            raise ExprError(f"{label} local action {uses} is not composite")
        for step in runs.get("steps") or []:
            if "run" not in step:
                continue
            before = (self.persist / "planted").read_text(encoding="utf-8") if (self.persist / "planted").exists() else ""
            self._bash(str(step["run"]), env, workspace if workspace.exists() else self.trees.merge)
            after = (self.persist / "planted").read_text(encoding="utf-8") if (self.persist / "planted").exists() else ""
            if after != before:
                result.executions.append(label)
            return
        raise ExprError(f"{label} local action {uses} has no run step")

    def _bash(self, script: str, env: dict, cwd: Path) -> None:
        script_file = cwd / ".runner-step.sh"
        if not cwd.exists():
            cwd.mkdir(parents=True)
        script_file.write_text(script, encoding="utf-8")
        completed = subprocess.run(
            ["/usr/bin/bash", "-e", str(script_file)],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        script_file.unlink(missing_ok=True)
        if completed.returncode != 0:
            safe_err = completed.stderr.replace(JOB_TOKEN, "<job-token>")
            safe_out = completed.stdout.replace(JOB_TOKEN, "<job-token>")
            raise AssertionError(f"step exited {completed.returncode}: {safe_out}{safe_err}")

    def _read_github_env(self, env: dict, path: Path, ctx: Context) -> None:
        if not path.exists():
            return
        for line in path.read_text(encoding="utf-8").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            ctx.env[key] = value
            env[key] = value


def _copy_tree(source: Path, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(source, dest, ignore=shutil.ignore_patterns(".git"))


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=Runner Test", "-c", "user.email=runner-test@example.com", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )


def _seed_repo(dest: Path) -> None:
    dest.mkdir(parents=True)
    for relative in (".github/workflows", ".github/actions", ".github/scripts", "scripts", "install"):
        source = ROOT / relative
        target = dest / relative
        shutil.copytree(source, target, ignore=shutil.ignore_patterns(".git"))
    _git(dest, "init", "-q")
    _git(dest, "add", ".")
    _git(dest, "commit", "-q", "-m", "base")


def plant_payloads(repo: Path, entry_points: set[str]) -> None:
    for relative in entry_points:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if relative.endswith(".py"):
            path.write_text(PYTHON_PAYLOAD, encoding="utf-8")
        elif relative.endswith("action.yml") or relative.endswith("action.yaml"):
            path.write_text(ACTION_PAYLOAD, encoding="utf-8")
        else:
            path.write_text(SHELL_PAYLOAD, encoding="utf-8")


def entry_points_for(doc: dict, source_name: str) -> set[str]:
    """Files the fork replaces. Computed from the base workflow, before the merge."""
    found: set[str] = set()
    triggers = _triggers(doc)
    if "pull_request" not in triggers:
        return found
    if not (_pull_request_types(triggers.get("pull_request")) & {"opened", "synchronize", "reopened"}):
        return found
    base = matching_base(triggers.get("pull_request"))
    if base is None:
        return found
    github = fork_context(base=base).github
    jobs = doc.get("jobs") or {}
    states: dict[str, str] = {}
    for name in jobs:
        _job_state(str(name), jobs, states, github)
    for name, job in jobs.items():
        if states[str(name)] != "success":
            continue
        ctx = fork_context(base=base)
        ctx.github = github
        needs = job.get("needs") or []
        if isinstance(needs, str):
            needs = [needs]
        ctx.needs = _needs_map(jobs, {str(dep): states[str(dep)] for dep in needs})
        try:
            rows = _matrix_rows(job, ctx)
        except ExprError as exc:
            raise ExprError(f"{source_name} job {name}: {exc}") from exc
        for row in rows:
            row_ctx = fork_context(base=base, matrix=row)
            row_ctx.github = github
            row_ctx.needs = ctx.needs
            labels = _labels(job.get("runs-on"), row_ctx)
            if is_hosted(labels):
                continue
            for step in job.get("steps") or []:
                step_ctx = fork_context(base=base, matrix=row)
                step_ctx.github = github
                step_ctx.needs = ctx.needs
                if "if" in step and not _truthy(evaluate(step.get("if"), step_ctx)):
                    continue
                uses = str(step.get("uses") or "")
                if LOCAL_USE.match(uses):
                    relative = uses[2:].strip("/")
                    manifest = "action.yml"
                    if (ROOT / relative / "action.yaml").exists() and not (ROOT / relative / "action.yml").exists():
                        manifest = "action.yaml"
                    found.add(f"{relative}/{manifest}")
                if "run" not in step:
                    continue
                script = substitute(str(step["run"]), step_ctx)
                for target in FILE_INVOKE.findall(script):
                    found.add(target.lstrip("./").rstrip("'\""))
                if SHELL_PIPE.search(script):
                    for match in RAW_URL.finditer(script):
                        found.add(match.group(1).split("?", 1)[0].rstrip("'\""))
                    if "install/install.sh" in script or "${HEAD_SHA}/install/install.sh" in str(step["run"]):
                        found.add("install/install.sh")
    return found


class ExpressionTest(unittest.TestCase):
    def test_fork_guard_is_false_and_same_repo_shape_is_not_enough_when_head_differs(self) -> None:
        ctx = fork_context(base="trunk")
        guard = (
            "github.event_name != 'pull_request' || "
            "github.event.pull_request.head.repo.full_name == github.repository"
        )
        self.assertFalse(_truthy(evaluate(guard, ctx)))
        wal = (
            "github.event_name != 'pull_request' || "
            "(github.event.pull_request.head.repo.full_name == github.repository && "
            "github.event.pull_request.user.login != 'dependabot[bot]')"
        )
        self.assertFalse(_truthy(evaluate(wal, ctx)))
        # A check that compares the head ref to itself does not exclude the fork.
        self.assertTrue(
            _truthy(
                evaluate(
                    "github.event.pull_request.head.ref == github.event.pull_request.head.ref",
                    ctx,
                )
            )
        )


class SchedulerTest(unittest.TestCase):
    def _trees(self, root: Path, payload_name: str = "scripts/payload.py") -> Trees:
        base = root / "base"
        merge = root / "merge"
        for tree in (base, merge):
            (tree / "scripts").mkdir(parents=True)
            (tree / "scripts" / "payload.py").write_text("print('base')\n", encoding="utf-8")
            (tree / "repo-name.txt").write_text("spiceai/spiceai\n", encoding="utf-8")
        (merge / payload_name).write_text(PYTHON_PAYLOAD, encoding="utf-8")
        (merge / "repo-name.txt").write_text("spiceai/spiceai\n", encoding="utf-8")
        return Trees(merge=merge, base=base)

    def test_unprotected_job_runs_the_merge_commit_script_and_keeps_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trees = self._trees(root)
            persist = root / "runner-home"
            doc = {
                True: {"pull_request": None},
                "jobs": {
                    "verify": {
                        "runs-on": "spiceai-dev-runners",
                        "env": {"GH_TOKEN": "${{ github.token }}"},
                        "permissions": {"contents": "read"},
                        "steps": [
                            {"uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"},
                            {"run": "python scripts/payload.py -v"},
                        ],
                    }
                },
            }
            result = Runner(trees, persist).run_file(doc, "fixture.yml")
        self.assertEqual(result.executions, ["fixture.yml:verify@spiceai-dev-runners#step1"])
        self.assertTrue(result.token_seen)
        self.assertIn("ran token_seen=yes", result.planted)
        # The runner deletes RUNNER_TEMP at the end of the job. The marker was
        # read from the runner home after that delete.

    def test_event_context_guard_skips_the_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trees = self._trees(root)
            doc = {
                True: {"pull_request": None},
                "jobs": {
                    "verify": {
                        "runs-on": "spiceai-dev-runners",
                        "env": {"GH_TOKEN": "${{ github.token }}"},
                        "if": "${{ github.event.pull_request.head.repo.full_name == github.repository }}",
                        "steps": [
                            {"uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"},
                            {"run": "python scripts/payload.py"},
                        ],
                    }
                },
            }
            result = Runner(trees, root / "runner-home").run_file(doc, "guarded.yml")
        self.assertEqual(result.executions, [])
        self.assertFalse(result.token_seen)

    def test_guard_on_a_different_step_still_runs_the_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trees = self._trees(root)
            doc = {
                True: {"pull_request": None},
                "jobs": {
                    "verify": {
                        "runs-on": "spiceai-dev-runners",
                        "env": {"GH_TOKEN": "${{ github.token }}"},
                        "steps": [
                            {
                                "if": "${{ github.event.pull_request.head.repo.full_name == github.repository }}",
                                "run": "echo skipped",
                            },
                            {"uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"},
                            {"run": "python scripts/payload.py"},
                        ],
                    }
                },
            }
            result = Runner(trees, root / "runner-home").run_file(doc, "wrong-step.yml")
        self.assertTrue(result.token_seen)

    def test_substituted_head_repository_does_not_run_the_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trees = self._trees(root)
            doc = {
                True: {"pull_request": None},
                "jobs": {
                    "verify": {
                        "runs-on": "spiceai-dev-runners",
                        "env": {"GH_TOKEN": "${{ github.token }}"},
                        "steps": [
                            {"uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"},
                            {
                                "run": 'if [ "${{ github.event.pull_request.head.repo.full_name }}" = "${{ github.repository }}" ]; then python scripts/payload.py; fi'
                            },
                        ],
                    }
                },
            }
            result = Runner(trees, root / "runner-home").run_file(doc, "inline.yml")
        self.assertEqual(result.executions, [])
        self.assertEqual(result.planted, "")
        self.assertFalse(result.token_seen)

    def test_check_against_a_workspace_file_still_runs_the_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trees = self._trees(root)
            doc = {
                True: {"pull_request": None},
                "jobs": {
                    "verify": {
                        "runs-on": "spiceai-dev-runners",
                        "env": {"GH_TOKEN": "${{ github.token }}"},
                        "steps": [
                            {"uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"},
                            {
                                "run": 'head_repo="$(cat repo-name.txt)"\nif [ "$head_repo" = "${{ github.repository }}" ]; then python scripts/payload.py; fi'
                            },
                        ],
                    }
                },
            }
            result = Runner(trees, root / "runner-home").run_file(doc, "workspace-check.yml")
        self.assertTrue(result.token_seen)

    def test_hosted_runner_is_outside_this_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trees = self._trees(root)
            doc = {
                True: {"pull_request": None},
                "jobs": {
                    "verify": {
                        "runs-on": "ubuntu-24.04",
                        "env": {"GH_TOKEN": "${{ github.token }}"},
                        "steps": [
                            {"uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"},
                            {"run": "python scripts/payload.py"},
                        ],
                    }
                },
            }
            result = Runner(trees, root / "runner-home").run_file(doc, "hosted.yml")
        self.assertEqual(result.executions, [])


class RepositoryTest(unittest.TestCase):
    def test_fork_pull_request_does_not_execute_repository_code_on_self_hosted_runners(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = root / "base"
            _seed_repo(base)
            merge = root / "merge"
            shutil.copytree(base, merge, ignore=shutil.ignore_patterns(".git"))
            _git(merge, "init", "-q")
            _git(merge, "add", ".")
            _git(merge, "commit", "-q", "-m", "base")
            _git(merge, "checkout", "-q", "-b", "attacker")
            points: set[str] = set()
            for path in sorted(WORKFLOWS.glob("*.yml")):
                doc = yaml.safe_load(path.read_text(encoding="utf-8"))
                points |= entry_points_for(doc, path.name)
            self.assertIn("scripts/pin_actions_to_sha.py", points)
            self.assertIn("scripts/test_pin_actions_to_sha.py", points)
            plant_payloads(merge, points)
            _git(merge, "add", ".")
            _git(merge, "commit", "-q", "-m", "fork")
            _git(merge, "checkout", "-q", "master")
            _git(merge, "merge", "--no-edit", "attacker")
            # The workflow file in the merge is still the base file. The
            # scripts the job runs are the fork's.
            workflow = ".github/workflows/verify-lockfile.yml"
            self.assertEqual((merge / workflow).read_bytes(), (base / workflow).read_bytes())
            self.assertNotEqual(
                (merge / "scripts/pin_actions_to_sha.py").read_bytes(),
                (base / "scripts/pin_actions_to_sha.py").read_bytes(),
            )
            persist = root / "runner-home"
            result = RunResult()
            runner = Runner(Trees(merge=merge, base=base), persist)
            for path in sorted((merge / ".github" / "workflows").glob("*.yml")):
                doc = yaml.safe_load(path.read_text(encoding="utf-8"))
                one = runner.run_file(doc, path.name)
                result.executions.extend(one.executions)
                result.build_tools.extend(one.build_tools)
                result.idle_self_hosted.extend(one.idle_self_hosted)
            if (persist / "planted").exists():
                result.planted = (persist / "planted").read_text(encoding="utf-8")
                result.token_seen = "token_seen=yes" in result.planted
        message = "\n".join(
            [
                "repository code ran on a self-hosted runner for a fork pull request:",
                *result.executions,
                "build tools scheduled and not invoked here:",
                *result.build_tools,
                f"job token visible to that code: {result.token_seen}",
                "planted file survived RUNNER_TEMP being removed: "
                + ("yes" if result.planted else "no"),
            ]
        )
        self.assertEqual(result.executions, [], message)
        self.assertEqual(result.build_tools, [], message)
        self.assertNotIn("token_seen=yes", result.planted)
        self.assertEqual(result.planted, "")


if __name__ == "__main__":
    unittest.main()
