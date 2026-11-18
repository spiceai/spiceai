#!/usr/bin/env python3
"""Same-repo pull requests must not run checkout code on the long-lived runner pool.

The tests follow the runner at actions/runner 2.338.0
(7afce9edc81e4138ed1eaa673a3c10c811faa092): expression truth and equality, `$/`
resolution to the workflow repository at the workflow SHA, and bash invocation of
`run` steps. They fail while `.github/workflows/cayenne_object_native.yml` still
builds and executes a same-repository pull request on `spiceai-dev-runners`.

Run:
  python3 .github/scripts/test_persistent_runner_pull_request_execution.py
"""

from __future__ import annotations

import json
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
from yaml.resolver import Resolver

REPO = Path(__file__).resolve().parents[2]
WORKFLOW = REPO / ".github" / "workflows" / "cayenne_object_native.yml"
RECLAIM = REPO / ".github" / "workflows" / "reclaim_runner_disk.yml"
INTEGRATION = REPO / ".github" / "workflows" / "integration.yml"
PICOMATCH = Path(__file__).resolve().parent / "vendor" / "picomatch"
HOST = "x86_64-unknown-linux-gnu"
TOOLCHAIN = "1.83.0"
REAL_TOOLCHAIN = Path(f"/usr/local/rustup/toolchains/{TOOLCHAIN}-{HOST}")

SHELL_MARKER = ".attacker-shell-ran"
BUILD_MARKER = ".attacker-build-rs-ran"
BINARY_MARKER = ".attacker-binary-ran"
LATER_MARKER = ".attacker-later-sourced"


class WorkflowLoader(yaml.SafeLoader):
    """Keep `on` as a string. PyYAML's default bool resolver turns it into True."""


WorkflowLoader.yaml_implicit_resolvers = {
    key: [(tag, pattern) for tag, pattern in patterns if tag != "tag:yaml.org,2002:bool"]
    for key, patterns in Resolver.yaml_implicit_resolvers.items()
}


def load_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        loaded = yaml.load(handle, Loader=WorkflowLoader)
    if not isinstance(loaded, dict):
        raise AssertionError(f"{path} did not parse as a mapping")
    return loaded


# --- GitHub expression evaluation (DTExpressions2 And/Or/Equal/Not) ---


def is_falsy(value: object) -> bool:
    if value is None or value is False:
        return True
    if value is True:
        return False
    if isinstance(value, float):
        return value == 0.0 or value != value
    if isinstance(value, int):
        return value == 0
    if isinstance(value, str):
        return value == ""
    return False


def is_truthy(value: object) -> bool:
    return not is_falsy(value)


def _kind(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    return "object"


def _parse_number(text: str) -> float:
    if text == "":
        return 0.0
    try:
        return float(text)
    except ValueError:
        return float("nan")


def _to_number(value: object) -> float:
    kind = _kind(value)
    if kind == "null":
        return 0.0
    if kind == "bool":
        return 1.0 if value else 0.0
    if kind == "number":
        return float(value)
    if kind == "string":
        return _parse_number(value)
    return float("nan")


def abstract_equal(left: object, right: object) -> bool:
    def coerce(lhs: object, rhs: object) -> tuple[object, object, str, str]:
        left_kind, right_kind = _kind(lhs), _kind(rhs)
        if left_kind == right_kind:
            return lhs, rhs, left_kind, right_kind
        if left_kind == "number" and right_kind == "string":
            return lhs, _to_number(rhs), "number", "number"
        if left_kind == "string" and right_kind == "number":
            return _to_number(lhs), rhs, "number", "number"
        if left_kind in {"bool", "null"}:
            return coerce(_to_number(lhs), rhs)
        if right_kind in {"bool", "null"}:
            return coerce(lhs, _to_number(rhs))
        return lhs, rhs, left_kind, right_kind

    lhs, rhs, left_kind, right_kind = coerce(left, right)
    if left_kind != right_kind:
        return False
    if left_kind == "null":
        return True
    if left_kind == "number":
        left_num, right_num = float(lhs), float(rhs)
        if left_num != left_num or right_num != right_num:
            return False
        return left_num == right_num
    if left_kind == "string":
        return str(lhs).lower() == str(rhs).lower()
    if left_kind == "bool":
        return bool(lhs) is bool(rhs)
    return lhs is rhs


def stringify(value: object) -> str:
    if value is None:
        return ""
    if value is True:
        return "true"
    if value is False:
        return "false"
    return str(value)


class ExprError(Exception):
    pass


class Parser:
    def __init__(self, text: str) -> None:
        self.text = text.strip()
        if self.text.startswith("${{") and self.text.endswith("}}"):
            self.text = self.text[3:-2].strip()
        self.index = 0

    def parse(self) -> object:
        value = self._or()
        self._skip()
        if self.index != len(self.text):
            raise ExprError(f"unparsed expression tail: {self.text[self.index:]!r}")
        return value

    def _skip(self) -> None:
        while self.index < len(self.text) and self.text[self.index].isspace():
            self.index += 1

    def _starts(self, token: str) -> bool:
        self._skip()
        return self.text.startswith(token, self.index)

    def _eat(self, token: str) -> bool:
        if self._starts(token):
            self.index += len(token)
            return True
        return False

    def _or(self) -> object:
        value = self._and()
        while self._eat("||"):
            rhs = self._and()
            if is_truthy(value):
                continue
            value = rhs
        return value

    def _and(self) -> object:
        value = self._not()
        while self._eat("&&"):
            if is_falsy(value):
                self._not()
                continue
            value = self._not()
        return value

    def _not(self) -> object:
        if self._eat("!"):
            return is_falsy(self._not())
        return self._cmp()

    def _cmp(self) -> object:
        left = self._primary()
        for operator in ("==", "!=", "<=", ">=", "<", ">"):
            if self._eat(operator):
                right = self._primary()
                equal = abstract_equal(left, right)
                if operator == "==":
                    return equal
                if operator == "!=":
                    return not equal
                raise ExprError(f"comparison {operator} is not used by these workflows")
        return left

    def _primary(self) -> object:
        self._skip()
        if self._eat("("):
            value = self._or()
            if not self._eat(")"):
                raise ExprError("missing )")
            return value
        if self._starts("'"):
            return self._string()
        if self._peek_number():
            return self._number()
        if self._peek_ident():
            name = self._ident()
            if name in {"true", "false", "null"} and not self._starts("(") and not self._starts("."):
                return {"true": True, "false": False, "null": None}[name]
            if self._eat("("):
                if not self._eat(")"):
                    raise ExprError(f"function {name} must be zero-arg here")
                return self.functions[name]()
            value: object = self.context.get(name)
            while self._eat("."):
                part = self._ident()
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
            return value
        raise ExprError(f"bad expression at {self.text[self.index:]!r}")

    def _string(self) -> str:
        self.index += 1
        chars: list[str] = []
        while self.index < len(self.text):
            char = self.text[self.index]
            self.index += 1
            if char == "'":
                if self.index < len(self.text) and self.text[self.index] == "'":
                    chars.append("'")
                    self.index += 1
                    continue
                return "".join(chars)
            chars.append(char)
        raise ExprError("unterminated string")

    def _peek_number(self) -> bool:
        self._skip()
        char = self.text[self.index : self.index + 1]
        return char.isdigit() or (
            char == "-" and self.index + 1 < len(self.text) and self.text[self.index + 1].isdigit()
        )

    def _number(self) -> int | float:
        self._skip()
        start = self.index
        if self.text[self.index] == "-":
            self.index += 1
        while self.index < len(self.text) and (self.text[self.index].isdigit() or self.text[self.index] == "."):
            self.index += 1
        token = self.text[start : self.index]
        if "." in token:
            return float(token)
        return int(token)

    def _peek_ident(self) -> bool:
        self._skip()
        if self.index >= len(self.text):
            return False
        return self.text[self.index].isalpha() or self.text[self.index] == "_"

    def _ident(self) -> str:
        self._skip()
        start = self.index
        while self.index < len(self.text) and (self.text[self.index].isalnum() or self.text[self.index] == "_"):
            self.index += 1
        if start == self.index:
            raise ExprError("expected identifier")
        return self.text[start : self.index]

    def evaluate(self, context: dict, functions: dict) -> object:
        self.context = context
        self.functions = functions
        return self.parse()


def evaluate(expression: str, context: dict, functions: dict) -> object:
    return Parser(expression).evaluate(context, functions)


def substitute(text: str, context: dict, functions: dict) -> str:
    def replace(match: re.Match[str]) -> str:
        return stringify(evaluate(match.group(1), context, functions))

    return re.sub(r"\$\{\{(.*?)\}\}", replace, text, flags=re.DOTALL)


# --- path filters ---


def github_glob(pattern: str, path: str) -> bool:
    """Whole-path match from the workflow syntax filter cheat sheet."""
    index = 0
    regex: list[str] = ["^"]
    while index < len(pattern):
        if pattern.startswith("**", index):
            regex.append(".*")
            index += 2
            continue
        char = pattern[index]
        index += 1
        if char == "*":
            regex.append("[^/]*")
        elif char == "?":
            if regex[-1] == "^":
                raise AssertionError(f"`?` needs a preceding atom in {pattern}")
            regex.append("?")
        elif char == "+":
            regex.append("+")
        elif char == "[":
            end = pattern.find("]", index)
            regex.append(re.escape(pattern[index:end]) if False else f"[{pattern[index:end]}]")
            index = end + 1
        else:
            regex.append(re.escape(char))
    regex.append("$")
    return re.search("".join(regex), path) is not None


def picomatch_any(patterns: list[str], filename: str) -> bool:
    script = textwrap.dedent(
        """
        const picomatch = require(process.argv[1]);
        const patterns = JSON.parse(process.argv[2]);
        const file = process.argv[3];
        const matchers = patterns.map((pattern) => picomatch(pattern, { dot: true }));
        process.stdout.write(matchers.some((match) => match(file)) ? "1" : "0");
        """
    )
    completed = subprocess.run(
        ["node", "-e", script, str(PICOMATCH), json.dumps(patterns), filename],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip() == "1"


def paths_filter_outputs(filters_yaml: str, changed: list[str]) -> dict[str, str]:
    """dorny/paths-filter default quantifier `some`: any changed file, output 'true'/'false'."""
    loaded = yaml.load(filters_yaml, Loader=WorkflowLoader)
    outputs: dict[str, str] = {}
    for name, patterns in loaded.items():
        if not isinstance(patterns, list):
            raise AssertionError(f"filter {name} is not a list")
        matched = any(picomatch_any([str(pattern)], changed_path) for pattern in patterns for changed_path in changed)
        # JSON.stringify(true) from @actions/core 1.10 toCommandValue, then the string 'true'.
        outputs[name] = "true" if matched else "false"
    return outputs


def workflow_paths_match(patterns: list[str], changed: list[str]) -> bool:
    if not changed:
        return False
    included: list[str] = []
    negated: list[str] = []
    for pattern in patterns:
        text = str(pattern)
        if text.startswith("!"):
            negated.append(text[1:])
        else:
            included.append(text)
    for path in changed:
        if any(github_glob(pattern, path) for pattern in included) and not any(
            github_glob(pattern, path) for pattern in negated
        ):
            return True
    return False


# --- workflow execution ---


def persistent_pools() -> set[str]:
    document = load_yaml(RECLAIM)
    pools = document["jobs"]["reclaim"]["strategy"]["matrix"]["pool"]
    return set(pools)


def functions_for(failed: bool) -> dict:
    return {
        "success": lambda: not failed,
        "failure": lambda: failed,
        "cancelled": lambda: False,
        "always": lambda: True,
    }


@dataclass
class StepRecord:
    job: str
    detail: str


@dataclass
class RunResult:
    triggered: bool
    jobs_ran: list[str] = field(default_factory=list)
    steps: list[StepRecord] = field(default_factory=list)
    home: Path | None = None

    def marker(self, name: str) -> bool:
        return self.home is not None and (self.home / name).exists()


def write_executable(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


def seed_toolchain(home: Path) -> None:
    toolchain = home / ".rustup" / "toolchains" / f"{TOOLCHAIN}-{HOST}"
    if not toolchain.exists():
        toolchain.parent.mkdir(parents=True, exist_ok=True)
        toolchain.symlink_to(REAL_TOOLCHAIN)
    write_executable(
        home / ".cargo" / "bin" / "rustup",
        textwrap.dedent(
            f"""\
            #!/bin/bash
            set -euo pipefail
            root="${{RUSTUP_HOME:-$HOME/.rustup}}"
            if [ "$1" = "show" ]; then
              printf 'Default host: {HOST}\\n'
              exit 0
            fi
            if [ "$1" = "which" ]; then
              tool="${{!#}}"
              printf '%s\\n' "$root/toolchains/{TOOLCHAIN}-{HOST}/bin/$tool"
              exit 0
            fi
            echo "unexpected rustup: $*" >&2
            exit 1
            """
        ),
    )


def host_stubs(directory: Path) -> None:
    write_executable(
        directory / "curl",
        textwrap.dedent(
            """\
            #!/bin/bash
            if [[ "$*" == *http_code* ]]; then
              printf '200'
              exit 0
            fi
            echo "curl stub: not calling the network" >&2
            exit 1
            """
        ),
    )
    write_executable(
        directory / "docker",
        textwrap.dedent(
            """\
            #!/bin/bash
            if [ "$1" = "port" ]; then
              printf '127.0.0.1:9\\n'
            fi
            exit 0
            """
        ),
    )


def attacker_setup_cc() -> str:
    return textwrap.dedent(
        f"""\
        name: Installs the cc toolchain
        description: composite action
        runs:
          using: composite
          steps:
            - name: Run the pull request's shell
              shell: bash
              run: |
                mkdir -p "${{CARGO_HOME:-$HOME/.cargo}}"
                dest="${{CARGO_HOME:-$HOME/.cargo}}/env"
                printf 'touch %q\\n' "$HOME/{LATER_MARKER}" > "$dest"
                touch "$HOME/{SHELL_MARKER}"
        """
    )


def attacker_package(root: Path) -> None:
    package = root / "crates" / "cayenne-metastore-model"
    (package / "src").mkdir(parents=True, exist_ok=True)
    (package / "Cargo.toml").write_text(
        textwrap.dedent(
            """\
            [package]
            name = "cayenne-metastore-model"
            version = "0.0.0"
            edition = "2021"
            """
        ),
        encoding="utf-8",
    )
    (package / "build.rs").write_text(
        textwrap.dedent(
            f"""\
            fn main() {{
                let home = std::env::var("HOME").expect("HOME");
                std::fs::write(format!("{{home}}/{BUILD_MARKER}"), b"build.rs").unwrap();
            }}
            """
        ),
        encoding="utf-8",
    )
    (package / "src" / "main.rs").write_text(
        textwrap.dedent(
            f"""\
            fn main() {{
                let home = std::env::var("HOME").expect("HOME");
                std::fs::write(format!("{{home}}/{BINARY_MARKER}"), b"bin").unwrap();
            }}
            """
        ),
        encoding="utf-8",
    )
    (root / "Cargo.toml").write_text(
        textwrap.dedent(
            """\
            [workspace]
            resolver = "2"
            members = ["crates/cayenne-metastore-model"]
            """
        ),
        encoding="utf-8",
    )
    (root / "rust-toolchain.toml").write_text(
        f'[toolchain]\nchannel = "{TOOLCHAIN}"\n',
        encoding="utf-8",
    )
    subprocess.run(
        ["cargo", "generate-lockfile", "--offline"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )


def merge_tree(root: Path) -> None:
    """Merge commit: repository actions, with setup-cc replaced by the pull request."""
    destination = root / ".github" / "actions"
    shutil.copytree(REPO / ".github" / "actions", destination)
    (destination / "setup-cc" / "action.yml").write_text(attacker_setup_cc(), encoding="utf-8")
    workflow_dir = root / ".github" / "workflows"
    workflow_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(WORKFLOW, workflow_dir / WORKFLOW.name)
    attacker_package(root)


def apply_env_file(env: dict[str, str], path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or "=" not in line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        env[key] = value


def apply_path_file(env: dict[str, str], path: Path) -> None:
    if not path.exists():
        return
    entries = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
    if not entries:
        return
    env["PATH"] = ":".join([*reversed(entries), env.get("PATH", "")])


def run_bash(script: str, cwd: Path, env: dict[str, str], shell_args: list[str]) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False, encoding="utf-8") as handle:
        handle.write(script)
        script_path = handle.name
    try:
        completed = subprocess.run(
            ["bash", *shell_args, script_path],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )
    finally:
        os.unlink(script_path)
    if completed.returncode != 0:
        raise RuntimeError(
            f"bash exited {completed.returncode}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )


class Executor:
    def __init__(self, tree: Path, home: Path, event: dict, changed: list[str]) -> None:
        self.tree = tree
        self.home = home
        self.event = event
        self.changed = changed
        self.workflow = load_yaml(tree / ".github" / "workflows" / WORKFLOW.name)
        self.pools = persistent_pools()
        self.result = RunResult(triggered=False, home=home)
        self.failed = False

    def context(self, extra: dict | None = None) -> dict:
        base = {
            "github": {
                "repository": "spiceai/spiceai",
                "event_name": self.event["event_name"],
                "event": self.event["payload"],
                "ref": self.event["ref"],
                "sha": self.event["sha"],
                "token": "placeholder-token",
                "run_id": "1",
                "run_attempt": "1",
            },
            "runner": {"os": "Linux", "arch": "X64"},
            "inputs": self.event.get("inputs", {}),
            "env": {},
            "steps": {},
            "needs": {},
        }
        if extra:
            base.update(extra)
        return base

    def eval(self, expression: str, context: dict) -> object:
        return evaluate(expression, context, functions_for(self.failed))

    def triggered(self) -> bool:
        pull = self.workflow["on"].get("pull_request")
        if not isinstance(pull, dict):
            return "pull_request" in self.workflow["on"]
        patterns = pull.get("paths")
        if not patterns:
            return True
        return workflow_paths_match(list(patterns), self.changed)

    def run(self) -> RunResult:
        if not self.triggered():
            self._later_privileged_job()
            return self.result
        self.result.triggered = True
        outputs = self._changes_job()
        needs = {"changes": {"outputs": outputs, "result": "success"}}
        for name in ("model", "s3", "object_store_occ"):
            job = self.workflow["jobs"][name]
            context = self.context({"needs": needs, "env": {}})
            if is_truthy(self.eval(str(job["if"]), context)):
                self._execute_job(name, job, needs)
        self._later_privileged_job()
        return self.result

    def _changes_job(self) -> dict[str, str]:
        job = self.workflow["jobs"]["changes"]
        self._require_persistent(job, "changes")
        self.result.jobs_ran.append("changes")
        filters = None
        for step in job["steps"]:
            if step.get("id") == "paths":
                filters = step["with"]["filters"]
        if not isinstance(filters, str):
            raise AssertionError("changes job has no paths filter")
        step_outputs = paths_filter_outputs(filters, self.changed)
        context = self.context(
            {
                "steps": {"paths": {"outputs": step_outputs}},
                "env": {},
            }
        )
        produced: dict[str, str] = {}
        for key, expression in job["outputs"].items():
            produced[key] = stringify(self.eval(str(expression), context))
        return produced

    def _require_persistent(self, job: dict, name: str) -> None:
        label = str(job["runs-on"])
        if label not in self.pools:
            raise AssertionError(f"{name} runs-on {label}, which reclaim_runner_disk.yml does not list as long-lived")

    def _execute_job(self, name: str, job: dict, needs: dict) -> None:
        self._require_persistent(job, name)
        self.result.jobs_ran.append(name)
        workspace = Path(tempfile.mkdtemp(prefix=f"gha-{name}-"))
        shutil.copytree(self.tree, workspace, dirs_exist_ok=True)
        env_file = workspace / "github.env"
        path_file = workspace / "github.path"
        env = os.environ.copy()
        env.update(
            {
                "HOME": str(self.home),
                "GITHUB_WORKSPACE": str(workspace),
                "GITHUB_ENV": str(env_file),
                "GITHUB_PATH": str(path_file),
                "GITHUB_OUTPUT": str(workspace / "github.out"),
                "RUNNER_OS": "Linux",
            }
        )
        stubs = workspace / "stubs"
        host_stubs(stubs)
        env["PATH"] = f"{stubs}:{self.home / '.cargo' / 'bin'}:{env.get('PATH', '')}"
        job_env = dict(self.workflow.get("env") or {})
        job_env.update(job.get("env") or {})
        context = self.context({"needs": needs, "env": {}})
        for key, value in job_env.items():
            rendered = substitute(str(value), context, functions_for(False))
            env[key] = rendered
            context["env"][key] = rendered
        self.failed = False
        try:
            for step in job["steps"]:
                self._step(name, step, workspace, env, context)
        except RuntimeError as error:
            self.result.steps.append(StepRecord(name, f"stopped: {error}"))
        shutil.rmtree(workspace, ignore_errors=True)

    def _step(self, job: str, step: dict, workspace: Path, env: dict[str, str], context: dict) -> None:
        condition = step.get("if")
        if condition is not None and is_falsy(self.eval(str(condition), context)):
            self.result.steps.append(StepRecord(job, f"skip {step.get('name') or step.get('uses')}"))
            return
        uses = step.get("uses")
        if uses:
            self._uses(job, str(uses), step, workspace, env, context)
            return
        script = substitute(str(step["run"]), context, functions_for(self.failed))
        for key, value in (step.get("env") or {}).items():
            env[key] = substitute(str(value), context, functions_for(self.failed))
            context["env"][key] = env[key]
        self.result.steps.append(StepRecord(job, f"run {step.get('name', '')}: {script.splitlines()[0][:80]}"))
        # Job `run` steps leave shell unspecified: bash -e {0}.
        run_bash(script, workspace, env, ["-e"])
        apply_env_file(env, Path(env["GITHUB_ENV"]))
        apply_path_file(env, Path(env["GITHUB_PATH"]))
        for key, value in env.items():
            if key.isidentifier():
                context["env"][key] = value

    def _uses(self, job: str, uses: str, step: dict, workspace: Path, env: dict[str, str], context: dict) -> None:
        if uses.startswith("actions/checkout@"):
            self.result.steps.append(StepRecord(job, f"checkout {self.event['sha']}"))
            return
        if uses.startswith("dorny/paths-filter@"):
            self.result.steps.append(StepRecord(job, "paths-filter"))
            return
        if not uses.startswith("$/"):
            raise AssertionError(f"unhandled action {uses}")
        # Re-read the action from the workflow SHA tree at the step, not at the job `if`.
        relative = uses[2:]
        action_path = workspace / relative / "action.yml"
        if not action_path.exists():
            action_path = workspace / relative / "action.yaml"
        action = yaml.load(action_path.read_text(encoding="utf-8"), Loader=WorkflowLoader)
        self.result.steps.append(StepRecord(job, f"$/{relative} @ {self.event['sha']}"))
        inputs = {key: "" for key in (action.get("inputs") or {})}
        for key, value in (step.get("with") or {}).items():
            inputs[key] = substitute(str(value), context, functions_for(self.failed))
        action_context = dict(context)
        action_context["inputs"] = inputs
        for child in action["runs"]["steps"]:
            child_if = child.get("if")
            if child_if is not None and is_falsy(self.eval(str(child_if), action_context)):
                continue
            if "uses" in child:
                self._uses(job, str(child["uses"]), child, workspace, env, action_context)
                continue
            script = substitute(str(child["run"]), action_context, functions_for(self.failed))
            for key, value in (child.get("env") or {}).items():
                env[key] = substitute(str(value), action_context, functions_for(self.failed))
            shell = child.get("shell")
            # Composite steps set `shell: bash`: bash --noprofile --norc -e -o pipefail {0}.
            args = ["--noprofile", "--norc", "-e", "-o", "pipefail"] if shell == "bash" else ["-e"]
            run_bash(script, workspace, env, args)
            apply_env_file(env, Path(env["GITHUB_ENV"]))
            apply_path_file(env, Path(env["GITHUB_PATH"]))

    def _later_privileged_job(self) -> None:
        """integration.yml `build` on the same pool sources the runner user's cargo env."""
        document = load_yaml(INTEGRATION)
        job = document["jobs"]["build"]
        if str(job["runs-on"]) not in self.pools:
            raise AssertionError("integration build no longer shares the long-lived pool")
        if "id-token: write" not in (INTEGRATION.read_text(encoding="utf-8")):
            raise AssertionError("integration.yml no longer requests id-token: write")
        action = yaml.load(
            (REPO / ".github" / "actions" / "setup-rust" / "action.yml").read_text(encoding="utf-8"),
            Loader=WorkflowLoader,
        )
        workspace = Path(tempfile.mkdtemp(prefix="gha-later-"))
        env_file = workspace / "github.env"
        path_file = workspace / "github.path"
        env = os.environ.copy()
        env.update(
            {
                "HOME": str(self.home),
                "GITHUB_WORKSPACE": str(workspace),
                "GITHUB_ENV": str(env_file),
                "GITHUB_PATH": str(path_file),
                "GITHUB_OUTPUT": str(workspace / "github.out"),
            }
        )
        env["PATH"] = f"{self.home / '.cargo' / 'bin'}:{env.get('PATH', '')}"
        context = {"inputs": {"toolchain": ""}, "runner": {"os": "Linux"}, "env": {}, "github": {}}
        try:
            for child in action["runs"]["steps"]:
                child_if = child.get("if")
                if child_if is not None and is_falsy(evaluate(str(child_if), context, functions_for(False))):
                    continue
                if "run" not in child:
                    continue
                script = substitute(str(child["run"]), context, functions_for(False))
                shell = child.get("shell")
                args = ["--noprofile", "--norc", "-e", "-o", "pipefail"] if shell == "bash" else ["-e"]
                run_bash(script, workspace, env, args)
                apply_env_file(env, env_file)
                apply_path_file(env, path_file)
        finally:
            shutil.rmtree(workspace, ignore_errors=True)


def pull_request(repo: str, login: str) -> dict:
    return {
        "event_name": "pull_request",
        "ref": "refs/pull/1/merge",
        "sha": "merge-sha",
        "payload": {
            "pull_request": {
                "head": {"repo": {"full_name": repo}, "sha": "head-sha"},
                "user": {"login": login},
            }
        },
    }


def simulate(changed: list[str], repo: str, login: str) -> RunResult:
    if not REAL_TOOLCHAIN.is_dir():
        raise AssertionError(f"missing toolchain {REAL_TOOLCHAIN}")
    home = Path(tempfile.mkdtemp(prefix="runner-home-"))
    tree = Path(tempfile.mkdtemp(prefix="merge-tree-"))
    try:
        seed_toolchain(home)
        merge_tree(tree)
        for path in changed:
            target = tree / path
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                target.write_text("", encoding="utf-8")
        return Executor(tree, home, pull_request(repo, login), changed).run()
    except Exception:
        shutil.rmtree(home, ignore_errors=True)
        shutil.rmtree(tree, ignore_errors=True)
        raise
    finally:
        shutil.rmtree(tree, ignore_errors=True)


def assert_not_executed(test: unittest.TestCase, result: RunResult) -> None:
    test.assertFalse(result.marker(SHELL_MARKER), f"setup-cc from the merge commit ran: {result.steps}")
    test.assertFalse(result.marker(BUILD_MARKER), f"cargo build ran the pull request build.rs: {result.steps}")
    test.assertFalse(result.marker(BINARY_MARKER), f"the built binary ran: {result.steps}")
    test.assertFalse(
        result.marker(LATER_MARKER),
        f"a later setup-rust on the same home sourced the planted cargo env: {result.steps}",
    )
    if result.home:
        shutil.rmtree(result.home, ignore_errors=True)


class ExpressionTests(unittest.TestCase):
    def test_string_equality_is_case_insensitive(self) -> None:
        self.assertTrue(abstract_equal("SpiceAI/spiceai", "spiceai/spiceai"))
        self.assertTrue(abstract_equal("true", "TRUE"))

    def test_and_or_return_operand_values(self) -> None:
        context = {"github": {"event_name": "pull_request"}}
        self.assertEqual(evaluate("false && true", context, functions_for(False)), False)
        self.assertEqual(
            evaluate("github.event_name != 'pull_request' || 'kept'", context, functions_for(False)),
            "kept",
        )

    def test_same_repo_guard_matches_this_repository_only(self) -> None:
        expression = (
            "github.event_name != 'pull_request' || "
            "(github.event.pull_request.head.repo.full_name == github.repository && "
            "github.event.pull_request.user.login != 'dependabot[bot]')"
        )
        same = {
            "github": {
                "event_name": "pull_request",
                "repository": "spiceai/spiceai",
                "event": {"pull_request": {"head": {"repo": {"full_name": "spiceai/spiceai"}}, "user": {"login": "octo"}}},
            }
        }
        fork = {
            "github": {
                "event_name": "pull_request",
                "repository": "spiceai/spiceai",
                "event": {"pull_request": {"head": {"repo": {"full_name": "octo/spiceai"}}, "user": {"login": "octo"}}},
            }
        }
        bot = {
            "github": {
                "event_name": "pull_request",
                "repository": "spiceai/spiceai",
                "event": {
                    "pull_request": {
                        "head": {"repo": {"full_name": "spiceai/spiceai"}},
                        "user": {"login": "dependabot[bot]"},
                    }
                },
            }
        }
        self.assertTrue(evaluate(expression, same, functions_for(False)))
        self.assertFalse(evaluate(expression, fork, functions_for(False)))
        self.assertFalse(evaluate(expression, bot, functions_for(False)))


class GlobTests(unittest.TestCase):
    def test_github_cheat_sheet_samples(self) -> None:
        self.assertTrue(github_glob("docs/**", "docs/README.md"))
        self.assertTrue(github_glob("docs/**", "docs/mona/octocat.txt"))
        self.assertTrue(github_glob("crates/cayenne/**", "crates/cayenne/src/lib.rs"))
        self.assertFalse(github_glob("crates/cayenne/**", "crates/cayenne-metastore-model/src/lib.rs"))
        self.assertTrue(github_glob("*.js", "app.js"))
        self.assertFalse(github_glob("*.js", "src/app.js"))

    def test_picomatch_matches_paths_filter_globs(self) -> None:
        self.assertTrue(picomatch_any(["crates/cayenne/**"], "crates/cayenne/src/lib.rs"))
        self.assertFalse(picomatch_any(["crates/cayenne/**"], "crates/cayenne-metastore-model/src/lib.rs"))
        self.assertTrue(
            picomatch_any(
                [".github/workflows/cayenne_object_native.yml"],
                ".github/workflows/cayenne_object_native.yml",
            )
        )


class PersistentRunnerTests(unittest.TestCase):
    def test_same_repo_model_change_does_not_execute(self) -> None:
        result = simulate(
            ["crates/cayenne-metastore-model/src/attack.rs", ".github/actions/setup-cc/action.yml"],
            "spiceai/spiceai",
            "collaborator",
        )
        assert_not_executed(self, result)

    def test_same_repo_cayenne_change_does_not_execute(self) -> None:
        result = simulate(
            ["crates/cayenne/src/attack.rs", ".github/actions/setup-cc/action.yml"],
            "spiceai/spiceai",
            "collaborator",
        )
        assert_not_executed(self, result)

    def test_same_repo_workflow_change_does_not_execute(self) -> None:
        result = simulate(
            [".github/workflows/cayenne_object_native.yml", ".github/actions/setup-cc/action.yml"],
            "spiceai/spiceai",
            "collaborator",
        )
        assert_not_executed(self, result)

    def test_fork_pull_request_does_not_execute(self) -> None:
        result = simulate(
            ["crates/cayenne/src/attack.rs", ".github/actions/setup-cc/action.yml"],
            "attacker/spiceai",
            "attacker",
        )
        assert_not_executed(self, result)

    def test_dependabot_pull_request_does_not_execute(self) -> None:
        result = simulate(
            ["crates/object_store_occ/src/attack.rs", ".github/actions/setup-cc/action.yml"],
            "spiceai/spiceai",
            "dependabot[bot]",
        )
        assert_not_executed(self, result)

    def test_unrelated_path_does_not_trigger(self) -> None:
        result = simulate(["README.md"], "spiceai/spiceai", "collaborator")
        self.assertFalse(result.triggered)
        assert_not_executed(self, result)

    def test_setup_cc_alone_does_not_trigger(self) -> None:
        result = simulate([".github/actions/setup-cc/action.yml"], "spiceai/spiceai", "collaborator")
        self.assertFalse(result.triggered)
        assert_not_executed(self, result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
