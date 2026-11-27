#!/usr/bin/env python3
"""Tests for where setup-spiced reads spiced, and that a read of trunk's build is attested.

A build lives where the ref that built it stores it: trunk's builds at the shared keys,
another ref's under `artifacts/untrusted/<repository id>/<ref>/`. So the place to read is the
build's ref, never the reader's. The action's own steps run here, from action.yml, against a
local Git server holding trunk and a feature commit and a stand-in `mc` that answers only
for the keys a test stores, so nothing is downloaded.
"""

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parent.parent / "actions" / "setup-spiced" / "action.yml"
COLD_START = Path(__file__).resolve().parent.parent / "workflows" / "testoperator_run_cold_start.yml"
REPOSITORY_ID = "394089041"
PULL_REQUEST = "refs/pull/16720/merge"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


TOKEN = re.compile(r"\s*(?:('(?:[^']|'')*')|(&&|\|\||==|!=|[(),!])|([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_-]*)*))")
FUNCTIONS = {
    "format": lambda text, *args: re.sub(r"\{(\d+)\}", lambda m: str(args[int(m.group(1))]), text),
    "startsWith": lambda text, start: str(text).lower().startswith(str(start).lower()),
}


def evaluate(expression, context):
    """A `${{ }}` expression's value, by GitHub's rules for the operators these workflows use:
    `&&` and `||` return an operand, `==` compares strings without case, and a missing context
    value is empty."""
    body = expression.strip()
    assert body.startswith("${{") and body.endswith("}}"), expression
    body = body[3:-2].strip()
    tokens, pos = [], 0
    while pos < len(body):
        m = TOKEN.match(body, pos)
        assert m and m.end() > pos, f"cannot read {body[pos:]!r}"
        string, op, name = m.groups()
        tokens.append(("str", string[1:-1].replace("''", "'")) if string is not None else ("op", op) if op else ("name", name))
        pos = m.end()
    i = 0

    def peek():
        return tokens[i] if i < len(tokens) else (None, None)

    def take(expected=None):
        nonlocal i
        assert expected is None or peek() == expected, (peek(), expected)
        i += 1
        return tokens[i - 1]

    def either():
        value = both()
        while peek() == ("op", "||"):
            take()
            other = both()
            value = value if value else other
        return value

    def both():
        value = compare()
        while peek() == ("op", "&&"):
            take()
            other = compare()
            value = other if value else value
        return value

    def compare():
        value = unary()
        if peek() in (("op", "=="), ("op", "!=")):
            op = take()[1]
            same = str(unary()).lower() == str(value).lower()
            value = same if op == "==" else not same
        return value

    def unary():
        if peek() == ("op", "!"):
            take()
            return not unary()
        if peek() == ("op", "("):
            take()
            value = either()
            take(("op", ")"))
            return value
        kind, value = take()
        if kind == "str":
            return value
        assert kind == "name", value
        if peek() == ("op", "("):
            take()
            args = []
            while peek() != ("op", ")"):
                args.append(either())
                if peek() == ("op", ","):
                    take()
            take(("op", ")"))
            return FUNCTIONS[value](*args)
        found = context
        for part in value.split("."):
            found = found.get(part, "") if isinstance(found, dict) else ""
        return found

    value = either()
    assert i == len(tokens), tokens[i:]
    return value


# Logs every call, and `stat` succeeds only for a key listed in $MC_STORED.
FAKE_MC = """#!/usr/bin/env bash
echo "mc $*" >> "$MC_LOG"
case "$1" in
  stat) grep -qxF -- "$2" "$MC_STORED" ;;
  cp) grep -qxF -- "$2" "$MC_STORED" && : > "$3" ;;
esac
"""


class SetupSpicedTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name).resolve()
        work = root / "work"
        work.mkdir()
        git(work, "init", "-q", "-b", "trunk")
        git(work, "config", "user.email", "ci@example.invalid")
        git(work, "config", "user.name", "ci")
        for n in range(3):
            (work / "f").write_text(str(n))
            git(work, "add", "f")
            git(work, "commit", "-qm", f"trunk {n}")
        cls.trunk = git(work, "rev-parse", "HEAD")
        git(work, "checkout", "-qb", "feature")
        (work / "f").write_text("feature")
        git(work, "commit", "-qam", "feature")
        cls.feature = git(work, "rev-parse", "HEAD")
        # A release branch cut from trunk, with a commit of its own.
        git(work, "checkout", "-q", "trunk")
        git(work, "checkout", "-qb", "release/1.0")
        (work / "f").write_text("release")
        git(work, "commit", "-qam", "release")
        cls.release = git(work, "rev-parse", "HEAD")
        git(work, "checkout", "-q", "trunk")
        server = root / "server"
        (server / "owner").mkdir(parents=True)
        bare = server / "owner" / "repo.git"
        git(root, "clone", "-q", "--bare", str(work), str(bare))
        git(bare, "config", "uploadpack.allowFilter", "true")
        cls.server = f"file://{server}"
        # The job's checkout, whose `origin` the find step fetches `ref`'s last commits from.
        cls.checkout = root / "checkout"
        git(root, "clone", "-q", str(bare), str(cls.checkout))
        action = yaml.safe_load(ACTION.read_text())
        cls.steps = {s.get("id") or s["name"]: s for s in action["runs"]["steps"]}
        cls.bin = root / "bin"
        cls.bin.mkdir()
        (cls.bin / "mc").write_text(FAKE_MC)
        (cls.bin / "mc").chmod(0o755)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(dir=self.tmp.name))
        self.mc_log = self.dir / "mc.log"
        self.stored = self.dir / "stored"
        self.mc_log.write_text("")
        self.stored.write_text("")

    def key(self, prefix, commit):
        return f"spice-minio/artifacts/{prefix}spiced/linux-x86_64/{commit}/spiced_linux_x86_64.tar.gz"

    def run_step(self, step, env):
        script = self.dir / "step.sh"
        script.write_text(self.steps[step]["run"])
        github_output = self.dir / "github_output"
        github_output.write_text("")
        github_env = self.dir / "github_env"
        github_env.write_text("")
        full_env = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(self.dir),
            "RUNNER_TEMP": str(self.dir),
            "GITHUB_OUTPUT": str(github_output),
            "GITHUB_ENV": str(github_env),
            "GITHUB_SERVER_URL": self.server,
            "GITHUB_REPOSITORY": "owner/repo",
            "GITHUB_REPOSITORY_ID": REPOSITORY_ID,
            "GH_TOKEN": "test-token",
            "MC_LOG": str(self.mc_log),
            "MC_STORED": str(self.stored),
            "PLATFORM_OS": "linux",
            "PLATFORM_ARCH": "x86_64",
            "PLATFORM_EXT": "tar.gz",
            "INPUT_WITH_ODBC": "",
            **env,
        }
        proc = subprocess.run(["bash", "-e", str(script)], cwd=self.checkout, env=full_env,
                              capture_output=True, text=True, timeout=60)
        values = dict(line.split("=", 1) for line in github_output.read_text().splitlines() if "=" in line)
        values.update(dict(line.split("=", 1) for line in github_env.read_text().splitlines() if "=" in line))
        return proc, values

    def locate(self, github_ref, ref="refs/heads/trunk", spiced_commit="", verify="false", event="push", base_ref=""):
        return self.run_step("locate", {"GITHUB_REF": github_ref, "REF": ref, "INPUT_SPICED_COMMIT": spiced_commit,
                                   "INPUT_VERIFY": verify, "RUN_EVENT": event,
                                   "PULL_REQUEST_BASE": base_ref if event == "pull_request" else ""})

    def test_a_pull_request_finds_trunks_latest_build_at_trunks_keys(self):
        # The cold-start regression: a pull request looked for trunk's builds under its own prefix.
        proc, located = self.locate(PULL_REQUEST)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": "", "trunk": "true"})
        self.stored.write_text(self.key("", self.trunk) + "\n")
        proc, found = self.run_step("Find the latest built spiced commit", {"REF": "refs/heads/trunk", "SOURCE_PREFIX": ""})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["SPICED_COMMIT"], self.trunk)
        self.assertIn(f"mc stat {self.key('', self.trunk)}", self.mc_log.read_text())
        self.assertNotIn("untrusted", self.mc_log.read_text())

    def test_a_trunk_commit_is_read_at_trunks_keys_from_any_ref(self):
        for github_ref in (PULL_REQUEST, "refs/heads/feature", "refs/heads/trunk"):
            with self.subTest(github_ref=github_ref):
                proc, located = self.locate(github_ref, spiced_commit=self.trunk)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": "", "trunk": "true"})

    def test_a_branch_build_is_read_where_that_branch_stored_it(self):
        proc, located = self.locate("refs/heads/feature", spiced_commit=self.feature)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": f"untrusted/{REPOSITORY_ID}/refs/heads/feature/", "trunk": "false"})
        # A release branch's own run, such as its leg of the scheduled dispatch.
        proc, located = self.locate("refs/heads/release/1.0", ref="refs/heads/release/1.0")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/release/1.0", "prefix": f"untrusted/{REPOSITORY_ID}/refs/heads/release/1.0/", "trunk": "false"})
        self.stored.write_text(self.key(located["prefix"], self.trunk) + "\n")
        self.run_step("download", {"SOURCE_PREFIX": located["prefix"], "SPICED_COMMIT": self.trunk})
        self.assertIn(f"mc cp {self.key(located['prefix'], self.trunk)} ", self.mc_log.read_text())

    def test_a_bare_branch_name_is_that_branch(self):
        proc, located = self.locate(PULL_REQUEST, ref="trunk")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": "", "trunk": "true"})
        proc, located = self.locate("refs/heads/release/1.0", ref="release/1.0")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located["ref"], "refs/heads/release/1.0")
        self.assertEqual(located["prefix"], f"untrusted/{REPOSITORY_ID}/refs/heads/release/1.0/")

    def test_a_ref_that_is_not_a_plain_ref_is_refused(self):
        for ref in ("refs/heads/x\nprefix<<E\nE\nz=", "refs/heads/a/../../../../", "refs/heads//x", "refs/heads/x/",
                    "refs/heads/x y"):
            with self.subTest(ref=ref):
                proc, located = self.locate(PULL_REQUEST, ref=ref)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("is not a plain ref", proc.stdout)
                self.assertEqual(located, {})

    def test_no_spiced_commit_means_no_download(self):
        proc, outputs = self.run_step("output_spiced_commit", {"SPICED_COMMIT": ""})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("no spiced commit was chosen", proc.stdout)

    def test_a_run_never_takes_another_refs_build(self):
        # A build of a ref other than trunk runs only in a run of that ref, with that ref's
        # credentials: a trunk run's store credentials write trunk's keys.
        for github_ref, ref, commit in (
            ("refs/heads/trunk", "refs/heads/feature", ""),
            ("refs/heads/trunk", "feature", ""),
            ("refs/heads/trunk", "refs/heads/release/1.0", ""),
            ("refs/heads/trunk", "refs/heads/feature", "feature"),
            ("refs/heads/trunk", "refs/heads/trunk", "feature"),
            (PULL_REQUEST, "refs/heads/release/1.0", ""),
            ("refs/heads/feature", "refs/heads/release/1.0", ""),
        ):
            with self.subTest(github_ref=github_ref, ref=ref, commit=commit):
                proc, located = self.locate(github_ref, ref=ref, spiced_commit=self.feature if commit else "")
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("runs only a spiced that trunk or this run's own ref built", proc.stdout)
                self.assertEqual(located, {})

    def test_a_pull_request_reads_its_base_branchs_build(self):
        prefix = f"untrusted/{REPOSITORY_ID}/refs/heads/release/1.0/"
        proc, located = self.locate("refs/pull/7/merge", ref="refs/heads/release/1.0", event="pull_request",
                                    base_ref="release/1.0")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/release/1.0", "prefix": prefix, "trunk": "false"})
        # Only its own base, and only for a pull_request event: the base name comes from the event.
        for event, base_ref in (("pull_request", "release/2.0"), ("pull_request", ""), ("push", "release/1.0"),
                                ("pull_request_target", "release/1.0"), ("workflow_dispatch", "release/1.0")):
            with self.subTest(event=event, base_ref=base_ref):
                proc, located = self.locate("refs/pull/7/merge", ref="refs/heads/release/1.0", event=event,
                                            base_ref=base_ref)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("runs only a spiced that trunk or this run's own ref built", proc.stdout)
                self.assertEqual(located, {})
        # A job with the full CI roles still takes only trunk's attested builds.
        proc, located = self.locate("refs/pull/7/merge", ref="refs/heads/release/1.0", verify="true",
                                    event="pull_request", base_ref="release/1.0")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("runs only a spiced built and attested on trunk", proc.stdout)

    def test_the_pull_request_check_reads_the_event_not_the_environment(self):
        locate = self.steps["locate"]
        self.assertEqual(locate["env"]["RUN_EVENT"], "${{ github.event_name }}")
        self.assertEqual(locate["env"]["PULL_REQUEST_BASE"],
                         "${{ github.event_name == 'pull_request' && github.base_ref || '' }}")
        self.assertNotIn("GITHUB_BASE_REF", locate["run"])
        self.assertNotIn("GITHUB_EVENT_NAME", locate["run"])
        # GITHUB_* values a step could add to the environment change nothing.
        proc, located = self.run_step("locate", {
            "GITHUB_REF": "refs/pull/7/merge", "REF": "refs/heads/release/1.0", "INPUT_SPICED_COMMIT": "",
            "INPUT_VERIFY": "false", "RUN_EVENT": "push", "PULL_REQUEST_BASE": "",
            "GITHUB_EVENT_NAME": "pull_request", "GITHUB_BASE_REF": "release/1.0"})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertEqual(located, {})

    def test_cold_start_reads_the_build_of_the_code_under_test(self):
        steps = yaml.safe_load(COLD_START.read_text())["jobs"]["cold-start"]["steps"]
        ref = next(s for s in steps if s.get("id") == "setup-spiced")["with"]["ref"]
        release_prefix = f"untrusted/{REPOSITORY_ID}/refs/heads/release/1.0/"
        self.stored.write_text(self.key(release_prefix, self.release) + "\n" + self.key("", self.trunk) + "\n")
        for event, github_ref, base_ref, expected, prefix, commit in (
            ("schedule", "refs/heads/trunk", "", "refs/heads/trunk", "", self.trunk),
            ("workflow_dispatch", "refs/heads/feature", "", "refs/heads/trunk", "", self.trunk),
            ("pull_request", PULL_REQUEST, "trunk", "refs/heads/trunk", "", self.trunk),
            # A release branch cut from trunk carries this file: its runs and its pull requests
            # test the branch's own build, not trunk's newer runtime.
            ("workflow_dispatch", "refs/heads/release/1.0", "", "refs/heads/release/1.0", release_prefix, self.release),
            ("pull_request", "refs/pull/7/merge", "release/1.0", "refs/heads/release/1.0", release_prefix, self.release),
        ):
            with self.subTest(event=event, github_ref=github_ref):
                resolved = evaluate(ref, {"github": {"event_name": event, "ref": github_ref, "base_ref": base_ref}})
                self.assertEqual(resolved, expected)
                proc, located = self.locate(github_ref, ref=resolved, event=event, base_ref=base_ref)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(located["prefix"], prefix)
                proc, found = self.run_step("Find the latest built spiced commit",
                                            {"REF": located["ref"], "SOURCE_PREFIX": located["prefix"]})
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(found["SPICED_COMMIT"], commit)

    def test_no_build_of_the_ref_says_which_ref_and_where(self):
        # A ref with no build of its own fails with the ref and the prefix it searched.
        prefix = f"untrusted/{REPOSITORY_ID}/refs/heads/release/1.0/"
        self.stored.write_text(self.key("", self.release) + "\n")
        proc, found = self.run_step("Find the latest built spiced commit",
                                    {"REF": "refs/heads/release/1.0", "SOURCE_PREFIX": prefix})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertNotIn("SPICED_COMMIT", found)
        self.assertIn("::error::No spiced build of refs/heads/release/1.0's last 10 commits is stored", proc.stdout)
        self.assertIn(f"under artifacts/{prefix}spiced/", proc.stdout)
        self.assertIn("run build_release.yml on refs/heads/release/1.0 first", proc.stdout)

    def test_a_spiced_commit_that_is_not_a_full_hash_is_refused(self):
        for commit in (f"{self.trunk}\nprefix=", self.trunk[:12], f"{self.trunk[:39]}$(id)", "../../../../x",
                       self.trunk.upper()):
            with self.subTest(commit=commit):
                proc, located = self.locate("refs/heads/feature", spiced_commit=commit)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("must be a full 40-character commit hash", proc.stdout)
                self.assertEqual(located, {})

    def test_a_full_role_job_never_takes_a_branch_build(self):
        proc, located = self.locate("refs/heads/release/1.0", ref="refs/heads/release/1.0", verify="true")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("runs only a spiced built and attested on trunk", proc.stdout)
        self.assertEqual(located, {})


    def test_every_read_at_trunks_keys_is_verified_whatever_the_role(self):
        verify = self.steps["Verify spiced's build attestation"]
        self.assertEqual(verify["if"], "steps.locate.outputs.trunk == 'true' || inputs.verify == 'true'")
        self.assertEqual(verify["with"]["source-commit"], "${{ steps.output_spiced_commit.outputs.SPICED_COMMIT }}")
        order = list(self.steps)
        self.assertLess(order.index("download"), order.index("Verify spiced's build attestation"))
        self.assertLess(order.index("Verify spiced's build attestation"), order.index("Install spiced"))

    def test_no_read_uses_the_readers_own_prefix(self):
        for step in ("Find the latest built spiced commit", "download"):
            with self.subTest(step=step):
                self.assertNotIn("CI_STORE_PREFIX", self.steps[step]["run"])
                self.assertIn("${SOURCE_PREFIX}spiced/", self.steps[step]["run"])


if __name__ == "__main__":
    unittest.main()
