#!/usr/bin/env python3
"""Tests that a trunk run builds and runs only trunk's images.

- spiced_docker_dev builds only a commit on trunk in a run on trunk: its build jobs hold that
  run's OIDC token, trunk's store and registry identity. Its provenance step runs here, from the
  workflow file, in a clone of a local Git server holding trunk and a feature commit.
- The cluster dispatcher names a `trunk-` image only in a run on trunk.
- The spice-mssql-bench image that full-role benchmark jobs run as a service is published only
  from trunk and pinned by digest.
"""

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / "workflows"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def job_steps(workflow, job):
    return yaml.safe_load((WORKFLOWS / workflow).read_text())["jobs"][job]["steps"]


class SpicedDockerDevTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name).resolve()
        work = root / "work"
        work.mkdir()
        git(work, "init", "-q", "-b", "trunk")
        git(work, "config", "user.email", "ci@example.invalid")
        git(work, "config", "user.name", "ci")
        (work / "f").write_text("0")
        git(work, "add", "f")
        git(work, "commit", "-qm", "trunk")
        cls.trunk = git(work, "rev-parse", "HEAD")
        git(work, "checkout", "-qb", "feature")
        (work / "f").write_text("feature")
        git(work, "commit", "-qam", "feature")
        cls.feature = git(work, "rev-parse", "HEAD")
        cls.bare = root / "repo.git"
        git(root, "clone", "-q", "--bare", str(work), str(cls.bare))
        cls.root = root
        steps = job_steps("spiced_docker_dev.yml", "setup")
        cls.script = next(s for s in steps if s.get("id") == "provenance")["run"]

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def provenance(self, github_ref, commit, tag="my-feature"):
        checkout = Path(tempfile.mkdtemp(dir=self.root))
        git(checkout, "clone", "-q", str(self.bare), ".")
        git(checkout, "checkout", "-q", commit)
        output = checkout / "github_output"
        output.write_text("")
        (checkout / "step.sh").write_text(self.script)
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(checkout),
            "GITHUB_OUTPUT": str(output),
            "GITHUB_REF": github_ref,
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_REF_NAME": github_ref.removeprefix("refs/heads/"),
            "INPUT_TAG": tag,
        }
        proc = subprocess.run(["bash", "-e", str(checkout / "step.sh")], cwd=checkout, env=env,
                              capture_output=True, text=True, timeout=60)
        return proc, output.read_text()

    def test_a_trunk_run_builds_only_a_trunk_commit(self):
        proc, output = self.provenance("refs/heads/trunk", self.feature)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("builds only a commit on trunk", proc.stdout)
        proc, output = self.provenance("refs/heads/trunk", self.trunk)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("on_trunk=true", output)

    def test_only_a_trunk_run_publishes_a_trunk_tag_and_only_for_its_commit(self):
        proc, _ = self.provenance("refs/heads/trunk", self.trunk, tag=f"trunk-{self.trunk[:12]}")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        for github_ref, commit, tag in (
            ("refs/heads/trunk", self.trunk, "trunk-0123456789ab"),
            ("refs/heads/feature", self.feature, f"trunk-{self.feature[:12]}"),
            ("refs/heads/feature", self.feature, f"trunk-{self.trunk[:12]}"),
        ):
            with self.subTest(github_ref=github_ref, tag=tag):
                proc, _ = self.provenance(github_ref, commit, tag=tag)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("Only a run on trunk publishes a trunk- tag", proc.stdout)
        proc, _ = self.provenance("refs/heads/feature", self.feature, tag="dev-feature")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_a_trunk_runs_image_is_attested_after_it_is_pushed(self):
        workflow = yaml.safe_load((WORKFLOWS / "spiced_docker_dev.yml").read_text())
        job = workflow["jobs"]["publish"]
        self.assertEqual(job["permissions"]["attestations"], "write")
        names = [s.get("name") for s in job["steps"]]
        attest = job["steps"][names.index("Attest the image")]
        self.assertEqual(attest["if"], "github.ref == 'refs/heads/trunk'")
        self.assertRegex(attest["uses"], r"^actions/attest-build-provenance@[0-9a-f]{40}$")
        self.assertEqual(attest["with"]["subject-digest"], "${{ steps.push.outputs.digest }}")
        self.assertIs(attest["with"]["push-to-registry"], True)
        self.assertLess(names.index("Import images and create manifest"), names.index("Attest the image"))

    def run_publish(self, ghcr_digest=None):
        """The publish step with a stub docker: the job's registry holds the index this job made."""
        job = yaml.safe_load((WORKFLOWS / "spiced_docker_dev.yml").read_text())["jobs"]["publish"]
        step = next(s for s in job["steps"] if s.get("id") == "push")
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "image-amd64.tar").write_text("")
            (tmp / "bin").mkdir()
            built = "sha256:" + "a" * 64
            calls = tmp / "calls"
            (tmp / "bin" / "docker").write_text(
                "#!/usr/bin/env bash\n"
                f'echo "$*" >> "{calls}"\n'
                'if [ "$1 $2 $3" = "buildx imagetools inspect" ] && [ "${5:-}" = --format ]; then\n'
                f'  case "$4" in localhost:5000/*) d="{built}" ;; *) d="{ghcr_digest or built}" ;; esac\n'
                '  printf \'{"digest":"%s"}\\n\' "$d"\n'
                "fi\n"
            )
            (tmp / "bin" / "docker").chmod(0o755)
            output = tmp / "output"
            output.write_text("")
            script = step["run"].replace("/tmp/", f"{tmp}/")
            env = {
                **os.environ,
                "PATH": f"{tmp / 'bin'}:{os.environ['PATH']}",
                "GITHUB_OUTPUT": str(output),
                "IMAGE_TAG": "trunk-0123456789ab",
                "REL_VERSION": "2.5.0-unstable",
            }
            proc = subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True, text=True)
            return proc, output.read_text(), calls.read_text().splitlines(), built

    def test_the_attested_digest_is_the_index_this_job_built(self):
        proc, output, calls, built = self.run_publish()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(output.strip(), f"digest={built}")
        copy = [c for c in calls if c.startswith("buildx imagetools create") and "ghcr.io" in c]
        self.assertEqual(len(copy), 1, calls)
        self.assertTrue(copy[0].endswith(f"localhost:5000/spiceai@{built}"), copy[0])
        self.assertIn("--prefer-index=false", copy[0])

    def test_a_tag_another_run_pushed_meanwhile_fails_before_the_attestation(self):
        proc, output, _, _ = self.run_publish(ghcr_digest="sha256:" + "b" * 64)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(output, "")
        self.assertIn("another run pushed the same tag", proc.stdout + proc.stderr)

    def test_another_refs_run_builds_its_commit(self):
        proc, output = self.provenance("refs/heads/feature", self.feature)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("on_trunk=false", output)

    def test_the_build_jobs_build_the_commit_setup_checked_and_keep_no_git_credentials(self):
        for job in ("build-arm64", "build-amd64"):
            with self.subTest(job=job):
                checkout = next(s for s in job_steps("spiced_docker_dev.yml", job)
                                if str(s.get("uses", "")).startswith("actions/checkout@"))
                self.assertEqual(checkout["with"]["ref"], "${{ needs.setup.outputs.revision }}")
                self.assertIs(checkout["with"]["persist-credentials"], False)

    def test_an_image_tag_is_validated(self):
        steps = job_steps("spiced_docker_dev.yml", "setup")
        script = next(s for s in steps if s.get("id") == "set_image_metadata")["run"]
        for tag, ok in (("my-feature", True), ("trunk-0123456789ab", True), ("a b", False),
                        ("x\nimage=evil", False), ("-x", False), ("a" * 129, False)):
            with self.subTest(tag=tag):
                with tempfile.TemporaryDirectory() as tmp:
                    output = Path(tmp) / "out"
                    output.write_text("")
                    proc = subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True,
                                          env={"PATH": os.environ["PATH"], "INPUT_TAG": tag, "GITHUB_OUTPUT": str(output)})
                    self.assertEqual(proc.returncode == 0, ok, proc.stdout + proc.stderr)
                    self.assertEqual(output.read_text() != "", ok)


class ClusterImageTest(unittest.TestCase):
    def test_a_trunk_runs_cluster_app_runs_only_an_attested_trunk_image(self):
        steps = job_steps("testoperator_run_cluster_bench_aws.yml", "run-cluster-bench")
        names = [s.get("name") or "" for s in steps]
        resolve = steps[names.index("Resolve the cluster app image")]
        verify = steps[names.index("Verify the cluster app image's attestation")]
        run = next(i for i, n in enumerate(names) if n.startswith("Run the cluster benchmark"))
        self.assertLess(names.index("Verify the cluster app image's attestation"), run)
        self.assertIn("github.ref == 'refs/heads/trunk'", resolve["if"])
        self.assertTrue(verify["if"].startswith("github.ref == 'refs/heads/trunk'"))
        self.assertEqual(verify["uses"], "$/.github/actions/verify-attestation")
        self.assertEqual(verify["with"]["image"], "${{ steps.app-image.outputs.image }}")
        self.assertEqual(verify["with"]["signer-workflow"], ".github/workflows/spiced_docker_dev.yml")

    def test_a_trunk_dispatch_takes_only_a_trunk_commit(self):
        steps = job_steps("testoperator_dispatch_cluster.yml", "resolve-image")
        script = next(s for s in steps if s.get("id") == "resolve")["run"]
        fake_gh = '#!/usr/bin/env bash\necho "$COMPARE_STATUS"\n'
        fake_docker = "#!/usr/bin/env bash\nexit 1\n"
        for github_ref, status, ok in (("refs/heads/trunk", "behind", True), ("refs/heads/trunk", "identical", True),
                                       ("refs/heads/trunk", "diverged", False), ("refs/heads/trunk", "ahead", False),
                                       ("refs/heads/feature", "ahead", True)):
            with self.subTest(github_ref=github_ref, status=status), tempfile.TemporaryDirectory() as tmp:
                work = Path(tmp)
                git(work, "init", "-q", "-b", "trunk")
                git(work, "config", "user.email", "ci@example.invalid")
                git(work, "config", "user.name", "ci")
                (work / "f").write_text("x")
                git(work, "add", "f")
                git(work, "commit", "-qm", "x")
                (work / "bin").mkdir()
                for name, text in (("gh", fake_gh), ("docker", fake_docker)):
                    (work / "bin" / name).write_text(text)
                    (work / "bin" / name).chmod(0o755)
                output = work / "out"
                output.write_text("")
                proc = subprocess.run(["bash", "-e", "-c", script], cwd=work, capture_output=True, text=True, env={
                    "PATH": f"{work / 'bin'}{os.pathsep}{os.environ['PATH']}", "GITHUB_REF": github_ref,
                    "GITHUB_REPOSITORY": "owner/repo", "GITHUB_OUTPUT": str(output), "COMPARE_STATUS": status,
                    "FORCE_REBUILD": "false", "CUSTOM_IMAGE_TAG": "", "INPUT_SPICED_REF": "trunk"})
                self.assertEqual(proc.returncode == 0, ok, proc.stdout + proc.stderr)

    def test_only_a_trunk_run_names_a_trunk_image(self):
        steps = job_steps("testoperator_dispatch_cluster.yml", "resolve-image")
        run = next(s for s in steps if s.get("id") == "resolve")["run"]
        self.assertRegex(run, r'refs/heads/trunk\) image_tag="trunk-\$\{spiced_sha_short\}"')
        self.assertRegex(run, r'\*\) image_tag="dev-\$\{spiced_sha_short\}"')
        self.assertEqual(len(re.findall(r'image_tag="trunk-', run)), 1)


class MssqlBenchImageTest(unittest.TestCase):
    PINNED = re.compile(r"'ghcr\.io/spiceai/spice-mssql-bench@sha256:[0-9a-f]{64}'")

    def test_it_is_published_only_from_trunk(self):
        steps = job_steps("spice_mssql_tpch_bench_docker.yml", "build")
        self.assertEqual(steps[0]["name"], "Refuse a run off trunk")
        self.assertIn('[ "$GITHUB_REF" != refs/heads/trunk ]', steps[0]["run"])

    def test_every_job_that_runs_it_pins_its_digest(self):
        users = 0
        for workflow in sorted(WORKFLOWS.glob("*.yml")):
            text = workflow.read_text()
            if workflow.name == "spice_mssql_tpch_bench_docker.yml" or "spice-mssql-bench" not in text:
                continue
            users += 1
            with self.subTest(workflow=workflow.name):
                for line in text.splitlines():
                    if "spice-mssql-bench" in line and "image:" in line:
                        self.assertRegex(line, self.PINNED)
        self.assertGreaterEqual(users, 2)


if __name__ == "__main__":
    unittest.main()
