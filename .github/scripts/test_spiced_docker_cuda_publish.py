#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for the CUDA publish step in .github/workflows/spiced_docker.yml.
#
# That step decides which Docker tags a release publishes. It only ever runs
# during a release dispatch, on a self-hosted runner, against real registries —
# so a mistake in it is expensive to discover and cannot be reproduced locally.
#
# These tests extract the step's script straight out of the workflow and run it
# with a stub `docker` on PATH, so the assertions are against the same text CI
# executes rather than a copy that can drift. Nothing is pulled or pushed: the
# stub records its arguments and exits 0.
"""Execute the spiced_docker CUDA publish step against a stub docker."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "spiced_docker.yml"
STEP_NAME = "Import images and create manifests"
JOB_NAME = "publish-cuda"

# The stub stands in for every `docker` call the step makes. It appends its
# arguments to $DOCKER_LOG so the test can assert on what would have run.
DOCKER_STUB = """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$DOCKER_LOG"
exit 0
"""


def bash_path(path: Path) -> str:
    """Return `path` as the shell sees it.

    On CI this is the identity. It exists so the suite is also runnable on a
    Windows dev machine, where bash is present but `C:\\x` is not a path it can
    glob.
    """
    if sys.platform != "win32":
        return str(path)
    drive, rest = os.path.splitdrive(path)
    return f"/{drive[0].lower()}{rest.replace(os.sep, '/')}"


def publish_script() -> str:
    """Return the CUDA publish step's `run:` script, read from the workflow."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"][JOB_NAME]["steps"]
    for step in steps:
        if step.get("name") == STEP_NAME:
            return step["run"]
    raise AssertionError(f"{JOB_NAME} has no step named {STEP_NAME!r}")


def run_publish(
    caps: list[str],
    *,
    publish: str = "true",
    should_tag_latest: str = "true",
    rel_version: str = "9.9.9",
    default_cap: str = "80",
    extra_files: list[str] | None = None,
) -> tuple[int, list[str], str]:
    """Run the publish step over fake artifacts for `caps`.

    Returns (exit code, the stub's recorded docker invocations, stderr).

    The only edit made to the script is repointing /tmp at a scratch directory,
    so the test cannot collide with a real /tmp or with a parallel run.
    """
    script = publish_script()

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        artifacts = tmpdir / "artifacts"
        artifacts.mkdir()
        script = script.replace(
            "/tmp/images-amd64-cuda-", f"{bash_path(artifacts)}/images-amd64-cuda-"
        )

        for cap in caps:
            (artifacts / f"images-amd64-cuda-{cap}.tar").write_text("fake", encoding="utf-8")
        for name in extra_files or []:
            (artifacts / name).write_text("fake", encoding="utf-8")

        bin_dir = tmpdir / "bin"
        bin_dir.mkdir()
        stub = bin_dir / "docker"
        stub.write_text(DOCKER_STUB, encoding="utf-8", newline="\n")
        stub.chmod(0o755)

        log = tmpdir / "docker.log"
        log.touch()

        env = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "DOCKER_LOG": str(log),
            "PUBLISH": publish,
            "REL_VERSION": rel_version,
            "SHOULD_TAG_LATEST": should_tag_latest,
            "DEFAULT_CUDA_COMPUTE_CAP": default_cap,
        }
        completed = subprocess.run(
            ["bash", "-c", script], env=env, capture_output=True, text=True
        )
        calls = [line for line in log.read_text(encoding="utf-8").splitlines() if line]
        return completed.returncode, calls, completed.stderr


def tags_in(calls: list[str]) -> set[str]:
    """Return every `-t <ref>` argument across the recorded docker calls."""
    tags: set[str] = set()
    for call in calls:
        parts = call.split()
        for index, part in enumerate(parts):
            if part == "-t" and index + 1 < len(parts):
                tags.add(parts[index + 1])
    return tags


CAPS = ["80", "86", "87", "89", "90"]


class PublishTest(unittest.TestCase):
    def test_every_capability_gets_its_own_tag(self):
        """Regression test for #10622: five capabilities in, five tag pairs out."""
        code, calls, stderr = run_publish(CAPS)
        self.assertEqual(code, 0, stderr)
        tags = tags_in(calls)
        for cap in CAPS:
            self.assertIn(f"ghcr.io/spiceai/spiceai:9.9.9-cuda-{cap}", tags)
            self.assertIn(f"spiceai/spiceai:9.9.9-cuda-{cap}", tags)
            self.assertIn(f"ghcr.io/spiceai/spiceai:latest-cuda-{cap}", tags)
            self.assertIn(f"spiceai/spiceai:latest-cuda-{cap}", tags)

    def test_the_unsuffixed_tags_alias_the_default_capability(self):
        """`latest-cuda` has always been an sm_80 image and has to stay one."""
        code, calls, stderr = run_publish(CAPS)
        self.assertEqual(code, 0, stderr)
        tags = tags_in(calls)
        self.assertIn("ghcr.io/spiceai/spiceai:latest-cuda", tags)
        self.assertIn("ghcr.io/spiceai/spiceai:9.9.9-cuda", tags)

        # ...and it is created from the 80 image, not from whichever ran last.
        aliasing = [call for call in calls if " -t ghcr.io/spiceai/spiceai:latest-cuda " in f" {call} "]
        self.assertEqual(len(aliasing), 1)
        self.assertTrue(aliasing[0].endswith("localhost:5000/spiceai:cuda-80-amd64"))

    def test_a_non_default_capability_gets_no_unsuffixed_tag(self):
        code, calls, stderr = run_publish(["89"])
        self.assertEqual(code, 0, stderr)
        tags = tags_in(calls)
        self.assertIn("ghcr.io/spiceai/spiceai:latest-cuda-89", tags)
        self.assertNotIn("ghcr.io/spiceai/spiceai:latest-cuda", tags)
        self.assertNotIn("ghcr.io/spiceai/spiceai:9.9.9-cuda", tags)

    def test_a_pre_release_skips_the_latest_tags(self):
        code, calls, stderr = run_publish(CAPS, should_tag_latest="false")
        self.assertEqual(code, 0, stderr)
        tags = tags_in(calls)
        self.assertIn("ghcr.io/spiceai/spiceai:9.9.9-cuda-90", tags)
        self.assertFalse({tag for tag in tags if ":latest" in tag})

    def test_a_non_publish_run_touches_no_registry(self):
        code, calls, stderr = run_publish(CAPS, publish="false")
        self.assertEqual(code, 0, stderr)
        tags = tags_in(calls)
        self.assertTrue(all(tag.startswith("localhost:5000/") for tag in tags), tags)
        self.assertIn("localhost:5000/spiceai:latest-cuda-90", tags)
        self.assertIn("localhost:5000/spiceai:latest-cuda", tags)

    def test_no_artifacts_is_not_a_failure(self):
        """A run with CUDA disabled must no-op rather than fail the workflow."""
        code, calls, stderr = run_publish([])
        self.assertEqual(code, 0, stderr)
        self.assertEqual(calls, [])

    def test_a_stray_tarball_is_refused(self):
        """The capability is parsed from a filename, so it is re-validated."""
        code, _, stderr = run_publish(["80"], extra_files=["images-amd64-cuda-evil.tar"])
        self.assertEqual(code, 1)
        self.assertIn("is not a compute capability", stderr)

    def test_each_capability_is_loaded_from_its_own_tarball(self):
        code, calls, stderr = run_publish(CAPS)
        self.assertEqual(code, 0, stderr)
        loads = [call for call in calls if call.startswith("load -i ")]
        self.assertEqual(len(loads), len(CAPS))
        for cap in CAPS:
            self.assertTrue(
                any(call.endswith(f"images-amd64-cuda-{cap}.tar") for call in loads),
                f"no load for capability {cap}: {loads}",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
