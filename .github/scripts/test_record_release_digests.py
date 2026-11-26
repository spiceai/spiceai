#!/usr/bin/env python3
"""Tests for record_release_digests.py, which publish_release.yml runs to record a release."""

import json
import tempfile
import unittest
from pathlib import Path

import record_release_digests as r

DIGESTS = Path(__file__).resolve().parents[1] / "release-digests.json"


def sums(**digests):
    return "".join(f"{digest}  {name}\n" for name, digest in digests.items())


class RecordReleaseDigestsTest(unittest.TestCase):
    def setUp(self):
        self.files = {name: str(i) * 64 for i, name in enumerate(r.ASSETS, start=1)}
        self.text = sums(**self.files, **{"spice_linux_x86_64.tar.gz": "9" * 64, "install.sh": "8" * 64})

    def test_a_release_records_its_spiced_archives_in_version_order(self):
        document = json.loads(DIGESTS.read_text())
        updated = r.record(document, "v2.4.10", self.text)
        self.assertEqual(updated["releases"]["v2.4.10"], self.files)
        self.assertEqual(list(updated["releases"])[-1], "v2.4.10")
        self.assertEqual(updated["description"], document["description"])
        self.assertEqual({k: v for k, v in updated["releases"].items() if k != "v2.4.10"}, document["releases"])

    def test_a_star_line_counts_and_a_missing_archive_is_refused(self):
        star = self.text.replace(f"  {r.ASSETS[0]}", f" *{r.ASSETS[0]}")
        self.assertEqual(r.record({}, "v2.5.0", star)["releases"]["v2.5.0"], self.files)
        short = "".join(line + "\n" for line in self.text.splitlines() if r.ASSETS[1] not in line)
        with self.assertRaisesRegex(r.Error, "does not list spiced_linux_aarch64"):
            r.record({}, "v2.5.0", short)

    def test_only_a_final_release_is_recorded(self):
        for tag in ("v2.5.0-rc.1", "2.5.0", "v2.5", "v2.5.0/../x"):
            with self.subTest(tag=tag), self.assertRaisesRegex(r.Error, "not a release tag"):
                r.record({}, tag, self.text)

    def test_a_recorded_release_never_changes(self):
        once = r.record({}, "v2.5.0", self.text)
        self.assertEqual(r.record(once, "v2.5.0", self.text), once)
        other = self.text.replace(self.files[r.ASSETS[2]], "f" * 64)
        with self.assertRaisesRegex(r.Error, "already recorded with other digests"):
            r.record(once, "v2.5.0", other)

    def test_main_writes_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            digests = Path(tmp) / "release-digests.json"
            digests.write_text(DIGESTS.read_text())
            sums_file = Path(tmp) / "SHA256SUMS"
            sums_file.write_text(self.text)
            self.assertEqual(r.main(["--tag", "v2.4.10", "--sums", str(sums_file), "--digests", str(digests)]), 0)
            self.assertIn("v2.4.10", json.loads(digests.read_text())["releases"])
            self.assertTrue(digests.read_text().endswith("}\n"))
            self.assertEqual(r.main(["--tag", "v2.5.0-rc.1", "--sums", str(sums_file), "--digests", str(digests)]), 1)


class PublishReleaseRecordsTest(unittest.TestCase):
    def test_publishing_opens_the_trunk_change_from_the_attested_sha256sums(self):
        import yaml

        workflow = Path(__file__).resolve().parents[1] / "workflows" / "publish_release.yml"
        job = yaml.safe_load(workflow.read_text())["jobs"]["record-digests"]
        self.assertEqual(job["needs"], ["resolve", "publish"])
        steps = job["steps"]
        self.assertEqual(steps[0]["with"]["ref"], "trunk")
        verify = next(s for s in steps if s.get("uses") == "$/.github/actions/verify-attestation")
        self.assertEqual(verify["with"]["release"], "true")
        self.assertEqual(verify["with"]["source-commit"], "${{ needs.resolve.outputs.commit }}")
        run = steps[-1]["run"]
        self.assertLess(steps.index(verify), len(steps) - 1)
        self.assertIn("record_release_digests.py", run)
        self.assertIn("--base trunk", run)

    def test_a_release_candidates_publish_records_nothing(self):
        import yaml

        workflow = Path(__file__).resolve().parents[1] / "workflows" / "publish_release.yml"
        job = yaml.safe_load(workflow.read_text())["jobs"]["record-digests"]
        self.assertEqual(job.get("if"), "${{ !contains(inputs.release_version, '-') }}")


if __name__ == "__main__":
    unittest.main()
