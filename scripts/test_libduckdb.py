#!/usr/bin/env python3
"""Tests for libduckdb.py's cache: a cached install is reused only when it checks out.

CI keeps the cache in a directory another job on the host can write, so an install found
there is reused only when its kept archive matches the recorded SHA-256, its library is the
one that archive holds and its manifest is the one the script writes. These tests run
`install` against a fabricated release, with `download` replaced, so nothing is fetched.
"""

import hashlib
import io
import json
import os
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import libduckdb  # noqa: E402

VERSION = "9.9.9"
RELEASE, LIBRARY = libduckdb.release_platform()
GENUINE = b"\x7fELF genuine libduckdb"


def release_archive(library_bytes: bytes) -> bytes:
    """A release archive as DuckDB 2.0 and later publish it: a tar.gz holding the library."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tf:
        info = tarfile.TarInfo(LIBRARY)
        info.size = len(library_bytes)
        tf.addfile(info, io.BytesIO(library_bytes))
    return buffer.getvalue()


ARCHIVE = release_archive(GENUINE)
EXPECTED = hashlib.sha256(ARCHIVE).hexdigest()


class InstallCacheTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cache = Path(self.tmp.name).resolve() / "cache"
        versions = Path(self.tmp.name) / "duckdb_versions.json"
        versions.write_text(json.dumps({"versions": [{"version": VERSION, "sha256": {RELEASE: EXPECTED}}]}))
        self.downloads = []
        self.served = ARCHIVE
        saved = (libduckdb.VERSIONS_FILE, libduckdb.download)
        self.addCleanup(lambda: (setattr(libduckdb, "VERSIONS_FILE", saved[0]), setattr(libduckdb, "download", saved[1])))
        libduckdb.VERSIONS_FILE = versions
        libduckdb.download = self.download

    def download(self, url, version):
        self.downloads.append(url)
        return self.served

    def install(self):
        return libduckdb.install(VERSION, self.cache)

    def assert_genuine(self, target):
        self.assertEqual((target / LIBRARY).read_bytes(), GENUINE)
        self.assertFalse((target / LIBRARY).is_symlink())
        self.assertEqual((target / libduckdb.MANIFEST).read_text(), libduckdb.manifest(target, LIBRARY))
        self.assertIn(f'shared = "{target / LIBRARY}"', (target / libduckdb.MANIFEST).read_text())

    def test_the_first_run_downloads_checks_and_installs(self):
        target = self.install()
        self.assertEqual(len(self.downloads), 1)
        self.assertEqual(target, self.cache / VERSION)
        self.assert_genuine(target)
        self.assertEqual((target / libduckdb.ARCHIVE).read_bytes(), ARCHIVE)

    def test_a_checked_install_is_reused_without_a_download(self):
        self.install()
        self.install()
        self.assertEqual(len(self.downloads), 1)

    def test_a_planted_install_with_a_matching_stamp_is_replaced(self):
        target = self.cache / VERSION
        target.mkdir(parents=True)
        (target / LIBRARY).write_bytes(b"planted library")
        (target / libduckdb.MANIFEST).write_text('[Driver]\nshared = "/tmp/planted.so"\nentrypoint = "x"\n')
        (target / ".sha256").write_text(EXPECTED + "\n")
        self.assertEqual(self.install(), target)
        self.assertEqual(len(self.downloads), 1)
        self.assert_genuine(target)

    def test_a_changed_library_beside_the_genuine_archive_is_replaced(self):
        target = self.install()
        (target / LIBRARY).write_bytes(b"planted library")
        self.install()
        self.assertEqual(len(self.downloads), 2)
        self.assert_genuine(target)

    def test_a_changed_manifest_is_replaced(self):
        target = self.install()
        (target / libduckdb.MANIFEST).write_text('[Driver]\nshared = "/tmp/planted.so"\nentrypoint = "x"\n')
        self.install()
        self.assertEqual(len(self.downloads), 2)
        self.assert_genuine(target)

    def test_a_kept_archive_that_does_not_match_is_replaced(self):
        target = self.install()
        (target / libduckdb.ARCHIVE).write_bytes(release_archive(b"planted library"))
        (target / LIBRARY).write_bytes(b"planted library")
        self.install()
        self.assertEqual(len(self.downloads), 2)
        self.assert_genuine(target)

    def test_a_library_that_is_a_link_is_replaced_even_with_the_right_bytes(self):
        target = self.install()
        elsewhere = Path(self.tmp.name) / "elsewhere.so"
        elsewhere.write_bytes(GENUINE)
        (target / LIBRARY).unlink()
        (target / LIBRARY).symlink_to(elsewhere)
        self.install()
        self.assertEqual(len(self.downloads), 2)
        self.assert_genuine(target)

    def test_a_download_that_does_not_match_fails_and_installs_nothing(self):
        self.served = release_archive(b"planted library")
        with self.assertRaises(libduckdb.Error):
            self.install()
        self.assertFalse((self.cache / VERSION).exists())


if __name__ == "__main__":
    unittest.main()
