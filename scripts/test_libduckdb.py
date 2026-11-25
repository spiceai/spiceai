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
import subprocess
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

    def test_an_install_holding_another_file_is_replaced(self):
        target = self.install()
        (target / "sqlite.toml").write_text('[Driver]\nshared = "/tmp/planted.so"\n')
        self.install()
        self.assertEqual(len(self.downloads), 2)
        self.assertEqual(sorted(os.listdir(target)), sorted([libduckdb.ARCHIVE, LIBRARY, libduckdb.MANIFEST]))
        self.assert_genuine(target)

    def test_a_cache_path_with_a_line_break_is_refused(self):
        self.cache = Path(self.tmp.name).resolve() / "cache\nBASH_ENV=/tmp/evil.sh\nX="
        with self.assertRaises(libduckdb.Error):
            self.install()
        self.assertEqual(self.downloads, [])

    def test_a_lock_file_left_as_a_link_is_never_used(self):
        # The cache directory itself is the lock, so a link where a lock file once was is
        # neither followed nor removed, and no run races another to remove it.
        outside = Path(self.tmp.name) / "outside"
        outside.write_text("keep me")
        self.cache.mkdir(parents=True)
        (self.cache / f".{VERSION}.lock").symlink_to(outside)
        self.assert_genuine(self.install())
        self.assertEqual(outside.read_text(), "keep me")
        self.assertTrue((self.cache / f".{VERSION}.lock").is_symlink())

    def run_main(self, cache, extra_env=None):
        """libduckdb.main() in its own process, with the fabricated release and a download
        that counts itself and takes a moment, as concurrent runs would see it."""
        archive = Path(self.tmp.name) / "release.tar.gz"
        archive.write_bytes(ARCHIVE)
        helper = Path(self.tmp.name) / "run_main.py"
        helper.write_text(
            "import os, sys, time\n"
            "from pathlib import Path\n"
            "sys.path.insert(0, os.environ['SCRIPTS'])\n"
            "import libduckdb\n"
            "libduckdb.VERSIONS_FILE = Path(os.environ['VERSIONS'])\n"
            "def download(url, version):\n"
            "    with open(os.environ['COUNTER'], 'a') as f:\n"
            "        f.write('download\\n')\n"
            "    time.sleep(0.3)\n"
            "    return Path(os.environ['ARCHIVE']).read_bytes()\n"
            "libduckdb.download = download\n"
            "sys.argv = ['libduckdb.py', '--version', os.environ['VERSION'], '--cache-dir', os.environ['CACHE']]\n"
            "sys.exit(libduckdb.main())\n"
        )
        env = dict(os.environ, SCRIPTS=os.path.dirname(os.path.abspath(libduckdb.__file__)),
                   VERSIONS=str(libduckdb.VERSIONS_FILE), COUNTER=str(Path(self.tmp.name) / "downloads"),
                   ARCHIVE=str(archive), VERSION=VERSION, CACHE=str(cache), PYTHONDONTWRITEBYTECODE="1")
        return subprocess.Popen([sys.executable, "-I", str(helper)], env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)

    def test_concurrent_runs_install_once_and_all_succeed(self):
        self.cache.mkdir(parents=True)
        (self.cache / f".{VERSION}.lock").symlink_to(Path(self.tmp.name) / "nowhere")
        runs = [self.run_main(self.cache) for _ in range(4)]
        results = [run.communicate(timeout=60) + (run.returncode,) for run in runs]
        self.assertEqual([code for _, _, code in results], [0, 0, 0, 0], results)
        self.assertEqual({out.strip() for out, _, _ in results}, {str(self.cache / VERSION)})
        self.assertEqual((Path(self.tmp.name) / "downloads").read_text().count("download"), 1)
        self.assert_genuine(self.cache / VERSION)

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "root can read anything")
    def test_an_unreadable_file_inside_an_install_is_replaced(self):
        target = self.install()
        (target / LIBRARY).chmod(0)
        self.assert_genuine(self.install())
        self.assertEqual(len(self.downloads), 2)

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "root can write anything")
    def test_a_file_system_error_while_installing_is_one_error(self):
        self.cache.mkdir(parents=True)
        self.cache.chmod(0o500)
        self.addCleanup(self.cache.chmod, 0o755)
        with self.assertRaises(libduckdb.Error) as raised:
            self.install()
        self.assertIn(f"could not install DuckDB {VERSION}", str(raised.exception))
        run = self.run_main(self.cache)
        out, err, code = run.communicate(timeout=60) + (run.wait(),)
        self.assertEqual(code, 1, err)
        self.assertNotIn("Traceback", err)
        self.assertEqual(len(err.strip().splitlines()), 1, err)
        self.assertTrue(err.startswith("error: could not install DuckDB"), err)

    def test_a_cache_path_with_a_path_separator_is_refused(self):
        self.cache = Path(self.tmp.name).resolve() / f"evil{os.pathsep}cache"
        with self.assertRaises(libduckdb.Error):
            self.install()
        self.assertEqual(self.downloads, [])

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "root can read anything")
    def test_an_install_that_cannot_be_read_is_replaced(self):
        target = self.install()
        target.chmod(0)
        self.addCleanup(lambda: target.exists() and target.chmod(0o755))
        self.assert_genuine(self.install())
        self.assertEqual(len(self.downloads), 2)

    def test_a_download_that_does_not_match_fails_and_installs_nothing(self):
        self.served = release_archive(b"planted library")
        with self.assertRaises(libduckdb.Error):
            self.install()
        self.assertFalse((self.cache / VERSION).exists())


if __name__ == "__main__":
    unittest.main()
