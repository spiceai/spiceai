#!/usr/bin/env python3
"""Tests for mssql_tpch_restore.sh, run against a stub `docker` that plays a SQL Server container."""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("mssql_tpch_restore.sh")

# `docker logs` prints $STUB_LOGS. Every sqlcmd query consumes the next line of the
# file named for its kind: a state query reads STUB_STATES ("DOWN" fails the
# connection, an empty line is a missing database), `SELECT 1` reads STUB_PINGS and
# RESTORE reads STUB_RESTORES (exit codes). A file that runs out repeats its last line.
STUB = """#!/usr/bin/env bash
set -euo pipefail
echo "$* SQLCMDPASSWORD=${SQLCMDPASSWORD:-}" >> "$STUB_CALLS"
next() {
  local file="$1" line
  line="$(head -n 1 "$file")"
  if [ "$(wc -l < "$file")" -gt 1 ]; then
    tail -n +2 "$file" > "$file.next"
    mv "$file.next" "$file"
  fi
  printf '%s' "$line"
}
case "$1" in
  logs) cat "$STUB_LOGS" ;;
  exec)
    query="${@: -1}"
    case "$query" in
      *state_desc*)
        state="$(next "$STUB_STATES")"
        if [ "$state" = DOWN ]; then echo "Sqlcmd: Error: Login timeout expired." >&2; exit 1; fi
        echo "$state" ;;
      'SELECT 1') exit "$(next "$STUB_PINGS")" ;;
      RESTORE*)
        code="$(next "$STUB_RESTORES")"
        if [ "$code" -ne 0 ]; then echo "Msg 3201, Level 16, State 2: Cannot open backup device." >&2; fi
        exit "$code" ;;
      *) echo "unexpected query: $query" >&2; exit 99 ;;
    esac ;;
  *) echo "unexpected docker command: $*" >&2; exit 99 ;;
esac
"""

PASSWORD = "S3cretP@ssw0rd"
STARTING = "2026-10-26 11:42:40.00 Server      Microsoft SQL Server 2022\nRestoring database tpch_sf1\n"
# What the image's setup.sh leaves in the log when its one RESTORE ran before SQL
# Server accepted connections.
LOGIN_TIMEOUT = STARTING + (
    "Sqlcmd: Error: Microsoft ODBC Driver 18 for SQL Server : Login timeout expired.\n"
    "Sqlcmd: Error: Microsoft ODBC Driver 18 for SQL Server : TCP Provider: Error code 0x2749.\n"
)
RESTORED = STARTING + "RESTORE DATABASE successfully processed 192882 pages in 8.673 seconds (173.744 MB/sec).\n"
# SQL Server's own error log reporting a failed login: not setup.sh's verdict.
SERVER_LOGIN_FAILED = STARTING + (
    "2026-10-26 11:44:33.58 Logon       Error: 18456, Severity: 14, State: 38.\n"
    "2026-10-26 11:44:33.58 Logon       Login failed for user 'sa'. Reason: Failed to open the explicitly specified database 'tpch_sf1'.\n"
)


class MssqlTpchRestoreTest(unittest.TestCase):
    def run_restore(self, logs, states, pings=("0",), restores=("0",), args=None, **env):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            stub = tmp_path / "docker"
            stub.write_text(STUB, newline="\n")
            stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
            files = {}
            for name, content in (
                ("logs", logs),
                ("states", "".join(f"{s}\n" for s in states)),
                ("pings", "".join(f"{p}\n" for p in pings)),
                ("restores", "".join(f"{r}\n" for r in restores)),
                ("calls", ""),
            ):
                files[name] = tmp_path / name
                files[name].write_text(content, newline="\n")
            proc_env = {
                **os.environ,
                "PATH": f"{tmp}{os.pathsep}{os.environ['PATH']}",
                "STUB_LOGS": str(files["logs"]),
                "STUB_STATES": str(files["states"]),
                "STUB_PINGS": str(files["pings"]),
                "STUB_RESTORES": str(files["restores"]),
                "STUB_CALLS": str(files["calls"]),
                "MSSQL_SA_PASSWORD": PASSWORD,
                "MSSQL_RESTORE_POLL_SECONDS": "0",
                **env,
            }
            proc = subprocess.run(
                ["bash", str(SCRIPT), *(args if args is not None else ["cid", "tpch_sf1"])],
                env=proc_env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            calls = files["calls"].read_text().splitlines()
        return proc, calls

    @staticmethod
    def split_call(call):
        """Splits a recorded stub call into its docker arguments and the SQLCMDPASSWORD it saw."""
        command, _, password = call.partition(" SQLCMDPASSWORD=")
        return command, password

    @staticmethod
    def restores(calls):
        return [c for c in calls if "RESTORE DATABASE" in c]

    def test_online_database_passes_without_a_restore(self):
        proc, calls = self.run_restore(RESTORED, ["ONLINE"])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("tpch_sf1 is ONLINE.", proc.stdout)
        self.assertEqual(self.restores(calls), [])

    def test_waits_for_the_image_restore_still_in_progress(self):
        proc, calls = self.run_restore(STARTING, ["DOWN", "", "RESTORING", "ONLINE"])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(self.restores(calls), [])
        self.assertNotIn("::warning::", proc.stdout)

    def test_restores_after_the_image_restore_hit_a_login_timeout(self):
        proc, calls = self.run_restore(LOGIN_TIMEOUT, ["", "ONLINE"])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("::warning::The image's restore of tpch_sf1 failed", proc.stdout)
        self.assertEqual(len(self.restores(calls)), 1)
        self.assertIn("FROM DISK = N'/data/tpch_sf1.bak' WITH REPLACE", self.restores(calls)[0])

    def test_waits_for_connections_before_restoring(self):
        proc, calls = self.run_restore(LOGIN_TIMEOUT, ["DOWN", "ONLINE"], pings=["1", "1", "0"])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        pings = [i for i, c in enumerate(calls) if self.split_call(c)[0].endswith("SELECT 1")]
        restore = next(i for i, c in enumerate(calls) if "RESTORE DATABASE" in c)
        self.assertEqual(len(pings), 3)
        self.assertGreater(restore, pings[-1])

    def test_server_login_errors_do_not_trigger_a_restore(self):
        proc, calls = self.run_restore(SERVER_LOGIN_FAILED, [""], MSSQL_RESTORE_TIMEOUT_SECONDS="1")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertEqual(self.restores(calls), [])
        self.assertIn("was not ONLINE within 1s (state: 'absent')", proc.stdout)

    def test_failed_restore_fails_with_the_container_log(self):
        proc, calls = self.run_restore(LOGIN_TIMEOUT, [""], restores=["1"])
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("::error::RESTORE DATABASE tpch_sf1 from /data/tpch_sf1.bak failed.", proc.stdout)
        self.assertIn("Login timeout expired", proc.stdout)

    def test_database_not_online_after_the_restore_fails(self):
        proc, _ = self.run_restore(LOGIN_TIMEOUT, ["", "RESTORING"])
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("::error::tpch_sf1 is 'RESTORING' after the restore, expected ONLINE.", proc.stdout)

    def test_gives_up_when_the_server_never_accepts_connections(self):
        proc, calls = self.run_restore(LOGIN_TIMEOUT, ["DOWN"], pings=["1"], MSSQL_RESTORE_TIMEOUT_SECONDS="1")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("::error::SQL Server did not accept connections within 1s.", proc.stdout)
        self.assertEqual(self.restores(calls), [])

    def test_every_query_is_time_bounded(self):
        proc, calls = self.run_restore(LOGIN_TIMEOUT, ["DOWN", "ONLINE"], MSSQL_RESTORE_TIMEOUT_SECONDS="120")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        execs = [self.split_call(c)[0].split() for c in calls if c.startswith("exec")]
        self.assertTrue(execs)
        for args in execs:
            self.assertIn("-t", args)
            self.assertGreaterEqual(int(args[args.index("-t") + 1]), 1)
        restore = next(a for a in execs if "RESTORE" in a)
        # The restore gets what is left of the deadline, not an unbounded wait.
        self.assertLessEqual(int(restore[restore.index("-t") + 1]), 120)

    def test_password_is_never_on_the_command_line(self):
        _, calls = self.run_restore(LOGIN_TIMEOUT, ["", "ONLINE"])
        self.assertTrue(calls)
        for call in calls:
            command, env_password = self.split_call(call)
            self.assertNotIn(PASSWORD, command)
            if command.startswith("exec"):
                self.assertEqual(env_password, PASSWORD)

    def test_rejects_unsafe_database_names(self):
        proc, calls = self.run_restore(RESTORED, ["ONLINE"], args=["cid", "tpch]; DROP DATABASE master; --"])
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(calls, [])

    def test_requires_the_password(self):
        proc, calls = self.run_restore(RESTORED, ["ONLINE"], MSSQL_SA_PASSWORD="")
        self.assertEqual(proc.returncode, 2)
        self.assertIn("MSSQL_SA_PASSWORD is not set", proc.stdout)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
