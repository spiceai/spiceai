#!/usr/bin/env python3
"""Exercise runtime download retries against a local HTTP server."""

import argparse
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


def archive_bytes(filename="spiced", member_type=tarfile.REGTYPE):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        content = b"installer download fixture\n"
        entry = tarfile.TarInfo(filename)
        entry.type = member_type
        entry.mode = 0o755
        if member_type in (tarfile.SYMTYPE, tarfile.LNKTYPE):
            entry.linkname = "missing-runtime"
        entry.size = len(content) if member_type == tarfile.REGTYPE else 0
        archive.addfile(entry, io.BytesIO(content) if entry.size else None)
    return output.getvalue()


def run_case(preamble, client, responses, expected_requests, expected_success):
    class Handler(BaseHTTPRequestHandler):
        requests = 0

        def do_GET(self):
            status, body = responses[min(type(self).requests, len(responses) - 1)]
            type(self).requests += 1
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "download.tar.gz"
            script = preamble + '''
SPICE_HTTP_REQUEST_CLI="$1"
MAX_RETRIES=3
RETRY_DELAY=0
downloadWithRetry "$2" "$3"
'''
            result = subprocess.run(
                [
                    "bash", "-c", script, "installer-download-test", client,
                    f"http://127.0.0.1:{server.server_port}/runtime.tar.gz", str(output),
                ],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            success = result.returncode == 0
            assert success == expected_success, result.stdout + result.stderr
            assert Handler.requests == expected_requests, (
                f"Expected {expected_requests} requests, got {Handler.requests} "
                f"(installer exit={result.returncode})"
            )
            if success:
                with tarfile.open(output, "r:gz") as archive:
                    assert [name.removeprefix("./") for name in archive.getnames()] == ["spiced"], "Unexpected archive contents"
                    assert all(member.isreg() for member in archive.getmembers()), "Runtime is not a regular file"
            else:
                assert not output.exists(), "Failed download was not removed"
            return {"requests": Handler.requests, "exit_code": result.returncode}
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", choices=["curl", "wget"], required=True)
    parser.add_argument(
        "--installer",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "install/install-spiced.sh",
    )
    args = parser.parse_args()
    preamble, marker, _main = args.installer.read_text().partition("\n# main\n")
    assert marker, "Installer main marker is missing"
    valid = archive_bytes()
    cases = {
        "valid_archive": ([(200, valid)], 1, True),
        "dot_prefixed_archive": ([(200, archive_bytes("./spiced"))], 1, True),
        "missing_runtime_then_archive": (
            [(200, archive_bytes("README")), (200, archive_bytes("README")), (200, valid)], 3, True,
        ),
        "missing_runtime_exhausts_retries": ([(200, archive_bytes("README"))], 3, False),
        "http_error_then_archive": (
            [(503, b"unavailable"), (503, b"unavailable"), (200, valid)], 3, True,
        ),
        "invalid_archive_then_archive": (
            [(200, b"invalid"), (200, b"invalid"), (200, valid)], 3, True,
        ),
        "truncated_archive_then_archive": (
            [(200, valid[:20]), (200, valid[:20]), (200, valid)], 3, True,
        ),
        "invalid_archive_exhausts_retries": ([(200, b"invalid")], 3, False),
    }
    for kind, member_type in {
        "symlink": tarfile.SYMTYPE,
        "hardlink": tarfile.LNKTYPE,
        "directory": tarfile.DIRTYPE,
    }.items():
        invalid = archive_bytes(member_type=member_type)
        cases[f"{kind}_then_archive"] = (
            [(200, invalid), (200, invalid), (200, valid)], 3, True,
        )
        cases[f"{kind}_exhausts_retries"] = ([(200, invalid)], 3, False)
    for name, (responses, requests, success) in cases.items():
        result = run_case(preamble, args.client, responses, requests, success)
        print(json.dumps({"client": args.client, "case": name, **result}), flush=True)


if __name__ == "__main__":
    main()
