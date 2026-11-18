#!/usr/bin/env python3
# Copyright 2026 The Spice.ai Authors
# SPDX-License-Identifier: Apache-2.0
"""A local stand-in for the GitHub API and release downloads, for the install script tests.

Usage: install-fake-github.py <state-dir>

Writes the port it listens on to <state-dir>/port. A request for /<case>/<path> is answered from
<state-dir>/<case>/:
  status/<path>  when it exists: its first line is "<code> [times]", the status to answer with,
                 for the first <times> requests of the path only when given; each further line
                 is a response header ("name: value"). The body is a GitHub-style error.
  files/<path>   otherwise, answered with 200 and the file's content.
Anything else gets a GitHub-style 404. Every request is appended to <state-dir>/<case>/requests
as "<path> <auth>", where <auth> is the scheme of its Authorization header, or "none".
"""

import http.server
import json
import os
import sys
import threading

state = os.path.realpath(sys.argv[1])
seen = {}
lock = threading.Lock()


def resolve(case, rel):
    path = os.path.realpath(os.path.join(state, case, rel))
    if not path.startswith(os.path.join(state, case) + os.sep):
        return None
    return path


class Handler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        case, _, path = self.path.lstrip("/").partition("/")
        path = path.split("?", 1)[0]
        auth = self.headers.get("Authorization")
        auth = auth.split(" ", 1)[0] if auth else "none"
        with lock:
            seen[self.path] = seen.get(self.path, 0) + 1
            hits = seen[self.path]
            if case and resolve(case, "requests"):
                with open(resolve(case, "requests"), "a") as log:
                    log.write(f"{path} {auth}\n")

        status_file = resolve(case, "status/" + path) if case and path else None
        if status_file and os.path.isfile(status_file):
            with open(status_file) as spec:
                first, *header_lines = spec.read().splitlines()
            code, _, times = first.partition(" ")
            if not times or hits <= int(times):
                headers = [line.split(":", 1) for line in header_lines if ":" in line]
                message = "API rate limit exceeded for 127.0.0.1." if code in ("403", "429") else f"HTTP {code}"
                self.reply(int(code), {"message": message}, headers)
                return

        file = resolve(case, "files/" + path) if case and path else None
        if file and os.path.isfile(file):
            with open(file, "rb") as content:
                self.reply(200, content.read())
            return
        self.reply(404, {"message": "Not Found"})

    def reply(self, code, body, headers=()):
        if not isinstance(body, bytes):
            body = json.dumps(body).encode()
        self.send_response(code)
        for name, value in headers:
            self.send_header(name.strip(), value.strip())
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


# A test run that loses track of the server must not leave it running for good.
threading.Timer(1800, os._exit, [0]).start()
server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(os.path.join(state, "port.tmp"), "w") as port:
    port.write(str(server.server_address[1]))
os.replace(os.path.join(state, "port.tmp"), os.path.join(state, "port"))
server.serve_forever()
