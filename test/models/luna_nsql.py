#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# SPDX-License-Identifier: Apache-2.0
"""Exercise Luna loading, chat, NSQL execution and native decisions in a real spiced.

Run: python3 test/models/luna_nsql.py --spiced /path/to/spiced
The upstream HTTP fixture is local and requires no API credentials.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


class Upstream(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, body, status=200):
        encoded = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        if self.path == f"/v1/models/{self.server.model}":
            self.reply({"id": self.server.model, "object": "model"})
        else:
            self.reply({"error": "Unexpected upstream GET"}, 404)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append((self.path, body))
        if body.get("model") != self.server.model:
            self.reply({"error": "Incorrect upstream model ID"}, 400)
        elif self.path == "/v1/chat/completions":
            content = "SELECT 42 AS answer"
            if (body.get("response_format") or {}).get("type") == "json_object":
                content = json.dumps({"sql": content})
            self.reply({
                "id": "chatcmpl-luna-test", "object": "chat.completion",
                "created": 0, "model": self.server.model,
                "choices": [{"index": 0, "finish_reason": "stop", "message": {
                    "role": "assistant", "content": content,
                }}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            })
        elif self.path == "/v1/decisions":
            self.reply({"model": self.server.model, "answers": [{
                "type": "predicate", "name": "correct", "probability": 0.875,
            }]})
        else:
            self.reply({"error": "Unexpected upstream POST"}, 404)


def check_model(binary, directory, model):
    directory.mkdir()
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    upstream.model = model
    upstream.requests = []
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    http_port, flight_port = free_port(), free_port()
    while flight_port == http_port:
        flight_port = free_port()
    base = f"http://127.0.0.1:{http_port}"
    pod = directory / "spicepod.yaml"
    pod.write_text(f"""version: v2
kind: Spicepod
name: luna-nsql-regression
models:
  - from: openai:{model}
    name: nsql
    params:
      endpoint: http://127.0.0.1:{upstream.server_port}/v1
      openai_api_key: test-not-a-real-key
      responses_api: disabled
""")
    try:
        with (directory / "spiced.log").open("w") as log:
            process = subprocess.Popen(
                [binary, str(pod), "--http", f"127.0.0.1:{http_port}",
                 "--flight", f"127.0.0.1:{flight_port}"],
                cwd=directory, stdout=log, stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 60
                last_status = "No response"
                while time.monotonic() < deadline:
                    assert process.poll() is None, "spiced exited during startup"
                    try:
                        status, last_status = request(base + "/v1/models?status=true")
                        if status == 200:
                            models = json.loads(last_status)["data"]
                            if models and models[0].get("status") == "Ready":
                                break
                            assert not any(m.get("status") == "Error" for m in models), last_status
                    except urllib.error.URLError:
                        pass
                    time.sleep(0.1)
                else:
                    raise AssertionError(f"Model did not become ready: {last_status}")

                # Execute the generated SQL and check the exact rows.
                status, direct = request(base + "/v1/nsql", {
                    "model": "nsql", "query": "Return 42 as answer",
                })
                print(f"{model}: explicit NSQL -> HTTP {status}: {direct}", flush=True)
                assert (status, json.loads(direct) if status == 200 else direct) == (
                    200, [{"answer": 42}],
                ), direct
                status, inferred = request(base + "/v1/nsql", {"query": "Return 42 as answer"})
                assert (status, json.loads(inferred)) == (200, [{"answer": 42}]), inferred

                status, chat = request(base + "/v1/chat/completions", {
                    "model": "nsql", "messages": [{"role": "user", "content": "Return SQL"}],
                })
                assert status == 200, chat
                assert json.loads(chat)["choices"][0]["message"]["content"] == "SELECT 42 AS answer"

                status, decisions = request(base + "/v1/decisions", {
                    "model": "nsql", "input": "42 is the answer",
                    "questions": [{"type": "predicate", "name": "correct", "instructions": "Is the answer 42?"}],
                })
                assert (status, json.loads(decisions)) == (200, {
                    "model": "nsql", "answers": [{
                        "type": "predicate", "name": "correct", "probability": 0.875,
                    }],
                }), decisions
                assert upstream.requests[-1][0] == "/v1/decisions", upstream.requests
                assert any(path == "/v1/chat/completions" for path, _ in upstream.requests)
                print(f"{model}: inferred NSQL, chat and native decisions OK", flush=True)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    except Exception:
        print((directory / "spiced.log").read_text()[-8000:], flush=True)
        raise
    finally:
        (directory / "upstream.json").write_text(json.dumps(upstream.requests, indent=2))
        upstream.shutdown()
        upstream.server_close()
        thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", default="spiced")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    binary = shutil.which(args.spiced)
    if binary is None:
        parser.error(f"spiced not found: {args.spiced}")
    binary = str(Path(binary).resolve())
    directory = (args.output_dir or Path(tempfile.mkdtemp(prefix="luna-nsql-"))).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    print(f"Artifacts: {directory}", flush=True)
    version = subprocess.check_output([binary, "--version"], text=True).strip()
    print(version, flush=True)
    (directory / "run.json").write_text(json.dumps({
        "date": datetime.now(timezone.utc).isoformat(),
        "binary": binary, "version": version,
    }, indent=2))
    for model in ["gpt-6-luna", "gpt-6-luna-2026-10-01"]:
        check_model(binary, directory / model, model)


if __name__ == "__main__":
    main()
