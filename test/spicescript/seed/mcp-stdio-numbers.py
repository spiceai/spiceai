#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A stdio MCP server with one tool, `numbers`, for tests of how Spice bounds MCP tool results.

`numbers` returns `count` numbers in `structuredContent`, each written `1e15`. A JSON parser
reads each as a float, which is written back as `1000000000000000.0`, so the result Spice
decodes and passes on is several times the size of the message this server sends: a message
under the stdio transport's line limit can carry a result over the tool result limit.
"""

import json
import sys


def send(message):
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


TOOL = {
    "name": "numbers",
    "description": "Returns `count` numbers.",
    "inputSchema": {
        "type": "object",
        "properties": {"count": {"type": "integer"}},
        "required": ["count"],
    },
}

for line in sys.stdin:
    if not line.strip():
        continue
    request = json.loads(line)
    if "id" not in request:
        continue  # a notification
    request_id = request["id"]
    method = request.get("method")
    if method == "initialize":
        send({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "numbers", "version": "1.0.0"},
            },
        })
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"tools": [TOOL]}})
    elif method == "tools/call":
        count = int(request["params"]["arguments"]["count"])
        numbers = ",".join(["1e15"] * count)
        sys.stdout.write(
            '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"%d numbers"}],'
            '"structuredContent":{"numbers":[%s]}}}\n' % (json.dumps(request_id), count, numbers)
        )
        sys.stdout.flush()
    elif method == "ping":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    else:
        # `server/discover` among them: answering it with an error makes the client fall back
        # to `initialize`, as it does for any server that predates the 2026-07-28 revision.
        send({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"},
        })
