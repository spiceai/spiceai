"""An MCP server, over stdio, that stands in for another Spice: it has one tool,
`sql`, which refuses every statement and quotes it back in its error, as a
database error can quote what it was sent. An argument names the tool
otherwise, as a Spicepod can rename its `sql` tool. A second argument makes
the text a JSON document, which escapes a quote, a backslash, or a letter
outside ASCII once more: `json` refuses the statement with it, as a peer that
returns structured errors does, and `json-result` answers with it.
`json-after-quote` refuses it with the document after plain text that quotes
a quote, as a parser error quotes the token it stopped at, and writes the
document's escapes in capitals, as .NET's System.Text.Json does.

A script starts it as a tool: `from: mcp:python3`, with `mcp_args` set to
this file's path, and the tool's name after it when it is not `sql`. It
needs nothing beyond the Python standard library.
"""

import json
import re
import sys

MODE = sys.argv[2] if len(sys.argv) > 2 else ""

SQL_TOOL = {
    "name": sys.argv[1] if len(sys.argv) > 1 else "sql",
    "description": "Run a SQL statement.",
    "inputSchema": {
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"],
    },
}


def result(request):
    method = request.get("method")
    params = request.get("params") or {}
    if method == "initialize":
        return {
            "protocolVersion": params.get("protocolVersion"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "sql-peer", "version": "1"},
        }
    if method == "tools/list":
        return {"tools": [SQL_TOOL]}
    if method == "tools/call":
        query = (params.get("arguments") or {}).get("query", "")
        text = f"refused: {query}"
        if MODE == "json":
            text = json.dumps({"error": text})
        elif MODE == "json-after-quote":
            document = re.sub(r"\\u([0-9a-f]{4})", lambda m: "\\u" + m.group(1).upper(), json.dumps({"error": text}))
            text = "Unexpected token '\"'; details: " + document
        elif MODE == "json-result":
            return {"content": [{"type": "text", "text": json.dumps({"echo": query})}], "isError": False}
        return {"content": [{"type": "text", "text": text}], "isError": True}
    if method == "ping":
        return {}
    # `server/discover` among them: the client then falls back to `initialize`.
    return None


for line in sys.stdin:
    if not line.strip():
        continue
    request = json.loads(line)
    if "id" not in request:
        continue
    answer = result(request)
    if answer is None:
        response = {
            "jsonrpc": "2.0",
            "id": request["id"],
            "error": {"code": -32601, "message": f"Method not found: {request.get('method')}"},
        }
    else:
        response = {"jsonrpc": "2.0", "id": request["id"], "result": answer}
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()
