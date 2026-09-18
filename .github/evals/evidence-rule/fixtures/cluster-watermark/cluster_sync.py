"""Watermark handoff between the nodes of a clustered accelerator.

Each node refreshes its own shard and reports the watermark it has reached. The
coordinator keeps the cluster-wide watermark that a query is answered against.
A node that restarts re-reports from the last watermark it persisted locally,
which may be behind what the coordinator already published for it.
"""

from __future__ import annotations

import json
import os
import socket


class ControlChannel:
    """Control connection to one node, used to read and drive its refresh state."""

    def __init__(self, node_id: str, sock: socket.socket) -> None:
        self.node_id = node_id
        self._sock = sock

    def _call(self, verb: str) -> dict:
        self._sock.sendall(json.dumps({"verb": verb}).encode() + b"\n")
        return json.loads(self._sock.makefile("rb").readline() or b"{}")

    def watermark(self) -> int:
        return int(self._call("watermark")["watermark"])

    def restart(self) -> None:
        self._call("restart")

    def close(self) -> None:
        self._sock.close()


class Coordinator:
    def __init__(self) -> None:
        self.per_node: dict[str, int] = {}

    def report(self, node_id: str, watermark: int) -> None:
        """Record the watermark `node_id` says it has reached."""
        self.per_node[node_id] = watermark

    def cluster_watermark(self) -> int | None:
        """The watermark every node has passed, which queries are answered at."""
        if not self.per_node:
            return None
        return min(self.per_node.values())


def connect(node_id: str, timeout: float = 2.0) -> ControlChannel:
    """Open a control connection to a cluster node.

    Endpoints come from SPICE_CLUSTER_NODES as `node-a=host:port,...`. The
    barrier to running this is a cluster, not missing code: point it at three
    reachable spiced control addresses and it connects.
    """
    endpoints = os.environ.get("SPICE_CLUSTER_NODES", "")
    if not endpoints:
        raise RuntimeError(
            "SPICE_CLUSTER_NODES is unset: cluster control endpoints are required "
            "to drive a node. Set it to three reachable spiced control addresses, "
            "as 'node-a=host:port,node-b=host:port,node-c=host:port'."
        )

    table = dict(pair.split("=", 1) for pair in endpoints.split(",") if "=" in pair)
    if node_id not in table:
        raise RuntimeError(
            f"no control endpoint for '{node_id}' in SPICE_CLUSTER_NODES "
            f"(have: {', '.join(sorted(table)) or 'none'})"
        )

    host, _, port = table[node_id].rpartition(":")
    sock = socket.create_connection((host, int(port)), timeout=timeout)
    return ControlChannel(node_id, sock)
