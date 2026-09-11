"""Drive a node restart against a running cluster and watch the watermark.

Needs three reachable spiced control endpoints in SPICE_CLUSTER_NODES. The
sequence under test is: let all three nodes advance, restart one, and see
whether the cluster watermark the coordinator publishes ever moves backwards.
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from _harness import record  # noqa: E402

from cluster_sync import Coordinator, connect  # noqa: E402

NODES = ("node-a", "node-b", "node-c")


def main() -> int:
    coordinator = Coordinator()
    channels = {}
    try:
        for node_id in NODES:
            channels[node_id] = connect(node_id)
            coordinator.report(node_id, channels[node_id].watermark())

        before = coordinator.cluster_watermark()
        print(f"cluster watermark before restart: {before}")

        # Restart one node and let it re-report from the watermark it last
        # persisted locally, which is the sequence under test.
        victim = NODES[0]
        channels[victim].restart()
        coordinator.report(victim, channels[victim].watermark())

        after = coordinator.cluster_watermark()
        print(f"cluster watermark after restart:  {after}")

        moved_backwards = before is not None and after is not None and after < before
        print(f"moved backwards: {moved_backwards}")
    except (RuntimeError, OSError) as err:
        record("cluster-watermark", fn="run_repro", outcome="unavailable", error=str(err))
        print(f"cannot run here: {err}", file=sys.stderr)
        return 2
    finally:
        for channel in channels.values():
            channel.close()

    record("cluster-watermark", fn="run_repro", outcome="ran",
           before=before, after=after, moved_backwards=moved_backwards)
    return 1 if moved_backwards else 0


if __name__ == "__main__":
    sys.exit(main())
