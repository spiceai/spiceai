# Copyright 2024-2026 The Spice.ai OSS Authors
# SPDX-License-Identifier: Apache-2.0
"""Independent committed-state oracle using real Redis MULTI/EXEC and WATCH.

A test-owned hash represents one WAL domain. Redis supplies atomic batches and
whole-hash conflict detection; it does not supply historical MVCC snapshots.
Only this uniquely named hash is deleted, including when a comparison fails.
"""

from __future__ import annotations

import importlib.metadata
import json
from uuid import uuid4


class RedisOracle:
    def __init__(self, url, artifacts, name):
        from redis import Redis

        version = importlib.metadata.version("redis")
        assert version == "6.4.0", f"expected redis-py 6.4.0, found {version}"
        self.client = Redis.from_url(
            url, decode_responses=False, socket_timeout=10, socket_connect_timeout=10,
        )
        self.key = f"spice-wal-oracle:{uuid4().hex}"
        self.log = (artifacts / f"{name}.jsonl").open("w", encoding="utf-8")

    def record(self, **entry):
        self.log.write(json.dumps(entry) + "\n")
        self.log.flush()

    def __enter__(self):
        try:
            assert self.client.ping()
            version = self.client.info("server")["redis_version"]
            assert not self.client.exists(self.key), "oracle namespace already exists"
            self.record(server_version=version, client_version="6.4.0", domain=self.key)
            print(f"independent oracle: Redis {version} (redis-py 6.4.0)", flush=True)
            return self
        except BaseException:
            self.client.close()
            self.log.close()
            raise

    def __exit__(self, *_):
        try:
            self.client.delete(self.key)
        finally:
            self.client.close()
            self.log.close()

    def scan(self, prefix=""):
        # HGETALL is a single atomic read, unlike a cursor scan during mutation.
        values = self.client.hgetall(self.key)
        result = [
            [key.decode("utf-8"), list(value)]
            for key, value in sorted(values.items()) if key.startswith(prefix.encode())
        ]
        self.record(operation="HGETALL", prefix=prefix, rows=result)
        return result

    def get(self, key):
        value = self.client.hget(self.key, key.encode())
        result = list(value) if value is not None else None
        self.record(operation="HGET", key=key, value=result)
        return result

    def queue(self, pipe, mutations):
        for key, value in mutations:
            if value is None:
                pipe.hdel(self.key, key.encode())
            else:
                pipe.hset(self.key, key.encode(), bytes(value))

    def commit(self, mutations):
        self.record(operation="MULTI/EXEC", mutations=mutations)
        with self.client.pipeline(transaction=True) as pipe:
            self.queue(pipe, mutations)
            result = pipe.execute(raise_on_error=True)
        assert len(result) == len(mutations), result
        self.record(operation="EXEC", replies=result)

    def reconnect(self):
        self.client.connection_pool.disconnect()
        assert self.client.ping()
        self.record(operation="reconnect")

    def conflict(self, winner, loser):
        """Force one native WATCH rejection with a separate writer connection."""
        from redis.exceptions import WatchError

        with self.client.pipeline(transaction=True) as stale:
            stale.watch(self.key)
            # WATCH reserves the pipeline's connection; commit uses another one.
            self.commit(winner)
            stale.multi()
            self.queue(stale, loser)
            self.record(operation="watched MULTI/EXEC", mutations=loser)
            try:
                stale.execute(raise_on_error=True)
            except WatchError:
                self.record(operation="EXEC", outcome="conflict")
                return "conflict"
        raise AssertionError("Redis accepted a stale watched transaction")
