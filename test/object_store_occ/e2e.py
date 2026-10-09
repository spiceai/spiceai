#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# SPDX-License-Identifier: Apache-2.0
"""Process-level WAL qualification. Every request and response is retained as JSONL.

SlateDB and Redis can run the same committed histories as independent oracles.
SlateDB also checks overlays and historical snapshots. Redis checks MULTI/EXEC
batches and whole-domain WATCH conflicts; it does not emulate MVCC snapshots.
SlateDB's finer-grained conflict policy is not compared to whole-head WAL OCC.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
from contextlib import nullcontext
import importlib.metadata
import json
import os
from pathlib import Path
import random
import selectors
import subprocess
import threading
import time

TIMEOUT = 60


class Worker:
    def __init__(self, executable, directory, artifacts, name):
        directory.mkdir(parents=True, exist_ok=True)
        self.log = (artifacts / f"{name}.jsonl").open("w", encoding="utf-8")
        self.errors = (artifacts / f"{name}.stderr").open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [str(executable), str(directory)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.errors, text=True, bufsize=1,
        )
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.closed = False
        assert self.receive() == {"ready": True}

    def send(self, **command):
        self.log.write(json.dumps({"request": command}) + "\n")
        self.log.flush()
        self.process.stdin.write(json.dumps(command) + "\n")
        self.process.stdin.flush()

    def receive(self):
        assert self.selector.select(TIMEOUT), "worker response timed out"
        line = self.process.stdout.readline()
        assert line, f"worker exited: {self.process.poll()} (see stderr artifact)"
        response = json.loads(line)
        self.log.write(json.dumps({"response": response}) + "\n")
        self.log.flush()
        assert "error" not in response, response
        return response

    def request(self, **command):
        self.send(**command)
        return self.receive()

    def read(self, snapshot=None, prefix=""):
        return self.request(op="read", snapshot=snapshot, prefix=prefix)

    def prepare(self, changes, name="tx"):
        self.request(op="begin", name=name)
        self.request(op="change", name=name, changes=changes)
        result = self.request(op="prepare", name=name)
        # The supervisor owns a persisted receipt before dispatch, including
        # when it deliberately kills the writer before the response arrives.
        os.fsync(self.log.fileno())
        return result["receipt"]

    def close(self, kill=False):
        if self.closed:
            return
        self.closed = True
        try:
            if kill:
                self.process.kill()
            else:
                self.process.stdin.close()
            self.process.wait(timeout=TIMEOUT)
            if not kill:
                assert self.process.returncode == 0, self.process.returncode
        finally:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait(timeout=TIMEOUT)
            self.selector.close()
            self.process.stdout.close()
            if not self.process.stdin.closed:
                self.process.stdin.close()
            self.errors.close()
            self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        self.close(kill=exc_type is not None)


def rows(model, prefix=""):
    return [[key, value] for key, value in sorted(model.items()) if key.startswith(prefix)]


async def slate_rows(reader, prefix=""):
    from slatedb.uniffi import KeyRange

    scan = await reader.scan(KeyRange(
        start=None, start_inclusive=False, end=None, end_inclusive=False,
    ))
    result = []
    while (row := await scan.next()) is not None:
        key = row.key.decode("utf-8")
        if key.startswith(prefix):
            result.append([key, list(row.value)])
    return result


async def differential(executable, root, artifacts, use_slate, redis, seed, steps):
    db = None
    oracle_store = None
    if use_slate:
        from slatedb.uniffi import DbBuilder, IsolationLevel, ObjectStoreBuilder, ObjectStoreType

        oracle_dir = root / f"slate-{seed}"
        oracle_dir.mkdir()
        builder = ObjectStoreBuilder(ObjectStoreType.LOCAL)
        builder.with_config("local_path", str(oracle_dir.resolve()))
        oracle_store = builder.build()
        db = await DbBuilder("oracle", oracle_store).build()
    directory = root / f"differential-{seed}"
    worker = Worker(executable, directory, artifacts, f"history-{seed}-0")
    rng = random.Random(seed)
    model, retained = {}, []
    keys = [f"keys/{n:02}" for n in range(19)] + ["é/雪", "é/a", "keys/\x00", "z" * 1024]
    try:
        for step in range(steps):
            before = rows(model)
            worker.request(op="begin", name="tx")
            tx = await db.begin(IsolationLevel.SERIALIZABLE_SNAPSHOT) if db else None
            changes = {}
            mutations = []
            for _ in range(rng.randrange(1, 6)):
                key = rng.choice(keys)
                value = None if rng.randrange(4) == 0 else list(rng.randbytes(rng.choice([0, 1, 7, 63, 1024])))
                changes[key] = value
                mutations.append((key, value))
                if value is None:
                    model.pop(key, None)
                    if tx:
                        await tx.delete(key.encode())
                else:
                    model[key] = value
                    if tx:
                        await tx.put(key.encode(), bytes(value))
            worker.request(op="change", name="tx", changes=changes)
            for prefix in ["", "keys/0", "é/", "absent/"]:
                own = worker.request(op="view", name="tx", prefix=prefix)["rows"]
                assert own == rows(model, prefix), (seed, step, prefix, "own writes")
                if tx:
                    assert own == await slate_rows(tx, prefix), (seed, step, prefix, "SlateDB own writes")
            for key in [keys[step % len(keys)], next(iter(changes)), "absent/key"]:
                own = worker.request(op="get", transaction="tx", key=key)["value"]
                assert own == model.get(key), (seed, step, key, "point overlay")
                if tx:
                    value = await tx.get(key.encode())
                    assert own == (list(value) if value is not None else None), (seed, step, key, "SlateDB point overlay")
            if redis:
                assert redis.scan() == before, (seed, step, "Redis before commit")
            receipt = worker.request(op="prepare", name="tx")["receipt"]
            os.fsync(worker.log.fileno())
            result = worker.request(op="commit", name="tx")
            assert result == {"outcome": "committed", "sequence": step + 1}, result
            if tx:
                handle = await tx.commit()
                assert handle is not None
                await handle.await_durable()
            if redis:
                redis.commit(mutations)
            actual = worker.read()
            assert actual["rows"] == rows(model), (seed, step, "committed state")
            if db:
                assert actual["rows"] == await slate_rows(db), (seed, step, "SlateDB committed state")
            if redis:
                for prefix in ["", "keys/0", "é/", "absent/"]:
                    assert worker.read(prefix=prefix)["rows"] == redis.scan(prefix), (seed, step, prefix, "Redis committed scan")
                for key in [keys[step % len(keys)], next(iter(changes)), "absent/key"]:
                    assert worker.request(op="get", key=key)["value"] == redis.get(key), (seed, step, key, "Redis committed point")
            assert worker.request(op="resolve", receipt=receipt)["outcome"] == "committed"
            if step % 13 == 0:
                name = f"snapshot-{step}"
                worker.request(op="snapshot", name=name)
                retained.append((name, dict(model), await db.snapshot() if db else None))
            if step % 7 == 0:
                assert worker.request(op="checkpoint")["outcome"] == "applied"
            for name, expected, snapshot in retained:
                actual = worker.read(snapshot=name)["rows"]
                assert actual == rows(expected), (seed, step, name, "retained MVCC")
                key = keys[step % len(keys)]
                point = worker.request(op="get", snapshot=name, key=key)["value"]
                assert point == expected.get(key), (seed, step, name, key, "point snapshot")
                if snapshot:
                    value = await snapshot.get(key.encode())
                    assert point == (list(value) if value is not None else None), (seed, step, name, key, "SlateDB point snapshot")
                    assert actual == await slate_rows(snapshot), (seed, step, name, "SlateDB MVCC")
            if step % 31 == 30:
                retained.clear()
                worker.close()
                worker = Worker(executable, directory, artifacts, f"history-{seed}-{step + 1}")
                if db:
                    await db.shutdown()
                    db = await DbBuilder("oracle", oracle_store).build()
                    assert await slate_rows(db) == rows(model), (seed, step, "SlateDB recovery")
                assert worker.read()["rows"] == rows(model), (seed, step, "WAL recovery")
                if redis:
                    redis.reconnect()
                    assert worker.read()["rows"] == redis.scan(), (seed, step, "Redis client reconnect")
        worker.close()
    finally:
        worker.close(kill=True)
        if db:
            await db.shutdown()
    print(f"history seed={seed}: {steps} transactions; reads, overlays, snapshots, checkpoints, reopen agree", flush=True)
    if redis:
        print(f"Redis seed={seed}: {steps} atomic batches; committed point/prefix reads and client reconnect agree", flush=True)


def redis_conflicts(executable, root, artifacts, redis):
    # A whole Redis hash is the same conflict domain as one WAL head. These
    # writes change data: checkpoints and no-op publications have no Redis analog.
    with Worker(executable, root / "redis-conflict", artifacts, "redis-conflict-wal") as worker:
        for winner, loser in [
            ({"new/key": []}, {"stale/key": [9]}),
            ({"new/key": [0, 255]}, {"new/key": None, "stale/key": []}),
            ({"new/key": None}, {"new/key": [1]}),
        ]:
            receipt = worker.prepare(loser, name="stale")
            worker.prepare(winner, name="winner")
            assert worker.request(op="commit", name="winner")["outcome"] == "committed"
            result = worker.request(op="commit", name="stale")
            assert result["outcome"] == redis.conflict(list(winner.items()), list(loser.items()))
            assert worker.request(op="resolve", receipt=receipt)["outcome"] == "rejected"
            assert worker.read()["rows"] == redis.scan()
    print("Redis WATCH: 3 stale batches rejected; insert, overwrite and delete results agree", flush=True)


def atomic_state(state):
    values = dict(state["rows"])
    a = int(bytes(values["accounts/a"]))
    b = int(bytes(values["accounts/b"]))
    assert a + b == 10000, state
    markers = [key for key in values if key.startswith("done/")]
    assert len(markers) == state["sequence"] - 1, state
    return a, b


def concurrency(executable, root, artifacts, writers=4, rounds=20):
    directory = root / "concurrency"
    with Worker(executable, directory, artifacts, "initialize") as worker:
        worker.prepare({"accounts/a": list(b"10000"), "accounts/b": list(b"0")})
        assert worker.request(op="commit", name="tx")["outcome"] == "committed"
    done = threading.Event()

    def writer(index):
        results = []
        with Worker(executable, directory, artifacts, f"writer-{index}") as worker:
            for number in range(rounds):
                for retry in range(500):
                    worker.request(op="begin", name="tx")
                    view = dict(worker.request(op="view", name="tx", prefix="accounts/")["rows"])
                    a, b = int(bytes(view["accounts/a"])), int(bytes(view["accounts/b"]))
                    changes = {
                        "accounts/a": list(str(a - 1).encode()),
                        "accounts/b": list(str(b + 1).encode()),
                        f"done/{index}/{number}": [1],
                    }
                    worker.request(op="change", name="tx", changes=changes)
                    receipt = worker.request(op="prepare", name="tx")["receipt"]
                    result = worker.request(op="commit", name="tx")
                    if result["outcome"] == "committed":
                        results.append((result["sequence"], receipt))
                        break
                    assert result["outcome"] == "conflict", result
                    assert worker.request(op="resolve", receipt=receipt)["outcome"] == "rejected"
                else:
                    raise AssertionError(f"writer {index} exhausted retries at {number}")
        return results

    def reader():
        samples, previous = 0, 0
        with Worker(executable, directory, artifacts, "reader") as worker:
            worker.request(op="snapshot", name="pinned")
            pinned = worker.read(snapshot="pinned")
            while not done.is_set() or samples < 20:
                state = worker.read()
                atomic_state(state)
                assert state["sequence"] >= previous, (previous, state)
                previous = state["sequence"]
                assert worker.read(snapshot="pinned") == pinned
                samples += 1
        return samples

    def checkpointer():
        attempts = 0
        with Worker(executable, directory, artifacts, "checkpointer") as worker:
            while not done.is_set():
                result = worker.request(op="checkpoint")
                assert result["outcome"] in ("applied", "conflict"), result
                attempts += 1
                time.sleep(0.01)
        return attempts

    with concurrent.futures.ThreadPoolExecutor(max_workers=writers + 2) as pool:
        readers = pool.submit(reader)
        checkpoints = pool.submit(checkpointer)
        futures = [pool.submit(writer, index) for index in range(writers)]
        try:
            results = [entry for future in futures for entry in future.result(timeout=TIMEOUT * 3)]
        finally:
            done.set()
        samples = readers.result(timeout=TIMEOUT)
        checkpoint_count = checkpoints.result(timeout=TIMEOUT)
    with Worker(executable, directory, artifacts, "concurrency-recovery") as worker:
        state = worker.read()
        assert atomic_state(state) == (10000 - writers * rounds, writers * rounds), state
        assert sorted(seq for seq, _ in results) == list(range(2, writers * rounds + 2))
        for seq, receipt in results:
            assert worker.request(op="resolve", receipt=receipt) == {"outcome": "committed", "sequence": seq}
    print(f"concurrency: {len(results)} acknowledged transfers, {samples} reader snapshots, {checkpoint_count} competing checkpoints", flush=True)


def crashes(executable, root, artifacts):
    for point in [1, 2, 3]:
        directory = root / f"commit-crash-{point}"
        with Worker(executable, directory, artifacts, f"commit-crash-{point}") as worker:
            receipt = worker.prepare({"a": [1], "b": [2]})
            worker.send(op="commit", name="tx", barrier=point)
            assert worker.receive() == {"paused": point}
            worker.close(kill=True)
        with Worker(executable, directory, artifacts, f"commit-recovery-{point}") as worker:
            result = worker.read()
            assert result["rows"] == ([["a", [1]], ["b", [2]]] if point == 3 else []), result
            assert worker.request(op="resolve", receipt=receipt)["outcome"] == ("committed" if point == 3 else "pending")
            assert worker.request(op="checkpoint")["outcome"] == "applied"
            assert worker.request(op="resolve", receipt=receipt)["outcome"] == ("committed" if point == 3 else "rejected")
            print(f"SIGKILL commit boundary={point}: rows={result['rows']}, sequence={result['sequence']}", flush=True)
    for point in [1, 3, 4, 5]:
        directory = root / f"checkpoint-crash-{point}"
        with Worker(executable, directory, artifacts, f"checkpoint-crash-{point}") as worker:
            for batch in range(3):
                worker.prepare({f"keys/{batch}": [batch] * (80 * 1024)})
                assert worker.request(op="commit", name="tx")["outcome"] == "committed"
            expected = worker.read()
            worker.send(op="checkpoint", barrier=point)
            assert worker.receive() == {"paused": point}
            worker.close(kill=True)
        with Worker(executable, directory, artifacts, f"checkpoint-recovery-{point}") as worker:
            assert worker.read() == expected
            assert worker.request(op="checkpoint")["outcome"] == "applied"
            assert worker.read() == expected
        print(f"SIGKILL checkpoint boundary={point}: recovered all 3 pages", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--slatedb", action="store_true", help="require the pinned independent oracle")
    parser.add_argument("--redis-url", help="require a Redis oracle, e.g. redis://127.0.0.1:6379/0")
    parser.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 17, 5489])
    parser.add_argument("--steps", type=int, default=96)
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    root = args.artifacts / "stores"
    root.mkdir()
    if args.slatedb:
        version = importlib.metadata.version("slatedb")
        assert version == "0.17.0", f"expected SlateDB 0.17.0, found {version}"
        print(f"independent oracle: SlateDB {version}", flush=True)
    else:
        print("SlateDB differential testing NOT RUN (pass --slatedb)", flush=True)
    if args.redis_url:
        from redis_oracle import RedisOracle
    else:
        print("Redis differential testing NOT RUN (pass --redis-url)", flush=True)
    for seed in args.seeds:
        oracle = RedisOracle(args.redis_url, args.artifacts, f"redis-{seed}") if args.redis_url else nullcontext()
        with oracle as redis:
            asyncio.run(differential(args.driver.resolve(), root, args.artifacts, args.slatedb, redis, seed, args.steps))
    if args.redis_url:
        with RedisOracle(args.redis_url, args.artifacts, "redis-conflict") as redis:
            redis_conflicts(args.driver.resolve(), root, args.artifacts, redis)
    concurrency(args.driver.resolve(), root, args.artifacts)
    crashes(args.driver.resolve(), root, args.artifacts)
    print("PASS: WAL process-level qualification", flush=True)


if __name__ == "__main__":
    main()
