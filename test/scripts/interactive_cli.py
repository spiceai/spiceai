#!/usr/bin/env python3

import os
import re
import select
import subprocess
import time
from dataclasses import dataclass
from typing import Pattern


class InteractionError(RuntimeError):
    pass


@dataclass
class ReadResult:
    output: str
    saw_prompt: bool = False
    saw_eof: bool = False


class PtyProcess:
    def __init__(self, command: list[str]):
        self.command = command
        self.master_fd, slave_fd = os.openpty()
        self.process = subprocess.Popen(
            command,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
        )
        os.close(slave_fd)

    def close(self) -> None:
        try:
            os.close(self.master_fd)
        except OSError:
            pass

        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)

    def send_line(self, value: str) -> None:
        os.write(self.master_fd, f"{value}\r".encode())

    def send_ctrl_c(self) -> None:
        os.write(self.master_fd, b"\x03")

    def read_until_prompt(
        self,
        prompt: Pattern[str],
        timeout: float,
        *,
        expected: Pattern[str] | None = None,
        error: Pattern[str] | None = None,
    ) -> ReadResult:
        deadline = time.monotonic() + timeout
        chunks: list[str] = []
        saw_expected = expected is None

        while time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            readable, _, _ = select.select([self.master_fd], [], [], remaining)
            if not readable:
                break

            try:
                data = os.read(self.master_fd, 4096)
            except OSError:
                return ReadResult("".join(chunks), saw_eof=True)

            if not data:
                return ReadResult("".join(chunks), saw_eof=True)

            text = data.decode(errors="replace")
            chunks.append(text)
            output = "".join(chunks)

            if error and error.search(output):
                raise InteractionError(f"Unexpected error output:\n{output}")

            if expected and expected.search(output):
                saw_expected = True

            if prompt.search(output):
                if not saw_expected:
                    raise InteractionError(f"Expected output was not observed before prompt:\n{output}")
                return ReadResult(output, saw_prompt=True)

        raise InteractionError(f"Timed out waiting for prompt. Output:\n{''.join(chunks)}")

    def read_until_match_or_eof(
        self,
        expected: Pattern[str],
        timeout: float,
        *,
        error: Pattern[str] | None = None,
    ) -> str:
        deadline = time.monotonic() + timeout
        chunks: list[str] = []

        while time.monotonic() < deadline:
            if expected.search("".join(chunks)):
                return "".join(chunks)

            remaining = max(0.0, deadline - time.monotonic())
            readable, _, _ = select.select([self.master_fd], [], [], remaining)
            if not readable:
                break

            try:
                data = os.read(self.master_fd, 4096)
            except OSError:
                break

            if not data:
                break

            text = data.decode(errors="replace")
            chunks.append(text)
            output = "".join(chunks)

            if error and error.search(output):
                raise InteractionError(f"Unexpected error output:\n{output}")

        output = "".join(chunks)
        if expected.search(output):
            return output
        raise InteractionError(f"Timed out waiting for expected output. Output:\n{output}")


def run_command(command: list[str], timeout: float = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
    )