import argparse
import asyncio
import importlib
import os
import pathlib
import select
import struct
import sys
import termios
import tty
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import Any

websockets = importlib.import_module("websockets")
ConnectionClosed = importlib.import_module("websockets.exceptions").ConnectionClosed

TerminalIdleGracePeriod = 0.1
TerminalDrainTimeout = 1.0
InitialOutputTimeout = 2.0
MinUserTimeoutMs = 100
MaxUserTimeoutMs = 10_000


@dataclass
class OutputTracker:
    last_activity: float | None = None
    ready: asyncio.Event | None = None


def format_socket_close(code: int | None, reason: str | None) -> str:
    details: list[str] = []
    if code is not None:
        details.append(f"code={code}")

    if reason:
        details.append(f"reason={reason}")

    detail_text = ", ".join(details) if details else "no close details"
    return f"socket closed ({detail_text})"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a Propeller 2 payload through the P2AAS websocket server and open a terminal.",
    )
    parser.add_argument(
        "payload",
        type=pathlib.Path,
        help="Path to the payload binary to upload.",
    )
    parser.add_argument(
        "--url",
        default="ws://127.0.0.1:12880/",
        help="P2AAS websocket URL.",
    )
    parser.add_argument(
        "--baudrate",
        type=int,
        help="User-code baudrate passed to the P2AAS server after upload.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        help="Post-upload user-code timeout in milliseconds passed to the P2AAS server.",
    )
    return parser.parse_args()


def build_url(base_url: str, baudrate: int | None, timeout_ms: int | None) -> str:
    parsed = urllib.parse.urlsplit(base_url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)

    if baudrate is not None:
        query.append(("baudrate", str(baudrate)))

    if timeout_ms is not None:
        query.append(("timeout_ms", str(timeout_ms)))

    return urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query)))


@contextmanager
def raw_stdin() -> Iterator[None]:
    if not sys.stdin.isatty():
        yield
        return

    fd = sys.stdin.fileno()
    previous = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, previous)


async def read_stdin_chunk(poll_interval: float = 0.1) -> bytes | None:
    while True:
        ready, _, _ = await asyncio.to_thread(select.select, [sys.stdin], [], [], poll_interval)
        if ready:
            return os.read(sys.stdin.fileno(), 1024)

        await asyncio.sleep(0)


async def forward_stdin_to_socket(socket: Any) -> str:
    while True:
        chunk = await read_stdin_chunk()
        if chunk is None:
            continue

        if chunk == b"":
            return "stdin reached EOF"

        if sys.stdin.isatty():
            eof_index = chunk.find(b"\x04")
            if eof_index >= 0:
                if eof_index > 0:
                    await socket.send(chunk[:eof_index])

                return "Ctrl-D"

        try:
            await socket.send(chunk)
        except ConnectionClosed as ex:
            return format_socket_close(ex.code, ex.reason)


async def forward_socket_to_stdout(socket: Any, tracker: OutputTracker) -> str:
    try:
        async for message in socket:
            if isinstance(message, str):
                data = message.encode()
            else:
                data = message

            tracker.last_activity = asyncio.get_running_loop().time()
            if tracker.ready is not None:
                tracker.ready.set()

            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
        return format_socket_close(socket.close_code, socket.close_reason)
    except ConnectionClosed:
        return format_socket_close(socket.close_code, socket.close_reason)


async def drain_terminal_output(tracker: OutputTracker) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + TerminalDrainTimeout
    last_activity = tracker.last_activity

    while loop.time() < deadline:
        await asyncio.sleep(TerminalIdleGracePeriod)
        if tracker.last_activity is None:
            continue

        if tracker.last_activity == last_activity:
            return

        last_activity = tracker.last_activity


async def run_terminal(url: str, payload_path: pathlib.Path) -> str:
    payload = payload_path.read_bytes()
    upload = struct.pack("<I", len(payload)) + payload

    async with websockets.connect(url, max_size=None) as socket:
        await socket.send(upload)

        ready_event = asyncio.Event()
        tracker = OutputTracker(ready=ready_event)
        receiver = asyncio.create_task(forward_socket_to_stdout(socket, tracker))

        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(ready_event.wait(), timeout=InitialOutputTimeout)

        sender = asyncio.create_task(forward_stdin_to_socket(socket))

        done, pending = await asyncio.wait({receiver, sender}, return_when=asyncio.FIRST_COMPLETED)

        if sender in done:
            exit_reason = await sender
            await drain_terminal_output(tracker)
            await socket.close()
            with suppress(ConnectionClosed, asyncio.CancelledError):
                await receiver
            return exit_reason
        else:
            exit_reason = await receiver
            sender.cancel()
            with suppress(asyncio.CancelledError):
                await sender
            return exit_reason


def main() -> None:
    args = parse_args()

    if not args.payload.is_file():
        raise SystemExit(f"Payload file not found: {args.payload}")

    if args.baudrate is not None and args.baudrate <= 0:
        raise SystemExit("--baudrate must be a positive integer")

    if args.timeout_ms is not None and args.timeout_ms <= 0:
        raise SystemExit("--timeout-ms must be a positive integer")

    if args.timeout_ms is not None and not (MinUserTimeoutMs <= args.timeout_ms <= MaxUserTimeoutMs):
        raise SystemExit(f"--timeout-ms must be between {MinUserTimeoutMs} and {MaxUserTimeoutMs}")

    url = build_url(args.url, args.baudrate, args.timeout_ms)

    with raw_stdin():
        try:
            exit_reason = asyncio.run(run_terminal(url, args.payload))
        except KeyboardInterrupt:
            exit_reason = "KeyboardInterrupt"

    print(f"\nExit reason: {exit_reason}", file=sys.stderr)


if __name__ == "__main__":
    main()
