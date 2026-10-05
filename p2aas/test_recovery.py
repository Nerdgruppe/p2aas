"""Run serial fault checks without hardware: .venv/bin/python p2aas/test_recovery.py.

Requires .NET 10 and websockets. Runs the real server source against a fake
System.IO.Ports adapter on port 12880; stop any existing server first.
"""

import asyncio
import base64
import contextlib
import os
import pathlib
import struct
import subprocess
import tempfile
import time

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROJECT = ROOT / "tests/serial-recovery/serial-recovery.csproj"
SERVER = PROJECT.parent / "bin/Debug/net10.0/serial-recovery.dll"
ENDPOINT = "ws://127.0.0.1:12880/"
PAYLOAD = bytes(range(256)) * 3
CHECKSUM = (0x706F7250 - sum(struct.unpack("<192I", PAYLOAD))) & 0xFFFFFFFF
ENCODED = base64.b64encode(PAYLOAD + struct.pack("<I", CHECKSUM)).decode()


@contextlib.asynccontextmanager
async def server(scenario):
    with tempfile.TemporaryDirectory(prefix="p2aas-recovery-") as directory:
        events = pathlib.Path(directory) / "events"
        with (pathlib.Path(directory) / "stderr").open("w+") as errors:
            process = await asyncio.create_subprocess_exec(
                "dotnet", str(SERVER), stdout=asyncio.subprocess.PIPE, stderr=errors,
                env=dict(os.environ, P2AAS_TEST_SCENARIO=scenario, P2AAS_TEST_EVENTS=str(events)),
            )
            try:
                assert await asyncio.wait_for(process.stdout.readline(), 5) == b"Server started. Waiting for connections...\n"
                yield events
            except BaseException:
                errors.seek(0)
                print(errors.read())
                raise
            finally:
                if process.returncode is None:
                    process.terminate()
                await process.wait()


async def session(expected=1008, url_mode=False, send_input=True, query="timeout_ms=100"):
    if url_mode:
        query += "&code=" + base64.urlsafe_b64encode(PAYLOAD).decode()
    async with websockets.connect(ENDPOINT + "?" + query) as socket:
        if not url_mode:
            # Fragmented payload receipt must not be repeated during recovery.
            await socket.send(struct.pack("<I", len(PAYLOAD)))
            await socket.send(PAYLOAD)
        if send_input:
            await socket.send(b"pending-input")
        received = bytearray()
        try:
            while True:
                received.extend(await asyncio.wait_for(socket.recv(), 14))
        except ConnectionClosed as close:
            assert close.rcvd is not None and close.rcvd.code == expected, close
            assert close.sent is not None, "Close handshake did not finish"
            if expected == 1011:
                assert close.rcvd.reason == "The server experienced an unexpected error.", close
        return bytes(received)


async def http_error(query, expected):
    try:
        async with websockets.connect(ENDPOINT + "?" + query):
            raise AssertionError("Unexpected upgrade")
    except InvalidStatus as error:
        assert error.response.status_code == expected, error
        if expected == 500:
            assert error.response.body == b"The server experienced an unexpected error."


def lines(events, prefix):
    return [line[len(prefix):] for line in events.read_text().splitlines() if line.startswith(prefix)]


async def main():
    subprocess.run(["dotnet", "build", str(PROJECT), "--ignore-failed-sources", "-v:quiet"], check=True)
    for scenario in ("healthy", "validation-closed", "validation-io", "upload-once", "upload-eof", "upload-closed", "upload-disposed", "switch-fail", "restore-fail"):
        for url_mode in (False, True):
            async with server(scenario) as events:
                started = time.monotonic()
                assert await session(url_mode=url_mode) == b"pending-input"
                assert lines(events, "OPEN ") == (["1"] if scenario in ("healthy", "restore-fail") else ["1", "2"])
                assert lines(events, "PAYLOAD ")[-1] == ENCODED
                if scenario == "upload-once":
                    assert lines(events, "PARTIAL ")[0] == ENCODED[:256]
                    assert time.monotonic() - started >= 0.48, "Runtime quota began before recovered upload"
            print(scenario, "URL" if url_mode else "WebSocket", "passed", flush=True)

    for scenario in ("open-fail", "probe-fail", "probe-timeout", "upload-twice", "validation-upload-fail"):
        async with server(scenario) as events:
            await session(expected=1011)
            assert lines(events, "OPEN ") == ["1", "2"]
            assert len(lines(events, "ATTEMPT ")) <= 2
        print(scenario, "passed", flush=True)

    async with server("validation-open-fail") as events:
        await http_error("baudrate=115200", 500)
        # A failed request must leave the listener running.
        await http_error("timeout_ms=no", 400)
        await http_error("baudrate=115200", 500)
        assert lines(events, "OPEN ") == ["1", "2", "3"]

    async with server("healthy") as events:
        for query in ("baudrate=12345", "baudrate=12346", "baudrate=0", "timeout_ms=no", "code=!"):
            await http_error(query, 400)
        async with websockets.connect(ENDPOINT) as socket:
            await socket.send("not binary")
            try:
                await socket.recv()
            except ConnectionClosed as close:
                assert close.rcvd.code == 1002, close
        assert lines(events, "OPEN ") == ["1"]

    for scenario in ("checksum", "loader-response"):
        async with server(scenario) as events:
            await session(expected=1002, send_input=False)
            assert lines(events, "OPEN ") == ["1"]

    for scenario in ("runtime-eof", "runtime-io", "runtime-write"):
        async with server(scenario) as events:
            await session(expected=1011, send_input=scenario == "runtime-write")
            assert lines(events, "OPEN ") == ["1"], "Runtime failure retried"
            assert await session() == b"pending-input"
            assert lines(events, "OPEN ") == ["1", "2"]
        print(scenario, "cleanup and next-request recovery passed", flush=True)

    for scenario in ("deadline", "deadline-probe", "upload-cancel"):
        async with server(scenario) as events:
            started = time.monotonic()
            await session(expected=1011, send_input=False)
            assert 9.5 < time.monotonic() - started < 12, "Recovery extended the request deadline"
            assert lines(events, "OPEN ") == (["1"] if scenario == "upload-cancel" else ["1", "2"])
        print(scenario, "passed", flush=True)

    # Execute the existing timeout check unchanged against the fake adapter.
    async with server("healthy"):
        process = await asyncio.create_subprocess_exec(
            os.sys.executable, str(ROOT / "p2aas/test_timeout.py"), ENDPOINT,
        )
        assert await asyncio.wait_for(process.wait(), 10) == 0
    print("P2AAS serial recovery checks passed")


if __name__ == "__main__":
    asyncio.run(main())
