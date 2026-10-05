"""Run after `dotnet build p2aas-run/p2aas-run.csproj` (requires websockets)."""

import asyncio
import os
import pathlib
import pty
import struct
import termios

import websockets


ROOT = pathlib.Path(__file__).resolve().parents[1]
CLIENT = ROOT / "p2aas-run/bin/Debug/net10.0/p2aas-run.dll"
FIRMWARE = ROOT / "example/payload.bin"


async def check(interactive: bool, close_code: int, expected_exit: int) -> None:
    firmware = FIRMWARE.read_bytes()
    received = []
    paths = []

    async def server(socket):
        paths.append(socket.request.path)
        received.append(await socket.recv())
        received.append(await socket.recv())
        for _ in range(3 if interactive else 1):
            received.append(await socket.recv())
        await socket.send(b"\x00ready\n")
        reason = "No time quota left for user code." if close_code == 1008 else "upload failed" if close_code == 1011 else ""
        await socket.close(close_code, reason)

    async with websockets.serve(server, "127.0.0.1", 0) as listener:
        port = listener.sockets[0].getsockname()[1]
        command = [
            "dotnet", str(CLIENT), "-s", f"ws://127.0.0.1:{port}/?token=ok&baudrate=123&timeout_ms=100",
            "-b", "230400", "-t", "500", "-I", "first", str(FIRMWARE),
        ]
        if interactive:
            command.insert(-1, "-i")
        process = await asyncio.create_subprocess_exec(
            *command, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(process.communicate(b"abc"), 5)

    assert process.returncode == expected_exit, errors
    assert output == b"\x00ready\n", output
    assert paths == ["/?token=ok&baudrate=230400&timeout_ms=500"], paths
    assert received[0] == struct.pack("<I", len(firmware)) + firmware
    assert received[1] == b"first"
    assert received[2:] == ([b"a", b"b", b"c"] if interactive else [b"abc"]), received[2:]


async def main() -> None:
    await check(False, 1000, 0)
    await check(True, 1008, 0)
    await check(False, 1011, 1)
    await check_terminal(True)
    await check_terminal(False)
    await check_connect_timeout()
    print("p2aas-run websocket checks passed")


async def check_terminal(interactive: bool) -> None:
    uploaded = asyncio.Event()
    received = []

    async def server(socket):
        await socket.recv()
        uploaded.set()
        for _ in range(7 if interactive else 1):
            received.append(await socket.recv())
        await socket.close()

    async with websockets.serve(server, "127.0.0.1", 0) as listener:
        port = listener.sockets[0].getsockname()[1]
        master, slave = pty.openpty()
        original_settings = termios.tcgetattr(slave)
        try:
            command = ["dotnet", str(CLIENT), "-s", f"ws://127.0.0.1:{port}/"]
            if not interactive:
                command.append("--no-interactive")
            process = await asyncio.create_subprocess_exec(
                *command, str(FIRMWARE),
                stdin=slave, stdout=slave, stderr=slave,
            )
            await asyncio.wait_for(uploaded.wait(), 5)
            await asyncio.sleep(0.05)
            os.write(master, b"ab\n\x03\x1b[A" if interactive else b"ab\n")
            await asyncio.wait_for(process.wait(), 5)
            assert process.returncode == 0, process.returncode
            assert received == ([b"a", b"b", b"\n", b"\x03", b"\x1b", b"[", b"A"] if interactive else [b"ab\n"]), received
            assert termios.tcgetattr(slave) == original_settings
        finally:
            os.close(master)
            os.close(slave)


async def check_connect_timeout() -> None:
    async def stalled_connection(reader, writer):
        try:
            await reader.read()
        finally:
            writer.close()

    async with await asyncio.start_server(stalled_connection, "127.0.0.1", 0) as listener:
        port = listener.sockets[0].getsockname()[1]
        process = await asyncio.create_subprocess_exec(
            "dotnet", str(CLIENT), "-s", f"ws://127.0.0.1:{port}/", "-t", "100", str(FIRMWARE),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        _, errors = await asyncio.wait_for(process.communicate(), 5)
        assert process.returncode == 1, (process.returncode, errors)
        assert b"Connection timed out after 100 ms" in errors, errors


if __name__ == "__main__":
    asyncio.run(main())
