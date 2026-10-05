"""Check orderly session timeout against a running P2AAS server (requires websockets)."""

import asyncio
import os
import pathlib
import struct
import sys
import urllib.parse

import websockets
from websockets.exceptions import ConnectionClosed


async def main() -> None:
    endpoint = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("P2AAS_ENDPOINT", "ws://127.0.0.1:12880/")
    url = urllib.parse.urlsplit(endpoint)
    query = dict(urllib.parse.parse_qsl(url.query))
    query.pop("code", None)
    query["timeout_ms"] = "100"
    endpoint = urllib.parse.urlunsplit(url._replace(query=urllib.parse.urlencode(query)))
    firmware = (pathlib.Path(__file__).resolve().parents[1] / "example/payload.bin").read_bytes()

    # A second request catches shutdowns that leave the first relay running.
    for _ in range(2):
        async with websockets.connect(endpoint) as socket:
            await socket.send(struct.pack("<I", len(firmware)) + firmware)
            try:
                while True:
                    await asyncio.wait_for(socket.recv(), 5)
            except ConnectionClosed as close:
                assert close.rcvd is not None, "Server disconnected without a close frame"
                assert close.rcvd.code == 1008, close
                assert close.rcvd.reason == "No time quota left for user code.", close
                assert close.sent is not None, "Close handshake did not complete"

    print("P2AAS timeout close-handshake check passed")


if __name__ == "__main__":
    asyncio.run(main())
