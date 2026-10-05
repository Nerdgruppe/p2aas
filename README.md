# p2aas

`p2aas` is a small .NET 10 service that exposes a Propeller 2 board over WebSocket.

It accepts one WebSocket connection at a time, uploads a Propeller 2 image through the serial boot loader, then turns the socket into a live serial bridge for the uploaded program. The implementation is intentionally compact and centered in `Program.cs`.

## What It Does

At a high level, the server:

1. Opens a configured serial device connected to a Propeller 2 board.
2. Validates the board at startup with `Prop_Chk`.
3. Listens for WebSocket upgrades on `http://*:12880/`.
4. For each accepted request:
    - validates query parameters,
    - obtains the payload either from the WebSocket upload stream or from the `code` query parameter,
    - resets the board,
    - uploads the image through `Prop_Txt`,
    - switches the serial port to the requested runtime baud rate,
    - relays bytes in both directions between WebSocket and serial.

The server currently processes requests sequentially. It is meant to be a small remote loader and terminal endpoint, not a multi-tenant service.

## Repository Layout

- `Program.cs`: the server implementation, including HTTP handling, WebSocket protocol, Propeller boot-loader upload, serial proxy, and failure tracing.
- `example/example.py`: a simple client that uploads a payload and enters a terminal session.
- `p2aas-run/`: the .NET WebSocket upload and terminal client.
- `example/payload.spin2`: example Propeller source.
- `example/payload.bin`: example compiled payload used by the client.
- `docs/p2boot.txt`: Propeller 2 boot-loader notes used as the protocol reference.
- `justfile`: helper recipes for assembling and loading the example payload directly.

## Prerequisites

Server runtime:

- .NET 10 SDK
- a Propeller 2 board reachable through a serial adapter
- permission to open the serial device configured in `Program.cs`

Optional tools:

- Python 3 with the `websockets` package for `example/example.py`
- `stty` for raw terminal input with `p2aas-run` on Unix
- `just` if you want to use the helper recipes in `justfile`
- `flexspin` and `loadp2` if you want to rebuild or load the example payload directly from the command line

## Build

Build the server from the repository root:

```bash
dotnet build
```

Build and run the .NET WebSocket client:

```bash
dotnet build p2aas-run/p2aas-run.csproj
P2AAS_ENDPOINT=ws://127.0.0.1:12880/ dotnet run --project p2aas-run -- example/payload.bin
```

`dotnet run --project p2aas-run -- --help` lists the options. The client uploads the binary over the WebSocket, then forwards device output to stdout. Terminal input is sent character by character; piped stdin is sent as one chunk. Use `--no-interactive` for line-buffered terminal input, `--input TEXT` to send text before stdin, and `--baudrate` or `--timeout` to configure the server session. A normal server close, including its session timeout close, exits successfully.

Publish a self-contained release build using the existing recipe:

```bash
just publish
```

## Configuration

The current implementation uses a hard-coded serial path in `Program.cs`:

```csharp
string portName = "/dev/serial/by-id/usb-FTDI_FT231X_USB_UART_DUAB9RPU-if00-port0";
```

The HTTP listener is also currently fixed in code:

```text
http://*:12880/
```

On this Linux setup, `http://*:12880/` works, while `http://0.0.0.0:12880/` did not.

## Running The Server

Start the service from the repository root:

```bash
dotnet run
```

On startup the service opens the serial port, resets the device, sends `Prop_Chk`, and requires the loader to answer with `Prop_Ver G`. If that probe fails, startup fails.

When startup succeeds, the server waits for WebSocket connections.

## Example Client Usage

The example client uploads a payload and then forwards your terminal to the Propeller program over WebSocket.

Run it like this:

```bash
python example/example.py example/payload.bin
```

Optional arguments:

- `--url`: override the websocket URL. Default: `ws://127.0.0.1:12880/`
- `--baudrate`: runtime serial baud rate after upload
- `--timeout-ms`: post-upload session timeout in milliseconds
- `--code-in-url`: send the payload via the URL `code` query parameter instead of via the websocket upload stream

Example:

```bash
python example/example.py \
    --url ws://127.0.0.1:12880/ \
    --baudrate 230400 \
    --timeout-ms 5000 \
    example/payload.bin
```

To exercise the URL-based upload mode:

```bash
python example/example.py --code-in-url example/payload.bin
```

Exit the terminal by sending EOF, typically `Ctrl-D`.

## Example Workflow using `websocat`

It's also possible to use `websocat` to connect to the system. This repository ships as tool to perform urlencode on CLI:

```sh-session
(.venv) [user@machine p2aas]$ websocat "ws://127.0.0.1:12880/?timeout_ms=10000&code=$(base64 --wrap 0 example/payload.bin | python ./example/urlencode.py)"
!
hello, world!
hello, world!
(.venv) [user@machine p2aas]$ 
```

## Example Payload Workflow

If you have the Propeller toolchain installed, the `justfile` contains a minimal workflow for the sample payload.

Assemble the sample image:

```bash
just assemble
```

Load it directly over serial without the WebSocket server:

```bash
just load
```

Load it directly and enter a serial terminal:

```bash
just run
```

These commands are useful for checking whether failures are in the hardware/toolchain path or in the `p2aas` server path.

## Protocol Overview

`p2aas` has two layers of protocol behavior:

1. a client-facing WebSocket protocol
2. a server-to-device Propeller 2 boot-loader protocol

The client only speaks the WebSocket side. The server handles all boot-loader details.

## Semantic Protocol Description

The connection is best described as a small state machine.

### State 1: HTTP Validation

Before any WebSocket upgrade is accepted, the server validates the incoming request.

Requirements:

- the request must be a WebSocket upgrade
- `baudrate`, if present, must be a positive integer usable on the serial port
- `timeout_ms`, if present, must be a positive integer between `100` and `10000`
- `code`, if present, must decode as base64 or base64url, must decode to at most `512 * 1024` bytes, and must decode to a byte length divisible by 4

If validation fails, the server returns HTTP `400 Bad Request` with a plain-text message and does not upgrade the connection.

### State 2: Upload Stream

After the WebSocket is accepted, the server chooses the upload source.

If the `code` query parameter is present, that selects URL-upload mode, even when the value is empty. In that mode, the payload comes from the decoded query parameter and the server does not read an upload prelude from the websocket.

If the `code` query parameter is absent, the server uses the websocket upload stream.

#### WebSocket Upload Mode

Semantically, the server reads a byte stream with this shape:

```text
uint32_le payload_length
payload_length bytes of payload
```

Important property:

- WebSocket message boundaries are not semantically meaningful during this phase.

That means all of the following are valid and equivalent from the server's point of view:

- one binary WebSocket message containing `length + payload`
- one binary message containing the 4-byte length, followed by another binary message containing the payload
- a fragmented prefix such as two bytes of length, then two more, then the payload

In other words, the upload phase is stream-oriented even though WebSocket is frame-based.

Upload invariants enforced in websocket mode:

- the stream must begin with a 4-byte little-endian payload length
- the payload length must be greater than zero
- the payload length must not exceed `512 * 1024`
- the payload length must be divisible by 4
- all upload data must arrive as binary WebSocket data
- if the socket closes before all bytes arrive, the upload fails

#### URL Code Mode

If `code` is present in the URL, the server decodes the payload directly from the query parameter.

Semantics in URL-code mode:

- `code` takes precedence over websocket upload bytes
- `code=` is valid and selects an empty payload
- both standard base64 and base64url are accepted
- URL-upload validation happens before websocket upgrade
- after the websocket is accepted, the connection proceeds directly toward device upload and then the runtime bridge

Practical note:

- URL length limits depend on the environment in front of the server, so websocket upload remains the safer choice for larger payloads

### State 3: Loader Translation

Once the server has received the payload bytes, it translates that client payload into the Propeller 2 boot-loader protocol.

The client does not send:

- `Prop_Chk`
- `Prop_Txt`
- Base64
- the final checksum long

The server does all of that internally.

The server-side upload sequence is:

1. switch serial to loader baud rate (`2_000_000`)
2. pulse DTR to reset the board
3. wait for the boot loader to become active
4. send the required initial `> ` auto-baud prefix
5. append the Propeller checksum long to the payload
6. Base64-encode the checksummed image
7. send `Prop_Txt 0 0 0 0 ... ?`
8. wait for the loader response byte

Expected loader responses:

- `.` means upload accepted and verified
- `!` means checksum failure
- anything else is treated as protocol failure

The behavior around `> ` and repeated `>` prefixes comes from the Propeller 2 loader's auto-baud requirements described in `docs/p2boot.txt`.

### State 4: Runtime Bridge

If the upload succeeds, the server switches the serial port to the requested runtime baud rate and the connection becomes an interactive byte bridge.

Semantics in this phase:

- bytes from the WebSocket are written to the serial port
- bytes from the serial port are sent back to the client as binary WebSocket messages
- clients should treat the session as a byte stream tunneled over WebSocket

The server may choose any convenient WebSocket framing for outbound serial data. Clients must not attach meaning to individual WebSocket message boundaries during the runtime session.

### State 5: Termination

The session ends when one of these happens:

- the client closes the socket
- the serial side closes or one side of the bridge terminates
- the configured runtime timeout expires
- an upload or protocol error occurs
- an unexpected server-side failure occurs

## Query Parameters

The service accepts query parameters on the WebSocket URL.

### `baudrate`

Sets the serial baud rate used after the upload completes successfully.

Default:

```text
115200
```

### `timeout_ms`

Sets the maximum allowed duration of the post-upload runtime bridge.

Default:

```text
2500
```

Allowed range:

```text
100..10000
```

### `code`

Sets the upload payload directly in the URL as base64-encoded binary.

Semantics:

- if `code` is present, the server does not read the initial upload payload from the websocket
- `code=` is valid and selects an empty payload
- both standard base64 and base64url are accepted
- the decoded payload must be at most `512 * 1024` bytes
- the decoded payload length must be divisible by 4

Recommendation:

- prefer base64url for generated URLs because it avoids `+` and `/` characters
- prefer websocket upload for larger payloads because URL length limits vary by client and server environment

Example URLs:

```text
ws://127.0.0.1:12880/
ws://127.0.0.1:12880/?baudrate=230400
ws://127.0.0.1:12880/?baudrate=230400&timeout_ms=5000
ws://127.0.0.1:12880/?code=<base64-or-base64url-payload>
```

## Error Behavior

Common failure modes:

- invalid query parameters: HTTP `400`
- invalid `code` query payload: HTTP `400`
- non-binary upload data: WebSocket protocol error
- truncated upload stream: WebSocket protocol error
- oversized or empty payload: WebSocket protocol error
- device checksum rejection: upload fails
- runtime timeout: WebSocket policy violation

Serial transport failures before runtime receive one immediate recovery attempt per request, shared between HTTP baud-rate validation and firmware upload. The server disposes the failed port, opens a fresh instance, and verifies the board with the same `Prop_Chk` probe used at startup. If an upload was interrupted, it resets the board and uploads the complete retained payload again on the same WebSocket. Clients do not need to resend firmware or reconnect; the request may take longer.

Recovery stays within the existing 10-second total request deadline, including hardware validation. The one-second probe timeout is also bounded by that deadline. The requested `timeout_ms` runtime quota starts after the successful upload. There is no backoff or second recovery attempt, and protocol errors, checksum rejection, and cancellation do not trigger recovery.

Failed recovery before the WebSocket upgrade returns HTTP `500` with `The server experienced an unexpected error.` Invalid baud rates still return HTTP `400`. Failed recovery after upgrade closes the WebSocket with `1011` (`InternalServerError`) and the same generic message. Serial failure or EOF during runtime also closes with `1011`; it does not restart the program. The next request can recover the failed port using its own allowance. Startup still fails if the initial open or probe fails.

The server keeps an in-memory per-request trace and prints it when a request does not end as `completed`. That trace includes websocket activity, serial activity, timestamps, and major branch decisions, which makes it the primary debugging aid for failed sessions.

## Recovery Checks

Run the hardware-free recovery checks with .NET 10 and Python's `websockets` package:

```bash
.venv/bin/python p2aas/test_recovery.py
```

Stop any server listening on port `12880` first. The harness compiles the actual server source against a fake serial adapter, exercises validation and upload recovery, checks failure close handshakes and runtime cleanup, and runs the existing timeout check. It verifies payload retransmission in WebSocket and URL upload modes and that pending runtime input survives an upload retry. The test project has no package dependencies.

For physical verification, run against the configured Propeller 2 adapter and interrupt its connection during a sufficiently large upload, restoring it before the immediate reopen attempt. Confirm that the request completes on the same WebSocket and logs a successful recovery. Disconnect during runtime and confirm close code `1011`, then restore the adapter and submit another request. The new request should reopen and probe it. Timing and native driver behavior require this hardware check in addition to the simulated faults.

## Implementation Notes

- single-file server implementation in `Program.cs`
- single active request at a time
- maximum payload size: `512 KiB`
- loader baud rate: `2_000_000`
- runtime default baud rate: `115200`
- upload mode is selected by request shape: websocket stream by default, or URL query payload when `code` is present
- websocket upload framing is stream-oriented across WebSocket messages

## Limitations

- the serial port path is hard-coded
- the HTTP listener address is hard-coded
- the service is single-request, not concurrent
- there is no authentication, authorization, or transport security layer
- the example client is intended for local development and operator use, not as a hardened production client

## References

- `docs/p2boot.txt` for the Propeller 2 serial loading behavior
- `example/example.py` for a minimal client implementation
- `justfile` for the sample payload workflow
