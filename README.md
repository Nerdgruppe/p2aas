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
   - reads a length-prefixed binary payload from the WebSocket,
   - resets the board,
   - uploads the image through `Prop_Txt`,
   - switches the serial port to the requested runtime baud rate,
   - relays bytes in both directions between WebSocket and serial.

The server currently processes requests sequentially. It is meant to be a small remote loader and terminal endpoint, not a multi-tenant service.

## Repository Layout

- `Program.cs`: the server implementation, including HTTP handling, WebSocket protocol, Propeller boot-loader upload, serial proxy, and failure tracing.
- `example/example.py`: a simple client that uploads a payload and enters a terminal session.
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
- `just` if you want to use the helper recipes in `justfile`
- `flexspin` and `loadp2` if you want to rebuild or load the example payload directly from the command line

## Build

Build the server from the repository root:

```bash
dotnet build
```

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

Example:

```bash
python example/example.py \
    --url ws://127.0.0.1:12880/ \
    --baudrate 230400 \
    --timeout-ms 5000 \
    example/payload.bin
```

Exit the terminal by sending EOF, typically `Ctrl-D`.

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

If validation fails, the server returns HTTP `400 Bad Request` with a plain-text message and does not upgrade the connection.

### State 2: Upload Stream

After the WebSocket is accepted, the client must upload the program image.

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

Upload invariants enforced by the server:

- the stream must begin with a 4-byte little-endian payload length
- the payload length must be greater than zero
- the payload length must not exceed `512 * 1024`
- the payload length must be divisible by 4
- all upload data must arrive as binary WebSocket data
- if the socket closes before all bytes arrive, the upload fails

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

Example URLs:

```text
ws://127.0.0.1:12880/
ws://127.0.0.1:12880/?baudrate=230400
ws://127.0.0.1:12880/?baudrate=230400&timeout_ms=5000
```

## Error Behavior

Common failure modes:

- invalid query parameters: HTTP `400`
- non-binary upload data: WebSocket protocol error
- truncated upload stream: WebSocket protocol error
- oversized or empty payload: WebSocket protocol error
- device checksum rejection: upload fails
- runtime timeout: WebSocket policy violation

The server keeps an in-memory per-request trace and prints it when a request does not end as `completed`. That trace includes websocket activity, serial activity, timestamps, and major branch decisions, which makes it the primary debugging aid for failed sessions.

## Implementation Notes

- single-file server implementation in `Program.cs`
- single active request at a time
- maximum payload size: `512 KiB`
- loader baud rate: `2_000_000`
- runtime default baud rate: `115200`
- upload framing is stream-oriented across WebSocket messages

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
