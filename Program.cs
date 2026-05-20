using System;
using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Net.WebSockets;
using System.IO.Ports;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

string portName = "/dev/serial/by-id/usb-FTDI_FT231X_USB_UART_DUAB9RPU-if00-port0";
const int MaxPayloadSize = 512 * 1024;
const int SerialChunkSize = 32;
const string ExpectedDeviceVersion = "Prop_Ver G";

var serialPort = new SerialPort(portName)
{
    BaudRate = 115200,
    DataBits = 8,
};

serialPort.Open();

HttpListener listener = new();
listener.Prefixes.Add($"http://0.0.0.0:12880/");
listener.Start();

Console.WriteLine("Server started. Waiting for connections...");

while (true)
{
    // we process one request after another:
    HttpListenerContext context = await listener.GetContextAsync();
    if (!context.Request.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        context.Response.Close();
        continue;
    }

    var cts = new CancellationTokenSource(delay: TimeSpan.FromMilliseconds(10_000));
    try
    {
        await ProcessWebSocketRequest(context, cts.Token);
    }
    catch (ProtocolViolationException ex)
    {
        Console.Error.WriteLine("Protocol violation detected:");
        Console.Error.WriteLine(ex.ToString());
    }
    catch (OperationCanceledException ex)
    {
        Console.Error.WriteLine("Connection timed out:");
        Console.Error.WriteLine(ex.ToString());
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine("Connection failed:");
        Console.Error.WriteLine(ex.ToString());
    }
}


async Task ProcessWebSocketRequest(HttpListenerContext context, CancellationToken cancellationToken)
{
    var webSocketContext = await context.AcceptWebSocketAsync(null);
    using var socket = webSocketContext.WebSocket;
    using var loadTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    loadTimeout.CancelAfter(TimeSpan.FromSeconds(10));

    try
    {
        var payload = await ReadPayload(socket, loadTimeout.Token);
        await LoadPayloadToDevice(payload, loadTimeout.Token);
        await BridgeSocketAndSerial(socket, cancellationToken);
        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None);
    }
    catch (ProtocolViolationException ex)
    {
        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.ProtocolError, ex.Message, CancellationToken.None);
        throw;
    }
    catch (OperationCanceledException) when (loadTimeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
    {
        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.PolicyViolation, "Timed out while loading payload.", CancellationToken.None);
        throw;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.ToString());

        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.InternalServerError, "The server experienced an unexpected error.", CancellationToken.None);
        throw;
    }

}

async Task<ReadOnlyMemory<byte>> ReadPayload(WebSocket socket, CancellationToken cancellationToken)
{
    var lengthBuffer = await ReadMessage(socket, new byte[sizeof(uint)], cancellationToken);
    if (lengthBuffer.Length != sizeof(uint))
    {
        throw new ProtocolViolationException($"Expected a {sizeof(uint)} byte length prefix, but received {lengthBuffer.Length} bytes.");
    }

    var payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(lengthBuffer.Span);
    if (payloadLength == 0)
    {
        throw new ProtocolViolationException("Expected a non-empty payload.");
    }

    if (payloadLength > MaxPayloadSize)
    {
        throw new ProtocolViolationException($"Expected a maximum payload size of {MaxPayloadSize} bytes, but received {payloadLength}.");
    }

    return await ReadMessage(socket, new byte[(int)payloadLength], cancellationToken);
}

async Task LoadPayloadToDevice(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
{
    if ((payload.Length % sizeof(uint)) != 0)
    {
        throw new ProtocolViolationException("Payload length must be divisible by 4.");
    }

    serialPort.DiscardInBuffer();
    serialPort.DiscardOutBuffer();

    await WriteSerialAsciiAsync("> ", cancellationToken);

    await WriteSerialAsciiAsync("Prop_Chk 0 0 0 0\r", cancellationToken);
    _ = await ReadSerialLineAsync(cancellationToken);

    var version = (await ReadSerialLineAsync(cancellationToken)).Trim(' ', '\r', '\n');
    if (!string.Equals(version, ExpectedDeviceVersion, StringComparison.Ordinal))
    {
        Console.Error.WriteLine($"Device identifies as \"{version}\", but expected \"{ExpectedDeviceVersion}\".");
    }

    var checksummedPayload = AppendChecksum(payload.Span);
    var encodedPayload = Convert.ToBase64String(checksummedPayload);

    await WriteSerialAsciiAsync("Prop_Txt 0 0 0 0 ", cancellationToken);
    for (var offset = 0; offset < encodedPayload.Length; offset += SerialChunkSize)
    {
        var chunkLength = Math.Min(SerialChunkSize, encodedPayload.Length - offset);
        await WriteSerialAsciiAsync(encodedPayload.Substring(offset, chunkLength), cancellationToken);

        if (offset + chunkLength < encodedPayload.Length)
        {
            await WriteSerialAsciiAsync("\r> ", cancellationToken);
        }
    }

    await WriteSerialAsciiAsync(" ?\r", cancellationToken);

    var response = await ReadSerialByteAsync(cancellationToken);
    switch (response)
    {
        case (byte)'.':
            return;
        case (byte)'!':
            throw new ProtocolViolationException("Device rejected the payload checksum.");
        default:
            throw new ProtocolViolationException($"Unexpected response from Prop_Txt: 0x{response:X2}.");
    }
}

byte[] AppendChecksum(ReadOnlySpan<byte> payload)
{
    var checksummedPayload = new byte[payload.Length + sizeof(uint)];
    payload.CopyTo(checksummedPayload);

    uint checksum = 0x706F7250;
    for (var offset = 0; offset < payload.Length; offset += sizeof(uint))
    {
        var word = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
        checksum -= word;
    }

    BinaryPrimitives.WriteUInt32LittleEndian(checksummedPayload.AsSpan(payload.Length), checksum);
    return checksummedPayload;
}

async Task BridgeSocketAndSerial(WebSocket socket, CancellationToken cancellationToken)
{
    using var relayCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    var socketToSerial = ForwardSocketToSerial(socket, relayCancellation.Token);
    var serialToSocket = ForwardSerialToSocket(socket, relayCancellation.Token);

    var completedRelay = await Task.WhenAny(socketToSerial, serialToSocket);
    await completedRelay;

    relayCancellation.Cancel();

    try
    {
        await Task.WhenAll(socketToSerial, serialToSocket);
    }
    catch (OperationCanceledException) when (relayCancellation.IsCancellationRequested)
    {
    }
}

async Task ForwardSocketToSerial(WebSocket socket, CancellationToken cancellationToken)
{
    var buffer = new byte[4096];
    while (socket.State == WebSocketState.Open)
    {
        var result = await socket.ReceiveAsync(buffer, cancellationToken);
        if (result.MessageType == WebSocketMessageType.Close)
        {
            return;
        }

        if (result.Count > 0)
        {
            await serialPort.BaseStream.WriteAsync(buffer.AsMemory(0, result.Count), cancellationToken);
            await serialPort.BaseStream.FlushAsync(cancellationToken);
        }
    }
}

async Task ForwardSerialToSocket(WebSocket socket, CancellationToken cancellationToken)
{
    var buffer = new byte[4096];
    while (socket.State == WebSocketState.Open)
    {
        var bytesRead = await serialPort.BaseStream.ReadAsync(buffer, cancellationToken);
        if (bytesRead == 0)
        {
            return;
        }

        await socket.SendAsync(buffer.AsMemory(0, bytesRead), WebSocketMessageType.Binary, true, cancellationToken);
    }
}

async Task<string> ReadSerialLineAsync(CancellationToken cancellationToken)
{
    using var lineBuffer = new MemoryStream();
    while (true)
    {
        var value = await ReadSerialByteAsync(cancellationToken);
        if (value == (byte)'\n')
        {
            return Encoding.ASCII.GetString(lineBuffer.GetBuffer(), 0, (int)lineBuffer.Length);
        }

        lineBuffer.WriteByte(value);
    }
}

async Task<byte> ReadSerialByteAsync(CancellationToken cancellationToken)
{
    var buffer = new byte[1];
    var bytesRead = await serialPort.BaseStream.ReadAsync(buffer, cancellationToken);
    if (bytesRead == 0)
    {
        throw new EndOfStreamException("Serial port closed while waiting for device data.");
    }

    return buffer[0];
}

async Task WriteSerialAsciiAsync(string value, CancellationToken cancellationToken)
{
    await serialPort.BaseStream.WriteAsync(Encoding.ASCII.GetBytes(value), cancellationToken);
    await serialPort.BaseStream.FlushAsync(cancellationToken);
}

async Task CloseSocketIfNeeded(WebSocket socket, WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
{
    if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
    {
        await socket.CloseAsync(closeStatus, statusDescription, cancellationToken);
    }
}

async Task<ReadOnlyMemory<byte>> ReadMessage(WebSocket socket, byte[] buffer, CancellationToken cancellationToken)
{
    var offset = 0;
    var endOfMessage = false;
    while (offset < buffer.Length)
    {
        var result = await socket.ReceiveAsync(buffer[offset..], cancellationToken);
        if (result.MessageType == WebSocketMessageType.Close)
        {
            throw new ProtocolViolationException("Connection closed before the full payload was received.");
        }

        if (result.MessageType != WebSocketMessageType.Binary)
        {
            throw new ProtocolViolationException("Expected binary websocket messages.");
        }

        offset += result.Count;
        if (result.EndOfMessage)
        {
            endOfMessage = true;
            break;
        }
    }

    if (!endOfMessage)
    {
        throw new ProtocolViolationException($"Received a websocket message larger than {buffer.Length} bytes.");
    }

    return buffer[0..offset];
}