using System;
using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Net.WebSockets;
using System.IO.Ports;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Buffers;

string portName = "/dev/serial/by-id/usb-FTDI_FT231X_USB_UART_DUAB9RPU-if00-port0";

const int LoaderBaudRate = 2_000_000;
const int DefaultUserBaudRate = 115_200;
const int DefaultUserCodeTimeoutMs = 2_500;
const int MinUserCodeTimeoutMs = 100;
const int MaxUserCodeTimeoutMs = 10_000;

const int MaxPayloadSize = 512 * 1024;
const int SerialChunkSize = 256;
const int ResetAssertTimeMs = 5;
const int ResetReleaseSettleTimeMs = 15;
const int PropChkTimeoutMs = 1_000;
const string ExpectedDeviceVersion = "Prop_Ver G";

var MaxRequestTime = TimeSpan.FromMilliseconds(10_000);

using var serialPort = await OpenSerialPort(portName);

HttpListener listener = new();
listener.Prefixes.Add("http://*:12880/");
listener.Start();

Console.WriteLine("Server started. Waiting for connections...");

var receiveBuffer = new byte[MaxPayloadSize + sizeof(uint)];

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

    RequestOptions requestOptions;
    try
    {
        requestOptions = ParseAndValidateRequestOptions(context.Request);
    }
    catch (BadHttpRequestException ex)
    {
        await RejectBadRequest(context.Response, ex.Message);
        continue;
    }

    using var cts = new CancellationTokenSource(delay: MaxRequestTime);
    try
    {
        await ProcessWebSocketRequest(context, requestOptions, receiveBuffer, cts.Token);
    }
    catch (ProtocolViolationException ex)
    {
        Console.Error.WriteLine("Protocol violation detected: {0}", ex.Message);
    }
    catch (WebSocketException ex)
    {
        Console.Error.WriteLine("WebSocket closed unexpectedly: {0}", ex.Message);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine("Connection failed:");
        Console.Error.WriteLine(ex.ToString());
    }
}

async Task ProcessWebSocketRequest(HttpListenerContext context, RequestOptions requestOptions, byte[] receiveBuffer, CancellationToken cancellationToken)
{
    var trace = new ConnectionTrace(context.Request, requestOptions);
    WebSocket? socket = null;
    using var userCodeTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

    var sw = Stopwatch.StartNew();
    var uploadStamp = TimeSpan.Zero;
    var exitReason = "not started";

    var uploadError = false;
    try
    {
        trace.LogDecision("accepting websocket upgrade");
        var webSocketContext = await context.AcceptWebSocketAsync(null);
        socket = webSocketContext.WebSocket;
        trace.LogDecision($"websocket accepted; state={socket.State}");

        ReadOnlyMemory<byte> payload;
        if (requestOptions.UploadMode == UploadMode.UrlCode)
        {
            Debug.Assert(requestOptions.UrlCodePayload is not null);
            payload = requestOptions.UrlCodePayload;
            trace.LogDecision($"using upload payload from URL query parameter 'code'; bytes={payload.Length}");
        }
        else
        {
            trace.LogDecision("using upload payload from websocket stream");
            payload = await ReadPayload(socket, receiveBuffer, trace, cancellationToken);
            trace.LogDecision($"payload received; bytes={payload.Length}");
        }

        Console.Error.WriteLine("Uploading {0} bytes from {1}...", payload.Length, context.Request.RemoteEndPoint);

        uploadError = true;
        trace.LogDecision($"starting device upload at loader baudrate {LoaderBaudRate}");
        await LoadPayloadToDevice(payload, requestOptions.UserBaudRate, trace, cancellationToken);
        uploadError = false;

        uploadStamp = sw.Elapsed;
        trace.LogDecision($"upload finished after {uploadStamp.TotalMilliseconds:F3} ms");

        userCodeTimeout.CancelAfter(requestOptions.UserCodeTimeout);
        trace.LogDecision($"starting bidirectional bridge with user timeout {requestOptions.UserCodeTimeout.TotalMilliseconds:F0} ms");
        await BridgeSocketAndSerial(socket, trace, userCodeTimeout.Token);
        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.NormalClosure, string.Empty, trace, CancellationToken.None);

        exitReason = "completed";
    }
    catch (ProtocolViolationException ex)
    {
        trace.LogDecision($"protocol violation branch taken: {ex.Message}");
        if (socket is not null)
        {
            await CloseSocketIfNeeded(socket, WebSocketCloseStatus.ProtocolError, ex.Message, trace, CancellationToken.None);
        }

        // Do not rethrow here, this is an expected case.
        exitReason = $"protocol violation: {ex.Message}";
    }
    catch (OperationCanceledException) when (userCodeTimeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
    {
        Debug.Assert(!uploadError);
        trace.LogDecision("user timeout branch taken");
        if (socket is not null)
        {
            await CloseSocketIfNeeded(socket, WebSocketCloseStatus.PolicyViolation, "No time quota left for user code.", trace, CancellationToken.None);
        }

        // Do not rethrow here, this is an expected case.
        exitReason = "user timeout";
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        if (uploadError)
        {
            // The upload is under our control, and should not be exposed to the user as a timeout:
            trace.LogDecision("request timeout branch taken during upload");
            exitReason = "request timeout during upload";
            if (socket is not null)
            {
                await CloseSocketIfNeeded(socket, WebSocketCloseStatus.InternalServerError, "The server experienced an unexpected error.", trace, CancellationToken.None);
            }

            throw;
        }
        else
        {
            // Do not rethrow here, this is an expected case:
            trace.LogDecision("global timeout branch taken after upload");
            if (socket is not null)
            {
                await CloseSocketIfNeeded(socket, WebSocketCloseStatus.PolicyViolation, "No time quota left.", trace, CancellationToken.None);
            }

            exitReason = "global timeout";
        }
    }
    catch (WebSocketException ex)
    {
        trace.LogDecision($"websocket exception branch taken: {ex.Message}");
        Console.Error.WriteLine("WebSocket closed unexpectedly: {0}", ex.Message);
        // Do not rethrow here, this is an expected case.
        exitReason = $"web socket error: {ex.Message}";
    }
    catch (Exception ex)
    {
        trace.LogDecision($"unexpected exception branch taken: {ex.GetType().Name}: {ex.Message}");
        exitReason = $"unexpected exception: {ex.GetType().Name}";
        if (socket is not null)
        {
            await CloseSocketIfNeeded(socket, WebSocketCloseStatus.InternalServerError, "The server experienced an unexpected error.", trace, CancellationToken.None);
        }

        throw;
    }
    finally
    {
        var endTime = sw.Elapsed;
        trace.LogDecision(
            $"request finished; uploadMs={uploadStamp.TotalMilliseconds:F3}; executionMs={(endTime - uploadStamp).TotalMilliseconds:F3}; exitReason={exitReason}");

        Console.Error.WriteLine(
            "  {0} ms upload time, {1} ms execution time, {2}",
            uploadStamp.TotalMilliseconds,
            (endTime - uploadStamp).TotalMilliseconds,
            exitReason
        );

        if (!string.Equals(exitReason, "completed", StringComparison.Ordinal))
        {
            trace.PrintTo(Console.Error, exitReason);
        }

        socket?.Dispose();
    }
}

async Task<SerialPortProxy> OpenSerialPort(string portName)
{
    var port = new SerialPortProxy(
        portName,
        LoaderBaudRate,
        TimeSpan.FromMilliseconds(ResetAssertTimeMs),
        TimeSpan.FromMilliseconds(ResetReleaseSettleTimeMs));

    try
    {
        port.Open();

        using var probeTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(PropChkTimeoutMs));
        try
        {
            await port.ResetAsync(probeTimeout.Token);

            await port.WriteAsciiAsync("> ", probeTimeout.Token);
            await port.WriteAsciiAsync("Prop_Chk 0 0 0 0\r", probeTimeout.Token);
            _ = await port.ReadLineAsync(probeTimeout.Token);

            var version = (await port.ReadLineAsync(probeTimeout.Token)).Trim(' ', '\r', '\n');
            if (!string.Equals(version, ExpectedDeviceVersion, StringComparison.Ordinal))
            {
                throw new IOException($"Prop_Chk returned '{version}' instead of '{ExpectedDeviceVersion}'.");
            }
        }
        catch (OperationCanceledException ex) when (probeTimeout.IsCancellationRequested)
        {
            throw new TimeoutException("Prop_Chk timed out while opening the serial port.", ex);
        }

        return port;
    }
    catch
    {
        port.Dispose();
        throw;
    }
}

RequestOptions ParseAndValidateRequestOptions(HttpListenerRequest request)
{
    var baudRate = ParsePositiveIntParameter(request, "baudrate", DefaultUserBaudRate);
    var timeoutMs = ParsePositiveIntParameter(request, "timeout_ms", DefaultUserCodeTimeoutMs);
    var urlCodePayload = ParseCodeParameter(request);

    if (timeoutMs < MinUserCodeTimeoutMs || timeoutMs > MaxUserCodeTimeoutMs)
    {
        throw new BadHttpRequestException($"Query parameter 'timeout_ms' must be between {MinUserCodeTimeoutMs} and {MaxUserCodeTimeoutMs} milliseconds.");
    }

    EnsureBaudRateIsUsable(baudRate);

    return new RequestOptions(
        urlCodePayload is null ? UploadMode.WebSocket : UploadMode.UrlCode,
        urlCodePayload,
        baudRate,
        TimeSpan.FromMilliseconds(timeoutMs));
}

int ParsePositiveIntParameter(HttpListenerRequest request, string parameterName, int defaultValue)
{
    var rawValue = request.QueryString[parameterName];
    if (string.IsNullOrWhiteSpace(rawValue))
    {
        return defaultValue;
    }

    if (!int.TryParse(rawValue, out var parsedValue) || parsedValue <= 0)
    {
        throw new BadHttpRequestException($"Expected query parameter '{parameterName}' to be a positive integer, but received '{rawValue}'.");
    }

    return parsedValue;
}

byte[]? ParseCodeParameter(HttpListenerRequest request)
{
    var rawValues = request.QueryString.GetValues("code");
    if (rawValues is null)
    {
        return null;
    }

    if (rawValues.Length != 1)
    {
        throw new BadHttpRequestException("Query parameter 'code' must not appear more than once.");
    }

    byte[] payload;
    try
    {
        payload = DecodeCodeParameter(rawValues[0] ?? string.Empty);
    }
    catch (FormatException ex)
    {
        throw new BadHttpRequestException("Query parameter 'code' must be valid base64 or base64url data.", ex);
    }

    if (payload.Length > MaxPayloadSize)
    {
        throw new BadHttpRequestException($"Query parameter 'code' must decode to at most {MaxPayloadSize} bytes, but decoded to {payload.Length} bytes.");
    }

    if ((payload.Length % sizeof(uint)) != 0)
    {
        throw new BadHttpRequestException("Query parameter 'code' must decode to a payload whose length is divisible by 4 bytes.");
    }

    return payload;
}

byte[] DecodeCodeParameter(string rawValue)
{
    if (rawValue.Length == 0)
    {
        return Array.Empty<byte>();
    }

    var normalized = rawValue
        .Replace(' ', '+')
        .Replace('-', '+')
        .Replace('_', '/');

    return Convert.FromBase64String(PadBase64(normalized));
}

string PadBase64(string value)
{
    return (value.Length % 4) switch
    {
        0 => value,
        2 => value + "==",
        3 => value + "=",
        _ => throw new FormatException("Invalid base64 length."),
    };
}

void EnsureBaudRateIsUsable(int baudRate)
{
    var originalBaudRate = serialPort.BaudRate;
    try
    {
        serialPort.SetBaudRate(baudRate);
    }
    catch (Exception ex) when (ex is ArgumentOutOfRangeException or IOException or UnauthorizedAccessException)
    {
        throw new BadHttpRequestException($"Query parameter 'baudrate' is not usable on this serial port: {baudRate}.", ex);
    }
    finally
    {
        serialPort.SetBaudRate(originalBaudRate);
    }
}

async Task RejectBadRequest(HttpListenerResponse response, string message)
{
    response.StatusCode = (int)HttpStatusCode.BadRequest;
    response.ContentType = "text/plain; charset=utf-8";

    response.AddHeader("X-P2AAS-Error", message);

    var bytes = Encoding.UTF8.GetBytes(message);
    response.ContentLength64 = bytes.Length;
    await response.OutputStream.WriteAsync(bytes);
    response.Close();
}

async Task<ReadOnlyMemory<byte>> ReadPayload(WebSocket socket, byte[] receiveBuffer, ConnectionTrace trace, CancellationToken cancellationToken)
{
    trace.LogDecision("reading 4-byte payload length prefix from websocket stream");
    await ReadExactlyFromWebSocket(socket, receiveBuffer.AsMemory(0, sizeof(uint)), trace, "payload length prefix", cancellationToken);

    var payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(receiveBuffer.AsSpan(0, sizeof(uint)));
    trace.LogDecision($"parsed payload length prefix={payloadLength}");
    if (payloadLength == 0)
    {
        throw new ProtocolViolationException("Expected a non-empty payload.");
    }

    if (payloadLength > MaxPayloadSize)
    {
        throw new ProtocolViolationException($"Expected a maximum payload size of {MaxPayloadSize} bytes, but received {payloadLength}.");
    }

    trace.LogDecision($"reading payload body from websocket stream; bytes={payloadLength}");
    await ReadExactlyFromWebSocket(socket, receiveBuffer.AsMemory(0, (int)payloadLength), trace, "payload body", cancellationToken);
    trace.LogDecision($"payload body read completed; bytes={payloadLength}");

    return receiveBuffer.AsMemory(0, (int)payloadLength);
}

async Task LoadPayloadToDevice(ReadOnlyMemory<byte> payload, int userBaudRate, ConnectionTrace trace, CancellationToken cancellationToken)
{
    if ((payload.Length % sizeof(uint)) != 0)
    {
        throw new ProtocolViolationException("Payload length must be divisible by 4.");
    }

    trace.LogDecision($"payload length validated as word-aligned; bytes={payload.Length}");

    serialPort.SetBaudRate(LoaderBaudRate, trace);
    await serialPort.ResetAsync(cancellationToken, trace);

    await serialPort.WriteAsciiAsync("> ", cancellationToken, trace);

    var checksummedPayload = AppendChecksum(payload.Span);
    var encodedPayload = Convert.ToBase64String(checksummedPayload);
    trace.LogDecision($"payload checksum appended; checksummedBytes={checksummedPayload.Length}; base64Bytes={encodedPayload.Length}");

    await serialPort.WriteAsciiAsync("Prop_Txt 0 0 0 0 ", cancellationToken, trace);
    for (var offset = 0; offset < encodedPayload.Length; offset += SerialChunkSize)
    {
        var chunkLength = Math.Min(SerialChunkSize, encodedPayload.Length - offset);
        trace.LogDecision($"sending Prop_Txt chunk offset={offset} chunkBytes={chunkLength}");
        await serialPort.WriteAsciiAsync(encodedPayload.Substring(offset, chunkLength), cancellationToken, trace);

        if (offset + chunkLength < encodedPayload.Length)
        {
            await serialPort.WriteAsciiAsync("\r> ", cancellationToken, trace);
        }
    }

    await serialPort.WriteAsciiAsync(" ?\r", cancellationToken, trace);

    trace.LogDecision("waiting for Prop_Txt completion response byte");
    var response = await serialPort.ReadByteAsync(cancellationToken, trace);
    switch (response)
    {
        case (byte)'.':
            trace.LogDecision($"device accepted payload; switching to user baudrate {userBaudRate}");
            serialPort.SetBaudRate(userBaudRate, trace);
            return;
        case (byte)'!':
            trace.LogDecision("device reported checksum rejection");
            throw new ProtocolViolationException("Device rejected the payload checksum.");
        default:
            trace.LogDecision($"device returned unexpected Prop_Txt response 0x{response:X2}");
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

async Task BridgeSocketAndSerial(WebSocket socket, ConnectionTrace trace, CancellationToken cancellationToken)
{
    trace.LogDecision("starting socket<->serial relay tasks");
    using var relayCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    var socketToSerial = ForwardSocketToSerial(socket, trace, relayCancellation.Token);
    var serialToSocket = ForwardSerialToSocket(socket, trace, relayCancellation.Token);

    Exception? relayFailure = null;

    try
    {
        var completedRelay = await Task.WhenAny(socketToSerial, serialToSocket);
        trace.LogDecision(completedRelay == socketToSerial
            ? "socket->serial relay completed first"
            : "serial->socket relay completed first");
        await completedRelay;
    }
    catch (Exception ex)
    {
        relayFailure = ex;
        trace.LogDecision($"relay failure observed: {ex.GetType().Name}: {ex.Message}");
        throw;
    }
    finally
    {
        trace.LogDecision("canceling relay companion task");
        relayCancellation.Cancel();

        try
        {
            await Task.WhenAll(socketToSerial, serialToSocket);
        }
        catch (OperationCanceledException) when (relayCancellation.IsCancellationRequested && relayFailure is null)
        {
            trace.LogDecision("relay companion task canceled cleanly");
        }
        catch (Exception) when (relayFailure is not null)
        {
            trace.LogDecision("relay companion task fault suppressed because another relay already failed");
        }
    }
}

async Task ForwardSocketToSerial(WebSocket socket, ConnectionTrace trace, CancellationToken cancellationToken)
{
    var buffer = new byte[4096];
    while (socket.State == WebSocketState.Open)
    {
        var result = await socket.ReceiveAsync(buffer, cancellationToken);
        trace.LogWebSocketReceive("socket->serial", result, buffer.AsMemory(0, result.Count));
        if (result.MessageType == WebSocketMessageType.Close)
        {
            trace.LogDecision("socket->serial relay stopped because a close frame was received");
            return;
        }

        if (result.Count > 0)
        {
            await serialPort.WriteAsync(buffer.AsMemory(0, result.Count), cancellationToken, trace);
        }
    }

    trace.LogDecision($"socket->serial relay stopped because websocket state became {socket.State}");
}

async Task ForwardSerialToSocket(WebSocket socket, ConnectionTrace trace, CancellationToken cancellationToken)
{
    var buffer = new byte[4096];
    while (socket.State == WebSocketState.Open)
    {
        var bytesRead = await serialPort.ReadAsync(buffer, cancellationToken, trace);
        if (bytesRead == 0)
        {
            trace.LogDecision("serial->socket relay stopped because the serial port returned EOF");
            return;
        }

        trace.LogWebSocketSend("serial->socket", WebSocketMessageType.Binary, buffer.AsMemory(0, bytesRead), endOfMessage: true);
        await socket.SendAsync(buffer.AsMemory(0, bytesRead), WebSocketMessageType.Binary, true, cancellationToken);
    }

    trace.LogDecision($"serial->socket relay stopped because websocket state became {socket.State}");
}

async Task CloseSocketIfNeeded(WebSocket socket, WebSocketCloseStatus closeStatus, string statusDescription, ConnectionTrace trace, CancellationToken cancellationToken)
{
    if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
    {
        trace.LogDecision($"sending close frame status={closeStatus} description='{statusDescription}' from state={socket.State}");
        await socket.CloseAsync(closeStatus, statusDescription, cancellationToken);
        trace.LogDecision($"close frame sent; new websocket state={socket.State}");
    }
    else
    {
        trace.LogDecision($"close skipped because websocket state was {socket.State}");
    }
}

async Task ReadExactlyFromWebSocket(WebSocket socket, Memory<byte> buffer, ConnectionTrace trace, string operation, CancellationToken cancellationToken)
{
    var offset = 0;
    while (offset < buffer.Length)
    {
        var result = await socket.ReceiveAsync(buffer[offset..], cancellationToken);
        trace.LogWebSocketReceive(operation, result, buffer.Slice(offset, result.Count));
        if (result.MessageType == WebSocketMessageType.Close)
        {
            throw new ProtocolViolationException("Connection closed before the full payload was received.");
        }

        if (result.MessageType != WebSocketMessageType.Binary)
        {
            throw new ProtocolViolationException("Expected binary websocket messages.");
        }

        offset += result.Count;
    }

    trace.LogDecision($"completed websocket stream read for {operation}; bytes={buffer.Length}");
}

sealed class SerialPortProxy : IDisposable
{
    private readonly SerialPort port;
    private readonly TimeSpan resetAssertTime;
    private readonly TimeSpan resetReleaseSettleTime;

    public SerialPortProxy(string portName, int baudRate, TimeSpan resetAssertTime, TimeSpan resetReleaseSettleTime)
    {
        this.resetAssertTime = resetAssertTime;
        this.resetReleaseSettleTime = resetReleaseSettleTime;

        port = new SerialPort(portName)
        {
            BaudRate = baudRate,
            DataBits = 8,
            Parity = Parity.None,
            StopBits = StopBits.One,
            Handshake = Handshake.None,
            DtrEnable = false,
            RtsEnable = false,
        };
    }

    public int BaudRate => port.BaudRate;

    public void Open(ConnectionTrace? trace = null)
    {
        trace?.LogDecision($"opening serial port {port.PortName} at baudrate {port.BaudRate}");
        port.Open();
        trace?.LogDecision("serial port opened successfully");
    }

    public void SetBaudRate(int baudRate, ConnectionTrace? trace = null)
    {
        var previousBaudRate = port.BaudRate;
        port.BaudRate = baudRate;
        trace?.LogSerialState($"baudrate changed from {previousBaudRate} to {baudRate}");
    }

    public async Task ResetAsync(CancellationToken cancellationToken, ConnectionTrace? trace = null)
    {
        trace?.LogSerialState($"asserting reset via DTR for {resetAssertTime.TotalMilliseconds:F0} ms");
        port.DtrEnable = true;
        await Task.Delay(resetAssertTime, cancellationToken);

        // After reset has been held low for a while, clear the buffers
        // so we get a fresh restart:
        port.DiscardInBuffer();
        port.DiscardOutBuffer();
        trace?.LogSerialState("discarded serial input/output buffers while reset was asserted");

        port.DtrEnable = false;
        await Task.Delay(resetReleaseSettleTime, cancellationToken);
        trace?.LogSerialState($"released reset and waited {resetReleaseSettleTime.TotalMilliseconds:F0} ms for loader readiness");
    }

    public async Task<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken, ConnectionTrace? trace = null)
    {
        var bytesRead = await port.BaseStream.ReadAsync(buffer, cancellationToken);
        trace?.LogSerialRead("stream read", buffer[..bytesRead].Span);
        return bytesRead;
    }

    public async Task WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken, ConnectionTrace? trace = null)
    {
        trace?.LogSerialWrite("stream write", buffer.Span);
        await port.BaseStream.WriteAsync(buffer, cancellationToken);
        await port.BaseStream.FlushAsync(cancellationToken);
    }

    public async Task<byte> ReadByteAsync(CancellationToken cancellationToken, ConnectionTrace? trace = null)
    {
        var buffer = new byte[1];
        var bytesRead = await port.BaseStream.ReadAsync(buffer, cancellationToken);
        if (bytesRead == 0)
        {
            throw new EndOfStreamException("Serial port closed while waiting for device data.");
        }

        trace?.LogSerialRead("byte read", buffer);
        return buffer[0];
    }

    public async Task<string> ReadLineAsync(CancellationToken cancellationToken, ConnectionTrace? trace = null)
    {
        using var lineBuffer = new MemoryStream();
        while (true)
        {
            var value = await ReadByteAsync(cancellationToken);
            if (value == (byte)'\n')
            {
                var line = Encoding.ASCII.GetString(lineBuffer.GetBuffer(), 0, (int)lineBuffer.Length);
                trace?.LogSerialTextRead("line read", line);
                return line;
            }

            lineBuffer.WriteByte(value);
        }
    }

    public async Task WriteAsciiAsync(string value, CancellationToken cancellationToken, ConnectionTrace? trace = null)
    {
        trace?.LogSerialTextWrite("ascii write", value);
        await port.BaseStream.WriteAsync(Encoding.ASCII.GetBytes(value), cancellationToken);
        await port.BaseStream.FlushAsync(cancellationToken);
    }

    public void Dispose()
    {
        port.Dispose();
    }
}

sealed class ConnectionTrace
{
    private const int DataPreviewLength = 64;

    private readonly object sync = new();
    private readonly List<string> entries = [];
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();
    private readonly string requestLabel;

    public ConnectionTrace(HttpListenerRequest request, RequestOptions requestOptions)
    {
        requestLabel = $"{request.HttpMethod} {request.RawUrl} from {request.RemoteEndPoint}";
        LogDecision(
            $"request started; request={requestLabel}; uploadMode={requestOptions.UploadMode}; baudrate={requestOptions.UserBaudRate}; timeoutMs={requestOptions.UserCodeTimeout.TotalMilliseconds:F0}");
    }

    public void LogDecision(string message)
    {
        Add("decision", message);
    }

    public void LogWebSocketReceive(string operation, WebSocketReceiveResult result, ReadOnlyMemory<byte> payload)
    {
        LogWebSocketReceiveCore(
            operation,
            result.MessageType,
            result.Count,
            result.EndOfMessage,
            result.CloseStatus?.ToString() ?? "-",
            result.CloseStatusDescription ?? string.Empty,
            payload);
    }

    public void LogWebSocketReceive(string operation, ValueWebSocketReceiveResult result, ReadOnlyMemory<byte> payload)
    {
        LogWebSocketReceiveCore(
            operation,
            result.MessageType,
            result.Count,
            result.EndOfMessage,
            "-",
            string.Empty,
            payload);
    }

    private void LogWebSocketReceiveCore(
        string operation,
        WebSocketMessageType messageType,
        int count,
        bool endOfMessage,
        string closeStatus,
        string closeStatusDescription,
        ReadOnlyMemory<byte> payload)
    {
        Add(
            "ws recv",
            $"{operation}; type={messageType}; count={count}; end={endOfMessage}; closeStatus={closeStatus}; closeDescription={FormatText(closeStatusDescription)}; {FormatBytes(payload.Span)}");
    }

    public void LogWebSocketSend(string operation, WebSocketMessageType messageType, ReadOnlyMemory<byte> payload, bool endOfMessage)
    {
        Add(
            "ws send",
            $"{operation}; type={messageType}; count={payload.Length}; end={endOfMessage}; {FormatBytes(payload.Span)}");
    }

    public void LogSerialState(string message)
    {
        Add("serial", message);
    }

    public void LogSerialRead(string operation, ReadOnlySpan<byte> data)
    {
        Add("serial rx", $"{operation}; {FormatBytes(data)}");
    }

    public void LogSerialWrite(string operation, ReadOnlySpan<byte> data)
    {
        Add("serial tx", $"{operation}; {FormatBytes(data)}");
    }

    public void LogSerialTextRead(string operation, string value)
    {
        Add("serial rx", $"{operation}; text={FormatText(value)}; bytes={Encoding.ASCII.GetByteCount(value)}");
    }

    public void LogSerialTextWrite(string operation, string value)
    {
        Add("serial tx", $"{operation}; text={FormatText(value)}; bytes={Encoding.ASCII.GetByteCount(value)}");
    }

    public void PrintTo(TextWriter writer, string exitReason)
    {
        string[] snapshot;
        lock (sync)
        {
            snapshot = entries.ToArray();
        }

        writer.WriteLine($"Connection trace ({exitReason}) for {requestLabel}:");
        foreach (var entry in snapshot)
        {
            writer.WriteLine(entry);
        }
    }

    private void Add(string category, string message)
    {
        var entry = $"[{stopwatch.Elapsed.TotalMilliseconds,10:F3} ms] {category}: {message}";
        lock (sync)
        {
            entries.Add(entry);
        }
    }

    private static string FormatBytes(ReadOnlySpan<byte> data)
    {
        if (data.Length == 0)
        {
            return "bytes=0";
        }

        var previewLength = Math.Min(data.Length, DataPreviewLength);
        var preview = data[..previewLength];
        var suffix = data.Length > previewLength ? $"...(+{data.Length - previewLength} bytes)" : string.Empty;
        return $"bytes={data.Length}; hex={Convert.ToHexString(preview)}{suffix}; ascii={FormatAsciiPreview(preview, data.Length > previewLength)}";
    }

    private static string FormatAsciiPreview(ReadOnlySpan<byte> data, bool truncated)
    {
        var chars = new char[data.Length + (truncated ? 3 : 0)];
        for (var index = 0; index < data.Length; index++)
        {
            var value = data[index];
            chars[index] = value is >= 32 and <= 126 ? (char)value : '.';
        }

        if (truncated)
        {
            chars[^3] = '.';
            chars[^2] = '.';
            chars[^1] = '.';
        }

        return new string(chars);
    }

    private static string FormatText(string value)
    {
        return '"' + value
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\r", "\\r", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal)
            .Replace("\t", "\\t", StringComparison.Ordinal) + '"';
    }
}

enum UploadMode
{
    WebSocket,
    UrlCode,
}

readonly record struct RequestOptions(UploadMode UploadMode, byte[]? UrlCodePayload, int UserBaudRate, TimeSpan UserCodeTimeout);

sealed class BadHttpRequestException : Exception
{
    public BadHttpRequestException(string message)
        : base(message)
    {
    }

    public BadHttpRequestException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}