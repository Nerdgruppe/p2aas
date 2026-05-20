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
    var webSocketContext = await context.AcceptWebSocketAsync(null);
    using var socket = webSocketContext.WebSocket;
    using var userCodeTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);



    var uploadError = false;
    try
    {
        var payload = await ReadPayload(socket, receiveBuffer, cancellationToken);

        uploadError = true;
        await LoadPayloadToDevice(payload, requestOptions.UserBaudRate, cancellationToken);
        uploadError = false;

        userCodeTimeout.CancelAfter(requestOptions.UserCodeTimeout);
        await BridgeSocketAndSerial(socket, userCodeTimeout.Token);
        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None);
    }
    catch (ProtocolViolationException ex)
    {
        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.ProtocolError, ex.Message, CancellationToken.None);
        // Do not rethrow here, this is an expected case.
    }
    catch (OperationCanceledException) when (userCodeTimeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
    {
        Debug.Assert(!uploadError);
        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.PolicyViolation, "No time quota left for user code.", CancellationToken.None);
        // Do not rethrow here, this is an expected case.
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        if (uploadError)
        {
            // The upload is under our control, and should not be exposed to the user as a timeout:
            await CloseSocketIfNeeded(socket, WebSocketCloseStatus.InternalServerError, "The server experienced an unexpected error.", CancellationToken.None);
            throw;
        }
        else
        {
            // Do not rethrow here, this is an expected case:
            await CloseSocketIfNeeded(socket, WebSocketCloseStatus.PolicyViolation, "No time quota left.", CancellationToken.None);
        }
    }
    catch (WebSocketException ex)
    {
        Console.Error.WriteLine("WebSocket closed unexpectedly: {0}", ex.Message);
        // Do not rethrow here, this is an expected case.
    }
    catch (Exception)
    {
        await CloseSocketIfNeeded(socket, WebSocketCloseStatus.InternalServerError, "The server experienced an unexpected error.", CancellationToken.None);
        throw;
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

    if (timeoutMs < MinUserCodeTimeoutMs || timeoutMs > MaxUserCodeTimeoutMs)
    {
        throw new BadHttpRequestException($"Query parameter 'timeout_ms' must be between {MinUserCodeTimeoutMs} and {MaxUserCodeTimeoutMs} milliseconds.");
    }

    EnsureBaudRateIsUsable(baudRate);

    return new RequestOptions(baudRate, TimeSpan.FromMilliseconds(timeoutMs));
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

void EnsureBaudRateIsUsable(int baudRate)
{
    var originalBaudRate = serialPort.BaudRate;
    try
    {
        serialPort.BaudRate = baudRate;
    }
    catch (Exception ex) when (ex is ArgumentOutOfRangeException or IOException or UnauthorizedAccessException)
    {
        throw new BadHttpRequestException($"Query parameter 'baudrate' is not usable on this serial port: {baudRate}.", ex);
    }
    finally
    {
        serialPort.BaudRate = originalBaudRate;
    }
}

async Task RejectBadRequest(HttpListenerResponse response, string message)
{
    response.StatusCode = (int)HttpStatusCode.BadRequest;
    response.ContentType = "text/plain; charset=utf-8";

    var bytes = Encoding.UTF8.GetBytes(message);
    response.ContentLength64 = bytes.Length;
    await response.OutputStream.WriteAsync(bytes);
    response.Close();
}

async Task<ReadOnlyMemory<byte>> ReadPayload(WebSocket socket, byte[] receiveBuffer, CancellationToken cancellationToken)
{
    var initialMessage = await ReadMessage(socket, receiveBuffer, cancellationToken);
    if (initialMessage.Length < sizeof(uint))
    {
        throw new ProtocolViolationException($"Expected at least a {sizeof(uint)} byte length prefix, but received {initialMessage.Length} bytes.");
    }

    var payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(initialMessage.Span[..sizeof(uint)]);
    if (payloadLength == 0)
    {
        throw new ProtocolViolationException("Expected a non-empty payload.");
    }

    if (payloadLength > MaxPayloadSize)
    {
        throw new ProtocolViolationException($"Expected a maximum payload size of {MaxPayloadSize} bytes, but received {payloadLength}.");
    }

    if (initialMessage.Length == sizeof(uint))
    {
        return await ReadMessage(socket, new byte[(int)payloadLength], cancellationToken);
    }

    var actualPayloadLength = initialMessage.Length - sizeof(uint);
    if (actualPayloadLength != payloadLength)
    {
        throw new ProtocolViolationException($"Expected {payloadLength} payload bytes, but received {actualPayloadLength} bytes.");
    }

    return initialMessage[sizeof(uint)..];
}

async Task LoadPayloadToDevice(ReadOnlyMemory<byte> payload, int userBaudRate, CancellationToken cancellationToken)
{
    if ((payload.Length % sizeof(uint)) != 0)
    {
        throw new ProtocolViolationException("Payload length must be divisible by 4.");
    }

    serialPort.BaudRate = LoaderBaudRate;
    await serialPort.ResetAsync(cancellationToken);

    await serialPort.WriteAsciiAsync("> ", cancellationToken);

    var checksummedPayload = AppendChecksum(payload.Span);
    var encodedPayload = Convert.ToBase64String(checksummedPayload);

    await serialPort.WriteAsciiAsync("Prop_Txt 0 0 0 0 ", cancellationToken);
    for (var offset = 0; offset < encodedPayload.Length; offset += SerialChunkSize)
    {
        var chunkLength = Math.Min(SerialChunkSize, encodedPayload.Length - offset);
        await serialPort.WriteAsciiAsync(encodedPayload.Substring(offset, chunkLength), cancellationToken);

        if (offset + chunkLength < encodedPayload.Length)
        {
            await serialPort.WriteAsciiAsync("\r> ", cancellationToken);
        }
    }

    await serialPort.WriteAsciiAsync(" ?\r", cancellationToken);

    var response = await serialPort.ReadByteAsync(cancellationToken);
    switch (response)
    {
        case (byte)'.':
            serialPort.BaudRate = userBaudRate;
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

    Exception? relayFailure = null;

    try
    {
        var completedRelay = await Task.WhenAny(socketToSerial, serialToSocket);
        await completedRelay;
    }
    catch (Exception ex)
    {
        relayFailure = ex;
        throw;
    }
    finally
    {
        relayCancellation.Cancel();

        try
        {
            await Task.WhenAll(socketToSerial, serialToSocket);
        }
        catch (OperationCanceledException) when (relayCancellation.IsCancellationRequested && relayFailure is null)
        {
        }
        catch (Exception) when (relayFailure is not null)
        {
        }
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
            await serialPort.WriteAsync(buffer.AsMemory(0, result.Count), cancellationToken);
        }
    }
}

async Task ForwardSerialToSocket(WebSocket socket, CancellationToken cancellationToken)
{
    var buffer = new byte[4096];
    while (socket.State == WebSocketState.Open)
    {
        var bytesRead = await serialPort.ReadAsync(buffer, cancellationToken);
        if (bytesRead == 0)
        {
            return;
        }

        await socket.SendAsync(buffer.AsMemory(0, bytesRead), WebSocketMessageType.Binary, true, cancellationToken);
    }
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
        var result = await socket.ReceiveAsync(buffer.AsMemory(offset), cancellationToken);
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

    public int BaudRate
    {
        get => port.BaudRate;
        set => port.BaudRate = value;
    }

    public void Open()
    {
        port.Open();
    }

    public async Task ResetAsync(CancellationToken cancellationToken)
    {
        port.DtrEnable = true;
        await Task.Delay(resetAssertTime, cancellationToken);

        // After reset has been held low for a while, clear the buffers
        // so we get a fresh restart:
        port.DiscardInBuffer();
        port.DiscardOutBuffer();

        port.DtrEnable = false;
        await Task.Delay(resetReleaseSettleTime, cancellationToken);
    }

    public async Task<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        return await port.BaseStream.ReadAsync(buffer, cancellationToken);
    }

    public async Task WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
    {
        await port.BaseStream.WriteAsync(buffer, cancellationToken);
        await port.BaseStream.FlushAsync(cancellationToken);
    }

    public async Task<byte> ReadByteAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[1];
        var bytesRead = await ReadAsync(buffer, cancellationToken);
        if (bytesRead == 0)
        {
            throw new EndOfStreamException("Serial port closed while waiting for device data.");
        }

        return buffer[0];
    }

    public async Task<string> ReadLineAsync(CancellationToken cancellationToken)
    {
        using var lineBuffer = new MemoryStream();
        while (true)
        {
            var value = await ReadByteAsync(cancellationToken);
            if (value == (byte)'\n')
            {
                return Encoding.ASCII.GetString(lineBuffer.GetBuffer(), 0, (int)lineBuffer.Length);
            }

            lineBuffer.WriteByte(value);
        }
    }

    public Task WriteAsciiAsync(string value, CancellationToken cancellationToken)
    {
        return WriteAsync(Encoding.ASCII.GetBytes(value), cancellationToken);
    }

    public void Dispose()
    {
        port.Dispose();
    }
}

readonly record struct RequestOptions(int UserBaudRate, TimeSpan UserCodeTimeout);

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