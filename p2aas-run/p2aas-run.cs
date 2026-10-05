using System.Buffers.Binary;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text;

const int MaxPayloadSize = 512 * 1024;
const string Help = """
    Usage: p2aas-run [-h] [-v] [-s <url>] [-b <baud>] [-t <ms>] [-i|--no-interactive] [-I <text>] <binary>

    Uploads a Propeller 2 firmware to P2AAS and relays its serial output.

      -h, --help                 Print this help
      -v, --verbose              Print debug output to stderr
      -s, --server <url>         WebSocket endpoint (default: P2AAS_ENDPOINT)
      -b, --baudrate <baud>      Runtime serial baud rate (default: server setting)
      -t, --timeout <ms>         Connect and runtime timeout, 100..10000 ms
      -I, --input <text>         Send UTF-8 text before stdin data
      -i, --interactive          Forward input one character at a time
          --no-interactive       Forward terminal lines, or piped stdin as one chunk

    Input is interactive by default when stdin is a terminal.
    """;

try
{
    var server = Environment.GetEnvironmentVariable("P2AAS_ENDPOINT");
    string? binary = null;
    string? initialInput = null;
    int? baudrate = null;
    int? timeout = null;
    bool? interactiveOption = null;
    var verbose = false;

    for (var i = 0; i < args.Length; i++)
    {
        string Value(string option) => ++i < args.Length ? args[i] : throw new ArgumentException($"{option} requires a value");

        switch (args[i])
        {
            case "-h" or "--help":
                Console.WriteLine(Help);
                return;
            case "-v" or "--verbose": verbose = true; break;
            case "-s" or "--server": server = Value(args[i]); break;
            case "-b" or "--baudrate": baudrate = ParseNumber(Value(args[i]), "baudrate", 1, int.MaxValue); break;
            case "-t" or "--timeout": timeout = ParseNumber(Value(args[i]), "timeout", 100, 10_000); break;
            case "-I" or "--input": initialInput = Value(args[i]); break;
            case "-i" or "--interactive": interactiveOption = true; break;
            case "--no-interactive": interactiveOption = false; break;
            default:
                if (args[i].StartsWith('-')) throw new ArgumentException($"Unknown option: {args[i]}");
                if (binary is not null) throw new ArgumentException("Exactly one firmware binary is required");
                binary = args[i];
                break;
        }
    }

    if (binary is null) throw new ArgumentException("A firmware binary is required");
    if (string.IsNullOrWhiteSpace(server)) throw new ArgumentException("Set P2AAS_ENDPOINT or pass --server");
    if (!Uri.TryCreate(server, UriKind.Absolute, out var endpoint) || endpoint.Scheme is not ("ws" or "wss"))
        throw new ArgumentException("Server must be an absolute ws:// or wss:// URL");

    var firmware = await File.ReadAllBytesAsync(binary);
    if (firmware.Length is 0 or > MaxPayloadSize || firmware.Length % 4 != 0)
        throw new ArgumentException($"Firmware must be nonempty, at most {MaxPayloadSize} bytes, and divisible by 4");

    var query = new List<string>();
    foreach (var part in endpoint.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
    {
        var name = Uri.UnescapeDataString(part.Split('=', 2)[0]);
        if (name.Equals("code", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("The server URL must not contain a code query parameter in WebSocket upload mode");
        if (baudrate is not null && name.Equals("baudrate", StringComparison.OrdinalIgnoreCase) ||
            timeout is not null && name.Equals("timeout_ms", StringComparison.OrdinalIgnoreCase))
            continue;
        query.Add(part);
    }
    if (baudrate is not null) query.Add($"baudrate={baudrate}");
    if (timeout is not null) query.Add($"timeout_ms={timeout}");
    var url = new UriBuilder(endpoint);
    url.Query = string.Join('&', query);

    using var socket = new ClientWebSocket();
    Log($"Connecting to {url.Uri}");
    using var connectCancellation = timeout is null ? new CancellationTokenSource() : new CancellationTokenSource(timeout.Value);
    try
    {
        await socket.ConnectAsync(url.Uri, connectCancellation.Token);
    }
    catch (OperationCanceledException) when (connectCancellation.IsCancellationRequested)
    {
        throw new TimeoutException($"Connection timed out after {timeout} ms");
    }

    var upload = new byte[sizeof(uint) + firmware.Length];
    BinaryPrimitives.WriteUInt32LittleEndian(upload, (uint)firmware.Length);
    firmware.CopyTo(upload, sizeof(uint));
    await socket.SendAsync(upload, WebSocketMessageType.Binary, true, CancellationToken.None);
    Log($"Uploaded {firmware.Length} firmware bytes");

    if (initialInput is not null)
    {
        await socket.SendAsync(Encoding.UTF8.GetBytes(initialInput), WebSocketMessageType.Binary, true, CancellationToken.None);
        Log($"Sent {Encoding.UTF8.GetByteCount(initialInput)} initial input bytes");
    }

    using var inputCancellation = new CancellationTokenSource();
    var interactive = interactiveOption ?? !Console.IsInputRedirected;
    using var rawTerminal = interactive && !Console.IsInputRedirected && !OperatingSystem.IsWindows()
        ? new RawTerminal() : null;
    var sender = ForwardInput(socket, interactive, inputCancellation.Token);
    var receiver = ReceiveOutput(socket);
    try
    {
        if (await Task.WhenAny(sender, receiver) == sender)
            await sender;
        await receiver;
    }
    finally
    {
        inputCancellation.Cancel();
    }

    void Log(string message)
    {
        if (verbose) Console.Error.WriteLine(message);
    }
}
catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException or WebSocketException or OperationCanceledException or TimeoutException)
{
    Console.Error.WriteLine($"p2aas-run: {ex.Message}");
    Environment.ExitCode = 1;
}

static int ParseNumber(string value, string name, int min, int max)
{
    if (!int.TryParse(value, out var number) || number < min || number > max)
        throw new ArgumentException($"{name} must be between {min} and {max}");
    return number;
}

static async Task ForwardInput(ClientWebSocket socket, bool interactive, CancellationToken cancellationToken)
{
    if (interactive && !Console.IsInputRedirected && OperatingSystem.IsWindows())
    {
        await Task.Run(async () =>
        {
            var previous = Console.TreatControlCAsInput;
            Console.TreatControlCAsInput = true;
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var key = Console.ReadKey(intercept: true);
                    if (key.KeyChar == '\0') continue;
                    var bytes = Encoding.UTF8.GetBytes([key.KeyChar]);
                    await socket.SendAsync(bytes, WebSocketMessageType.Binary, true, cancellationToken);
                }
            }
            finally
            {
                Console.TreatControlCAsInput = previous;
            }
        }, cancellationToken);
    }
    else if (interactive)
    {
        using var input = !Console.IsInputRedirected && !OperatingSystem.IsWindows()
            ? File.OpenRead("/dev/stdin") : Console.OpenStandardInput();
        var byteBuffer = new byte[1];
        while (await input.ReadAsync(byteBuffer, cancellationToken) != 0)
            await socket.SendAsync(byteBuffer, WebSocketMessageType.Binary, true, cancellationToken);
    }
    else if (!Console.IsInputRedirected)
    {
        string? line;
        while ((line = await Task.Run(Console.ReadLine, cancellationToken)) is not null)
            await socket.SendAsync(Encoding.UTF8.GetBytes(line + "\n"), WebSocketMessageType.Binary, true, cancellationToken);
    }
    else
    {
        using var data = new MemoryStream();
        await Console.OpenStandardInput().CopyToAsync(data, cancellationToken);
        if (data.Length > 0)
            await socket.SendAsync(data.ToArray(), WebSocketMessageType.Binary, true, cancellationToken);
    }
}

static async Task ReceiveOutput(ClientWebSocket socket)
{
    var buffer = new byte[4096];
    var output = Console.OpenStandardOutput();
    while (true)
    {
        var result = await socket.ReceiveAsync(buffer, CancellationToken.None);
        if (result.MessageType == WebSocketMessageType.Close)
        {
            await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
            if (result.CloseStatus == WebSocketCloseStatus.NormalClosure ||
                result.CloseStatus == WebSocketCloseStatus.PolicyViolation &&
                result.CloseStatusDescription is "No time quota left for user code." or "No time quota left.")
                return;
            throw new WebSocketException($"Server closed connection: {result.CloseStatus} ({result.CloseStatusDescription})");
        }
        await output.WriteAsync(buffer.AsMemory(0, result.Count));
        await output.FlushAsync();
    }
}

sealed class RawTerminal : IDisposable
{
    private readonly string originalSettings;

    public RawTerminal()
    {
        originalSettings = RunStty("-g").Trim();
        RunStty("raw", "-echo");
    }

    public void Dispose() => RunStty(originalSettings);

    private static string RunStty(params string[] arguments)
    {
        var start = new ProcessStartInfo("stty") { RedirectStandardOutput = true };
        foreach (var argument in arguments) start.ArgumentList.Add(argument);
        using var process = Process.Start(start) ?? throw new IOException("Could not start stty");
        var output = process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0) throw new IOException("Could not configure the terminal with stty");
        return output;
    }
}
