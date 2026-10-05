// Compile the production server against this adapter; no production test hooks.
using System.Text;
using System.Threading.Channels;

namespace System.IO.Ports;

public enum Parity { None }
public enum StopBits { One }
public enum Handshake { None }

public sealed class SerialPort : IDisposable
{
    internal static readonly string Scenario = Environment.GetEnvironmentVariable("P2AAS_TEST_SCENARIO") ?? "healthy";
    private static int opens, uploads;
    private static bool validationFault, uploadFault, runtimeFault;
    private readonly FakeStream stream;
    private bool open, checkedOpen;
    private int baudRate;
    private bool validatedBaudRate;
    private int generation;

    internal static void Log(string value) => File.AppendAllText(
        Environment.GetEnvironmentVariable("P2AAS_TEST_EVENTS")!, value + "\n");

    public SerialPort(string portName) { PortName = portName; stream = new(this); }
    public string PortName { get; }
    public int DataBits { get; set; }
    public Parity Parity { get; set; }
    public StopBits StopBits { get; set; }
    public Handshake Handshake { get; set; }
    public bool RtsEnable { get; set; }
    public bool DtrEnable { get { RequireOpen(); return false; } set { if (generation > 0) RequireOpen(); } }
    public bool IsOpen
    {
        get
        {
            if (Scenario == "validation-closed" && generation == 1 && !checkedOpen)
            {
                checkedOpen = true;
                open = false;
            }
            return open;
        }
    }
    public int BaudRate
    {
        get => baudRate;
        set
        {
            if (value == 12345) throw new ArgumentOutOfRangeException(nameof(value));
            if (value == 12346 && open) throw new IOException("Unsupported baud rate");
            if (generation > 0) RequireOpen();
            if (generation == 1 && value == 115200 && Scenario.StartsWith("validation-") && !validationFault)
            {
                validationFault = true;
                open = false;
                throw new IOException("Validation device disconnected");
            }
            if (generation == 1 && Scenario == "restore-fail")
            {
                if (value == 115200) validatedBaudRate = true;
                else if (validatedBaudRate && !validationFault)
                {
                    validationFault = true;
                    throw new IOException("Failed to restore baud rate on healthy device");
                }
            }
            if (stream.Runtime && value == 115200 && Scenario == "switch-fail" && !uploadFault)
            {
                uploadFault = true;
                open = false;
                throw new IOException("Runtime baud switch lost the port");
            }
            baudRate = value;
        }
    }
    public Stream BaseStream { get { RequireOpen(); return stream; } }
    public void Open()
    {
        generation = ++opens;
        Log($"OPEN {generation}");
        if (generation > 1 && Scenario is "open-fail" or "validation-open-fail")
            throw new IOException("Cannot reopen adapter");
        open = true;
    }
    public void DiscardInBuffer() { RequireOpen(); stream.Clear(); }
    public void DiscardOutBuffer() { RequireOpen(); stream.ResetUpload(); }
    public void Dispose() { open = false; stream.Dispose(); Log($"DISPOSE {generation}"); }
    private void RequireOpen() { if (!open) throw new InvalidOperationException("Port is closed"); }

    private sealed class FakeStream(SerialPort owner) : Stream
    {
        private readonly Channel<byte> incoming = Channel.CreateUnbounded<byte>();
        private readonly StringBuilder encoded = new();
        private bool uploading, eof, disposed;
        public bool Runtime { get; private set; }
        public void Clear() { while (incoming.Reader.TryRead(out _)) { } }
        public void ResetUpload() { uploading = false; Runtime = false; encoded.Clear(); }
        private void Respond(string text) { foreach (var value in Encoding.ASCII.GetBytes(text)) incoming.Writer.TryWrite(value); }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            owner.RequireOpen();
            var text = Encoding.ASCII.GetString(buffer.Span);
            if (text.StartsWith("Prop_Chk"))
            {
                Log($"PROBE {owner.generation}");
                if (owner.generation > 1 && Scenario is "probe-timeout" or "deadline-probe") return;
                Respond(owner.generation > 1 && Scenario == "probe-fail" ? "\r\nWrong board\r\n" : "\r\nProp_Ver G\r\n");
            }
            else if (text.StartsWith("Prop_Txt"))
            {
                uploads++;
                uploading = true;
                encoded.Clear();
                Log($"ATTEMPT {uploads}");
                if (Scenario is "deadline" or "deadline-probe") await Task.Delay(9400, cancellationToken);
                if (Scenario == "upload-cancel") await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            else if (uploading && text == " ?\r")
            {
                uploading = false;
                Log("PAYLOAD " + encoded);
                if (Scenario == "upload-eof" && !uploadFault) { uploadFault = true; eof = true; }
                else if (Scenario == "checksum") Respond("!");
                else if (Scenario == "loader-response") Respond("x");
                else { Respond("."); Runtime = true; }
            }
            else if (uploading && text != "\r> ")
            {
                encoded.Append(text);
                if ((Scenario is "upload-once" or "upload-twice" or "open-fail" or "probe-fail" or "probe-timeout"
                    or "upload-closed" or "upload-disposed" or "deadline" or "deadline-probe" || Scenario == "validation-upload-fail")
                    && (!uploadFault || Scenario == "upload-twice"))
                {
                    uploadFault = true;
                    Log("PARTIAL " + encoded);
                    if (Scenario == "upload-once") await Task.Delay(400, cancellationToken);
                    if (Scenario == "upload-closed") { owner.open = false; throw new InvalidOperationException("Port closed"); }
                    if (Scenario == "upload-disposed") { owner.open = false; throw new ObjectDisposedException("serial"); }
                    throw new IOException("Upload device disconnected");
                }
            }
            else if (Runtime)
            {
                if (Scenario == "runtime-write" && !runtimeFault)
                {
                    runtimeFault = true;
                    throw new IOException("Runtime write failed");
                }
                Log("INPUT " + Convert.ToHexString(buffer.Span));
                foreach (var value in buffer.Span) incoming.Writer.TryWrite(value);
            }
        }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (disposed) throw new ObjectDisposedException("serial");
            if (eof) return 0;
            if (Runtime && incoming.Reader.Count == 0 && Scenario is "runtime-eof" or "runtime-io" && !runtimeFault)
            {
                runtimeFault = true;
                if (Scenario == "runtime-eof") return 0;
                throw new IOException("Runtime read failed");
            }
            var value = await incoming.Reader.ReadAsync(cancellationToken);
            buffer.Span[0] = value;
            return 1;
        }
        protected override void Dispose(bool disposing) { disposed = true; incoming.Writer.TryComplete(); base.Dispose(disposing); }
        public override Task FlushAsync(CancellationToken cancellationToken) { cancellationToken.ThrowIfCancellationRequested(); return Task.CompletedTask; }
        public override void Flush() { }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
