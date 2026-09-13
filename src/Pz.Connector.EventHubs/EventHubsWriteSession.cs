using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

/// <summary>One producer per session. Rows are grouped by partition-key value and packed into the
/// service's own size-checked batches; every send is awaited before WriteBatchAsync returns, so
/// nothing is in flight at commit and commit cannot fail for a reason the write did not already
/// report. Body, key and properties are built into fresh objects before the batch is handed over,
/// so nothing from the engine-owned Arrow batch outlives the call. An event that does not fit an
/// empty batch is refused naming the row: a size limit is not a transient condition.</summary>
internal sealed class EventHubsWriteSession : ISinkWriteSession
{
    private readonly IEventHubWriter _writer;
    private readonly EventHubsOutputConfig _output;
    private readonly string _context;
    private readonly EventHubsRedactor _redactor;
    private readonly ILogger _logger;
    private readonly RowJsonWriter? _json;
    private readonly int _bodyIndex;
    private readonly int _keyIndex;
    private readonly int[] _propertyIndexes;
    private readonly string? _contentType;
    private long _rows;
    private long _batches;
    private bool _committed;
    private bool _aborted;
    private bool _disposed;

    public EventHubsWriteSession(IEventHubWriter writer, EventHubsOutputConfig output, Schema schema, string namespaceHost,
        EventHubsRedactor redactor, ILogger logger)
    {
        _writer = writer;
        _output = output;
        _context = $"event hub '{output.EventHub}' in namespace '{namespaceHost}'";
        _redactor = redactor;
        _logger = logger;
        _json = output.BodyColumn is null ? new RowJsonWriter(schema, output.JsonColumnIndexes(schema)) : null;
        _bodyIndex = IndexOf(schema, output.BodyColumn);
        _keyIndex = IndexOf(schema, output.PartitionKeyColumn);
        _propertyIndexes = output.PropertyColumns.Select(p => IndexOf(schema, p)).ToArray();
        _contentType = output.ContentType ?? (output.BodyColumn is null ? "application/json" : null);
    }

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        ThrowIfFinished();
        // Insertion-ordered groups: the first row with a key fixes that group's position; rows keep
        // their order inside a group, which is the order the partition will hold them in.
        var order = new List<string?>();
        var groups = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        List<int>? nullKey = null;
        for (var row = 0; row < batch.Length; row++)
        {
            var key = _keyIndex >= 0 ? ArrowScalars.Format(batch.Column(_keyIndex), row) : null;
            if (key is null)
            {
                if (nullKey is null) { nullKey = []; order.Add(null); }
                nullKey.Add(row);
            }
            else
            {
                if (!groups.TryGetValue(key, out var rows)) { rows = []; groups[key] = rows; order.Add(key); }
                rows.Add(row);
            }
        }

        foreach (var key in order)
        {
            await SendGroupAsync(key, key is null ? nullKey! : groups[key], batch, ct).ConfigureAwait(false);
        }

        _batches++;
    }

    private async Task SendGroupAsync(string? key, List<int> rows, RecordBatch batch, CancellationToken ct)
    {
        var open = await CreateBatchAsync(key, ct).ConfigureAwait(false);
        try
        {
            foreach (var row in rows)
            {
                ct.ThrowIfCancellationRequested();
                var e = Build(batch, row, key);
                if (!open.TryAdd(e))
                {
                    if (open.Count == 0)
                    {
                        throw TooLarge(row, open.MaximumSizeInBytes);
                    }

                    await SendAsync(open, ct).ConfigureAwait(false);
                    // Create the replacement before disposing the sent one: if CreateBatchAsync
                    // throws, `open` is still the (already sent, not yet disposed) batch the
                    // `finally` below must clean up -- reassigning `open` only after this succeeds
                    // means a throw here can never leave `open` pointing at an already-disposed batch.
                    var next = await CreateBatchAsync(key, ct).ConfigureAwait(false);
                    open.Dispose();
                    open = next;
                    if (!open.TryAdd(e))
                    {
                        throw TooLarge(row, open.MaximumSizeInBytes);
                    }
                }

                _rows++;
            }

            if (open.Count > 0)
            {
                await SendAsync(open, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            open.Dispose();
        }
    }

    private OutgoingEvent Build(RecordBatch batch, int row, string? key)
    {
        var body = _bodyIndex >= 0 ? ArrowScalars.Bytes(batch.Column(_bodyIndex), row) ?? [] : _json!.Write(batch, row);
        var props = new List<KeyValuePair<string, object>>(_propertyIndexes.Length);
        for (var i = 0; i < _propertyIndexes.Length; i++)
        {
            if (ArrowScalars.PropertyValue(batch.Column(_propertyIndexes[i]), row) is { } value)
            {
                props.Add(new KeyValuePair<string, object>(_output.PropertyColumns[i], value));
            }
        }

        return new OutgoingEvent(body, _contentType, key, props);
    }

    private async Task<IEventBatch> CreateBatchAsync(string? key, CancellationToken ct)
    {
        try
        {
            return await _writer.CreateBatchAsync(key, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw Classify(ex, "creating a batch");
        }
    }

    private async Task SendAsync(IEventBatch open, CancellationToken ct)
    {
        try
        {
            await _writer.SendAsync(open, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw Classify(ex, "sending");
        }
    }

    private PzConnectorException TooLarge(int row, long maximumSizeInBytes) =>
        EventHubsErrors.Fatal(Codes.EventTooLarge,
            $"{_context}: row {row} does not fit an empty batch ({maximumSizeInBytes} bytes); " +
            "shrink the body or the properties, or name a smaller `body:` column", _redactor);

    private PzConnectorException Classify(Exception ex, string what) =>
        ex is Azure.Messaging.EventHubs.EventHubsException { Reason: Azure.Messaging.EventHubs.EventHubsException.FailureReason.ResourceNotFound }
            ? EventHubsErrors.Fatal(Codes.HubNotFound, $"{_context} does not exist; create it first", _redactor)
            : EventHubsErrors.Wrap(ex, _redactor, Codes.SendFailed, $"{_context}: {what}");

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        ThrowIfFinished();
        _committed = true;
        _logger.LogDebug("eventhubs: {Hub}: committed {Rows} rows in {Batches} batches", _output.EventHub, _rows, _batches);
        return ValueTask.FromResult(new WriteResult(_rows, _batches));
    }

    public ValueTask AbortAsync(CancellationToken ct)
    {
        if (_committed)
        {
            throw new InvalidOperationException("AbortAsync after CommitAsync is not allowed");
        }

        // Nothing to unsend: every batch handed to the service was awaited, which is what
        // AbortSemantics.None declares. The session only refuses further work.
        _aborted = true;
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _writer.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void ThrowIfFinished()
    {
        if (_committed) throw new InvalidOperationException("the session is already committed");
        if (_aborted) throw new InvalidOperationException("the session is aborted");
    }

    private static int IndexOf(Schema schema, string? column) =>
        column is null ? -1 : schema.FieldsList.ToList().FindIndex(f => f.Name == column);
}
