namespace Pz.Connector.EventHubs.Tests.Fakes;

/// <summary>In-memory stand-in for <see cref="IEventHubsClientFactory"/>: every hub is a list of
/// events per partition plus a captured list of sends, so a test can seed a read or assert a
/// write without a live namespace. Tasks 6 and 7 extend this with source/sink-specific scripting.</summary>
internal sealed class FakeClientFactory : IEventHubsClientFactory
{
    public Dictionary<string, FakeHub> Hubs { get; } = new(StringComparer.Ordinal);

    public Func<EventHubsConnectionConfig, string>? Probe { get; set; }

    /// <summary>The connection the last reader/writer was built from: what the factory receives is
    /// the only place a connection-level option (the consumer group, the transport) can be observed,
    /// since the SDK clients they configure are not built here.</summary>
    public EventHubsConnectionConfig? LastReaderConfig { get; private set; }

    public EventHubsConnectionConfig? LastWriterConfig { get; private set; }

    // The lookup is deferred to first use (not done here): a real reader's construction never
    // touches the network, so a fake standing in for it must not throw ResourceNotFound until a
    // caller actually asks the hub something, matching where that error surfaces in production.
    public IEventHubReader CreateReader(EventHubsConnectionConfig connection, string eventHub)
    {
        LastReaderConfig = connection;
        return new FakeReader(this, eventHub);
    }

    // Same deferral as CreateReader: a real producer client's constructor never contacts the
    // service either, so an unregistered hub must not surface until the fake writer is actually
    // asked to create a batch or send one.
    public IEventHubWriter CreateWriter(EventHubsConnectionConfig connection, string eventHub)
    {
        LastWriterConfig = connection;
        return new FakeWriter(this, eventHub);
    }

    public Task<string> ProbeAsync(EventHubsConnectionConfig connection, CancellationToken ct) =>
        Task.FromResult(Probe is null ? "ok" : Probe(connection));

    internal FakeHub Get(string eventHub) => Hubs.TryGetValue(eventHub, out var hub)
        ? hub
        : throw new Azure.Messaging.EventHubs.EventHubsException(eventHub, $"'{eventHub}' not found",
            Azure.Messaging.EventHubs.EventHubsException.FailureReason.ResourceNotFound);
}

/// <summary>One event hub's state: events landed per partition (in sequence order, as a real
/// partition would hold them) and events captured on send.</summary>
internal sealed class FakeHub
{
    /// <summary>Partition id -> events in sequence order. Beginning/last sequence numbers are
    /// derived from this list's first/last entry (empty list => -1/-1, matching the real SDK).</summary>
    public SortedDictionary<string, List<ReceivedEvent>> Partitions { get; } = new(StringComparer.Ordinal);

    public long MaxBatchBytes { get; set; } = 1_048_576;

    public List<(string? PartitionKey, List<OutgoingEvent> Events)> Sent { get; } = [];

    /// <summary>Hook: (partition, would-return) -> returns. Lets a test inject a short read, an
    /// idle gap, or a scripted failure without changing the stored events.</summary>
    public Func<string, IReadOnlyList<ReceivedEvent>, IReadOnlyList<ReceivedEvent>>? OnReceive { get; set; }

    public int ReceiveCalls;

    /// <summary>Partition readers opened via <see cref="FakeReader.OpenPartition"/> / disposed since.</summary>
    public int OpenedReaders;
    public int DisposedReaders;
}

internal sealed class FakeReader(FakeClientFactory factory, string eventHub) : IEventHubReader
{
    private FakeHub Hub => factory.Get(eventHub);

    public Task<IReadOnlyList<string>> GetPartitionIdsAsync(CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<string>>(Hub.Partitions.Keys.ToList());

    public Task<PartitionInfo> GetPartitionPropertiesAsync(string partitionId, CancellationToken ct)
    {
        var events = Events(partitionId);
        return Task.FromResult(events.Count == 0
            ? new PartitionInfo(partitionId, IsEmpty: true, BeginningSequenceNumber: -1, LastEnqueuedSequenceNumber: -1)
            : new PartitionInfo(partitionId, IsEmpty: false, events[0].SequenceNumber, events[^1].SequenceNumber));
    }

    public IPartitionReader OpenPartition(string partitionId, StartAt start)
    {
        var hub = Hub;
        hub.OpenedReaders++;
        return new FakePartitionReader(hub, partitionId, start);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private List<ReceivedEvent> Events(string partitionId) =>
        Hub.Partitions.TryGetValue(partitionId, out var list) ? list : [];
}

internal sealed class FakePartitionReader : IPartitionReader
{
    private readonly FakeHub _hub;
    private readonly string _partitionId;
    private int _index;

    public FakePartitionReader(FakeHub hub, string partitionId, StartAt start)
    {
        _hub = hub;
        _partitionId = partitionId;
        var events = Events();
        _index = start.SequenceNumber is { } sequence
            ? FirstIndexAtOrAfter(events, e => e.SequenceNumber >= sequence)
            : FirstIndexAtOrAfter(events, e => e.EnqueuedTime >= start.EnqueuedTime!.Value);
    }

    public Task<IReadOnlyList<ReceivedEvent>> ReceiveAsync(int maximumEventCount, TimeSpan maximumWaitTime, CancellationToken ct)
    {
        _hub.ReceiveCalls++;
        var events = Events();
        var slice = _index < events.Count ? events.Skip(_index).Take(maximumEventCount).ToList() : [];
        _index += slice.Count;
        IReadOnlyList<ReceivedEvent> result = slice;
        return Task.FromResult(_hub.OnReceive is { } hook ? hook(_partitionId, result) : result);
    }

    public ValueTask DisposeAsync()
    {
        _hub.DisposedReaders++;
        return ValueTask.CompletedTask;
    }

    private List<ReceivedEvent> Events() => _hub.Partitions.TryGetValue(_partitionId, out var list) ? list : [];

    private static int FirstIndexAtOrAfter(List<ReceivedEvent> events, Predicate<ReceivedEvent> match)
    {
        var index = events.FindIndex(match);
        return index < 0 ? events.Count : index;
    }
}

internal sealed class FakeWriter : IEventHubWriter
{
    private readonly Func<FakeHub> _hub;

    /// <summary>Direct construction against an already-resolved hub, for tests exercising the
    /// session's own logic without going through a factory.</summary>
    public FakeWriter(FakeHub hub) => _hub = () => hub;

    /// <summary>Deferred resolution, matching <see cref="FakeClientFactory.CreateWriter"/>: the hub
    /// is looked up on first use, not at construction, so an unregistered hub surfaces exactly where
    /// production does -- at the first batch, not at BeginWriteAsync.</summary>
    public FakeWriter(FakeClientFactory factory, string eventHub) => _hub = () => factory.Get(eventHub);

    /// <summary>When set, <see cref="SendAsync"/> throws this instead of recording the send.</summary>
    public Exception? FailSend { get; set; }

    public Task<IEventBatch> CreateBatchAsync(string? partitionKey, CancellationToken ct) =>
        Task.FromResult<IEventBatch>(new FakeBatch(partitionKey, _hub().MaxBatchBytes));

    public Task SendAsync(IEventBatch batch, CancellationToken ct)
    {
        if (FailSend is { } ex)
        {
            throw ex;
        }

        var fake = (FakeBatch)batch;
        _hub().Sent.Add((fake.PartitionKey, fake.Events));
        return Task.CompletedTask;
    }

    /// <summary>Times the real handle's dispose would be released -- a session must release it
    /// exactly once, even across a repeated DisposeAsync call.</summary>
    public int DisposeCalls { get; private set; }

    public ValueTask DisposeAsync()
    {
        DisposeCalls++;
        return ValueTask.CompletedTask;
    }
}

/// <summary>Approximates the real SDK's size accounting well enough to exercise batching logic:
/// each event costs its body plus a fixed per-event overhead plus its properties' string lengths.</summary>
internal sealed class FakeBatch(string? partitionKey, long maxBatchBytes) : IEventBatch
{
    private const int PerEventOverheadBytes = 64;

    private long _sizeInBytes;

    public string? PartitionKey { get; } = partitionKey;

    public List<OutgoingEvent> Events { get; } = [];

    /// <summary>The real SDK batch is a native handle: using one after Dispose is undefined. The
    /// fake throws instead, so a session that disposed a batch before it was done with it fails
    /// here rather than passing on a fake and misbehaving against the service.</summary>
    public bool Disposed { get; private set; }

    public int Count
    {
        get
        {
            ObjectDisposedException.ThrowIf(Disposed, this);
            return Events.Count;
        }
    }

    public long MaximumSizeInBytes => maxBatchBytes;

    public bool TryAdd(OutgoingEvent e)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        var size = e.Body.Length + PerEventOverheadBytes;
        foreach (var kv in e.Properties)
        {
            size += kv.Key.Length;
            if (kv.Value is string s)
            {
                size += s.Length;
            }
        }

        if (_sizeInBytes + size > maxBatchBytes)
        {
            return false;
        }

        _sizeInBytes += size;
        Events.Add(e);
        return true;
    }

    public void Dispose() => Disposed = true;
}
