using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Messaging.EventHubs.Producer;

namespace Pz.Connector.EventHubs;

internal sealed record PartitionInfo(string PartitionId, bool IsEmpty, long BeginningSequenceNumber, long LastEnqueuedSequenceNumber);

internal sealed record ReceivedEvent(long SequenceNumber, string? Offset, DateTimeOffset EnqueuedTime, string? PartitionKey,
    byte[] Body, string? ContentType, IReadOnlyList<KeyValuePair<string, object?>> Properties);

internal sealed record OutgoingEvent(byte[] Body, string? ContentType, string? PartitionKey, IReadOnlyList<KeyValuePair<string, object>> Properties);

/// <summary>Where a partition read begins: an inclusive sequence number, or the first event enqueued
/// at or after a time. Exactly one is set.</summary>
internal readonly record struct StartAt(long? SequenceNumber, DateTimeOffset? EnqueuedTime)
{
    public static StartAt Sequence(long n) => new(n, null);
    public static StartAt Enqueued(DateTimeOffset t) => new(null, t);
}

internal interface IPartitionReader : IAsyncDisposable
{
    Task<IReadOnlyList<ReceivedEvent>> ReceiveAsync(int maximumEventCount, TimeSpan maximumWaitTime, CancellationToken ct);
}

internal interface IEventHubReader : IAsyncDisposable
{
    Task<IReadOnlyList<string>> GetPartitionIdsAsync(CancellationToken ct);
    Task<PartitionInfo> GetPartitionPropertiesAsync(string partitionId, CancellationToken ct);
    IPartitionReader OpenPartition(string partitionId, StartAt start);
}

internal interface IEventBatch : IDisposable
{
    int Count { get; }
    long MaximumSizeInBytes { get; }
    bool TryAdd(OutgoingEvent e);
}

internal interface IEventHubWriter : IAsyncDisposable
{
    Task<IEventBatch> CreateBatchAsync(string? partitionKey, CancellationToken ct);
    Task SendAsync(IEventBatch batch, CancellationToken ct);
}

/// <summary>The one place SDK clients are built, so the read loop and the write session can be
/// exercised with scripted fakes and so every client carries the same credential and transport.</summary>
internal interface IEventHubsClientFactory
{
    IEventHubReader CreateReader(EventHubsConnectionConfig connection, string eventHub);
    IEventHubWriter CreateWriter(EventHubsConnectionConfig connection, string eventHub);

    /// Namespace-level probe: parse-only for a connection string; a token acquisition for the Entra auths. Returns the success message.
    Task<string> ProbeAsync(EventHubsConnectionConfig connection, CancellationToken ct);
}

internal sealed class EventHubsClientFactory : IEventHubsClientFactory
{
    public static readonly EventHubsClientFactory Instance = new();

    private static readonly string[] TokenScopes = ["https://eventhubs.azure.net/.default"];

    public IEventHubReader CreateReader(EventHubsConnectionConfig connection, string eventHub) => new Reader(connection, eventHub);

    public IEventHubWriter CreateWriter(EventHubsConnectionConfig connection, string eventHub)
    {
        var options = new EventHubProducerClientOptions { ConnectionOptions = { TransportType = connection.Transport } };
        var client = connection.Auth == "connection_string"
            ? new EventHubProducerClient(connection.ConnectionString, eventHub, options)
            : new EventHubProducerClient(connection.Namespace, eventHub, CreateCredential(connection), options);
        return new Writer(client);
    }

    /// <summary>A namespace has no entity-less endpoint to ping: the connection-string probe is
    /// the parse the config already did, reported with the host; the Entra probes prove the
    /// credential can mint a token for Event Hubs, which is the part that fails in practice.</summary>
    public async Task<string> ProbeAsync(EventHubsConnectionConfig connection, CancellationToken ct)
    {
        if (connection.Auth == "connection_string")
        {
            return $"namespace {connection.NamespaceHost} (connection string parsed; reachability is verified per event hub at read/write time)";
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            await CreateCredential(connection).GetTokenAsync(new TokenRequestContext(TokenScopes), timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // The linked source fired, not the caller: a bare "the operation was canceled" would name
            // neither the namespace nor the deadline that produced it.
            throw EventHubsErrors.Fatal(Codes.InvalidConnection,
                $"namespace '{connection.NamespaceHost}': no token for {TokenScopes[0]} within 10s; " +
                "check the credential and the network path", connection.Redactor);
        }

        return $"namespace {connection.NamespaceHost} (token acquired for {connection.Auth})";
    }

    internal static TokenCredential CreateCredential(EventHubsConnectionConfig connection) => connection.Auth switch
    {
        "service_principal" => new ClientSecretCredential(connection.TenantId, connection.ClientId, connection.ClientSecret),
        "credential_chain" => new DefaultAzureCredential(),
        "managed_identity" => new ManagedIdentityCredential(string.IsNullOrEmpty(connection.ClientId)
            ? ManagedIdentityId.SystemAssigned
            : ManagedIdentityId.FromUserAssignedClientId(connection.ClientId)),
        _ => throw new InvalidOperationException($"auth '{connection.Auth}' has no credential"),
    };

    private sealed class Reader : IEventHubReader
    {
        private readonly EventHubsConnectionConfig _connection;
        private readonly string _eventHub;
        private readonly TokenCredential? _credential;
        private readonly EventHubConsumerClient _client;

        public Reader(EventHubsConnectionConfig connection, string eventHub)
        {
            _connection = connection;
            _eventHub = eventHub;
            // Built once and reused by every PartitionReceiver this reader opens: a reader that
            // opened N partitions and rebuilt the credential each time would mint N+1 credentials
            // for the same identity, for no benefit.
            _credential = connection.Auth == "connection_string" ? null : CreateCredential(connection);
            var options = new EventHubConsumerClientOptions { ConnectionOptions = { TransportType = connection.Transport } };
            _client = _credential is null
                ? new EventHubConsumerClient(connection.ConsumerGroup, connection.ConnectionString, eventHub, options)
                : new EventHubConsumerClient(connection.ConsumerGroup, connection.Namespace, eventHub, _credential, options);
        }

        public async Task<IReadOnlyList<string>> GetPartitionIdsAsync(CancellationToken ct) =>
            await _client.GetPartitionIdsAsync(ct).ConfigureAwait(false);

        public async Task<PartitionInfo> GetPartitionPropertiesAsync(string partitionId, CancellationToken ct)
        {
            var p = await _client.GetPartitionPropertiesAsync(partitionId, ct).ConfigureAwait(false);
            return new PartitionInfo(p.Id, p.IsEmpty, p.BeginningSequenceNumber, p.LastEnqueuedSequenceNumber);
        }

        public IPartitionReader OpenPartition(string partitionId, StartAt start)
        {
            var position = start.SequenceNumber is { } n
                ? EventPosition.FromSequenceNumber(n, isInclusive: true)
                : EventPosition.FromEnqueuedTime(start.EnqueuedTime!.Value);
            var options = new PartitionReceiverOptions
            {
                ConnectionOptions = { TransportType = _connection.Transport },
                TrackLastEnqueuedEventProperties = false,
            };
            var receiver = _credential is null
                ? new PartitionReceiver(_connection.ConsumerGroup, partitionId, position, _connection.ConnectionString, _eventHub, options)
                : new PartitionReceiver(_connection.ConsumerGroup, partitionId, position, _connection.Namespace, _eventHub, _credential, options);
            return new PartitionReaderImpl(receiver);
        }

        public ValueTask DisposeAsync() => _client.DisposeAsync();
    }

    private sealed class PartitionReaderImpl(PartitionReceiver receiver) : IPartitionReader
    {
        public async Task<IReadOnlyList<ReceivedEvent>> ReceiveAsync(int maximumEventCount, TimeSpan maximumWaitTime, CancellationToken ct)
        {
            var events = await receiver.ReceiveBatchAsync(maximumEventCount, maximumWaitTime, ct).ConfigureAwait(false);
            var list = new List<ReceivedEvent>();
            foreach (var e in events)
            {
                var props = new List<KeyValuePair<string, object?>>(e.Properties.Count);
                foreach (var kv in e.Properties)
                {
                    props.Add(new KeyValuePair<string, object?>(kv.Key, kv.Value));
                }

                list.Add(new ReceivedEvent(e.SequenceNumber, e.OffsetString, e.EnqueuedTime, e.PartitionKey,
                    e.EventBody.ToArray(), e.ContentType, props));
            }

            return list;
        }

        public ValueTask DisposeAsync() => receiver.DisposeAsync();
    }

    private sealed class Writer(EventHubProducerClient client) : IEventHubWriter
    {
        public async Task<IEventBatch> CreateBatchAsync(string? partitionKey, CancellationToken ct) =>
            new Batch(await client.CreateBatchAsync(new CreateBatchOptions { PartitionKey = partitionKey }, ct).ConfigureAwait(false));

        public Task SendAsync(IEventBatch batch, CancellationToken ct) => client.SendAsync(((Batch)batch).Inner, ct);

        public ValueTask DisposeAsync() => client.DisposeAsync();
    }

    private sealed class Batch(EventDataBatch inner) : IEventBatch
    {
        public EventDataBatch Inner => inner;
        public int Count => inner.Count;
        public long MaximumSizeInBytes => inner.MaximumSizeInBytes;

        public bool TryAdd(OutgoingEvent e)
        {
            var data = new EventData(new BinaryData(e.Body)) { ContentType = e.ContentType };
            foreach (var kv in e.Properties)
            {
                data.Properties[kv.Key] = kv.Value;
            }

            return inner.TryAdd(data);
        }

        public void Dispose() => inner.Dispose();
    }
}
