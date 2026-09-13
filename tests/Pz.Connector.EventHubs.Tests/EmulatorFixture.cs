using System.Globalization;
using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Testcontainers.EventHubs;

namespace Pz.Connector.EventHubs.Tests;

[CollectionDefinition("eventhubs")]
public sealed class EventHubsCollection : ICollectionFixture<EmulatorFixture>;

/// <summary>One emulator (plus the Azurite it needs) per test run. The emulator cannot create
/// entities after it starts, so every hub is declared up front; a fact that needs a hub to itself
/// leases one, and the rest share a hub and read past everything already in it from a position
/// <see cref="CaptureNextAsync"/> took before they seeded. Helpers talk to the emulator with the
/// SDK's own clients, deliberately not through the connector: a fact that used the code under test
/// to seed and verify would prove nothing.</summary>
public sealed class EmulatorFixture : IAsyncLifetime
{
    public const string SmallHub = "small";
    public const string LargeHub = "large";

    /// <summary>The emulator namespace admits at most ten entities in total, and the configuration
    /// builder refuses an eleventh before a container is ever started -- so the two named hubs above
    /// plus this pool is the entire budget, and a lease is spent only by a fact that actually writes.</summary>
    public const int PoolSize = 8;

    /// <summary>Consecutive MaximumWaitTime expiries that end a partition read.</summary>
    private const int SilencesBeforeDrained = 3;

    private EventHubsContainer? _container;
    private int _leased;

    public string ConnectionString { get; private set; } = "";

    /// <summary>The namespace host the connector derives from the same connection string, which is
    /// what a sync-state token carries.</summary>
    public string NamespaceHost => EventHubsConnectionStringProperties.Parse(ConnectionString).Endpoint?.Host ?? "";

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        // $Default exists in every namespace the emulator starts and cannot be declared again, so no
        // consumer group is named here -- the connector reads through $Default.
        var configuration = EventHubsServiceConfiguration.Create()
            .WithEntity(SmallHub, 2)
            .WithEntity(LargeHub, 1);
        for (var i = 0; i < PoolSize; i++)
        {
            configuration = configuration.WithEntity(PoolName(i), 2);
        }

        // Built here rather than in a field initializer: Build() resolves and pings the docker
        // endpoint, so a constructor that built it would throw before the probe above could no-op --
        // and a collection fixture that throws is a failed fixture, not a skip. The image is a
        // constructor argument because the parameterless builder is retired in Testcontainers 4.15.
        // The module brings its own Azurite, already serving blob, queue and table: the emulator's
        // MetadataStore health check fails against a blob-only one and the container exits.
        _container = new EventHubsBuilder("mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest")
            .WithAcceptLicenseAgreement(true)
            .WithConfigurationBuilder(configuration)
            .Build();
        await _container.StartAsync().ConfigureAwait(false);
        ConnectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public static string PoolName(int index) => $"pool-{index:00}";

    /// <summary>The next unused hub from the pre-declared pool. Leases are handed out for the life of
    /// the run and never returned: an event hub cannot be emptied, so a reused hub would hand the next
    /// fact the previous one's events. The remedy for exhaustion is not a bigger pool -- the
    /// ten-entity ceiling is already reached -- but a suite that shares one hub: capture the hub's
    /// position with <see cref="CaptureNextAsync"/> and read back with <see cref="ReadSinceAsync"/>,
    /// which confines every fact to its own events on a single lease.</summary>
    public string LeaseHub()
    {
        var index = Interlocked.Increment(ref _leased) - 1;
        return index < PoolSize
            ? PoolName(index)
            : throw new InvalidOperationException(
                "hub pool exhausted; the namespace is at its ten-entity ceiling, so share a hub instead: " +
                "capture the hub's position with CaptureNextAsync and read with it as the prior sync state");
    }

    public Dictionary<string, object?> ConnectionConfig() =>
        new() { ["auth"] = "connection_string", ["connection_string"] = ConnectionString };

    public async Task SendAsync(string hub,
        IEnumerable<(string? Key, byte[] Body, string? ContentType, IReadOnlyList<KeyValuePair<string, object>> Props)> events)
    {
        await using var producer = new EventHubProducerClient(ConnectionString, hub);

        // One batch per partition key: a batch carries a single key for every event in it, so events
        // that disagree cannot share one.
        foreach (var group in events.GroupBy(e => e.Key, StringComparer.Ordinal))
        {
            var options = new CreateBatchOptions { PartitionKey = group.Key };
            var batch = await producer.CreateBatchAsync(options).ConfigureAwait(false);
            try
            {
                foreach (var (_, body, contentType, properties) in group)
                {
                    var data = new EventData(body) { ContentType = contentType };
                    foreach (var (name, value) in properties)
                    {
                        data.Properties[name] = value;
                    }

                    if (batch.TryAdd(data))
                    {
                        continue;
                    }

                    // TryAdd refused: the batch is at the service's size ceiling. Send what is in it,
                    // then add the refused event to a fresh batch -- it has not been sent anywhere yet.
                    if (batch.Count == 0)
                    {
                        throw new InvalidOperationException($"event hub '{hub}': a single event exceeds the batch size");
                    }

                    await producer.SendAsync(batch).ConfigureAwait(false);
                    batch.Dispose();
                    batch = await producer.CreateBatchAsync(options).ConfigureAwait(false);
                    if (!batch.TryAdd(data))
                    {
                        throw new InvalidOperationException($"event hub '{hub}': a single event exceeds the batch size");
                    }
                }

                if (batch.Count > 0)
                {
                    await producer.SendAsync(batch).ConfigureAwait(false);
                }
            }
            finally
            {
                batch.Dispose();
            }
        }
    }

    /// <summary>Plain UTF-8 bodies, no properties and no content type.</summary>
    public Task SendTextAsync(string hub, IEnumerable<string> bodies, string? key = null) =>
        SendAsync(hub, bodies.Select(b => (key, Encoding.UTF8.GetBytes(b), (string?)null,
            (IReadOnlyList<KeyValuePair<string, object>>)[])));

    /// <summary>Everything currently in the hub, all partitions, ordered by (partition, sequence
    /// number). Each partition stops at its last enqueued sequence number, which is read before the
    /// enumeration starts: a hub's read is otherwise endless, since nothing marks the end of a feed.</summary>
    public async Task<List<EventData>> ReadAllAsync(string hub) =>
        (await ReadCoreAsync(hub, null).ConfigureAwait(false)).Select(e => e.Data).ToList();

    /// <summary>What the hub holds at or after <paramref name="next"/>, a map
    /// <see cref="CaptureNextAsync"/> took before the caller seeded: the read-back half of sharing one
    /// hub between facts. A partition whose last enqueued sequence number is still below its captured
    /// position held nothing new and is skipped, so a caller that wrote to one partition never waits
    /// out a drain of the others.</summary>
    public async Task<List<EventData>> ReadSinceAsync(string hub, IReadOnlyDictionary<string, long> next) =>
        (await ReadCoreAsync(hub, next).ConfigureAwait(false)).Select(e => e.Data).ToList();

    /// <summary>The same read, each event paired with the partition it landed on: the placement a
    /// partition key decides is not carried by the event itself, so a fact about placement can only
    /// learn it from the partition it was read out of.</summary>
    public Task<List<(string Partition, EventData Data)>> ReadSinceByPartitionAsync(
        string hub, IReadOnlyDictionary<string, long> next) => ReadCoreAsync(hub, next);

    private async Task<List<(string Partition, EventData Data)>> ReadCoreAsync(string hub, IReadOnlyDictionary<string, long>? next)
    {
        await using var consumer = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, ConnectionString, hub);
        var collected = new List<(string Id, int Partition, EventData Data)>();
        foreach (var id in await consumer.GetPartitionIdsAsync().ConfigureAwait(false))
        {
            var properties = await consumer.GetPartitionPropertiesAsync(id).ConfigureAwait(false);
            if (properties.IsEmpty)
            {
                continue;
            }

            var from = EventPosition.Earliest;
            if (next is not null)
            {
                var begin = next.TryGetValue(id, out var captured) ? captured : 0;
                if (properties.LastEnqueuedSequenceNumber < begin)
                {
                    continue;
                }

                from = EventPosition.FromSequenceNumber(begin, isInclusive: true);
            }

            var partition = int.Parse(id, CultureInfo.InvariantCulture);
            var options = new ReadEventOptions { MaximumWaitTime = TimeSpan.FromSeconds(2) };
            var silences = 0;
            await foreach (var received in consumer.ReadEventsFromPartitionAsync(id, from, options).ConfigureAwait(false))
            {
                // A null Data is MaximumWaitTime elapsing with nothing handed over. The emulator
                // delivers a burst and then goes quiet for seconds with events still to come, so one
                // silence is not the end of the partition; several in a row, with the last enqueued
                // sequence number still unreached, is as close to an end as this read can get.
                if (received.Data is null)
                {
                    if (++silences >= SilencesBeforeDrained)
                    {
                        break;
                    }

                    continue;
                }

                silences = 0;
                collected.Add((id, partition, received.Data));
                if (received.Data.SequenceNumber >= properties.LastEnqueuedSequenceNumber)
                {
                    break;
                }
            }
        }

        return collected.OrderBy(e => e.Partition).ThenBy(e => e.Data.SequenceNumber).Select(e => (e.Id, e.Data)).ToList();
    }

    /// <summary>Per partition, the sequence number a reader must begin at to see only what is sent
    /// after this call returns: one past the last enqueued, or 0 for a partition nothing was ever
    /// written to. A fact turns this into a prior sync-state token, which is how several facts share
    /// one hub -- the namespace admits ten entities in total, far fewer than one per fact.</summary>
    public async Task<Dictionary<string, long>> CaptureNextAsync(string hub)
    {
        var next = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (id, properties) in await PropertiesAsync(hub).ConfigureAwait(false))
        {
            // A partition that never held an event reports last = -1; the clamp keeps the position a
            // token may carry, which is never negative.
            next[id] = Math.Max(0, properties.LastEnqueuedSequenceNumber + 1);
        }

        return next;
    }

    public async Task<Dictionary<string, PartitionProperties>> PropertiesAsync(string hub)
    {
        await using var consumer = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, ConnectionString, hub);
        var properties = new Dictionary<string, PartitionProperties>(StringComparer.Ordinal);
        foreach (var id in await consumer.GetPartitionIdsAsync().ConfigureAwait(false))
        {
            properties[id] = await consumer.GetPartitionPropertiesAsync(id).ConfigureAwait(false);
        }

        return properties;
    }
}
