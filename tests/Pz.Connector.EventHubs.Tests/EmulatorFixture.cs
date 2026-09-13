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
/// entities after it starts, so every hub a fact may need is declared up front and leased one per
/// fact; a fact never sees another fact's events. Helpers talk to the emulator with the SDK's own
/// clients, deliberately not through the connector: a fact that used the code under test to seed
/// and verify would prove nothing.</summary>
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
    /// fact the previous one's events.</summary>
    public string LeaseHub()
    {
        var index = Interlocked.Increment(ref _leased) - 1;
        return index < PoolSize ? PoolName(index) : throw new InvalidOperationException("hub pool exhausted; raise PoolSize");
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
    public async Task<List<EventData>> ReadAllAsync(string hub)
    {
        await using var consumer = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, ConnectionString, hub);
        var collected = new List<(int Partition, EventData Data)>();
        foreach (var id in await consumer.GetPartitionIdsAsync().ConfigureAwait(false))
        {
            var properties = await consumer.GetPartitionPropertiesAsync(id).ConfigureAwait(false);
            if (properties.IsEmpty)
            {
                continue;
            }

            var partition = int.Parse(id, CultureInfo.InvariantCulture);
            var options = new ReadEventOptions { MaximumWaitTime = TimeSpan.FromSeconds(2) };
            var silences = 0;
            await foreach (var received in consumer.ReadEventsFromPartitionAsync(id, EventPosition.Earliest, options).ConfigureAwait(false))
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
                collected.Add((partition, received.Data));
                if (received.Data.SequenceNumber >= properties.LastEnqueuedSequenceNumber)
                {
                    break;
                }
            }
        }

        return collected.OrderBy(e => e.Partition).ThenBy(e => e.Data.SequenceNumber).Select(e => e.Data).ToList();
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
