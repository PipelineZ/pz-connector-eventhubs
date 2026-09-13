using Pz.Connector.EventHubs.Tests.Fakes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class SourceTests
{
    private const string Cs = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private static DatasetSpec Spec(IReadOnlyDictionary<string, object?>? options = null, string? priorSyncState = null) =>
        new("eventhubs", "h", options ?? new Dictionary<string, object?>()) { PriorSyncState = priorSyncState };

    /// <summary>Opens a source exactly as the engine would -- through the connector's
    /// <see cref="ISourceConnector"/> surface -- so the ctor/config wiring in
    /// <c>EventHubsConnector.OpenAsync</c> is exercised, not just <see cref="EventHubsSource"/> in
    /// isolation.</summary>
    private static async Task<ISource> OpenSourceAsync(FakeClientFactory factory)
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "connection_string", ["connection_string"] = Cs });
        ISourceConnector connector = new EventHubsConnector(null, factory);
        return await connector.OpenAsync(config, CancellationToken.None);
    }

    private static async Task<int> CountRowsAsync(IDatasetPartition partition)
    {
        var count = 0;
        await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            count += batch.Length;
            batch.Dispose();
        }

        return count;
    }

    [Fact]
    public async Task Natural_read_shape_is_feed()
    {
        var source = (INaturalReadShapeSource)await OpenSourceAsync(new FakeClientFactory());
        Assert.Equal(NaturalReadShape.Feed, source.GetNaturalReadShape(Spec()));
    }

    [Fact]
    public async Task There_is_no_native_scan()
    {
        var source = await OpenSourceAsync(new FakeClientFactory());
        Assert.False(source.TryGetNativeScan(Spec(), out var scan));
        Assert.Null(scan);
    }

    [Fact]
    public async Task Schema_is_the_envelope_and_never_touches_the_factory()
    {
        // No hub registered: if GetSchemaAsync opened a reader or otherwise reached the factory for
        // 'h', the fake would throw ResourceNotFound. It doesn't, because the schema is fixed and
        // known without contacting the service.
        var source = await OpenSourceAsync(new FakeClientFactory());
        var schema = await source.GetSchemaAsync(Spec(), CancellationToken.None);
        Assert.Same(EnvelopeBatchBuilder.Schema, schema.Schema);
    }

    [Fact]
    public async Task Bad_dataset_option_is_a_named_config_error()
    {
        var source = await OpenSourceAsync(new FakeClientFactory());
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            source.GetSchemaAsync(Spec(new Dictionary<string, object?> { ["bogus"] = 1 }), CancellationToken.None).AsTask());
        Assert.StartsWith("PZEH0201:", ex.Message);
    }

    [Fact]
    public async Task Plan_read_returns_exactly_one_partition()
    {
        var factory = new FakeClientFactory { Hubs = { ["h"] = HubWith(("0", 3)) } };
        var source = await OpenSourceAsync(factory);
        var partitions = await source.PlanReadAsync(Spec(), new ReadHints(), CancellationToken.None);
        Assert.Single(partitions);
    }

    [Fact]
    public async Task Plan_read_carries_the_prior_sync_state_into_the_partition()
    {
        var factory = new FakeClientFactory { Hubs = { ["h"] = HubWith(("0", 10)) } };
        var source = await OpenSourceAsync(factory);
        var token = new SequenceToken("localhost", "h", new Dictionary<string, long> { ["0"] = 5 }).Serialize();

        var partitions = await source.PlanReadAsync(Spec(priorSyncState: token), new ReadHints(), CancellationToken.None);
        var partition = Assert.Single(partitions);

        Assert.Equal(5, await CountRowsAsync(partition));
    }

    private static FakeHub HubWith(params (string PartitionId, int Count)[] partitions)
    {
        var hub = new FakeHub();
        foreach (var (id, count) in partitions)
        {
            hub.Partitions[id] = Enumerable.Range(0, count)
                .Select(i => new ReceivedEvent(i, $"o{i}", DateTimeOffset.UtcNow, null, "x"u8.ToArray(), null, []))
                .ToList();
        }

        return hub;
    }
}
