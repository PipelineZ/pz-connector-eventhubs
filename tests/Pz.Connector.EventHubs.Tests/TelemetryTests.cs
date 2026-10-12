using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Messaging.EventHubs;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connector.EventHubs.Tests.Fakes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>What the connector reports through the ambient stats handle: one request per receive or
/// send, the payload bytes it moved, and a throttle only for a busy service.</summary>
public sealed class TelemetryTests
{
    private const string Cs = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
    private static readonly DateTimeOffset T0 = new(2026, 9, 13, 10, 0, 0, TimeSpan.Zero);

    private static EventHubsPartition Partition(FakeClientFactory factory)
    {
        var errors = new List<string>();
        var connection = EventHubsConnectionConfig.Parse(
            new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "connection_string", ["connection_string"] = Cs }), errors)!;
        var dataset = new EventHubsDatasetConfig("h", new StartPosition(StartKind.Earliest, null), PayloadEncoding.Utf8);
        return new EventHubsPartition(connection, factory, dataset, null, NullLogger.Instance);
    }

    private static FakeClientFactory HubWithThreeEvents()
    {
        var factory = new FakeClientFactory();
        var hub = new FakeHub();
        hub.Partitions["0"] = Enumerable.Range(0, 3)
            .Select(i => new ReceivedEvent(i, $"o{i}", T0, null, new byte[10], null, [])).ToList();
        factory.Hubs["h"] = hub;
        return factory;
    }

    private static async Task DrainAsync(EventHubsPartition partition)
    {
        await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            batch.Dispose();
        }
    }

    [Fact]
    public async Task Receiving_three_ten_byte_events_reports_one_request_thirty_bytes_three_events()
    {
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await DrainAsync(Partition(HubWithThreeEvents()));
        }

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Equal(30, s.WireBytes);
        Assert.Equal(0, s.Throttled);
        Assert.Equal([new KeyValuePair<string, double>("eventhubs.events", 3)], s.Extras);
    }

    [Fact]
    public async Task A_busy_service_on_receive_counts_the_request_and_a_throttle()
    {
        var factory = HubWithThreeEvents();
        factory.Hubs["h"].OnReceive = (_, _) =>
            throw new EventHubsException("h", "busy", EventHubsException.FailureReason.ServiceBusy);
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(Partition(factory)));
        }

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Equal(1, s.Throttled);
    }

    [Fact]
    public async Task A_non_busy_failure_is_not_a_throttle()
    {
        var factory = HubWithThreeEvents();
        factory.Hubs["h"].OnReceive = (_, _) =>
            throw new EventHubsException("h", "gone", EventHubsException.FailureReason.ResourceNotFound);
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(Partition(factory)));
        }

        Assert.Equal(0, acc.Snapshot().Throttled);
    }

    private static readonly Schema OneColumn = new([new Field("name", StringType.Default, true)], null);

    private static EventHubsWriteSession Session(FakeWriter writer)
    {
        var errors = new List<string>();
        var output = EventHubsOutputConfig.Parse(new OutputSpec("eventhubs", "h", "append", "fail_on_change", new Dictionary<string, object?>()), errors);
        Assert.Empty(errors);
        return new EventHubsWriteSession(writer, output!, OneColumn, "ns.servicebus.windows.net", EventHubsRedactor.None, NullLogger.Instance);
    }

    private static RecordBatch OneRow() =>
        new(OneColumn, [new StringArray.Builder().Append("ann").Build()], 1);

    [Fact]
    public async Task Sending_a_batch_reports_one_request_its_size_and_one_batch()
    {
        var hub = new FakeHub();
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await using var session = Session(new FakeWriter(hub));
            using var batch = OneRow();
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var sent = Assert.Single(hub.Sent);
        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.True(s.WireBytes > 0);
        Assert.Equal(sent.Events.Sum(e => e.Body.Length + 64), s.WireBytes);
        Assert.Equal([new KeyValuePair<string, double>("eventhubs.batches", 1)], s.Extras);
        Assert.Equal(0, s.Throttled);
    }

    [Fact]
    public async Task A_busy_service_on_send_counts_the_request_and_a_throttle_but_no_bytes()
    {
        var writer = new FakeWriter(new FakeHub())
        {
            FailSend = new EventHubsException("h", "busy", EventHubsException.FailureReason.ServiceBusy),
        };
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await using var session = Session(writer);
            using var batch = OneRow();
            await Assert.ThrowsAsync<PzConnectorException>(async () => await session.WriteBatchAsync(batch, CancellationToken.None));
        }

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Equal(1, s.Throttled);
        Assert.Equal(0, s.WireBytes);
        Assert.Empty(s.Extras);
    }
}
