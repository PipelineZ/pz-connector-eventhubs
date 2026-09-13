using Apache.Arrow;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connector.EventHubs.Tests.Fakes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class PartitionLoopTests
{
    private const string Cs = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
    private static readonly DateTimeOffset T0 = new(2026, 9, 13, 10, 0, 0, TimeSpan.Zero);

    private static EventHubsConnectionConfig Config(int idleTimeoutSeconds = 60)
    {
        var errors = new List<string>();
        var values = new Dictionary<string, object?>
        {
            ["auth"] = "connection_string", ["connection_string"] = Cs, ["idle_timeout"] = idleTimeoutSeconds,
        };
        return EventHubsConnectionConfig.Parse(new ConnectorConfig(values), errors)!;
    }

    private static EventHubsDatasetConfig Dataset() => new("h", new StartPosition(StartKind.Earliest, null), PayloadEncoding.Utf8);

    private static ReceivedEvent Ev(long seq) => new(seq, $"o{seq}", T0, null, "x"u8.ToArray(), null, []);

    private static FakeHub Hub(params (string PartitionId, IEnumerable<long> Sequences)[] partitions)
    {
        var hub = new FakeHub();
        foreach (var (id, sequences) in partitions)
        {
            hub.Partitions[id] = sequences.Select(Ev).ToList();
        }

        return hub;
    }

    private static EventHubsPartition Partition(FakeClientFactory factory, SequenceToken? token = null, TimeProvider? time = null,
        int idleTimeoutSeconds = 60) =>
        new(Config(idleTimeoutSeconds), factory, Dataset(), token, NullLogger.Instance, time);

    /// <summary>Flattens every yielded batch's (partition, sequence_number) columns, in read order,
    /// and disposes each batch -- every fact reads the whole partition to completion or cancels it.</summary>
    private static async Task<List<(string Partition, long Sequence)>> DrainAsync(EventHubsPartition partition, BatchOptions? options = null,
        CancellationToken ct = default)
    {
        var rows = new List<(string, long)>();
        await foreach (var batch in partition.ReadAsync(options ?? new BatchOptions(), ct))
        {
            using (batch)
            {
                var partitionColumn = (StringArray)batch.Column(1);
                var sequenceColumn = (Int64Array)batch.Column(2);
                for (var i = 0; i < batch.Length; i++)
                {
                    rows.Add((partitionColumn.GetString(i), sequenceColumn.GetValue(i)!.Value));
                }
            }
        }

        return rows;
    }

    [Fact]
    public async Task Lands_every_event_up_to_the_bound_across_partitions()
    {
        var factory = new FakeClientFactory();
        factory.Hubs["h"] = Hub(("0", Enumerable.Range(0, 10).Select(i => (long)i)), ("1", Enumerable.Range(0, 5).Select(i => (long)i)));
        var partition = Partition(factory);

        var rows = await DrainAsync(partition);

        Assert.Equal(15, rows.Count);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => ("0", (long)i)), rows.Take(10));
        Assert.Equal(Enumerable.Range(0, 5).Select(i => ("1", (long)i)), rows.Skip(10));
        Assert.True(partition.TryGetSyncStateCandidate(out var candidate));
        Assert.Equal("""{"v":1,"event_hub":"h","partitions":{"0":10,"1":5}}""", candidate);
    }

    [Fact]
    public async Task Events_past_the_plan_time_bound_are_not_landed()
    {
        var factory = new FakeClientFactory();
        var hub = Hub(("0", Enumerable.Range(0, 10).Select(i => (long)i)));
        factory.Hubs["h"] = hub;
        var appended = false;
        hub.OnReceive = (_, events) =>
        {
            if (!appended)
            {
                appended = true;
                // Simulates a producer landing more events while this run is already mid-read: the
                // plan's bound was fixed before this point, so these must never be landed this run.
                hub.Partitions["0"].Add(Ev(10));
                hub.Partitions["0"].Add(Ev(11));
            }

            return events;
        };
        var partition = Partition(factory);

        var rows = await DrainAsync(partition);

        Assert.Equal(10, rows.Count);
        Assert.True(partition.TryGetSyncStateCandidate(out var candidate));
        Assert.Equal("""{"v":1,"event_hub":"h","partitions":{"0":10}}""", candidate);
    }

    [Fact]
    public async Task Resumes_from_the_token_and_reads_nothing_when_caught_up()
    {
        var factory = new FakeClientFactory();
        factory.Hubs["h"] = Hub(("0", Enumerable.Range(0, 10).Select(i => (long)i)), ("1", Enumerable.Range(0, 5).Select(i => (long)i)));

        var firstToken = new SequenceToken("h", new Dictionary<string, long> { ["0"] = 5, ["1"] = 5 });
        var first = Partition(factory, firstToken);
        var firstRows = await DrainAsync(first);
        Assert.Equal(Enumerable.Range(5, 5).Select(i => ("0", (long)i)), firstRows);
        Assert.True(first.TryGetSyncStateCandidate(out var firstCandidate));
        Assert.Equal("""{"v":1,"event_hub":"h","partitions":{"0":10,"1":5}}""", firstCandidate);

        var secondToken = new SequenceToken("h", new Dictionary<string, long> { ["0"] = 10, ["1"] = 5 });
        var second = Partition(factory, secondToken);
        var secondRows = await DrainAsync(second);
        Assert.Empty(secondRows);
        Assert.True(second.TryGetSyncStateCandidate(out var secondCandidate));
        Assert.Equal(firstCandidate, secondCandidate);
    }

    [Fact]
    public async Task Cancellation_mid_read_yields_no_candidate()
    {
        var factory = new FakeClientFactory();
        factory.Hubs["h"] = Hub(("0", Enumerable.Range(0, 5000).Select(i => (long)i)));
        var partition = Partition(factory);
        using var cts = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var yielded = 0;
            await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 100), cts.Token))
            {
                batch.Dispose();
                yielded++;
                if (yielded == 1)
                {
                    cts.Cancel();
                }
            }
        });

        Assert.False(partition.TryGetSyncStateCandidate(out var candidate));
        Assert.Null(candidate);
    }

    [Fact]
    public async Task Idle_timeout_is_transient_and_leaves_no_candidate()
    {
        var factory = new FakeClientFactory();
        var hub = Hub(("0", Enumerable.Range(0, 10).Select(i => (long)i)));
        factory.Hubs["h"] = hub;
        var time = new ManualTimeProvider();
        var calls = 0;
        hub.OnReceive = (_, events) =>
        {
            calls++;
            if (calls == 1)
            {
                return events.Take(5).ToList();
            }

            // The service goes quiet from here on; advancing the clock inside the hook is what a
            // real idle gap looks like from the loop's point of view -- time passes between polls.
            time.Advance(TimeSpan.FromSeconds(3));
            return [];
        };
        var partition = Partition(factory, time: time, idleTimeoutSeconds: 2);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(partition));

        Assert.True(ex.IsTransient);
        Assert.Contains("PZEH0206", ex.Message);
        Assert.Contains("no event arrived for 2s", ex.Message);
        Assert.False(partition.TryGetSyncStateCandidate(out var candidate));
        Assert.Null(candidate);
    }

    [Fact]
    public async Task Unknown_hub_is_a_non_transient_named_error()
    {
        var factory = new FakeClientFactory();
        var partition = Partition(factory);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(partition));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZEH0204", ex.Message);
        Assert.Contains("event hub 'h'", ex.Message);
        Assert.Contains(Config().NamespaceHost, ex.Message);
    }

    [Fact]
    public async Task Retention_loss_from_the_plan_is_surfaced()
    {
        var factory = new FakeClientFactory();
        factory.Hubs["h"] = Hub(("0", Enumerable.Range(100, 10).Select(i => (long)i)));
        var token = new SequenceToken("h", new Dictionary<string, long> { ["0"] = 0 });
        var partition = Partition(factory, token);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(partition));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZEH0203", ex.Message);
    }

    [Fact]
    public async Task Receivers_are_disposed_per_partition()
    {
        var factory = new FakeClientFactory();
        var hub = Hub(("0", Enumerable.Range(0, 3).Select(i => (long)i)), ("1", Enumerable.Range(0, 3).Select(i => (long)i)));
        factory.Hubs["h"] = hub;
        var partition = Partition(factory);

        await DrainAsync(partition);

        Assert.Equal(2, hub.OpenedReaders);
        Assert.Equal(2, hub.DisposedReaders);
    }
}
