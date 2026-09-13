using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>TestKit source contract against the emulator. SmallDataset is the 2-partition hub with 120
/// ~100-byte events (>= 100 rows, >= 2 batches under the suite's 4KB target). LargeDataset is the
/// 1-partition hub, seeded with events fat enough that the default 32MB batch target — not the 122,880-row
/// ceiling — is what closes the first batch: the emulator hands a consumer a burst and then stalls for
/// seconds, so a six-figure row count would take tens of minutes to reach a second batch, while bytes
/// reach one in seconds and prove the same thing the fact asks for, that cancelling mid-read is observable.
/// Both hubs are seeded once per run. No BoundedWindow/Checkpoint/ChangeCapture fixtures: the connector
/// declares none of those capabilities.</summary>
[Collection("eventhubs")]
[Trait("Category", "Docker")]
public sealed class SourceAcceptance : SourceConnectorAcceptanceTests
{
    private const int SmallRows = 120;
    private const int SmallBodyPad = 80;

    /// <summary>Fat enough that 32MB closes before 122,880 rows: 32MB / this is the first batch's row
    /// count, and the seed below sends more than twice that many events.</summary>
    private const int LargeBodyPad = 200_000;
    private const int LargeRows = 250;

    private static readonly SemaphoreSlim Seed = new(1, 1);
    private static bool _seeded;

    private readonly EmulatorFixture _emulator;

    public SourceAcceptance(EmulatorFixture emulator)
    {
        _emulator = emulator;
        DockerFacts.SkipUnlessDocker();
        SeedAsync().GetAwaiter().GetResult();
    }

    protected override ConnectorConfig ValidConfig => new(_emulator.ConnectionConfig());

    protected override DatasetSpec SmallDataset => new("eventhubs", EmulatorFixture.SmallHub, new Dictionary<string, object?>());

    protected override DatasetSpec? LargeDataset => new("eventhubs", EmulatorFixture.LargeHub, new Dictionary<string, object?>());

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new EventHubsConnector();

    /// <summary>The sync-state round trip the TestKit has no generic fact for: a first read yields every
    /// event and a token, and a second read resuming from that token yields only what arrived after it.
    /// This is the connector's whole incremental story, and it is only true against a real service —
    /// the token is per-partition sequence numbers the emulator, not the connector, assigns.</summary>
    [SkippableFact]
    public async Task Resuming_from_the_sync_state_token_yields_only_later_events()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = _emulator.LeaseHub();
        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 10).Select(i => $"first-{i}"), key: "a");

        var spec = new DatasetSpec("eventhubs", hub, new Dictionary<string, object?>());
        ISourceConnector connector = new EventHubsConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var (firstRows, token) = await ReadAsync(source, spec);
        Assert.Equal(10, firstRows);
        Assert.NotNull(token);

        // A resume that saw nothing new must yield nothing at all -- and still offer a token, or the
        // engine would have nothing to store for the next run.
        var (idleRows, idleToken) = await ReadAsync(source, spec with { PriorSyncState = token });
        Assert.Equal(0, idleRows);
        Assert.NotNull(idleToken);

        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 5).Select(i => $"second-{i}"), key: "a");
        var (resumedRows, resumedToken) = await ReadAsync(source, spec with { PriorSyncState = idleToken });
        Assert.Equal(5, resumedRows);
        Assert.NotNull(resumedToken);

        // The token is the hub's own arithmetic, read back from the emulator rather than from the
        // connector: every partition resumes one past what the service reports as last enqueued,
        // including the partition no event ever landed on (last -1, so the resume position is 0).
        var parsed = SequenceToken.Parse(resumedToken!, hub, EventHubsRedactor.None);
        foreach (var (id, partitionProperties) in await _emulator.PropertiesAsync(hub))
        {
            Assert.Equal(partitionProperties.LastEnqueuedSequenceNumber + 1, parsed.Next[id]);
        }
    }

    private static async Task<(long Rows, string? Token)> ReadAsync(ISource source, DatasetSpec spec)
    {
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions);
        var rows = 0L;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        Assert.True(((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var token));
        return (rows, token);
    }

    private async Task SeedAsync()
    {
        await Seed.WaitAsync();
        try
        {
            if (_seeded)
            {
                return;
            }

            // Two partition keys so the 2-partition hub is read as more than one partition, and so the
            // envelope's partition_key column carries something.
            var pad = new string('x', SmallBodyPad);
            for (var key = 0; key < 2; key++)
            {
                var slice = key;
                await _emulator.SendTextAsync(EmulatorFixture.SmallHub,
                    Enumerable.Range(0, SmallRows / 2).Select(i => $"{{\"id\":{(i * 2) + slice},\"pad\":\"{pad}\"}}"),
                    key: $"k{slice}");
            }

            var fat = new string('x', LargeBodyPad);
            await _emulator.SendTextAsync(EmulatorFixture.LargeHub,
                Enumerable.Range(0, LargeRows).Select(i => $"{{\"id\":{i},\"pad\":\"{fat}\"}}"));
            _seeded = true;
        }
        finally
        {
            Seed.Release();
        }
    }
}
