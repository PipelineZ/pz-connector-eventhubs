using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>What the source does against a real service, where the sequence numbers, the bounds and
/// the enqueue times are the broker's rather than a fake's. Most facts share
/// <see cref="EmulatorFixture.SmallHub"/>: the namespace admits ten entities in total and the
/// acceptance suites spend most of the pool, so a fact captures the hub's next-sequence map, seeds,
/// and reads with that map as its prior sync state -- which is exactly a resume, and confines the
/// fact to its own events. Only a fact whose subject is a first run with no stored state (a `start:`
/// that must not be outranked, an untouched hub) leases a hub.
///
/// No `start: &lt;timestamp&gt;` fact lives here: a stored position outranks `start:` for every
/// partition the token names, so the timestamp branch is unreachable through a shared hub's captured
/// token, and a hub of its own is a hub the pool cannot spare. <see cref="ReadPlanTests"/> carries
/// the arithmetic and <see cref="PartitionLoopTests"/> the position it produces.</summary>
[Collection("eventhubs")]
[Trait("Category", "Docker")]
public sealed class SourceBehaviorTests
{
    /// <summary>Bodies fat enough that a cancelled read of the acceptance suite's large hub is not
    /// accidentally cheap, matching that suite's own seed when this fact reaches an empty hub first.</summary>
    private const int FatBodyBytes = 200_000;

    private static readonly Lock LeaseGate = new();
    private static string? _firstRunHub;
    private static string? _untouchedHub;

    private readonly EmulatorFixture _emulator;

    public SourceBehaviorTests(EmulatorFixture emulator)
    {
        _emulator = emulator;
        DockerFacts.SkipUnlessDocker();
    }

    [SkippableFact]
    public async Task First_run_lands_everything_and_the_token_names_every_partition()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = EmulatorFixture.SmallHub;
        var captured = await _emulator.CaptureNextAsync(hub);

        // Two partition keys: whichever partitions they hash to, the token must name every partition
        // of the hub, and the read must begin at the captured position on each one it touches.
        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 15).Select(i => $"first-a-{i}"), key: "first-a");
        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 15).Select(i => $"first-b-{i}"), key: "first-b");

        var (rows, token) = await ReadAsync(hub, [], Token(hub, captured));

        Assert.Equal(30, rows.Count);
        Assert.All(rows, r => Assert.Equal(hub, r.EventHub));
        foreach (var partition in rows.GroupBy(r => r.Partition, StringComparer.Ordinal))
        {
            var sequences = partition.Select(r => r.SequenceNumber).ToList();
            var begin = captured[partition.Key];
            Assert.Equal(Enumerable.Range(0, sequences.Count).Select(i => begin + i), sequences);
        }

        Assert.NotNull(token);
        var parsed = SequenceToken.Parse(token!, _emulator.NamespaceHost, hub, EventHubsRedactor.None);
        Assert.Equal(["0", "1"], parsed.Next.Keys.OrderBy(k => k, StringComparer.Ordinal));

        // Every landed event moved exactly one partition's resume position, so the token as a whole
        // advanced by the size of the seed.
        Assert.Equal(30, parsed.Next.Values.Sum() - captured.Values.Sum());
    }

    [SkippableFact]
    public async Task Second_run_lands_only_new_events()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = EmulatorFixture.SmallHub;
        var captured = await _emulator.CaptureNextAsync(hub);
        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 10).Select(i => $"second-early-{i}"), key: "second");

        var (early, first) = await ReadAsync(hub, [], Token(hub, captured));
        Assert.Equal(10, early.Count);
        Assert.NotNull(first);

        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 5).Select(i => $"second-later-{i}"), key: "second");

        var (later, second) = await ReadAsync(hub, [], first);

        Assert.Equal(5, later.Count);
        Assert.All(later, r => Assert.StartsWith("second-later-", r.Body, StringComparison.Ordinal));
        Assert.NotNull(second);
        var before = SequenceToken.Parse(first!, _emulator.NamespaceHost, hub, EventHubsRedactor.None);
        var after = SequenceToken.Parse(second!, _emulator.NamespaceHost, hub, EventHubsRedactor.None);
        Assert.Equal(5, after.Next.Values.Sum() - before.Next.Values.Sum());
        Assert.All(after.Next, entry => Assert.True(entry.Value >= before.Next[entry.Key],
            $"partition {entry.Key} went backwards: {before.Next[entry.Key]} -> {entry.Value}"));
    }

    [SkippableFact]
    public async Task Events_sent_during_a_read_are_not_landed()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = EmulatorFixture.SmallHub;
        var captured = await _emulator.CaptureNextAsync(hub);

        // One key, so the seed and the events that arrive mid-read share a partition: the bound that
        // has to hold them back is that partition's own last enqueued sequence number at plan time.
        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 300).Select(i => $"drift-seed-{i}"), key: "drift");

        var (rows, token) = await ReadAsync(hub, [], Token(hub, captured), new BatchOptions(MaxRowsPerBatch: 50),
            afterFirstBatch: () => _emulator.SendTextAsync(hub, Enumerable.Range(0, 20).Select(i => $"drift-late-{i}"), key: "drift"));

        Assert.Equal(300, rows.Count);
        Assert.DoesNotContain(rows, r => r.Body.StartsWith("drift-late-", StringComparison.Ordinal));
        Assert.NotNull(token);

        var (late, _) = await ReadAsync(hub, [], token);

        Assert.Equal(20, late.Count);
        Assert.All(late, r => Assert.StartsWith("drift-late-", r.Body, StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Start_latest_lands_nothing_and_still_tokens()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = FirstRunHub();
        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 10).Select(i => $"latest-before-{i}"), key: "latest");

        var (skipped, token) = await ReadAsync(hub, new Dictionary<string, object?> { ["start"] = "latest" }, priorToken: null);

        Assert.Empty(skipped);
        Assert.NotNull(token);
        var parsed = SequenceToken.Parse(token!, _emulator.NamespaceHost, hub, EventHubsRedactor.None);
        foreach (var (id, properties) in await _emulator.PropertiesAsync(hub))
        {
            Assert.Equal(properties.LastEnqueuedSequenceNumber + 1, parsed.Next[id]);
        }

        await _emulator.SendTextAsync(hub, Enumerable.Range(0, 3).Select(i => $"latest-after-{i}"), key: "latest");

        // `start:` still says latest, and is ignored: a partition the token names resumes from the
        // stored position, or the second run of a `start: latest` dataset would land nothing forever.
        var (arrived, advanced) = await ReadAsync(hub, new Dictionary<string, object?> { ["start"] = "latest" }, token);

        Assert.Equal(3, arrived.Count);
        Assert.All(arrived, r => Assert.StartsWith("latest-after-", r.Body, StringComparison.Ordinal));
        Assert.NotNull(advanced);
    }

    [SkippableFact]
    public async Task Zero_row_run_still_emits_a_token()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = UntouchedHub();

        var (rows, token) = await ReadAsync(hub, [], priorToken: null);

        Assert.Empty(rows);

        // Nothing was read, and the engine still has something to store: without a token a hub that
        // stays quiet would be re-planned from `start:` on every run.
        Assert.NotNull(token);
        var parsed = SequenceToken.Parse(token!, _emulator.NamespaceHost, hub, EventHubsRedactor.None);
        Assert.Equal(2, parsed.Next.Count);
        Assert.Equal(0, parsed.Next["0"]);
        Assert.Equal(0, parsed.Next["1"]);
    }

    [SkippableFact]
    public async Task Token_for_another_hub_is_refused()
    {
        DockerFacts.SkipUnlessDocker();
        var foreign = new SequenceToken(_emulator.NamespaceHost, "some-other-hub", new Dictionary<string, long> { ["0"] = 0, ["1"] = 0 }).Serialize();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await ReadAsync(EmulatorFixture.SmallHub, [], foreign));

        Assert.StartsWith("PZEH0202:", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
        Assert.Contains("some-other-hub", ex.Message, StringComparison.Ordinal);
        Assert.Contains(EmulatorFixture.SmallHub, ex.Message, StringComparison.Ordinal);

        // The token is engine state, not something a run artifact may carry.
        Assert.DoesNotContain(foreign, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>The emulator answers a request for an entity it does not hold with a communication
    /// problem rather than the service's own not-found condition, so the reason-to-PZEH0204 mapping
    /// is the one thing here only a real namespace can drive; see
    /// <see cref="PartitionLoopTests.Unknown_hub_is_a_non_transient_named_error"/>. What must hold
    /// against either is this: the read stops with a coded, redacted error naming the hub, and hands
    /// back no token -- a hub that is not there must never read as a hub that is empty.</summary>
    [SkippableFact]
    public async Task Unknown_hub_fails_with_a_coded_error_and_no_candidate()
    {
        DockerFacts.SkipUnlessDocker();
        ISourceConnector connector = new EventHubsConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(_emulator.ConnectionConfig()), CancellationToken.None);
        var partitions = await source.PlanReadAsync(
            new DatasetSpec("eventhubs", "does-not-exist", new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.Matches("^PZEH0[0-9]{3}: eventhubs: ", ex.Message);
        Assert.Contains("does-not-exist", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(_emulator.ConnectionString, ex.Message, StringComparison.Ordinal);
        Assert.False(((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var candidate));
        Assert.Null(candidate);
    }

    [SkippableFact]
    public async Task Cancellation_stops_within_five_seconds_and_leaves_no_candidate()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = EmulatorFixture.LargeHub;
        await EnsureTwoEventsAsync(hub);

        ISourceConnector connector = new EventHubsConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(_emulator.ConnectionConfig()), CancellationToken.None);
        var partitions = await source.PlanReadAsync(
            new DatasetSpec("eventhubs", hub, new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions);

        using var cts = new CancellationTokenSource();
        var elapsed = new Stopwatch();

        // One row per batch, so the cancel lands between batches however much the hub holds.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), cts.Token))
            {
                batch.Dispose();
                elapsed.Start();
                await cts.CancelAsync();
            }
        });

        Assert.True(elapsed.IsRunning, "the read ended before a batch was handed over, so nothing was cancelled");
        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(5), $"cancellation took {elapsed.Elapsed}");

        // A cancelled read landed part of a slice, so offering a token would let the next run resume
        // past events that were never handed to the engine.
        Assert.False(((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var candidate));
        Assert.Null(candidate);
    }

    [SkippableFact]
    public async Task Envelope_carries_key_properties_and_content_type()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = EmulatorFixture.SmallHub;
        var captured = await _emulator.CaptureNextAsync(hub);
        await _emulator.SendAsync(hub,
        [
            ("envelope-key", Encoding.UTF8.GetBytes("hello"), "text/plain",
                (IReadOnlyList<KeyValuePair<string, object>>)[new("n", 1L), new("s", "x")]),
        ]);

        var (rows, _) = await ReadAsync(hub, [], Token(hub, captured));

        var row = Assert.Single(rows);
        Assert.Equal(hub, row.EventHub);
        Assert.Equal("envelope-key", row.PartitionKey);
        Assert.Equal("hello", row.Body);
        Assert.Equal("text/plain", row.ContentType);
        Assert.Equal(captured[row.Partition], row.SequenceNumber);
        Assert.NotEqual("", row.Offset);

        using var properties = JsonDocument.Parse(row.Properties);
        Assert.Equal(2, properties.RootElement.EnumerateObject().Count());
        Assert.Equal(1L, properties.RootElement.GetProperty("n").GetInt64());
        Assert.Equal("x", properties.RootElement.GetProperty("s").GetString());

        // The enqueue time is the broker's, so it is checked against what the broker itself reports
        // for the partition the event landed on, truncated to the microsecond the column stores.
        var reported = (await _emulator.PropertiesAsync(hub))[row.Partition].LastEnqueuedTime;
        Assert.Equal(reported.UtcTicks / 10, row.EnqueuedTime.UtcTicks / 10);
    }

    [SkippableFact]
    public async Task Base64_encoding_lands_raw_bytes()
    {
        DockerFacts.SkipUnlessDocker();

        // A body that is not UTF-8 stays on a leased hub: any full read of a shared hub would decode
        // it and fail every other fact reading from earliest.
        var hub = FirstRunHub();
        var captured = await _emulator.CaptureNextAsync(hub);
        await _emulator.SendAsync(hub,
            [(null, new byte[] { 0xFF, 0xFE }, null, (IReadOnlyList<KeyValuePair<string, object>>)[])]);
        var prior = Token(hub, captured);

        var (rows, token) = await ReadAsync(hub, new Dictionary<string, object?> { ["encoding"] = "base64" }, prior);

        var row = Assert.Single(rows);
        Assert.Equal("//4=", row.Body);
        Assert.NotNull(token);

        // The same bytes under the default encoding are a refusal, not a replacement character:
        // silently altered payloads are worse than a stopped feed.
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await ReadAsync(hub, new Dictionary<string, object?> { ["encoding"] = "utf8" }, prior));

        Assert.StartsWith("PZEH0205:", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
        Assert.Contains("base64", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>One landed envelope row, copied out while the batch is alive: a
    /// <see cref="RecordBatch"/> owns pooled native buffers released on dispose, so a fact never
    /// holds one past the read that produced it.</summary>
    private sealed record Row(string EventHub, string Partition, long SequenceNumber, string Offset,
        DateTimeOffset EnqueuedTime, string? PartitionKey, string Body, string Properties, string? ContentType);

    /// <summary>The prior sync state a fact hands the connector so it lands only what it sent: the
    /// hub's per-partition next-sequence map, captured before the seed, in the connector's own token
    /// shape.</summary>
    private string Token(string hub, IReadOnlyDictionary<string, long> next) =>
        new SequenceToken(_emulator.NamespaceHost, hub, next).Serialize();

    private static void Collect(List<Row> rows, RecordBatch batch)
    {
        var eventHub = (StringArray)batch.Column(0);
        var partition = (StringArray)batch.Column(1);
        var sequence = (Int64Array)batch.Column(2);
        var offset = (StringArray)batch.Column(3);
        var enqueued = (TimestampArray)batch.Column(4);
        var key = (StringArray)batch.Column(5);
        var body = (StringArray)batch.Column(6);
        var properties = (StringArray)batch.Column(7);
        var contentType = (StringArray)batch.Column(8);
        for (var i = 0; i < batch.Length; i++)
        {
            rows.Add(new Row(eventHub.GetString(i)!, partition.GetString(i)!, sequence.GetValue(i)!.Value,
                offset.GetString(i)!, enqueued.GetTimestamp(i)!.Value, key.GetString(i), body.GetString(i)!,
                properties.GetString(i)!, contentType.GetString(i)));
        }
    }

    /// <summary>Opens the connector, plans the dataset, drains its one partition and takes the token
    /// candidate -- the whole shape of a source node's read, which is what these facts are about.
    /// <paramref name="afterFirstBatch"/> runs once, while the enumeration is suspended on its first
    /// batch, so a fact can change the hub mid-read without a wall-clock guess at when to do it.</summary>
    private async Task<(List<Row> Rows, string? Token)> ReadAsync(string hub, Dictionary<string, object?> options,
        string? priorToken, BatchOptions? batchOptions = null, Func<Task>? afterFirstBatch = null)
    {
        ISourceConnector connector = new EventHubsConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(_emulator.ConnectionConfig()), CancellationToken.None);
        var spec = new DatasetSpec("eventhubs", hub, options) { PriorSyncState = priorToken };
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions);

        var rows = new List<Row>();
        var batches = 0;
        await foreach (var batch in partition.ReadAsync(batchOptions ?? BatchOptions.Default, CancellationToken.None))
        {
            using (batch)
            {
                Collect(rows, batch);
            }

            if (++batches == 1 && afterFirstBatch is not null)
            {
                await afterFirstBatch();
            }
        }

        return (rows, ((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var token) ? token : null);
    }

    /// <summary>The acceptance suite seeds the large hub, but a run that reaches this class first
    /// finds it empty, and a cancellation fact needs a second batch to be cancelled before.</summary>
    private async Task EnsureTwoEventsAsync(string hub)
    {
        var properties = await _emulator.PropertiesAsync(hub);
        var held = properties.Values.Sum(p => p.IsEmpty ? 0 : p.LastEnqueuedSequenceNumber - p.BeginningSequenceNumber + 1);
        if (held < 2)
        {
            await _emulator.SendTextAsync(hub, Enumerable.Repeat(new string('x', FatBodyBytes), 2));
        }
    }

    /// <summary>A hub of this class's own, shared by the facts that must not be outranked by a stored
    /// position and by the one that writes bytes no UTF-8 read may meet. Leased on first use and held
    /// for the run: an event hub cannot be emptied, so a hub is never handed back.</summary>
    private string FirstRunHub()
    {
        lock (LeaseGate)
        {
            return _firstRunHub ??= _emulator.LeaseHub();
        }
    }

    /// <summary>A hub nothing ever writes to, which is the only way to observe a read of a hub that
    /// has never held an event.</summary>
    private string UntouchedHub()
    {
        lock (LeaseGate)
        {
            return _untouchedHub ??= _emulator.LeaseHub();
        }
    }
}
