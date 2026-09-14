using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Messaging.EventHubs.Producer;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>What the sink puts on the wire, read back with the SDK's own consumer rather than with
/// the connector: a fact that verified through the code under test would prove only that it agrees
/// with itself. Every fact here shares one leased hub -- the namespace admits ten entities in total,
/// far fewer than one per fact -- and is confined to its own events by capturing the hub's
/// per-partition end before it writes and reading back from there.</summary>
[Collection("eventhubs")]
[Trait("Category", "Docker")]
public sealed class SinkBehaviorTests
{
    /// <summary>A body no batch can hold, whatever the service's ceiling: the largest event a
    /// namespace accepts is one megabyte.</summary>
    private const int OversizedBodyBytes = 1_100_000;

    private static readonly Lock LeaseGate = new();
    private static string? _hub;

    private readonly EmulatorFixture _emulator;

    public SinkBehaviorTests(EmulatorFixture emulator)
    {
        _emulator = emulator;
        DockerFacts.SkipUnlessDocker();
    }

    [SkippableFact]
    public async Task Whole_row_json_round_trips_every_type()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = Hub();
        var captured = await _emulator.CaptureNextAsync(hub);

        var schema = new Schema(
        [
            new Field("i32", Int32Type.Default, true),
            new Field("i64", Int64Type.Default, true),
            new Field("dbl", DoubleType.Default, true),
            new Field("dec", new Decimal128Type(38, 6), true),
            new Field("b", BooleanType.Default, true),
            new Field("d", Date32Type.Default, true),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
            new Field("s", StringType.Default, true),
            new Field("nothing", StringType.Default, true),
        ], null);
        var timestamp = new DateTimeOffset(2026, 9, 7, 10, 11, 12, TimeSpan.Zero).AddTicks(1234560);

        using (var batch = new RecordBatch(schema,
        [
            new Int32Array.Builder().Append(7).Build(),
            new Int64Array.Builder().Append(9_000_000_000L).Build(),
            new DoubleArray.Builder().Append(1.5).Build(),
            new Decimal128Array.Builder(new Decimal128Type(38, 6)).Append(12.345m).Build(),
            new BooleanArray.Builder().Append(true).Build(),
            new Date32Array.Builder().Append(new DateOnly(2026, 9, 7)).Build(),
            new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC")).Append(timestamp).Build(),
            new StringArray.Builder().Append("he said \"hi\"").Build(),
            new StringArray.Builder().AppendNull().Build(),
        ], 1))
        {
            Assert.Equal(new WriteResult(1, 1), await WriteAsync(hub, [], batch));
        }

        var e = Assert.Single(await _emulator.ReadSinceAsync(hub, captured));

        // Byte for byte what the row serializes to: the body is a wire format other systems read,
        // so a changed spelling of a number, a decimal or a timestamp is a breaking change.
        Assert.Equal(
            """{"i32":7,"i64":9000000000,"dbl":1.5,"dec":"12.345000","b":true,"d":"2026-09-07","ts":"2026-09-07T10:11:12.123456Z","s":"he said \"hi\"","nothing":null}""",
            Encoding.UTF8.GetString(e.EventBody.ToArray()));
        Assert.Equal("application/json", e.ContentType);
    }

    [SkippableFact]
    public async Task Partition_key_and_properties_are_routed_not_embedded()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = Hub();
        var captured = await _emulator.CaptureNextAsync(hub);
        var schema = RoutingSchema();

        using (var batch = new RecordBatch(schema,
        [
            new Int64Array.Builder().Append(1L).Build(),
            new StringArray.Builder().Append("ann").Build(),
            new StringArray.Builder().Append("eu").Build(),
            new Int64Array.Builder().Append(7L).Build(),
            new BooleanArray.Builder().Append(true).Build(),
        ], 1))
        {
            await WriteAsync(hub, RoutingOptions(), batch);
        }

        var e = Assert.Single(await _emulator.ReadSinceAsync(hub, captured));

        // A column that routes is not also carried: the body is every column the output did not
        // name, so a consumer reading the body sees the payload and nothing about the transport.
        Assert.Equal("""{"id":1,"name":"ann"}""", Encoding.UTF8.GetString(e.EventBody.ToArray()));
        Assert.Equal("eu", e.PartitionKey);

        // AMQP carries a property's type, and the row's type is what arrives -- a consumer that
        // reads `n` as a number must not have to parse a string.
        Assert.Equal(2, e.Properties.Count);
        Assert.Equal(7L, Assert.IsType<long>(e.Properties["n"]));
        Assert.True(Assert.IsType<bool>(e.Properties["flag"]));
    }

    [SkippableFact]
    public async Task Same_key_lands_on_one_partition()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = Hub();
        var captured = await _emulator.CaptureNextAsync(hub);
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, true),
            new Field("region", StringType.Default, true),
        ], null);

        var ids = new Int64Array.Builder();
        var regions = new StringArray.Builder();
        for (var row = 0; row < 40; row++)
        {
            ids.Append(row);
            regions.Append(row % 2 == 0 ? "alpha" : "beta");
        }

        using (var batch = new RecordBatch(schema, [ids.Build(), regions.Build()], 40))
        {
            Assert.Equal(new WriteResult(40, 1), await WriteAsync(hub,
                new Dictionary<string, object?> { ["partition_key"] = "region" }, batch));
        }

        var landed = await _emulator.ReadSinceByPartitionAsync(hub, captured);

        Assert.Equal(40, landed.Count);
        foreach (var key in landed.GroupBy(e => e.Data.PartitionKey, StringComparer.Ordinal))
        {
            // Ordering downstream is per partition, so a key that straddled two of them would hand a
            // consumer that key's rows in an order the sink never wrote.
            Assert.Equal(20, key.Count());
            Assert.Single(key.Select(e => e.Partition).Distinct(StringComparer.Ordinal));
        }

        Assert.Equal(["alpha", "beta"],
            landed.Select(e => e.Data.PartitionKey).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal));
    }

    [SkippableFact]
    public async Task Explicit_body_is_verbatim_and_content_type_configurable()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = Hub();
        var captured = await _emulator.CaptureNextAsync(hub);
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, true),
            new Field("payload", StringType.Default, true),
        ], null);

        using (var batch = new RecordBatch(schema,
        [
            new Int64Array.Builder().Append(1L).Build(),
            new StringArray.Builder().Append("not json, and not wrapped in any").Build(),
        ], 1))
        {
            await WriteAsync(hub, new Dictionary<string, object?>
            {
                ["body"] = "payload",
                ["content_type"] = "text/plain",
            }, batch);
        }

        var e = Assert.Single(await _emulator.ReadSinceAsync(hub, captured));

        // A named body column is the whole event: no envelope, and no other column of the row.
        Assert.Equal("not json, and not wrapped in any", Encoding.UTF8.GetString(e.EventBody.ToArray()));
        Assert.Equal("text/plain", e.ContentType);
    }

    [SkippableFact]
    public async Task Oversized_row_is_refused_naming_the_row()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = Hub();
        var captured = await _emulator.CaptureNextAsync(hub);
        var limit = await BatchLimitAsync(hub);

        // The ceiling is the service's, taken from the service rather than pinned to a number here,
        // and the body is chosen to clear any ceiling a namespace may raise it to.
        Assert.InRange(limit, 1_000_000, OversizedBodyBytes - 1);

        using var batch = BodyBatch(new string('x', OversizedBodyBytes));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await WriteAsync(hub, BodyOptions(), batch));

        Assert.StartsWith("PZEH0303:", ex.Message, StringComparison.Ordinal);

        // A row that cannot fit an empty batch will not fit the next one either: retrying is only a
        // slower way to fail, and the message has to say which row so the pipeline can drop it.
        Assert.False(ex.IsTransient);
        Assert.Contains("row 0", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"{limit} bytes", ex.Message, StringComparison.Ordinal);

        Assert.Empty(await _emulator.ReadSinceAsync(hub, captured));
    }

    /// <summary>The emulator answers a request for an entity it does not hold with a communication
    /// problem rather than the service's own not-found condition, so the code the write ends with is
    /// the one thing here only a real namespace can drive; <see cref="WriteSessionTests"/> carries the
    /// not-found mapping. What must hold against either is this: the write stops with a coded,
    /// redacted error naming the hub, at the latest on the first batch.</summary>
    [SkippableFact]
    public async Task Unknown_hub_fails_with_a_coded_error_naming_it()
    {
        DockerFacts.SkipUnlessDocker();
        ISinkConnector connector = new EventHubsConnector();
        await using var sink = await connector.OpenAsync(new ConnectorConfig(_emulator.ConnectionConfig()), CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            // Inside the assertion, both of them: the client's constructor never contacts the
            // service, so whether the absence surfaces at BeginWriteAsync or at the first batch is
            // the service's business, not a contract a fact may pin.
            using var batch = BodyBatch("nobody will read this");
            await using var session = await sink.BeginWriteAsync(
                Spec("nope", BodyOptions()), batch.Schema, CancellationToken.None);
            await session.WriteBatchAsync(batch, CancellationToken.None);
        });

        Assert.Matches("^PZEH0[0-9]{3}: eventhubs: ", ex.Message);
        Assert.Contains("nope", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(_emulator.ConnectionString, ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Abort_after_failed_batch_refuses_commit()
    {
        DockerFacts.SkipUnlessDocker();
        var hub = Hub();
        var captured = await _emulator.CaptureNextAsync(hub);

        ISinkConnector connector = new EventHubsConnector();
        await using var sink = await connector.OpenAsync(new ConnectorConfig(_emulator.ConnectionConfig()), CancellationToken.None);
        Assert.Equal(AbortSemantics.None, sink.AbortSemantics);

        using var good = BodyBatch("landed before the failure");
        await using var session = await sink.BeginWriteAsync(Spec(hub, BodyOptions()), good.Schema, CancellationToken.None);
        await session.WriteBatchAsync(good, CancellationToken.None);

        using (var oversized = BodyBatch(new string('x', OversizedBodyBytes)))
        {
            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                async () => await session.WriteBatchAsync(oversized, CancellationToken.None));
            Assert.StartsWith("PZEH0303:", ex.Message, StringComparison.Ordinal);
        }

        await session.AbortAsync(CancellationToken.None);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.CommitAsync(CancellationToken.None));

        // AbortSemantics.None, against the service rather than a fake: the batch that went out before
        // the failure is already visible downstream, and no abort can call it back.
        var e = Assert.Single(await _emulator.ReadSinceAsync(hub, captured));
        Assert.Equal("landed before the failure", Encoding.UTF8.GetString(e.EventBody.ToArray()));
    }

    private static OutputSpec Spec(string hub, Dictionary<string, object?> options) =>
        new("eventhubs", hub, "append", "fail_on_change", options);

    private static Schema RoutingSchema() => new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
        new Field("region", StringType.Default, true),
        new Field("n", Int64Type.Default, true),
        new Field("flag", BooleanType.Default, true),
    ], null);

    private static Dictionary<string, object?> RoutingOptions() => new()
    {
        ["partition_key"] = "region",
        ["properties"] = new List<object?> { "n", "flag" },
    };

    private static Dictionary<string, object?> BodyOptions() => new() { ["body"] = "payload" };

    /// <summary>One row whose single String column is the event body, verbatim.</summary>
    private static RecordBatch BodyBatch(string payload)
    {
        var schema = new Schema([new Field("payload", StringType.Default, true)], null);
        return new RecordBatch(schema, [new StringArray.Builder().Append(payload).Build()], 1);
    }

    /// <summary>The whole shape of a sink node's write: open, begin, hand over every batch, commit.
    /// Batches stay the caller's -- the engine owns them across the call, and so does a fact.</summary>
    private async Task<WriteResult> WriteAsync(string hub, Dictionary<string, object?> options, params RecordBatch[] batches)
    {
        ISinkConnector connector = new EventHubsConnector();
        await using var sink = await connector.OpenAsync(new ConnectorConfig(_emulator.ConnectionConfig()), CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(Spec(hub, options), batches[0].Schema, CancellationToken.None);
        foreach (var batch in batches)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        return await session.CommitAsync(CancellationToken.None);
    }

    /// <summary>The largest event the service will take, asked of the service itself through its own
    /// producer -- the same question the session's batch answers, put to a client that is not the one
    /// under test.</summary>
    private async Task<long> BatchLimitAsync(string hub)
    {
        await using var producer = new EventHubProducerClient(_emulator.ConnectionString, hub);
        using var probe = await producer.CreateBatchAsync();
        return probe.MaximumSizeInBytes;
    }

    /// <summary>This class's one hub, leased on first use and held for the run: an event hub cannot
    /// be emptied, so a hub is never handed back.</summary>
    private string Hub()
    {
        lock (LeaseGate)
        {
            return _hub ??= _emulator.LeaseHub();
        }
    }
}
