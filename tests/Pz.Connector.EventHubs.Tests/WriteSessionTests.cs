using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Messaging.EventHubs;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connector.EventHubs.Tests.Fakes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>The write session's grouping, batching, and failure-classification decisions, driven
/// through a scripted <see cref="FakeWriter"/> instead of a live namespace. Every fact here is
/// deterministic: the fake answers immediately, so nothing waits on wall-clock time.</summary>
public sealed class WriteSessionTests
{
    private const string Cs = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private static readonly Schema Schema = new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
        new Field("region", StringType.Default, true),
        new Field("n", Int32Type.Default, true),
    ], null);

    [Fact]
    public async Task Whole_row_json_excludes_partition_key_and_property_columns_and_sets_content_type()
    {
        var hub = new FakeHub();
        await using var session = Session(hub, partitionKey: "region", properties: ["n"]);

        using (var batch = Batch((1, "ann", "eu", 7)))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var group = Assert.Single(hub.Sent);
        Assert.Equal("eu", group.PartitionKey);
        var e = Assert.Single(group.Events);
        Assert.Equal("""{"id":1,"name":"ann"}""", Encoding.UTF8.GetString(e.Body));
        Assert.Equal("application/json", e.ContentType);
        Assert.Equal([new KeyValuePair<string, object>("n", 7L)], e.Properties);
    }

    [Fact]
    public async Task Explicit_body_column_is_verbatim_and_content_type_unset_by_default()
    {
        var hub = new FakeHub();
        await using var session = Session(hub, body: "name");

        using (var batch = Batch((1, "ann", "eu", 7)))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var group = Assert.Single(hub.Sent);
        Assert.Null(group.PartitionKey);
        var e = Assert.Single(group.Events);
        Assert.Equal("ann", Encoding.UTF8.GetString(e.Body));
        Assert.Null(e.ContentType);
    }

    [Fact]
    public async Task Rows_are_grouped_by_partition_key_preserving_order_and_null_key_is_its_own_group()
    {
        var hub = new FakeHub();
        await using var session = Session(hub, partitionKey: "region", body: "id");

        using (var batch = Batch((1, "a", "x", null), (2, "b", null, null), (3, "c", "y", null), (4, "d", "x", null), (5, "e", null, null)))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        Assert.Equal(3, hub.Sent.Count);
        Assert.Equal("x", hub.Sent[0].PartitionKey);
        Assert.Equal(["1", "4"], hub.Sent[0].Events.Select(e => Encoding.UTF8.GetString(e.Body)));
        Assert.Null(hub.Sent[1].PartitionKey);
        Assert.Equal(["2", "5"], hub.Sent[1].Events.Select(e => Encoding.UTF8.GetString(e.Body)));
        Assert.Equal("y", hub.Sent[2].PartitionKey);
        Assert.Equal(["3"], hub.Sent[2].Events.Select(e => Encoding.UTF8.GetString(e.Body)));
    }

    [Fact]
    public async Task Overflow_sends_the_open_batch_and_continues()
    {
        var hub = new FakeHub { MaxBatchBytes = 150 };
        await using var session = Session(hub, body: "name");

        using (var batch = Batch((0, "n0", null, null), (1, "n1", null, null), (2, "n2", null, null), (3, "n3", null, null), (4, "n4", null, null)))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        Assert.True(hub.Sent.Count >= 2);
        var all = hub.Sent.SelectMany(g => g.Events).Select(e => Encoding.UTF8.GetString(e.Body)).ToList();
        Assert.Equal(["n0", "n1", "n2", "n3", "n4"], all);
    }

    [Fact]
    public async Task Oversized_row_fails_non_transient_naming_the_row_and_limit()
    {
        var hub = new FakeHub { MaxBatchBytes = 50 };
        await using var session = Session(hub, body: "name");

        using var batch = Batch((0, "n0", null, null));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await session.WriteBatchAsync(batch, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("row 0", ex.Message);
        Assert.Contains("50 bytes", ex.Message);
    }

    [Fact]
    public async Task Commit_returns_rows_and_batches()
    {
        var hub = new FakeHub();
        await using var session = Session(hub, body: "name");

        using (var batch = Batch((0, "n0", null, null), (1, "n1", null, null)))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        using (var batch = Batch((2, "n2", null, null), (3, "n3", null, null), (4, "n4", null, null)))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var result = await session.CommitAsync(CancellationToken.None);
        Assert.Equal(new WriteResult(5, 2), result);
    }

    [Fact]
    public async Task Abort_then_commit_is_refused_and_writer_disposed_once()
    {
        var hub = new FakeHub();
        var writer = new FakeWriter(hub);
        var session = Session(writer);

        using (var batch = Batch((0, "n0", null, null)))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        await session.AbortAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.CommitAsync(CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            using var batch = Batch((1, "n1", null, null));
            await session.WriteBatchAsync(batch, CancellationToken.None);
        });

        // Idempotent: a repeated dispose must not release the underlying writer twice.
        await session.DisposeAsync();
        await session.DisposeAsync();
        Assert.Equal(1, writer.DisposeCalls);
    }

    [Fact]
    public async Task Send_failure_is_wrapped_with_PZEH0304()
    {
        var hub = new FakeHub();
        var writer = new FakeWriter(hub) { FailSend = new EventHubsException("h", "boom", EventHubsException.FailureReason.ServiceBusy) };
        await using var session = Session(writer);

        using var batch = Batch((0, "n0", null, null));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await session.WriteBatchAsync(batch, CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Contains("PZEH0304", ex.Message);
    }

    [Fact]
    public async Task Unknown_hub_at_begin_write_is_PZEH0204()
    {
        var factory = new FakeClientFactory();
        var connection = EventHubsConnector.ParseOrThrow(
            new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "connection_string", ["connection_string"] = Cs }));
        var sink = new EventHubsSink(connection, factory, NullLogger.Instance);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(new OutputSpec("eventhubs", "h", "append", "fail_on_change", new Dictionary<string, object?>()),
                Schema, CancellationToken.None));

        Assert.StartsWith("PZEH0204:", ex.Message);
    }

    private static EventHubsWriteSession Session(FakeHub hub, string? partitionKey = null, string? body = null, IReadOnlyList<string>? properties = null) =>
        Session(new FakeWriter(hub), partitionKey, body, properties);

    private static EventHubsWriteSession Session(
        FakeWriter writer, string? partitionKey = null, string? body = null, IReadOnlyList<string>? properties = null)
    {
        var options = new Dictionary<string, object?>();
        if (partitionKey is not null) options["partition_key"] = partitionKey;
        if (body is not null) options["body"] = body;
        if (properties is not null) options["properties"] = properties.Cast<object?>().ToList();

        var errors = new List<string>();
        var output = EventHubsOutputConfig.Parse(new OutputSpec("eventhubs", "h", "append", "fail_on_change", options), errors);
        Assert.Empty(errors);
        return new EventHubsWriteSession(writer, output!, Schema, "ns.servicebus.windows.net", EventHubsRedactor.None, NullLogger.Instance);
    }

    private static RecordBatch Batch(params (long Id, string? Name, string? Region, int? N)[] rows)
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        var regions = new StringArray.Builder();
        var ns = new Int32Array.Builder();
        foreach (var r in rows)
        {
            ids.Append(r.Id);
            if (r.Name is null) names.AppendNull(); else names.Append(r.Name);
            if (r.Region is null) regions.AppendNull(); else regions.Append(r.Region);
            if (r.N is null) ns.AppendNull(); else ns.Append(r.N.Value);
        }

        return new RecordBatch(Schema, [ids.Build(), names.Build(), regions.Build(), ns.Build()], rows.Length);
    }
}
