using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connector.EventHubs.Tests.Fakes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>Sink surface exercised the way the engine reaches it -- through
/// <see cref="ISinkConnector.OpenAsync"/> -- so the connector's wiring (ParseOrThrow, the logger
/// factory) is covered, not just <see cref="EventHubsSink"/> in isolation.</summary>
public sealed class SinkTests
{
    private const string Cs = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private static readonly Schema Schema = new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
    ], null);

    private static OutputSpec Spec(string mode = "append", Dictionary<string, object?>? options = null) =>
        new("eventhubs", "h", mode, "fail_on_change", options ?? []);

    private static async Task<ISink> OpenSinkAsync(FakeClientFactory factory)
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "connection_string", ["connection_string"] = Cs });
        return await ((ISinkConnector)new EventHubsConnector(null, factory)).OpenAsync(config, CancellationToken.None);
    }

    [Fact]
    public async Task Abort_semantics_is_none()
    {
        var sink = await OpenSinkAsync(new FakeClientFactory { Hubs = { ["h"] = new FakeHub() } });

        Assert.Equal(AbortSemantics.None, sink.AbortSemantics);
    }

    [Fact]
    public async Task There_is_no_native_copy()
    {
        var sink = await OpenSinkAsync(new FakeClientFactory { Hubs = { ["h"] = new FakeHub() } });

        Assert.False(sink.TryGetNativeCopy(Spec(), out var copy));
        Assert.Null(copy);
    }

    [Fact]
    public async Task Replace_mode_is_refused_as_a_named_config_error()
    {
        var sink = await OpenSinkAsync(new FakeClientFactory { Hubs = { ["h"] = new FakeHub() } });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            sink.BeginWriteAsync(Spec("replace"), Schema, CancellationToken.None).AsTask());

        Assert.StartsWith("PZEH0301:", ex.Message);
        Assert.False(ex.IsTransient);
        Assert.Contains("append-only", ex.Message);
    }

    [Fact]
    public async Task Unknown_hub_is_a_named_config_error()
    {
        var sink = await OpenSinkAsync(new FakeClientFactory());

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            sink.BeginWriteAsync(Spec(), Schema, CancellationToken.None).AsTask());

        Assert.StartsWith("PZEH0204:", ex.Message);
    }
}
