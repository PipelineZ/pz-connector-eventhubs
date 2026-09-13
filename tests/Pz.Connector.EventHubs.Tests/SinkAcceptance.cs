using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>TestKit sink contract. The suite writes a fixed (id Int64, name String) schema; the
/// connector sends each row as {"id":..,"name":..}, so read-back parses the JSON bodies into the same
/// two columns. One hub per test-class instance (xunit instantiates per fact), leased on first use so a
/// fact that never reaches the output does not spend one -- an event hub cannot be emptied, so a hub is
/// never handed back. No Merge/Replace/Checkpoint outputs: none is declared.</summary>
[Collection("eventhubs")]
[Trait("Category", "Docker")]
public sealed class SinkAcceptance : SinkConnectorAcceptanceTests
{
    private readonly EmulatorFixture _emulator;
    private readonly Lazy<string> _hub;

    public SinkAcceptance(EmulatorFixture emulator)
    {
        _emulator = emulator;
        DockerFacts.SkipUnlessDocker();
        _hub = new Lazy<string>(emulator.LeaseHub);
    }

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new EventHubsConnector();

    protected override ConnectorConfig ValidConfig => new(_emulator.ConnectionConfig());

    protected override OutputSpec SmallOutput =>
        new("eventhubs", _hub.Value, "append", "fail_on_change", new Dictionary<string, object?>());

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var events = await _emulator.ReadAllAsync(spec.Output);
        if (events.Count == 0)
        {
            return [];
        }

        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var e in events)
        {
            using var document = JsonDocument.Parse(Encoding.UTF8.GetString(e.EventBody.ToArray()));
            ids.Append(document.RootElement.GetProperty("id").GetInt64());
            names.Append(document.RootElement.GetProperty("name").GetString()!);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, false)], null);
        return [new RecordBatch(schema, [ids.Build(), names.Build()], events.Count)];
    }
}
