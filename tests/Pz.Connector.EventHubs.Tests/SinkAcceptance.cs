using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>TestKit sink contract. The suite writes a fixed (id Int64, name String) schema; the
/// connector sends each row as {"id":..,"name":..}, so read-back parses the JSON bodies into the same
/// two columns. One hub for the whole suite -- the namespace admits ten entities in total, far fewer
/// than one per fact -- with the facts held apart by position rather than by hub: xunit builds the
/// class once per fact, so each fact captures where the hub ends before it writes and read-back
/// begins there. No Merge/Replace/Checkpoint outputs: none is declared.</summary>
[Collection("eventhubs")]
[Trait("Category", "Docker")]
public sealed class SinkAcceptance : SinkConnectorAcceptanceTests, IAsyncLifetime
{
    private static readonly Lock LeaseGate = new();
    private static string? _hub;

    private readonly EmulatorFixture _emulator;
    private readonly string _hubName;
    private Dictionary<string, long> _captured = [];

    public SinkAcceptance(EmulatorFixture emulator)
    {
        _emulator = emulator;
        DockerFacts.SkipUnlessDocker();
        _hubName = LeasedHub(emulator);
    }

    /// <summary>xunit awaits this after the constructor and before the fact, which is the one place
    /// the capture both has a service to ask and is still ahead of everything the fact writes.</summary>
    public async Task InitializeAsync() => _captured = await _emulator.CaptureNextAsync(_hubName);

    /// <summary>Nothing to release: the hub is the run's, not this instance's.</summary>
    public Task DisposeAsync() => Task.CompletedTask;

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new EventHubsConnector();

    protected override ConnectorConfig ValidConfig => new(_emulator.ConnectionConfig());

    protected override OutputSpec SmallOutput =>
        new("eventhubs", _hubName, "append", "fail_on_change", new Dictionary<string, object?>());

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var events = await _emulator.ReadSinceAsync(spec.Output, _captured);
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

    /// <summary>The suite's one hub, leased on first use and held for the run: an event hub cannot be
    /// emptied, so a hub is never handed back.</summary>
    private static string LeasedHub(EmulatorFixture emulator)
    {
        lock (LeaseGate)
        {
            return _hub ??= emulator.LeaseHub();
        }
    }
}
