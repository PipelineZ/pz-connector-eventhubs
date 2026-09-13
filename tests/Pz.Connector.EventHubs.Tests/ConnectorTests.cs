using System.Text.Json;
using Pz.Connector.EventHubs.Tests.Fakes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class ConnectorTests
{
    private const string Cs = "Endpoint=sb://ns1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abcDEF123ghi=";

    [Fact]
    public void Config_schemas_are_json_objects()
    {
        var c = new EventHubsConnector();
        Assert.Equal(JsonValueKind.Object, JsonDocument.Parse(c.ConnectionConfigSchema).RootElement.ValueKind);
        Assert.Equal(JsonValueKind.Object, JsonDocument.Parse(c.DatasetConfigSchema).RootElement.ValueKind);
    }

    [Fact]
    public async Task Validate_aggregates_every_error()
    {
        var result = await new EventHubsConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "service_principal", ["bogus"] = 1 }), CancellationToken.None);
        Assert.False(result.IsValid);
        Assert.True(result.Errors.Count >= 5);
    }

    [Fact]
    public async Task Check_connection_reports_the_probe_message()
    {
        var factory = new FakeClientFactory { Probe = c => $"namespace {c.NamespaceHost}" };
        var check = await new EventHubsConnector(null, factory).CheckConnectionAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "connection_string", ["connection_string"] = Cs }), CancellationToken.None);
        Assert.True(check.Ok);
        Assert.Equal("namespace ns1.servicebus.windows.net", check.Message);
    }

    [Fact]
    public async Task Check_connection_failure_is_redacted_and_not_thrown()
    {
        var factory = new FakeClientFactory { Probe = _ => throw new UnauthorizedAccessException($"bad key abcDEF123ghi= in {Cs}") };
        var check = await new EventHubsConnector(null, factory).CheckConnectionAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "connection_string", ["connection_string"] = Cs }), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.DoesNotContain("abcDEF123ghi=", check.Message);
    }

    [Fact]
    public async Task Check_connection_with_invalid_config_is_a_failed_probe()
    {
        var check = await new EventHubsConnector().CheckConnectionAsync(new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.Contains("'auth' is required", check.Message);
    }

    [Fact]
    public async Task Real_factory_probe_for_a_connection_string_is_offline()
    {
        var errors = new List<string>();
        var config = EventHubsConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "connection_string", ["connection_string"] = Cs }), errors)!;
        var message = await EventHubsClientFactory.Instance.ProbeAsync(config, CancellationToken.None);
        Assert.Contains("ns1.servicebus.windows.net", message);
        Assert.DoesNotContain("abcDEF123ghi=", message);
    }
}
