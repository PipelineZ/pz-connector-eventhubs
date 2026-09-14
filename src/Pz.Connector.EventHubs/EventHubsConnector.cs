using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

/// <summary>Azure Event Hubs for pz: an event hub is a feed-shaped dataset resumed from per-partition
/// sequence numbers the engine stores as the dataset's sync-state token; a sink output is an
/// append-only send. Capabilities: SyncState only -- one token per dataset means one pz partition
/// per dataset, and there is no SQL fragment DuckDB could scan a namespace with.</summary>
public sealed class EventHubsConnector : IConnector, ISourceConnector, ISinkConnector
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly IEventHubsClientFactory _factory;

    public EventHubsConnector(ILoggerFactory? loggerFactory = null) : this(loggerFactory, EventHubsClientFactory.Instance) { }

    internal EventHubsConnector(ILoggerFactory? loggerFactory, IEventHubsClientFactory factory)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _factory = factory;
    }

    public ConnectorInfo Info { get; } = new(
        "eventhubs",
        typeof(EventHubsConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities => ConnectorCapabilities.SyncState;

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["auth"], "properties": {
            "auth": { "enum": ["connection_string", "credential_chain", "service_principal", "managed_identity"] },
            "connection_string": { "type": "string" }, "namespace": { "type": "string" },
            "tenant_id": { "type": "string" }, "client_id": { "type": "string" }, "client_secret": { "type": "string" },
            "consumer_group": { "type": "string" },
            "transport": { "enum": ["amqp_tcp", "amqp_websockets"] },
            "idle_timeout": { "type": "integer", "minimum": 1, "maximum": 3600 } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "properties": {
            "event_hub": { "type": "string" }, "start": { "type": "string" }, "encoding": { "enum": ["utf8", "base64"] } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        EventHubsConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(errors.Count == 0 ? ValidationResult.Success : new ValidationResult(errors));
    }

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = EventHubsConnectionConfig.Parse(config, errors);
        if (connection is null)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
        }

        try
        {
            return new ConnectionCheck(true, await _factory.ProbeAsync(connection, ct).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Every failure is a failed probe, never a crash: reporting it is the whole point.
            return new ConnectionCheck(false, EventHubsErrors.Wrap(ex, connection.Redactor, Codes.InvalidConnection,
                $"namespace '{connection.NamespaceHost}': probing").Message);
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult<ISource>(new EventHubsSource(ParseOrThrow(config), _factory, _loggerFactory.CreateLogger<EventHubsSource>()));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult<ISink>(new EventHubsSink(ParseOrThrow(config), _factory, _loggerFactory.CreateLogger<EventHubsSink>()));

    internal static EventHubsConnectionConfig ParseOrThrow(ConnectorConfig config)
    {
        var errors = new List<string>();
        return EventHubsConnectionConfig.Parse(config, errors)
            ?? throw EventHubsErrors.Fatal(Codes.InvalidConnection, "invalid connection config: " + string.Join("; ", errors), EventHubsRedactor.None);
    }
}
