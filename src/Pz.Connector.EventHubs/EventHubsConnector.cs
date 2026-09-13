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

    public EventHubsConnector(ILoggerFactory? loggerFactory = null)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
    }

    public ConnectorInfo Info { get; } = new(
        "eventhubs",
        typeof(EventHubsConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities => ConnectorCapabilities.SyncState;

    public string ConnectionConfigSchema => "{}";

    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => throw new NotImplementedException();

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => throw new NotImplementedException();

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => throw new NotImplementedException();

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => throw new NotImplementedException();
}
