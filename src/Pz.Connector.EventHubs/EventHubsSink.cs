using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Azure.Messaging.EventHubs;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

/// <summary>Append-only send. No native copy (DuckDB cannot speak the AMQP protocol) and
/// <see cref="AbortSemantics.None"/>: an event handed to the service is already visible downstream,
/// so abort only refuses further writes -- it never unsends what already went out.</summary>
internal sealed class EventHubsSink(EventHubsConnectionConfig connection, IEventHubsClientFactory factory, ILogger logger) : ISink
{
    public AbortSemantics AbortSemantics => AbortSemantics.None;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var errors = new List<string>();
        var output = EventHubsOutputConfig.Parse(spec, errors);
        output?.ValidateAgainst(spec.Output, schema, errors);
        if (!string.Equals(spec.Mode, "append", StringComparison.Ordinal))
        {
            errors.Add($"output '{spec.Output}': mode '{spec.Mode}' is not supported; eventhubs is append-only");
        }

        if (output is null || errors.Count > 0)
        {
            // Parse and ValidateAgainst already name the output in every message they add; the
            // type-matrix refusals inside that list carry their own PZEH0302 prefix, but the whole
            // aggregate is still reported as one PZEH0301 failure.
            throw EventHubsErrors.Fatal(Codes.InvalidOutput, string.Join("; ", errors), connection.Redactor);
        }

        IEventHubWriter writer;
        try
        {
            writer = factory.CreateWriter(connection, output.EventHub);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw Classify(ex, output.EventHub);
        }

        return ValueTask.FromResult<ISinkWriteSession>(
            new EventHubsWriteSession(writer, output, schema, connection.NamespaceHost, connection.Redactor, logger));
    }

    private PzConnectorException Classify(Exception ex, string eventHub) =>
        ex is EventHubsException { Reason: EventHubsException.FailureReason.ResourceNotFound }
            ? EventHubsErrors.Fatal(Codes.HubNotFound,
                $"event hub '{eventHub}' in namespace '{connection.NamespaceHost}' does not exist; create it first", connection.Redactor)
            : EventHubsErrors.Wrap(ex, connection.Redactor, Codes.HubNotFound, $"event hub '{eventHub}': building the writer");

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
