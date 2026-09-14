using System.Text;
using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

/// <summary>The dataset's sync-state token: the next sequence number to read per partition of one
/// event hub in one namespace. Written with stable key order so the engine's stored state is
/// byte-stable across runs that changed nothing. The namespace is part of the token because
/// sequence numbers are only meaningful inside the namespace that issued them: the same hub name in
/// another namespace numbers its partitions independently, so resuming there would silently skip or
/// re-read events. Parsing never places the token text in an error: it is engine-opaque state and
/// must not surface in run artifacts.</summary>
internal sealed record SequenceToken(string Namespace, string EventHub, IReadOnlyDictionary<string, long> Next)
{
    public const int Version = 1;

    public string Serialize()
    {
        var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("v", Version);
            writer.WriteString("namespace", Namespace);
            writer.WriteString("event_hub", EventHub);
            writer.WriteStartObject("partitions");
            foreach (var partition in Ordered(Next.Keys))
            {
                writer.WriteNumber(partition, Next[partition]);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    public static SequenceToken Parse(string json, string expectedNamespace, string expectedEventHub, EventHubsRedactor redactor)
    {
        static PzConnectorException Malformed(EventHubsRedactor redactor, string why) => EventHubsErrors.Fatal(Codes.BadToken,
            $"stored sync state is not an eventhubs sequence token ({why}); run with --full-refresh or clear the dataset's state with `pz state`", redactor);

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (JsonException)
        {
            throw Malformed(redactor, "not JSON");
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("v", out var v))
            {
                throw Malformed(redactor, "missing version");
            }

            if (!v.TryGetInt32(out var version))
            {
                throw Malformed(redactor, "version is not an integer");
            }

            if (version != Version)
            {
                throw Malformed(redactor, $"version {version}, expected {Version}");
            }

            if (!root.TryGetProperty("namespace", out var namespaceElement) || namespaceElement.ValueKind != JsonValueKind.String)
            {
                throw Malformed(redactor, "missing namespace");
            }

            if (!root.TryGetProperty("event_hub", out var eventHubElement) || eventHubElement.ValueKind != JsonValueKind.String
                || !root.TryGetProperty("partitions", out var partitions) || partitions.ValueKind != JsonValueKind.Object)
            {
                throw Malformed(redactor, "missing event_hub or partitions");
            }

            var ns = namespaceElement.GetString()!;
            if (!string.Equals(ns, expectedNamespace, StringComparison.Ordinal))
            {
                throw EventHubsErrors.Fatal(Codes.BadToken,
                    $"stored sync state belongs to namespace '{ns}' but the connection now points at '{expectedNamespace}'; " +
                    "run with --full-refresh to start over, or point the connection back at the original namespace", redactor);
            }

            var eventHub = eventHubElement.GetString()!;
            if (!string.Equals(eventHub, expectedEventHub, StringComparison.Ordinal))
            {
                throw EventHubsErrors.Fatal(Codes.BadToken,
                    $"stored sync state belongs to event hub '{eventHub}' but the dataset now reads event hub '{expectedEventHub}'; " +
                    "run with --full-refresh to start over, or point the dataset back at the original event hub", redactor);
            }

            var next = new Dictionary<string, long>();
            foreach (var property in partitions.EnumerateObject())
            {
                if (property.Name.Length == 0 || !property.Value.TryGetInt64(out var sequence) || sequence < 0)
                {
                    throw Malformed(redactor, "bad partition entry");
                }

                next[property.Name] = sequence;
            }

            return new SequenceToken(ns, eventHub, next);
        }
    }

    private static IEnumerable<string> Ordered(IEnumerable<string> keys) => PartitionIds.Order(keys, k => k);
}
