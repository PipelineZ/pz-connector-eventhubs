using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

internal enum StartKind { Earliest, Latest, Timestamp }

/// <summary>Where a partition with no stored sequence number begins: the first run, --full-refresh,
/// or a partition added since the token was written.</summary>
internal sealed record StartPosition(StartKind Kind, DateTimeOffset? Timestamp);

internal enum PayloadEncoding { Utf8, Base64 }

/// <summary>Per-dataset read options: <c>event_hub</c> (defaults to the entity name), <c>start</c>, <c>encoding</c>.</summary>
internal sealed record EventHubsDatasetConfig(string EventHub, StartPosition Start, PayloadEncoding Encoding)
{
    private static readonly string[] KnownKeys = ["event_hub", "start", "encoding"];

    public static EventHubsDatasetConfig? Parse(DatasetSpec spec, List<string> errors)
    {
        var start = errors.Count;
        foreach (var key in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"dataset '{spec.Dataset}': unknown read option '{key}'; known: event_hub, start, encoding");
        }

        var eventHub = spec.Dataset;
        if (spec.Options.TryGetValue("event_hub", out var eventHubRaw))
        {
            eventHub = eventHubRaw?.ToString() ?? "";
            if (eventHub.Length == 0)
            {
                errors.Add($"dataset '{spec.Dataset}': 'event_hub' must be a non-empty string");
            }
        }

        var position = new StartPosition(StartKind.Earliest, null);
        if (spec.Options.TryGetValue("start", out var startRaw) && startRaw is not null)
        {
            var text = startRaw.ToString()!;
            if (text == "earliest")
            {
                position = new StartPosition(StartKind.Earliest, null);
            }
            else if (text == "latest")
            {
                position = new StartPosition(StartKind.Latest, null);
            }
            else if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
                         DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
            {
                position = new StartPosition(StartKind.Timestamp, ts);
            }
            else
            {
                errors.Add($"dataset '{spec.Dataset}': 'start' must be earliest, latest, or an ISO-8601 timestamp; got '{text}'");
            }
        }

        var encoding = PayloadEncoding.Utf8;
        if (spec.Options.TryGetValue("encoding", out var encodingRaw) && encodingRaw is not null)
        {
            encoding = encodingRaw.ToString() switch
            {
                "utf8" => PayloadEncoding.Utf8,
                "base64" => PayloadEncoding.Base64,
                var other => Bad(errors, spec.Dataset, other),
            };
        }

        return errors.Count == start ? new EventHubsDatasetConfig(eventHub, position, encoding) : null;

        static PayloadEncoding Bad(List<string> errors, string dataset, string? value)
        {
            errors.Add($"dataset '{dataset}': 'encoding' must be utf8 or base64; got '{value}'");
            return PayloadEncoding.Utf8;
        }
    }
}
