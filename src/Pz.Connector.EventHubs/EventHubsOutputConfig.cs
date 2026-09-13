using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

/// <summary>Per-output write options. Column names are checked against the real schema at
/// BeginWriteAsync (<see cref="ValidateAgainst"/>); Parse only knows the option shapes. When no
/// <c>body:</c> column is named, the event body is every column not named by <c>partition_key:</c>
/// or <c>properties:</c>, serialized as one JSON object.</summary>
internal sealed record EventHubsOutputConfig(
    string EventHub, string? BodyColumn, string? PartitionKeyColumn, IReadOnlyList<string> PropertyColumns, string? ContentType)
{
    private static readonly string[] KnownKeys = ["event_hub", "body", "partition_key", "properties", "content_type"];
    private static readonly ArrowTypeId[] PartitionKeyTypes = [ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64];
    private static readonly ArrowTypeId[] PropertyTypes =
        [ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64, ArrowTypeId.Double, ArrowTypeId.Boolean, ArrowTypeId.Date32, ArrowTypeId.Timestamp];

    /// <summary>Exactly what <c>RowJsonWriter</c> can spell -- pz's v0 type matrix. A column outside
    /// it must be refused here, while the errors still aggregate and before a writer exists;
    /// reaching the writer with one throws mid-batch, after events have already been produced.</summary>
    private static readonly ArrowTypeId[] JsonTypes =
    [
        ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64, ArrowTypeId.Double,
        ArrowTypeId.Decimal128, ArrowTypeId.Boolean, ArrowTypeId.Date32, ArrowTypeId.Timestamp,
    ];

    public static EventHubsOutputConfig? Parse(OutputSpec spec, List<string> errors)
    {
        var start = errors.Count;
        var prefix = $"output '{spec.Output}'";
        foreach (var unknownKey in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"{prefix}: unknown write option '{unknownKey}'; known: event_hub, body, partition_key, properties, content_type");
        }

        var eventHub = spec.Output;
        if (spec.Options.TryGetValue("event_hub", out var eventHubRaw))
        {
            eventHub = eventHubRaw?.ToString() ?? "";
            if (eventHub.Length == 0)
            {
                errors.Add($"{prefix}: 'event_hub' must be a non-empty string");
            }
        }

        var partitionKey = Name(spec, "partition_key", prefix, errors);
        var body = Name(spec, "body", prefix, errors);

        var properties = new List<string>();
        if (spec.Options.TryGetValue("properties", out var propertiesRaw) && propertiesRaw is not null)
        {
            if (propertiesRaw is IEnumerable<object?> list && propertiesRaw is not string)
            {
                foreach (var item in list)
                {
                    if (item?.ToString() is { Length: > 0 } name)
                    {
                        properties.Add(name);
                    }
                    else
                    {
                        errors.Add($"{prefix}: 'properties' entries must be non-empty column names");
                    }
                }
            }
            else
            {
                errors.Add($"{prefix}: 'properties' must be a list of column names");
            }
        }

        string? contentType = null;
        if (spec.Options.TryGetValue("content_type", out var contentTypeRaw) && contentTypeRaw is not null)
        {
            contentType = contentTypeRaw.ToString();
            if (string.IsNullOrEmpty(contentType))
            {
                errors.Add($"{prefix}: 'content_type' must be a non-empty string");
            }
        }

        return errors.Count == start ? new EventHubsOutputConfig(eventHub, body, partitionKey, properties.ToArray(), contentType) : null;
    }

    public void ValidateAgainst(string output, Schema schema, List<string> errors)
    {
        var prefix = $"output '{output}'";
        Check(prefix, schema, "partition_key", PartitionKeyColumn, PartitionKeyTypes, errors);
        Check(prefix, schema, "body", BodyColumn, [ArrowTypeId.String], errors);
        foreach (var property in PropertyColumns)
        {
            Check(prefix, schema, "properties", property, PropertyTypes, errors);
        }

        // Every column the event body is built from, which is empty when 'body:' names one.
        foreach (var index in JsonColumnIndexes(schema))
        {
            var field = schema.FieldsList[index];
            if (!JsonTypes.Contains(field.DataType.TypeId))
            {
                errors.Add($"{Codes.ColumnRefused}: {prefix}: column '{field.Name}' is {field.DataType.TypeId}, which the event body's JSON "
                    + $"cannot carry; allowed: {string.Join(", ", JsonTypes)}. Name a 'body' column, or drop it from the pipeline's projection");
            }
        }
    }

    public IReadOnlyList<int> JsonColumnIndexes(Schema schema)
    {
        if (BodyColumn is not null)
        {
            return [];
        }

        var excluded = new HashSet<string>(PropertyColumns, StringComparer.Ordinal);
        if (PartitionKeyColumn is not null)
        {
            excluded.Add(PartitionKeyColumn);
        }

        var fields = schema.FieldsList;
        return Enumerable.Range(0, fields.Count).Where(i => !excluded.Contains(fields[i].Name)).ToArray();
    }

    // PropertyColumns is an IReadOnlyList<string>: compiler-generated record equality compares it by
    // reference (List<T>/arrays don't override Equals), so two configs parsed from separately built
    // property lists would never compare equal. Give the record real value equality instead.
    public bool Equals(EventHubsOutputConfig? other) =>
        other is not null
        && EventHub == other.EventHub
        && BodyColumn == other.BodyColumn
        && PartitionKeyColumn == other.PartitionKeyColumn
        && ContentType == other.ContentType
        && PropertyColumns.SequenceEqual(other.PropertyColumns, StringComparer.Ordinal);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(EventHub);
        hash.Add(BodyColumn);
        hash.Add(PartitionKeyColumn);
        hash.Add(ContentType);
        foreach (var property in PropertyColumns)
        {
            hash.Add(property);
        }

        return hash.ToHashCode();
    }

    private static string? Name(OutputSpec spec, string option, string prefix, List<string> errors)
    {
        if (!spec.Options.TryGetValue(option, out var raw) || raw is null)
        {
            return null;
        }

        var name = raw.ToString();
        if (string.IsNullOrEmpty(name))
        {
            errors.Add($"{prefix}: '{option}' must be a column name");
            return null;
        }

        return name;
    }

    private static void Check(string prefix, Schema schema, string option, string? column, ArrowTypeId[] allowed, List<string> errors)
    {
        if (column is null)
        {
            return;
        }

        var field = schema.FieldsList.FirstOrDefault(f => f.Name == column);
        if (field is null)
        {
            errors.Add($"{prefix}: '{option}' names column '{column}', which the pipeline does not produce");
        }
        else if (!allowed.Contains(field.DataType.TypeId))
        {
            errors.Add($"{prefix}: '{option}' column '{column}' is {field.DataType.TypeId}; allowed: {string.Join(", ", allowed)}");
        }
    }
}
