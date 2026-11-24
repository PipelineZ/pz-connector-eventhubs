using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

/// <summary>One row of an Arrow batch as a UTF-8 JSON object over a fixed projection of columns,
/// property order = schema order of the projection. Hand-written over the v0 type matrix: no
/// reflection, and the number/string choices are a documented contract (Decimal128 as a string to
/// keep precision; NaN/Infinity as null because JSON has no spelling for them).</summary>
internal sealed class RowJsonWriter(Schema schema, IReadOnlyList<int> columns)
{
    // The default encoder escapes ASCII punctuation (e.g. '"' as ") for safe HTML embedding,
    // which this event body is never rendered into; relaxed escaping keeps the JSON minimal
    // (\" for a quote) and is still spec-valid since only '"', '\', and control characters are escaped.
    private static readonly JsonWriterOptions Options = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private readonly string[] _names = columns.Select(i => schema.FieldsList[i].Name).ToArray();
    private readonly MemoryStream _buffer = new();

    public byte[] Write(RecordBatch batch, int row)
    {
        _buffer.SetLength(0);
        using (var writer = new Utf8JsonWriter(_buffer, Options))
        {
            writer.WriteStartObject();
            for (var c = 0; c < columns.Count; c++)
            {
                var column = batch.Column(columns[c]);
                writer.WritePropertyName(_names[c]);
                WriteValue(writer, column, row);
            }

            writer.WriteEndObject();
        }

        return _buffer.ToArray();
    }

    /// <summary>One cell, at any depth. Leaves are spelled exactly as a flat column's are (Decimal128
    /// as a string, timestamps as microsecond ISO-8601 UTC, non-finite doubles as null), so a field
    /// inside a struct reads the same as the same type at the top level; a struct is an object, a
    /// list an array, a map an object keyed by the key's text. Offsets are read the way the Arrow
    /// layout stores them: list and map offsets are absolute into the child, a fixed-size list
    /// starts at (array offset + row) * size.</summary>
    private static void WriteValue(Utf8JsonWriter writer, IArrowArray column, int row)
    {
        if (column.IsNull(row))
        {
            writer.WriteNullValue();
            return;
        }

        switch (column)
        {
            case Int32Array a: writer.WriteNumberValue(a.GetValue(row)!.Value); break;
            case Int64Array a: writer.WriteNumberValue(a.GetValue(row)!.Value); break;
            case DoubleArray a:
                var d = a.GetValue(row)!.Value;
                if (double.IsFinite(d)) writer.WriteNumberValue(d); else writer.WriteNullValue();
                break;
            case BooleanArray a: writer.WriteBooleanValue(a.GetValue(row)!.Value); break;
            case MapArray a:
            {
                // Two keys with the same text would repeat one property, which JSON parsers resolve differently.
                var entries = a.KeyValues;
                var seen = new HashSet<string>(StringComparer.Ordinal);
                writer.WriteStartObject();
                for (var i = a.ValueOffsets[row]; i < a.ValueOffsets[row + 1]; i++)
                {
                    var key = ArrowScalars.Format(entries.Fields[0], i)
                        ?? throw new PzConnectorException("a map entry has a null key, which a JSON object cannot hold", isTransient: false);
                    if (!seen.Add(key))
                    {
                        throw new PzConnectorException(
                            "a map holds two entries whose keys have the same text, which a JSON object cannot hold -- make the keys distinct in the pipeline",
                            isTransient: false);
                    }

                    writer.WritePropertyName(key);
                    WriteValue(writer, entries.Fields[1], i);
                }

                writer.WriteEndObject();
                break;
            }

            case ListArray a:
                writer.WriteStartArray();
                for (var i = a.ValueOffsets[row]; i < a.ValueOffsets[row + 1]; i++)
                {
                    WriteValue(writer, a.Values, i);
                }

                writer.WriteEndArray();
                break;
            case FixedSizeListArray a:
            {
                var size = ((FixedSizeListType)a.Data.DataType).ListSize;
                var start = (a.Offset + row) * size;
                writer.WriteStartArray();
                for (var i = start; i < start + size; i++)
                {
                    WriteValue(writer, a.Values, i);
                }

                writer.WriteEndArray();
                break;
            }

            case StructArray a:
            {
                var fields = ((StructType)a.Data.DataType).Fields;
                writer.WriteStartObject();
                for (var k = 0; k < fields.Count; k++)
                {
                    writer.WritePropertyName(fields[k].Name);
                    WriteValue(writer, a.Fields[k], row);
                }

                writer.WriteEndObject();
                break;
            }

            default: writer.WriteStringValue(ArrowScalars.Format(column, row)); break;
        }
    }
}

/// <summary>Scalar text formatting shared by JSON string values, row keys, and property values.</summary>
internal static class ArrowScalars
{
    public static string? Format(IArrowArray column, int row)
    {
        if (column.IsNull(row))
        {
            return null;
        }

        return column switch
        {
            StringArray a => a.GetString(row),
            Int32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            DoubleArray a => a.GetValue(row)!.Value.ToString("R", CultureInfo.InvariantCulture),
            // Through SqlDecimal, never GetValue: System.Decimal holds 28-29 significant digits, and
            // for a wider Decimal128 GetValue returns the value with the excess digits dropped rather
            // than throwing, so a 38-digit amount would land silently truncated.
            // SqlDecimal.ToString is culture-independent and renders every stored digit at the
            // column's scale, which for values that fit is byte-identical to the decimal rendering.
            Decimal128Array a => a.GetSqlDecimal(row)!.Value.ToString(),
            BooleanArray a => a.GetValue(row)!.Value ? "true" : "false",
            Date32Array a => a.GetDateOnly(row)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            TimestampArray a => a.GetTimestamp(row)!.Value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture),
            _ => throw new NotSupportedException($"column type {column.Data.DataType.TypeId} is outside pz's type matrix"),
        };
    }

    public static byte[]? Bytes(IArrowArray column, int row) =>
        Format(column, row) is { } text ? Encoding.UTF8.GetBytes(text) : null;

    /// <summary>An application-property value with its type kept where AMQP has one: integers as
    /// long, doubles as double, booleans as bool, everything else as the same text Format spells.</summary>
    public static object? PropertyValue(IArrowArray column, int row)
    {
        if (column.IsNull(row))
        {
            return null;
        }

        return column switch
        {
            Int32Array a => (long)a.GetValue(row)!.Value,
            Int64Array a => a.GetValue(row)!.Value,
            DoubleArray a => a.GetValue(row)!.Value,
            BooleanArray a => a.GetValue(row)!.Value,
            _ => Format(column, row),
        };
    }
}
