using System.Globalization;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.EventHubs;

/// <summary>Pivots received events into the fixed envelope through the ABI's
/// <see cref="ArrowBatchBuilder"/>, so batch buffers come from the pooled native allocator the
/// engine expects. The body is text: UTF-8 decoded strictly (a byte sequence that is not UTF-8 is
/// a refusal naming the event, never a replacement character -- silently altered data is worse
/// than a stopped feed), or base64 when the dataset says so.</summary>
internal sealed class EnvelopeBatchBuilder
{
    public static readonly Schema Schema = new(
    [
        new Field("event_hub", StringType.Default, nullable: false),
        new Field("partition", StringType.Default, nullable: false),
        new Field("sequence_number", Int64Type.Default, nullable: false),
        new Field("offset", StringType.Default, nullable: false),
        new Field("enqueued_time", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false),
        new Field("partition_key", StringType.Default, nullable: true),
        new Field("body", StringType.Default, nullable: false),
        new Field("properties", StringType.Default, nullable: false),
        new Field("content_type", StringType.Default, nullable: true),
    ], null);

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly ArrowBatchBuilder _inner;
    private readonly PayloadEncoding _encoding;
    private readonly EventHubsRedactor _redactor;
    private readonly object?[] _row = new object?[Schema.FieldsList.Count];

    public EnvelopeBatchBuilder(PayloadEncoding encoding, BatchOptions options, EventHubsRedactor redactor)
    {
        _inner = new ArrowBatchBuilder(Schema, options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);
        _encoding = encoding;
        _redactor = redactor;
    }

    public int PendingRows => _inner.PendingRows;

    // The row cap is enforced by the inner builder and only observed when the caller polls
    // TryTakeBatch -- a batch is never held here, since a RecordBatch owns pooled native buffers
    // that only the caller's dispose (via the engine) can release; queuing one behind this class
    // would put buffers out of the engine's sight. Callers must poll TryTakeBatch after every
    // Append for the split to land at the configured row/byte boundary rather than only at Flush.
    public void Append(string eventHub, string partition, ReceivedEvent e)
    {
        _row[0] = eventHub;
        _row[1] = partition;
        _row[2] = e.SequenceNumber;
        _row[3] = e.Offset;
        _row[4] = e.EnqueuedTime.ToUniversalTime();
        _row[5] = e.PartitionKey;
        _row[6] = Decode(e.Body, eventHub, partition, e.SequenceNumber);
        _row[7] = PropertiesToJson(e.Properties);
        _row[8] = e.ContentType;
        _inner.AppendRow(_row);
    }

    public bool TryTakeBatch(out RecordBatch? batch) => _inner.TryTakeBatch(out batch);

    public RecordBatch? Flush() => _inner.Flush();

    public static string PropertiesToJson(IReadOnlyList<KeyValuePair<string, object?>> properties)
    {
        if (properties.Count == 0)
        {
            return "{}";
        }

        var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var (name, value) in properties)
            {
                writer.WritePropertyName(name);
                WriteValue(writer, value);
            }

            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.ToArray());

        static void WriteValue(Utf8JsonWriter writer, object? value)
        {
            switch (value)
            {
                case null:
                    writer.WriteNullValue();
                    break;
                case string s:
                    writer.WriteStringValue(s);
                    break;
                case bool b:
                    writer.WriteBooleanValue(b);
                    break;
                case sbyte or byte or short or ushort or int or uint or long:
                    writer.WriteNumberValue(Convert.ToInt64(value, CultureInfo.InvariantCulture));
                    break;
                case ulong ul:
                    writer.WriteNumberValue(ul);
                    break;
                case float f:
                    if (float.IsFinite(f)) writer.WriteNumberValue(f); else writer.WriteNullValue();
                    break;
                case double d:
                    if (double.IsFinite(d)) writer.WriteNumberValue(d); else writer.WriteNullValue();
                    break;
                case decimal dec:
                    writer.WriteNumberValue(dec);
                    break;
                case DateTime dt:
                    writer.WriteStringValue(dt.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture));
                    break;
                case DateTimeOffset dto:
                    writer.WriteStringValue(dto.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture));
                    break;
                case byte[] bytes:
                    writer.WriteStringValue(Convert.ToBase64String(bytes));
                    break;
                case ArraySegment<byte> segment:
                    writer.WriteStringValue(Convert.ToBase64String(segment));
                    break;
                case Guid or Uri or TimeSpan or char:
                    writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture));
                    break;
                default:
                    writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture) ?? "");
                    break;
            }
        }
    }

    private string Decode(byte[] bytes, string eventHub, string partition, long sequenceNumber)
    {
        if (_encoding == PayloadEncoding.Base64)
        {
            return Convert.ToBase64String(bytes);
        }

        return TryUtf8(bytes) ?? throw EventHubsErrors.Fatal(Codes.BodyNotUtf8,
            $"event hub '{eventHub}' partition {partition} sequence number {sequenceNumber}: the event body is not valid UTF-8; " +
            "set `encoding: base64` on the dataset to land raw bytes as base64 text", _redactor);
    }

    private static string? TryUtf8(byte[] bytes)
    {
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }
}
