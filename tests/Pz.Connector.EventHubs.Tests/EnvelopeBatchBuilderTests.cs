using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class EnvelopeBatchBuilderTests
{
    private static readonly DateTimeOffset T0 = new(2026, 9, 13, 10, 0, 0, TimeSpan.Zero);

    private static ReceivedEvent Ev(long seq, byte[] body, params (string, object?)[] props) =>
        new(seq, $"o{seq}", T0, "k", body, "application/json", props.Select(p => new KeyValuePair<string, object?>(p.Item1, p.Item2)).ToList());

    [Fact]
    public void Schema_is_the_nine_column_envelope()
    {
        Assert.Equal(["event_hub", "partition", "sequence_number", "offset", "enqueued_time", "partition_key", "body", "properties", "content_type"],
            EnvelopeBatchBuilder.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void Utf8_body_lands_as_text_and_empty_body_as_empty_string()
    {
        var b = new EnvelopeBatchBuilder(PayloadEncoding.Utf8, new BatchOptions(), EventHubsRedactor.None);
        b.Append("h", "0", Ev(1, "{\"a\":1}"u8.ToArray()));
        b.Append("h", "0", Ev(2, []));
        using var batch = b.Flush()!;
        var body = (StringArray)batch.Column(6);
        Assert.Equal("{\"a\":1}", body.GetString(0));
        Assert.Equal("", body.GetString(1));
        Assert.Equal("0", ((StringArray)batch.Column(1)).GetString(0));
        Assert.Equal(2L, ((Int64Array)batch.Column(2)).GetValue(1));
        Assert.Equal("o1", ((StringArray)batch.Column(3)).GetString(0));
        Assert.Equal("k", ((StringArray)batch.Column(5)).GetString(0));
        Assert.Equal("application/json", ((StringArray)batch.Column(8)).GetString(0));
    }

    [Fact]
    public void Invalid_utf8_names_the_event_and_points_at_base64()
    {
        var b = new EnvelopeBatchBuilder(PayloadEncoding.Utf8, new BatchOptions(), EventHubsRedactor.None);
        var ex = Assert.Throws<PzConnectorException>(() => b.Append("h", "1", Ev(7, [0xFF, 0xFE])));
        Assert.StartsWith("PZEH0205:", ex.Message);
        Assert.Contains("event hub 'h' partition 1 sequence number 7", ex.Message);
        Assert.Contains("encoding: base64", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Base64_encoding_lands_raw_bytes()
    {
        var b = new EnvelopeBatchBuilder(PayloadEncoding.Base64, new BatchOptions(), EventHubsRedactor.None);
        b.Append("h", "0", Ev(1, [0xFF, 0xFE]));
        using var batch = b.Flush()!;
        Assert.Equal("//4=", ((StringArray)batch.Column(6)).GetString(0));
    }

    [Fact]
    public void Properties_are_typed_json()
    {
        var json = EnvelopeBatchBuilder.PropertiesToJson(
        [
            new("s", "x"), new("i", 5), new("l", 7L), new("d", 1.5), new("b", true), new("n", null),
            new("t", new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero)), new("g", Guid.Empty),
            new("bytes", new byte[] { 1, 2 }), new("nan", double.NaN),
        ]);
        Assert.Equal("""{"s":"x","i":5,"l":7,"d":1.5,"b":true,"n":null,"t":"2026-01-02T03:04:05.000000Z","g":"00000000-0000-0000-0000-000000000000","bytes":"AQI=","nan":null}""", json);
        Assert.Equal("{}", EnvelopeBatchBuilder.PropertiesToJson([]));
    }

    [Fact]
    public void Batches_split_at_max_rows()
    {
        var b = new EnvelopeBatchBuilder(PayloadEncoding.Utf8, new BatchOptions(MaxRowsPerBatch: 2), EventHubsRedactor.None);
        for (var i = 0; i < 3; i++) b.Append("h", "0", Ev(i, "x"u8.ToArray()));
        Assert.True(b.TryTakeBatch(out var first));
        Assert.Equal(2, first!.Length);
        first.Dispose();
        Assert.False(b.TryTakeBatch(out _));
        using var rest = b.Flush()!;
        Assert.Equal(1, rest.Length);
    }
}
