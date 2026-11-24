using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class NestedJsonTests
{
    private static readonly Decimal128Type Dec = new(38, 3);
    private static readonly TimestampType Ts = new(TimeUnit.Microsecond, "UTC");

    private static RecordBatch Batch(out Schema schema)
    {
        var recType = new StructType([
            new Field("amt", Dec, true),
            new Field("at", Ts, true),
            new Field("d", Date32Type.Default, true),
            new Field("f", DoubleType.Default, true),
            new Field("tags", new ListType(new Field("item", StringType.Default, true)), true),
        ]);
        var tags = new ListArray.Builder(StringType.Default);
        var tagValues = (StringArray.Builder)tags.ValueBuilder;
        tags.Append();
        tagValues.Append("a");
        tagValues.AppendNull();
        tags.Append();
        var at = new DateTimeOffset(2026, 9, 7, 10, 11, 12, TimeSpan.Zero).AddTicks(1234560);
        var rec = new StructArray(recType, 2,
            [
                new Decimal128Array.Builder(Dec).Append(12.345m).AppendNull().Build(),
                new TimestampArray.Builder(Ts).Append(at).AppendNull().Build(),
                new Date32Array.Builder().Append(new DateOnly(2026, 9, 7)).AppendNull().Build(),
                new DoubleArray.Builder().Append(double.NaN).Append(2.5).Build(),
                tags.Build(),
            ],
            new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Build(), nullCount: 0);

        var pairType = new FixedSizeListType(new Field("item", Int32Type.Default, true), 2);
        var pair = new FixedSizeListArray(new ArrayData(pairType, 2, 0, 0,
            [ArrowBuffer.Empty], [new Int32Array.Builder().AppendRange([1, 2, 3, 4]).Build().Data]));

        var map = new MapArray.Builder(new MapType(StringType.Default, Int64Type.Default));
        var keys = (StringArray.Builder)map.KeyBuilder;
        var values = (Int64Array.Builder)map.ValueBuilder;
        map.Append();
        keys.Append("k1");
        values.Append(1);
        keys.Append("k2");
        values.AppendNull();
        map.Append();

        var xs = new ListArray.Builder(Int64Type.Default);
        var xValues = (Int64Array.Builder)xs.ValueBuilder;
        xs.Append();
        xValues.Append(5);
        xs.AppendNull();

        schema = new Schema([
            new Field("rec", recType, true), new Field("pair", pairType, true),
            new Field("m", new MapType(StringType.Default, Int64Type.Default), true),
            new Field("xs", new ListType(new Field("item", Int64Type.Default, true)), true),
        ], null);
        return new RecordBatch(schema, [rec, pair, map.Build(), xs.Build()], 2);
    }

    [Fact]
    public void Nested_cells_are_objects_and_arrays_with_leaves_spelled_as_flat_columns_are()
    {
        using var batch = Batch(out var schema);
        var writer = new RowJsonWriter(schema, [0, 1, 2, 3]);

        Assert.Equal(
            """{"rec":{"amt":"12.345","at":"2026-09-07T10:11:12.123456Z","d":"2026-09-07","f":null,"tags":["a",null]},"pair":[1,2],"m":{"k1":1,"k2":null},"xs":[5]}""",
            Encoding.UTF8.GetString(writer.Write(batch, 0)));
        Assert.Equal(
            """{"rec":{"amt":null,"at":null,"d":null,"f":2.5,"tags":[]},"pair":[3,4],"m":{},"xs":null}""",
            Encoding.UTF8.GetString(writer.Write(batch, 1)));
    }

    [Fact]
    public void Nested_cells_survive_a_sliced_batch()
    {
        using var batch = Batch(out var schema);
        using var tail = batch.Slice(1, 1);
        var writer = new RowJsonWriter(schema, [0, 1, 2, 3]);

        Assert.Equal(
            """{"rec":{"amt":null,"at":null,"d":null,"f":2.5,"tags":[]},"pair":[3,4],"m":{},"xs":null}""",
            Encoding.UTF8.GetString(writer.Write(tail, 0)));
    }

    [Fact]
    public void Nested_columns_validate_in_the_whole_row_body()
    {
        var schema = Schema(out _);
        var errors = new List<string>();
        EventHubsOutputConfig.Parse(new("eventhubs", "h", "append", "fail_on_change", new Dictionary<string, object?>()), errors)!
            .ValidateAgainst("h", schema, errors);

        Assert.Empty(errors);
    }

    [Theory]
    [InlineData("rec")]
    [InlineData("xs")]
    [InlineData("m")]
    public void A_nested_partition_key_is_refused_naming_the_column(string column)
    {
        var schema = Schema(out _);
        var errors = new List<string>();
        EventHubsOutputConfig.Parse(new("eventhubs", "h", "append", "fail_on_change",
            new Dictionary<string, object?> { ["partition_key"] = column }), errors)!
            .ValidateAgainst("h", schema, errors);

        var error = Assert.Single(errors);
        Assert.Contains($"partition_key' column '{column}'", error);
    }

    [Theory]
    [InlineData("body")]
    [InlineData("properties")]
    public void A_nested_body_or_property_column_is_refused(string option)
    {
        var schema = Schema(out _);
        var errors = new List<string>();
        var value = option == "body" ? "rec" : (object)new List<object?> { "rec" };
        EventHubsOutputConfig.Parse(new("eventhubs", "h", "append", "fail_on_change",
            new Dictionary<string, object?> { [option] = value }), errors)!
            .ValidateAgainst("h", schema, errors);

        Assert.Contains(errors, e => e.Contains("'rec'"));
    }

    [Fact]
    public async Task The_sink_refuses_a_nested_partition_key_before_any_writer_exists()
    {
        var schema = Schema(out _);
        await using var sink = await ((ISinkConnector)new EventHubsConnector()).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["auth"] = "connection_string",
                ["connection_string"] = "Endpoint=sb://x.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v",
            }), CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(new("eventhubs", "h", "append", "fail_on_change",
                new Dictionary<string, object?> { ["partition_key"] = "rec" }), schema, CancellationToken.None));
        Assert.Contains("'rec'", ex.Message);
    }

    private static Schema Schema(out RecordBatch? unused)
    {
        unused = null;
        return new Schema([
            new Field("id", Int64Type.Default, true),
            new Field("rec", new StructType([new Field("n", Int32Type.Default, true)]), true),
            new Field("xs", new ListType(new Field("item", Int64Type.Default, true)), true),
            new Field("m", new MapType(StringType.Default, Int64Type.Default), true),
        ], null);
    }
}
