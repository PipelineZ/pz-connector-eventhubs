using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class OutputConfigTests
{
    private static readonly Schema Schema = new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
        new Field("src", StringType.Default, true),
        new Field("amount", DoubleType.Default, true),
        new Field("payload", StringType.Default, true),
    ], null);

    private static OutputSpec Spec(Dictionary<string, object?> options, string output = "order-events") =>
        new("eventhubs", output, "append", "fail_on_change", options);

    [Fact]
    public void Defaults_to_entity_event_hub_whole_row_no_partition_key_no_properties_no_content_type()
    {
        var errors = new List<string>();
        var config = EventHubsOutputConfig.Parse(Spec([]), errors);

        Assert.Empty(errors);
        Assert.Equal(new EventHubsOutputConfig("order-events", null, null, [], null), config);
        Assert.Equal([0, 1, 2, 3, 4], config!.JsonColumnIndexes(Schema));
    }

    [Fact]
    public void Partition_key_and_properties_are_excluded_from_the_json_body()
    {
        var errors = new List<string>();
        var config = EventHubsOutputConfig.Parse(Spec(new()
        {
            ["event_hub"] = "h", ["partition_key"] = "id", ["properties"] = new List<object?> { "src" }, ["content_type"] = "application/octet-stream",
        }), errors);

        Assert.Empty(errors);
        config!.ValidateAgainst("order-events", Schema, errors);
        Assert.Empty(errors);
        Assert.Equal([1, 3, 4], config.JsonColumnIndexes(Schema));
        Assert.Equal("application/octet-stream", config.ContentType);
    }

    [Fact]
    public void Configs_parsed_from_separately_built_property_lists_compare_equal()
    {
        var errors = new List<string>();
        var a = EventHubsOutputConfig.Parse(Spec(new() { ["properties"] = new List<object?> { "src", "id" } }), errors);
        var b = EventHubsOutputConfig.Parse(Spec(new() { ["properties"] = new List<object?> { "src", "id" } }), errors);

        Assert.Equal(a, b);
    }

    [Fact]
    public void Explicit_body_column_excludes_nothing_else()
    {
        var errors = new List<string>();
        var config = EventHubsOutputConfig.Parse(Spec(new() { ["body"] = "payload", ["partition_key"] = "id" }), errors);

        config!.ValidateAgainst("order-events", Schema, errors);
        Assert.Empty(errors);
        Assert.Empty(config.JsonColumnIndexes(Schema));
    }

    [Fact]
    public void Parse_reports_bad_options()
    {
        var errors = new List<string>();
        var config = EventHubsOutputConfig.Parse(Spec(new()
        {
            ["content_type"] = "", ["properties"] = "src", ["nope"] = 1L, ["event_hub"] = "",
        }), errors);

        Assert.Null(config);
        Assert.Equal(4, errors.Count);
        Assert.Contains(errors, e => e.Contains("'content_type'"));
        Assert.Contains(errors, e => e.Contains("'properties'") && e.Contains("list"));
        Assert.Contains(errors, e => e.Contains("'nope'"));
        Assert.Contains(errors, e => e.Contains("'event_hub'"));
    }

    [Fact]
    public void Unknown_option_lists_the_known_ones()
    {
        var errors = new List<string>();
        EventHubsOutputConfig.Parse(Spec(new() { ["nope"] = 1L }), errors);

        var error = Assert.Single(errors);
        Assert.Contains("event_hub, body, partition_key, properties, content_type", error);
    }

    [Fact]
    public void ValidateAgainst_refuses_a_column_the_event_body_json_cannot_carry()
    {
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("ratio", FloatType.Default, true)], null);
        var errors = new List<string>();
        var config = EventHubsOutputConfig.Parse(Spec([]), errors)!;

        config.ValidateAgainst("order-events", schema, errors);

        // Named by column and type, PZEH0302-prefixed, and aggregated like every other schema error
        // -- not thrown out of the row writer once the writer is already running.
        var error = Assert.Single(errors);
        Assert.Contains("PZEH0302:", error);
        Assert.Contains("output 'order-events':", error);
        Assert.Contains("'ratio'", error);
        Assert.Contains("Float", error);
        Assert.Contains("the event body's JSON cannot carry", error);
    }

    [Fact]
    public void ValidateAgainst_reports_missing_and_mistyped_columns()
    {
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, true),
            new Field("amount", DoubleType.Default, true),
            new Field("ratio", FloatType.Default, true),
        ], null);
        var errors = new List<string>();
        var config = EventHubsOutputConfig.Parse(Spec(new()
        {
            ["partition_key"] = "amount", ["body"] = "id", ["properties"] = new List<object?> { "missing", "ratio" },
        }), errors)!;

        config.ValidateAgainst("order-events", schema, errors);

        Assert.Equal(4, errors.Count);
        Assert.All(errors, e => Assert.Contains("output 'order-events':", e));
        Assert.Contains(errors, e => e.Contains("'partition_key'") && e.Contains("amount") && e.Contains("Double"));
        Assert.Contains(errors, e => e.Contains("'body'") && e.Contains("id") && e.Contains("String"));
        Assert.Contains(errors, e => e.Contains("'properties'") && e.Contains("missing"));
        Assert.Contains(errors, e => e.Contains("'properties'") && e.Contains("ratio"));
    }

    [Fact]
    public void ValidateAgainst_allows_every_property_type_in_the_matrix()
    {
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, true),
            new Field("n", Int32Type.Default, true),
            new Field("d", DoubleType.Default, true),
            new Field("flag", BooleanType.Default, true),
            new Field("day", Date32Type.Default, true),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
        ], null);
        var errors = new List<string>();
        var config = EventHubsOutputConfig.Parse(Spec(new()
        {
            ["properties"] = new List<object?> { "id", "n", "d", "flag", "day", "ts" },
        }), errors)!;

        config.ValidateAgainst("order-events", schema, errors);

        Assert.Empty(errors);
    }
}
