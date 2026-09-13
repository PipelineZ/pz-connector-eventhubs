using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class DatasetConfigTests
{
    private static DatasetSpec Spec(Dictionary<string, object?> options, string dataset = "orders") => new("events", dataset, options);

    [Fact]
    public void Defaults_event_hub_to_the_entity_name_earliest_utf8()
    {
        var errors = new List<string>();
        var config = EventHubsDatasetConfig.Parse(Spec([]), errors);

        Assert.Empty(errors);
        Assert.Equal(new EventHubsDatasetConfig("orders", new StartPosition(StartKind.Earliest, null), PayloadEncoding.Utf8), config);
    }

    [Fact]
    public void Parses_every_option()
    {
        var errors = new List<string>();
        var config = EventHubsDatasetConfig.Parse(Spec(new()
        {
            ["event_hub"] = "orders-v2", ["start"] = "2026-01-02T03:04:05Z", ["encoding"] = "base64",
        }), errors);

        Assert.Empty(errors);
        Assert.Equal("orders-v2", config!.EventHub);
        Assert.Equal(StartKind.Timestamp, config.Start.Kind);
        Assert.Equal(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), config.Start.Timestamp);
        Assert.Equal(PayloadEncoding.Base64, config.Encoding);
    }

    [Fact]
    public void Latest_is_recognized()
    {
        var errors = new List<string>();
        var config = EventHubsDatasetConfig.Parse(Spec(new() { ["start"] = "latest" }), errors);

        Assert.Equal(StartKind.Latest, config!.Start.Kind);
    }

    [Fact]
    public void Reports_every_bad_option()
    {
        var errors = new List<string>();
        var config = EventHubsDatasetConfig.Parse(Spec(new()
        {
            ["start"] = "yesterday", ["encoding"] = "hex", ["nope"] = 1L, ["event_hub"] = "",
        }), errors);

        Assert.Null(config);
        Assert.Equal(4, errors.Count);
        Assert.Contains(errors, e => e.Contains("'start'") && e.Contains("ISO-8601"));
        Assert.Contains(errors, e => e.Contains("'encoding'"));
        Assert.Contains(errors, e => e.Contains("'nope'"));
        Assert.Contains(errors, e => e.Contains("'event_hub'"));
        Assert.All(errors, e => Assert.StartsWith("dataset 'orders': ", e));
    }

    [Fact]
    public void Unknown_option_lists_known_keys()
    {
        var errors = new List<string>();
        EventHubsDatasetConfig.Parse(Spec(new() { ["nope"] = 1L }), errors);

        Assert.Contains(errors, e => e.Contains("known: event_hub, start, encoding"));
    }
}
