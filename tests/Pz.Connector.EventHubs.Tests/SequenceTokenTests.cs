using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class SequenceTokenTests
{
    [Fact]
    public void Round_trips_with_stable_key_order()
    {
        var token = new SequenceToken("orders", new Dictionary<string, long> { ["10"] = 5, ["2"] = 7, ["0"] = 1235 });
        var json = token.Serialize();
        Assert.Equal("""{"v":1,"event_hub":"orders","partitions":{"0":1235,"2":7,"10":5}}""", json);
        var back = SequenceToken.Parse(json, "orders", EventHubsRedactor.None);
        Assert.Equal(token.Next.OrderBy(k => k.Key), back.Next.OrderBy(k => k.Key));
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("""{"v":2,"event_hub":"orders","partitions":{}}""")]
    [InlineData("""{"v":1,"event_hub":"orders","partitions":{"0":-1}}""")]
    [InlineData("""{"v":1,"partitions":{}}""")]
    public void Malformed_is_refused_without_echoing_the_text(string json)
    {
        var ex = Assert.Throws<PzConnectorException>(() => SequenceToken.Parse(json, "orders", EventHubsRedactor.None));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZEH0202:", ex.Message);
        Assert.DoesNotContain(json, ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
    }

    [Fact]
    public void Different_event_hub_is_refused()
    {
        var ex = Assert.Throws<PzConnectorException>(() =>
            SequenceToken.Parse("""{"v":1,"event_hub":"other","partitions":{"0":1}}""", "orders", EventHubsRedactor.None));
        Assert.Contains("belongs to event hub 'other'", ex.Message);
        Assert.Contains("'orders'", ex.Message);
    }
}
