using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class SequenceTokenTests
{
    private const string Ns = "ns1.servicebus.windows.net";

    [Fact]
    public void Round_trips_with_the_namespace_and_stable_key_order()
    {
        var token = new SequenceToken(Ns, "orders", new Dictionary<string, long> { ["10"] = 5, ["2"] = 7, ["0"] = 1235 });
        var json = token.Serialize();
        Assert.Equal("""{"v":1,"namespace":"ns1.servicebus.windows.net","event_hub":"orders","partitions":{"0":1235,"2":7,"10":5}}""", json);
        var back = SequenceToken.Parse(json, Ns, "orders", EventHubsRedactor.None);
        Assert.Equal(Ns, back.Namespace);
        Assert.Equal("orders", back.EventHub);
        Assert.Equal(token.Next.OrderBy(k => k.Key), back.Next.OrderBy(k => k.Key));
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("""{"v":2,"namespace":"ns1.servicebus.windows.net","event_hub":"orders","partitions":{}}""")]
    [InlineData("""{"v":1,"namespace":"ns1.servicebus.windows.net","event_hub":"orders","partitions":{"0":-1}}""")]
    [InlineData("""{"v":1,"namespace":"ns1.servicebus.windows.net","partitions":{}}""")]
    [InlineData("""{"v":1,"event_hub":"orders","partitions":{"0":1}}""")]
    [InlineData("""{"v":1,"namespace":7,"event_hub":"orders","partitions":{"0":1}}""")]
    public void Malformed_is_refused_without_echoing_the_text(string json)
    {
        var ex = Assert.Throws<PzConnectorException>(() => SequenceToken.Parse(json, Ns, "orders", EventHubsRedactor.None));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZEH0202:", ex.Message);
        Assert.DoesNotContain(json, ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
    }

    [Fact]
    public void Different_event_hub_is_refused()
    {
        var ex = Assert.Throws<PzConnectorException>(() => SequenceToken.Parse(
            """{"v":1,"namespace":"ns1.servicebus.windows.net","event_hub":"other","partitions":{"0":1}}""",
            Ns, "orders", EventHubsRedactor.None));
        Assert.Contains("belongs to event hub 'other'", ex.Message);
        Assert.Contains("'orders'", ex.Message);
    }

    [Fact]
    public void Different_namespace_is_refused()
    {
        // Sequence numbers are per namespace: the same hub name elsewhere numbers its partitions on
        // its own, so a token from another namespace must stop the run rather than resume against it.
        var ex = Assert.Throws<PzConnectorException>(() => SequenceToken.Parse(
            """{"v":1,"namespace":"other.servicebus.windows.net","event_hub":"orders","partitions":{"0":1}}""",
            Ns, "orders", EventHubsRedactor.None));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZEH0202:", ex.Message);
        Assert.Contains("belongs to namespace 'other.servicebus.windows.net'", ex.Message);
        Assert.Contains($"now points at '{Ns}'", ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
    }
}
