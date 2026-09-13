namespace Pz.Connector.EventHubs.Tests;

public sealed class RedactorTests
{
    private const string Cs = "Endpoint=sb://x.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abcDEF123ghi=";

    [Fact]
    public void Connection_string_and_its_key_component_are_masked()
    {
        var r = new EventHubsRedactor([Cs, "abcDEF123ghi="]);
        Assert.Equal("failed: *** / ***", r.Redact($"failed: {Cs} / abcDEF123ghi="));
    }

    [Fact]
    public void Shared_access_key_component_is_masked_even_when_not_seeded()
    {
        var r = EventHubsRedactor.None;
        Assert.Equal("SharedAccessKey=***;x", r.Redact("SharedAccessKey=zzz;x"));
        Assert.Equal("SharedAccessSignature=***", r.Redact("SharedAccessSignature=sr=foo&sig=bar"));
    }

    [Fact]
    public void Short_secrets_are_not_matched()
    {
        Assert.Equal("ab", new EventHubsRedactor(["ab"]).Redact("ab"));
    }
}
