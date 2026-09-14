using Pz.Connectors.TestKit;

namespace Pz.Connector.EventHubs.Tests;

/// <summary>The TestKit's credential shapes through this connector's redactor, seeded with the same
/// synthetic secret the suite embeds, exactly as a real config would seed it with the connection
/// string and its shared access key.</summary>
public sealed class RedactionContract : ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) =>
        new EventHubsRedactor(["pz-testkit-secret-value"]).Redact(thirdPartyMessage);
}
