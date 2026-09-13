using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class ConnectorInfoTests
{
    [Fact]
    public void Name_and_capabilities()
    {
        var connector = new EventHubsConnector();
        Assert.Equal("eventhubs", connector.Info.Name);
        Assert.Equal(ConnectorCapabilities.SyncState, connector.Capabilities);
        Assert.Equal(ProtocolVersion.Major, connector.Info.ProtocolMajor);
    }
}
