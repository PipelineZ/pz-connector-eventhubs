using Azure.Identity;
using Azure.Messaging.EventHubs;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class ErrorsTests
{
    [Theory]
    [InlineData(EventHubsException.FailureReason.ServiceBusy, true)]
    [InlineData(EventHubsException.FailureReason.ServiceTimeout, true)]
    [InlineData(EventHubsException.FailureReason.ServiceCommunicationProblem, true)]
    [InlineData(EventHubsException.FailureReason.ResourceNotFound, false)]
    [InlineData(EventHubsException.FailureReason.MessageSizeExceeded, false)]
    [InlineData(EventHubsException.FailureReason.ClientClosed, false)]
    public void Event_hubs_exception_keeps_the_sdk_classification(EventHubsException.FailureReason reason, bool transient)
    {
        var ex = new EventHubsException("hub", "boom", reason);
        var wrapped = EventHubsErrors.Wrap(ex, EventHubsRedactor.None, Codes.SendFailed, "event hub 'hub': sending");
        Assert.Equal(transient, wrapped.IsTransient);
        Assert.StartsWith("PZEH0304: eventhubs: event hub 'hub': sending: ", wrapped.Message);
        Assert.Contains(reason.ToString(), wrapped.Message);
    }

    [Fact]
    public void Auth_failures_are_not_transient_and_are_redacted()
    {
        var ex = new UnauthorizedAccessException("key s3cr3tvalue rejected");
        var wrapped = EventHubsErrors.Wrap(ex, new EventHubsRedactor(["s3cr3tvalue"]), Codes.InvalidConnection, "namespace 'x'");
        Assert.False(wrapped.IsTransient);
        Assert.DoesNotContain("s3cr3tvalue", wrapped.Message);
    }

    [Fact]
    public void Timeouts_and_sockets_are_transient()
    {
        Assert.True(EventHubsErrors.Wrap(new TimeoutException(), EventHubsRedactor.None, Codes.SendFailed, "x").IsTransient);
        Assert.True(EventHubsErrors.Wrap(new System.Net.Sockets.SocketException(10061), EventHubsRedactor.None, Codes.SendFailed, "x").IsTransient);
        Assert.True(EventHubsErrors.Wrap(new IOException(), EventHubsRedactor.None, Codes.SendFailed, "x").IsTransient);
    }

    [Fact]
    public void Already_wrapped_passes_through()
    {
        var original = EventHubsErrors.Fatal(Codes.HubNotFound, "gone", EventHubsRedactor.None);
        Assert.Same(original, EventHubsErrors.Wrap(original, EventHubsRedactor.None, Codes.SendFailed, "x"));
        Assert.Equal("PZEH0204: eventhubs: gone", original.Message);
    }

    [Fact]
    public void Cancellation_is_not_transient()
    {
        Assert.False(EventHubsErrors.IsTransient(new OperationCanceledException()));
    }

    [Fact]
    public void Credential_failures_are_not_transient()
    {
        Assert.False(EventHubsErrors.IsTransient(new AuthenticationFailedException("bad credential")));
        Assert.False(EventHubsErrors.IsTransient(new CredentialUnavailableException("no credential available")));
    }
}
