using System.Net.Sockets;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

internal static class Codes
{
    public const string InvalidConnection = "PZEH0101";
    public const string InvalidDataset = "PZEH0201";
    public const string BadToken = "PZEH0202";
    public const string PositionLost = "PZEH0203";
    public const string HubNotFound = "PZEH0204";
    public const string BodyNotUtf8 = "PZEH0205";
    public const string IdleTimeout = "PZEH0206";
    public const string ReceiveFailed = "PZEH0207";
    public const string InvalidOutput = "PZEH0301";
    public const string ColumnRefused = "PZEH0302";
    public const string EventTooLarge = "PZEH0303";
    public const string SendFailed = "PZEH0304";
}

/// <summary>Turns SDK failures into the engine's exception, classified for retry. The Event Hubs
/// client already decides transience per failure reason (service busy/timeout/communication are
/// recoverable; a missing resource, an oversized event, a closed client are not) and that ruling
/// is kept. Credential failures and authorization refusals are final; raw transport failures
/// (timeouts, sockets, I/O) are transient. Anything unmapped is non-transient: an unknown failure
/// retried is a failure hidden. Every message passes the redactor.</summary>
internal static class EventHubsErrors
{
    public static bool IsTransient(Exception ex) => ex switch
    {
        EventHubsException eh => eh.IsTransient,
        AuthenticationFailedException or CredentialUnavailableException or UnauthorizedAccessException => false,
        TimeoutException or SocketException or IOException => true,
        _ => false,
    };

    public static PzConnectorException Wrap(Exception ex, EventHubsRedactor redactor, string code, string context)
    {
        if (ex is PzConnectorException already)
        {
            return already;
        }

        var detail = ex is EventHubsException eh ? $"{eh.Reason} -- {eh.Message}" : ex.Message;
        return new PzConnectorException(redactor.Redact($"{code}: eventhubs: {context}: {detail}"), IsTransient(ex), innerException: ex);
    }

    public static PzConnectorException Fatal(string code, string message, EventHubsRedactor redactor) =>
        new(redactor.Redact($"{code}: eventhubs: {message}"), isTransient: false);

    public static PzConnectorException Transient(string code, string message, EventHubsRedactor redactor) =>
        new(redactor.Redact($"{code}: eventhubs: {message}"), isTransient: true);
}
