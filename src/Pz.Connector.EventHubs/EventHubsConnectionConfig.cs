using Azure.Messaging.EventHubs;

namespace Pz.Connector.EventHubs;

/// <summary>The typed connection surface. <c>auth</c> selects one of four credential shapes, each
/// with its own required fields (checked offline and all at once); a field that belongs to a
/// different shape is refused so one config never says two things. The connection string is
/// namespace-level: the event hub is always the dataset/output entity, never an EntityPath.</summary>
internal sealed record EventHubsConnectionConfig(
    string Auth,
    string? ConnectionString,
    string? Namespace,
    string? TenantId,
    string? ClientId,
    string? ClientSecret,
    string ConsumerGroup,
    EventHubsTransportType Transport,
    int IdleTimeoutSeconds,
    EventHubsRedactor Redactor)
{
    public const int DefaultIdleTimeoutSeconds = 60;
    public const int MaxIdleTimeoutSeconds = 3600;
    public const string DefaultConsumerGroup = "$Default";

    public static readonly string[] AuthMethods = ["connection_string", "credential_chain", "service_principal", "managed_identity"];

    private static readonly string[] KnownKeys =
        ["auth", "connection_string", "namespace", "tenant_id", "client_id", "client_secret", "consumer_group", "transport", "idle_timeout"];

    private static readonly string[] Transports = ["amqp_tcp", "amqp_websockets"];

    /// <summary>The namespace host: <c>namespace</c> for the Entra auths, the connection string's
    /// endpoint host otherwise. Safe to print -- it is never a secret.</summary>
    public string NamespaceHost { get; init; } = "";

    public static EventHubsConnectionConfig? Parse(Pz.Connectors.Abstractions.ConnectorConfig config, List<string> errors)
    {
        var start = errors.Count;
        var secrets = new List<string>();

        foreach (var key in config.Values.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown connection key '{key}'; known keys: {string.Join(", ", KnownKeys)}");
        }

        var auth = config.GetString("auth") ?? "";
        var authKnown = AuthMethods.Contains(auth, StringComparer.Ordinal);
        if (!authKnown)
        {
            errors.Add(auth.Length == 0
                ? $"'auth' is required (one of: {string.Join(", ", AuthMethods)})"
                : $"'auth' must be one of {string.Join(", ", AuthMethods)} (got '{auth}')");
        }
        else
        {
            foreach (var field in RequiredFields(auth).Where(f => string.IsNullOrEmpty(config.GetString(f))))
            {
                errors.Add($"'{field}' is required for auth '{auth}'");
            }

            foreach (var field in ForbiddenFields(auth).Where(f => config.Values.ContainsKey(f)))
            {
                errors.Add($"'{field}' is not used by auth '{auth}'");
            }
        }

        var host = "";
        var connectionString = config.GetString("connection_string");
        if (!string.IsNullOrEmpty(connectionString))
        {
            secrets.Add(connectionString);
            try
            {
                var parsed = EventHubsConnectionStringProperties.Parse(connectionString);
                if (!string.IsNullOrEmpty(parsed.SharedAccessKey))
                {
                    secrets.Add(parsed.SharedAccessKey);
                }

                if (!string.IsNullOrEmpty(parsed.SharedAccessSignature))
                {
                    secrets.Add(parsed.SharedAccessSignature);
                }

                if (!string.IsNullOrEmpty(parsed.EventHubName))
                {
                    errors.Add($"{Codes.ConnectionStringRefused}: 'connection_string' carries an EntityPath; use a namespace-level " +
                               "connection string -- the event hub is the entity name in connections.yml");
                }

                host = parsed.Endpoint?.Host ?? "";
                if (host.Length == 0)
                {
                    errors.Add($"{Codes.ConnectionStringRefused}: 'connection_string' has no Endpoint");
                }
            }
            catch (Exception ex) when (ex is FormatException or ArgumentException)
            {
                errors.Add($"{Codes.ConnectionStringRefused}: 'connection_string' is not an Event Hubs connection string " +
                           "(expected Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...)");
            }
        }

        var ns = config.GetString("namespace");
        if (!string.IsNullOrEmpty(ns))
        {
            if (ns.Contains("://", StringComparison.Ordinal) || ns.Contains('/') || ns.Contains(' '))
            {
                errors.Add("'namespace' must be a bare host name such as myns.servicebus.windows.net");
            }
            else if (auth != "connection_string")
            {
                host = ns;
            }
        }

        if (config.GetString("client_secret") is { Length: > 0 } secret)
        {
            secrets.Add(secret);
        }

        var consumerGroup = config.GetString("consumer_group");
        if (config.Values.ContainsKey("consumer_group") && string.IsNullOrEmpty(consumerGroup))
        {
            errors.Add("'consumer_group' must be a non-empty string");
        }

        var transport = EventHubsTransportType.AmqpTcp;
        var transportText = config.GetString("transport");
        if (!string.IsNullOrEmpty(transportText))
        {
            switch (transportText)
            {
                case "amqp_tcp": transport = EventHubsTransportType.AmqpTcp; break;
                case "amqp_websockets": transport = EventHubsTransportType.AmqpWebSockets; break;
                default: errors.Add($"'transport' must be one of {string.Join(", ", Transports)} (got '{transportText}')"); break;
            }
        }

        var idle = Options.Int(config.Values, "idle_timeout", DefaultIdleTimeoutSeconds, 1, MaxIdleTimeoutSeconds, "", errors);

        var redactor = new EventHubsRedactor(secrets);
        for (var i = start; i < errors.Count; i++)
        {
            errors[i] = redactor.Redact(errors[i]);
        }

        return errors.Count == start
            ? new EventHubsConnectionConfig(auth, connectionString, ns, config.GetString("tenant_id"), config.GetString("client_id"),
                config.GetString("client_secret"), string.IsNullOrEmpty(consumerGroup) ? DefaultConsumerGroup : consumerGroup,
                transport, idle, redactor) { NamespaceHost = host }
            : null;
    }

    private static string[] RequiredFields(string auth) => auth switch
    {
        "connection_string" => ["connection_string"],
        "service_principal" => ["namespace", "tenant_id", "client_id", "client_secret"],
        "credential_chain" or "managed_identity" => ["namespace"],
        _ => [],
    };

    private static string[] ForbiddenFields(string auth) => auth switch
    {
        "connection_string" => ["namespace", "tenant_id", "client_id", "client_secret"],
        "service_principal" => ["connection_string"],
        "credential_chain" => ["connection_string", "tenant_id", "client_id", "client_secret"],
        "managed_identity" => ["connection_string", "tenant_id", "client_secret"],
        _ => [],
    };
}
