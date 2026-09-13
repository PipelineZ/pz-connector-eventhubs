using Azure.Messaging.EventHubs;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class ConnectionConfigTests
{
    private const string Cs = "Endpoint=sb://ns1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abcDEF123ghi=";

    private static EventHubsConnectionConfig? Parse(List<string> errors, params (string, object?)[] kv) =>
        EventHubsConnectionConfig.Parse(new ConnectorConfig(kv.ToDictionary(p => p.Item1, p => p.Item2)), errors);

    [Fact]
    public void Connection_string_auth_parses_and_seeds_the_redactor()
    {
        var errors = new List<string>();
        var c = Parse(errors, ("auth", "connection_string"), ("connection_string", Cs));
        Assert.Empty(errors);
        Assert.NotNull(c);
        Assert.Equal("ns1.servicebus.windows.net", c!.NamespaceHost);
        Assert.Equal("$Default", c.ConsumerGroup);
        Assert.Equal(EventHubsTransportType.AmqpTcp, c.Transport);
        Assert.Equal(60, c.IdleTimeoutSeconds);
        Assert.DoesNotContain("abcDEF123ghi=", c.Redactor.Redact($"x {Cs} y abcDEF123ghi="));
    }

    [Fact]
    public void Emulator_connection_string_is_accepted()
    {
        var errors = new List<string>();
        var c = Parse(errors, ("auth", "connection_string"),
            ("connection_string", "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"));
        Assert.Empty(errors);
        Assert.Equal("localhost", c!.NamespaceHost);
    }

    [Fact]
    public void Entity_path_in_the_connection_string_is_refused()
    {
        var errors = new List<string>();
        Assert.Null(Parse(errors, ("auth", "connection_string"), ("connection_string", Cs + ";EntityPath=orders")));
        var e = Assert.Single(errors);
        Assert.Contains("namespace-level connection string", e);
        Assert.DoesNotContain("abcDEF123ghi=", e);
    }

    [Fact]
    public void Unparseable_connection_string_never_echoes_it()
    {
        var errors = new List<string>();
        Assert.Null(Parse(errors, ("auth", "connection_string"), ("connection_string", "garbage-with-secret-token")));
        Assert.DoesNotContain("garbage-with-secret-token", Assert.Single(errors));
    }

    [Fact]
    public void Every_auth_lists_its_required_fields_in_one_pass()
    {
        var errors = new List<string>();
        Assert.Null(Parse(errors, ("auth", "service_principal")));
        Assert.Equal(4, errors.Count); // namespace, tenant_id, client_id, client_secret
        errors.Clear();
        Assert.Null(Parse(errors, ("auth", "credential_chain")));
        Assert.Contains("'namespace' is required for auth 'credential_chain'", Assert.Single(errors));
    }

    [Fact]
    public void Fields_of_another_auth_are_refused()
    {
        var errors = new List<string>();
        Assert.Null(Parse(errors, ("auth", "connection_string"), ("connection_string", Cs), ("client_secret", "zzz"), ("namespace", "n.servicebus.windows.net")));
        Assert.Equal(2, errors.Count);
        Assert.Contains(errors, e => e.Contains("'client_secret' is not used by auth 'connection_string'"));
        Assert.Contains(errors, e => e.Contains("'namespace' is not used by auth 'connection_string'"));
    }

    [Fact]
    public void Unknown_keys_bad_auth_bad_enums_and_namespace_shape_are_all_reported()
    {
        var errors = new List<string>();
        Assert.Null(Parse(errors, ("auth", "magic"), ("bogus", 1), ("transport", "http"), ("namespace", "sb://x/")));
        Assert.Contains(errors, e => e.Contains("'auth' must be one of connection_string, credential_chain, service_principal, managed_identity"));
        Assert.Contains(errors, e => e.Contains("unknown connection key 'bogus'"));
        Assert.Contains(errors, e => e.Contains("'transport' must be one of amqp_tcp, amqp_websockets"));
        Assert.Contains(errors, e => e.Contains("'namespace' must be a bare host name"));
    }

    [Fact]
    public void Managed_identity_and_service_principal_parse()
    {
        var errors = new List<string>();
        var mi = Parse(errors, ("auth", "managed_identity"), ("namespace", "n.servicebus.windows.net"), ("client_id", "cid"), ("transport", "amqp_websockets"), ("idle_timeout", 5.0), ("consumer_group", "cg"));
        Assert.Empty(errors);
        Assert.Equal(EventHubsTransportType.AmqpWebSockets, mi!.Transport);
        Assert.Equal(5, mi.IdleTimeoutSeconds);
        Assert.Equal("cg", mi.ConsumerGroup);
        var sp = Parse(errors, ("auth", "service_principal"), ("namespace", "n.servicebus.windows.net"), ("tenant_id", "t"), ("client_id", "c"), ("client_secret", "verysecret1"));
        Assert.Empty(errors);
        Assert.Equal("***", sp!.Redactor.Redact("verysecret1"));
    }
}
