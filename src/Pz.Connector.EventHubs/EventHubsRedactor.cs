using System.Text.RegularExpressions;

namespace Pz.Connector.EventHubs;

/// <summary>Strips credentials from any text that may reach an exception message, a log line, or
/// a ConnectionCheck: every configured secret is replaced wherever it occurs (a service error can
/// echo it in any position), longest first so a whole connection string goes before its key
/// component, and the key-bearing connection-string components are rewritten even when the value
/// is not one of ours. Secrets shorter than 3 characters are not matched -- replacing them would
/// shred unrelated text.</summary>
internal sealed partial class EventHubsRedactor
{
    public const string Mask = "***";

    public static readonly EventHubsRedactor None = new([]);

    private readonly string[] _secrets;

    public EventHubsRedactor(IReadOnlyList<string> secrets)
    {
        _secrets = secrets.Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal).OrderByDescending(s => s.Length).ToArray();
    }

    public string Redact(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        foreach (var secret in _secrets)
        {
            text = text.Replace(secret, Mask, StringComparison.Ordinal);
        }

        return KeyComponent().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
    }

    // SharedAccessKey=... / SharedAccessSignature=... as they appear in a connection string dump;
    // the value stops at ';' or whitespace so the next component survives.
    [GeneratedRegex("""(?<key>\b(?:SharedAccessKey|SharedAccessSignature))=[^\s;]+""")]
    private static partial Regex KeyComponent();
}
