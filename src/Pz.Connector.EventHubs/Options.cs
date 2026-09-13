using System.Globalization;

namespace Pz.Connector.EventHubs;

/// <summary>Integer options as they arrive: YAML hands over ints and longs; the out-of-process
/// host's protobuf Struct has only doubles, so an integral double is an integer too.</summary>
internal static class Options
{
    public static int Int(IReadOnlyDictionary<string, object?> options, string option, int fallback, int min, int max, string prefix, List<string> errors)
    {
        if (!options.TryGetValue(option, out var raw) || raw is null)
        {
            return fallback;
        }

        if (raw is not bool && raw is IFormattable f && long.TryParse(f.ToString(null, CultureInfo.InvariantCulture), out var n)
            && n >= min && n <= max)
        {
            return (int)n;
        }

        var prefixed = prefix.Length == 0 ? "" : prefix + ": ";
        errors.Add(max == int.MaxValue
            ? $"{prefixed}'{option}' must be an integer of at least {min}"
            : $"{prefixed}'{option}' must be an integer between {min} and {max}");
        return fallback;
    }

    public static List<string>? Strings(IReadOnlyDictionary<string, object?> options, string option, string prefix, List<string> errors)
    {
        if (!options.TryGetValue(option, out var raw) || raw is null)
        {
            return null;
        }

        if (raw is string || raw is not IEnumerable<object?> list)
        {
            errors.Add($"{prefix}: '{option}' must be a list of column names");
            return null;
        }

        var result = new List<string>();
        foreach (var item in list)
        {
            if (item?.ToString() is { Length: > 0 } name)
            {
                result.Add(name);
            }
            else
            {
                errors.Add($"{prefix}: '{option}' entries must be non-empty column names");
            }
        }

        return result;
    }
}
