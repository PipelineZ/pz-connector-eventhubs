using System.Globalization;

namespace Pz.Connector.EventHubs;

/// <summary>Partition ids order numerically ("0", "2", "10", ...) when every one parses as an
/// <see cref="int"/> -- the case for every Event Hubs partition id in practice -- and fall back to
/// ordinal string order otherwise, so a plan or a token is still deterministically ordered even
/// over ids that don't parse.</summary>
internal static class PartitionIds
{
    public static IEnumerable<T> Order<T>(IEnumerable<T> items, Func<T, string> id)
    {
        var all = items.ToArray();
        return all.All(i => int.TryParse(id(i), out _))
            ? all.OrderBy(i => int.Parse(id(i), CultureInfo.InvariantCulture))
            : all.OrderBy(id, StringComparer.Ordinal);
    }
}
