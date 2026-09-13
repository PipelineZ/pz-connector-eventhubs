namespace Pz.Connector.EventHubs;

/// <summary>What one run reads from one partition: [Start, Bound] inclusive, Bound being the last
/// enqueued sequence number at plan time, which is what makes a run finite and a retry re-read the
/// same slice. Next is the token entry the run will hand back -- always Bound + 1, because sequence
/// numbers are contiguous per partition and nothing above the bound is landed.</summary>
internal sealed record PartitionPlan(string PartitionId, StartAt? Start, long Bound, long Next)
{
    public bool Done => Start is null;
}

/// <summary>Pure resume/bound arithmetic. A stored position outside what the partition still holds
/// is an error, never a silent skip: below the earliest available means retention already dropped
/// events the dataset never landed; beyond last + 1 means the hub was recreated or its sequence
/// reset under the stored state. Every partition's problem is reported in one message.</summary>
internal static class ReadPlan
{
    public static IReadOnlyList<PartitionPlan> Compute(string eventHub, SequenceToken? token, StartPosition start,
        IReadOnlyList<PartitionInfo> partitions, EventHubsRedactor redactor)
    {
        var plans = new List<PartitionPlan>(partitions.Count);
        var problems = new List<string>();
        foreach (var p in Ordered(partitions))
        {
            // An empty partition that once held events keeps its last sequence number; one that
            // never did reports begin = last = -1. Either way the first readable position is last + 1.
            var begin = p.IsEmpty ? p.LastEnqueuedSequenceNumber + 1 : p.BeginningSequenceNumber;
            var last = p.LastEnqueuedSequenceNumber;
            if (token is not null && token.Next.TryGetValue(p.PartitionId, out var stored))
            {
                if (stored < begin)
                {
                    problems.Add($"partition {p.PartitionId}: stored sequence number {stored} is below the earliest available {begin}, " +
                                 "so events were dropped by retention before this run landed them");
                    continue;
                }

                if (stored > last + 1)
                {
                    problems.Add($"partition {p.PartitionId}: stored sequence number {stored} is beyond the last enqueued {last}, " +
                                 "so the event hub was recreated or its sequence reset under the stored state");
                    continue;
                }

                plans.Add(new PartitionPlan(p.PartitionId, stored > last ? null : StartAt.Sequence(stored), last, last + 1));
                continue;
            }

            StartAt? at = start.Kind switch
            {
                StartKind.Earliest => begin > last ? null : StartAt.Sequence(begin),
                StartKind.Latest => null,
                _ => p.IsEmpty ? null : StartAt.Enqueued(start.Timestamp!.Value),
            };
            plans.Add(new PartitionPlan(p.PartitionId, at, last, last + 1));
        }

        if (problems.Count > 0)
        {
            throw EventHubsErrors.Fatal(Codes.PositionLost,
                $"event hub '{eventHub}' cannot resume from the stored sync state: {string.Join("; ", problems)}. " +
                "Run with --full-refresh to start from `start:` again, or edit the dataset's state with `pz state`", redactor);
        }

        return plans;
    }

    public static IEnumerable<PartitionInfo> Ordered(IReadOnlyList<PartitionInfo> partitions) =>
        partitions.All(p => int.TryParse(p.PartitionId, out _))
            ? partitions.OrderBy(p => int.Parse(p.PartitionId, System.Globalization.CultureInfo.InvariantCulture))
            : partitions.OrderBy(p => p.PartitionId, StringComparer.Ordinal);
}
