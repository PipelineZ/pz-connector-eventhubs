using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs.Tests;

public sealed class ReadPlanTests
{
    private static PartitionInfo P(string id, bool empty, long begin, long last) => new(id, empty, begin, last);
    private static readonly StartPosition Earliest = new(StartKind.Earliest, null);
    private static readonly StartPosition Latest = new(StartKind.Latest, null);
    private static readonly StartPosition At = new(StartKind.Timestamp, new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
    private static SequenceToken T(params (string, long)[] e) => new("ns", "h", e.ToDictionary(x => x.Item1, x => x.Item2));

    [Fact]
    public void First_run_earliest_reads_begin_to_last()
    {
        var plan = ReadPlan.Compute("h", null, Earliest, [P("0", false, 100, 199)], EventHubsRedactor.None);
        var p = Assert.Single(plan);
        Assert.Equal(StartAt.Sequence(100), p.Start);
        Assert.Equal(199, p.Bound);
        Assert.Equal(200, p.Next);
        Assert.False(p.Done);
    }

    [Fact]
    public void Never_written_partition_is_done_with_next_zero()
    {
        var p = Assert.Single(ReadPlan.Compute("h", null, Earliest, [P("0", true, -1, -1)], EventHubsRedactor.None));
        Assert.True(p.Done);
        Assert.Equal(0, p.Next);
    }

    [Fact]
    public void Emptied_partition_keeps_last_and_is_done()
    {
        var p = Assert.Single(ReadPlan.Compute("h", null, Earliest, [P("0", true, 50, 49)], EventHubsRedactor.None));
        Assert.True(p.Done);
        Assert.Equal(50, p.Next);
    }

    [Fact]
    public void Token_resumes_inclusive_and_is_done_when_caught_up()
    {
        var plan = ReadPlan.Compute("h", T(("0", 150), ("1", 200)), Earliest, [P("0", false, 100, 199), P("1", false, 100, 199)], EventHubsRedactor.None);
        Assert.Equal(StartAt.Sequence(150), plan[0].Start);
        Assert.Equal(200, plan[0].Next);
        Assert.True(plan[1].Done);
        Assert.Equal(200, plan[1].Next);
    }

    [Fact]
    public void Token_below_begin_is_retention_loss_and_all_problems_are_reported_together()
    {
        var ex = Assert.Throws<PzConnectorException>(() => ReadPlan.Compute("h", T(("0", 10), ("1", 500)), Earliest,
            [P("0", false, 100, 199), P("1", false, 100, 199)], EventHubsRedactor.None));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZEH0203:", ex.Message);
        Assert.Contains("partition 0: stored sequence number 10 is below the earliest available 100", ex.Message);
        Assert.Contains("partition 1: stored sequence number 500 is beyond the last enqueued 199", ex.Message);
        Assert.Contains("--full-refresh", ex.Message);
        Assert.Contains("pz state", ex.Message);
    }

    [Fact]
    public void Latest_and_timestamp_starts()
    {
        var latest = Assert.Single(ReadPlan.Compute("h", null, Latest, [P("0", false, 100, 199)], EventHubsRedactor.None));
        Assert.True(latest.Done);
        Assert.Equal(200, latest.Next);
        var at = Assert.Single(ReadPlan.Compute("h", null, At, [P("0", false, 100, 199)], EventHubsRedactor.None));
        Assert.Equal(StartAt.Enqueued(At.Timestamp!.Value), at.Start);
        Assert.Equal(199, at.Bound);
    }

    [Fact]
    public void New_partition_uses_start_while_known_ones_resume()
    {
        var plan = ReadPlan.Compute("h", T(("0", 150)), Latest, [P("0", false, 100, 199), P("1", false, 0, 9)], EventHubsRedactor.None);
        Assert.Equal(StartAt.Sequence(150), plan[0].Start);
        Assert.True(plan[1].Done);
        Assert.Equal(10, plan[1].Next);
    }

    [Fact]
    public void Numeric_ids_order_numerically()
    {
        var plan = ReadPlan.Compute("h", null, Earliest, [P("10", true, -1, -1), P("2", true, -1, -1), P("0", true, -1, -1)], EventHubsRedactor.None);
        Assert.Equal(["0", "2", "10"], plan.Select(p => p.PartitionId));
    }

    [Fact]
    public void Non_numeric_ids_fall_back_to_ordinal_order()
    {
        var plan = ReadPlan.Compute("h", null, Earliest, [P("b", true, -1, -1), P("a", true, -1, -1), P("10", true, -1, -1)], EventHubsRedactor.None);
        Assert.Equal(["10", "a", "b"], plan.Select(p => p.PartitionId));
    }
}
