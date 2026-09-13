namespace Pz.Connector.EventHubs.Tests.Fakes;

/// <summary>A clock that only moves when a test moves it, so an idle-timeout fact can assert on
/// elapsed time without a real wall-clock wait.</summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    public long Ticks;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => Ticks;

    public void Advance(TimeSpan by) => Ticks += by.Ticks;
}
