namespace Pz.Connector.EventHubs.Tests;

public sealed class OptionsTests
{
    [Fact]
    public void Integral_double_is_an_integer()
    {
        var errors = new List<string>();
        Assert.Equal(90, Options.Int(new Dictionary<string, object?> { ["idle_timeout"] = 90.0 }, "idle_timeout", 60, 1, 3600, "", errors));
        Assert.Empty(errors);
    }

    [Fact]
    public void Out_of_range_and_fractional_are_errors()
    {
        var errors = new List<string>();
        Options.Int(new Dictionary<string, object?> { ["idle_timeout"] = 0 }, "idle_timeout", 60, 1, 3600, "", errors);
        Options.Int(new Dictionary<string, object?> { ["idle_timeout"] = 1.5 }, "idle_timeout", 60, 1, 3600, "", errors);
        Assert.Equal(2, errors.Count);
        Assert.All(errors, e => Assert.Contains("'idle_timeout' must be an integer between 1 and 3600", e));
    }
}
