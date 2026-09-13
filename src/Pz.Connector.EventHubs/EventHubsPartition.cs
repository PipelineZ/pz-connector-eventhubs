using System.Runtime.CompilerServices;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.EventHubs;

/// <summary>The whole event hub as one pz partition. Plan: partition ids and properties, then
/// <see cref="ReadPlan"/>. Read: one receiver per unfinished partition in id order, drained until
/// its bound. The token candidate is the plan's Next map -- sequence numbers are contiguous per
/// partition, so reaching the bound means every event below it was landed -- and it exists only
/// after a completed enumeration: a cancelled or failed read leaves the stored token untouched, so
/// the engine re-reads the same slice next time.</summary>
internal sealed class EventHubsPartition(
    EventHubsConnectionConfig connection, IEventHubsClientFactory factory, EventHubsDatasetConfig dataset,
    SequenceToken? token, ILogger logger, TimeProvider? time = null) : IDatasetPartition, ISyncStatePartition
{
    internal const int ReceiveBatchSize = 500;
    internal static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(1);

    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private string? _candidate;

    public bool TryGetSyncStateCandidate(out string? candidate)
    {
        candidate = _candidate;
        return candidate is not null;
    }

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        _candidate = null;
        var redactor = connection.Redactor;
        var hub = dataset.EventHub;
        var context = $"event hub '{hub}' in namespace '{connection.NamespaceHost}'";

        await using var reader = factory.CreateReader(connection, hub);
        IReadOnlyList<PartitionPlan> plans;
        try
        {
            var ids = await reader.GetPartitionIdsAsync(ct).ConfigureAwait(false);
            var infos = new List<PartitionInfo>(ids.Count);
            foreach (var id in ids)
            {
                ct.ThrowIfCancellationRequested();
                infos.Add(await reader.GetPartitionPropertiesAsync(id, ct).ConfigureAwait(false));
            }

            plans = ReadPlan.Compute(hub, token, dataset.Start, infos, redactor);
        }
        catch (Azure.Messaging.EventHubs.EventHubsException ex) when (ex.Reason == Azure.Messaging.EventHubs.EventHubsException.FailureReason.ResourceNotFound)
        {
            throw EventHubsErrors.Fatal(Codes.HubNotFound, $"{context} does not exist; create it or fix the dataset's `event_hub:`", redactor);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw EventHubsErrors.Wrap(ex, redactor, Codes.ReceiveFailed, $"{context}: planning the read");
        }

        logger.LogDebug("eventhubs: {Hub}: {Partitions} partitions, {Unfinished} with events to read",
            hub, plans.Count, plans.Count(p => !p.Done));

        var builder = new EnvelopeBatchBuilder(dataset.Encoding, options, redactor);
        var idle = TimeSpan.FromSeconds(connection.IdleTimeoutSeconds);
        foreach (var plan in plans.Where(p => !p.Done))
        {
            ct.ThrowIfCancellationRequested();
            await using var partition = reader.OpenPartition(plan.PartitionId, plan.Start!.Value);
            var lastEvent = _time.GetTimestamp();
            var finished = false;
            while (!finished)
            {
                ct.ThrowIfCancellationRequested();
                IReadOnlyList<ReceivedEvent> events;
                try
                {
                    events = await partition.ReceiveAsync(ReceiveBatchSize, ReceiveWait, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
                {
                    throw EventHubsErrors.Wrap(ex, redactor, Codes.ReceiveFailed, $"{context} partition {plan.PartitionId}: receiving");
                }

                if (events.Count == 0)
                {
                    if (_time.GetElapsedTime(lastEvent) > idle)
                    {
                        throw EventHubsErrors.Transient(Codes.IdleTimeout,
                            $"{context} partition {plan.PartitionId}: no event arrived for {connection.IdleTimeoutSeconds}s with the partition " +
                            $"still short of sequence number {plan.Bound}; the service may be unreachable", redactor);
                    }

                    continue;
                }

                lastEvent = _time.GetTimestamp();
                foreach (var e in events)
                {
                    if (e.SequenceNumber > plan.Bound)
                    {
                        finished = true;
                        break;
                    }

                    builder.Append(hub, plan.PartitionId, e);

                    // Polled after every event, not once per received batch: the builder holds no
                    // queue, so waiting for a whole receive batch to drain before polling would let
                    // pending rows grow past MaxRowsPerBatch.
                    if (builder.TryTakeBatch(out var batch))
                    {
                        yield return batch!;
                        // The idle clock measures service silence, not engine backpressure on the batch just handed over.
                        lastEvent = _time.GetTimestamp();
                    }

                    if (e.SequenceNumber == plan.Bound)
                    {
                        finished = true;
                        break;
                    }
                }
            }
        }

        if (builder.Flush() is { } last)
        {
            yield return last;
        }

        _candidate = new SequenceToken(hub, plans.ToDictionary(p => p.PartitionId, p => p.Next)).Serialize();
    }
}
