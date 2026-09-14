using Pz.Connector.EventHubs;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new EventHubsConnector(ctx.LoggerFactory)).ConfigureAwait(false);
