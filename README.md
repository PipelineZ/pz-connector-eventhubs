# Pz.Connector.EventHubs

Azure Event Hubs source and sink for [PipelineZ](https://pipelinez.dev) (`pz`), served out of
process. An event hub reads as a **feed**: every run lands the events between the per-partition
sequence numbers stored from the last run and each partition's last enqueued sequence number at
the start of this one, and hands `pz` the new numbers as the dataset's sync-state token. A sink
output **appends**: one sent event per row.

## Installation

```yaml
# project.yml
connectors:
  - package: Pz.Connector.EventHubs
    version: 0.1.0
```

`pz restore` installs the binary for your platform (linux-x64, linux-arm64, osx-arm64, win-x64) and
`pz run` spawns it. The binary is Native AOT: no .NET runtime is needed on the machine that runs it,
and it starts in milliseconds.

## Connection

```yaml
# connections.yml
hub:
  connector: eventhubs
  auth: connection_string                     # connection_string | credential_chain | service_principal | managed_identity
  connection_string: ${EVENTHUBS_CONNECTION}  # auth: connection_string -- namespace-level, no EntityPath
  namespace: myns.servicebus.windows.net      # every other auth -- the fully qualified namespace host
  tenant_id: ${AZURE_TENANT_ID}               # auth: service_principal
  client_id: ${AZURE_CLIENT_ID}               # auth: service_principal; optional for managed_identity (user-assigned)
  client_secret: ${AZURE_CLIENT_SECRET}       # auth: service_principal
  consumer_group: $Default                    # optional; default $Default
  transport: amqp_tcp                         # optional; amqp_tcp (default) | amqp_websockets
  idle_timeout: 60                            # optional, seconds 1..3600; default 60
```

`auth` picks one credential shape and only that shape's fields: `connection_string` needs the
connection string; `service_principal` needs `namespace`, `tenant_id`, `client_id` and
`client_secret`; `credential_chain` (the ambient Azure credential chain) and `managed_identity`
need `namespace`. A field belonging to a different shape is refused, so one connection never says
two things, and every problem in the block is reported at once (`PZEH0101`).

The connection string is namespace-level: the event hub is the entity name in `connections.yml`,
never an `EntityPath` component. `idle_timeout` bounds silence on a read -- an unfinished partition
that delivers no event for that long fails transiently and the engine retries the node, leaving the
stored token untouched.

The connection string, the client secret, and the `SharedAccessKey` / `SharedAccessSignature`
components of the connection string are redacted from every error, log line and connection check.

Against the [Event Hubs emulator](https://learn.microsoft.com/azure/event-hubs/overview-emulator):

```yaml
  auth: connection_string
  connection_string: "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
```

## Reading an event hub

```yaml
  entities:
    orders:
      read:
        event_hub: orders        # optional; defaults to the entity name
        start: earliest          # earliest (default) | latest | 2026-01-01T00:00:00Z
        encoding: utf8           # utf8 (default) | base64
```

Every event lands as one row:

| column | type | |
|---|---|---|
| `event_hub` | varchar | the resolved event hub name |
| `partition` | varchar | partition ids are strings (`0`, `1`, ...) |
| `sequence_number` | bigint | |
| `offset` | varchar, nullable | the service's offset for the event, when it reports one |
| `enqueued_time` | timestamp (UTC) | |
| `partition_key` | varchar, nullable | |
| `body` | varchar | text, or base64 of the raw bytes with `encoding: base64`; `''` for an empty body |
| `properties` | varchar | JSON object of the application properties; `{}` when there are none |
| `content_type` | varchar, nullable | |

Decode in SQL: `json_extract_string(body, '$.customer')`, `json_extract_string(properties,
'$.schema_version')`. With `encoding: utf8` a body that is not valid UTF-8 fails the read
(`PZEH0205`), naming the event hub, partition and sequence number; switch that dataset to
`base64`.

`start:` applies only to a partition with no stored sequence number: the first run,
`pz run --full-refresh`, or a partition added since the token was written. It never re-applies to a
partition the connector has already recorded, even one that has never held an event.

The token records the namespace and the event hub the numbers came from. Pointing the connection at
a different namespace, or the dataset at a different event hub, fails the run (`PZEH0202`) instead of
resuming against numbers that mean something else there -- sequence numbers are assigned per
partition of one hub in one namespace. Start over with `pz run --full-refresh`, or point the
connection back where the token was written.

A stored sequence number that retention has already dropped fails the run (`PZEH0203`,
non-transient) rather than skipping events, naming the partition, the stored number and the
earliest one still available. A number past the end of the partition fails the same way -- an event
hub deleted and recreated under the same name restarts its sequence numbers. Recover with
`pz run --full-refresh`, or edit the dataset's state with `pz state`.

A feed dataset paired with an `append` output needs `duplicates: accept` on that output
(`PZ0214`): a retried run can re-deliver a slice. For the same reason a feed cannot feed a
`replace` output at all (`PZ0335`) -- a slice is not a snapshot, and replacing would discard rows
an earlier run already delivered.

## Writing an event hub

```yaml
  entities:
    order_events:
      write:
        event_hub: order-events              # optional; defaults to the entity name
        strategy: append                     # the only strategy event hubs support
        body: payload                        # optional varchar column sent verbatim; omit for whole-row JSON
        partition_key: region                # optional column (varchar, integer, or bigint)
        properties: [source, schema_version] # optional columns sent as application properties
        content_type: application/json       # optional
```

Without `body:`, each row becomes a JSON object of every column not named in `partition_key:` or
`properties:`: integers and doubles as numbers, decimals as strings (every digit of a 38-digit
value), booleans, dates as `yyyy-MM-dd`, timestamps as `yyyy-MM-ddTHH:mm:ss.ffffffZ`, nulls as
`null`. A column of any other type is refused when the write starts, before an event is sent,
naming the column (`PZEH0302`): name a `body:` column, or drop it from the pipeline's projection.
The key and the properties are therefore not repeated inside the body; to have a column in both
places, project it twice under two names and point `partition_key:` at the copy. With `body:` set,
the event body is that column and nothing else: no JSON object is built, so the exclusion rule does
not apply and a `partition_key:` or `properties:` column changes nothing about what is sent.

Application properties keep their type across the wire: varchar stays a string, integer and bigint
arrive as a 64-bit integer, double as a double, boolean as a boolean, and date and timestamp as
`yyyy-MM-dd` / `yyyy-MM-ddTHH:mm:ss.ffffffZ` strings. A null property value is omitted from the
event rather than sent as a null; a null `partition_key` value sends the event with no partition
key, letting the service place it.

Rows with the same partition key travel in the same batch and land on the same partition, in the
order the pipeline produced them. Batching is therefore per distinct key value per pipeline batch: a
low-cardinality key (a region, a tenant, a shard) packs many rows into each send, while a key that is
close to unique per row -- an order id, a uuid -- costs one send per row and is much slower. Omit
`partition_key:` for maximum throughput when placement does not matter: every row then travels in the
service-placed batches, which is the cheapest shape. Every send is awaited before `WriteBatchAsync` returns, so
nothing is outstanding when a batch is acknowledged. Delivery across runs is at-least-once, as for
every `append` output, and a sent event cannot be unsent: a failed run's events stay in the hub.

A single row whose event exceeds the service's per-batch size limit (~1 MB) fails the write
(`PZEH0303`), naming the row's position in the batch -- no partial batch is silently dropped. The event hub must
already exist; a send to an unknown one fails with `PZEH0204` where the service reports it
missing -- the local emulator answers a missing hub with a transient communication failure instead,
which surfaces as `PZEH0304` on a write and `PZEH0207` on a read.

## Packaging

The package carries one Native AOT binary per RID (linux-x64, linux-arm64, osx-arm64, win-x64) and
declares `runtime: "process"`: `pz` spawns it and speaks the connector protocol over a pipe, so a
crash or a hang in it is contained to that process.

## Development

```bash
dotnet build Pz.Connector.EventHubs.slnx -c Release
dotnet test Pz.Connector.EventHubs.slnx -c Release --no-build          # emulator facts need docker; they SKIP without it
dotnet publish src/Pz.Connector.EventHubs -c Release -r linux-x64      # stage the host RID
dotnet pack src/Pz.Connector.EventHubs -c Release -o packages          # nupkg with pz.connector.json
```

Native AOT needs a C toolchain (`clang` and the platform's linker) on the publishing machine. On a
cold NuGet cache, restore the RID explicitly first (`dotnet restore src/Pz.Connector.EventHubs -r
linux-x64`) before publishing with `--no-restore`; `publish -r` alone can skip pulling the
RID-specific packs the build needs (NETSDK1112).

`tests/e2e` runs the packaged connector end to end through `pz` against the emulator: seed a hub
from a CSV, relay it hub-to-hub, drain it back to a file. `docker compose -f
tests/e2e/emulator/docker-compose.yml up -d` starts the namespace those projects expect.

Releases are tag-triggered (`v*`) and publish to nuget.org through trusted publishing.
