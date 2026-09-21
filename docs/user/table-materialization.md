# Materialize a Stream to a Table

Ursa can materialize a stream into an external table for analytics
consumption. The framework supports Apache Iceberg, Delta Lake, Delta on
Unity Catalog, and ClickHouse out of the box.

Internal compacted objects (COs) are controlled independently of external table
materialization by `compactedObjectEnabled` (default: `true`) in the compaction
service configuration. Keep it enabled for UFK, which needs WAL-to-CO compaction
for stream replay. For external Kafka/Pulsar table materialization that does not
need internal COs, set `compactedObjectEnabled=false`; external table writes and
commits continue normally. This setting does not enable an external sink: one must
still be configured for external-only materialization.

## Concept Quick Reference

- **TableCatalog** — a registered, named table store (e.g.,
  `iceberg-glue-prod`, `clickhouse-analytics`). Set up once at cluster scope.
- **TableMaterializationPolicy** — the per-stream or per-namespace policy
  describing where + how to materialize.
- Namespace policy is **active** — it materializes every stream in the
  namespace.
- Stream policy is the **override** — set only the fields that differ from
  the namespace default. Set `enabled = false` to opt out.

The exact records live in
`io.lakestream.api.materialization`; every functional field of
`TableMaterializationPolicy` is an `Optional<T>` so the same record can be
used as a full policy (at the namespace) or as a sparse override (at the
stream).

## Quickstart (via the StreamCatalog API)

```java
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.Compression;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.PartitionSpec;
import io.lakestream.api.materialization.PartitionTransform;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableConf;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// 1. Ops registers a TableCatalog (typically at startup via operator config).
TableCatalog ch = new TableCatalog(
        "clickhouse-prod",
        TableCatalogType.CLICKHOUSE,
        Map.of("dsn", "jdbc:ch://host:8123/default", "user", "ursa"),
        Map.of());
streamCatalog.registerTableCatalog(ch).join();

// 2. Platform team attaches a namespace policy. Every functional field is an
//    Optional, so set only what the namespace actually owns.
TableMaterializationPolicy namespacePolicy = new TableMaterializationPolicy(
        Optional.of("clickhouse-prod"),                        // catalogRef
        Optional.empty(),                                      // tableNaming: use source logical name
        Optional.empty(),                                      // tableIdentifier
        Optional.empty(),                                      // enabled
        Optional.empty(),                                      // framework
        Optional.of(EvolutionPolicy.forClickHouse()),          // evolution
        Optional.empty(),                                      // primaryKey
        Optional.empty(),                                      // baseSchemaVersion
        Optional.empty(),                                      // table
        Map.of());                                             // connectionOverrides
streamCatalog.setNamespaceMaterialization("analytics", namespacePolicy).join();

// 3. New streams in the "analytics" namespace materialize automatically.
//    Stream owners can override with a stream-level policy:
TableMaterializationPolicy override = new TableMaterializationPolicy(
        Optional.empty(),                                      // catalogRef inherits
        Optional.empty(),                                      // tableNaming
        Optional.empty(),                                      // tableIdentifier
        Optional.empty(),                                      // enabled
        Optional.empty(),                                      // framework
        Optional.empty(),                                      // evolution
        Optional.of(List.of("tenant_id", "event_id")),         // primaryKey
        Optional.empty(),                                      // baseSchemaVersion
        Optional.of(new TableConf(
                Optional.of(List.of(new PartitionSpec(
                        "event_date",
                        PartitionTransform.DAY,
                        Optional.empty()))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Compression.ZSTD))),               // table
        Map.of());                                             // connectionOverrides
streamCatalog.setStreamMaterialization(
        new StreamIdentifier("analytics", "events"), override).join();

// 4. Or opt one stream out of the namespace's active policy:
TableMaterializationPolicy optOut = new TableMaterializationPolicy(
        Optional.empty(),                                      // catalogRef
        Optional.empty(),                                      // tableNaming
        Optional.empty(),                                      // tableIdentifier
        Optional.of(false),                                    // enabled = false
        Optional.empty(),                                      // framework
        Optional.empty(),                                      // evolution
        Optional.empty(),                                      // primaryKey
        Optional.empty(),                                      // baseSchemaVersion
        Optional.empty(),                                      // table
        Map.of());                                             // connectionOverrides
streamCatalog.setStreamMaterialization(
        new StreamIdentifier("analytics", "internal_audit"), optOut).join();
```

> Note: `TableMaterializationPolicy` is a plain Java record — there is no
> builder or `toBuilder()` / wither API. Construct via the canonical
> constructor and use `TableMaterializationPolicy.empty()` as the starting
> point if you only need a couple of fields populated. Stream-level
> overrides only need the fields that differ from the namespace policy;
> every other field uses `Optional.empty()` to inherit.

> **What Ursa 1.0 applies.** Policy resolution uses `catalogRef`, `tableNaming` and
> `tableIdentifier`, and honors a stream-level `enabled = false`. A namespace-level `enabled` is
> ignored, and `connectionOverrides` are taken from the stream-level policy only. The built-in
> materializers also use these fields:
>
> - Iceberg and Delta: `connectionOverrides`. They take their base schema version from the
>   `clusterBaseSchemaVersion` task property, not from the policy.
> - ClickHouse: `primaryKey`, `framework.writeMode`, `framework.commit.batchSize` and
>   `connectionOverrides`
>
> The remaining fields are stored with the policy but not applied yet: `evolution`,
> `baseSchemaVersion`, `table` (partitioning, sort order, compression), `framework.startPosition`,
> `framework.paused`, `framework.errorHandling`, and the commit retry settings.

## Table Naming

A stream-level explicit `tableIdentifier` has highest priority. Otherwise an explicitly configured
namespace `TableNaming` template is used. With neither configured, external destinations use the
source-owned `lakestream.source.logical.name`, then the legacy `lakestream.kafka.topic.name`, and
finally `stream.name`.

Source integrations own these metadata properties. For example, Kafka records the topic automatically,
so a UUID-qualified storage stream can materialize to a stable topic-named SDT.

The resolved table name never replaces storage identity. Internal COs, partition metadata, and
Offset indexes continue to use the incarnation-qualified stream/log identity. Internal storage
compaction runs independently of whether an external destination is enabled.

`TableNaming.tableNameTemplate` is interpolated once per stream when the policy is resolved. Four
variables are supported, all case-sensitive:

| Variable | Resolves to |
|----------|-------------|
| `${stream.namespace}` | The stream's namespace. |
| `${stream.name}` | The storage stream's name within its namespace. |
| `${stream.logicalName}` | The source-owned logical name, falling back to `stream.name`. |
| `${stream.property.<key>}` | The value of the stream property `<key>`, taken from `StreamMetadata.properties()`. |

`${stream.property.<key>}` lets one namespace policy route streams to tables
named after something the stream itself carries — a tenant, a region, a dataset
name a producer set as a stream property:

```java
new TableNaming(Optional.of("analytics"), "${stream.property.dataset}_events")
```

Resolution fails with `IllegalArgumentException` when the template names a
variable that does not exist, and equally when it names a stream property that
is unset or blank on that stream: an unresolvable name is never silently
replaced with a default or an empty string. Templates are interpolated per
stream, so a policy valid for one stream can still fail for its neighbour.

Only `TableNaming.toTableIdentifier(StreamIdentifier, Map<String, String>)`
resolves property variables. The single-argument
`toTableIdentifier(StreamIdentifier)` has no properties to consult and rejects
any template that uses one. `tableNamespacePrefix` is used literally and is
never interpolated.

Both catalog-side resolution and the compaction worker's backwards-compatible task-property
fallback pass the available stream properties into the resolver. A property template therefore has
the same semantics on both paths. Resolution still fails when the referenced property is absent or
blank.

## Configuration Keys

Operator-side keys read on `CompactionScheduler` startup:

| Key | Default | Notes |
|-----|---------|-------|
| `materializationServiceClass` | `io.lakestream.ursa.lakehouse.compact.LakehouseMaterializationService` | Active `MaterializationService` SPI impl. |
| `compactionStorageBindingsClass` | `io.lakestream.ursa.lakehouse.compact.LakehouseCompactionStorageBindings` | Wires the publish / commit / cleanup runners. |
| `compactionServiceClass` | _(deprecated alias)_ | Honoured for one release. The scheduler logs a WARN when set without `materializationServiceClass`. |
| `iceberg.catalog.<name>.*` / `delta.catalog.<name>.*` / `unityCatalog*` | _(none)_ | Per-catalog connection settings. Translated into `TableCatalog` records on startup by `TableCatalogBootstrap`. |
| `clickhouse.catalog.<name>.dsn` / `…user` / `…password-ref` | _(none)_ | ClickHouse catalog connection bootstrap. |

See [ursa-storage-compact/AGENTS.md](../../ursa-storage-compact/AGENTS.md#configuration-keys-operator-surface)
for the full table.

## Internal compaction

Internal Parquet CO generation runs independently of SDT and does not require a table catalog.
The destination backend is selected by its catalog type and materializer factory. Internal CO
cleanup deletes stream storage files and indexes without changing SDT tables.

An external default policy is generated at startup only when both `materializationEnabled`
and SDT are enabled (`clusterSdtEnabled`, with `sdt.enabled` taking precedence).
Disabling SDT prevents this automatic policy creation while preserving internal CO compaction.

CO write results always record the per-file offset index, including when schema changes
produce multiple files in one compaction task. No configuration switch is required.

## Supported Sinks

| Backend | Type Constant | Declared Evolution Policy |
|---------|---------------|------------------|
| Iceberg | `TableCatalogType.ICEBERG` | `EvolutionPolicy.forIceberg()` — addColumn, addNullableColumn, widenType |
| Delta Lake | `TableCatalogType.DELTA` | `EvolutionPolicy.forDelta()` — same as Iceberg |
| Delta on Unity Catalog | `TableCatalogType.DELTA_UC` | `EvolutionPolicy.forDelta()` |
| ClickHouse | `TableCatalogType.CLICKHOUSE` | `EvolutionPolicy.forClickHouse()` — addColumn, addNullableColumn only |

Each materializer declares an evolution policy. Ursa 1.0 doesn't use it to accept or reject schema
changes yet. Each materializer evolves its own table as the source schema changes.

To add a materializer for another destination, see
[Write a materializer](../developer/materializer-guide.md).

## Troubleshooting

- **`MESSAGE_SCHEMA_INCOMPATIBLE`** — a record's schema can't be applied to
  the destination table. What happens next depends on the materializer:
  - ClickHouse fails the task with this code when a column's type would change.
    It only ever adds columns.
  - Iceberg and Delta raise it when a record's schema version hasn't been
    applied to the table and the table is already at a newer version, or when
    the version is below the base schema version before the table exists. The
    record goes to the dead-letter table, and materialization continues. If
    Delta's dead-letter table is disabled (`delta.dlt.enabled=false`), the task
    fails instead.

  Check the source schema's version history in the schema registry, and the
  `clusterBaseSchemaVersion` setting.
- **`MaterializationException` with `LAKEHOUSE_*` codes** — sink-side commit
  failure. Check the underlying catalog (Iceberg/Delta/Unity) status.
- **No materialization happening** — verify
  `streamCatalog.resolveMaterialization(streamId).join().isPresent()`. Common causes:
  `catalogRef` doesn't resolve to a registered `TableCatalog`; stream policy set
  `enabled = false`.

Ursa 1.0 doesn't emit materialization-specific metrics. To debug, use the
compactor logs:

- `Materializing [start,end) of stream … into catalog …` when a task starts.
- `Committed task … in MaterializationService` when its materializers have
  committed. For Iceberg and Delta, the table commit itself happens afterwards,
  in a separate group-commit step.
- `During compact error` when a task fails, with the exception.
- `Quarantine topic … (code=…)` when a failed task is held back before its
  next retry.

The `ursa.storage.compact.failed.task.count` counter counts failed tasks, except
transient source errors and tasks that are deleted as terminal. The compactor
exports metrics through OpenTelemetry only if you choose an exporter, for
example with `-Dotel.metrics.exporter=otlp` and the exporter on the classpath.
The default is `none`.

## See Also

- [LIP-161: Table Materialization Framework](../lip/LIP-161-Table-Materialization-Framework.md)
- [Write a materializer](../developer/materializer-guide.md)
- [Compaction Orchestration Flow](../../ursa-storage-compact/AGENTS.md#orchestration-flow-t10)
- [ClickHouse module notes](../../ursa-storage-clickhouse/AGENTS.md)
- [Lakehouse materializer adapter notes](../../ursa-storage-lakehouse/AGENTS.md)
