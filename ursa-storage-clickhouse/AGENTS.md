# ursa-storage-clickhouse

ClickHouse sink for the stream materialization framework. Implements the
`TableMaterializer` SPI defined in `ursa-storage-materialization` and is
discovered by the orchestrator via `ServiceLoader`.

## Key Classes

| Class | Purpose |
|-------|---------|
| `ClickHouseTableMaterializerFactory` | `TableCatalogType.CLICKHOUSE` factory, registered via `META-INF/services` |
| `ClickHouseTableMaterializer` | Batched INSERT into `ReplacingMergeTree` or `MergeTree` per `WriteMode` |
| `ClickHouseTableSchemaService` | Creates the table and adds columns (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`) for the rows being written; also implements `TableSchemaService` |
| `AvroToClickHouseSchema` | Avro → ClickHouse type-translation helper; the production write path doesn't use it |
| `ClickHouseConnectionFactory` | JDBC connection construction from `TableCatalog.connection` + `policy.connectionOverrides` |
| `ClickHouseTableEngine` | Maps a policy's `WriteMode` + primary key onto a concrete table engine (`MergeTree` / `ReplacingMergeTree`) |
| `ClickHouseColumn` / `ClickHouseSchema` | Lightweight POJOs representing the resolved column set |

## SPI Registration

```
src/main/resources/META-INF/services/io.lakestream.ursa.materialization.TableMaterializerFactory
  → io.lakestream.ursa.clickhouse.ClickHouseTableMaterializerFactory
```

`LakehouseMaterializationService` discovers the factory via `ServiceLoader`
when a stream resolves to a `TableCatalog` of type `CLICKHOUSE`. No
configuration is required beyond registering the `TableCatalog` itself.

## Running Tests

```bash
# Unit tests (default — fast, no Docker)
mvn -B -ntp test -pl ursa-storage-clickhouse

# Integration tests (requires Docker)
mvn -B -ntp test -pl ursa-storage-clickhouse -Dgroups=clickhouse -DexcludeGroups=
```

The integration tests use Testcontainers and require Docker. The
`clickhouse` tag is excluded by default via the module pom's
`<excludeGroups>clickhouse</excludeGroups>` so unit tests stay fast.

## Pitfalls

- `clickhouse-jdbc:0.6.5:all` is pinned in this module
  (`<clickhouse-jdbc.version>0.6.5</clickhouse-jdbc.version>` in the module
  pom) because the parent dependency management does not cover it.
- Schema evolution on the write path goes through `ensureColumns`: it creates
  the table if needed, adds columns that appear in a batch, and rejects a type
  change on an existing column with
  `MaterializationException(MESSAGE_SCHEMA_INCOMPATIBLE)`. Columns missing from
  a batch are left in place. `evolveTableSchema` also rejects drops, but the
  write path doesn't call it.
- Idempotency relies on `ReplacingMergeTree` `ORDER BY` over the primary
  key. `WriteMode.UPSERT` or `CDC`, or a non-empty primary key in the policy
  (`TableMaterializationPolicy.primaryKey()`), selects `ReplacingMergeTree`.
  `UPSERT` and `CDC` need a primary key: without one, the table is created
  without an `ORDER BY`. Otherwise the engine is a plain `MergeTree`
  (`ORDER BY tuple()`), which keeps duplicate rows when a task is replayed.
  Rows are inserted every `batchSize` rows during `write()`, so they are
  visible before `commit()`.
- The integration tests pull the ClickHouse server image lazily; first run
  is slow.
- `clickhouse-http-client` is test-scoped. The `:all` driver loads the HTTP
  transport by reflection, so the integration tests need it, and a deployment
  must provide it on the compactor classpath. Don't remove it from the pom
  even though no source code references it.
