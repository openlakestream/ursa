# Write a materializer

A *materializer* turns a stream into a table in some other system.

Ursa writes each stream to object storage through a write-ahead log (WAL). In the background, a
service called the *compactor* rewrites the WAL into larger, columnar *compacted objects*. Ursa's
stream materialization framework runs inside that compaction pass. The compactor reads a range of a
stream once, and hands each entry to the materializer, which writes it to the destination and
commits.

Ursa ships materializers for Apache Iceberg, Delta Lake (including Delta on Unity Catalog) and
ClickHouse. This guide shows you how to write one for a destination that Ursa doesn't support yet.

The design behind the framework is in
[LIP-161](https://github.com/lakestream-io/lips/blob/main/proposals/LIP-161-Table-Materialization-Framework.md).
For how users configure materialization, see
[Materialize a stream to a table](../user/table-materialization.md).

## Before you start

- **Materializers live in their own repositories.** Build yours against the published Ursa
  artifacts. This repository keeps the Iceberg, Delta and ClickHouse materializers.
- **Every new materializer needs a LIP.** Start with the
  [New materializer proposal](https://github.com/lakestream-io/ursa/issues/new?template=materializer_proposal.yml)
  form. Then write the LIP in [lakestream-io/lips](https://github.com/lakestream-io/lips), following
  its [contributing guide](https://github.com/lakestream-io/lips/blob/main/CONTRIBUTING.md). The
  template has a section for materializers.
- **You also need a small pull request here.** In Ursa 1.0, the materializer type is a closed enum
  in `lakestream-api` (`TableCatalogType`), and the compactor's code switches on it. Each new materializer
  therefore adds a constant and a few `case` lines to this repository. The
  [registration pull request](#register-the-type-in-ursa) section lists exactly what changes. Opening
  up the SPI so that this step disappears is planned work.
- **Materializers are written in Java.** The compactor discovers them with `ServiceLoader` and runs
  them inside its JVM. To build one in another language, start a thread in
  [Discussions: Ideas](https://github.com/lakestream-io/ursa/discussions/categories/ideas). An
  out-of-process design is an open question, and we'd like to work it out with you.
- **Develop against a local build until a release has your type.** Your factory refers to your
  `TableCatalogType` constant, which exists only in your registration branch until a release ships
  it. In your checkout of that branch, first give Ursa a version of its own, so that your build
  doesn't shadow the published release in your local Maven repository:
  `mvn versions:set -DnewVersion=1.1.0-mysink-SNAPSHOT -DgenerateBackupPoms=false`. Then run
  `mvn -B -ntp install -DskipTests`, and use that version as `ursa.version`. Operators can run your
  materializer once they run an Ursa release that includes your type.

## How a materializer runs

```text
StreamCatalog.resolveMaterialization(stream)       policy + catalog -> ResolvedMaterialization
        │
CompactionWorker                                    one compaction task = one partition's log, one offset range
        │
LakehouseMaterializationService                     picks the factory whose catalogType() matches
        │
TableMaterializerFactory.create(...)                builds a new materializer for this task
        │
for each entry in the range:  write(entry)          your materializer, and Ursa's internal compacted-object writer
        │
commit()                                            your materializer first, then the internal writer
```

The details that shape your implementation:

- **One materializer per task.** A compaction task covers one offset range of one *partition log*,
  the log that stores one partition of a stream. The factory's `create` method runs for every task,
  and the materializer it returns is used once, by one thread, then dropped. Partitions of the same
  stream run as separate tasks, often at the same time.
- **The range is read once.** Each entry goes to your materializer and to Ursa's internal
  compacted-object writer, and each receives its own reference to the entry.
- **A failed task usually replays the same range.** If anything in a task fails, including a step
  after your `commit()` succeeded, the compactor usually runs the task again later, over the same
  offset range, with a new materializer. [Handle failures](#handle-failures) lists the exceptions.
- **Everything happens inside the compactor.** Materialization reads from object storage, not from
  brokers, so it adds no load to producers or consumers. It does share the compactor's fixed pool of
  task threads with every other stream, and a stream's internal compaction commits only after its
  materializer does. A slow or failing destination therefore holds up compaction.

The compactor calls materializers only when `materializationEnabled=true` is set in its
configuration. The default, `false`, keeps the older compaction path, which never calls
materializers.

## Set up the project

Depend on the Ursa artifacts from Maven Central. Use `provided` scope for all of them, because the
compactor already ships them at runtime.

```xml
<properties>
  <ursa.version>1.0.0</ursa.version>
</properties>

<dependencies>
  <dependency>
    <groupId>org.openlakestream</groupId>
    <artifactId>ursa-storage-materialization</artifactId>
    <version>${ursa.version}</version>
    <scope>provided</scope>
  </dependency>
  <!-- GenericEntry wraps ursa-storage-core's Entry type. -->
  <dependency>
    <groupId>org.openlakestream</groupId>
    <artifactId>ursa-storage-core</artifactId>
    <version>${ursa.version}</version>
    <scope>provided</scope>
  </dependency>
  <!-- EntryEncoderContext refers to Iceberg's Variant type. Match the version Ursa uses. -->
  <dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-core</artifactId>
    <version>1.10.0</version>
    <scope>provided</scope>
  </dependency>
  <!-- Your destination's client library goes here, at compile scope. -->
</dependencies>
```

`ursa-storage-materialization` brings `lakestream-api`, `ursa-storage-common` and the Kafka
schema-registry client with it. Ursa targets Java 17.

## Implement the factory

The factory is the entry point that the compactor discovers. It has three jobs: declare its
catalog type, build a materializer for each task, and (in the future) provide a schema service.

```java
// MySinkMaterializerFactory.java
package com.example.ursa.mysink;

import io.lakestream.api.StreamMetadata;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.TableMaterializerFactory;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSourceMetadata;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

public final class MySinkMaterializerFactory implements TableMaterializerFactory {

    @Override
    public TableCatalogType catalogType() {
        // Added to lakestream-api by your registration pull request.
        return TableCatalogType.MYSINK;
    }

    @Override
    public TableMaterializer<?> create(TableMaterializationPolicy policy,
                                       TableCatalog catalog,
                                       StreamMetadata stream,
                                       MaterializationRuntime runtime) {
        if (catalog.type() != catalogType()) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "Catalog " + catalog.name() + " has type " + catalog.type());
        }
        // The compactor has already resolved the destination table.
        TableIdentifier table = policy.tableIdentifier().orElseThrow(() ->
                new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                        "No table identifier for stream " + stream.identifier().fullName()));

        // Per-stream overrides win over the catalog's connection settings.
        Map<String, String> connection = new HashMap<>(catalog.connection());
        connection.putAll(policy.connectionOverrides());

        // The logical topic name is what the schema registry knows the stream by.
        String topic = KafkaSourceMetadata.topicName(
                stream.identifier().fullName(), runtime.taskProperties());
        // The partition log identifies this task's source; use it to make commits idempotent.
        String partitionLog = runtime.taskProperties().get(MaterializationRuntime.SOURCE_TOPIC_PROPERTY);

        MySinkRowEncoder encoder = new MySinkRowEncoder(runtime.schemaService());
        MySinkClient client = MySinkClient.connect(connection);
        return new MySinkMaterializer(client, table, encoder, topic, partitionLog);
    }

    @Override
    @Nullable
    public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                  TableCatalog catalog,
                                                  StreamMetadata stream) {
        // Ursa 1.0 doesn't call this method. Returning null is fine.
        return null;
    }
}
```

`MySinkClient` stands in for your destination's client library. The examples use these methods:

```java
// MySinkClient.java
package com.example.ursa.mysink;

import io.lakestream.api.materialization.TableIdentifier;
import java.util.List;
import java.util.Map;

/** Stands in for your destination's client library. */
public interface MySinkClient extends AutoCloseable {

    static MySinkClient connect(Map<String, String> connection) {
        throw new UnsupportedOperationException("Connect to your destination here");
    }

    /** Whether a batch ending at {@code lastOffset} of {@code partitionLog} is already committed. */
    boolean hasCommitted(String partitionLog, long lastOffset);

    /** Writes the rows and the commit marker in a single transaction. */
    void writeAtomically(TableIdentifier table, List<Map<String, Object>> rows,
                         String partitionLog, long lastOffset);

    @Override
    void close();
}
```

Some notes on the factory:

- **Keep `create` cheap.** It runs for every task. The ClickHouse materializer opens one JDBC
  connection per task. If your destination's connections are expensive, share a client across
  tasks, and make it thread-safe, because tasks run concurrently. A shared client must not be closed
  in the materializer's `commit()` or `close()`, as the example below closes its own.
- **Don't hold on to the policy or the catalog** beyond the materializer you return.
- **Clean up if construction fails.** If `create` throws after it opened a connection, close that
  connection first.
  [`ClickHouseTableMaterializerFactory`](../../ursa-storage-clickhouse/src/main/java/io/lakestream/ursa/clickhouse/ClickHouseTableMaterializerFactory.java)
  shows the pattern.
- **Use the right topic name.** `KafkaSourceMetadata.topicName` returns the logical topic name that
  the schema registry uses. `MaterializationRuntime.SOURCE_TOPIC_PROPERTY` holds the partition log
  for this task, which is unique per partition. Use it when you record what you committed.

## Implement the materializer

A `TableMaterializer<GenericEntry>` receives entries, buffers them, and commits them once.

```java
// MySinkMaterializer.java
package com.example.ursa.mysink;

import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.CommitResult;
import io.lakestream.ursa.materialization.MaterializationContext;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.MissingSchemaVersionTracker;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MySinkMaterializer implements TableMaterializer<GenericEntry> {

    private static final Logger LOG = LoggerFactory.getLogger(MySinkMaterializer.class);

    private final MySinkClient client;
    private final TableIdentifier table;
    private final MySinkRowEncoder encoder;
    private final String topic;
    private final String partitionLog;
    private final List<Map<String, Object>> rows = new ArrayList<>();
    private long lastOffset = -1;
    private boolean committed;
    private boolean closed;

    MySinkMaterializer(MySinkClient client, TableIdentifier table, MySinkRowEncoder encoder,
                       String topic, String partitionLog) {
        this.client = client;
        this.table = table;
        this.encoder = encoder;
        this.topic = topic;
        this.partitionLog = partitionLog;
    }

    @Override
    public void write(GenericEntry entry, MaterializationContext context) {
        // From this point on you own the entry, even if this method throws.
        if (committed || closed) {
            entry.entry().payload().release();
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "write() after commit() or close()");
        }
        EntryEncoderContext encoderContext = EntryEncoderContext.builder()
                .missingSchemaVersionTracker(new MissingSchemaVersionTracker())
                .build();
        try {
            // encode() takes over the entry and releases it on every path.
            encoder.encode(topic, entry, new ResultConsumer<MaterializationRecord<Map<String, Object>>>() {
                @Override
                public void onResult(MaterializationRecord<Map<String, Object>> record) {
                    rows.add(record.record());
                    record.metadata().ifPresent(metadata ->
                            lastOffset = Math.max(lastOffset, metadata.getEntryHeader().offset()));
                }

                @Override
                public void onErrorWithCtx(Object failed, Throwable error) {
                    // A failed record arrives as its own GenericEntry, which this callback owns.
                    try {
                        throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                                "Can't decode a record from " + topic, error);
                    } finally {
                        if (failed instanceof GenericEntry failedEntry) {
                            failedEntry.entry().payload().release();
                        }
                    }
                }
            }, null, encoderContext);
        } catch (MaterializationException e) {
            throw e;
        } catch (RuntimeException e) {
            // Anything else thrown from write() would be treated as a source read error.
            throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                    "Can't decode an entry from " + topic, e);
        }
    }

    @Override
    public CommitResult commit() {
        if (committed) {
            return new CommitResult(0, 0, Map.of());
        }
        if (closed) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR, "commit() after close()");
        }
        try {
            // A retried task replays the same range. Skip it if it's already committed.
            if (!rows.isEmpty() && !client.hasCommitted(partitionLog, lastOffset)) {
                client.writeAtomically(table, rows, partitionLog, lastOffset);
            }
            committed = true;
            return new CommitResult(rows.size(), 0, Map.of("mysink.table", table.namespace() + "." + table.name()));
        } catch (MaterializationException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_COMMIT_ERROR,
                    "Commit to " + table.namespace() + "." + table.name() + " failed", e);
        } finally {
            // Ursa 1.0 doesn't call close() after a successful commit, so release resources here.
            close();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            client.close();
        } catch (RuntimeException e) {
            // close() runs on cleanup paths, so log instead of throwing.
            LOG.warn("Failed to close the client for {}.{}", table.namespace(), table.name(), e);
        }
    }

    @Override
    public EvolutionPolicy supportedEvolutions() {
        // Describe the schema changes your destination can follow. Ursa 1.0 doesn't consult this yet.
        return EvolutionPolicy.forClickHouse();
    }
}
```

### The contract

| Method | What the framework expects |
|---|---|
| `write(entry, context)` | You own `entry` from the moment `write` is called, **including when you throw**. Release its payload exactly once, on every path. If you hand the entry to an encoder, the encoder releases it. Throw only `MaterializationException` (see [Handle failures](#handle-failures)). |
| `commit()` | Makes everything written so far durable. The framework calls it once, after the last `write`. Make a second call harmless, and make the whole commit safe to replay (see [Commit exactly once](#commit-exactly-once)). |
| `close()` | Releases resources. Make it idempotent, and don't throw from it: the framework calls it from cleanup paths, where a new exception would hide the original failure. |
| `supportedEvolutions()` | Describes the schema changes your destination supports. Ursa 1.0 stores and returns it, but doesn't use it to gate changes yet. |

Three behaviors of Ursa 1.0 to design around:

- **`close()` isn't called after a successful task.** The compactor calls `close()` only when
  writing or committing fails, which can happen after your own `commit()` succeeded, if a later
  commit in the same task failed. After a successful task, the materializer is dropped without a
  `close()` call. So release connections and other resources at the end of `commit()`, as the
  example does, and make `close()` safe to call afterwards.
- **Don't rely on `MaterializationContext`.** Its offset is the task's start offset plus the entry's
  index within the task, which isn't a record offset. Its timestamp is 0, and its schema version and
  metadata are empty. Take offsets and schema versions from the decoded records instead:
  `MaterializationRecord.metadata()` carries each record's entry header and schema version.
- **An instance is used by one thread.** You don't need to synchronize inside a materializer, but
  anything you share between materializers must be thread-safe.

## Decode the records

Stream entries hold native Kafka `MemoryRecords` batches. Extend `KafkaEntryEncoder` to turn each
record into your destination's row type. The base class splits batches, looks up each record's schema
in the schema registry, deserializes the value, and releases the entry. You implement `transform`.

```java
// MySinkRowEncoder.java
package com.example.ursa.mysink;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaEntryEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

final class MySinkRowEncoder extends KafkaEntryEncoder<Map<String, Object>> {

    MySinkRowEncoder(SchemaService<?> schemaService) {
        // The compactor supplies a KafkaSchemaService; any other type is rejected here.
        super(schemaService);
    }

    @Override
    protected Map<String, Object> transform(Object value, SchemaMetadata schema, SchemaKey schemaKey,
                                            TableSchemaService tableSchemaService,
                                            EntryEncoderContext context) throws MessageSerDeException {
        if (value == null) {
            throw new MessageSerDeException(ExceptionCode.MESSAGE_NULL_VALUE, "Null values aren't supported");
        }
        // A naive mapping, to show the input types. Replace it with your destination's type mapping.
        Map<String, Object> row = new LinkedHashMap<>();
        if (value instanceof GenericRecord record) {
            for (Schema.Field field : record.getSchema().getFields()) {
                row.put(field.name(), record.get(field.name()));
            }
        } else if (value instanceof JsonNode json) {
            json.fields().forEachRemaining(field -> row.put(field.getKey(), field.getValue()));
        } else if (value instanceof Message message) {
            message.getAllFields().forEach((field, fieldValue) -> row.put(field.getName(), fieldValue));
        } else {
            row.put("value", value);
        }
        return row;
    }
}
```

What `transform` receives depends on `schema.getSchemaType()`:

| Schema type | `value` |
|---|---|
| `AVRO` | A `GenericRecord`, or a plain Java value for primitive Avro schemas |
| `JSON` | A Jackson `JsonNode` |
| `PROTOBUF` | A protobuf `DynamicMessage` |
| `PRIMITIVE` (the topic has no registered schema, or the message has no schema-registry header) | The raw `byte[]` |

A tombstone, a record with a null value, arrives as `null`. Decide whether your destination deletes
the row, skips it, or fails. The example fails.

Some things to know about decoding:

- **Construct your encoder directly**, as the factory above does.
  [`EntrySerdeFactory`](../../ursa-storage-materialization/src/main/java/io/lakestream/ursa/materialization/serde/EntrySerdeFactory.java)
  registers encoders under a closed `SerdeType` enum, so registering yours there would replace a
  built-in encoder for the whole compactor.
- **Bad records reach `onErrorWithCtx`.** When one record can't be decoded, your callback receives a
  new entry that holds just that record. When the whole entry can't be decoded, it receives the
  original entry, with an extra reference. Either way, your callback owns what it receives and must
  release it. If the callback returns normally, decoding continues, and in the whole-entry case the
  entry is skipped. If the callback throws, as in the example, the whole entry fails.
- **Transactional and control batches aren't supported.** The decoder rejects them, so they reach
  `onErrorWithCtx` as whole-entry failures.
- **Keep your own schema cache.**
  [`SchemaCache.INSTANCE`](../../ursa-storage-materialization/src/main/java/io/lakestream/ursa/materialization/serde/SchemaCache.java)
  is shared by every materializer in the compactor. Create your own `SchemaCache`, or your own map,
  for converted schemas.

## Map and evolve the schema

If your destination has a schema of its own, implement `TableSchemaService<Long, S>`, where `S` is
your destination's schema type:

| Method | Contract |
|---|---|
| `getTableSchema(version)` | The destination schema that corresponds to a source schema version, or `null` if it doesn't exist yet |
| `getLatestSchemaVersion()` | The newest source schema version the table has been evolved to, or `-1` if the table doesn't exist |
| `evolveTableSchema(schemas)` | Creates or alters the table for each version in order, and returns the versions it applied |

Then call `SchemaEvolutionManager.evolveSchema` from `transform`, before you convert the value. It
throws checked exceptions, and it reports schema problems as a `RuntimeExceptionWithCode`. Unwrap
that the way the Iceberg encoder does, or the base class reports every schema problem as
`MESSAGE_DESERIALIZE_FROM_SOURCE_ERROR`:

```java
S tableSchema;
try {
    tableSchema = SchemaEvolutionManager.evolveSchema(
            myTableSchemaService,   // your TableSchemaService<Long, S>
            schemaService,          // the encoder's source schema service
            schemaKey,              // passed to transform
            (SchemaMetadata sourceSchema, EntryEncoderContext ctx) -> convert(sourceSchema),
            context);
} catch (RuntimeExceptionWithCode e) {
    // Keep the real code, for example MESSAGE_SCHEMA_INCOMPATIBLE.
    throw new MessageSerDeException(e.getRealException().getExceptionCode(), e.getRealException());
} catch (Exception e) {
    throw new MessageSerDeException(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR, e);
}
```

`evolveSchema` returns the existing table schema when that version was already applied. Otherwise it
loads every source schema version up to the record's version, converts each one with your function,
and asks your service to apply them.

It fails with `MESSAGE_SCHEMA_INCOMPATIBLE` in three cases:

- the record's schema version hasn't been applied to the table, and the table is already at a newer
  version
- the table doesn't exist yet, and the record's version is below the base schema version passed in
  `EntryEncoderContext`
- your service didn't apply the requested version. That includes a version your conversion function
  failed on: `evolveSchema` logs the failure and leaves that version out.

Two things to design for:

- **Make every DDL statement safe to run twice** (for example, `CREATE TABLE IF NOT EXISTS`). A
  replayed task repeats it.
- **Handle concurrent schema changes.** Partitions of the same stream run as separate tasks at the
  same time, and they can try to create or alter the same table at the same moment.

[`KafkaEntryToIcebergRecordEncoder`](../../ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/v2/serde/iceberg/KafkaEntryToIcebergRecordEncoder.java)
shows the full pattern. The ClickHouse materializer takes a simpler route: it adds columns as new
fields appear in the rows it writes, and it rejects a change to an existing column's type
([`ClickHouseTableSchemaService.ensureColumns`](../../ursa-storage-clickhouse/src/main/java/io/lakestream/ursa/clickhouse/ClickHouseTableSchemaService.java)).

## Commit exactly once

A failed task is retried later, over exactly the same offset range, with a new materializer. The
failure can come after your `commit()` succeeded, for example when Ursa's internal writer fails to
commit. So your destination can see the same records twice unless you prevent it.

The reliable approach is a **commit marker**. Record what you committed, in the same transaction as
the data. Before you write, check whether that range is already committed. The example uses the
partition log (`MaterializationRuntime.SOURCE_TOPIC_PROPERTY`) plus the highest record offset in the
batch. A retry replays the same range, so it finds the same marker.

How the existing materializers handle this:

- **Iceberg and Delta** tag each commit with the stream, the task and the offset range: in the
  Iceberg snapshot summary (`lakestream.tags`) and in Delta file tags. After a crash, the committer
  checks those tags before committing again (see
  [`IcebergCommitter`](../../ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/IcebergCommitter.java)).
- **ClickHouse** inserts rows in batches during `write()`, so rows are visible before `commit()`.
  For duplicates it relies on `ReplacingMergeTree`, which removes rows with the same primary key
  when parts merge. A table with no primary key, in neither `UPSERT` nor `CDC` mode, is a plain
  `MergeTree`, and it keeps the duplicates that a replay writes.

If your destination can't commit data and a marker atomically, document what a replay does, so
users know what to expect.

## Handle failures

Ursa 1.0 leaves failure handling to the materializer. The framework has no retries inside a task, no
dead-letter queue, no schema-change gating, and no materialization metrics: the compactor passes a
no-op `failureMessageHandler()` and `metrics()` in `MaterializationRuntime`. The `errorHandling`,
`maxRetries` and `retryDelayMs` fields in the policy are stored but not yet applied.

When your materializer throws, the compactor looks at the exception's `ExceptionCode` to decide what
happens to the task:

| Code | What the compactor does |
|---|---|
| `SOURCE_READ_ERROR`, `SOURCE_THROTTLED` | Retries the task right away |
| `NO_MORE_RECORDS`, `SOURCE_CLIENT_ERROR` | Retries after `retryableQuarantineInSeconds` (default 30) |
| `NO_SUCH_LOG`, `NO_SUCH_STREAM`, `NO_SUCH_ENTRIES`, `NO_SUCH_OFFSET`, `COMPACTION_NO_WRITE_RESULT` | **Deletes the task. That range is never materialized.** |
| Anything else | Retries after `nonRetryableQuarantineInSeconds` (default 300) |

In practice:

- **Throw `MaterializationException`** with a code that describes the failure, such as
  `MESSAGE_PARSE_FAILED`, `MESSAGE_SCHEMA_INCOMPATIBLE`, `LAKEHOUSE_WRITE_ERROR` or
  `LAKEHOUSE_COMMIT_ERROR`. The `LAKEHOUSE_*` codes are the general sink-side write and commit codes.
- **Wrap every other exception in `write()`.** The framework reads entries and calls `write()` in the
  same loop. Any other exception thrown from `write()` is reported as `SOURCE_READ_ERROR`, so the
  task is retried immediately, over and over. Other exceptions from `create()` or `commit()` are
  reported as `INTERNAL_ERROR`.
- **Never use the `SOURCE_*`, `NO_SUCH_*` or `COMPACTION_NO_WRITE_RESULT` codes** for problems in
  your destination. They are reserved for the source side. The `NO_SUCH_*` codes and
  `COMPACTION_NO_WRITE_RESULT` delete the task and silently skip data.
- **Decide what a record you can't write means.** The built-in materializers differ. ClickHouse
  fails the task, which then retries every five minutes until the destination or the materializer
  changes, so a record that can never be written blocks its partition. Iceberg and Delta write
  records they can't decode or convert to a dead-letter table, and carry on. (Delta does this
  unless `delta.dlt.enabled=false`.) If your destination has no such place and you skip bad records,
  log each one clearly, because nothing else will record it.
- **Never block on `MaterializationRuntime.materializationExecutor()`.** It is the thread pool that
  runs compaction tasks, so waiting on work you submitted to it can deadlock the compactor.

## Register the factory

Add a service file to your jar, so the compactor's `ServiceLoader` can find the factory:

```text
src/main/resources/META-INF/services/io.lakestream.ursa.materialization.TableMaterializerFactory
```

The file contains one line, the factory's class name:

```text
com.example.ursa.mysink.MySinkMaterializerFactory
```

Two cases to watch for:

- If a factory can't be instantiated, the compactor logs a warning and skips it. A policy that points
  at its catalog type later fails with `No TableMaterializerFactory registered for catalog type`.
- If two factories declare the same catalog type, the first one on the classpath wins and the other
  is ignored, with a warning.

## Configure a catalog and a policy

Materialization targets a `TableCatalog`: a name, a type, and the connection settings your factory
reads. Register one through the `StreamCatalog` API:

```java
streamCatalog.registerTableCatalog(new TableCatalog(
        "mysink-prod",
        TableCatalogType.MYSINK,
        Map.of("uri", "mysink://host:1234", "user", "ursa"),
        Map.of())).join();
```

Then attach a materialization policy to a namespace or a stream, as described in
[Materialize a stream to a table](../user/table-materialization.md). Your factory receives the
resolved policy.

When it resolves the policy, the framework applies `catalogRef` and `tableNaming`, and a
stream-level `enabled = false`. The resolved policy always has a `tableIdentifier`, and its
`connectionOverrides` come from the stream-level policy only. Honoring the other fields is up to
your materializer:

- `primaryKey`
- `framework.writeMode`
- `framework.commit.batchSize`
- `table` (partitioning, sort order, compression)
- `connectionOverrides`
- `baseSchemaVersion`: pass it to `SchemaEvolutionManager` through
  `EntryEncoderContext.baseSchemaVersion`

Document which ones you support. Ursa 1.0 doesn't apply `evolution`, `framework.startPosition`,
`framework.paused`, `framework.errorHandling` or the commit retry settings.

Operators can also declare catalogs in the compactor configuration, with keys such as
`clickhouse.catalog.<name>.dsn`. That needs a prefix for your type in
[`TableCatalogBootstrap`](../../ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/v2/TableCatalogBootstrap.java),
which is part of the registration pull request if you want it.

## Test it

Use [`ursa-storage-clickhouse`'s tests](../../ursa-storage-clickhouse/src/test/java/io/lakestream/ursa/clickhouse)
as a starting point:

- **Unit tests** build entries from real Kafka `MemoryRecords`. Copy the small fixture in
  [`MemoryRecordsEntries`](../../ursa-storage-clickhouse/src/test/java/io/lakestream/ursa/clickhouse/MemoryRecordsEntries.java).
  After each `write`, assert that the payload's `refCnt()` is 0, on both the success and the failure
  paths.
- **A discovery test** loads your factory through `ServiceLoader`, to catch a missing or misspelled
  service file.
- **Integration tests** run against the real destination in a container, using Testcontainers. Tag
  them (for example, `@Tag("mysink")`) and exclude the tag from the default test run, so that unit
  tests stay fast.

Before you publish, make sure your tests cover these cases:

- [ ] Every path through `write` releases the entry exactly once. So does a failure in the middle of
      a batch.
- [ ] Replaying the same range after a successful commit writes nothing new.
- [ ] Calling `commit()` twice is harmless, and so is calling `close()` after a failure.
- [ ] A new column in the source schema reaches the destination, or fails with a clear error.
- [ ] A record that can't be decoded or written fails the task with a sink-side code (not a `NO_SUCH_*`
      code).
- [ ] Two partitions of the same stream can create and evolve the table at the same time.

## Package and run it

1. **Build a jar** that contains your materializer, its service file, and any dependencies the
   compactor doesn't already have. Leave the `provided` Ursa artifacts out.
2. **Put it on the compactor's classpath.** `bin/compact` loads the compactor's own jar, every jar in
   `$COMPACT_HOME/lib/`, and the paths in the `COMPACT_EXTRA_CLASSPATH` environment variable, and
   nothing else.
3. **Watch for classpath conflicts.** The compactor loads every materializer on one flat classpath,
   next to Iceberg, Delta, Parquet, Avro, Jackson and Netty. If your client library needs different
   versions of any of these, shade and relocate them in your jar.
4. **Turn materialization on** by setting `materializationEnabled=true` in the compactor
   configuration.
5. **Register a catalog and attach a policy**, as described above.

## Register the type in Ursa

Until the SPI is opened up, each new materializer needs one small pull request in this repository.
Open it after your LIP is accepted, and link to the LIP. Your materializer works only with Ursa
releases that include this change.

| File | Change |
|---|---|
| [`TableCatalogType`](../../lakestream-api/src/main/java/io/lakestream/api/materialization/TableCatalogType.java) | Add your constant, with JavaDoc |
| [`EnumsTest`](../../lakestream-api/src/test/java/io/lakestream/api/materialization/EnumsTest.java) | Update the expected number of values |
| [`LakehouseMaterializationService`](../../ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/compact/LakehouseMaterializationService.java) | Add your constant to the two `switch` expressions over the catalog type. In `withResolvedMaterialization`, map it to a `LakehouseType`: materializers that commit on their own use `NONE`, as ClickHouse does. In `evolutionPolicyFor`, choose its `EvolutionPolicy`. Without these, the module doesn't compile. |
| [`TableCatalogBootstrap`](../../ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/v2/TableCatalogBootstrap.java) | Optional: a `<type>.catalog.<name>.*` configuration prefix |
| [`table-materialization.md`](../user/table-materialization.md) | Add your materializer to the list of materializers, with a link to its repository |

One compatibility consequence: once a catalog with your type is stored, an Ursa build that doesn't
know the type can't read that catalog back, and listing table catalogs fails for every catalog, not
just yours. The LIP's rollback section should say so.

## The ClickHouse materializer as a reference

[`ursa-storage-clickhouse`](../../ursa-storage-clickhouse) is the closest template for a new
materializer. It depends only on the SPI, it commits on its own, and it has both unit and
Testcontainers tests.

| Class | Role |
|---|---|
| `ClickHouseTableMaterializerFactory` | Checks the catalog type, resolves the table, opens a connection, and wires the encoder |
| `ClickHouseTableMaterializer` | Buffers rows, inserts in batches, and treats `commit()` as final |
| `KafkaEntryToClickHouseRowEncoder` | A `KafkaEntryEncoder` subclass that turns Avro, JSON and Protobuf values into rows |
| `ClickHouseTableSchemaService` | Creates the table and adds columns as new fields appear |
| `ClickHouseConnectionFactory` | Merges catalog settings with policy overrides, and passes on only the keys the driver accepts |
| `ClickHouseTableEngine` | Maps the policy's write mode and primary key to a table engine |

Whatever you copy, keep one rule from [The contract](#the-contract): the compactor doesn't call
`close()` after a successful task, so release your resources in `commit()`.

The Iceberg and Delta materializers are a less useful template. Their commits go through a group
commit step that is built into the compactor for those two formats. A new file-based format has
to commit on its own, the way ClickHouse does.

## Getting help

Ask in [Discussions: Q&A](https://github.com/lakestream-io/ursa/discussions/categories/q-a), or on
your proposal issue. When your materializer is ready, share it in
[Discussions: Show and tell](https://github.com/lakestream-io/ursa/discussions/categories/show-and-tell).
