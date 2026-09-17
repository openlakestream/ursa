# Stream Storage and External Tables

Ursa keeps stream storage separate from table materialization.

## Internal stream storage

WAL Objects provide durable storage for new entries. Background compaction creates per-log
Compacted Objects (COs), including Parquet files. Ursa retains their Stream Offset Index so
consumers can resume or replay retained messages by offset. Ursa owns CO retention and cleanup.
Internal COs are storage files, not catalog tables. Their generation is enabled by default and
independent of SDT. Keep `compactedObjectEnabled=true` for UFK stream replay; external Kafka/Pulsar
table materialization can set it to `false` when internal COs are not needed.

## External tables (SDT)

A Stream-Delivered-to Table is a destination outside Ursa's internal stream storage. Materialization
writes and commits records into the destination; internal CO files are never registered in it.
Supported sinks include Iceberg, Delta Lake, Delta on Unity Catalog, and ClickHouse.

- Query the destination using its table APIs.
- Use append or upsert where supported, and configure destination partitioning independently.
- Let the destination system manage table retention, deletion, and maintenance.
- Read or replay the stream through Ursa's internal storage, not through the destination table.

Stream storage and SDT output are separate copies. Deleting retained internal COs does not delete
SDT data, and changing a destination table does not change the stream's Offset Index.

See [Table materialization](user/table-materialization.md) for configuration and
[Storage concepts](concepts.md) for the WAL, CO, and Offset Index model.
