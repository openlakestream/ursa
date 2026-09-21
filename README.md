# Ursa

**Stream as a storage primitive for the lakehouse paradigm**

Ursa is a storage engine that implements [Lakestream](https://openlakestream.org), an open API and specification for stream storage on object storage, with a stream materialization framework that defines how a stream becomes a lakehouse table. Ursa turns any object, block, or file store into stream storage, and it materializes streams into lakehouse tables and other queryable states.

Ursa 1.0 is published to Maven Central under the `org.openlakestream` group; the [quickstart](https://openlakestream.org/docs/ursa/quickstart) embeds it in a Java program. It is built to be embedded in messaging brokers: [Ursa for Apache Kafka (UFK)](https://github.com/lakestream-io/kafka) is a Kafka distribution built on the Lakestream API and specification that stores its diskless topics through Ursa.

## Principles

The storage is designed with the following principles.

### Stream Storage and Table Materialization

Ursa retains stream records in WAL and Compacted Objects with an Offset Index for streaming reads
and replay. Optional table materialization delivers records into external analytical tables whose
files and lifecycle are independent of internal stream storage.

### Diskless & Leaderless Architecture

By turning any object, block, or file store into shared log or stream storage, Ursa enables diskless event-streaming integrations without coupling the storage engine to a broker protocol. This provides:

- No single point of failure or bottleneck for partitions
- Reduced cross-AZ network traffic
- Lower operational complexity (no leader elections or rebalancing)
- Rebalance-free architecture for event brokers

### Integrated Table Delivery

Background materialization delivers stream records to external tables using the same WAL read pass
as internal compaction. Applications can consume retained records through the stream API and query
the delivered copy through the destination table's APIs.

## Key Features

- **Stream-First Storage Primitive**: Unified abstraction for data streams and lakehouse tables
- **Multi-Cloud Support**: AWS S3, Google Cloud Storage, Azure Blob Storage
- **Lakehouse Integration**: Delta Lake and Apache Iceberg with multi-catalog support
- **High Performance**: Read/write caching, batching, and prefetching optimizations
- **Observability**: Metrics for the storage and compaction paths (see [Metrics](docs/Metrics.md))

## Concepts & Architecture

- [Storage Concepts](docs/concepts.md)
- [Lakehouse Tables](docs/lakehouse-tables.md)
- [Feature Matrix](docs/feature-matrix.md)

## Modules

| Module | Description |
|--------|-------------|
| `lakestream-api` | Protocol-neutral stream, log, and catalog API |
| [ursa-storage-core](ursa-storage-core/README.md) | Core storage engine with multi storage backend support |
| [ursa-storage-common](ursa-storage-common/README.md) | Shared utilities, exceptions, and common interfaces |
| `ursa-storage-lakestream` | Lakestream API implementation and catalog |
| `ursa-storage-materialization` | Stream-to-table materialization SPI and Kafka codecs |
| [ursa-storage-lakehouse](ursa-storage-lakehouse/README.md) | Lakehouse integration with catalog support |
| [ursa-storage-lakehouse-kafka-reader](ursa-storage-lakehouse-kafka-reader/README.md) | Isolated Kafka compacted-data reader |
| [ursa-storage-kafka-runtime](ursa-storage-kafka-runtime/README.md) | Leaf runtime that wires Ursa and Kafka compacted reads behind the Lakestream API |
| `ursa-storage-clickhouse` | ClickHouse materialization sink |
| [ursa-storage-compact](ursa-storage-compact/README.md) | Distributed compaction service |
| [ursa-storage-containers](ursa-storage-containers/README.md) | Test infrastructure and testcontainers |
| [ursa-storage-test](ursa-storage-test/README.md) | Integration and end-to-end tests |
| [ursa-storage-tools](ursa-storage-tools/README.md) | Performance testing and benchmarking tools |

## Build & Run

See [Build & Run Locally](docs/developer/build.md) for complete build and local run instructions.

## Development

See [Contributing Guide](CONTRIBUTING.md) for information on how to contribute to the project.

## Support

- **Issues**: Report bugs and feature requests on [GitHub Issues](https://github.com/lakestream-io/ursa/issues)
- **Documentation**: See [docs/](docs/) for detailed guides
- **Questions**: Ask in [GitHub Discussions](https://github.com/lakestream-io/ursa/discussions)
- **Specification**: The Lakestream API and specification are documented at [openlakestream.org](https://openlakestream.org)

## License

Ursa is licensed under the [Apache License, Version 2.0](LICENSE).

The binary distribution contains only Apache 2.0 and other permissively licensed third-party jars.
JSON Schema and Protobuf records are decoded without the Confluent Community License provider
artifacts; see [Third-party license notes](docs/developer/third-party-licenses.md) for details and
the build guard rails.
