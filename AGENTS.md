# Ursa: instructions for coding agents

This file is for AI coding agents such as Claude Code, Codex, Copilot and Cursor. People should start
with [CONTRIBUTING.md](CONTRIBUTING.md).

Some modules have their own `AGENTS.md` with more detail. Read it before you change that module.
Each `CLAUDE.md` in this repository is a symlink to the `AGENTS.md` next to it. **Always edit
`AGENTS.md`, never `CLAUDE.md`.** A write that replaces the file would turn the symlink into a copy.

## Rules that come first

These rules implement the project's [AI policy](AI_POLICY.md) and override anything else in this
file.

- **Never add a `Signed-off-by:` line, even if asked.** Only the human can certify the Developer
  Certificate of Origin. When a commit is ready, tell them to review it and run
  `git commit --amend -s --no-edit`, or `git rebase --signoff origin/main` for several commits.
- **End each commit message with exactly one AI trailer: `Assisted-by: <tool>`.** This repository
  configures Claude Code to use `Assisted-by: Claude Code` (see `.claude/settings.json`). If your tool
  adds its own `Co-authored-by:` trailer, that counts; don't add both.
- **Don't act outside the local checkout without explicit approval for that specific action.** That
  covers pushing, opening or editing a pull request or issue, and commenting on GitHub. Being asked
  to start a task isn't permission to publish it. When a maintainer asks you through an `@claude`
  mention on GitHub, that request approves your reply, and the commits it asks for, in that run.
- **Leave force-pushes to the human.** If a push needs `--force`, for example after the commits
  were re-signed, hand it over.
- **Draft pull request descriptions from
  [the template](.github/pull_request_template.md)**, including the *AI assistance* section. Give the
  draft to the human to edit and post.
- **Report what you verified and what you didn't.** Never say a test or check passed unless you ran
  it and saw it pass.

## Project

Ursa is lakehouse-native stream storage built on object storage, and it implements the Lakestream
API. The project targets Java 17 and is built as a Maven multi-module reactor.

The core API and implementation are protocol neutral. Broker integrations translate their native
record representation at the repository boundary. They must not leak protocol types into
`lakestream-api`, `ursa-storage-common`, `ursa-storage-core` or `ursa-storage-lakestream`.

| Layer | Module | Responsibility |
|-------|--------|----------------|
| Public API | `lakestream-api` | Stream metadata, log, cursor, catalog, and materialization contracts |
| Shared code | `ursa-storage-common` | Utilities, configuration helpers, and common exceptions |
| Storage engine | `ursa-storage-core` | WAL and object-storage implementation; internal `StorageApi` |
| API implementation | `ursa-storage-lakestream` | Catalogs, layouts, logs, cursors, readers, and writers |
| Materialization SPI | `ursa-storage-materialization` | Sink SPI, schema handling, and Kafka record decoding |
| Lakehouse sink | `ursa-storage-lakehouse` | Iceberg and Delta materialization |
| Kafka compacted reader | `ursa-storage-lakehouse-kafka-reader` | Isolated Kafka-format compacted-object reader |
| Kafka runtime | `ursa-storage-kafka-runtime` | Leaf runtime for Kafka ingestion and reads |
| ClickHouse sink | `ursa-storage-clickhouse` | ClickHouse materialization implementation |
| Orchestrator | `ursa-storage-compact` | WAL-to-compacted-object scheduling and sink dispatch |
| Test support | `ursa-storage-containers`, `ursa-storage-test` | Containers and end-to-end coverage |
| Tools | `ursa-storage-tools` | Benchmarks and operational utilities |

New integrations should use `lakestream-api`. `StorageApi` remains an internal engine contract.
Kafka-specific codecs and readers are intentionally isolated in integration modules, not in the
protocol-neutral layers.

### Core domain model

```text
StreamCatalog
  -> StreamMetadata
      -> StreamLayout
          -> LogId
  -> openLog -> Log
      -> LogCursor
      -> LogStorage
  -> openReader / openWriter

StreamWriter / StreamReader route stream operations through the selected layout.
UnifiedStreamReader routes reads between raw WAL data and compacted objects.
```

Payload ownership is explicit. Callers must release or close reference-counted entry buffers at the
ownership boundary documented by the API.

### Materialization

`ursa-storage-materialization` defines the `TableMaterializer` SPI. Implementations are loaded with
`ServiceLoader`, which keeps the lakehouse and ClickHouse sinks separate. Kafka entries are decoded
directly from native Kafka `MemoryRecords`, before schema evolution and table encoding.
Protocol-neutral modules must not depend on a broker client or a broker metadata model.

To write a new materializer, follow
[docs/developer/materializer-guide.md](docs/developer/materializer-guide.md). New materializers live
in their own repositories and need a LIP.

The compaction orchestrator reads these implementation-class properties:

| Property | Purpose |
|----------|---------|
| `materializationServiceClass` | Selects stream-to-table dispatch |
| `compactionStorageBindingsClass` | Selects publish, commit, and cleanup bindings |
| `compactionServiceClass` | Deprecated compatibility alias |

## Commands

Build with JDK 17. JDK 25 can't compile the project yet (the build's Lombok version doesn't support
it), so if `mvn -version` shows another JDK, set `JAVA_HOME` to a JDK 17.

```bash
# Build everything, skipping tests
mvn -B -ntp clean install -DskipTests

# Test one module (prefer the narrowest module that covers your change)
mvn -B -ntp test -pl ursa-storage-core

# Tagged groups
mvn -B -ntp test -pl ursa-storage-lakehouse -Dgroups=lakehouse -DexcludeGroups=docker
mvn -B -ntp test -pl ursa-storage-clickhouse -Dgroups=clickhouse -DexcludeGroups=
mvn -B -ntp test -pl ursa-storage-test -Dgroups=docker

# Quality gates (CI runs all four)
mvn -B -ntp license:check
mvn -B -ntp checkstyle:check
mvn -B -ntp clean install -DskipTests
mvn -B -ntp spotbugs:check

# Add missing license headers
mvn -B -ntp license:format
```

Claude Code has the same steps as slash commands in `.claude/commands`: `/build-and-check`,
`/fix-style`, `/review-changes` and `/test-module`.

Many tests, tagged or not, start their services with Testcontainers. This repository doesn't maintain
a Docker Compose stack. Start Docker and check the daemon with `docker info` before you run tests.

## Code conventions

- Every source file needs the repository license header.
- Every main Java package needs a `package-info.java`.
- Don't use wildcard imports.
- Static imports come before regular imports, and both groups are alphabetized.
- Use SLF4J rather than standard-output logging.
- Test class names end in `Test`, not `Tests`.
- Keep lines at or below 120 characters.
- Manage dependency versions through the repository BOMs.
- Fix real SpotBugs findings rather than adding broad exclusions.
- Confluent Community License artifacts (`kafka-json-schema-*`, `kafka-protobuf-*`) are allowed in
  test scope only, and the enforcer rule fails the build otherwise. JSON Schema and Protobuf decoding
  uses the `serde.kafka.schema` package in `ursa-storage-materialization` instead.
- In `ursa-storage-lakehouse`, new code goes in the `v2` packages.

## Boundaries

**Always**

- Run the narrowest affected module's tests, plus `license:check` and `checkstyle:check`, before you
  say a change is done. Before you draft a pull request, run all four quality gates, including
  `spotbugs:check`.
- Honor buffer ownership: release what you own, on every path.
- Follow existing patterns in the module you're changing.

**Ask first**

- Adding or upgrading a dependency.
- Changing public types in `lakestream-api`, an SPI contract, or the on-object or WAL format. These
  also need a LIP ([openlakestream/lips](https://github.com/openlakestream/lips)).
- Adding or renaming configuration keys.
- Creating a module.
- Changing anything under `.github/workflows`.
- Deleting, disabling or loosening a test.

**Never**

- Edit `LICENSE` or `NOTICE`, or change the text of license headers.
- Hand-edit generated code, or change vendored code (`io.delta.kernel`, `org.apache.iceberg.avro`)
  unless you were asked to.
- Add SpotBugs exclusions, checkstyle suppressions or `@SuppressWarnings` just to get a green build.
- Add Confluent Community License artifacts outside test scope.
- Put broker or protocol types in the protocol-neutral modules.
- Force-push, rewrite published history, or commit secrets.
- Claim performance, cost or compatibility results in code or docs unless a test or benchmark in this
  repository backs them.

## Where to read next

- [Write a materializer](docs/developer/materializer-guide.md)
- [Module map](docs/agent/module-map.md): which module owns what, and how dependencies flow
- [Common pitfalls](docs/agent/common-pitfalls.md)
- [Error remediation](docs/agent/error-remediation.md): fixes for common build failures
- [Contributing](CONTRIBUTING.md) and the [AI policy](AI_POLICY.md)
- [LIP process](https://github.com/openlakestream/lips/blob/main/CONTRIBUTING.md): LIPs for every
  Lakestream component live in openlakestream/lips
- [Build locally](docs/developer/build.md)
- [Third-party license notes](docs/developer/third-party-licenses.md)
- [Concepts](docs/concepts.md)
- [Table materialization](docs/user/table-materialization.md)
- [Materialization design (LIP-161)](https://github.com/openlakestream/lips/blob/main/proposals/LIP-161-Table-Materialization-Framework.md)
