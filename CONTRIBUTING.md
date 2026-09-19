# Contributing to Ursa

Thanks for your interest in Ursa. Ursa implements the [Lakestream](https://openlakestream.org) API.
It is stream storage built on object storage, with a stream materialization framework that turns
streams into tables. Bug reports, fixes, documentation, tests, new materializers and feedback on the
API are all welcome.

> **Using an AI assistant?** Read the [AI policy](AI_POLICY.md) first.
> **Are you a coding agent?** Start with [AGENTS.md](AGENTS.md).

## Ways to contribute

- **Report a bug or request a feature.** Open an
  [issue](https://github.com/lakestream-io/ursa/issues/new/choose).
- **Ask a question or share an idea.** Start a
  [discussion](https://github.com/lakestream-io/ursa/discussions).
- **Fix something.** Issues labeled
  [`good first issue`](https://github.com/lakestream-io/ursa/labels/good%20first%20issue) and
  [`help wanted`](https://github.com/lakestream-io/ursa/labels/help%20wanted) are good places to
  start. Comment on the issue to say you're working on it, so nobody duplicates your work.
- **Improve the docs.** If something confused you, it will confuse the next person too.
- **Write a materializer.** Materialize streams into a table format or store that Ursa doesn't
  support yet. See [Write a materializer](docs/developer/materializer-guide.md).
- **Shape the API.** The Lakestream API is still evolving. Feedback from people building on it is
  the most useful kind.

## Where to talk

| For | Use |
|---|---|
| Bugs and concrete feature requests | [Issues](https://github.com/lakestream-io/ursa/issues) |
| Questions | [Discussions: Q&A](https://github.com/lakestream-io/ursa/discussions/categories/q-a) |
| Design ideas, and proposals before they become a LIP | [Discussions: Ideas](https://github.com/lakestream-io/ursa/discussions/categories/ideas) |
| Something you built with Ursa | [Discussions: Show and tell](https://github.com/lakestream-io/ursa/discussions/categories/show-and-tell) |
| Security vulnerabilities | Report privately, as described in [SECURITY.md](SECURITY.md) |
| Conduct concerns | See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) |

The project doesn't run a Slack workspace or a mailing list, so decisions happen where everyone can
read them.

## Before you write code

- **Small, self-contained changes** can go straight to a pull request. Examples: a bug fix with a
  test, a documentation correction, a typo.
- **For anything larger**, open an issue or a discussion first. That includes a new feature, a
  refactor across modules, or a change in behavior. Agreeing on the approach first saves you from
  writing code that has to be redone.
- **Some changes need a LIP** (Lakestream Improvement Proposal):
  - new or changed public types in `lakestream-api`
  - changes to an SPI contract, that is, an interface that plug-ins implement, such as
    `TableMaterializer`
  - changes to the formats Ursa writes to object storage (the write-ahead log, or WAL, and the
    compacted objects it's rewritten into), or to serialized field numbers and identifiers
  - every new materializer

  See [docs/lip](docs/lip/README.md) for the process and the template.

## Build and test

You need Java 17 or later, Maven 3.6.3 or later, and Docker. Many tests, tagged or not, start the
services they need, such as Oxia or an object-storage emulator, in containers through Testcontainers.

```bash
git clone https://github.com/lakestream-io/ursa.git
cd ursa
mvn -B -ntp clean install -DskipTests    # build and install every module
mvn -B -ntp test -pl ursa-storage-core   # test one module
```

On Windows, the `CLAUDE.md` files in this repository are symbolic links. Clone with
`git clone -c core.symlinks=true https://github.com/lakestream-io/ursa.git`. Creating symlinks also
requires Developer Mode or administrator rights.

Some groups of tests are tagged so that CI can run them in separate jobs. CI runs everything else
with:

```bash
mvn -B -ntp test -DexcludeGroups='docker,clickhouse,lakehouse,test-base'
```

It runs the tagged groups in separate jobs:

| Tag | Covers | Run with |
|---|---|---|
| `lakehouse` | Iceberg and Delta materialization | `mvn -B -ntp test -pl ursa-storage-lakehouse -Dgroups=lakehouse -DexcludeGroups=docker` |
| `clickhouse` | ClickHouse, against a container | `mvn -B -ntp test -pl ursa-storage-clickhouse -Dgroups=clickhouse -DexcludeGroups=` |
| `docker` | Object-storage backends, against containers | `mvn -B -ntp test -pl ursa-storage-test -Dgroups=docker` |

A plain `mvn test` runs the `docker` and `lakehouse` groups too.

Before you open a pull request, run the same checks CI runs:

```bash
mvn -B -ntp license:check       # license headers; fix with: mvn -B -ntp license:format
mvn -B -ntp checkstyle:check    # code style
mvn -B -ntp clean install -DskipTests
mvn -B -ntp spotbugs:check      # static analysis
```

For more:

- [docs/developer/build.md](docs/developer/build.md) covers building and running Ursa locally.
- [docs/agent/error-remediation.md](docs/agent/error-remediation.md) lists common build failures and
  how to fix them. It is written for coding agents, but it works just as well for people.

## How the code is organized

```text
ursa/
├── lakestream-api/                         # Public, protocol-neutral API
├── ursa-storage-common/                    # Shared utilities
├── ursa-storage-core/                      # WAL and object-storage engine
├── ursa-storage-lakestream/                # Catalog, log, cursor, and stream implementation
├── ursa-storage-materialization/           # Materialization SPI and Kafka codecs
├── ursa-storage-lakehouse/                 # Iceberg and Delta integration
├── ursa-storage-lakehouse-kafka-reader/    # Isolated Kafka compacted reader
├── ursa-storage-kafka-runtime/             # Leaf Lakestream runtime for Kafka ingestion and reads
├── ursa-storage-clickhouse/                # ClickHouse materializer
├── ursa-storage-compact/                   # Compaction orchestrator
├── ursa-storage-containers/                # Test infrastructure
├── ursa-storage-test/                      # End-to-end tests
└── ursa-storage-tools/                     # Performance and diagnostic tools
```

A few rules keep the layers clean:

- The public API and the core storage path stay independent of broker protocols. Record-format
  adapters belong in an integration module. Table-format behavior belongs in its sink module.
- The Kafka runtime is a leaf. Core and the Lakestream APIs never depend on it or on the Kafka reader
  module.
- New integrations build on `lakestream-api`. `StorageApi` is an internal engine contract.

To go deeper:

- [docs/agent/module-map.md](docs/agent/module-map.md) shows which module owns what and how
  dependencies flow.
- [docs/concepts.md](docs/concepts.md) explains the storage model.

## Code style

Checkstyle enforces most of these rules. The rest come up in review.

- Every source file starts with the repository license header. `mvn -B -ntp license:format` adds it.
- Every main Java package has a `package-info.java`.
- No wildcard imports. Static imports come first, and both groups are alphabetized.
- Lines are at most 120 characters.
- Log through SLF4J, never to standard output.
- Public APIs have JavaDoc.
- Keep buffer ownership explicit. Say whether a method takes, retains or copies a reference-counted
  buffer, and release what you own.
- Preserve serialized field numbers and identifiers when you evolve a schema.
- Dependency versions come from the BOMs managed in the root `pom.xml`.
- Fix real SpotBugs findings instead of adding exclusions.

## Tests

- Add a test for the behavior you change. Test class names end in `Test`, not `Tests`.
- Test classes run in parallel, so they must not share mutable state. Use unique stream and topic
  names.
- Tests that need a service, such as Oxia or object storage, start it with Testcontainers.
- Tests that read entries must release them. The test JVMs run Netty leak detection.

## Commits

### Sign your commits (DCO)

Every commit needs a Developer Certificate of Origin sign-off:

```bash
git commit -s -m "Fence stream lifecycle operations"
```

The `-s` flag adds a line such as `Signed-off-by: Your Name <you@example.com>`. The line certifies
that you wrote the change, or otherwise have the right to submit it under the project's license. The
full text is at [developercertificate.org](https://developercertificate.org/).

A DCO check runs on every pull request. If you forgot to sign off, fix the last commit with
`git commit --amend -s --no-edit`, or a series with `git rebase --signoff origin/main`, and then
force-push your branch.

We don't use a CLA. The DCO sign-off is all we ask.

### Write useful messages

Start with a short summary in the imperative mood, such as "Fence stream lifecycle operations". Then
explain why the change is needed, if that isn't obvious. Pull requests are squash-merged, so the pull
request title becomes the commit subject on `main`.

### Say when AI helped

If an AI tool helped meaningfully, add an `Assisted-by:` trailer. `Co-authored-by:` is also
accepted. The [AI policy](AI_POLICY.md) explains what counts.

## Pull requests

1. Fork the repository on GitHub, and add your fork as a remote:
   `git remote add fork https://github.com/<your-username>/ursa.git`. Create a branch for your
   change, and push it to `fork`.
2. Keep each pull request to one logical change. Smaller pull requests get reviewed sooner.
3. Fill in the pull request template: what changed and why, compatibility, how you tested it, and AI
   assistance.
4. Update the documentation in the same pull request when you change behavior or a contract.
5. Make sure CI passes.
6. A code owner for the files you touched reviews and approves the change. Code owners are listed in
   [CODEOWNERS](.github/CODEOWNERS). These docs call them maintainers.

We aim to respond promptly. If your pull request has been quiet for a while, @-mention one of the code
owners for the files you changed.

## License

Ursa is licensed under the [Apache License 2.0](LICENSE), and so is your contribution. Some
third-party artifacts may only be used in tests. See
[docs/developer/third-party-licenses.md](docs/developer/third-party-licenses.md) for which ones. The
build enforces this.
