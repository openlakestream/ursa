# Build and run locally

## Prerequisites

- Java 17+
- Maven 3.6.3+
- Docker for integration tests

All dependencies come from public Maven repositories, so no credentials are needed.

## Build

```bash
mvn -B -ntp clean install -DskipTests
```

## Quality gates

```bash
mvn -B -ntp license:check
mvn -B -ntp checkstyle:check
mvn -B -ntp spotbugs:check
```

## Local dependencies

This repository does not maintain a Docker Compose stack. Integration tests use Testcontainers to
provision Oxia, object storage, and other required services. Start your Docker daemon and verify it:

```bash
docker info
```

Protocol-facing services are not required to run core unit tests.

## Tests

```bash
# Every test in every module (needs Docker)
mvn -B -ntp test

# One module
mvn -B -ntp test -pl ursa-storage-core

# Full reactor verification
mvn -B -ntp verify
```

Integration tests use Testcontainers and require a running Docker daemon.
