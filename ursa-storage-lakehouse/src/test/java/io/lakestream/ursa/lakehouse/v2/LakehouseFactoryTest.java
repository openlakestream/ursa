/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.materialization.serde.SchemaService;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("lakehouse")
class LakehouseFactoryTest {

    @TempDir
    Path storage;

    @Test
    void compactedObjectWriterCanBeDisabledAndOverriddenPerTask() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storagePath", storage.toUri().toString());
        properties.setProperty("compactedObjectEnabled", "false");
        // Disabling CO must return before attempting to construct a writer.
        properties.setProperty("entrySerDeType", "invalid");
        try (var factory = new LakehouseFactory(new LakehouseConfiguration(properties), mock(SchemaService.class))) {
            assertThat(factory.getCompactedObjectWriter("default/orders-partition-0", Map.of())).isEmpty();
            var writer = factory.getCompactedObjectWriter("default/orders-partition-0", Map.of(
                    "compactedObjectEnabled", "true", "entrySerDeType", "KAFKA_BATCHED_RAW_PARQUET"))
                    .orElseThrow();
            writer.close();
        }
    }

    @Test
    void taskCanDisableCompactedObjectWriterWithDefaultConfiguration() throws Exception {
        try (var factory = new LakehouseFactory(new LakehouseConfiguration(), mock(SchemaService.class))) {
            assertThat(factory.getCompactedObjectWriter("default/orders-partition-0",
                    Map.of("compactedObjectEnabled", "false"))).isEmpty();
        }
    }

    @Test
    void internalWriterExistsWithoutExternalTableAndDoesNotOpenCatalog() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storagePath", storage.toUri().toString());
        properties.setProperty("sdt.enabled", "false");
        // A configured lakehouse type must not make the internal writer create a catalog table.
        properties.setProperty("lakehouseType", "ICEBERG");
        try (var factory = new LakehouseFactory(new LakehouseConfiguration(properties), mock(SchemaService.class))) {
            var writer = factory.getCompactedObjectWriter("default/orders-id-partition-0", Map.of()).orElseThrow();
            try {
                assertThat(writer).isExactlyInstanceOf(LakehouseWriter.class);
                assertThat(factory.getExternalWriter("default/orders-id-partition-0", Map.of())).isEmpty();
            } finally {
                writer.close();
            }
        }
    }
}
