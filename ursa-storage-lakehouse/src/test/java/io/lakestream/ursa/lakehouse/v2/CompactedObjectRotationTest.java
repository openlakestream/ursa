/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetWriteResult;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.LakehouseEntryMetadata;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.Unpooled;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("lakehouse")
class CompactedObjectRotationTest {
    @TempDir
    Path storage;

    @Test
    void schemaChangeWithinOneBatchCountsMessagesInTheirActualFiles() throws Exception {
        LakehouseWriter writer = writer("KAFKA_PARQUET");
        AtomicBoolean batchCounted = new AtomicBoolean();
        try {
            write(writer, "First", 0, 2, batchCounted);
            write(writer, "Second", 1, 2, batchCounted);
            var results = writer.close();
            assertThat(results).hasSize(2);
            for (var result : results) {
                var file = (ParquetWriteResult) result;
                assertThat(file.getNumberOfRecords()).isEqualTo(1);
                assertThat(((AtomicLong) file.getExtraMetadata()
                        .get(AbstractLakehouseWriter.BATCH_MESSAGE_COUNT)).get()).isEqualTo(1);
            }
            assertThat(((ParquetWriteResult) results.get(0)).getExtraMetadata())
                    .containsEntry(AbstractLakehouseWriter.LAST_ENTRY_ID_IN_FILE, 0L);
            assertThat(((ParquetWriteResult) results.get(1)).getExtraMetadata())
                    .containsEntry(AbstractLakehouseWriter.LAST_ENTRY_ID_IN_FILE, 1L);
        } finally {
            writer.close();
        }
    }

    @Test
    void rawBatchRowsKeepTheFullMessageCount() throws Exception {
        LakehouseWriter writer = writer("KAFKA_BATCHED_RAW_PARQUET");
        try {
            write(writer, "Raw", 0, 3, new AtomicBoolean());
            write(writer, "Raw", 3, 4, new AtomicBoolean());
            var results = writer.close();
            assertThat(results).hasSize(1);
            var file = (ParquetWriteResult) results.get(0);
            assertThat(file.getNumberOfRecords()).isEqualTo(2);
            assertThat(((AtomicLong) file.getExtraMetadata()
                    .get(AbstractLakehouseWriter.BATCH_MESSAGE_COUNT)).get()).isEqualTo(7);
        } finally {
            writer.close();
        }
    }

    private LakehouseWriter writer(String serde) {
        Properties props = new Properties();
        props.setProperty("storagePath", storage.toUri().toString());
        props.setProperty("entrySerDeType", serde);
        return new LakehouseWriter("default/events", mock(EntrySerdeFactory.class),
                new LakehouseConfiguration(props));
    }

    private static void write(LakehouseWriter writer, String schemaName, long offset, int batchSize,
                              AtomicBoolean batchCounted) throws Exception {
        var schema = SchemaBuilder.record(schemaName).fields().requiredString("value").endRecord();
        var record = new GenericData.Record(schema);
        record.put("value", "test");
        var header = new EntryHeader(offset, batchSize, 0L, 1, 1L);
        var payload = Unpooled.buffer(1).writeByte(0);
        var metadata = new LakehouseEntryMetadata(header, null);
        metadata.setLakehouseEntryOffset(offset, 0);
        try {
            writer.doWrite(batchCounted, new GenericEntry(new Entry(header, payload)),
                    new MaterializationRecord<Object>(record, metadata), System.nanoTime());
        } finally {
            payload.release();
        }
    }
}
