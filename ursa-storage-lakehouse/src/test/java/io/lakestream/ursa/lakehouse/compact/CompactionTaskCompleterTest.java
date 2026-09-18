/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static io.lakestream.ursa.lakehouse.AbstractLakehouseWriter.BATCH_MESSAGE_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.common.CompactedObjectFileIndex;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.CompactStreamTaskSerde;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.IWriteResult;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCompactStreamTask;
import io.lakestream.ursa.lakehouse.iceberg.IcebergWriteResult;
import io.lakestream.ursa.lakehouse.io.parquet.ParquetWriteResult;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link CompactionTaskCompleter} — the extracted task-completion + persistence
 * logic shared by the legacy {@code UrsaCompactionWorker} and the new
 * {@code LakehouseMaterializationService}. It records write results onto the task, marks it
 * {@code COMPACTED}, and persists it via {@link CompactTaskManager} so the group-commit runner
 * finalizes the catalog snapshot — it does NOT commit to the catalog itself.
 */
@Tag("lakehouse")
class CompactionTaskCompleterTest {

    private static CompactStreamTask task() {
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic("default/events-partition-0");
        task.setStartOffset(0L);
        task.setEndOffset(5L);
        return task;
    }

    @Test
    void externalResultsPersistTaskAsCompactedIcebergTask() throws Exception {
        CompactTaskManager ctm = mock(CompactTaskManager.class);
        when(ctm.updateCompactTask(any())).thenReturn(CompletableFuture.completedFuture(null));
        CompactionTaskCompleter completer = new CompactionTaskCompleter(ctm);

        IWriteResult result = new IcebergWriteResult(mock(WriteResult.class));
        completer.completeCompaction(task(), List.of(), List.of(result), List.of());

        ArgumentCaptor<CompactStreamTask> captor = ArgumentCaptor.forClass(CompactStreamTask.class);
        verify(ctm).updateCompactTask(captor.capture());
        CompactStreamTask persisted = captor.getValue();
        assertThat(persisted.getStatus()).isEqualTo(CompactStreamTask.COMPACTED);
        assertThat(persisted).isInstanceOf(IcebergCompactStreamTask.class);
        // The runner reads COMPACTED tasks from Oxia and group-commits; the completer must not
        // have committed to any catalog itself (no committer involved here).
    }

    @Test
    void dltOnlyResultsStillPersistTaskAsCompacted() throws Exception {
        // Every record failed serde: no data results, only DLT results. completeExternalCompaction
        // must still wrap + persist the task (using the DLT result type) so the failures are not lost.
        CompactTaskManager ctm = mock(CompactTaskManager.class);
        when(ctm.updateCompactTask(any())).thenReturn(CompletableFuture.completedFuture(null));
        CompactionTaskCompleter completer = new CompactionTaskCompleter(ctm);

        IWriteResult dlt = new IcebergWriteResult(mock(WriteResult.class));
        completer.completeCompaction(task(), List.of(), List.of(), List.of(dlt));

        ArgumentCaptor<CompactStreamTask> captor = ArgumentCaptor.forClass(CompactStreamTask.class);
        verify(ctm).updateCompactTask(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(CompactStreamTask.COMPACTED);
        assertThat(captor.getValue()).isInstanceOf(IcebergCompactStreamTask.class);
    }

    @Test
    void allInternalFilesRemainAddressableWithoutAnIndexFeatureFlag() throws Exception {
        CompactTaskManager ctm = mock(CompactTaskManager.class);
        when(ctm.updateCompactTask(any())).thenReturn(CompletableFuture.completedFuture(null));
        CompactionTaskCompleter completer = new CompactionTaskCompleter(ctm);
        ParquetWriteResult first = internalResult("first.parquet", 2, 3);
        ParquetWriteResult second = internalResult("second.parquet", 4, 2);
        CompactStreamTask task = task();

        completer.completeCompaction(task, List.of(first, second), List.of(), List.of());

        verify(ctm).updateCompactTask(task);
        assertThat(task.getCompactedObjectWriteResults()).hasSize(2);
        assertThat(task.getNumberOfRecordsInCompactedFile()).isEqualTo(5);
        CompactStreamTask restored = CompactStreamTaskSerde.INSTANCE.deserialize(
                CompactStreamTaskSerde.INSTANCE.serialize(task));
        assertThat(restored.getCompactedObjectWriteResults()).isEqualTo(task.getCompactedObjectWriteResults());
        CompactedObjectFileIndex index = new CompactedObjectFileIndex();
        task.getCompactedObjectWriteResults().forEach(result ->
                index.append(result.getLastEntryId(), result.getFilePath()));
        assertThat(index.get(0)).isEqualTo("first.parquet");
        assertThat(index.get(2)).isEqualTo("first.parquet");
        assertThat(index.get(3)).isEqualTo("second.parquet");
        assertThat(index.get(4)).isEqualTo("second.parquet");
    }

    private static ParquetWriteResult internalResult(String file, long lastOffset, long count) {
        return new ParquetWriteResult(URI.create("file:///tmp/co/"), file, 100, count,
                file + ".index", null, Map.of(BATCH_MESSAGE_COUNT, new AtomicLong(count),
                        "lastEntryIdInFile", lastOffset, "lastBatchIdInFile", 0L));
    }

    @Test
    void noWriteResultsIsRejected() {
        CompactTaskManager ctm = mock(CompactTaskManager.class);
        CompactionTaskCompleter completer = new CompactionTaskCompleter(ctm);

        assertThatThrownBy(() ->
                completer.completeCompaction(task(), List.of(), List.of(), List.of()))
                .isInstanceOf(ExceptionWithCode.class)
                .hasMessageContaining("No write results");
    }
}
