/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.cleaner;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class CompactedDataCleanupHandlerTest {

    private CompactedDataCleanupHandler handler;

    @AfterEach
    void tearDown() {
        if (handler != null) {
            handler.stop();
        }
    }

    @Test
    void internalCleanupDoesNotResolveTableConfiguration() {
        StorageConfig config = mock(StorageConfig.class);
        when(config.getCompactedDataCleanupThreadNum()).thenReturn(1);
        StorageApi storage = mock(StorageApi.class);
        when(storage.readIndexes(1L, 0L, 10L, true))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        FileStorage files = mock(FileStorage.class);
        handler = new CompactedDataCleanupHandler(config, storage, files);

        handler.cleanup(new TopicCleanupTask("default/orders-id-partition-0", 1L, 10L)).join();

        verify(config, never()).getProperties();
        verify(files, never()).deleteAsync(any());
    }

    @Test
    void failedFileDeletionDoesNotTrimStreamIndexes() {
        StorageConfig config = mock(StorageConfig.class);
        when(config.getCompactedDataCleanupThreadNum()).thenReturn(1);
        StorageApi storage = mock(StorageApi.class);
        FileStorage files = mock(FileStorage.class);
        String path = "compacted/default/orders-id-partition-0/data.parquet";
        when(files.deleteAsync(List.of(path)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("delete failed")));
        handler = spy(new CompactedDataCleanupHandler(config, storage, files));
        TopicCleanupTask task = new TopicCleanupTask("default/orders-id-partition-0", 1L, 10L);
        var stats = new ParquetFileStat("data.parquet", path, 100L, null, Map.of(), Map.of());
        var queue = new LinkedList<CompactedDataCleanupHandler.SubTask>();
        queue.add(new CompactedDataCleanupHandler.SubTask(task, List.of(stats), 10L));
        doReturn(CompletableFuture.completedFuture(queue)).when(handler).splitTasks(task);

        assertThrows(CompletionException.class, () -> handler.cleanup(task).join());

        verify(files).deleteAsync(List.of(path));
        verify(storage, never()).hardTrimStream(anyLong(), anyLong());
        verify(config, never()).getProperties();
    }
}
