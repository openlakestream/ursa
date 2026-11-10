/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.Position;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.lakestream.ursa.storage.impl.exception.WalFileAlreadyCompactedException;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultUnifiedStreamReaderTest {

    @Mock
    private StorageApi storageApi;

    @Mock
    private CompactedObjectReader compactedReader;

    @Mock
    private EntryIndexCache entryIndexCache;

    private DefaultUnifiedStreamReader reader;

    @BeforeEach
    void setUp() {
        reader = new DefaultUnifiedStreamReader(storageApi, compactedReader, entryIndexCache);
    }

    @Test
    void testReadFromRaw() throws Exception {
        LogId logId = LogId.of(101);
        long startOffset = 10;
        EntryHeader header = new EntryHeader(10, 3, 1000L, 100, 500);
        Position position = new Position("wal-file-1", -1, Position.FileType.RAW);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(101L, 10L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));

        Entry entry = Entry.of(header, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        when(storageApi.readEntries(eq(101L), eq(10L), eq(5), eq(1024)))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, startOffset, 5, 1024L).get();

        assertEquals(1, result.entries().size());
        assertEquals(13, result.nextOffset()); // 10 + 3
        verifyNoInteractions(compactedReader);
        result.entries().forEach(LogEntry::close);
    }

    @Test
    void testReadFromParquet() throws Exception {
        LogId logId = LogId.of(202);
        long startOffset = 50;
        EntryHeader header = new EntryHeader(50, 10, 2000L, 500, 1000);
        Position position = new Position("parquet-file-1", 0, Position.FileType.PARQUET);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.COMPACT,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(202L, 50L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));

        Entry parquetEntry = Entry.of(
            new EntryHeader(50, 10, 2000L, 500, 1000),
            Unpooled.wrappedBuffer(new byte[]{4, 5, 6}));
        CompactedObjectReader.ReadResult readResult =
            new CompactedObjectReader.ReadResult(List.of(parquetEntry.toLogEntry()));

        when(compactedReader.readMessagesWithEntryIndexAsync(
            eq(entryIndex), eq(50L), eq(50L), eq(5L), eq(1024L)))
            .thenReturn(CompletableFuture.completedFuture(readResult));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, startOffset, 5, 1024L).get();

        assertEquals(1, result.entries().size());
        assertEquals(60, result.nextOffset()); // 50 + 10
        verify(compactedReader).readMessagesWithEntryIndexAsync(
            eq(entryIndex), eq(50L), eq(50L), eq(5L), eq(1024L));
        result.entries().forEach(LogEntry::close);
    }

    @Test
    void testReadFromParquetV2MultipleEntries() throws Exception {
        LogId logId = LogId.of(303);
        long startOffset = 100;
        EntryHeader header = new EntryHeader(100, 20, 3000L, 800, 2000);
        Position position = new Position("parquet-file-2", 0, Position.FileType.PARQUET);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.COMPACT,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(303L, 100L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));

        Entry entry1 = Entry.of(
            new EntryHeader(100, 5, 3000L, 200, 200),
            Unpooled.wrappedBuffer(new byte[]{1}));
        Entry entry2 = Entry.of(
            new EntryHeader(105, 5, 3001L, 200, 400),
            Unpooled.wrappedBuffer(new byte[]{2}));
        CompactedObjectReader.ReadResult readResult =
            new CompactedObjectReader.ReadResult(
                List.of(entry1.toLogEntry(), entry2.toLogEntry()));

        when(compactedReader.readMessagesWithEntryIndexAsync(
            eq(entryIndex), eq(100L), eq(100L), eq(10L), eq(2048L)))
            .thenReturn(CompletableFuture.completedFuture(readResult));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, startOffset, 10, 2048L).get();

        assertEquals(2, result.entries().size());
        assertEquals(110, result.nextOffset()); // 105 + 5
        result.entries().forEach(LogEntry::close);
    }

    @Test
    void testMetadataFailureClosesCompactedEntries() {
        LogId logId = LogId.of(304);
        long startOffset = 100;
        EntryHeader header = new EntryHeader(100, 1, 3000L, 10, 10);
        EntryIndex entryIndex = new EntryIndex(
            header,
            new Position("parquet-file-3", 0, Position.FileType.PARQUET),
            1,
            EntryIndex.IndexType.COMPACT,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
        LogEntry brokenEntry = mock(LogEntry.class);
        when(brokenEntry.offset()).thenThrow(new IllegalStateException("invalid metadata"));
        when(entryIndexCache.get(logId.id(), startOffset))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));
        when(compactedReader.readMessagesWithEntryIndexAsync(
            entryIndex, startOffset, startOffset, 1L, 1024L))
            .thenReturn(CompletableFuture.completedFuture(
                new CompactedObjectReader.ReadResult(List.of(brokenEntry))));

        assertThrows(ExecutionException.class,
            () -> reader.readEntries(logId, startOffset, 1, 1024L).get());

        verify(brokenEntry).close();
    }

    @Test
    void testReadNotFoundIndex() throws Exception {
        LogId logId = LogId.of(404);
        when(entryIndexCache.get(404L, 0L))
            .thenReturn(CompletableFuture.completedFuture(EntryIndex.NOT_FOUND));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 0L, 10, 4096L).get();

        assertTrue(result.entries().isEmpty());
        assertEquals(0, result.nextOffset());
        verifyNoInteractions(storageApi);
        verifyNoInteractions(compactedReader);
    }

    @Test
    void testReadNullIndex() throws Exception {
        LogId logId = LogId.of(500);
        when(entryIndexCache.get(500L, 0L))
            .thenReturn(CompletableFuture.completedFuture(null));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 0L, 10, 4096L).get();

        assertTrue(result.entries().isEmpty());
        assertEquals(0, result.nextOffset());
    }

    @Test
    void testReadRawEmpty() throws Exception {
        LogId logId = LogId.of(600);
        EntryHeader header = new EntryHeader(0, 1, 1000L, 10, 10);
        Position position = new Position("wal-file", -1, Position.FileType.RAW);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(600L, 0L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));
        when(storageApi.readEntries(eq(600L), eq(0L), eq(10), eq(4096)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 0L, 10, 4096L).get();

        assertTrue(result.entries().isEmpty());
        assertEquals(0, result.nextOffset());
    }

    @Test
    void testEmptyRawReadDropsTheStaleIndexAndReadsTheCompactedOne() throws Exception {
        LogId logId = LogId.of(700);
        EntryHeader rawHeader = new EntryHeader(40, 20, 1000L, 100, 500);
        EntryIndex staleRaw = new EntryIndex(rawHeader, new Position("deleted-wal", -1, Position.FileType.RAW), 1,
            EntryIndex.IndexType.NORMAL, Optional.empty(), Optional.empty(), Optional.empty());
        EntryHeader compactedHeader = new EntryHeader(30, 40, 1000L, 800, 500);
        EntryIndex compacted = new EntryIndex(compactedHeader, new Position("parquet", 0, Position.FileType.PARQUET),
            1, EntryIndex.IndexType.COMPACT, Optional.empty(), Optional.empty(), Optional.empty());

        // The cache still holds the RAW index that compaction replaced; once dropped it resolves the new one.
        when(entryIndexCache.get(700L, 50L))
            .thenReturn(CompletableFuture.completedFuture(staleRaw))
            .thenReturn(CompletableFuture.completedFuture(compacted));
        when(storageApi.readEntries(eq(700L), eq(50L), eq(5), eq(1024)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        Entry parquetEntry = Entry.of(new EntryHeader(50, 20, 1000L, 100, 500),
            Unpooled.wrappedBuffer(new byte[]{7}));
        when(compactedReader.readMessagesWithEntryIndexAsync(
            eq(compacted), eq(50L), eq(30L), eq(5L), eq(1024L)))
            .thenReturn(CompletableFuture.completedFuture(
                new CompactedObjectReader.ReadResult(List.of(parquetEntry.toLogEntry()))));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 50L, 5, 1024L).get();

        assertEquals(1, result.entries().size());
        assertEquals(70, result.nextOffset());
        verify(entryIndexCache).invalidate(700L);
        result.entries().forEach(LogEntry::close);
    }

    /**
     * The cache keeps an index under the offset it was first looked up at and serves the rest of its range
     * through that key. A stale RAW index first looked up in the middle of its range, as when a consumer resumes
     * from a committed offset, has to be dropped too when a later offset in its range reads nothing through it.
     */
    @Test
    void testStaleRawIndexCachedUnderAnotherOffsetOfItsRangeIsDropped() throws Exception {
        LogId logId = LogId.of(702);
        EntryIndex staleRaw = new EntryIndex(new EntryHeader(40, 20, 1000L, 100, 500),
            new Position("compacted-wal", -1, Position.FileType.RAW), 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());
        EntryIndex compacted = new EntryIndex(new EntryHeader(30, 40, 1000L, 800, 500),
            new Position("parquet", 0, Position.FileType.PARQUET), 1, EntryIndex.IndexType.COMPACT,
            Optional.empty(), Optional.empty(), Optional.empty());
        AtomicReference<EntryIndex> indexInOxia = new AtomicReference<>(staleRaw);
        EntryIndexCache cache = new EntryIndexCache(
            (streamId, offset) -> CompletableFuture.completedFuture(indexInOxia.get()), 100, 600);
        DefaultUnifiedStreamReader cachingReader = new DefaultUnifiedStreamReader(storageApi, compactedReader, cache);

        // First looked up at 45, the RAW index is cached under key 45 only.
        assertSame(staleRaw, cache.get(702L, 45L).get());
        // Compaction replaces it in Oxia, and a RAW read at 50 now finds nothing.
        indexInOxia.set(compacted);
        when(storageApi.readEntries(eq(702L), eq(50L), eq(5), eq(1024)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        Entry parquetEntry = Entry.of(new EntryHeader(50, 20, 1000L, 100, 500),
            Unpooled.wrappedBuffer(new byte[]{7}));
        when(compactedReader.readMessagesWithEntryIndexAsync(
            eq(compacted), eq(50L), eq(30L), eq(5L), eq(1024L)))
            .thenReturn(CompletableFuture.completedFuture(
                new CompactedObjectReader.ReadResult(List.of(parquetEntry.toLogEntry()))));

        UnifiedStreamReader.ReadResult result = cachingReader.readEntries(logId, 50L, 5, 1024L).get();

        assertEquals(1, result.entries().size());
        assertEquals(70, result.nextOffset());
        verify(storageApi, times(1)).readEntries(eq(702L), eq(50L), eq(5), eq(1024));
        result.entries().forEach(LogEntry::close);
    }

    /**
     * A RAW read whose start and end resolve to the single PARQUET key compaction left fails with
     * {@link WalFileAlreadyCompactedException} instead of returning nothing. It is the same stale index.
     */
    @Test
    void testWalFileAlreadyCompactedFromAStaleRawIndexReadsTheCompactedOne() throws Exception {
        LogId logId = LogId.of(703);
        EntryIndex staleRaw = new EntryIndex(new EntryHeader(40, 20, 1000L, 100, 500),
            new Position("compacted-wal", -1, Position.FileType.RAW), 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());
        EntryIndex compacted = new EntryIndex(new EntryHeader(30, 40, 1000L, 800, 500),
            new Position("parquet", 0, Position.FileType.PARQUET), 1, EntryIndex.IndexType.COMPACT,
            Optional.empty(), Optional.empty(), Optional.empty());
        when(entryIndexCache.get(703L, 50L))
            .thenReturn(CompletableFuture.completedFuture(staleRaw))
            .thenReturn(CompletableFuture.completedFuture(compacted));
        when(storageApi.readEntries(eq(703L), eq(50L), eq(1), eq(1024)))
            .thenReturn(CompletableFuture.failedFuture(new WalFileAlreadyCompactedException()));
        Entry parquetEntry = Entry.of(new EntryHeader(50, 1, 1000L, 100, 500),
            Unpooled.wrappedBuffer(new byte[]{7}));
        when(compactedReader.readMessagesWithEntryIndexAsync(
            eq(compacted), eq(50L), eq(30L), eq(1L), eq(1024L)))
            .thenReturn(CompletableFuture.completedFuture(
                new CompactedObjectReader.ReadResult(List.of(parquetEntry.toLogEntry()))));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 50L, 1, 1024L).get();

        assertEquals(1, result.entries().size());
        assertEquals(51, result.nextOffset());
        verify(entryIndexCache).invalidate(703L);
        result.entries().forEach(LogEntry::close);
    }

    @Test
    void testWalFileAlreadyCompactedFromAFreshlyResolvedIndexIsRetriedOnlyOnce() {
        LogId logId = LogId.of(704);
        EntryIndex raw = new EntryIndex(new EntryHeader(0, 1, 1000L, 10, 10),
            new Position("wal-file", -1, Position.FileType.RAW), 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());
        when(entryIndexCache.get(704L, 0L)).thenReturn(CompletableFuture.completedFuture(raw));
        when(storageApi.readEntries(eq(704L), eq(0L), eq(10), eq(4096)))
            .thenReturn(CompletableFuture.failedFuture(new WalFileAlreadyCompactedException()));

        ExecutionException thrown = assertThrows(ExecutionException.class,
            () -> reader.readEntries(logId, 0L, 10, 4096L).get());

        assertInstanceOf(WalFileAlreadyCompactedException.class, thrown.getCause());
        verify(storageApi, times(2)).readEntries(eq(704L), eq(0L), eq(10), eq(4096));
        verify(entryIndexCache, times(1)).invalidate(704L);
    }

    @Test
    void testOtherRawReadFailuresAreNotRetried() {
        LogId logId = LogId.of(705);
        EntryIndex raw = new EntryIndex(new EntryHeader(0, 1, 1000L, 10, 10),
            new Position("wal-file", -1, Position.FileType.RAW), 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());
        when(entryIndexCache.get(705L, 0L)).thenReturn(CompletableFuture.completedFuture(raw));
        IllegalStateException failure = new IllegalStateException("storage unavailable");
        when(storageApi.readEntries(eq(705L), eq(0L), eq(10), eq(4096)))
            .thenReturn(CompletableFuture.failedFuture(failure));

        ExecutionException thrown = assertThrows(ExecutionException.class,
            () -> reader.readEntries(logId, 0L, 10, 4096L).get());

        assertSame(failure, thrown.getCause());
        verify(storageApi, times(1)).readEntries(eq(705L), eq(0L), eq(10), eq(4096));
        verify(entryIndexCache, never()).invalidate(anyLong());
    }

    @Test
    void testEmptyRawReadFromAFreshlyResolvedIndexIsRetriedOnlyOnce() throws Exception {
        LogId logId = LogId.of(701);
        EntryHeader header = new EntryHeader(0, 1, 1000L, 10, 10);
        EntryIndex raw = new EntryIndex(header, new Position("wal-file", -1, Position.FileType.RAW), 1,
            EntryIndex.IndexType.NORMAL, Optional.empty(), Optional.empty(), Optional.empty());
        when(entryIndexCache.get(701L, 0L)).thenReturn(CompletableFuture.completedFuture(raw));
        when(storageApi.readEntries(eq(701L), eq(0L), eq(10), eq(4096)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 0L, 10, 4096L).get();

        assertTrue(result.entries().isEmpty());
        verify(storageApi, times(2)).readEntries(eq(701L), eq(0L), eq(10), eq(4096));
        verify(entryIndexCache, times(1)).invalidate(701L);
    }

    @Test
    void testMaxSizeBytesClamping() throws Exception {
        LogId logId = LogId.of(700);
        EntryHeader header = new EntryHeader(0, 1, 1000L, 10, 10);
        Position position = new Position("wal-file", -1, Position.FileType.RAW);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(700L, 0L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));
        // maxSizeBytes exceeds Integer.MAX_VALUE — should clamp to Integer.MAX_VALUE
        Entry entry = Entry.of(header, Unpooled.wrappedBuffer(new byte[]{1}));
        when(storageApi.readEntries(eq(700L), eq(0L), eq(10), eq(Integer.MAX_VALUE)))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        UnifiedStreamReader.ReadResult clamped = reader.readEntries(logId, 0L, 10, Long.MAX_VALUE).get();

        verify(storageApi).readEntries(700L, 0L, 10, Integer.MAX_VALUE);
        clamped.entries().forEach(LogEntry::close);
    }

    @Test
    void testClose() throws Exception {
        reader.close();
        verify(compactedReader).close();
    }
}
