/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link UnifiedStreamReader} that routes reads
 * to either RAW (WAL) or PARQUET (compacted) storage based on the entry index
 * file type.
 *
 * <p>Routing logic:
 * <ol>
 *   <li>Look up the {@link EntryIndex} for the given offset via {@link EntryIndexCache}.</li>
 *   <li>Check {@link io.lakestream.api.Position.FileType}:
 *       <ul>
 *         <li>{@code RAW} — delegate to {@link StorageApi#readEntries(long, long, int, int)}</li>
 *         <li>{@code PARQUET} — delegate to
 *             {@link CompactedObjectReader#readMessagesWithEntryIndexAsync}</li>
 *       </ul>
 *   </li>
 *   <li>Compute {@code nextOffset} from the returned entries.</li>
 * </ol>
 *
 * <p>This implementation handles basic routing only. Advanced prefetching and
 * caching strategies (e.g., the cursor's index prefetch queue, parquet cache
 * management) remain in the cursor for now.
 */
public class DefaultUnifiedStreamReader implements UnifiedStreamReader {

    private final StorageApi storageApi;
    private final CompactedObjectReader compactedReader;
    private final EntryIndexCache entryIndexCache;

    public DefaultUnifiedStreamReader(StorageApi storageApi,
                                      CompactedObjectReader compactedReader,
                                      EntryIndexCache entryIndexCache) {
        this.storageApi = storageApi;
        this.compactedReader = compactedReader;
        this.entryIndexCache = entryIndexCache;
    }

    @Override
    public CompletableFuture<ReadResult> readEntries(
            LogId logId, long startOffset, int maxMessageCount, long maxSizeBytes) {
        CompletableFuture<ReadResult> read =
            readIndexed(logId.id(), startOffset, maxMessageCount, maxSizeBytes, true);
        return OwnedResultFutures.transfer(read, result -> {
            if (result != null) {
                OwnedResultFutures.closeLogEntries(result.entries());
            }
        });
    }

    /**
     * Resolves the entry index of {@code startOffset} and reads through it.
     *
     * <p>A RAW read that comes back empty although an index resolved is not trusted. Compaction replaces a
     * RAW index with a PARQUET one under the same key and deletes the WAL file, while this reader's cached
     * index can still be the RAW one: the RAW read then finds nothing, every later read resolves the same
     * cached index and finds nothing again, and a consumer waiting at that offset is answered empty until
     * the cache entry expires or another failure clears the cache. The stale index is dropped and the
     * offset is resolved once more; only an empty answer from a freshly resolved index is returned.
     */
    private CompletableFuture<ReadResult> readIndexed(
            long streamId, long startOffset, int maxMessageCount, long maxSizeBytes, boolean retryEmptyRaw) {
        return entryIndexCache.get(streamId, startOffset)
            .thenCompose(entryIndex -> {
                if (entryIndex == null || entryIndex == EntryIndex.NOT_FOUND) {
                    return CompletableFuture.completedFuture(
                        new ReadResult(List.of(), startOffset));
                }
                return switch (entryIndex.position().fileType()) {
                    case RAW -> readFromRaw(streamId, startOffset, maxMessageCount, maxSizeBytes)
                        .thenCompose(result -> {
                            if (retryEmptyRaw && result.entries().isEmpty()) {
                                entryIndexCache.invalidate(streamId, startOffset);
                                if (entryIndex.header().offset() != startOffset) {
                                    entryIndexCache.invalidate(streamId, entryIndex.header().offset());
                                }
                                return readIndexed(streamId, startOffset, maxMessageCount, maxSizeBytes, false);
                            }
                            return CompletableFuture.completedFuture(result);
                        });
                    case PARQUET -> readFromCompacted(
                        entryIndex, startOffset, maxMessageCount, maxSizeBytes);
                };
            });
    }

    private CompletableFuture<ReadResult> readFromRaw(
            long streamId, long startOffset, int maxMessageCount, long maxSizeBytes) {
        int maxSize = (int) Math.min(maxSizeBytes, Integer.MAX_VALUE);
        return storageApi.readEntries(streamId, startOffset, maxMessageCount, maxSize)
            .thenApply(entries -> createReadResult(Entry.toLogEntries(entries), startOffset));
    }

    private CompletableFuture<ReadResult> readFromCompacted(
            EntryIndex entryIndex, long startOffset, int maxMessageCount, long maxSizeBytes) {
        long baseOffset = entryIndex.header().offset();
        return compactedReader.readMessagesWithEntryIndexAsync(
                entryIndex, startOffset, baseOffset, maxMessageCount, maxSizeBytes)
            .thenApply(readResult -> createReadResult(readResult.entries(), startOffset));
    }

    private static ReadResult createReadResult(List<LogEntry> entries, long startOffset) {
        try {
            long nextOffset = startOffset;
            for (LogEntry entry : entries) {
                nextOffset = entry.offset() + entry.numberOfRecords();
            }
            return new ReadResult(entries, nextOffset);
        } catch (RuntimeException | Error metadataFailure) {
            closeEntriesAfterFailure(entries, metadataFailure);
            throw metadataFailure;
        }
    }

    private static void closeEntriesAfterFailure(List<LogEntry> entries, Throwable failure) {
        for (LogEntry entry : entries) {
            if (entry == null) {
                continue;
            }
            try {
                entry.close();
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    @Override
    public void close() throws Exception {
        compactedReader.close();
    }
}
