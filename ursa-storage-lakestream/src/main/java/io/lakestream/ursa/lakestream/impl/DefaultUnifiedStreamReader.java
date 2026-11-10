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
import io.lakestream.ursa.storage.impl.exception.WalFileAlreadyCompactedException;
import io.lakestream.ursa.utils.FutureUtils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
     * <p>A RAW read through a resolved index is not trusted when it comes back empty or fails with
     * {@link WalFileAlreadyCompactedException}. Compaction rewrites the RAW index keys of a range in Oxia
     * into one PARQUET key, while the shared {@link EntryIndexCache} can still hold a RAW index for that
     * range. The RAW read looks the range up in Oxia again and finds the PARQUET index there: when its
     * start and end resolve to that one key it throws {@link WalFileAlreadyCompactedException}, and when it
     * scans a range of keys it stops at the PARQUET one and returns nothing. Every later read would resolve
     * the same cached index and get the same answer, so a consumer waiting at that offset would make no
     * progress until the cache entry expires. The stream's cached indexes are dropped and the offset is
     * resolved once more; the answer of a read through a freshly resolved index is returned as it is.
     */
    private CompletableFuture<ReadResult> readIndexed(
            long streamId, long startOffset, int maxMessageCount, long maxSizeBytes, boolean retryStaleRaw) {
        return entryIndexCache.get(streamId, startOffset)
            .thenCompose(entryIndex -> {
                if (entryIndex == null || entryIndex == EntryIndex.NOT_FOUND) {
                    return CompletableFuture.completedFuture(
                        new ReadResult(List.of(), startOffset));
                }
                return switch (entryIndex.position().fileType()) {
                    case RAW -> {
                        CompletableFuture<ReadResult> raw =
                            readFromRaw(streamId, startOffset, maxMessageCount, maxSizeBytes);
                        if (!retryStaleRaw) {
                            yield raw;
                        }
                        yield raw
                            .handle((result, failure) -> rereadIfStale(
                                streamId, startOffset, maxMessageCount, maxSizeBytes, result, failure))
                            .thenCompose(Function.identity());
                    }
                    case PARQUET -> readFromCompacted(
                        entryIndex, startOffset, maxMessageCount, maxSizeBytes);
                };
            });
    }

    private CompletableFuture<ReadResult> rereadIfStale(long streamId, long startOffset, int maxMessageCount,
            long maxSizeBytes, ReadResult result, Throwable failure) {
        boolean stale = failure == null
            ? result.entries().isEmpty()
            : FutureUtils.unwrapCompletionException(failure) instanceof WalFileAlreadyCompactedException;
        if (!stale) {
            return failure == null
                ? CompletableFuture.completedFuture(result)
                : CompletableFuture.failedFuture(failure);
        }
        // Drop every cached index of the stream, not only the keys of this offset and of the stale index's
        // start: the cache stores an index under the offset it was first looked up at and serves later offsets
        // in its range through that key, so the stale index can sit under any key. Other cached RAW indexes of
        // the stream may be stale after the same compaction as well.
        entryIndexCache.invalidate(streamId);
        return readIndexed(streamId, startOffset, maxMessageCount, maxSizeBytes, false);
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
