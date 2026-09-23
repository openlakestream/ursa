/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.reader;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface CompactedObjectReader {

    /**
     * Result of a compacted object read.
     *
     * @param entries zero or more entries read from the compacted object
     */
    record ReadResult(List<LogEntry> entries) { }

    CompletableFuture<ReadResult> readMessagesWithEntryIndexAsync(EntryIndex entryIndex, long startOffset,
                                                               long baseOffset, long maxNumOfMessages,
                                                               long maxSize);

    void close();
}
