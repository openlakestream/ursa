/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.reader;

import io.lakestream.api.EntryIndex;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class NoopCompactedObjectReader implements CompactedObjectReader {

    @Override
    public CompletableFuture<ReadResult> readMessagesWithEntryIndexAsync(
            EntryIndex entryIndex, long startOffset, long baseOffset, long maxNumOfMessages, long maxSize) {
        return CompletableFuture.failedFuture(new IOException("Not available because lakehouse reader is disabled"));
    }

    @Override
    public void close() { }
}
