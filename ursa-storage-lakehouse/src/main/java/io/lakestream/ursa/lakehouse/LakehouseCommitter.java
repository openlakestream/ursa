/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;

public interface LakehouseCommitter extends AutoCloseable {

    boolean tableExists()throws LakehouseException;

    void createTable(Schema schema) throws LakehouseException;

    boolean isTheCompactStreamTaskCommitted(CompactStreamTask compactStreamTask) throws IOException;

    long commit(List<ParquetFileStat> fileStats) throws LakehouseException;

    void updateTablePropertiesIfNeeded(Map<String, String> properties) throws LakehouseException;

    String getName();
}
