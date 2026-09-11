/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.api.materialization.Compression;
import org.junit.jupiter.api.Test;

class MaterializationJsonTest {

    @Test
    void readsLegacyManagedPolicy() throws Exception {
        assertLegacyTableSettings("MANAGED");
    }

    @Test
    void readsLegacyExternalPolicy() throws Exception {
        assertLegacyTableSettings("EXTERNAL");
    }

    @Test
    void readsLegacyCustomPolicy() throws Exception {
        assertLegacyTableSettings("CUSTOM");
    }

    private void assertLegacyTableSettings(String legacyMode) throws Exception {
        var json = new ObjectMapper().readTree("""
                {"catalogRef":"analytics","enabled":true,
                 "table":{"compression":"ZSTD","targetFileSizeBytes":1024}}
                """);
        ((com.fasterxml.jackson.databind.node.ObjectNode) json.get("table")).put("mode", legacyMode);

        var policy = MaterializationJson.policyFromJson(json);
        var table = policy.table().orElseThrow();
        assertEquals(Compression.ZSTD, table.compression().orElseThrow());
        assertEquals(1024L, table.targetFileSizeBytes().orElseThrow());

        var serialized = MaterializationJson.policyToJson(policy);
        assertFalse(serialized.get("table").has("mode"));
        assertEquals(policy, MaterializationJson.policyFromJson(serialized));
    }
}
