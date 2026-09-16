/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import io.lakestream.api.SourceMetadataProperties;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableNaming;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/** Resolves writer destinations and carries their final identity to asynchronous committers. */
public final class StreamTableNaming {

    /** Overrides the table name a stream materializes into. See {@code TableNaming} for the syntax. */
    public static final String TABLE_NAME_TEMPLATE_PROPERTY = "tableNameTemplate";

    /** Internal task property containing the final resolved table namespace. */
    public static final String RESOLVED_TABLE_NAMESPACE_PROPERTY =
            "lakestream.materialization.resolved.table.namespace";

    /** Internal task property containing the final resolved table name. */
    public static final String RESOLVED_TABLE_NAME_PROPERTY =
            "lakestream.materialization.resolved.table.name";

    private StreamTableNaming() {
    }

    /** Stores the final table identity in writer configuration. */
    public static void applyResolvedTableIdentifier(Properties properties, TableIdentifier identifier) {
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(identifier, "identifier");
        properties.setProperty(RESOLVED_TABLE_NAMESPACE_PROPERTY, identifier.namespace());
        properties.setProperty(RESOLVED_TABLE_NAME_PROPERTY, identifier.name());
    }

    /** Returns an immutable task-property snapshot carrying the final table identity. */
    public static Map<String, String> withResolvedTableIdentifier(
            Map<String, String> properties, TableIdentifier identifier) {
        Objects.requireNonNull(identifier, "identifier");
        Map<String, String> resolved = new HashMap<>(properties == null ? Map.of() : properties);
        resolved.put(RESOLVED_TABLE_NAMESPACE_PROPERTY, identifier.namespace());
        resolved.put(RESOLVED_TABLE_NAME_PROPERTY, identifier.name());
        return Map.copyOf(resolved);
    }

    /** Returns {@code namespace/name}, the writer representation of a table identifier. */
    public static String qualifiedName(TableIdentifier identifier) {
        Objects.requireNonNull(identifier, "identifier");
        return identifier.namespace() + "/" + identifier.name();
    }

    /** Returns the dead-letter table next to {@code identifier}. */
    public static TableIdentifier deadLetterTable(TableIdentifier identifier, String suffix) {
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(suffix, "suffix");
        return new TableIdentifier(identifier.namespace(), identifier.name() + suffix);
    }

    /** Resolves the same destination for writers and asynchronous committers. */
    public static TableIdentifier resolve(String logName, Properties properties) {
        Optional<TableIdentifier> resolved = resolvedTableIdentifier(properties);
        if (resolved.isPresent()) {
            return resolved.get();
        }

        TopicName identity = TopicName.getStreamIdentity(logName);
        StreamIdentifier stream = StreamIdentifier.of(identity.getNamespace(), identity.getLocalName());
        String template = properties == null ? null : properties.getProperty(TABLE_NAME_TEMPLATE_PROPERTY);
        if (template != null) {
            return new TableNaming(Optional.empty(), template).toTableIdentifier(stream, asMap(properties));
        }
        String tableName = SourceMetadataProperties.logicalName(stream, asMap(properties));
        return new TableIdentifier(stream.namespace(), tableName);
    }

    private static Optional<TableIdentifier> resolvedTableIdentifier(Properties properties) {
        if (properties == null) {
            return Optional.empty();
        }
        String namespace = properties.getProperty(RESOLVED_TABLE_NAMESPACE_PROPERTY);
        String name = properties.getProperty(RESOLVED_TABLE_NAME_PROPERTY);
        if (namespace == null && name == null) {
            return Optional.empty();
        }
        if (namespace == null || namespace.isBlank() || name == null || name.isBlank()) {
            throw new IllegalArgumentException("Incomplete resolved table identifier in task properties");
        }
        return Optional.of(new TableIdentifier(namespace, name));
    }

    private static Map<String, String> asMap(Properties properties) {
        if (properties == null) {
            return Map.of();
        }
        Map<String, String> map = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }
        return map;
    }
}
