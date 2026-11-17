package com.darkfleet.intelligence;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * @param replay true when this instance serves a stack in replay mode: it reads the {@code replay.*}
 *               topics Track &amp; Detection writes from replayed data, never the live ones
 * @param schema the database schema holding this instance's tables, so replayed data never mixes
 *               with live data in the same tables
 */
@ConfigurationProperties("darkfleet.intelligence")
public record IntelligenceProperties(boolean replay, String schema) {

    private static final Pattern SCHEMA_NAME = Pattern.compile("[a-z_][a-z0-9_]*");

    public IntelligenceProperties {
        Objects.requireNonNull(schema, "darkfleet.intelligence.schema is required");
        if (!SCHEMA_NAME.matcher(schema).matches()) {
            throw new IllegalArgumentException("darkfleet.intelligence.schema must be a plain lower-case name: " + schema);
        }
    }
}
