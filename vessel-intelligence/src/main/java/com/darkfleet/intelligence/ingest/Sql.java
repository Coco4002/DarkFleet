package com.darkfleet.intelligence.ingest;

import com.darkfleet.common.kafka.JsonSerde;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** Conversions for JDBC parameters. */
final class Sql {

    private Sql() {
    }

    /** A timestamptz parameter, in UTC. */
    static OffsetDateTime ts(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    /** A jsonb parameter, written the same way the messages are. */
    static String json(Object value) {
        return JsonSerde.mapper().writeValueAsString(value);
    }

    static String name(Enum<?> value) {
        return value == null ? null : value.name();
    }
}
