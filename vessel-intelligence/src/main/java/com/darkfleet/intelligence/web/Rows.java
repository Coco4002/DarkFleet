package com.darkfleet.intelligence.web;

import com.darkfleet.common.kafka.JsonSerde;
import tools.jackson.databind.JsonNode;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/** Reading typed values out of JDBC rows. */
public final class Rows {

    private Rows() {
    }

    public static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp ts = rs.getTimestamp(column);
        return ts == null ? null : ts.toInstant();
    }

    public static Integer integer(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    public static Double decimal(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    /** A jsonb column as JSON, so the API returns it nested rather than as a string. */
    public static JsonNode json(ResultSet rs, String column) throws SQLException {
        String text = rs.getString(column);
        return text == null ? null : JsonSerde.mapper().readTree(text);
    }

    public static List<String> strings(ResultSet rs, String column) throws SQLException {
        Array array = rs.getArray(column);
        if (array == null) {
            return List.of();
        }
        return List.of((String[]) array.getArray());
    }
}
