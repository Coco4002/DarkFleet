package com.darkfleet.common.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * JSON serde for the shared message records. Dates are written as ISO-8601 strings and
 * unknown fields are ignored, so consumers tolerate fields added by newer producers.
 */
public final class JsonSerde<T> implements Serde<T> {

    private static final JsonMapper MAPPER = JsonMapper.builder()
            .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    public static JsonMapper mapper() {
        return MAPPER;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (JacksonException e) {
                throw new SerializationException("Failed to serialize message for topic " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, targetType);
            } catch (JacksonException e) {
                throw new SerializationException("Failed to deserialize message from topic " + topic, e);
            }
        };
    }
}
