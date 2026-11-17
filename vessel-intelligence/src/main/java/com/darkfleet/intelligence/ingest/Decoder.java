package com.darkfleet.intelligence.ingest;

import com.darkfleet.common.kafka.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads the JSON records of a batch into the shared message types. A record that can't be read
 * (malformed JSON, a required field missing, a JSON {@code null}) is logged and skipped, so one bad
 * message never blocks the topic; retrying it would fail the same way forever.
 */
final class Decoder {

    private static final Logger log = LoggerFactory.getLogger(Decoder.class);

    private Decoder() {
    }

    static <T> List<T> decode(List<ConsumerRecord<String, byte[]>> records, Class<T> type) {
        List<T> decoded = new ArrayList<>(records.size());
        for (ConsumerRecord<String, byte[]> record : records) {
            if (record.value() == null || record.value().length == 0) {
                continue;
            }
            try {
                T value = JsonSerde.mapper().readValue(record.value(), type);
                if (value == null) {
                    log.warn("Skipping null {} at {}-{} offset {}", type.getSimpleName(), record.topic(),
                            record.partition(), record.offset());
                    continue;
                }
                decoded.add(value);
            } catch (JacksonException e) {
                log.warn("Skipping unreadable {} at {}-{} offset {}: {}", type.getSimpleName(), record.topic(),
                        record.partition(), record.offset(), e.getOriginalMessage());
            }
        }
        return decoded;
    }
}
