package com.darkfleet.detection;

import com.darkfleet.common.model.NormalizedAisMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;

/**
 * Orders AIS messages by when they were transmitted rather than when they reached Kafka, which is
 * what makes a replay keep its original timing. A message stamped further in the future than
 * {@code maxClockSkew} past its reception comes from a broken clock; returning a negative time makes
 * Kafka Streams drop it with a warning.
 */
public record EventTimeExtractor(Duration maxClockSkew) implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof NormalizedAisMessage m) {
            return m.eventTime().isAfter(m.ingestionTime().plus(maxClockSkew)) ? -1 : m.eventTime().toEpochMilli();
        }
        return record.timestamp();
    }
}
