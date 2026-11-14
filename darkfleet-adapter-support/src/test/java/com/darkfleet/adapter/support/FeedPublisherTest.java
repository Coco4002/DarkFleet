package com.darkfleet.adapter.support;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.FeedHealth;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FeedPublisherTest {

    private static final Instant NOW = Instant.parse("2026-11-14T16:00:00Z");

    private KafkaTemplate<String, Object> kafka;
    private FeedPublisher publisher;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafka = mock(KafkaTemplate.class);
        publisher = new FeedPublisher(AisSource.AISSTREAM, DarkFleetTopics.AIS_RAW, kafka,
                Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofMinutes(10), 1000);
    }

    private static NormalizedAisMessage position(String mmsi, Instant at) {
        return NormalizedAisMessage.positionReport(mmsi, at, 59.8, 25.4, 10.0, 90.0, null, 0, AisSource.AISSTREAM, false);
    }

    @Test
    void publishesKeyedByMmsiAndCountsOnlyAcknowledgedSends() {
        CompletableFuture<Object> pending = new CompletableFuture<>();
        when(kafka.send(anyString(), anyString(), any())).thenReturn((CompletableFuture) pending);

        assertTrue(publisher.publish(position("273450990", NOW)));
        verify(kafka).send(eq(DarkFleetTopics.AIS_RAW), eq("273450990"), any());
        assertEquals(0, publisher.stats().published(), "not acknowledged yet");

        pending.complete(null);
        assertEquals(1, publisher.stats().published());
        assertEquals(0, publisher.stats().sendFailures());
    }

    @Test
    void dropsExactRepeatsWithinTheWindow() {
        when(kafka.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        assertTrue(publisher.publish(position("273450990", NOW)));
        assertFalse(publisher.publish(position("273450990", NOW)), "identical report from a second receiver");
        assertTrue(publisher.publish(position("273450990", NOW.plusSeconds(1))));
        verify(kafka, times(2)).send(anyString(), anyString(), any());
        assertEquals(3, publisher.stats().received());
        assertEquals(1, publisher.stats().duplicates());
    }

    @Test
    void keepsDifferentReportsWithinTheSameSecond() {
        when(kafka.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        var first = position("273450990", NOW);
        var moved = NormalizedAisMessage.positionReport("273450990", NOW, 59.81, 25.41, 10.0, 90.0, null, 0,
                AisSource.AISSTREAM, false);
        assertTrue(publisher.publish(first));
        assertTrue(publisher.publish(moved), "same second, different content");
        assertEquals(0, publisher.stats().duplicates());
    }

    @Test
    void reportsWhoseFieldsShareAHashAreNotTreatedAsRepeats() {
        when(kafka.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        assertEquals("Aa".hashCode(), "BB".hashCode(), "precondition: colliding strings");
        var a = NormalizedAisMessage.staticReport("273450990", NOW, AisSource.AISSTREAM, false, null, "Aa", null, 80, null, null);
        var b = NormalizedAisMessage.staticReport("273450990", NOW, AisSource.AISSTREAM, false, null, "BB", null, 80, null, null);
        assertTrue(publisher.publish(a));
        assertTrue(publisher.publish(b));
        assertEquals(0, publisher.stats().duplicates());
    }

    @Test
    void aFailedSendCanBeRetried() {
        when(kafka.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("broker down")))
                .thenReturn(CompletableFuture.completedFuture(null));
        assertTrue(publisher.publish(position("273450990", NOW)));
        assertTrue(publisher.publish(position("273450990", NOW)), "the failed message is not a repeat");
        assertEquals(1, publisher.stats().published());
        assertEquals(1, publisher.stats().sendFailures());
        assertEquals(0, publisher.stats().duplicates());
    }

    @Test
    void awaitSettledWaitsForEveryAcknowledgement() throws InterruptedException {
        CompletableFuture<Object> pending = new CompletableFuture<>();
        when(kafka.send(anyString(), anyString(), any())).thenReturn((CompletableFuture) pending);
        publisher.publish(position("273450990", NOW));
        assertFalse(publisher.awaitSettled(Duration.ofMillis(50)));
        pending.complete(null);
        assertTrue(publisher.awaitSettled(Duration.ofMillis(50)));
    }

    @Test
    void countsFailedSendsInsteadOfReportingSuccess() {
        when(kafka.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("broker down")));
        publisher.publish(position("273450990", NOW));
        assertEquals(0, publisher.stats().published());
        assertEquals(1, publisher.stats().sendFailures());
    }

    @Test
    void countsSendsThatFailBeforeReachingKafka() {
        when(kafka.send(anyString(), anyString(), any())).thenThrow(new KafkaException("metadata timeout"));
        assertFalse(publisher.publish(position("273450990", NOW)));
        assertEquals(1, publisher.stats().sendFailures());
    }

    @Test
    void heartbeatReportsTheIntervalCountsAndConnectionState() {
        when(kafka.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(kafka.send(eq(DarkFleetTopics.FEED_HEALTH), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        publisher.publish(position("273450990", NOW));
        publisher.publish(NormalizedAisMessage.staticReport("273450990", NOW, AisSource.AISSTREAM, false,
                "9341029", "SHADOW AURORA", null, 80, 14.5, null));

        FeedConnection connection = new FeedConnection() {
            public boolean isConnected() {
                return true;
            }

            public long totalReconnects() {
                return 3;
            }
        };
        var reporter = new FeedHealthReporter(publisher, connection, kafka, Duration.ofSeconds(30),
                Clock.fixed(NOW, ZoneOffset.UTC));
        FeedHealth health = reporter.snapshot();
        assertEquals(AisSource.AISSTREAM, health.source());
        assertTrue(health.connected());
        assertEquals(2, health.messagesInInterval());
        assertEquals(1, health.positionsInInterval());
        assertEquals(3, health.totalReconnects());
        assertEquals(30, health.intervalSeconds());
        assertEquals(NOW, health.lastMessageAt());

        assertArrayEquals(new long[]{0, 0}, publisher.drainIntervalCounts(), "counters reset after each heartbeat");
        reporter.report();
        verify(kafka).send(eq(DarkFleetTopics.FEED_HEALTH), eq("AISSTREAM"), any(FeedHealth.class));
        reporter.close();
    }
}
