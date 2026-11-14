package com.darkfleet.adapter.aisstream;

import com.darkfleet.adapter.support.FeedConnection;
import com.darkfleet.adapter.support.FeedPublisher;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Live aisstream.io WebSocket feed: subscribes to the configured areas, normalizes frames and
 * publishes them; reconnects with exponential backoff (up to 60 s).
 */
@Service
public class AisStreamFeedService implements FeedConnection {

    private static final Logger log = LoggerFactory.getLogger(AisStreamFeedService.class);

    private final AisStreamProperties properties;
    private final AisStreamNormalizer normalizer;
    private final FeedPublisher publisher;
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor();
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean manualDisconnect = new AtomicBoolean(false);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong totalReconnects = new AtomicLong(0);
    private final AtomicLong framesReceived = new AtomicLong(0);
    private final AtomicLong framesIgnored = new AtomicLong(0);
    private final AtomicReference<WebSocket> activeWebSocket = new AtomicReference<>(null);
    /** Increases with every connection attempt and every disconnect; callbacks from older attempts are ignored. */
    private final AtomicLong attempt = new AtomicLong(0);
    private final AtomicBoolean connecting = new AtomicBoolean(false);

    public AisStreamFeedService(AisStreamProperties properties, AisStreamNormalizer normalizer, FeedPublisher publisher) {
        this.properties = properties;
        this.normalizer = normalizer;
        this.publisher = publisher;
    }

    /** Starts after the application (and its Kafka topics) is ready. */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (properties.autoConnect() && properties.hasApiKey()) {
            connectAsync();
        } else {
            log.info("aisstream.io adapter in standby (no API key or auto-connect off).");
        }
    }

    @PreDestroy
    public synchronized void shutdown() {
        manualDisconnect.set(true);
        attempt.incrementAndGet();
        closeActiveSocket();
        reconnectScheduler.shutdownNow();
    }

    public synchronized void connectAsync() {
        if (manualDisconnect.get() || connected.get() || connecting.get()) {
            return;
        }
        if (!properties.hasApiKey()) {
            log.warn("Cannot connect to aisstream.io without an API key.");
            return;
        }
        long thisAttempt = attempt.incrementAndGet();
        connecting.set(true);
        log.info("Connecting to aisstream.io at {} ...", properties.wsUrl());
        try {
            httpClient.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .buildAsync(URI.create(properties.wsUrl()), new AisStreamListener(thisAttempt))
                    .whenComplete((ws, error) -> onHandshake(thisAttempt, ws, error));
        } catch (RuntimeException e) {
            connecting.set(false);
            scheduleReconnect("could not start connection: " + e.getMessage());
        }
    }

    private synchronized void onHandshake(long thisAttempt, WebSocket ws, Throwable error) {
        if (thisAttempt != attempt.get() || manualDisconnect.get()) {
            // Disconnected (or superseded) while the handshake was in flight: don't install this socket.
            if (ws != null) {
                ws.abort();
            }
            return;
        }
        connecting.set(false);
        if (error != null) {
            connected.set(false);
            scheduleReconnect("handshake failed: " + error.getMessage());
            return;
        }
        activeWebSocket.set(ws);
        connected.set(true);
        consecutiveFailures.set(0);
        ws.sendText(buildSubscriptionPayload(), true);
        log.info("Subscribed to aisstream.io for {} area(s).", properties.boundingBoxes().size());
    }

    private synchronized void onConnectionLost(long thisAttempt, WebSocket ws, String reason) {
        if (thisAttempt != attempt.get()) {
            return; // an old socket closing after a disconnect or reconnect
        }
        connected.set(false);
        connecting.set(false);
        activeWebSocket.compareAndSet(ws, null);
        scheduleReconnect(reason);
    }

    /** Demo of a single-feed outage: closes the feed and stops reconnecting until {@link #reconnect()}. */
    public synchronized void simulateDisconnect() {
        manualDisconnect.set(true);
        attempt.incrementAndGet();
        connecting.set(false);
        connected.set(false);
        closeActiveSocket();
        log.warn("aisstream.io feed disconnected on request (simulated outage).");
    }

    public synchronized void reconnect() {
        manualDisconnect.set(false);
        consecutiveFailures.set(0);
        connectAsync();
    }

    void handleFrame(String rawJson) {
        if (manualDisconnect.get()) {
            return;
        }
        framesReceived.incrementAndGet();
        Optional<NormalizedAisMessage> normalized = normalizer.normalize(rawJson);
        if (normalized.isPresent()) {
            publisher.publish(normalized.get());
        } else {
            framesIgnored.incrementAndGet();
        }
    }

    String buildSubscriptionPayload() {
        // aisstream expects each box as [[lat1, lon1], [lat2, lon2]]
        List<List<List<Double>>> boxes = properties.boundingBoxes().stream()
                .map(b -> List.of(List.of(b.get(0), b.get(1)), List.of(b.get(2), b.get(3))))
                .toList();
        Map<String, Object> subscription = new LinkedHashMap<>();
        subscription.put("APIKey", properties.apiKey());
        subscription.put("BoundingBoxes", boxes);
        subscription.put("FilterMessageTypes", List.of("PositionReport", "StandardClassBPositionReport", "ShipStaticData"));
        return JsonSerde.mapper().writeValueAsString(subscription);
    }

    public Map<String, Object> getStatus() {
        var stats = publisher.stats();
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("adapter", AisSource.AISSTREAM.name());
        status.put("displayName", AisSource.AISSTREAM.getDisplayName());
        status.put("connected", connected.get());
        status.put("manualDisconnectSimulated", manualDisconnect.get());
        status.put("apiKeyConfigured", properties.hasApiKey());
        status.put("boundingBoxes", properties.boundingBoxes());
        status.put("totalReconnects", totalReconnects.get());
        status.put("framesReceived", framesReceived.get());
        status.put("framesIgnored", framesIgnored.get());
        status.put("messagesReceived", stats.received());
        status.put("duplicatesDropped", stats.duplicates());
        status.put("messagesPublished", stats.published());
        status.put("sendFailures", stats.sendFailures());
        status.put("lastMessageAt", stats.lastMessageAt() != null ? stats.lastMessageAt().toString() : "NONE");
        return status;
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public long totalReconnects() {
        return totalReconnects.get();
    }

    private void scheduleReconnect(String reason) {
        if (manualDisconnect.get() || reconnectScheduler.isShutdown()) {
            return;
        }
        int failures = consecutiveFailures.incrementAndGet();
        totalReconnects.incrementAndGet();
        long backoffSeconds = Math.min(60L, 1L << Math.min(failures, 6));
        log.warn("aisstream.io disconnected ({}). Reconnect attempt #{} in {} s.", reason, failures, backoffSeconds);
        reconnectScheduler.schedule(this::connectAsync, backoffSeconds, TimeUnit.SECONDS);
    }

    private void closeActiveSocket() {
        WebSocket ws = activeWebSocket.getAndSet(null);
        if (ws != null) {
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "adapter disconnect");
            } catch (RuntimeException ignored) {
                // closing anyway
            }
        }
    }

    private final class AisStreamListener implements WebSocket.Listener {
        private final long listenerAttempt;
        private final StringBuilder buffer = new StringBuilder();

        AisStreamListener(long listenerAttempt) {
            this.listenerAttempt = listenerAttempt;
        }

        private boolean current() {
            return listenerAttempt == attempt.get();
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String frame = buffer.toString();
                buffer.setLength(0);
                if (current()) {
                    handleFrame(frame);
                }
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            buffer.append(StandardCharsets.UTF_8.decode(data));
            if (last) {
                String frame = buffer.toString();
                buffer.setLength(0);
                if (current()) {
                    handleFrame(frame);
                }
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            onConnectionLost(listenerAttempt, webSocket, "server closed the connection (" + statusCode + ": " + reason + ")");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            onConnectionLost(listenerAttempt, webSocket, "connection error: " + error.getMessage());
        }
    }
}
