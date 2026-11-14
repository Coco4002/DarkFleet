package com.darkfleet.adapter.aisstream;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class AisStreamFeedService {

    private static final Logger log = LoggerFactory.getLogger(AisStreamFeedService.class);

    private final AisStreamNormalizer normalizer;
    private final KafkaTemplate<String, NormalizedAisMessage> kafkaTemplate;
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Value("${darkfleet.aisstream.ws-url:wss://stream.aisstream.io/v0/stream}")
    private String wsUrl;

    @Value("${darkfleet.aisstream.api-key:}")
    private String apiKey;

    @Value("${darkfleet.aisstream.auto-connect:true}")
    private boolean autoConnect;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean manualDisconnect = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong duplicatesDropped = new AtomicLong(0);
    private final AtomicReference<Instant> lastMessageAt = new AtomicReference<>(null);
    private final AtomicReference<WebSocket> activeWebSocket = new AtomicReference<>(null);

    private final Map<String, Instant> dedupCache = new ConcurrentHashMap<>();

    public AisStreamFeedService(
            AisStreamNormalizer normalizer,
            KafkaTemplate<String, NormalizedAisMessage> kafkaTemplate
    ) {
        this.normalizer = normalizer;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void init() {
        if (autoConnect && apiKey != null && !apiKey.isBlank()) {
            connectAsync();
        } else {
            log.info("aisstream.io adapter initialized in standby mode (set DARKFLEET_AISSTREAM_API_KEY for live WebSocket).");
        }
    }

    @PreDestroy
    public void shutdown() {
        manualDisconnect.set(true);
        closeActiveSocket();
        reconnectScheduler.shutdownNow();
    }

    public synchronized void connectAsync() {
        manualDisconnect.set(false);
        if (connected.get()) {
            return;
        }
        if (apiKey == null || apiKey.isBlank()) {
            log.warn("Cannot open live WebSocket to aisstream.io without an API key; adapter remains ready for REST/replay ingestion.");
            return;
        }
        try {
            log.info("Connecting to aisstream.io WebSocket at {}...", wsUrl);
            httpClient.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .buildAsync(URI.create(wsUrl), new AisStreamListener())
                    .whenComplete((ws, error) -> {
                        if (error != null) {
                            connected.set(false);
                            scheduleReconnect("WebSocket handshake error: " + error.getMessage());
                        } else {
                            activeWebSocket.set(ws);
                            connected.set(true);
                            reconnectAttempts.set(0);
                            sendSubscription(ws);
                        }
                    });
        } catch (Exception e) {
            scheduleReconnect("Exception initiating connection: " + e.getMessage());
        }
    }

    public synchronized void simulateDisconnect() {
        manualDisconnect.set(true);
        connected.set(false);
        closeActiveSocket();
        log.warn("aisstream.io feed manually disconnected (simulating single-feed outage).");
    }

    public synchronized void reconnect() {
        manualDisconnect.set(false);
        reconnectAttempts.set(0);
        connectAsync();
    }

    /**
     * Processes a raw aisstream.io JSON frame, normalizes it, deduplicates, and publishes to Kafka.
     */
    public Optional<NormalizedAisMessage> handleRawFrame(String rawJson) {
        if (manualDisconnect.get()) {
            return Optional.empty();
        }
        messagesReceived.incrementAndGet();
        Optional<NormalizedAisMessage> normalizedOpt = normalizer.normalize(rawJson);
        normalizedOpt.ifPresent(this::publishNormalized);
        return normalizedOpt;
    }

    public boolean publishNormalized(NormalizedAisMessage message) {
        if (manualDisconnect.get()) {
            return false;
        }
        String dedupKey = message.dedupKey();
        Instant now = Instant.now();
        if (dedupCache.size() > 25_000) {
            dedupCache.entrySet().removeIf(e -> Duration.between(e.getValue(), now).toMinutes() > 15);
        }
        if (dedupCache.putIfAbsent(dedupKey, now) != null) {
            duplicatesDropped.incrementAndGet();
            return false;
        }

        lastMessageAt.set(now);
        messagesPublished.incrementAndGet();
        try {
            kafkaTemplate.send(DarkFleetTopics.AIS_RAW, message.mmsi(), message);
        } catch (Exception e) {
            log.debug("Kafka send deferred/logged for MMSI {}: {}", message.mmsi(), e.getMessage());
        }
        return true;
    }

    public Map<String, Object> getStatus() {
        return Map.of(
                "adapter", AisSource.AISSTREAM.name(),
                "displayName", AisSource.AISSTREAM.getDisplayName(),
                "connected", connected.get(),
                "manualDisconnectSimulated", manualDisconnect.get(),
                "apiKeyConfigured", apiKey != null && !apiKey.isBlank(),
                "reconnectAttempts", reconnectAttempts.get(),
                "messagesReceived", messagesReceived.get(),
                "messagesPublished", messagesPublished.get(),
                "duplicatesDropped", duplicatesDropped.get(),
                "lastMessageAt", lastMessageAt.get() != null ? lastMessageAt.get().toString() : "NONE"
        );
    }

    public String buildSubscriptionPayload() {
        try {
            Map<String, Object> subscription = Map.of(
                    "APIKey", apiKey != null ? apiKey : "",
                    "BoundingBoxes", List.of(
                            // Gulf of Finland to the Danish straits
                            List.of(List.of(53.5, 9.0), List.of(60.8, 30.5))
                    ),
                    "FilterMessageTypes", List.of(
                            "PositionReport",
                            "StandardClassBPositionReport",
                            "ShipStaticData"
                    )
            );
            return JsonSerde.mapper().writeValueAsString(subscription);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build aisstream subscription JSON", e);
        }
    }

    private void sendSubscription(WebSocket ws) {
        String payload = buildSubscriptionPayload();
        ws.sendText(payload, true);
        log.info("Subscribed to aisstream.io for the Baltic (Gulf of Finland -> Danish straits).");
    }

    private void scheduleReconnect(String reason) {
        if (manualDisconnect.get()) {
            return;
        }
        int attempt = reconnectAttempts.incrementAndGet();
        long backoffSeconds = Math.min(60L, (1L << Math.min(attempt, 6)));
        log.warn("aisstream.io disconnected ({}). Scheduling reconnect attempt #{} in {}s", reason, attempt, backoffSeconds);
        reconnectScheduler.schedule(this::connectAsync, backoffSeconds, TimeUnit.SECONDS);
    }

    private void closeActiveSocket() {
        WebSocket ws = activeWebSocket.getAndSet(null);
        if (ws != null) {
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "Adapter shutdown or disconnect");
            } catch (Exception ignored) {
            }
        }
    }

    private final class AisStreamListener implements WebSocket.Listener {
        private final StringBuilder buffer = new StringBuilder();

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
                handleRawFrame(frame);
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
                handleRawFrame(frame);
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            connected.set(false);
            scheduleReconnect("Server closed WebSocket (" + statusCode + ": " + reason + ")");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            connected.set(false);
            scheduleReconnect("WebSocket error: " + error.getMessage());
        }
    }
}
