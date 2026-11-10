package com.darkfleet.adapter.digitraffic;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fintraffic Digitraffic MQTT over WebSocket + REST feed client.
 * Attribution: Traffic data provided by Fintraffic / digitraffic.fi (CC BY 4.0).
 */
@Service
public class DigitrafficFeedService {

    private static final Logger log = LoggerFactory.getLogger(DigitrafficFeedService.class);
    public static final String ATTRIBUTION = "Fintraffic / digitraffic.fi (CC BY 4.0)";

    private final DigitrafficNormalizer normalizer;
    private final KafkaTemplate<String, NormalizedAisMessage> kafkaTemplate;
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Value("${darkfleet.digitraffic.mqtt-url:wss://meri.digitraffic.fi:443/mqtt}")
    private String mqttUrl;

    @Value("${darkfleet.digitraffic.rest-locations-url:https://meri.digitraffic.fi/api/ais/v1/locations}")
    private String restLocationsUrl;

    @Value("${darkfleet.digitraffic.auto-connect:false}")
    private boolean autoConnect;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean manualDisconnect = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong duplicatesDropped = new AtomicLong(0);
    private final AtomicReference<Instant> lastMessageAt = new AtomicReference<>(null);
    private final Map<String, Instant> dedupCache = new ConcurrentHashMap<>();

    private volatile MqttClient mqttClient;

    public DigitrafficFeedService(
            DigitrafficNormalizer normalizer,
            KafkaTemplate<String, NormalizedAisMessage> kafkaTemplate
    ) {
        this.normalizer = normalizer;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void init() {
        if (autoConnect) {
            reconnectScheduler.execute(this::connectMqtt);
        } else {
            log.info("Fintraffic Digitraffic adapter initialized in standby mode (Attribution: {}).", ATTRIBUTION);
        }
    }

    @PreDestroy
    public void shutdown() {
        manualDisconnect.set(true);
        closeMqttClient();
        reconnectScheduler.shutdownNow();
    }

    public synchronized void connectMqtt() {
        if (manualDisconnect.get() || connected.get()) {
            return;
        }
        try {
            closeMqttClient();
            String clientId = "darkfleet-digitraffic-" + UUID.randomUUID().toString().substring(0, 8);
            mqttClient = new MqttClient(mqttUrl, clientId, new MemoryPersistence());
            mqttClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    connected.set(true);
                    reconnectAttempts.set(0);
                    try {
                        mqttClient.subscribe("vessels-v2/+/location", 0);
                        mqttClient.subscribe("vessels-v2/+/metadata", 0);
                        log.info("Subscribed to Fintraffic Digitraffic MQTT topics (vessels-v2/+/location, vessels-v2/+/metadata).");
                    } catch (Exception e) {
                        log.warn("Failed to subscribe to Digitraffic topics: {}", e.getMessage());
                    }
                }

                @Override
                public void connectionLost(Throwable cause) {
                    connected.set(false);
                    scheduleReconnect("MQTT connection lost: " + (cause != null ? cause.getMessage() : "unknown"));
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                    handleMqttMessage(topic, payload);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(false);
            options.setConnectionTimeout(10);
            options.setKeepAliveInterval(30);
            mqttClient.connect(options);
        } catch (Exception e) {
            connected.set(false);
            scheduleReconnect("MQTT connect failure: " + e.getMessage());
        }
    }

    public synchronized void simulateDisconnect() {
        manualDisconnect.set(true);
        connected.set(false);
        closeMqttClient();
        log.warn("Fintraffic Digitraffic feed manually disconnected (simulating single-feed outage).");
    }

    public synchronized void reconnect() {
        manualDisconnect.set(false);
        reconnectAttempts.set(0);
        reconnectScheduler.execute(this::connectMqtt);
    }

    public Optional<NormalizedAisMessage> handleMqttMessage(String topic, String payloadJson) {
        if (manualDisconnect.get()) {
            return Optional.empty();
        }
        messagesReceived.incrementAndGet();
        Optional<NormalizedAisMessage> normalized = normalizer.normalizeMqttPayload(topic, payloadJson);
        normalized.ifPresent(this::publishNormalized);
        return normalized;
    }

    public int pollRestLocationsSnapshot() {
        if (manualDisconnect.get()) {
            return 0;
        }
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(restLocationsUrl))
                    .header("Digitraffic-User", "DarkFleet-UniversityProject/1.0")
                    .header("Accept", "application/json")
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0;
            }
            JsonNode root = JsonSerde.mapper().readTree(response.body());
            JsonNode features = root.path("features");
            if (!features.isArray()) {
                return 0;
            }
            int published = 0;
            for (JsonNode feature : features) {
                Optional<NormalizedAisMessage> msg = normalizer.normalizeGeoJsonFeature(feature);
                if (msg.isPresent() && publishNormalized(msg.get())) {
                    published++;
                }
            }
            return published;
        } catch (Exception e) {
            log.warn("Digitraffic REST snapshot fetch failed: {}", e.getMessage());
            return 0;
        }
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
            log.debug("Kafka send deferred/logged for Digitraffic MMSI {}: {}", message.mmsi(), e.getMessage());
        }
        return true;
    }

    public Map<String, Object> getStatus() {
        return Map.of(
                "adapter", AisSource.DIGITRAFFIC.name(),
                "displayName", AisSource.DIGITRAFFIC.getDisplayName(),
                "attribution", ATTRIBUTION,
                "connected", connected.get(),
                "manualDisconnectSimulated", manualDisconnect.get(),
                "reconnectAttempts", reconnectAttempts.get(),
                "messagesReceived", messagesReceived.get(),
                "messagesPublished", messagesPublished.get(),
                "duplicatesDropped", duplicatesDropped.get(),
                "lastMessageAt", lastMessageAt.get() != null ? lastMessageAt.get().toString() : "NONE"
        );
    }

    private void scheduleReconnect(String reason) {
        if (manualDisconnect.get()) {
            return;
        }
        int attempt = reconnectAttempts.incrementAndGet();
        long backoffSeconds = Math.min(60L, (1L << Math.min(attempt, 6)));
        log.warn("Digitraffic disconnected ({}). Reconnect attempt #{} in {}s", reason, attempt, backoffSeconds);
        reconnectScheduler.schedule(this::connectMqtt, backoffSeconds, TimeUnit.SECONDS);
    }

    private void closeMqttClient() {
        MqttClient client = this.mqttClient;
        this.mqttClient = null;
        if (client != null) {
            try {
                if (client.isConnected()) {
                    client.disconnectForcibly(1000, 1000);
                }
                client.close();
            } catch (Exception ignored) {
            }
        }
    }
}
