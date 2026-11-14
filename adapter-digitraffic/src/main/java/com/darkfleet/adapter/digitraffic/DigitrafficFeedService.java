package com.darkfleet.adapter.digitraffic;

import com.darkfleet.adapter.support.FeedConnection;
import com.darkfleet.adapter.support.FeedPublisher;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
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
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live Fintraffic Digitraffic feed over MQTT (WebSocket), with a REST snapshot for bootstrapping.
 * Source: Fintraffic / digitraffic.fi, licence CC BY 4.0.
 */
@Service
public class DigitrafficFeedService implements FeedConnection {

    private static final Logger log = LoggerFactory.getLogger(DigitrafficFeedService.class);
    public static final String ATTRIBUTION = "Fintraffic / digitraffic.fi (CC BY 4.0)";

    private final DigitrafficNormalizer normalizer;
    private final FeedPublisher publisher;
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor();
    private final String mqttUrl;
    private final boolean autoConnect;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean manualDisconnect = new AtomicBoolean(false);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong totalReconnects = new AtomicLong(0);
    private final AtomicLong framesReceived = new AtomicLong(0);
    private final AtomicLong framesIgnored = new AtomicLong(0);

    private volatile MqttClient mqttClient;

    public DigitrafficFeedService(DigitrafficNormalizer normalizer, FeedPublisher publisher,
                                  @Value("${darkfleet.digitraffic.mqtt-url}") String mqttUrl,
                                  @Value("${darkfleet.digitraffic.auto-connect}") boolean autoConnect) {
        this.normalizer = normalizer;
        this.publisher = publisher;
        this.mqttUrl = mqttUrl;
        this.autoConnect = autoConnect;
    }

    /** Starts after the application (and its Kafka topics) is ready. */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (autoConnect) {
            reconnectScheduler.execute(this::connectMqtt);
        } else {
            log.info("Digitraffic adapter in standby (auto-connect off).");
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
            String clientId = "darkfleet-" + UUID.randomUUID().toString().substring(0, 8);
            MqttClient client = new MqttClient(mqttUrl, clientId, new MemoryPersistence());
            client.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    if (client != mqttClient) {
                        return; // a replaced client
                    }
                    connected.set(true);
                    consecutiveFailures.set(0);
                    try {
                        client.subscribe("vessels-v2/+/location", 0);
                        client.subscribe("vessels-v2/+/metadata", 0);
                        log.info("Subscribed to Digitraffic MQTT (vessels-v2/+/location, vessels-v2/+/metadata).");
                    } catch (Exception e) {
                        log.warn("Could not subscribe to Digitraffic topics: {}", e.getMessage());
                    }
                }

                @Override
                public void connectionLost(Throwable cause) {
                    if (client != mqttClient) {
                        return; // a replaced client closing
                    }
                    connected.set(false);
                    scheduleReconnect("connection lost: " + (cause != null ? cause.getMessage() : "unknown"));
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    if (client != mqttClient) {
                        return;
                    }
                    handleMessage(topic, new String(message.getPayload(), StandardCharsets.UTF_8));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // subscribe-only client
                }
            });
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(false);
            options.setConnectionTimeout(10);
            options.setKeepAliveInterval(30);
            mqttClient = client;
            client.connect(options);
        } catch (Exception e) {
            connected.set(false);
            scheduleReconnect("connect failed: " + e.getMessage());
        }
    }

    /** Demo of a single-feed outage: closes the feed and stops reconnecting until {@link #reconnect()}. */
    public synchronized void simulateDisconnect() {
        manualDisconnect.set(true);
        connected.set(false);
        closeMqttClient();
        log.warn("Digitraffic feed disconnected on request (simulated outage).");
    }

    public synchronized void reconnect() {
        manualDisconnect.set(false);
        consecutiveFailures.set(0);
        reconnectScheduler.execute(this::connectMqtt);
    }

    void handleMessage(String topic, String payloadJson) {
        if (manualDisconnect.get()) {
            return;
        }
        framesReceived.incrementAndGet();
        Optional<NormalizedAisMessage> normalized = normalizer.normalizeMqttPayload(topic, payloadJson);
        if (normalized.isPresent()) {
            publisher.publish(normalized.get());
        } else {
            framesIgnored.incrementAndGet();
        }
    }

    public Map<String, Object> getStatus() {
        var stats = publisher.stats();
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("adapter", AisSource.DIGITRAFFIC.name());
        status.put("displayName", AisSource.DIGITRAFFIC.getDisplayName());
        status.put("attribution", ATTRIBUTION);
        status.put("connected", connected.get());
        status.put("manualDisconnectSimulated", manualDisconnect.get());
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
        log.warn("Digitraffic disconnected ({}). Reconnect attempt #{} in {} s.", reason, failures, backoffSeconds);
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
                // closing anyway
            }
        }
    }
}
