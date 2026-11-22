package com.darkfleet.gateway.live;

import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.web.ApiException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Public live stream (Server-Sent Events). Sends the current full snapshot of all active vessels
 * immediately on connect and every {@code darkfleet.gateway.snapshot-interval} (~5 s) thereafter,
 * plus a heartbeat comment every 20 s so reverse proxies keep the stream open.
 * <p>
 * The snapshot is written to JSON once per tick and shared. Each client is written to on its own
 * virtual thread, one event at a time: a tick that finds the client still busy is skipped for it
 * (the next snapshot replaces it anyway), and a client that misses {@link #MAX_MISSED_TICKS} ticks
 * in a row is disconnected, so a slow browser never holds up the others. A disconnected client counts
 * towards {@link #MAX_CONNECTIONS} until its connection has really closed (a stuck write ends at the
 * server's write timeout).
 */
@Component
public class LiveSnapshotStreamer {

    private static final Logger log = LoggerFactory.getLogger(LiveSnapshotStreamer.class);
    static final Duration CONNECTION_LIFETIME = Duration.ofMinutes(30);
    static final int MAX_CONNECTIONS = 200;
    static final int MAX_MISSED_TICKS = 3;

    private final LiveSnapshotStore store;
    private final JsonMapper json;
    private final int maxStreamsPerIp;
    private final Set<Client> clients = ConcurrentHashMap.newKeySet();
    /** Open streams per client address; reserved and released inside compute, so the count can't be lost. */
    private final Map<String, Integer> activeByIp = new ConcurrentHashMap<>();
    private final ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();

    public LiveSnapshotStreamer(LiveSnapshotStore store, JsonMapper json, GatewayProperties properties) {
        this.store = store;
        this.json = json;
        this.maxStreamsPerIp = properties.maxLiveStreamsPerIp();
    }

    public SseEmitter connect(String clientIp) {
        if (clients.size() >= MAX_CONNECTIONS) {
            throw ApiException.unavailable("Too many open live streams; try again later");
        }
        boolean[] admitted = {false};
        activeByIp.compute(clientIp, (ip, open) -> {
            int count = open == null ? 0 : open;
            if (count >= maxStreamsPerIp) {
                return open;
            }
            admitted[0] = true;
            return count + 1;
        });
        if (!admitted[0]) {
            throw ApiException.tooManyRequests(
                    "Too many open live streams from this address; close another tab and try again");
        }
        Client client = new Client(new SseEmitter(CONNECTION_LIFETIME.toMillis()), clientIp);
        Runnable cleanup = () -> removeClient(client);
        client.emitter.onCompletion(cleanup);
        client.emitter.onTimeout(cleanup);
        client.emitter.onError(e -> cleanup.run());
        clients.add(client);
        String snapshot = snapshotJson();
        client.offer(() -> SseEmitter.event().name("snapshot").data(snapshot));
        return client.emitter;
    }

    private void removeClient(Client client) {
        if (clients.remove(client)) {
            activeByIp.computeIfPresent(client.ipKey, (ip, open) -> open <= 1 ? null : open - 1);
        }
    }

    @Scheduled(fixedRateString = "${darkfleet.gateway.snapshot-interval}")
    public void broadcastSnapshot() {
        if (clients.isEmpty()) {
            return;
        }
        String snapshot = snapshotJson();
        clients.forEach(client -> client.offer(() -> SseEmitter.event().name("snapshot").data(snapshot)));
    }

    @Scheduled(fixedRate = 20_000)
    public void heartbeat() {
        clients.forEach(client -> client.offer(() -> SseEmitter.event().comment("heartbeat")));
    }

    @PreDestroy
    void stop() {
        senders.shutdownNow();
    }

    public int connections() {
        return clients.size();
    }

    private String snapshotJson() {
        return json.writeValueAsString(store.snapshot());
    }

    /** Stops serving a client. Completing waits for a write in progress, so it runs off the caller's thread. */
    private void drop(Client client) {
        if (client.closing.compareAndSet(false, true)) {
            senders.execute(client.emitter::complete);
        }
    }

    private final class Client {

        final SseEmitter emitter;
        final String ipKey;
        final AtomicBoolean sending = new AtomicBoolean();
        final AtomicBoolean closing = new AtomicBoolean();
        final AtomicInteger missed = new AtomicInteger();

        Client(SseEmitter emitter, String ipKey) {
            this.emitter = emitter;
            this.ipKey = ipKey;
        }

        void offer(Supplier<SseEmitter.SseEventBuilder> event) {
            if (closing.get()) {
                return;
            }
            if (!sending.compareAndSet(false, true)) {
                if (missed.incrementAndGet() >= MAX_MISSED_TICKS) {
                    log.debug("Dropping a live stream that fell {} ticks behind", MAX_MISSED_TICKS);
                    drop(this);
                }
                return;
            }
            missed.set(0);
            senders.execute(() -> {
                try {
                    emitter.send(event.get());
                } catch (IOException | IllegalStateException e) {
                    drop(this);
                } finally {
                    sending.set(false);
                }
            });
        }
    }
}
