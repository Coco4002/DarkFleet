package com.darkfleet.gateway.live;

import com.darkfleet.gateway.live.LiveSnapshotStore.LiveSnapshot;
import com.darkfleet.gateway.web.ApiException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Public live stream (Server-Sent Events). Sends the current full snapshot of all active vessels
 * immediately on connect and every {@code darkfleet.gateway.snapshot-interval} (~5 s) thereafter,
 * plus a heartbeat comment every 20 s so reverse proxies keep the stream open.
 */
@Component
public class LiveSnapshotStreamer {

    private static final Logger log = LoggerFactory.getLogger(LiveSnapshotStreamer.class);
    static final Duration CONNECTION_LIFETIME = Duration.ofMinutes(30);
    static final int MAX_CONNECTIONS = 200;
    static final int MAX_QUEUED = 64;

    private final LiveSnapshotStore store;
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final ExecutorService sender = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(MAX_QUEUED), Thread.ofPlatform().name("live-snapshot-sse").daemon().factory(),
            (task, executor) -> log.warn("Live snapshot stream backed up; dropping a tick"));

    public LiveSnapshotStreamer(LiveSnapshotStore store) {
        this.store = store;
    }

    public SseEmitter connect() {
        if (emitters.size() >= MAX_CONNECTIONS) {
            throw ApiException.unavailable("Too many open live streams; try again later");
        }
        SseEmitter emitter = new SseEmitter(CONNECTION_LIFETIME.toMillis());
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        emitters.add(emitter);
        LiveSnapshot initial = store.snapshot();
        sender.execute(() -> {
            send(emitter, SseEmitter.event().comment("connected"));
            send(emitter, SseEmitter.event().name("snapshot").data(initial, MediaType.APPLICATION_JSON));
        });
        return emitter;
    }

    @Scheduled(fixedRateString = "${darkfleet.gateway.snapshot-interval}")
    public void broadcastSnapshot() {
        if (emitters.isEmpty()) {
            return;
        }
        LiveSnapshot snapshot = store.snapshot();
        sender.execute(() -> {
            for (SseEmitter emitter : emitters) {
                send(emitter, SseEmitter.event().name("snapshot").data(snapshot, MediaType.APPLICATION_JSON));
            }
        });
    }

    @Scheduled(fixedRate = 20_000)
    public void heartbeat() {
        if (emitters.isEmpty()) {
            return;
        }
        sender.execute(() -> {
            for (SseEmitter emitter : emitters) {
                send(emitter, SseEmitter.event().comment("heartbeat"));
            }
        });
    }

    @PreDestroy
    void stop() {
        sender.shutdownNow();
    }

    public int connections() {
        return emitters.size();
    }

    private void send(SseEmitter emitter, SseEmitter.SseEventBuilder event) {
        try {
            emitter.send(event);
        } catch (IOException | IllegalStateException e) {
            emitters.remove(emitter);
            log.debug("Dropped a live snapshot stream: {}", e.getMessage());
        }
    }
}
