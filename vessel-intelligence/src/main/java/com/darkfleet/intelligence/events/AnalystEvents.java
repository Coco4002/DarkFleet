package com.darkfleet.intelligence.events;

import com.darkfleet.intelligence.web.ApiException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The analysts' live stream (server-sent events). Events go out only after their transaction commits,
 * so nobody is told about a change that was rolled back. A connection lasts at most
 * {@link #CONNECTION_LIFETIME}; browsers reconnect on their own. A heartbeat every 20 s keeps proxies
 * from closing idle connections. Events are written by one thread of their own, in order: a browser
 * that stops reading can hold up the stream, but never the Kafka consumer or the scheduler that
 * published the event. The stream is best effort: when {@link #MAX_QUEUED} events are waiting, new
 * ones are dropped (analysts can always list detections and proposals).
 */
@Component
public class AnalystEvents {

    private static final Logger log = LoggerFactory.getLogger(AnalystEvents.class);
    static final Duration CONNECTION_LIFETIME = Duration.ofMinutes(30);
    /** A handful of analysts is expected; this caps what a misbehaving client can hold open. */
    static final int MAX_CONNECTIONS = 50;

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    static final int MAX_QUEUED = 1000;

    private final ExecutorService sender = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(MAX_QUEUED), Thread.ofPlatform().name("analyst-events").daemon().factory(),
            (task, executor) -> log.warn("Analyst event stream backed up; dropping an event"));

    public SseEmitter connect() {
        if (emitters.size() >= MAX_CONNECTIONS) {
            throw ApiException.unavailable("Too many open event streams; try again later");
        }
        SseEmitter emitter = new SseEmitter(CONNECTION_LIFETIME.toMillis());
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        emitters.add(emitter);
        send(emitter, SseEmitter.event().comment("connected"));
        return emitter;
    }

    /** Runs immediately when published outside a transaction, after commit when inside one. */
    @TransactionalEventListener(fallbackExecution = true)
    public void publish(AnalystEvent event) {
        sender.execute(() -> {
            for (SseEmitter emitter : emitters) {
                send(emitter, SseEmitter.event().name(event.name()).data(event.data(), MediaType.APPLICATION_JSON));
            }
        });
    }

    @Scheduled(fixedRate = 20_000)
    void heartbeat() {
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

    int connections() {
        return emitters.size();
    }

    private void send(SseEmitter emitter, SseEmitter.SseEventBuilder event) {
        try {
            emitter.send(event);
        } catch (IOException | IllegalStateException e) {
            // the client went away; Spring completes the emitter, this just stops using it
            emitters.remove(emitter);
            log.debug("Dropped an analyst event stream: {}", e.getMessage());
        }
    }

}
