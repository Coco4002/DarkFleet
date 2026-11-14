package com.darkfleet.adapter.replay;

import com.darkfleet.adapter.support.FeedPublisher;
import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Replays one DMA file at a time into {@code ais.replay}. Files are read from the configured data
 * directory only (by plain file name, no symbolic links), streamed line by line, as .csv or as the
 * .zip DMA publishes. Each run has its own publisher, so repeat filtering and counters are per run,
 * and a run only counts as finished once Kafka has acknowledged every message.
 */
@Service
public class ReplayRunService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReplayRunService.class);
    private static final Pattern SAFE_FILE_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*\\.(csv|zip)");
    private static final Duration SETTLE_TIMEOUT = Duration.ofMinutes(2);
    /** More distinct vessels than a DMA day file has (about 20,000); beyond this the file is not what we expect. */
    static final int MAX_VESSELS_BEFORE_WINDOW = 200_000;

    public enum State { IDLE, RUNNING, FINISHED, CANCELLED, FAILED }

    /**
     * @param file        file name inside the data directory, e.g. aisdk-2026-11-11.zip
     * @param from        optional: skip messages before this time
     * @param to          optional: stop at messages after this time
     * @param speed       optional: replay speed relative to real time (60 = one hour per minute);
     *                    empty or 0 = as fast as possible
     * @param boundingBox optional [minLat, minLon, maxLat, maxLon]; positions outside are skipped
     */
    public record ReplayRequest(String file, Instant from, Instant to, Double speed, List<Double> boundingBox) {
    }

    /**
     * @param messagesSent messages handed to Kafka in this run
     * @param published    of those, acknowledged by Kafka
     * @param sendFailures of those, failed
     * @param duplicates   exact repeats dropped in this run
     */
    public record RunStatus(State state, String file, Instant startedAt, Instant finishedAt, long rowsRead,
                            long messagesSent, long published, long sendFailures, long duplicates,
                            Instant firstEventTime, Instant lastEventTime, String error) {
    }

    private final KafkaTemplate<String, Object> kafka;
    private final Path dataDir;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "replay"));
    private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
    private final AtomicLong rowsRead = new AtomicLong();

    private volatile FeedPublisher runPublisher;
    private volatile State state = State.IDLE;
    private volatile String file;
    private volatile Instant startedAt;
    private volatile Instant finishedAt;
    private volatile Instant firstEventTime;
    private volatile Instant lastEventTime;
    private volatile String error;

    public ReplayRunService(KafkaTemplate<String, Object> darkFleetKafkaTemplate,
                            @Value("${darkfleet.replay.data-dir}") Path dataDir) {
        this.kafka = darkFleetKafkaTemplate;
        this.dataDir = dataDir.toAbsolutePath().normalize();
    }

    public List<String> listFiles() throws IOException {
        if (!Files.isDirectory(dataDir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(dataDir)) {
            return files.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
                    .map(p -> p.getFileName().toString())
                    .filter(name -> SAFE_FILE_NAME.matcher(name).matches())
                    .sorted()
                    .toList();
        }
    }

    public synchronized RunStatus start(ReplayRequest request) {
        if (state == State.RUNNING) {
            throw new IllegalStateException("A replay is already running: " + file);
        }
        Path path = resolve(request.file());
        List<Double> box = request.boundingBox();
        if (box != null && !box.isEmpty() && (box.size() != 4 || box.get(0) >= box.get(2) || box.get(1) >= box.get(3))) {
            throw new IllegalArgumentException("boundingBox must be [minLat, minLon, maxLat, maxLon]");
        }
        if (request.speed() != null && request.speed() < 0) {
            throw new IllegalArgumentException("speed must not be negative");
        }
        if (request.from() != null && request.to() != null && request.from().isAfter(request.to())) {
            throw new IllegalArgumentException("from must not be after to");
        }
        cancelRequested.set(false);
        rowsRead.set(0);
        runPublisher = new FeedPublisher(AisSource.DMA_REPLAY, DarkFleetTopics.AIS_REPLAY, kafka);
        state = State.RUNNING;
        file = request.file();
        startedAt = Instant.now();
        finishedAt = null;
        firstEventTime = null;
        lastEventTime = null;
        error = null;
        FeedPublisher publisher = runPublisher;
        executor.execute(() -> run(path, request, publisher));
        return status();
    }

    public void cancel() {
        cancelRequested.set(true);
    }

    public RunStatus status() {
        FeedPublisher publisher = runPublisher;
        var stats = publisher != null ? publisher.stats() : null;
        return new RunStatus(state, file, startedAt, finishedAt, rowsRead.get(),
                stats != null ? stats.sent() : 0, stats != null ? stats.published() : 0,
                stats != null ? stats.sendFailures() : 0, stats != null ? stats.duplicates() : 0,
                firstEventTime, lastEventTime, error);
    }

    /** Resolves a plain file name to a regular file (not a link) inside the data directory. */
    Path resolve(String name) {
        if (name == null || !SAFE_FILE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("file must be a plain .csv or .zip file name");
        }
        Path path = dataDir.resolve(name).normalize();
        try {
            if (!path.startsWith(dataDir) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                    || !path.toRealPath().startsWith(dataDir.toRealPath())) {
                throw new IllegalArgumentException("No such replay file: " + name);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("No such replay file: " + name);
        }
        return path;
    }

    private void run(Path path, ReplayRequest request, FeedPublisher publisher) {
        log.info("Replay of {} started.", path.getFileName());
        State result;
        String failure = null;
        try (InputStream in = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS);
             BufferedReader reader = openCsv(in, path)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("empty file");
            }
            DmaCsvParser parser = DmaCsvParser.fromHeader(header);
            Pacer pacer = new Pacer(request.speed());
            // Latest static report of each vessel seen before the window, sent with its first message
            // inside the window so the vessel keeps its identity.
            Map<String, NormalizedAisMessage> identityBeforeWindow = new HashMap<>();
            String line;
            boolean pastWindow = false;
            rows:
            while (!pastWindow && !cancelRequested.get() && (line = reader.readLine()) != null) {
                rowsRead.incrementAndGet();
                for (NormalizedAisMessage msg : parser.parse(line)) {
                    Instant t = msg.eventTime();
                    if (request.from() != null && t.isBefore(request.from())) {
                        if (msg.messageType() == AisMessageType.STATIC_VOYAGE) {
                            identityBeforeWindow.put(msg.mmsi(), msg);
                            if (identityBeforeWindow.size() > MAX_VESSELS_BEFORE_WINDOW) {
                                throw new IOException("More than " + MAX_VESSELS_BEFORE_WINDOW
                                        + " vessels before the time window; refusing to drop their identities");
                            }
                        }
                        continue;
                    }
                    if (request.to() != null && t.isAfter(request.to())) {
                        pastWindow = true; // DMA files are in time order: nothing later is wanted
                        continue rows;
                    }
                    if (!inBox(msg, request.boundingBox())) {
                        continue;
                    }
                    pacer.waitUntil(t, cancelRequested);
                    if (cancelRequested.get()) {
                        break rows;
                    }
                    NormalizedAisMessage identity = identityBeforeWindow.remove(msg.mmsi());
                    if (identity != null && msg.messageType() == AisMessageType.POSITION_REPORT) {
                        publisher.publish(identity);
                    }
                    if (firstEventTime == null) {
                        firstEventTime = t;
                    }
                    lastEventTime = t;
                    publisher.publish(msg);
                }
            }
            if (!publisher.awaitSettled(SETTLE_TIMEOUT)) {
                throw new IOException("Kafka did not confirm all messages within " + SETTLE_TIMEOUT.toMinutes() + " min");
            }
            long failures = publisher.stats().sendFailures();
            if (failures > 0) {
                throw new IOException(failures + " messages could not be sent to Kafka");
            }
            result = cancelRequested.get() ? State.CANCELLED : State.FINISHED;
            log.info("Replay of {} {}: {} rows, {} messages published ({} not vessels, {} invalid).",
                    path.getFileName(), result.name().toLowerCase(Locale.ROOT), rowsRead.get(),
                    publisher.stats().published(), parser.skippedNotVessel(), parser.skippedInvalid());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result = State.CANCELLED;
        } catch (Exception e) {
            failure = e.getMessage();
            result = State.FAILED;
            log.warn("Replay of {} failed: {}", path.getFileName(), e.toString());
        }
        // Only now, with the file closed, does the run become finished, so a new run can't start
        // while this one is still winding down.
        finish(result, failure);
    }

    private synchronized void finish(State result, String failure) {
        error = failure;
        finishedAt = Instant.now();
        state = result;
    }

    private static BufferedReader openCsv(InputStream in, Path path) throws IOException {
        if (path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".zip")) {
            ZipInputStream zip = new ZipInputStream(in);
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
                    return new BufferedReader(new InputStreamReader(zip, StandardCharsets.UTF_8), 1 << 16);
                }
            }
            throw new IOException("No .csv file inside " + path.getFileName());
        }
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16);
    }

    private static boolean inBox(NormalizedAisMessage msg, List<Double> box) {
        if (box == null || box.isEmpty() || msg.messageType() != AisMessageType.POSITION_REPORT) {
            return true;
        }
        double lat = msg.latitude();
        double lon = msg.longitude();
        return lat >= box.get(0) && lat <= box.get(2) && lon >= box.get(1) && lon <= box.get(3);
    }

    @Override
    public void close() {
        cancelRequested.set(true);
        executor.shutdownNow();
    }

    /** Spaces messages out so the recording plays at the requested speed. */
    static final class Pacer {
        private final double speed;
        private Instant firstEvent;
        private long wallStartNanos;

        Pacer(Double speed) {
            this.speed = speed == null ? 0 : speed;
        }

        void waitUntil(Instant eventTime, AtomicBoolean cancel) throws InterruptedException {
            if (speed <= 0) {
                return;
            }
            if (firstEvent == null) {
                firstEvent = eventTime;
                wallStartNanos = System.nanoTime();
                return;
            }
            long dueNanos = wallStartNanos + (long) (Duration.between(firstEvent, eventTime).toNanos() / speed);
            long waitNanos;
            while ((waitNanos = dueNanos - System.nanoTime()) > 0 && !cancel.get()) {
                Thread.sleep(Math.min(Duration.ofNanos(waitNanos).toMillis() + 1, 1_000));
            }
        }
    }
}
