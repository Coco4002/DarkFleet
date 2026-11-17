package com.darkfleet.intelligence.ingest;

import com.darkfleet.common.model.CoverageCell;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.intelligence.events.AnalystEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Consumes what Track &amp; Detection publishes, one batch at a time. A batch is stored in one
 * transaction and its offsets are committed only after that, so a crash means the batch comes again;
 * the stores make storing it twice harmless. Reads only committed records ({@code read_committed}):
 * Track &amp; Detection writes in transactions, and an aborted one must not show up as a detection.
 * <p>
 * A batch the database refuses because of a value in it (SQL state class 22, "data exception", e.g. a
 * NUL character in a text field) is stored again message by message, and only the messages it refuses
 * are logged and skipped: retrying them would fail forever and hold up everything behind them. Any
 * other failure fails the batch, which is retried until it is stored: the database is down, or a
 * track point's daily partition was dropped by retention after it was ensured (the retry creates it
 * again).
 */
@Component
class IngestListeners {

    private static final Logger log = LoggerFactory.getLogger(IngestListeners.class);

    private final VesselStore vessels;
    private final TrackPartitions partitions;
    private final TrackPointStore trackPoints;
    private final CoverageStore coverage;
    private final DetectionStore detections;
    private final ApplicationEventPublisher events;

    IngestListeners(VesselStore vessels, TrackPartitions partitions, TrackPointStore trackPoints,
                    CoverageStore coverage, DetectionStore detections, ApplicationEventPublisher events) {
        this.vessels = vessels;
        this.partitions = partitions;
        this.trackPoints = trackPoints;
        this.coverage = coverage;
        this.detections = detections;
        this.events = events;
    }

    @KafkaListener(topics = "#{@intelTopics.vesselStatic()}")
    void vesselStatic(List<ConsumerRecord<String, byte[]>> records) {
        store(Decoder.decode(records, VesselStatic.class), batch -> {
            vessels.save(batch);
            return List.of();
        });
    }

    @KafkaListener(topics = "#{@intelTopics.trackPoints()}")
    void trackPoints(List<ConsumerRecord<String, byte[]>> records) {
        List<TrackPoint> points = Decoder.decode(records, TrackPoint.class).stream()
                .filter(point -> {
                    boolean valid = TrackPointStore.valid(point);
                    if (!valid) {
                        log.warn("Skipping track point of {} at {} with impossible position {}, {}",
                                point.mmsi(), point.time(), point.latitude(), point.longitude());
                    }
                    return valid;
                })
                .toList();
        partitions.ensure(points.stream().map(TrackPoint::time).toList());
        store(points, batch -> {
            trackPoints.save(batch);
            return List.of();
        });
    }

    @KafkaListener(topics = "#{@intelTopics.coverageCells()}")
    void coverageCells(List<ConsumerRecord<String, byte[]>> records) {
        store(Decoder.decode(records, CoverageCell.class), batch -> {
            coverage.save(batch);
            return List.of();
        });
    }

    /** New and newer detections also go to the analysts' live stream, after they are stored. */
    @KafkaListener(topics = "#{@intelTopics.detections()}")
    void detections(List<ConsumerRecord<String, byte[]>> records) {
        List<Detection> changed = store(Decoder.decode(records, Detection.class), detections::save);
        for (Detection d : changed) {
            log.info("Detection {} {} v{} for {}", d.detectionId(), d.status(), d.version(), d.mmsi());
            events.publishEvent(new AnalystEvent("detection", d));
        }
    }

    /**
     * Saves a batch in one transaction; if the database refuses its content, saves each message in its
     * own transaction and skips the ones refused.
     *
     * @param save stores a list in one transaction and returns what it changed
     */
    static <T> List<T> store(List<T> messages, Function<List<T>, List<T>> save) {
        try {
            return save.apply(messages);
        } catch (DataIntegrityViolationException batchRefused) {
            if (!refusesAValue(batchRefused)) {
                throw batchRefused;
            }
            List<T> changed = new ArrayList<>();
            for (T message : messages) {
                try {
                    changed.addAll(save.apply(List.of(message)));
                } catch (DataIntegrityViolationException e) {
                    if (!refusesAValue(e)) {
                        throw e;
                    }
                    log.warn("Skipping a message the database refuses: {} ({})", message,
                            e.getMostSpecificCause().getMessage());
                }
            }
            return changed;
        }
    }

    /** True when the database refused a value itself (SQL state class 22), which no retry can change. */
    static boolean refusesAValue(DataIntegrityViolationException e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql && sql.getSQLState() != null) {
                return sql.getSQLState().startsWith("22");
            }
        }
        return false;
    }
}
