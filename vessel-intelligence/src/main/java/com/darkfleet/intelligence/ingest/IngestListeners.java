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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumes what Track &amp; Detection publishes, one batch at a time. A batch is stored in one
 * transaction and its offsets are committed only after that, so a crash means the batch comes again;
 * the stores make storing it twice harmless. Reads only committed records ({@code read_committed}):
 * Track &amp; Detection writes in transactions, and an aborted one must not show up as a detection.
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
        vessels.save(Decoder.decode(records, VesselStatic.class));
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
        trackPoints.save(points);
    }

    @KafkaListener(topics = "#{@intelTopics.coverageCells()}")
    void coverageCells(List<ConsumerRecord<String, byte[]>> records) {
        coverage.save(Decoder.decode(records, CoverageCell.class));
    }

    /** New and newer detections also go to the analysts' live stream, after they are stored. */
    @KafkaListener(topics = "#{@intelTopics.detections()}")
    void detections(List<ConsumerRecord<String, byte[]>> records) {
        List<Detection> changed = detections.save(Decoder.decode(records, Detection.class));
        for (Detection d : changed) {
            log.info("Detection {} {} v{} for {}", d.detectionId(), d.status(), d.version(), d.mmsi());
            events.publishEvent(new AnalystEvent("detection", d));
        }
    }
}
