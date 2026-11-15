package com.darkfleet.detection.coverage;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.CoverageCell;
import com.darkfleet.detection.DetectionClock;
import com.darkfleet.detection.DetectionProperties;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Counts position reports per H3 cell, hour and feed, partitioned by cell. After every completed
 * hour (plus a grace period for late reports) it publishes, for each cell that had traffic within
 * the coverage window, a {@link CoverageCell} with that hour and the window's totals ending with it;
 * so a cell's latest record always describes the last 24 hours, even when the cell has gone quiet.
 * A cell drops out once its window is empty. Reports for an hour that is already complete are ignored.
 */
public class CoverageProcessor implements Processor<String, CoverageTick, String, CoverageCell> {

    public static final String STORE = "coverage";

    private final DetectionProperties properties;
    private final DetectionClock clock;
    private ProcessorContext<String, CoverageCell> context;
    private KeyValueStore<String, CellCoverage> cells;

    public CoverageProcessor(DetectionProperties properties, DetectionClock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void init(ProcessorContext<String, CoverageCell> context) {
        this.context = context;
        this.cells = context.getStateStore(STORE);
        clock.schedule(context, this::publishCompletedHours);
    }

    @Override
    public void process(Record<String, CoverageTick> record) {
        CoverageTick tick = record.value();
        if (tick == null) {
            return;
        }
        Instant hour = tick.time().truncatedTo(ChronoUnit.HOURS);
        if (!hour.isAfter(lastCompleteHour(clock.now(context)))) {
            return; // too late: that hour is complete
        }
        CellCoverage cell = cells.get(record.key());
        if (cell == null) {
            cell = new CellCoverage();
        } else if (cell.lastPublishedHour != null && !hour.isAfter(cell.lastPublishedHour)) {
            return; // that hour is already published
        }
        cell.hours.computeIfAbsent(hour.getEpochSecond(), h -> new CellCoverage.Hour()).add(tick);
        cells.put(record.key(), cell);
    }

    private void publishCompletedHours(Instant now) {
        Instant lastComplete = lastCompleteHour(now);
        List<KeyValue<String, CellCoverage>> all = new ArrayList<>();
        try (KeyValueIterator<String, CellCoverage> iterator = cells.all()) {
            iterator.forEachRemaining(all::add);
        }
        for (KeyValue<String, CellCoverage> entry : all) {
            CellCoverage cell = entry.value;
            Instant hour = cell.lastPublishedHour == null
                    ? Instant.ofEpochSecond(cell.hours.firstKey())
                    : cell.lastPublishedHour.plus(Duration.ofHours(1));
            boolean changed = false;
            for (; !hour.isAfter(lastComplete) && !cell.hours.isEmpty(); hour = hour.plus(Duration.ofHours(1))) {
                cell.hours.headMap(hour.minus(CoverageCell.WINDOW).getEpochSecond(), true).clear();
                if (!cell.hours.isEmpty()) {
                    context.forward(new Record<>(entry.key, summary(entry.key, cell, hour), now.toEpochMilli()));
                    CellCoverage.Hour counted = cell.hours.get(hour.getEpochSecond());
                    if (counted != null) {
                        counted.markPublished();
                    }
                }
                cell.lastPublishedHour = hour;
                changed = true;
            }
            if (cell.hours.isEmpty()) {
                cells.delete(entry.key);
            } else if (changed) {
                cells.put(entry.key, cell);
            }
        }
    }

    /** The start of the latest hour that is over, grace period included. */
    private Instant lastCompleteHour(Instant now) {
        return now.minus(properties.coverageGrace()).truncatedTo(ChronoUnit.HOURS).minus(Duration.ofHours(1));
    }

    private static CoverageCell summary(String h3Cell, CellCoverage cell, Instant hourStart) {
        CellCoverage.Hour hour = cell.hours.get(hourStart.getEpochSecond());
        Map<AisSource, Long> windowPositions = new HashMap<>();
        int windowMaxStopped = 0;
        for (CellCoverage.Hour h : cell.hours.headMap(hourStart.getEpochSecond(), true).values()) {
            h.positions.forEach((source, count) -> windowPositions.merge(source, count, Long::sum));
            windowMaxStopped = Math.max(windowMaxStopped, h.stoppedCount);
        }
        return hour == null
                ? new CoverageCell(h3Cell, hourStart, Map.of(), 0, 0, windowPositions, windowMaxStopped)
                : new CoverageCell(h3Cell, hourStart, hour.positions, hour.vesselCount, hour.stoppedCount,
                        windowPositions, windowMaxStopped);
    }
}
