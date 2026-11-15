package com.darkfleet.detection;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.CoverageCell;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.common.model.FeedHealth;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

/**
 * What every processor can look up regardless of its partition: the rules in force, cell coverage
 * and the feeds' heartbeats. All three are global tables, each instance holding a full copy.
 */
public final class Lookups {

    public static final String RULES_STORE = "rules";
    public static final String CELLS_STORE = "coverage-cells";
    public static final String FEEDS_STORE = "feed-health";

    private final ReadOnlyKeyValueStore<String, ?> rules;
    private final ReadOnlyKeyValueStore<String, ?> cells;
    private final ReadOnlyKeyValueStore<String, ?> feeds;
    private final DetectionProperties properties;
    /** Since when each feed has been working without interruption, as seen by {@link #refreshFeeds}. */
    private final Map<AisSource, Instant> workingSince = new EnumMap<>(AisSource.class);

    public Lookups(ProcessorContext<?, ?> context, DetectionProperties properties) {
        this.rules = context.getStateStore(RULES_STORE);
        this.cells = context.getStateStore(CELLS_STORE);
        this.feeds = context.getStateStore(FEEDS_STORE);
        this.properties = properties;
    }

    /** The latest approved rule set once one has arrived, else the baseline. */
    public DetectionRules rules() {
        DetectionRules approved = value(rules, DetectionRules.KEY);
        DetectionRules baseline = properties.baselineRules();
        return approved != null && approved.version() > baseline.version() ? approved : baseline;
    }

    /**
     * The cell's statistics, or null when there are no current ones. A cell with traffic in the last
     * 24 hours gets a record for every completed hour, so a record whose hour ended more than a check
     * after its grace period ago is out of date (the cell has gone quiet, or coverage isn't running).
     */
    public CoverageCell cell(String h3Cell, Instant now) {
        CoverageCell cell = value(cells, h3Cell);
        Instant oldestCurrentHour = now.minus(properties.coverageGrace()).minus(properties.checkInterval())
                .minus(Duration.ofHours(2));
        return cell != null && !cell.hourStart().isBefore(oldestCurrentHour) ? cell : null;
    }

    public boolean covered(String h3Cell, AisSource source, Instant now, DetectionRules rules) {
        CoverageCell cell = cell(h3Cell, now);
        return cell != null && cell.windowPositions(source) >= rules.coverageMinPositions();
    }

    public boolean portOrAnchorage(String h3Cell, Instant now, DetectionRules rules) {
        CoverageCell cell = cell(h3Cell, now);
        return cell != null && cell.windowMaxStoppedVessels() >= rules.portMinStoppedVessels();
    }

    /** Reads the latest heartbeats; call once before a round of {@link #workingSince} questions. */
    public void refreshFeeds(Instant now) {
        if (!properties.mode().usesFeedHealth()) {
            return;
        }
        for (AisSource source : AisSource.values()) {
            if (working(value(feeds, source.name()), now)) {
                workingSince.putIfAbsent(source, now);
            } else {
                workingSince.remove(source);
            }
        }
    }

    /**
     * Since when the feed has been working without interruption, or null if it isn't working now.
     * After a restart this is the first check that saw it working, which errs towards not raising gaps.
     * Without heartbeats (replay) every feed counts as working throughout.
     */
    public Instant workingSince(AisSource source) {
        return properties.mode().usesFeedHealth() ? workingSince.get(source) : Instant.EPOCH;
    }

    private boolean working(FeedHealth health, Instant now) {
        if (health == null || !health.connected() || health.lastMessageAt() == null) {
            return false;
        }
        Instant cutoff = now.minus(properties.feedStaleAfter());
        return !health.emittedAt().isBefore(cutoff) && !health.lastMessageAt().isBefore(cutoff);
    }

    @SuppressWarnings("unchecked")
    private static <V> V value(ReadOnlyKeyValueStore<String, ?> store, String key) {
        Object stored = store.get(key);
        return (V) (stored instanceof ValueAndTimestamp<?> withTimestamp ? withTimestamp.value() : stored);
    }
}
