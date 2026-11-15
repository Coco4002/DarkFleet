package com.darkfleet.detection.coverage;

import com.darkfleet.common.model.AisSource;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Hourly counts of one cell over the coverage window; stored per H3 cell. */
public class CellCoverage {

    /** Keyed by the hour's start in epoch seconds, oldest first. */
    public TreeMap<Long, Hour> hours = new TreeMap<>();
    /** Reports for this hour or earlier arrive too late to be counted. */
    public Instant lastPublishedHour;

    public static class Hour {
        public Map<AisSource, Long> positions = new HashMap<>();
        /** Distinct vessels; emptied once the hour is published, the counts kept. */
        public Set<String> vessels = new HashSet<>();
        public Set<String> stopped = new HashSet<>();
        public int vesselCount;
        public int stoppedCount;
        public boolean published;

        void add(CoverageTick tick) {
            positions.merge(tick.source(), 1L, Long::sum);
            vessels.add(tick.mmsi());
            if (tick.stopped()) {
                stopped.add(tick.mmsi());
            }
            vesselCount = vessels.size();
            stoppedCount = stopped.size();
        }

        void markPublished() {
            published = true;
            vessels = new HashSet<>();
            stopped = new HashSet<>();
        }
    }
}
