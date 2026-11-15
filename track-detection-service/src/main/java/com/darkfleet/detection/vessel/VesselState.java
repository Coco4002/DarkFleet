package com.darkfleet.detection.vessel;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.detection.support.Sightings;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** Everything Track &amp; Detection remembers about one vessel; stored per MMSI in a state store. */
public class VesselState {

    /** Latest identity, merged from the static reports of all feeds. */
    public Integer shipType;
    public Double draughtMeters;
    public String vesselName;
    public String imoNumber;
    /** Time of the static report the identity above comes from. */
    public Instant identityAt;
    /** Time of the newest static report per feed. */
    public Map<AisSource, Instant> lastStaticAt = new HashMap<>();
    /** The static report last published per feed. */
    public Map<AisSource, VesselStatic> publishedStatic = new HashMap<>();

    /** The newest position from any feed. */
    public Position last;
    /** When each feed last delivered a position of this vessel. */
    public Map<AisSource, Instant> lastSeenBySource = new HashMap<>();
    public TrackPoint lastTrackPoint;
    /** The current silence was looked at and won't become a gap (port, coverage, unknown course). */
    public boolean silenceSettled;

    /** IMO numbers this MMSI broadcast, for the "one MMSI, several IMO numbers" check. */
    public Sightings imoSightings = new Sightings();
    public String claimedImo;
    public Instant claimedAt;

    /** Open detections, or loitering still building up ({@link Loitering#opened} null). */
    public Detection gap;
    public Loitering loitering;
    public Detection identityConflict;
    /** The last report that showed the identity conflict. */
    public Instant identityConflictSeenAt;

    public record Position(
            Instant time,
            double latitude,
            double longitude,
            Double sogKnots,
            Double cogDegrees,
            Integer navStatus,
            AisSource source,
            String h3Cell
    ) {
        /** AIS navigational status 5. */
        boolean moored() {
            return navStatus != null && navStatus == 5;
        }
    }

    public static class Loitering {
        public Instant startedAt;
        public double startLatitude;
        public double startLongitude;
        public String startCell;
        public Instant lastSlowAt;
        public double lastLatitude;
        public double lastLongitude;
        /** Reports in a row that weren't slow; one stray report doesn't end an episode. */
        public int misses;
        public Detection opened;
    }

    /** The latest time anything was heard from this vessel. */
    Instant lastHeard() {
        Instant latest = last == null ? null : last.time();
        for (Instant t : lastStaticAt.values()) {
            if (latest == null || t.isAfter(latest)) {
                latest = t;
            }
        }
        return latest;
    }
}
