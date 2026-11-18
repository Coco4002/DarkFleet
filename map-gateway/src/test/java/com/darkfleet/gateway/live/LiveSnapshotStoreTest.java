package com.darkfleet.gateway.live;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.FeedHealth;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.live.LiveSnapshotStore.FeedHealthView;
import com.darkfleet.gateway.live.LiveSnapshotStore.LiveSnapshot;
import com.darkfleet.gateway.live.LiveSnapshotStore.LiveVesselView;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveSnapshotStoreTest {

    private static final Instant T0 = Instant.parse("2026-11-18T12:00:00Z");

    @Test
    void mergesStaticReportsOutOfOrderAndEvictsExpiredVessels() {
        LiveSnapshotStore store = new LiveSnapshotStore(properties(false), Clock.fixed(T0, ZoneOffset.UTC));
        String mmsi = "230000001";

        // Newer static report lacks length/beam; older report arriving later fills them in without overwriting the new destination
        store.onVesselStatic(new VesselStatic(mmsi, T0.minusSeconds(60), AisSource.DIGITRAFFIC,
                "9074729", "FINNISH STAR", "OJAB", 80, 10.5, "HELSINKI", null, null, null));
        store.onVesselStatic(new VesselStatic(mmsi, T0.minusSeconds(300), AisSource.AISSTREAM,
                "9074729", "OLD NAME", "OJAB", 80, 10.2, "TALLINN", 185, 31, null));

        // Out-of-order track points: only the newer position wins
        store.onTrackPoint(new TrackPoint(mmsi, T0.minusSeconds(120), 60.1, 24.9, 12.0, 90.0, 91.0, 0,
                AisSource.DIGITRAFFIC, "861126d37ffffff"));
        store.onTrackPoint(new TrackPoint(mmsi, T0.minusSeconds(300), 59.9, 24.5, 11.0, 80.0, 80.0, 0,
                AisSource.AISSTREAM, "861126d37ffffff"));

        // A second vessel whose last report was 2 hours ago (> 1 h vesselExpiry)
        store.onTrackPoint(new TrackPoint("273000009", T0.minus(Duration.ofHours(2)), 59.5, 24.0, 10.0, 180.0, 180.0, 0,
                AisSource.AISSTREAM, "8611aa7a7ffffff"));

        LiveSnapshot snapshot = store.snapshot();
        assertEquals(1, snapshot.vessels().size());
        LiveVesselView v = snapshot.vessels().getFirst();
        assertEquals(mmsi, v.mmsi());
        assertEquals("FINNISH STAR", v.name());
        assertEquals("HELSINKI", v.destination());
        assertEquals(185, v.lengthMeters());
        assertEquals(31, v.beamMeters());
        assertEquals("FI", v.flagCode());
        assertEquals("Finland", v.flagCountry());
        assertEquals(60.1, v.latitude(), 1e-9);

        assertEquals(1, store.evictExpired(T0));
    }

    @Test
    void marksFeedStaleWhenDisconnectedOrSilent() {
        LiveSnapshotStore store = new LiveSnapshotStore(properties(false), Clock.fixed(T0, ZoneOffset.UTC));
        store.onFeedHealth(new FeedHealth(AisSource.DIGITRAFFIC, T0.minusSeconds(15), 30, true, 1100, 1050,
                T0.minusSeconds(2), 50_000, 0, 0));
        store.onFeedHealth(new FeedHealth(AisSource.AISSTREAM, T0.minusSeconds(15), 30, true, 0, 0,
                T0.minus(Duration.ofMinutes(5)), 10_000, 0, 1));

        var feeds = store.feeds();
        assertEquals(2, feeds.size());
        FeedHealthView aisstream = feeds.get(0);
        FeedHealthView digitraffic = feeds.get(1);
        assertEquals(AisSource.AISSTREAM, aisstream.source());
        assertTrue(aisstream.stale(), "Feed with no message in 5 minutes (> 2m threshold) must be marked stale");
        assertEquals(AisSource.DIGITRAFFIC, digitraffic.source());
        assertFalse(digitraffic.stale());
    }

    private static GatewayProperties properties(boolean replay) {
        return new GatewayProperties(replay, Duration.ofHours(1), Duration.ofSeconds(5), Duration.ofMinutes(2),
                "http://localhost:8086", "", Duration.ofSeconds(5), "", Duration.ofSeconds(5), Duration.ofHours(1));
    }
}
