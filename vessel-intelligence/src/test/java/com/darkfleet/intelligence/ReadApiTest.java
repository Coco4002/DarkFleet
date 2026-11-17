package com.darkfleet.intelligence;

import com.darkfleet.intelligence.ingest.TrackPartitions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The read API over seeded data: access rules, search, profile, track, detections, coverage. */
class ReadApiTest extends IntegrationTest {

    private static final String MMSI = "273000001";
    private static final String OTHER = "273000002";
    private static final String IMO = "9700001";
    private static final String CELL = "8611aa7a7ffffff";

    @Autowired
    private TrackPartitions partitions;

    @BeforeEach
    void seed() {
        Seed seed = new Seed(jdbc, partitions);
        seed.vessel(MMSI, IMO, "VOLGA STAR");
        seed.identity(MMSI, IMO, "VOLGA STAR", "2026-11-10T00:00:00Z", "2026-11-12T09:00:00Z");
        seed.identity(MMSI, IMO, "VOLGA", "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z");
        seed.identity(OTHER, IMO, "VOLGA STAR", "2026-11-11T00:00:00Z", "2026-11-11T06:00:00Z");
        seed.trackPoints(MMSI, CELL, Instant.parse("2026-11-12T10:00:00Z"), Instant.parse("2026-11-12T10:10:00Z"),
                Instant.parse("2026-11-12T10:20:00Z"), Instant.parse("2026-11-12T10:30:00Z"));
        seed.keptTrackPoint(MMSI, Instant.parse("2026-10-20T00:00:00Z"));
        seed.detection("read:gap", "UNEXPLAINED_GAP", "OPEN", MMSI, null, "2026-11-12T11:00:00Z", null, 59.5, 24.0, CELL);
        seed.detection("read:encounter", "POTENTIAL_ENCOUNTER", "CLOSED", MMSI, OTHER, "2026-11-12T06:00:00Z",
                "2026-11-12T09:00:00Z", 59.6, 24.2, CELL);
        seed.detection("read:identity", "IDENTITY_INCONSISTENCY", "OPEN", MMSI, OTHER, "2026-11-11T06:00:00Z", null,
                null, null, null);
        seed.coverage(CELL, "2026-11-12T10:00:00Z");
        jdbc.update("""
                INSERT INTO sanction (id, name, imo, topics, url, import_id)
                VALUES ('read-test-listing', 'VOLGA STAR', ?, '{sanction}', 'https://example.org/x', 0)
                ON CONFLICT DO NOTHING""", IMO);
    }

    @Test
    void everyEndpointNeedsAValidKey() throws Exception {
        mvc.perform(get(null, "/api/v1/vessels")).andExpect(status().isUnauthorized());
        mvc.perform(get("wrong-key", "/api/v1/vessels")).andExpect(status().isUnauthorized());
        mvc.perform(get(SERVICE_KEY, "/api/v1/vessels")).andExpect(status().isOk());
        mvc.perform(get(null, "/actuator/health")).andExpect(status().isOk());
        // the API description is public and spells out the three proposal payloads
        mvc.perform(get(null, "/v3/api-docs")).andExpect(status().isOk())
                .andExpect(jsonPath("$.components.schemas.ProposalRequest.properties.payload.oneOf.length()").value(3))
                .andExpect(jsonPath("$.components.schemas.RuleChangePayload.properties.gapMinutes").exists());
    }

    @Test
    void servicesReadButDontWatchTheAnalystStreamAndOnlyAdminsSeeTheAuditLog() throws Exception {
        mvc.perform(get(SERVICE_KEY, "/api/v1/events")).andExpect(status().isForbidden());
        mvc.perform(get(ANALYST_KEY, "/api/v1/audit")).andExpect(status().isForbidden());
        mvc.perform(get(ADMIN_KEY, "/api/v1/audit")).andExpect(status().isOk());
        mvc.perform(get(ANALYST_KEY, "/api/v1/events")).andExpect(request().asyncStarted());
    }

    @Test
    void searchFindsByNameImoAndMmsiPrefix() throws Exception {
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels?q=volga"))
                .andExpect(jsonPath("$.items[*].mmsi", hasItem(MMSI)));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels?q=IMO" + IMO))
                .andExpect(jsonPath("$.items[*].mmsi", contains(MMSI)));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels?q=2730000"))
                .andExpect(jsonPath("$.items[*].mmsi", hasItem(MMSI)));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels?q=volga&sanctioned=true"))
                .andExpect(jsonPath("$.items[?(@.mmsi == '" + MMSI + "')].sanctioned", contains(true)));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels?q=100%25_"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items", hasSize(0)));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels?size=0")).andExpect(status().isBadRequest());
    }

    @Test
    void profileBringsEverythingAboutTheVesselTogether() throws Exception {
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}", MMSI))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.name").value("VOLGA STAR"))
                .andExpect(jsonPath("$.lastPosition.time").value("2026-11-12T10:30:00Z"))
                .andExpect(jsonPath("$.identities[*].name", contains("VOLGA STAR", "VOLGA")))
                .andExpect(jsonPath("$.otherMmsisWithSameImo", contains(OTHER)))
                .andExpect(jsonPath("$.listings[*].id", hasItem("read-test-listing")))
                .andExpect(jsonPath("$.recentDetections[*].detectionId",
                        containsInAnyOrder("read:gap", "read:encounter", "read:identity")))
                .andExpect(jsonPath("$.recentDetections[?(@.detectionId == 'read:gap')].evidence.note", contains("seeded")));

        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}", "999999999")).andExpect(status().isNotFound());
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}", "12345")).andExpect(status().isBadRequest());
    }

    @Test
    void trackComesFromTheDailyPartitionsAndTheKeptPoints() throws Exception {
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}/track?from=2026-11-12T10:05:00Z&to=2026-11-12T10:30:00Z", MMSI))
                .andExpect(jsonPath("$[*].time", contains("2026-11-12T10:10:00Z", "2026-11-12T10:20:00Z")))
                .andExpect(jsonPath("$[0].latitude").value(59.5));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}/track?from=2026-10-19T12:00:00Z&to=2026-10-20T12:00:00Z", MMSI))
                .andExpect(jsonPath("$[*].time", contains("2026-10-20T00:00:00Z")));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}/track?from=2026-11-10T00:00:00Z&to=2026-11-12T00:00:01Z", MMSI))
                .andExpect(status().isBadRequest());
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}/track?from=not-a-time", MMSI))
                .andExpect(status().isBadRequest());
    }

    @Test
    void detectionsFilterByTypeVesselTimeAndArea() throws Exception {
        mvc.perform(get(ANALYST_KEY, "/api/v1/detections?type=POTENTIAL_ENCOUNTER&mmsi=" + OTHER))
                .andExpect(jsonPath("$.items[*].detectionId", contains("read:encounter")));
        mvc.perform(get(ANALYST_KEY, "/api/v1/encounters?mmsi=" + MMSI))
                .andExpect(jsonPath("$.items[*].detectionId", contains("read:encounter")));
        mvc.perform(get(ANALYST_KEY, "/api/v1/detections?mmsi=" + MMSI + "&bbox=23.9,59.4,24.1,59.55"))
                .andExpect(jsonPath("$.items[*].detectionId", contains("read:gap")));
        mvc.perform(get(ANALYST_KEY, "/api/v1/detections?mmsi=" + MMSI
                        + "&from=2026-11-12T00:00:00Z&to=2026-11-13T00:00:00Z&status=OPEN"))
                .andExpect(jsonPath("$.items[*].detectionId", contains("read:gap")));
        mvc.perform(get(ANALYST_KEY, "/api/v1/detections?mmsi=" + MMSI + "&size=1"))
                .andExpect(jsonPath("$.items", hasSize(1))).andExpect(jsonPath("$.hasMore").value(true));

        mvc.perform(get(ANALYST_KEY, "/api/v1/detections?bbox=1,2,3")).andExpect(status().isBadRequest());
        mvc.perform(get(ANALYST_KEY, "/api/v1/detections?type=GUILTY")).andExpect(status().isBadRequest());
        mvc.perform(get(ANALYST_KEY, "/api/v1/detections/{id}", "read:gap"))
                .andExpect(jsonPath("$.detection.latitude").value(59.5))
                .andExpect(jsonPath("$.assessments").isArray());
        mvc.perform(get(ANALYST_KEY, "/api/v1/detections/{id}", "nope")).andExpect(status().isNotFound());
    }

    @Test
    void coverageForAnHour() throws Exception {
        mvc.perform(get(ANALYST_KEY, "/api/v1/coverage?hour=2026-11-12T10:20:00Z"))
                .andExpect(jsonPath("$[?(@.h3Cell == '" + CELL + "')].positionsBySource.DIGITRAFFIC", contains(340)));
        mvc.perform(get(ANALYST_KEY, "/api/v1/coverage?hour=2026-11-12T11:00:00Z"))
                .andExpect(jsonPath("$[*].h3Cell", not(hasItem(CELL))));
    }
}
