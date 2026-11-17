package com.darkfleet.intelligence;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.intelligence.ingest.TrackPartitions;
import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Proposals and what approving them does: cases with copied evidence, the watchlist, rule changes that
 * reach Kafka through the outbox, assessments. A service (the agent's MCP server) proposes; only an
 * analyst decides; only an administrator decides rule changes.
 */
class DecisionApiTest extends IntegrationTest {

    private static final String MMSI = "273100001";
    private static final String OTHER = "273100009";
    private static final String CELL = "8611aa707ffffff";

    @Autowired
    private TrackPartitions partitions;

    @BeforeEach
    void seed() {
        Seed seed = new Seed(jdbc, partitions);
        seed.vessel(MMSI, "9700002", "NORDIC TEST");
        seed.identity(MMSI, "9700002", "NORDIC TEST", "2026-11-01T00:00:00Z", "2026-11-13T12:00:00Z");
        seed.detection("decide:gap", "UNEXPLAINED_GAP", "CLOSED", MMSI, null, "2026-11-13T08:00:00Z",
                "2026-11-13T10:00:00Z", 59.7, 24.9, CELL);
        seed.detection("decide:other", "LOITERING", "OPEN", OTHER, null, "2026-11-13T08:00:00Z", null, 59.7, 24.9, CELL);
        // 6 h around the gap is kept as evidence; 23:00 the day before is outside it
        seed.trackPoints(MMSI, CELL, Instant.parse("2026-11-12T23:00:00Z"), Instant.parse("2026-11-13T07:00:00Z"),
                Instant.parse("2026-11-13T07:55:00Z"), Instant.parse("2026-11-13T10:05:00Z"));
        seed.coverage(CELL, "2026-11-13T08:00:00Z");
    }

    @Test
    void aServiceProposesACaseAndAnAnalystOpensIt() throws Exception {
        Map<String, Object> request = proposal("CASE", Map.of(
                "title", "Unexplained gap east of Hanko", "mmsi", MMSI, "detectionIds", List.of("decide:gap")), "case-1");

        String id = JsonPath.read(mvc.perform(post(SERVICE_KEY, "/api/v1/proposals", request))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.proposedBy").value("mcp"))
                .andReturn().getResponse().getContentAsString(), "$.id");
        // sent again (a retry): the same proposal, not a second one
        mvc.perform(post(SERVICE_KEY, "/api/v1/proposals", request))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(id));

        mvc.perform(post(SERVICE_KEY, "/api/v1/proposals/{id}/approve", null, id)).andExpect(status().isForbidden());
        String caseId = JsonPath.read(mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/approve",
                        Map.of("note", "worth a look"), id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPROVED"))
                .andExpect(jsonPath("$.decidedBy").value("analyst"))
                .andExpect(jsonPath("$.resultType").value("CASE"))
                .andReturn().getResponse().getContentAsString(), "$.resultId");
        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/approve", null, id)).andExpect(status().isConflict());

        mvc.perform(get(ANALYST_KEY, "/api/v1/cases/{id}", caseId))
                .andExpect(jsonPath("$.summary.status").value("OPEN"))
                .andExpect(jsonPath("$.detections[*].detectionId", contains("decide:gap")))
                .andExpect(jsonPath("$.evidence[*].kind", containsInAnyOrder("DETECTION", "TRACK", "COVERAGE", "IDENTITY")))
                .andExpect(jsonPath("$.evidence[?(@.kind == 'TRACK')].data[*].time",
                        contains("2026-11-13T07:00:00+00:00", "2026-11-13T07:55:00+00:00", "2026-11-13T10:05:00+00:00")))
                .andExpect(jsonPath("$.evidence[?(@.kind == 'IDENTITY')].data[*].name", contains("NORDIC TEST")));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}", MMSI)).andExpect(jsonPath("$.cases[*].id", hasItem(caseId)));

        mvc.perform(post(ANALYST_KEY, "/api/v1/cases/{id}/close", Map.of(), caseId)).andExpect(status().isBadRequest());
        mvc.perform(post(ANALYST_KEY, "/api/v1/cases/{id}/close", Map.of("note", "receiver outage"), caseId))
                .andExpect(status().isOk()).andExpect(jsonPath("$.summary.status").value("CLOSED"));
        mvc.perform(post(ANALYST_KEY, "/api/v1/cases/{id}/close", Map.of("note", "again"), caseId))
                .andExpect(status().isConflict());

        mvc.perform(get(ADMIN_KEY, "/api/v1/audit?entityType=PROPOSAL&entityId=" + id))
                .andExpect(jsonPath("$.items[*].action", contains("APPROVE")))
                .andExpect(jsonPath("$.items[0].actor").value("analyst"))
                .andExpect(jsonPath("$.items[0].before.status").value("PENDING"))
                .andExpect(jsonPath("$.items[0].after.status").value("APPROVED"));
        mvc.perform(get(ADMIN_KEY, "/api/v1/audit?entityType=CASE&entityId=" + caseId))
                .andExpect(jsonPath("$.items[*].action", contains("CLOSE")));
    }

    @Test
    void aProposalMustBeAboutRealDetectionsOfThatVessel() throws Exception {
        mvc.perform(post(SERVICE_KEY, "/api/v1/proposals", proposal("CASE", Map.of(
                        "title", "Mixed up", "mmsi", MMSI, "detectionIds", List.of("decide:other", "missing")), "case-bad")))
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("decide:other")));
        mvc.perform(post(SERVICE_KEY, "/api/v1/proposals", proposal("CASE", Map.of(
                        "title", "Wrong shape", "mmsi", MMSI, "detectionIds", "decide:gap"), "case-shape")))
                .andExpect(status().isUnprocessableContent());
        mvc.perform(post(SERVICE_KEY, "/api/v1/proposals", Map.of("payload", Map.of(), "rationale", "x", "requestId", "r")))
                .andExpect(status().isBadRequest());
        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/approve", null, "00000000-0000-0000-0000-000000000000"))
                .andExpect(status().isNotFound());
    }

    @Test
    void watchlistChangesOnlyThroughApprovedProposals() throws Exception {
        String add = create(proposal("WATCHLIST_CHANGE",
                Map.of("action", "ADD", "mmsi", MMSI, "imo", "9700002", "reason", "Repeated gaps near Porkkala"), "wl-add"));
        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/approve", null, add)).andExpect(status().isOk());
        mvc.perform(get(SERVICE_KEY, "/api/v1/watchlist")).andExpect(jsonPath("$.items[*].mmsi", hasItem(MMSI)));
        mvc.perform(get(SERVICE_KEY, "/api/v1/vessels?watchlisted=true"))
                .andExpect(jsonPath("$.items[*].mmsi", hasItem(MMSI)));

        // adding it twice: the approval fails and the proposal stays pending
        String again = create(proposal("WATCHLIST_CHANGE",
                Map.of("action", "ADD", "mmsi", MMSI, "reason", "duplicate"), "wl-add-2"));
        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/approve", null, again)).andExpect(status().isConflict());
        mvc.perform(get(ANALYST_KEY, "/api/v1/proposals/{id}", again)).andExpect(jsonPath("$.status").value("PENDING"));

        String remove = create(proposal("WATCHLIST_CHANGE",
                Map.of("action", "REMOVE", "mmsi", MMSI, "reason", "Explained by a receiver outage"), "wl-remove"));
        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/approve", null, remove)).andExpect(status().isOk());
        mvc.perform(get(SERVICE_KEY, "/api/v1/watchlist")).andExpect(jsonPath("$.items[*].mmsi", not(hasItem(MMSI))));
        mvc.perform(get(SERVICE_KEY, "/api/v1/watchlist?includeRemoved=true"))
                .andExpect(jsonPath("$.items[?(@.mmsi == '" + MMSI + "')].removedBy", contains("analyst")));
    }

    @Test
    void anApprovedRuleChangeReachesTrackAndDetectionThroughKafka() throws Exception {
        Map<String, Object> rules = baselineWith("gapMinutes", 90);
        String id = create(proposal("RULE_CHANGE", rules, "rules-90"));

        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/approve", null, id)).andExpect(status().isForbidden());
        int version = Integer.parseInt(JsonPath.read(mvc.perform(post(ADMIN_KEY, "/api/v1/proposals/{id}/approve", null, id))
                .andExpect(status().isOk()).andExpect(jsonPath("$.resultType").value("RULE_SET"))
                .andReturn().getResponse().getContentAsString(), "$.resultId"));

        mvc.perform(get(SERVICE_KEY, "/api/v1/rules/current"))
                .andExpect(jsonPath("$.version").value(version))
                .andExpect(jsonPath("$.rules.gapMinutes").value(90))
                .andExpect(jsonPath("$.approvedBy").value("admin"));
        mvc.perform(get(SERVICE_KEY, "/api/v1/rules"))
                .andExpect(jsonPath("$.items[-1].version").value(0))
                .andExpect(jsonPath("$.items[-1].approvedBy").value("baseline"));

        ConsumerRecord<String, String> published = awaitRecord(DarkFleetTopics.RULES_APPROVED,
                r -> JsonSerde.mapper().readValue(r.value(), DetectionRules.class).version() == version);
        assertEquals(DetectionRules.KEY, published.key());
        DetectionRules sent = JsonSerde.mapper().readValue(published.value(), DetectionRules.class);
        assertEquals(90, sent.gapMinutes());
        assertEquals(DetectionRules.baseline().encounterMaxDistanceMeters(), sent.encounterMaxDistanceMeters());
        awaitCount(0, "SELECT count(*) FROM outbox WHERE published_at IS NULL");
    }

    @Test
    void ruleValuesOutsideTheSaneRangeAreRefused() throws Exception {
        mvc.perform(post(ADMIN_KEY, "/api/v1/proposals", proposal("RULE_CHANGE", baselineWith("gapMinutes", 5), "rules-5")))
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.detail").value("gapMinutes must be between 15 and 1440"));
        Map<String, Object> missing = baselineWith("gapMinutes", 60);
        missing.remove("encounterMinMinutes");
        mvc.perform(post(ADMIN_KEY, "/api/v1/proposals", proposal("RULE_CHANGE", missing, "rules-missing")))
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.detail").value("encounterMinMinutes is required"));
        // JSON has no NaN, but Jackson reads the string "NaN" as one; every comparison with it is false
        mvc.perform(post(ADMIN_KEY, "/api/v1/proposals",
                        proposal("RULE_CHANGE", baselineWith("loiteringMaxSpeedKnots", "NaN"), "rules-nan")))
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.detail").value("loiteringMaxSpeedKnots must be between 0.1 and 10"));
    }

    @Test
    void aRejectionNeedsAReasonAndIsFinal() throws Exception {
        String id = create(proposal("WATCHLIST_CHANGE", Map.of("action", "ADD", "mmsi", OTHER, "reason", "loitering"), "wl-reject"));
        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/reject", Map.of(), id)).andExpect(status().isBadRequest());
        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/reject", Map.of("note", "pilot boarding area"), id))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.decisionNote").value("pilot boarding area"));
        mvc.perform(post(ANALYST_KEY, "/api/v1/proposals/{id}/approve", null, id)).andExpect(status().isConflict());
        mvc.perform(get(ANALYST_KEY, "/api/v1/proposals?status=REJECTED"))
                .andExpect(jsonPath("$.items[*].id", hasItem(id)));
    }

    @Test
    void anAssessmentRecordsThatNoActionIsNeeded() throws Exception {
        Map<String, Object> request = Map.of("detectionId", "decide:gap", "category", "RECEIVER_GAP",
                "rationale", "Digitraffic kept receiving the vessel during the gap", "requestId", "assess-1");
        String id = JsonPath.read(mvc.perform(post(SERVICE_KEY, "/api/v1/assessments", request))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString(), "$.id");
        mvc.perform(post(SERVICE_KEY, "/api/v1/assessments", request))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(id));
        mvc.perform(get(ANALYST_KEY, "/api/v1/detections/{id}", "decide:gap"))
                .andExpect(jsonPath("$.assessments[*].id", contains(id)))
                .andExpect(jsonPath("$.assessments[0].assessedBy").value("mcp"));

        mvc.perform(post(SERVICE_KEY, "/api/v1/assessments", Map.of("detectionId", "missing", "category", "RECEIVER_GAP",
                "rationale", "x", "requestId", "assess-2"))).andExpect(status().isUnprocessableContent());
        mvc.perform(post(SERVICE_KEY, "/api/v1/assessments", Map.of("detectionId", "decide:gap", "category", "GUILTY",
                "rationale", "x", "requestId", "assess-3"))).andExpect(status().isBadRequest());
    }

    @Test
    void theProposalInboxListsPendingProposals() throws Exception {
        String id = create(proposal("WATCHLIST_CHANGE", Map.of("action", "ADD", "imo", "9700002", "reason", "inbox"), "inbox-1"));
        mvc.perform(get(ANALYST_KEY, "/api/v1/proposals?status=PENDING&type=WATCHLIST_CHANGE"))
                .andExpect(jsonPath("$.items[*].id", hasItem(id)))
                .andExpect(jsonPath("$.items[*].status", not(hasItem("APPROVED"))));
        mvc.perform(get(ANALYST_KEY, "/api/v1/proposals?size=201")).andExpect(status().isBadRequest());
        mvc.perform(get(ANALYST_KEY, "/api/v1/proposals?size=1")).andExpect(jsonPath("$.items", hasSize(1)));
    }

    private String create(Map<String, Object> request) throws Exception {
        return JsonPath.read(mvc.perform(post(SERVICE_KEY, "/api/v1/proposals", request))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString(), "$.id");
    }

    private static Map<String, Object> proposal(String type, Map<String, Object> payload, String requestId) {
        return Map.of("type", type, "payload", payload, "rationale", "Test rationale",
                "evidence", Map.of("source", "test"), "confidence", 0.7, "requestId", requestId);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> baselineWith(String field, Object value) {
        Map<String, Object> rules = new HashMap<>(JsonSerde.mapper().convertValue(DetectionRules.baseline(), Map.class));
        rules.remove("version");
        rules.put(field, value);
        return rules;
    }
}
