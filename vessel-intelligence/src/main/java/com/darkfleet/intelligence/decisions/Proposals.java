package com.darkfleet.intelligence.decisions;

import com.darkfleet.common.model.DetectionRules;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** The proposal types, what each one carries, and the API's request and response shapes. */
public final class Proposals {

    private Proposals() {
    }

    public enum Type { CASE, WATCHLIST_CHANGE, RULE_CHANGE }

    public enum Status { PENDING, APPROVED, REJECTED }

    /**
     * A new proposal.
     *
     * @param payload    what to do; its shape depends on the type ({@link CasePayload},
     *                   {@link WatchlistPayload}, {@link RuleChangePayload})
     * @param evidence   the facts behind it, free-form JSON object
     * @param confidence 0 to 1, optional
     * @param requestId  chosen by the proposer; sending the same request again returns the first proposal
     */
    public record ProposalRequest(Type type, JsonNode payload, String rationale, JsonNode evidence, Double confidence,
                                  String requestId) {
    }

    public record ProposalView(String id, Type type, Status status, JsonNode payload, String rationale,
                               JsonNode evidence, Double confidence, String proposedBy, String requestId,
                               Instant createdAt, String decidedBy, Instant decidedAt, String decisionNote,
                               String resultType, String resultId) {
    }

    public record DecisionRequest(String note) {
    }

    /** Open a case about one vessel, built on detections of that vessel. */
    public record CasePayload(String title, String mmsi, List<String> detectionIds) {
    }

    public enum WatchlistAction { ADD, REMOVE }

    /** Put a vessel on the watchlist or take it off, by MMSI and/or IMO number. */
    public record WatchlistPayload(WatchlistAction action, String mmsi, String imo, String reason) {
    }

    /**
     * A complete new rule set (every threshold); the version is assigned on approval. Each value must
     * lie in a sane range, so even an approved mistake can't switch a detection off or flood analysts.
     */
    public record RuleChangePayload(
            Integer gapMinutes, Integer coverageMinPositions, Integer portMinStoppedVessels,
            Double loiteringMaxSpeedKnots, Integer loiteringMinMinutes, Double encounterMaxDistanceMeters,
            Double encounterMaxSpeedKnots, Integer encounterMinMinutes, Integer identityWindowHours,
            Double draughtChangeMeters) {

        /** @return what is wrong, empty when valid */
        public List<String> problems() {
            List<String> problems = new ArrayList<>();
            range(problems, "gapMinutes", gapMinutes, 15, 1440);
            range(problems, "coverageMinPositions", coverageMinPositions, 1, 10_000);
            range(problems, "portMinStoppedVessels", portMinStoppedVessels, 1, 1000);
            range(problems, "loiteringMaxSpeedKnots", loiteringMaxSpeedKnots, 0.1, 10);
            range(problems, "loiteringMinMinutes", loiteringMinMinutes, 15, 2880);
            range(problems, "encounterMaxDistanceMeters", encounterMaxDistanceMeters, 50, 5000);
            range(problems, "encounterMaxSpeedKnots", encounterMaxSpeedKnots, 0.1, 10);
            range(problems, "encounterMinMinutes", encounterMinMinutes, 15, 1440);
            range(problems, "identityWindowHours", identityWindowHours, 1, 720);
            range(problems, "draughtChangeMeters", draughtChangeMeters, 0.1, 10);
            return problems;
        }

        public DetectionRules toRules(int version) {
            return new DetectionRules(version, gapMinutes, coverageMinPositions, portMinStoppedVessels,
                    loiteringMaxSpeedKnots, loiteringMinMinutes, encounterMaxDistanceMeters, encounterMaxSpeedKnots,
                    encounterMinMinutes, identityWindowHours, draughtChangeMeters);
        }

        private static void range(List<String> problems, String name, Number value, double min, double max) {
            if (value == null) {
                problems.add(name + " is required");
            } else if (!(value.doubleValue() >= min && value.doubleValue() <= max)) { // also refuses NaN
                problems.add(name + " must be between " + fmt(min) + " and " + fmt(max));
            }
        }

        private static String fmt(double v) {
            return v == Math.rint(v) ? String.valueOf((long) v) : String.valueOf(v);
        }
    }
}
