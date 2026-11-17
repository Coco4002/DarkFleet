package com.darkfleet.intelligence.decisions;

import com.darkfleet.intelligence.read.Mappers;
import com.darkfleet.intelligence.read.Views.AssessmentView;
import com.darkfleet.intelligence.web.ApiException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * "No action needed" verdicts on detections, recorded by the agent or an analyst. They change nothing
 * else, so they need no approval; they are what lets the agent say it looked and found nothing.
 */
@Component
public class Assessments {

    public enum Category { RECEIVER_GAP, EQUIPMENT_FAULT, NORMAL_OPERATIONS, INSUFFICIENT_EVIDENCE }

    /** @param requestId chosen by the caller; the same request sent again returns the first verdict */
    public record AssessmentRequest(String detectionId, Category category, String rationale, String requestId) {
    }

    public record Recorded(AssessmentView assessment, boolean created) {
    }

    private final NamedParameterJdbcTemplate jdbc;

    public Assessments(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public Recorded record(AssessmentRequest request, String assessedBy) {
        if (request.detectionId() == null || request.category() == null || request.rationale() == null
                || request.rationale().isBlank() || request.requestId() == null || request.requestId().isBlank()) {
            throw ApiException.badRequest("detectionId, category, rationale and requestId are required");
        }
        if (request.rationale().length() > 4000 || request.requestId().length() > 200) {
            throw ApiException.badRequest("rationale is at most 4000 characters, requestId at most 200");
        }
        var params = new MapSqlParameterSource("id", UUID.randomUUID())
                .addValue("detection", request.detectionId()).addValue("category", request.category().name())
                .addValue("rationale", request.rationale().strip()).addValue("by", assessedBy)
                .addValue("request", request.requestId());
        Boolean exists = jdbc.queryForObject("SELECT EXISTS (SELECT 1 FROM detection WHERE detection_id = :detection)",
                params, Boolean.class);
        if (!Boolean.TRUE.equals(exists)) {
            throw ApiException.unprocessable("No detection " + request.detectionId());
        }
        List<AssessmentView> inserted = jdbc.query("""
                INSERT INTO assessment (id, detection_id, category, rationale, assessed_by, request_id)
                VALUES (:id, :detection, :category, :rationale, :by, :request)
                ON CONFLICT ON CONSTRAINT assessment_request DO NOTHING RETURNING *""", params, Mappers.ASSESSMENT);
        if (!inserted.isEmpty()) {
            return new Recorded(inserted.getFirst(), true);
        }
        return new Recorded(jdbc.query("SELECT * FROM assessment WHERE assessed_by = :by AND request_id = :request",
                params, Mappers.ASSESSMENT).getFirst(), false);
    }
}
