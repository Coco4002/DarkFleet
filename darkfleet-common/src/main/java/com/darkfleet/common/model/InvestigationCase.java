package com.darkfleet.common.model;

import java.time.Instant;
import java.util.List;

public record InvestigationCase(
        String caseId,
        String title,
        String summary,
        String primaryMmsi,
        String secondaryMmsi,
        String imoNumber,
        CorridorRegion corridor,
        String status,
        ConfidenceLevel confidenceLevel,
        List<String> linkedEventIds,
        List<String> evidenceSummary,
        String originatingProposalId,
        String approvedByAnalyst,
        Instant createdAt,
        Instant updatedAt
) {
}
