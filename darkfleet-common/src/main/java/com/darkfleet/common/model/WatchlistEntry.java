package com.darkfleet.common.model;

import java.time.Instant;

public record WatchlistEntry(
        String mmsi,
        String imoNumber,
        String vesselName,
        String reason,
        ConfidenceLevel priorityLevel,
        String originatingProposalId,
        String approvedByAnalyst,
        Instant addedAt
) {
}
