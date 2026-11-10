package com.darkfleet.common.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Proposal submitted by the AI agent via MCP tools (propose_watchlist_change, propose_case, propose_rule_change).
 * Validated by Vessel Intelligence and held in PENDING_APPROVAL until a human analyst approves or rejects it.
 */
public record AnalystProposal(
        String proposalId,
        ProposalType proposalType,
        ProposalStatus status,
        String targetMmsi,
        String relatedEventId,
        String title,
        String rationale,
        List<String> competingExplanationsWeighed,
        List<String> supportingEvidence,
        ConfidenceLevel confidenceLevel,
        double confidenceScore,
        Map<String, Object> proposedPayload,
        String proposedBy,
        Instant createdAt,
        String reviewedBy,
        String reviewNotes,
        Instant reviewedAt
) {
}
