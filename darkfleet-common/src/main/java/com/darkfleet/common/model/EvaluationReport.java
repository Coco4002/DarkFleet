package com.darkfleet.common.model;

import java.time.Instant;
import java.util.List;

/**
 * Benchmark evaluation comparing the DarkFleet Agent against a Rules-Only Baseline
 * on a labelled dataset of recorded maritime scenarios.
 */
public record EvaluationReport(
        int totalLabelledCases,
        double rulesBaselineAccuracy,
        double rulesBaselineFalsePositiveRate,
        int rulesUnwarrantedWatchlistProposals,
        double agentAccuracy,
        double agentFalsePositiveRate,
        int agentUnwarrantedWatchlistProposals,
        List<CaseComparison> caseResults,
        Instant evaluatedAt
) {
    public record CaseComparison(
            String scenarioId,
            String description,
            String groundTruthLabel,
            String rulesBaselineDecision,
            boolean rulesCorrect,
            String agentDecision,
            boolean agentCorrect,
            String agentEvidenceReasoning
    ) {
    }
}
