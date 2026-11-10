package com.darkfleet.common.model;

public enum ProposalType {
    WATCHLIST_CHANGE("propose_watchlist_change", "Propose adding or removing a vessel from the analyst watchlist"),
    CASE_OPEN_OR_UPDATE("propose_case", "Propose opening or updating an investigation case with attached evidence"),
    RULE_CHANGE("propose_rule_change", "Propose tuning a detection rule parameter"),
    NO_ACTION("no_action", "Recommend no escalation after weighing competing explanations");

    private final String mcpToolName;
    private final String description;

    ProposalType(String mcpToolName, String description) {
        this.mcpToolName = mcpToolName;
        this.description = description;
    }

    public String getMcpToolName() {
        return mcpToolName;
    }

    public String getDescription() {
        return description;
    }
}
