package com.darkfleet.common.model;

public enum ProposalStatus {
    PENDING_APPROVAL("Pending Analyst Approval"),
    APPROVED("Approved by Analyst"),
    REJECTED("Rejected by Analyst");

    private final String label;

    ProposalStatus(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }
}
