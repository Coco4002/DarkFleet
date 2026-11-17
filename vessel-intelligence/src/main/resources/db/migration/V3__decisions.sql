-- The human side: proposals (from the agent or an analyst) and what an analyst's approval creates.
-- Nothing here is written by the Kafka consumers.

CREATE TABLE proposal (
    id             uuid PRIMARY KEY,
    type           text        NOT NULL CHECK (type IN ('CASE', 'WATCHLIST_CHANGE', 'RULE_CHANGE')),
    status         text        NOT NULL CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED')),
    payload        jsonb       NOT NULL,
    rationale      text        NOT NULL,
    evidence       jsonb       NOT NULL,
    confidence     numeric(4, 3) CHECK (confidence BETWEEN 0 AND 1),
    proposed_by    text        NOT NULL,
    -- chosen by the proposer; the same request sent again returns the first proposal
    request_id     text        NOT NULL,
    decided_by     text,
    decided_at     timestamptz,
    decision_note  text,
    -- what the approval created, e.g. CASE / <case id>
    result_type    text,
    result_id      text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT proposal_request UNIQUE (proposed_by, request_id),
    CONSTRAINT proposal_decided CHECK ((status = 'PENDING') = (decided_at IS NULL))
);
CREATE INDEX proposal_status ON proposal (status, created_at DESC);

-- "case" is a reserved word in SQL.
CREATE TABLE vessel_case (
    id           uuid PRIMARY KEY,
    title        text        NOT NULL,
    mmsi         text        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    proposal_id  uuid        NOT NULL REFERENCES proposal (id),
    opened_by    text        NOT NULL,
    opened_at    timestamptz NOT NULL,
    closed_by    text,
    closed_at    timestamptz,
    close_note   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX vessel_case_mmsi ON vessel_case (mmsi);
CREATE INDEX vessel_case_status ON vessel_case (status, opened_at DESC);

CREATE TABLE vessel_case_detection (
    case_id      uuid NOT NULL REFERENCES vessel_case (id),
    detection_id text NOT NULL REFERENCES detection (detection_id),
    PRIMARY KEY (case_id, detection_id)
);
CREATE INDEX vessel_case_detection_detection ON vessel_case_detection (detection_id);

-- Copied when the case opens, so a case keeps its evidence after track history expires.
CREATE TABLE vessel_case_evidence (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    case_id    uuid        NOT NULL REFERENCES vessel_case (id),
    kind       text        NOT NULL CHECK (kind IN ('DETECTION', 'TRACK', 'COVERAGE', 'IDENTITY')),
    ref        text        NOT NULL,   -- the detection or vessel the item is about
    data       jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX vessel_case_evidence_case ON vessel_case_evidence (case_id);

CREATE TABLE watchlist_entry (
    id                   uuid PRIMARY KEY,
    mmsi                 text,
    imo                  text,
    reason               text        NOT NULL,
    active               boolean     NOT NULL,
    proposal_id          uuid        NOT NULL REFERENCES proposal (id),
    added_by             text        NOT NULL,
    added_at             timestamptz NOT NULL,
    removal_proposal_id  uuid REFERENCES proposal (id),
    removed_by           text,
    removed_at           timestamptz,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT watchlist_entry_target CHECK (mmsi IS NOT NULL OR imo IS NOT NULL)
);
CREATE UNIQUE INDEX watchlist_active_mmsi ON watchlist_entry (mmsi) WHERE active AND mmsi IS NOT NULL;
CREATE UNIQUE INDEX watchlist_active_imo ON watchlist_entry (imo) WHERE active AND imo IS NOT NULL;

-- A recorded "no action needed" verdict on a detection (from the agent or an analyst).
CREATE TABLE assessment (
    id            uuid PRIMARY KEY,
    detection_id  text        NOT NULL REFERENCES detection (detection_id),
    category      text        NOT NULL CHECK (category IN
                      ('RECEIVER_GAP', 'EQUIPMENT_FAULT', 'NORMAL_OPERATIONS', 'INSUFFICIENT_EVIDENCE')),
    rationale     text        NOT NULL,
    assessed_by   text        NOT NULL,
    request_id    text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT assessment_request UNIQUE (assessed_by, request_id)
);
CREATE INDEX assessment_detection ON assessment (detection_id);

-- Every rule set ever in force; version 0 is the shared baseline, the highest version is current.
CREATE TABLE rule_set (
    version      integer PRIMARY KEY CHECK (version >= 0),
    rules        jsonb       NOT NULL,
    proposal_id  uuid REFERENCES proposal (id),
    approved_by  text        NOT NULL,
    approved_at  timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Events to publish to Kafka, written in the same transaction as the change they announce.
CREATE TABLE outbox (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic         text        NOT NULL,
    key           text        NOT NULL,
    payload       jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Who changed what, when, with the state before and after.
CREATE TABLE audit_log (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at           timestamptz NOT NULL DEFAULT now(),
    actor        text        NOT NULL,
    action       text        NOT NULL,
    entity_type  text        NOT NULL,
    entity_id    text        NOT NULL,
    before       jsonb,
    after        jsonb
);
CREATE INDEX audit_log_entity ON audit_log (entity_type, entity_id, at DESC);
CREATE INDEX audit_log_at ON audit_log (at DESC);
