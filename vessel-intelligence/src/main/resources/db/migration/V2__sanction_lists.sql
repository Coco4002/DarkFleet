-- Vessels on OpenSanctions' maritime list (CC BY-NC 4.0): sanctioned, shadow fleet, detained in port
-- inspections, persons of interest, regulatory warnings. Replaced as a whole by every import.
CREATE TABLE sanction (
    id         text PRIMARY KEY,               -- OpenSanctions entity id
    name       text        NOT NULL,
    imo        text,                           -- seven digits, without the "IMO" prefix
    mmsi       text[]      NOT NULL DEFAULT '{}',
    topics     text[]      NOT NULL,           -- e.g. sanction, mare.shadow, mare.detained, poi, reg.warn
    flag       text,
    countries  text[]      NOT NULL DEFAULT '{}',
    datasets   text[]      NOT NULL DEFAULT '{}',
    aliases    text[]      NOT NULL DEFAULT '{}',
    url        text        NOT NULL,
    import_id  bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sanction_imo ON sanction (imo) WHERE imo IS NOT NULL;
CREATE INDEX sanction_mmsi ON sanction USING gin (mmsi);

-- Every import attempt, kept so the status shows how old the list is and why an import failed.
CREATE TABLE sanction_import (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_url  text        NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    status      text        NOT NULL CHECK (status IN ('IMPORTED', 'UNCHANGED', 'FAILED')),
    sha256      text,
    vessels     integer,
    skipped     integer,
    message     text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
