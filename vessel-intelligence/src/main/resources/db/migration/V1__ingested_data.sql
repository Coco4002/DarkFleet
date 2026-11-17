-- Data Vessel Intelligence receives from Track & Detection over Kafka. Every table is keyed so that a
-- message delivered twice changes nothing. There are no foreign keys to vessel: Kafka keeps order only
-- within a partition of one topic, so a track point can arrive before its vessel's static data.
-- Times are timestamptz and always written in UTC.

CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;

-- Latest known identity and voyage data per vessel (vessel.static). A field missing from a newer
-- report keeps its last known value.
CREATE TABLE vessel (
    mmsi         text PRIMARY KEY,
    imo          text,
    name         text,
    call_sign    text,
    ship_type    integer,
    length_m     integer,
    beam_m       integer,
    draught_m    double precision,
    destination  text,
    eta          timestamptz,
    source       text        NOT NULL,
    reported_at  timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX vessel_imo ON vessel (imo) WHERE imo IS NOT NULL;

-- Every distinct identity (IMO number, name, call sign) a vessel has broadcast, per feed, with the
-- period it was seen. Rows are never replaced: a changed name or IMO number is a new row next to the
-- old one. Ship type and size are kept as last reported for that identity.
CREATE TABLE identity_report (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mmsi        text        NOT NULL,
    source      text        NOT NULL,
    imo         text,
    name        text,
    call_sign   text,
    ship_type   integer,
    length_m    integer,
    beam_m      integer,
    first_seen  timestamptz NOT NULL,
    last_seen   timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT identity_report_identity UNIQUE NULLS NOT DISTINCT (mmsi, source, imo, name, call_sign)
);
CREATE INDEX identity_report_imo ON identity_report (imo) WHERE imo IS NOT NULL;

-- Thinned track points (track.points), one partition per UTC day. Partitions are created by the
-- service when a point for a new day arrives, so replayed history lands in its own days too.
CREATE TABLE track_point (
    mmsi             text        NOT NULL,
    time             timestamptz NOT NULL,
    position         public.geometry(Point, 4326) NOT NULL,
    sog_knots        double precision,
    cog_degrees      double precision,
    heading_degrees  double precision,
    nav_status       integer,
    source           text        NOT NULL,
    h3_cell          text        NOT NULL,
    PRIMARY KEY (mmsi, time)
) PARTITION BY RANGE (time);

-- Hourly reception statistics per H3 cell (coverage.cells); a later record for the same hour wins.
CREATE TABLE coverage_cell (
    h3_cell                         text        NOT NULL,
    hour_start                      timestamptz NOT NULL,
    positions_by_source             jsonb       NOT NULL,
    vessels                         integer     NOT NULL,
    stopped_vessels                 integer     NOT NULL,
    window_positions_by_source      jsonb       NOT NULL,
    window_max_stopped_vessels      integer     NOT NULL,
    created_at                      timestamptz NOT NULL DEFAULT now(),
    updated_at                      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (h3_cell, hour_start)
);
CREATE INDEX coverage_cell_hour ON coverage_cell (hour_start);

-- Detections (detections), one row per detection id holding its newest version: an older or
-- repeated version never overwrites a newer one.
CREATE TABLE detection (
    detection_id   text PRIMARY KEY,
    type           text        NOT NULL,
    status         text        NOT NULL,
    version        integer     NOT NULL,
    mmsi           text        NOT NULL,
    other_mmsi     text,
    started_at     timestamptz NOT NULL,
    ended_at       timestamptz,
    position       public.geometry(Point, 4326),
    h3_cell        text,
    rules_version  integer     NOT NULL,
    evidence       jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX detection_mmsi ON detection (mmsi, started_at DESC);
CREATE INDEX detection_other_mmsi ON detection (other_mmsi, started_at DESC) WHERE other_mmsi IS NOT NULL;
CREATE INDEX detection_type_status ON detection (type, status, started_at DESC);
