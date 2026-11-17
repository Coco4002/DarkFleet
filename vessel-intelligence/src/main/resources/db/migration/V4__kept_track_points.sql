-- Track points kept past the retention period because they lie within the keep window (6 h) of a
-- detection of that vessel. Copied out of a daily partition just before the partition is dropped;
-- track queries read both tables.
CREATE TABLE track_point_kept (
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
);
