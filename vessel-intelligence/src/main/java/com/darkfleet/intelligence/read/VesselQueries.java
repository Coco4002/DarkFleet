package com.darkfleet.intelligence.read;

import com.darkfleet.intelligence.read.Views.IdentityView;
import com.darkfleet.intelligence.read.Views.ListingView;
import com.darkfleet.intelligence.read.Views.TrackPointView;
import com.darkfleet.intelligence.read.Views.VesselData;
import com.darkfleet.intelligence.read.Views.VesselProfile;
import com.darkfleet.intelligence.read.Views.VesselSummary;
import com.darkfleet.intelligence.web.ApiException;
import com.darkfleet.intelligence.web.PageResult;
import com.darkfleet.intelligence.web.Rows;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Repository
@Transactional(readOnly = true)
public class VesselQueries {

    /** Longest track window one request may ask for. */
    public static final Duration MAX_TRACK_WINDOW = Duration.ofHours(48);
    private static final Pattern MMSI = Pattern.compile("\\d{9}");
    private static final Pattern IMO = Pattern.compile("(?i)(?:IMO)?\\s*(\\d{7})");

    /** A listing matches a vessel by IMO number or by MMSI. */
    private static final String LISTED = "(s.imo = v.imo OR s.mmsi @> ARRAY[v.mmsi])";
    private static final String SANCTIONED = "EXISTS (SELECT 1 FROM sanction s WHERE " + LISTED + " AND 'sanction' = ANY (s.topics))";
    private static final String ANY_LISTING = "EXISTS (SELECT 1 FROM sanction s WHERE " + LISTED + ")";
    private static final String WATCHLISTED = "EXISTS (SELECT 1 FROM watchlist_entry w WHERE w.active AND (w.mmsi = v.mmsi OR w.imo = v.imo))";

    private final NamedParameterJdbcTemplate jdbc;

    public VesselQueries(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @param q            MMSI (or its start), IMO number (with or without "IMO") or part of the name
     * @param listed       only vessels on any OpenSanctions maritime list
     */
    public PageResult<VesselSummary> search(String q, boolean sanctioned, boolean listed, boolean watchlisted,
                                            int page, int size) {
        PageResult.check(page, size);
        List<String> where = new ArrayList<>();
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (q != null && !q.isBlank()) {
            String text = q.trim();
            List<String> any = new ArrayList<>();
            any.add("v.name ILIKE '%' || :name || '%'");
            params.addValue("name", escapeLike(text));
            if (text.matches("\\d{1,9}")) {
                any.add("v.mmsi LIKE :mmsi || '%'");
                params.addValue("mmsi", text);
            }
            var imo = IMO.matcher(text);
            if (imo.matches()) {
                any.add("v.imo = :imo");
                params.addValue("imo", imo.group(1));
            }
            where.add("(" + String.join(" OR ", any) + ")");
        }
        if (sanctioned) {
            where.add(SANCTIONED);
        }
        if (listed) {
            where.add(ANY_LISTING);
        }
        if (watchlisted) {
            where.add(WATCHLISTED);
        }
        String sql = "SELECT v.mmsi, v.imo, v.name, v.call_sign, v.ship_type, v.length_m, v.destination, v.reported_at, "
                + SANCTIONED + " AS sanctioned, " + ANY_LISTING + " AS listed, " + WATCHLISTED + " AS watchlisted "
                + "FROM vessel v"
                + (where.isEmpty() ? "" : " WHERE " + String.join(" AND ", where))
                + " ORDER BY v.name NULLS LAST, v.mmsi" + PageResult.limit(page, size);
        List<VesselSummary> rows = jdbc.query(sql, params, (rs, i) -> new VesselSummary(
                rs.getString("mmsi"), rs.getString("imo"), rs.getString("name"), rs.getString("call_sign"),
                Rows.integer(rs, "ship_type"), Rows.integer(rs, "length_m"), rs.getString("destination"),
                Rows.instant(rs, "reported_at"), rs.getBoolean("sanctioned"), rs.getBoolean("listed"),
                rs.getBoolean("watchlisted")));
        return PageResult.of(rows, page, size);
    }

    public VesselProfile profile(String mmsi) {
        requireMmsi(mmsi);
        var params = new MapSqlParameterSource("mmsi", mmsi);
        VesselData data = jdbc.query("""
                SELECT mmsi, imo, name, call_sign, ship_type, length_m, beam_m, draught_m, destination, eta,
                       source, reported_at
                FROM vessel WHERE mmsi = :mmsi""", params, (rs, i) -> new VesselData(
                rs.getString("mmsi"), rs.getString("imo"), rs.getString("name"), rs.getString("call_sign"),
                Rows.integer(rs, "ship_type"), Rows.integer(rs, "length_m"), Rows.integer(rs, "beam_m"),
                Rows.decimal(rs, "draught_m"), rs.getString("destination"), Rows.instant(rs, "eta"),
                rs.getString("source"), Rows.instant(rs, "reported_at"))).stream().findFirst().orElse(null);
        TrackPointView last = jdbc.query("SELECT " + Mappers.TRACK_POINT_COLUMNS
                        + " FROM track_point WHERE mmsi = :mmsi ORDER BY time DESC LIMIT 1", params, Mappers.TRACK_POINT)
                .stream().findFirst().orElse(null);
        var detections = jdbc.query("SELECT " + Mappers.DETECTION_COLUMNS
                + " FROM detection d WHERE d.mmsi = :mmsi OR d.other_mmsi = :mmsi ORDER BY d.started_at DESC LIMIT 50",
                params, Mappers.DETECTION);
        if (data == null && last == null && detections.isEmpty()) {
            throw ApiException.notFound("No vessel with MMSI " + mmsi + " has been seen");
        }

        String imo = data == null ? null : data.imo();
        params.addValue("imo", imo);
        var identities = jdbc.query("""
                SELECT source, imo, name, call_sign, ship_type, length_m, beam_m, first_seen, last_seen
                FROM identity_report WHERE mmsi = :mmsi ORDER BY last_seen DESC""", params, (rs, i) -> new IdentityView(
                rs.getString("source"), rs.getString("imo"), rs.getString("name"), rs.getString("call_sign"),
                Rows.integer(rs, "ship_type"), Rows.integer(rs, "length_m"), Rows.integer(rs, "beam_m"),
                Rows.instant(rs, "first_seen"), Rows.instant(rs, "last_seen")));
        List<String> sameImo = imo == null ? List.of() : jdbc.queryForList("""
                SELECT DISTINCT mmsi FROM identity_report WHERE imo = :imo AND mmsi <> :mmsi ORDER BY mmsi""",
                params, String.class);
        var listings = jdbc.query("""
                SELECT id, name, imo, mmsi, topics, flag, datasets, url FROM sanction
                WHERE imo = :imo OR mmsi @> ARRAY[CAST(:mmsi AS text)] ORDER BY name""", params, (rs, i) -> new ListingView(
                rs.getString("id"), rs.getString("name"), rs.getString("imo"), Rows.strings(rs, "mmsi"),
                Rows.strings(rs, "topics"), rs.getString("flag"), Rows.strings(rs, "datasets"), rs.getString("url")));
        var watchlist = jdbc.query("""
                SELECT * FROM watchlist_entry WHERE active AND (mmsi = :mmsi OR imo = :imo) ORDER BY added_at DESC""",
                params, Mappers.WATCHLIST);
        var cases = jdbc.query("SELECT * FROM vessel_case WHERE mmsi = :mmsi ORDER BY opened_at DESC", params,
                Mappers.CASE_SUMMARY);
        return new VesselProfile(mmsi, data, last, identities, sameImo, listings, watchlist, detections, cases);
    }

    /**
     * Track points in [from, to), including those kept around detections after their day expired. UNION,
     * not UNION ALL: a kept point delivered again after its day was dropped is in both tables.
     */
    public List<TrackPointView> track(String mmsi, Instant from, Instant to) {
        requireMmsi(mmsi);
        if (!from.isBefore(to)) {
            throw ApiException.badRequest("from must be before to");
        }
        if (Duration.between(from, to).compareTo(MAX_TRACK_WINDOW) > 0) {
            throw ApiException.badRequest("A track window can be at most " + MAX_TRACK_WINDOW.toHours() + " hours");
        }
        var params = new MapSqlParameterSource("mmsi", mmsi)
                .addValue("from", Timestamp.from(from)).addValue("to", Timestamp.from(to));
        return jdbc.query("SELECT " + Mappers.TRACK_POINT_COLUMNS
                + " FROM track_point WHERE mmsi = :mmsi AND time >= :from AND time < :to"
                + " UNION SELECT " + Mappers.TRACK_POINT_COLUMNS
                + " FROM track_point_kept WHERE mmsi = :mmsi AND time >= :from AND time < :to"
                + " ORDER BY time", params, Mappers.TRACK_POINT);
    }

    static void requireMmsi(String mmsi) {
        if (mmsi == null || !MMSI.matcher(mmsi).matches()) {
            throw ApiException.badRequest("An MMSI has 9 digits");
        }
    }

    /** Makes % and _ in user text match literally in ILIKE (default escape character is \). */
    static String escapeLike(String text) {
        return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}
