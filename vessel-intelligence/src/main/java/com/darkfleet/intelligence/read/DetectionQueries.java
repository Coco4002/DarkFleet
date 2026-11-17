package com.darkfleet.intelligence.read;

import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.intelligence.read.Views.DetectionDetail;
import com.darkfleet.intelligence.read.Views.DetectionView;
import com.darkfleet.intelligence.web.ApiException;
import com.darkfleet.intelligence.web.PageResult;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Repository
@Transactional(readOnly = true)
public class DetectionQueries {

    private final NamedParameterJdbcTemplate jdbc;

    public DetectionQueries(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Detections that started in [from, to), newest first.
     *
     * @param mmsi either vessel of the detection
     * @param bbox area the detection lies in; detections without a position never match it
     */
    public PageResult<DetectionView> list(DetectionType type, DetectionStatus status, String mmsi, Instant from,
                                          Instant to, BoundingBox bbox, int page, int size) {
        PageResult.check(page, size);
        List<String> where = new ArrayList<>();
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (type != null) {
            where.add("d.type = :type");
            params.addValue("type", type.name());
        }
        if (status != null) {
            where.add("d.status = :status");
            params.addValue("status", status.name());
        }
        if (mmsi != null) {
            VesselQueries.requireMmsi(mmsi);
            where.add("(d.mmsi = :mmsi OR d.other_mmsi = :mmsi)");
            params.addValue("mmsi", mmsi);
        }
        if (from != null) {
            where.add("d.started_at >= :from");
            params.addValue("from", Timestamp.from(from));
        }
        if (to != null) {
            where.add("d.started_at < :to");
            params.addValue("to", Timestamp.from(to));
        }
        if (bbox != null) {
            where.add("d.position && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)");
            params.addValue("minLon", bbox.minLon()).addValue("minLat", bbox.minLat())
                    .addValue("maxLon", bbox.maxLon()).addValue("maxLat", bbox.maxLat());
        }
        String sql = "SELECT " + Mappers.DETECTION_COLUMNS + " FROM detection d"
                + (where.isEmpty() ? "" : " WHERE " + String.join(" AND ", where))
                + " ORDER BY d.started_at DESC, d.detection_id" + PageResult.limit(page, size);
        return PageResult.of(jdbc.query(sql, params, Mappers.DETECTION), page, size);
    }

    public DetectionDetail get(String detectionId) {
        var params = new MapSqlParameterSource("id", detectionId);
        DetectionView detection = jdbc.query("SELECT " + Mappers.DETECTION_COLUMNS
                        + " FROM detection d WHERE d.detection_id = :id", params, Mappers.DETECTION)
                .stream().findFirst()
                .orElseThrow(() -> ApiException.notFound("No detection " + detectionId));
        var assessments = jdbc.query("SELECT * FROM assessment WHERE detection_id = :id ORDER BY created_at",
                params, Mappers.ASSESSMENT);
        var cases = jdbc.queryForList("""
                SELECT case_id::text FROM vessel_case_detection WHERE detection_id = :id ORDER BY case_id""",
                params, String.class);
        return new DetectionDetail(detection, assessments, cases);
    }
}
