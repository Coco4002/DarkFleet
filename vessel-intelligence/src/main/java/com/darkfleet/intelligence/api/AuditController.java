package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.web.PageResult;
import com.darkfleet.intelligence.web.Rows;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/v1/audit")
@PreAuthorize(Access.ADMIN)
@Tag(name = "Audit")
class AuditController {

    record AuditEntry(long id, Instant at, String actor, String action, String entityType, String entityId,
                      JsonNode before, JsonNode after) {
    }

    private final NamedParameterJdbcTemplate jdbc;

    AuditController(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping
    @Operation(summary = "Who decided what and when, with the state before and after (administrators)")
    PageResult<AuditEntry> list(@RequestParam(required = false) String entityType,
                                @RequestParam(required = false) String entityId,
                                @RequestParam(defaultValue = "0") int page,
                                @RequestParam(defaultValue = "" + PageResult.DEFAULT_SIZE) int size) {
        PageResult.check(page, size);
        List<String> where = new ArrayList<>();
        var params = new MapSqlParameterSource();
        if (entityType != null) {
            where.add("entity_type = :type");
            params.addValue("type", entityType);
        }
        if (entityId != null) {
            where.add("entity_id = :id");
            params.addValue("id", entityId);
        }
        return PageResult.of(jdbc.query("SELECT * FROM audit_log"
                + (where.isEmpty() ? "" : " WHERE " + String.join(" AND ", where))
                + " ORDER BY at DESC, id DESC" + PageResult.limit(page, size), params, (rs, i) -> new AuditEntry(
                rs.getLong("id"), Rows.instant(rs, "at"), rs.getString("actor"), rs.getString("action"),
                rs.getString("entity_type"), rs.getString("entity_id"), Rows.json(rs, "before"),
                Rows.json(rs, "after"))), page, size);
    }
}
