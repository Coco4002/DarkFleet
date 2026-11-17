package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.sanctions.SanctionImporter;
import com.darkfleet.intelligence.web.Caller;
import com.darkfleet.intelligence.web.Rows;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/api/v1/sanctions")
@Tag(name = "Sanction lists")
class SanctionController {

    /** The list's state: the latest successful import and the latest attempt (which may have failed). */
    record Status(long vessels, Attempt lastSuccess, Attempt lastAttempt, String licence) {
    }

    record Attempt(String status, Instant finishedAt, Integer vessels, String message, String sourceUrl) {
    }

    private static final String LICENCE = "OpenSanctions maritime list, CC BY-NC 4.0 (non-commercial use)";

    private final SanctionImporter importer;
    private final JdbcTemplate jdbc;

    SanctionController(SanctionImporter importer, JdbcTemplate jdbc) {
        this.importer = importer;
        this.jdbc = jdbc;
    }

    @GetMapping("/status")
    @PreAuthorize(Access.READ)
    @Operation(summary = "How many listed vessels are loaded and when the list was last imported")
    Status status() {
        long vessels = jdbc.queryForObject("SELECT count(*) FROM sanction", Long.class);
        return new Status(vessels,
                attempt("WHERE status IN ('IMPORTED', 'UNCHANGED')"),
                attempt(""),
                LICENCE);
    }

    @PostMapping("/import")
    @PreAuthorize(Access.ADMIN)
    @Operation(summary = "Import the list now (administrators); 409 while another import runs")
    SanctionImporter.Result importNow(Authentication authentication) {
        return importer.importList(Caller.of(authentication).name());
    }

    private Attempt attempt(String where) {
        return jdbc.query("SELECT status, finished_at, vessels, message, source_url FROM sanction_import " + where
                        + " ORDER BY id DESC LIMIT 1",
                (rs, i) -> new Attempt(rs.getString("status"), Rows.instant(rs, "finished_at"),
                        Rows.integer(rs, "vessels"), rs.getString("message"), rs.getString("source_url")))
                .stream().findFirst().orElse(null);
    }
}
